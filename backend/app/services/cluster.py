"""采集节点集群业务规则：状态流转、字段校验与筛选口径都收在这里。

节点状态不再由本模块自己维护一份：加入/下线/移除都记为生命周期回执，
由 ``node_state`` 求值后写回 ``status``，展示字段 ``节点状态`` 始终镜像它。
节点详情和节点可用性结果读同一条下线依据，任务分片调度也消费这份结果。
"""
from __future__ import annotations

from typing import Any

from app.services.node_state import (
    EVENT_OFFLINE,
    EVENT_RECOVERY,
    EVENT_REMOVE,
    STATUS_OFFLINE,
    STATUS_ONLINE,
    STATUS_PENDING,
    STATUS_REMOVED,
    node_state,
)
from app.store import store

MODULE = "cluster"
REQUIRED_FIELDS = ["节点编号", "节点地址", "节点角色"]
ACTION_EVENTS = {
    "加入集群": EVENT_RECOVERY,
    "下线节点": EVENT_OFFLINE,
    "移除节点": EVENT_REMOVE,
}
ACTION_RULES = {"加入集群": STATUS_ONLINE, "下线节点": STATUS_OFFLINE, "移除节点": STATUS_REMOVED}


def _with_status(row: dict[str, Any]) -> dict[str, Any]:
    """列表/详情统一用 ``status`` 作为节点状态的唯一读口径。"""
    projected = {key: value for key, value in row.items() if not key.startswith("_")}
    projected["节点状态"] = row.get("status")
    return projected


class ClusterService:
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        # 列表与详情读同一份结果：先把每个节点的状态/可用性对齐再取页。
        with store.transaction():
            for node in store.rows(MODULE):
                node_state.sync_availability(node)
        rows = store.rows(MODULE)
        if keyword:
            rows = [
                row
                for row in rows
                if keyword in str(row.get("节点编号", "")) or keyword in str(row.get("节点地址", ""))
            ]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return [_with_status(row) for row in rows[start:start + size]], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        return node_state.node_detail(entry)

    def find_entry_by_code(self, node_code: str) -> dict[str, Any] | None:
        return store.find_by(MODULE, 节点编号=node_code)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        node_code = str(values.get("节点编号")).strip()
        existing = store.find_by(MODULE, 节点编号=node_code)
        if existing is not None:
            # 回写幂等：同一个节点编号重复回写只更新字段，不追加第二条节点记录。
            with store.transaction():
                for field in REQUIRED_FIELDS:
                    if values.get(field) is not None:
                        existing[field] = values.get(field)
                node_state.sync_availability(existing)
            return node_state.node_detail(existing), []
        with store.transaction():
            entry = {"id": store.next_id(MODULE)}
            entry.update({field: values.get(field) for field in REQUIRED_FIELDS})
            entry["status"] = STATUS_PENDING
            entry["节点状态"] = STATUS_PENDING
            entry["pending"] = True
            entry["abnormal"] = False
            entry.setdefault("心跳时间", None)
            store.rows(MODULE).append(entry)
            node_state.sync_availability(entry)
        return node_state.node_detail(entry), []

    def run_action(self, entry_id: int, action: str, receipt_id: str | None = None) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"集群节点 {entry_id} 不存在或已归档"
        if action not in ACTION_EVENTS:
            return None, f"动作「{action}」不属于采集节点集群可执行范围"
        event = ACTION_EVENTS[action]
        if receipt_id is None:
            latest = node_state.receipts_for(str(entry.get("节点编号")))
            last = latest[-1] if latest else None
            if last is not None and last.get("事件") == event and entry.get("status") == ACTION_RULES[action]:
                return node_state.node_detail(entry), f"集群节点已{action}，重复回写按幂等处理（版本保留）"
            receipt_id = f"ACT-{entry.get('节点编号')}-{event}-{store.next_id('node_lifecycle')}"
        with store.transaction():
            applied, message = node_state.apply_event(
                entry, event=action, receipt_id=receipt_id, source=f"集群动作：{action}"
            )
            if applied is None:
                raise ValueError(message)
        return node_state.node_detail(applied), message

    def record_receipt(self, entry_id: int, values: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        """接收一条生命周期回执（恢复/离线/移除），两份回执都能逐条追溯。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"集群节点 {entry_id} 不存在或已归档"
        event = str(values.get("event") or values.get("事件") or "").strip()
        receipt_id = str(values.get("receipt_id") or "").strip()
        if not event:
            return None, "生命周期回执缺少 event（恢复/离线/移除）"
        if not receipt_id:
            return None, "生命周期回执缺少 receipt_id，无法追溯先后"
        occurred_at = str(values.get("occurred_at") or "").strip() or None
        with store.transaction():
            applied, message = node_state.apply_event(
                entry,
                event=event,
                receipt_id=receipt_id,
                occurred_at=occurred_at,
                source=str(values.get("source") or "生命周期回执"),
            )
            if applied is None:
                raise ValueError(message)
        return node_state.node_detail(applied), message

    def record_heartbeat(self, entry_id: int, values: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"集群节点 {entry_id} 不存在或已归档"
        receipt_id = str(values.get("receipt_id") or "").strip()
        with store.transaction():
            node, message, _duplicated = node_state.record_heartbeat(
                entry,
                receipt_id=receipt_id,
                beat_time=str(values.get("心跳时间") or values.get("beat_time") or "").strip() or None,
                status=str(values.get("状态") or values.get("status") or "").strip() or None,
            )
            if node is None:
                raise ValueError(message)
        return node_state.node_detail(node), message

    def availability(self, *, status: str | None = None) -> list[dict[str, Any]]:
        return node_state.list_availability(status=status)

    def detail_by_code(self, node_code: str) -> dict[str, Any] | None:
        entry = self.find_entry_by_code(node_code)
        if entry is None:
            return None
        return node_state.node_detail(entry)

    def describe_policy(self) -> dict[str, Any]:
        return node_state.describe_policy()

    def set_policy(self, policy: str, actor: str = "值班管理员") -> tuple[dict[str, Any] | None, str]:
        return node_state.set_policy(policy, actor)
