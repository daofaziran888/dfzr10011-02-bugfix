"""节点状态链路：集群节点、心跳、生命周期回执与节点可用性结果的唯一事实来源。

采集节点集群、节点心跳、节点生命周期记录原先各自读一份“节点状态”，修复节点
下线后再校验时就会各自算出不同结论。这里把所有状态读取收敛到一次求值：

* 节点记录上的 ``status`` 是生命周期状态（待加入 / 运行中 / 已离线 / 已移除），
  展示字段 ``节点状态`` 永远由它镜像，不再单独维护。
* 心跳与生命周期回执只追加、不覆盖；两份回执都保留时间与接收顺序，恢复与
  离线的先后可以逐条追溯。
* 节点可用性结果由回执求值后持久化，节点详情与分片候选范围读的是同一条记录。
* 恢复/离线两份回执冲突时，终态策略决定结果：
  - ``recovery_first``（恢复终态优先）：详情显示恢复，分片候选暂保留节点，
    复核清单附一条“离线待清理”。
  - ``offline_first``（离线终态优先）：详情显示离线，分片候选立即移除节点，
    复核清单附一条“恢复待办”。
* 重复回执按 ``(节点编号, receipt_id)`` 幂等，不追加记录、不新建节点、不升版本。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.store import store

NODE_MODULE = "cluster"
POLICY_MODULE = "node_state_policy"
LIFECYCLE_MODULE = "node_lifecycle"
HEARTBEAT_MODULE = "node_heartbeat"
AVAILABILITY_MODULE = "node_availability"

POLICY_RECOVERY_FIRST = "recovery_first"
POLICY_OFFLINE_FIRST = "offline_first"
DEFAULT_POLICY = POLICY_OFFLINE_FIRST
POLICY_LABELS = {
    POLICY_RECOVERY_FIRST: "恢复终态优先",
    POLICY_OFFLINE_FIRST: "离线终态优先",
}
POLICIES = tuple(POLICY_LABELS)

STATUS_PENDING = "待加入"
STATUS_ONLINE = "运行中"
STATUS_OFFLINE = "已离线"
STATUS_REMOVED = "已移除"
STATUS_ORDER = [STATUS_PENDING, STATUS_ONLINE, STATUS_OFFLINE, STATUS_REMOVED]

AVAILABILITY_ONLINE = "在线"
AVAILABILITY_OFFLINE = "已离线"
AVAILABILITY_PENDING = "待加入"
AVAILABILITY_REMOVED = "已移除"

RECHECK_CLEANUP = "离线待清理"
RECHECK_RESTORE = "恢复待办"
RECHECK_OFFLINE = "下线复核"
RECHECK_PENDING = "待处理"
RECHECK_DONE = "已完成"

EVENT_RECOVERY = "恢复"
EVENT_OFFLINE = "离线"
EVENT_REMOVE = "移除"
EVENT_JOIN = "加入"
EVENT_ONLINE = "在线"

ONLINE_EVENTS = {EVENT_RECOVERY, EVENT_JOIN, EVENT_ONLINE}
RECOVERY_EVENTS = {EVENT_RECOVERY, EVENT_ONLINE}
JOIN_EVENTS = {EVENT_JOIN, "加入集群"}
OFFLINE_EVENTS = {EVENT_OFFLINE, "下线", "下线节点"}
REMOVE_EVENTS = {EVENT_REMOVE, "移除节点"}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _receipt_order(receipt: dict[str, Any]) -> tuple[int, str]:
    return int(receipt.get("接收序号") or 0), str(receipt.get("received_at") or "")


class NodeStateService:
    """节点状态求值、回执落库与可用性结果同步。"""

    # ---------- 终态策略 ----------

    def policy_record(self) -> dict[str, Any]:
        rows = store.rows(POLICY_MODULE)
        if not rows:
            row = {
                "id": 1,
                "policy": DEFAULT_POLICY,
                "策略名称": POLICY_LABELS[DEFAULT_POLICY],
                "更新人": "系统默认",
                "更新时间": _now(),
            }
            rows.append(row)
            return row
        return rows[0]

    def policy(self) -> str:
        policy = str(self.policy_record().get("policy") or DEFAULT_POLICY)
        return policy if policy in POLICIES else DEFAULT_POLICY

    def describe_policy(self) -> dict[str, Any]:
        record = self.policy_record()
        policy = self.policy()
        return {
            "policy": policy,
            "策略名称": POLICY_LABELS[policy],
            "候选": [{"policy": key, "名称": label} for key, label in POLICY_LABELS.items()],
            "更新人": record.get("更新人"),
            "更新时间": record.get("更新时间"),
        }

    def set_policy(self, policy: str, actor: str = "值班管理员") -> tuple[dict[str, Any] | None, str]:
        if policy not in POLICIES:
            return None, f"终态策略「{policy}」不在允许范围内"
        with store.transaction():
            record = self.policy_record()
            record["policy"] = policy
            record["策略名称"] = POLICY_LABELS[policy]
            record["更新人"] = actor
            record["更新时间"] = _now()
            for node in store.rows(NODE_MODULE):
                self.sync_availability(node)
            self.record_policy_event(policy, actor)
        return self.describe_policy(), f"终态策略已切换为{POLICY_LABELS[policy]}"

    def record_policy_event(self, policy: str, actor: str) -> dict[str, Any]:
        rows = store.rows(LIFECYCLE_MODULE)
        entry = {
            "id": store.next_id(LIFECYCLE_MODULE),
            "节点编号": "*",
            "receipt_id": f"POLICY-{policy}",
            "事件": "策略切换",
            "from_status": "",
            "to_status": policy,
            "回执时间": _now(),
            "received_at": _now(),
            "接收序号": self._next_seq(rows),
            "来源": f"策略切换（{actor}）",
        }
        rows.append(entry)
        return entry

    # ---------- 回执与心跳 ----------

    def _next_seq(self, rows: list[dict[str, Any]]) -> int:
        return max((int(row.get("接收序号") or 0) for row in rows), default=0) + 1

    def receipts_for(self, node_code: str) -> list[dict[str, Any]]:
        rows = [
            dict(row)
            for row in store.rows(LIFECYCLE_MODULE)
            if str(row.get("节点编号")) == node_code
        ]
        rows.sort(key=_receipt_order)
        return rows

    def heartbeat_for(self, node_code: str) -> list[dict[str, Any]]:
        rows = [
            dict(row)
            for row in store.rows(HEARTBEAT_MODULE)
            if str(row.get("节点编号")) == node_code
        ]
        rows.sort(key=_receipt_order)
        return rows

    def record_heartbeat(
        self,
        node: dict[str, Any],
        *,
        receipt_id: str,
        beat_time: str | None = None,
        status: str | None = None,
    ) -> tuple[dict[str, Any] | None, str, bool]:
        """登记一次心跳；心跳里若带状态，走与生命周期回执同一套求值。

        返回 ``(节点, 说明, 是否重复回执)``。重复回执不追加记录、不升版本。
        """
        node_code = str(node.get("节点编号"))
        receipt_id = (receipt_id or "").strip()
        if not receipt_id:
            return None, "心跳回执缺少 receipt_id，无法追溯", False
        duplicated = store.find_by(HEARTBEAT_MODULE, 节点编号=node_code, receipt_id=receipt_id)
        if duplicated is not None:
            return node, f"心跳回执 {receipt_id} 已登记，按幂等处理", True

        beat_time = (beat_time or _now()).strip()
        beat_status = (status or "").strip()
        with store.transaction():
            heartbeat = {
                "id": store.next_id(HEARTBEAT_MODULE),
                "节点编号": node_code,
                "receipt_id": receipt_id,
                "心跳时间": beat_time,
                "状态": beat_status or EVENT_ONLINE,
                "received_at": _now(),
                "接收序号": self._next_seq(store.rows(HEARTBEAT_MODULE)),
            }
            store.rows(HEARTBEAT_MODULE).append(heartbeat)
            node["心跳时间"] = beat_time
            node["节点状态"] = node.get("status")
            if beat_status:
                event = self._event_from_status(beat_status)
                applied, message = self.apply_event(
                    node, event=event, receipt_id=receipt_id, occurred_at=beat_time, source="心跳回执"
                )
                if applied is None:
                    raise ValueError(message)
            else:
                self.sync_availability(node)
                message = "心跳已登记"
        return node, message, False

    @staticmethod
    def _event_from_status(status: str) -> str:
        if status in ("离线", STATUS_OFFLINE, "已下线"):
            return EVENT_OFFLINE
        return EVENT_RECOVERY

    def apply_event(
        self,
        node: dict[str, Any],
        *,
        event: str,
        receipt_id: str,
        occurred_at: str | None = None,
        source: str = "生命周期回执",
    ) -> tuple[dict[str, Any] | None, str]:
        """追加一条生命周期回执并同步节点状态与可用性结果。

        必须在调用方的事务里执行；节点记录的改写与可用性结果同生共死。
        """
        node_code = str(node.get("节点编号"))
        receipt_id = (receipt_id or "").strip()
        if not receipt_id:
            return None, "生命周期回执缺少 receipt_id，无法追溯"
        if event in ONLINE_EVENTS:
            normalized = EVENT_RECOVERY
        elif event in OFFLINE_EVENTS:
            normalized = EVENT_OFFLINE
        elif event in REMOVE_EVENTS:
            normalized = EVENT_REMOVE
        else:
            return None, f"事件「{event}」不属于节点生命周期可受理范围"

        existing = store.find_by(LIFECYCLE_MODULE, 节点编号=node_code, receipt_id=receipt_id)
        if existing is not None:
            self.sync_availability(node)
            return node, f"回执 {receipt_id} 已处理，按幂等处理（版本保留）"

        lifecycle = {
            "id": store.next_id(LIFECYCLE_MODULE),
            "节点编号": node_code,
            "receipt_id": receipt_id,
            "事件": normalized,
            "from_status": node.get("status"),
            "to_status": "",
            "回执时间": occurred_at or _now(),
            "received_at": _now(),
            "接收序号": self._next_seq(store.rows(LIFECYCLE_MODULE)),
            "来源": source,
        }
        store.rows(LIFECYCLE_MODULE).append(lifecycle)

        evaluated = self.evaluate(node_code, base_status=str(node.get("status") or STATUS_PENDING))
        node["status"] = evaluated["节点状态"]
        node["节点状态"] = evaluated["节点状态"]
        node["pending"] = evaluated["节点状态"] not in (STATUS_REMOVED,)
        node["abnormal"] = evaluated["节点状态"] == STATUS_OFFLINE
        lifecycle["to_status"] = evaluated["节点状态"]
        # 可用性结果与节点记录在同一个事务里落库，调用方无需再补一次。
        self.sync_availability(node)
        return node, f"节点 {node_code} 已记录{normalized}回执 {receipt_id}"

    # ---------- 求值 ----------

    def evaluate(self, node_code: str, *, base_status: str | None = None) -> dict[str, Any]:
        """由回执求值出节点生命周期状态与可用性结果（不落库）。

        冲突只发生在“先恢复、随后离线”这一种先后上：恢复回执之后又来了离线
        回执，两份回执的先后都能逐条追溯。离线之后再收到恢复属于正常重新上线，
        不构成冲突。加入集群不算恢复，不会和普通下线拼成冲突。
        """
        policy = self.policy()
        receipts = self.receipts_for(node_code)
        node = store.find_by(NODE_MODULE, 节点编号=node_code)
        if base_status is None:
            base_status = str((node or {}).get("status") or STATUS_PENDING)

        last_join = self._latest(receipts, JOIN_EVENTS)
        last_recovery = self._latest(receipts, RECOVERY_EVENTS)
        last_offline = self._latest(receipts, OFFLINE_EVENTS)
        last_remove = self._latest(receipts, REMOVE_EVENTS)
        online_receipts = [row for row in (last_join, last_recovery) if row is not None]
        last_online = max(online_receipts, key=_receipt_order) if online_receipts else None

        # 先恢复、随后离线：两份回执同时存在且恢复排在离线之前。
        conflict = (
            last_recovery is not None
            and last_offline is not None
            and _receipt_order(last_recovery) < _receipt_order(last_offline)
            and (
                last_remove is None
                or _receipt_order(last_remove) < _receipt_order(last_offline)
            )
        )

        terminals = [row for row in (last_online, last_offline, last_remove) if row is not None]
        latest = max(terminals, key=_receipt_order) if terminals else None
        if conflict:
            chosen = last_recovery if policy == POLICY_RECOVERY_FIRST else last_offline
        else:
            chosen = latest

        if chosen is None:
            status = base_status if base_status in STATUS_ORDER else STATUS_PENDING
            availability = {
                STATUS_PENDING: AVAILABILITY_PENDING,
                STATUS_ONLINE: AVAILABILITY_ONLINE,
                STATUS_OFFLINE: AVAILABILITY_OFFLINE,
                STATUS_REMOVED: AVAILABILITY_REMOVED,
            }.get(status, AVAILABILITY_PENDING)
            candidate = status == STATUS_ONLINE
            source = "节点状态"
            terminal = status
        else:
            if chosen.get("事件") in REMOVE_EVENTS:
                status = STATUS_REMOVED
                availability = AVAILABILITY_REMOVED
                candidate = False
                terminal = "移除"
            elif chosen.get("事件") in ONLINE_EVENTS:
                status = STATUS_ONLINE
                availability = AVAILABILITY_ONLINE
                candidate = True
                terminal = "恢复" if chosen.get("事件") == EVENT_RECOVERY else "加入"
            else:
                status = STATUS_OFFLINE
                availability = AVAILABILITY_OFFLINE
                candidate = False
                terminal = "离线"
            source = "生命周期回执"

        offline_receipt = last_offline.get("receipt_id") if last_offline else None
        recovery_receipt = last_recovery.get("receipt_id") if last_recovery else None

        recheck: list[dict[str, Any]] = []
        conflict_note = ""
        if conflict and last_offline and last_recovery:
            if policy == POLICY_RECOVERY_FIRST:
                conflict_note = (
                    f"恢复回执 {last_recovery['receipt_id']} 与离线回执 "
                    f"{last_offline['receipt_id']} 冲突，按恢复终态优先：详情显示恢复，"
                    "分片候选暂保留节点，离线状态待清理"
                )
                recheck.append({
                    "类型": RECHECK_CLEANUP,
                    "节点编号": node_code,
                    "receipt_id": last_offline["receipt_id"],
                    "回执时间": last_offline.get("回执时间"),
                    "说明": (
                        f"离线回执 {last_offline['receipt_id']} 尚未清理，"
                        "节点暂保留在分片候选范围"
                    ),
                    "状态": RECHECK_PENDING,
                })
            else:
                conflict_note = (
                    f"恢复回执 {last_recovery['receipt_id']} 与离线回执 "
                    f"{last_offline['receipt_id']} 冲突，按离线终态优先：详情显示离线，"
                    "分片候选立即移除节点，恢复事项待办"
                )
                recheck.append({
                    "类型": RECHECK_RESTORE,
                    "节点编号": node_code,
                    "receipt_id": last_recovery["receipt_id"],
                    "回执时间": last_recovery.get("回执时间"),
                    "说明": (
                        f"恢复回执 {last_recovery['receipt_id']} 未生效，"
                        "节点已移出分片候选范围"
                    ),
                    "状态": RECHECK_PENDING,
                })
        elif chosen is not None and status == STATUS_OFFLINE:
            recheck.append({
                "类型": RECHECK_OFFLINE,
                "节点编号": node_code,
                "receipt_id": offline_receipt,
                "回执时间": (last_offline or {}).get("回执时间"),
                "说明": "节点已按离线回执移出分片候选范围",
                "状态": RECHECK_DONE,
            })

        trace = [str(row.get("receipt_id")) for row in receipts]
        availability_record = {
            "节点编号": node_code,
            "可用性状态": availability,
            "candidate": candidate,
            "冲突": conflict,
            "终态": terminal,
            "终态策略": policy,
            "策略名称": POLICY_LABELS[policy],
            "候选说明": conflict_note or self._candidate_note(availability, candidate),
            "下线依据": offline_receipt,
            "恢复依据": recovery_receipt,
            "依据回执": trace,
            "状态来源": source,
            "复核清单": recheck,
            "节点状态": status,
        }
        return availability_record

    @staticmethod
    def _candidate_note(availability: str, candidate: bool) -> str:
        if candidate:
            return "节点可用性结果为在线，保留在分片候选范围"
        if availability == AVAILABILITY_REMOVED:
            return "节点已移除，不在分片候选范围"
        if availability == AVAILABILITY_PENDING:
            return "节点待加入，尚未进入分片候选范围"
        return "节点已离线，已移出分片候选范围"

    @staticmethod
    def _latest(receipts: list[dict[str, Any]], events: set[str]) -> dict[str, Any] | None:
        matched = [row for row in receipts if row.get("事件") in events]
        return matched[-1] if matched else None

    # ---------- 可用性结果落库 ----------

    def sync_availability(self, node: dict[str, Any]) -> dict[str, Any]:
        """把求值结果写入节点可用性结果表，并让节点记录镜像同一个状态。

        节点记录的 ``status`` 与展示字段 ``节点状态`` 都从这里统一改写，避免
        “可用性结果已经下线、节点详情还显示运行中”的错位；语义没变时只同步
        展示字段，版本保持原版本（重复回执、刷新详情都不升版本）。
        """
        evaluated = self.evaluate(str(node.get("节点编号")))
        node["status"] = evaluated["节点状态"]
        node["节点状态"] = evaluated["节点状态"]
        node["pending"] = evaluated["节点状态"] != STATUS_REMOVED
        node["abnormal"] = evaluated["节点状态"] == STATUS_OFFLINE
        rows = store.rows(AVAILABILITY_MODULE)
        existing = store.find_by(AVAILABILITY_MODULE, 节点编号=evaluated["节点编号"])
        fingerprint = self._fingerprint(evaluated)
        if existing is None:
            record = {"id": store.next_id(AVAILABILITY_MODULE), "version": 1, "更新时间": _now()}
            record.update(evaluated)
            record["_fingerprint"] = fingerprint
            rows.append(record)
            return record
        if existing.get("_fingerprint") != fingerprint:
            existing.update(evaluated)
            existing["version"] = int(existing.get("version") or 1) + 1
            existing["更新时间"] = _now()
            existing["_fingerprint"] = fingerprint
        else:
            # 语义没变，只同步展示字段，版本保持原样（重复回执/刷新都不升版本）。
            existing["终态策略"] = evaluated["终态策略"]
            existing["策略名称"] = evaluated["策略名称"]
        return existing

    @staticmethod
    def _fingerprint(evaluated: dict[str, Any]) -> tuple[Any, ...]:
        return (
            evaluated["可用性状态"],
            evaluated["终态"],
            bool(evaluated["candidate"]),
            bool(evaluated["冲突"]),
            evaluated["下线依据"],
            evaluated["恢复依据"],
            tuple(evaluated["依据回执"]),
            tuple((item["类型"], item["receipt_id"], item["状态"]) for item in evaluated["复核清单"]),
            evaluated["候选说明"],
        )

    def availability_for(self, node_code: str) -> dict[str, Any] | None:
        """读取节点可用性结果；节点详情与分片候选范围共用这一份下线依据。"""
        if not node_code:
            return None
        existing = store.find_by(AVAILABILITY_MODULE, 节点编号=node_code)
        if existing is None:
            node = store.find_by(NODE_MODULE, 节点编号=node_code)
            if node is None:
                return None
            with store.transaction():
                existing = self.sync_availability(node)
        return existing

    def list_availability(self, *, status: str | None = None) -> list[dict[str, Any]]:
        rows = [dict(row) for row in store.rows(AVAILABILITY_MODULE)]
        # 先按节点记录补全，保证结果数据与节点清单一一对应。
        for node in store.rows(NODE_MODULE):
            code = str(node.get("节点编号"))
            if not any(row.get("节点编号") == code for row in rows):
                available = self.availability_for(code)
                if available is not None:
                    rows.append(dict(available))
        if status:
            rows = [row for row in rows if row.get("可用性状态") == status]
        rows.sort(key=lambda row: str(row.get("节点编号")))
        return [self.public(row) for row in rows]

    @staticmethod
    def public(record: dict[str, Any]) -> dict[str, Any]:
        return {key: value for key, value in record.items() if not key.startswith("_")}

    # ---------- 节点详情 ----------

    def node_detail(self, node: dict[str, Any]) -> dict[str, Any]:
        node_code = str(node.get("节点编号"))
        with store.transaction():
            self.sync_availability(node)
        availability = self.availability_for(node_code) or {}
        receipts = self.receipts_for(node_code)
        return {
            **{key: value for key, value in node.items() if not key.startswith("_")},
            "节点状态": node.get("status"),
            "availability": self.public(availability),
            "生命周期记录": receipts,
            "心跳记录": self.heartbeat_for(node_code),
            "回执追溯": [str(row.get("receipt_id")) for row in receipts],
            "版本": int(availability.get("version") or 1),
        }


node_state = NodeStateService()
