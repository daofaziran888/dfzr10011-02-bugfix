"""任务分片调度业务规则：状态流转、字段校验与筛选口径都收在这里。

分片候选范围不再自己判断节点死活，而是消费 ``cluster`` 写出的节点可用性结果：
可用性结果为在线才进入候选，已离线/已移除的节点立即移出候选范围。
"""
from __future__ import annotations

from typing import Any

from app.services.node_state import node_state
from app.store import store

MODULE = "shard"
REQUIRED_FIELDS = ["分片编号", "采集计划", "分片范围"]
STATUS_ORDER = ["待分配", "执行中", "已完成", "执行失败"]
ACTION_RULES = {"分配节点": "待分配", "重试分片": "执行中", "标记完成": "执行失败"}
NEGATIVE_ACTIONS = []


class ShardService:
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("分片编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        rows = [self._with_candidate(row) for row in rows]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def _with_candidate(self, row: dict[str, Any]) -> dict[str, Any]:
        """给分片带上所分配节点的可用性结果，页面据此展示候选是否还在。"""
        projected = {key: value for key, value in row.items() if not key.startswith("_")}
        node_code = str(projected.get("分配节点") or "")
        available = node_state.availability_for(node_code) if node_code else None
        projected["节点可用性"] = available.get("可用性状态") if available else None
        projected["节点候选"] = bool(available.get("candidate")) if available else None
        projected["终态策略"] = available.get("终态策略") if available else None
        projected["下线依据"] = available.get("下线依据") if available else None
        projected["恢复依据"] = available.get("恢复依据") if available else None
        return projected

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        return self._with_candidate(entry)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in REQUIRED_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def candidates(self) -> list[dict[str, Any]]:
        """分片可分配的节点候选范围：只保留可用性结果为在线的节点。"""
        return [item for item in node_state.list_availability() if item.get("candidate")]

    def recheck_page(self) -> dict[str, Any]:
        """修复节点下线后状态复核页的数据：结果数据与待清理/待办清单对齐。"""
        availability = node_state.list_availability()
        recheck: list[dict[str, Any]] = []
        for item in availability:
            for task in item.get("复核清单") or []:
                recheck.append({
                    **task,
                    "可用性状态": item.get("可用性状态"),
                    "终态策略": item.get("终态策略"),
                    "候选": item.get("candidate"),
                    "下线依据": item.get("下线依据"),
                    "恢复依据": item.get("恢复依据"),
                })
        candidates = [item for item in availability if item.get("candidate")]
        excluded = [item for item in availability if not item.get("candidate")]
        return {
            "policy": node_state.describe_policy(),
            "availability": availability,
            "candidates": candidates,
            "excluded": excluded,
            "recheck": recheck,
            "summary": {
                "节点总数": len(availability),
                "候选节点": len(candidates),
                "已移出候选": len(excluded),
                "复核项": len(recheck),
                "待清理": sum(1 for item in recheck if item.get("类型") == "离线待清理"),
                "待办": sum(1 for item in recheck if item.get("类型") == "恢复待办"),
            },
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"分片任务 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于任务分片调度可执行范围"
        if action == "分配节点":
            node_code = str(entry.get("分配节点") or "")
            if node_code:
                available = node_state.availability_for(node_code)
                if available is not None and not available.get("candidate"):
                    return None, (
                        f"节点 {node_code} 当前可用性为{available.get('可用性状态')}，"
                        "不在分片候选范围内，需先处理复核清单"
                    )
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return self._with_candidate(entry), f"分片任务已{action}"
