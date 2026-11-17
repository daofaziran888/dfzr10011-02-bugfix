"""采集节点集群接口：维护集群节点，覆盖加入集群、下线节点、移除节点等动作。

除业务动作外，还暴露节点心跳、生命周期回执、终态策略和节点可用性结果：
节点详情与任务分片调度的候选范围读的是同一份可用性记录。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.cluster import ClusterService

router = APIRouter(prefix="/api/cluster", tags=["采集节点集群"])

service = ClusterService()

LIST_FIELDS = ["节点编号", "节点地址", "节点角色", "CPU核心", "内存容量", "网络带宽", "心跳时间", "节点状态"]
STATUSES = ["待加入", "运行中", "已离线", "已移除"]


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按节点编号检索"),
    status: str | None = Query(default=None, description="待加入、运行中、已离线、已移除"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按节点编号与状态过滤采集节点集群列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/availability")
def list_availability(status: str | None = Query(default=None, description="在线、已离线、待加入、已移除")) -> dict[str, Any]:
    """节点可用性结果数据：与节点详情、分片候选范围共用同一条下线依据。"""
    items = service.availability(status=status)
    return {"items": items, "total": len(items), "policy": service.describe_policy()}


@router.get("/policy")
def get_policy() -> dict[str, Any]:
    """当前冲突终态策略及可选做法。"""
    return service.describe_policy()


@router.post("/policy", response_model=ActionResult)
def set_policy(payload: EntryPayload) -> ActionResult:
    """切换恢复终态优先 / 离线终态优先，并同步刷新全部可用性结果。"""
    policy = str(payload.values.get("policy") or "").strip()
    actor = str(payload.values.get("actor") or "值班管理员").strip()
    detail, message = service.set_policy(policy, actor)
    if detail is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=detail)


@router.get("/by-code/{node_code}")
def get_entry_by_code(node_code: str) -> dict:
    """按节点编号读取节点详情，包含生命周期回执、心跳与可用性结果。"""
    entry = service.detail_by_code(node_code)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"集群节点 {node_code} 不存在或已归档")
    return entry


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出采集节点集群清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "cluster", "total": total, "items": items}


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条集群节点明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"集群节点 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条集群节点；同一节点编号重复回写只更新原记录，不追加第二条。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="集群节点已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条集群节点执行加入集群、下线节点、移除节点；不允许的动作会被拦下并说明原因。

    可选传 ``receipt_id`` 让外部回执可追溯；不传时按动作生成一条可回溯的回执编号。
    """
    action = str(payload.values.get("action") or "").strip()
    receipt_id = str(payload.values.get("receipt_id") or "").strip() or None
    entry, message = service.run_action(entry_id, action, receipt_id=receipt_id)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.post("/{entry_id}/receipts", response_model=ActionResult)
def record_receipt(entry_id: int, payload: EntryPayload) -> ActionResult:
    """登记一条生命周期回执（恢复/离线/移除）；重复回执按幂等处理。"""
    entry, message = service.record_receipt(entry_id, payload.values)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.post("/{entry_id}/heartbeats", response_model=ActionResult)
def record_heartbeat(entry_id: int, payload: EntryPayload) -> ActionResult:
    """登记一次节点心跳；带状态的心跳与生命周期回执走同一套求值。"""
    entry, message = service.record_heartbeat(entry_id, payload.values)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
