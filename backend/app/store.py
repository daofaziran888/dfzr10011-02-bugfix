"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。
节点心跳、节点生命周期记录与节点可用性结果属于链路内部数据，不单独出现在
运营概览的业务模块列表里，但和业务表共用同一份仓库与同一套事务。
"""
from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from threading import RLock
from typing import Any, Iterator

from app.seed import INTERNAL_SEED, SEED_ROWS

INTERNAL_MODULES = frozenset(INTERNAL_SEED)


class Store:
    def __init__(self) -> None:
        self._lock = RLock()
        self.reset()

    def module_names(self) -> list[str]:
        return sorted(name for name in self._tables if name not in INTERNAL_MODULES)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def find_by(self, module: str, **criteria: Any) -> dict[str, Any] | None:
        """按业务字段找一条记录，供回执去重、节点编码唯一性校验使用。"""
        for row in self.rows(module):
            if all(row.get(key) == value for key, value in criteria.items()):
                return row
        return None

    def next_id(self, module: str) -> int:
        return max((int(row.get("id", 0)) for row in self.rows(module)), default=0) + 1

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """把一次状态写入包成原子操作。

        节点记录、节点状态和节点可用性结果必须一起落库：中间任何一步抛错，
        这里整体回滚到写入前的快照，不会留下“节点还在候选里、可用性却已经下线”
        这类半写状态。快照用深拷贝，避免嵌套的复核清单被就地改掉。
        """
        with self._lock:
            snapshot = deepcopy(self._tables)
            try:
                yield
            except Exception:
                self._tables = snapshot
                raise

    def reset(self) -> None:
        """恢复示例数据，便于本地联调和自动化验收之间互不串数据。"""
        with self._lock:
            self._tables: dict[str, list[dict[str, Any]]] = {
                name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
            }
            for name, rows in INTERNAL_SEED.items():
                self._tables[name] = [dict(row) for row in rows]

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        return {"cards": cards, "modules": modules}


store = Store()
