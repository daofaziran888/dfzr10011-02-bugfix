"""节点状态链路的回归测试：下线错位、回执冲突、幂等与事务原子性。

运行：cd backend && python -m unittest discover -s tests -v
"""
from __future__ import annotations

import unittest

from app.services import node_state as state_module
from app.services.cluster import ClusterService
from app.services.shard import ShardService
from app.store import store


class NodeStateLinkTest(unittest.TestCase):
    def setUp(self) -> None:
        store.reset()
        self.cluster = ClusterService()
        self.shard = ShardService()

    def _detail(self, code: str = "CLUS-0002"):
        entry = self.cluster.find_entry_by_code(code)
        assert entry is not None
        return self.cluster.get_entry(int(entry["id"]))

    def _candidate_codes(self) -> list[str]:
        return [item["节点编号"] for item in self.shard.candidates()]

    def test_offline_receipt_aligns_detail_candidate_and_recheck(self) -> None:
        entry, message = self.cluster.record_receipt(
            2, {"event": "离线", "receipt_id": "OFF-1", "occurred_at": "2026-09-11"}
        )
        self.assertIsNotNone(entry, message)
        detail = self._detail()
        self.assertEqual(detail["节点状态"], "已离线")
        self.assertEqual(detail["availability"]["可用性状态"], "已离线")
        self.assertEqual(detail["availability"]["下线依据"], "OFF-1")
        self.assertNotIn("CLUS-0002", self._candidate_codes())
        shard = self.shard.get_entry(2)
        assert shard is not None
        self.assertFalse(shard["节点候选"])
        recheck = self.shard.recheck_page()
        self.assertIn("OFF-1", [item["receipt_id"] for item in recheck["recheck"]])
        self.assertIn("CLUS-0002", [item["节点编号"] for item in recheck["excluded"]])

    def test_recovery_first_keeps_candidate_with_cleanup_item(self) -> None:
        policy, message = self.cluster.set_policy("recovery_first")
        self.assertIsNotNone(policy, message)
        self.cluster.record_receipt(2, {"event": "恢复", "receipt_id": "REC-1"})
        self.cluster.record_receipt(2, {"event": "离线", "receipt_id": "OFF-1"})
        detail = self._detail()
        self.assertEqual(detail["节点状态"], "运行中")
        self.assertTrue(detail["availability"]["candidate"])
        self.assertIn("CLUS-0002", self._candidate_codes())
        kinds = [item["类型"] for item in detail["availability"]["复核清单"]]
        self.assertIn("离线待清理", kinds)

    def test_offline_first_removes_candidate_with_restore_item(self) -> None:
        self.cluster.set_policy("offline_first")
        self.cluster.record_receipt(2, {"event": "恢复", "receipt_id": "REC-1"})
        self.cluster.record_receipt(2, {"event": "离线", "receipt_id": "OFF-1"})
        detail = self._detail()
        self.assertEqual(detail["节点状态"], "已离线")
        self.assertFalse(detail["availability"]["candidate"])
        self.assertNotIn("CLUS-0002", self._candidate_codes())
        kinds = [item["类型"] for item in detail["availability"]["复核清单"]]
        self.assertIn("恢复待办", kinds)

    def test_receipts_trace_both_side(self) -> None:
        self.cluster.record_receipt(2, {"event": "恢复", "receipt_id": "REC-1"})
        self.cluster.record_receipt(2, {"event": "离线", "receipt_id": "OFF-1"})
        detail = self._detail()
        self.assertIn("REC-1", detail["availability"]["依据回执"])
        self.assertIn("OFF-1", detail["availability"]["依据回执"])
        self.assertEqual(detail["availability"]["恢复依据"], "REC-1")
        self.assertEqual(detail["availability"]["下线依据"], "OFF-1")

    def test_duplicate_writeback_is_idempotent(self) -> None:
        first, _ = self.cluster.record_receipt(2, {"event": "离线", "receipt_id": "OFF-1"})
        assert first is not None
        version = first["版本"]
        nodes_before = len(store.rows("cluster"))
        again, _ = self.cluster.record_receipt(2, {"event": "离线", "receipt_id": "OFF-1"})
        assert again is not None
        self.assertEqual(again["版本"], version)
        self.assertEqual(len(store.rows("cluster")), nodes_before)
        self.assertEqual(
            len([r for r in store.rows("node_availability") if r["节点编号"] == "CLUS-0002"]), 1
        )
        writeback, missing = self.cluster.create_entry(
            {"节点编号": "CLUS-0002", "节点地址": "10.0.0.2", "节点角色": "采集"}
        )
        self.assertEqual(missing, [])
        assert writeback is not None
        self.assertEqual(len(store.rows("cluster")), nodes_before)
        self.assertEqual(writeback["版本"], version)
        self.assertEqual(writeback["节点状态"], "已离线")

    def test_refresh_keeps_persisted_version(self) -> None:
        self.cluster.record_receipt(2, {"event": "恢复", "receipt_id": "REC-1"})
        self.cluster.record_receipt(2, {"event": "离线", "receipt_id": "OFF-1"})
        first = self._detail()
        second = self._detail()
        self.assertEqual(first["版本"], second["版本"])
        self.assertEqual(first["availability"], second["availability"])
        self.assertEqual(
            [item["receipt_id"] for item in first["生命周期记录"]],
            [item["receipt_id"] for item in second["生命周期记录"]],
        )

    def test_half_failure_rolls_back_node_and_availability(self) -> None:
        before = self._detail()
        original = state_module.node_state.sync_availability

        def boom(node):  # noqa: ANN001
            raise RuntimeError("模拟可用性结果写入失败")

        state_module.node_state.sync_availability = boom
        try:
            with self.assertRaises(RuntimeError):
                self.cluster.record_receipt(2, {"event": "离线", "receipt_id": "OFF-FAIL"})
        finally:
            state_module.node_state.sync_availability = original
        after = self._detail()
        self.assertEqual(after["节点状态"], before["节点状态"])
        self.assertEqual(after["availability"], before["availability"])
        self.assertEqual(after["版本"], before["版本"])
        self.assertNotIn(
            "OFF-FAIL", [item["receipt_id"] for item in store.rows("node_lifecycle")]
        )
        self.assertIn("CLUS-0002", self._candidate_codes())

    def test_offline_node_is_rejected_by_shard_assignment(self) -> None:
        self.cluster.run_action(2, "下线节点", receipt_id="ACT-OFF-1")
        entry, message = self.shard.run_action(2, "分配节点")
        self.assertIsNone(entry)
        self.assertIn("候选范围", message)


if __name__ == "__main__":
    unittest.main()
