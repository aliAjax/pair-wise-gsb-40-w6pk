import sys
import tempfile
import threading
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import DomainError, MaritimeSARService  # noqa: E402


class MaritimeSARFlowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = MaritimeSARService(Path(self.tmp.name) / "test.db")
        self.incident = self.service.create_incident(
            "coord1", "coordinator", "SAR-001", "海燕号", 31.0, 122.0, 10.0, 3, "东海中心"
        )
        self.asset = self.service.add_asset(
            "coord1", "coordinator", "海巡01", "vessel", ["surface", "night"], 31.0, 122.0, 20, 100, 5
        )

    def tearDown(self):
        self.tmp.cleanup()

    def test_complete_assignment_clue_offline_and_close_flow(self):
        area = self.service.create_search_area(
            "coord1", "coordinator", self.incident["id"], "A-01", "surface", 31.1, 122.1, 8, 1
        )
        assigned = self.service.assign_area("coord1", "coordinator", area["id"], self.asset["id"], self.asset["version"])
        self.assertEqual("assigned", assigned["status"])
        clue = self.service.record_clue(
            "field1", "field", self.incident["id"], "evt-1", 31.1, 122.1, 0.9, "visual", area["id"]
        )
        self.assertEqual("verified", self.service.verify_clue("analyst1", "analyst", clue["id"], "verified")["status"])
        batch = self.service.merge_offline_batch(
            "field1", "field", "batch-1",
            [{"type": "clue", "client_event_id": "off-1", "incident_id": self.incident["id"],
              "latitude": 31.11, "longitude": 122.11, "confidence": 0.7, "source": "radio"}],
        )
        self.assertEqual(1, batch["summary"]["accepted"])
        self.assertTrue(self.service.merge_offline_batch("field1", "field", "batch-1", [])["idempotent"])
        updated_asset = self.service.list_assets()[0]
        self.service.withdraw_asset("coord1", "coordinator", self.asset["id"], "任务移交", updated_asset["version"])
        current_area = self.service.state()["search_areas"][0]
        self.service.complete_area("coord1", "coordinator", area["id"], "abandoned", current_area["version"])
        current_incident = [x for x in self.service.state()["incidents"] if x["id"] == self.incident["id"]][0]
        closed = self.service.close_incident("coord1", "coordinator", self.incident["id"], "resolved", current_incident["version"])
        self.assertEqual("closed", closed["status"])
        self.assertGreaterEqual(len(self.service.incident_timeline(self.incident["id"])), 6)

    def test_duplicate_alarm_and_invalid_position_are_controlled(self):
        duplicate = self.service.create_incident(
            "op1", "operator", "SAR-002", "海燕号", 31.01, 122.01, 5.0, 3, "东海中心"
        )
        self.assertEqual("duplicate", duplicate["status"])
        self.assertEqual(self.incident["id"], duplicate["duplicate_of"])
        invalid = self.service.record_clue(
            "field1", "field", self.incident["id"], "evt-far", 45.0, 130.0, 0.8, "radio"
        )
        self.assertEqual("invalid", invalid["status"])
        with self.assertRaises(DomainError):
            self.service.verify_clue("field1", "field", invalid["id"], "verified")

    def test_assignment_conflict_and_permission(self):
        area = self.service.create_search_area(
            "coord1", "coordinator", self.incident["id"], "A-02", "surface", 31.1, 122.1, 5
        )
        self.service.assign_area("coord1", "coordinator", area["id"], self.asset["id"], self.asset["version"])
        area2 = self.service.create_search_area(
            "coord1", "coordinator", self.incident["id"], "A-03", "surface", 31.2, 122.2, 5
        )
        with self.assertRaises(DomainError) as ctx:
            self.service.assign_area("coord1", "coordinator", area2["id"], self.asset["id"], self.asset["version"])
        self.assertEqual(409, ctx.exception.status)
        with self.assertRaises(DomainError) as ctx2:
            self.service.create_search_area("field1", "field", self.incident["id"], "A-04", "surface", 31, 122, 5)
        self.assertEqual(403, ctx2.exception.status)


class DuplicateMergeFlowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = MaritimeSARService(Path(self.tmp.name) / "test.db")
        self.primary = self.service.create_incident(
            "coord1", "coordinator", "SAR-100", "海燕号", 31.0, 122.0, 10.0, 3, "东海中心"
        )
        self.asset = self.service.add_asset(
            "coord1", "coordinator", "海巡01", "vessel", ["surface", "night"], 31.0, 122.0, 20, 100, 5
        )
        self.dup = self.service.create_incident(
            "op1", "operator", "SAR-101", "海燕号", 31.05, 122.05, 5.0, 3, "东海中心"
        )
        self.assertEqual("duplicate", self.dup["status"])
        # 待确认的重复报警上已经核查出区域和线索
        self.area = self.service.create_search_area(
            "coord1", "coordinator", self.dup["id"], "DUP-A1", "surface", 31.06, 122.06, 6, 2
        )
        self.clue = self.service.record_clue(
            "field1", "field", self.dup["id"], "dup-clue-1", 31.06, 122.06, 0.8, "visual", self.area["id"]
        )
        self.assigned = self.service.assign_area(
            "coord1", "coordinator", self.area["id"], self.asset["id"]
        )

    def tearDown(self):
        self.tmp.cleanup()

    def _state_of(self, incident_id):
        state = self.service.state()
        return (
            [a for a in state["search_areas"] if a["incident_id"] == incident_id],
            [c for c in state["clues"] if c["incident_id"] == incident_id],
            [b for b in state["merge_batches"]],
        )

    def test_confirm_merge_moves_areas_clues_and_keeps_code_traceable(self):
        result = self.service.confirm_duplicate_merge(
            "coord1", "coordinator", "merge-1", self.dup["id"], self.primary["id"]
        )
        self.assertFalse(result["idempotent"])
        self.assertEqual(1, result["summary"]["area_count"])
        self.assertEqual(1, result["summary"]["clue_count"])
        dup_areas, dup_clues, batches = self._state_of(self.dup["id"])
        primary_areas, primary_clues, _ = self._state_of(self.primary["id"])
        self.assertEqual([], dup_areas)
        self.assertEqual([], dup_clues)
        self.assertEqual(["DUP-A1"], [a["code"] for a in primary_areas])
        self.assertEqual([self.clue["id"]], [c["id"] for c in primary_clues])
        # 原编号仍可追溯：重复报警记录保留，只是状态转为 merged
        with self.service.connect() as conn:
            row = conn.execute("SELECT * FROM incidents WHERE id=?", (self.dup["id"],)).fetchone()
        self.assertEqual("merged", row["status"])
        self.assertEqual("SAR-101", row["code"])
        self.assertEqual(self.primary["id"], row["duplicate_of"])
        self.assertEqual("merged", batches[0]["status"])
        # 资源分配随区域一起并入，不被拆坏
        self.assertEqual(self.asset["id"], primary_areas[0]["assigned_asset_id"])

    def test_concurrent_confirmations_only_one_succeeds(self):
        outcomes: list[dict[str, object]] = []
        barrier = threading.Barrier(2)

        def confirm(actor: str, batch_id: str) -> None:
            barrier.wait()
            try:
                self.service.confirm_duplicate_merge(
                    actor, "coordinator", batch_id, self.dup["id"], self.primary["id"]
                )
                outcomes.append({"ok": True, "actor": actor})
            except DomainError as exc:
                outcomes.append({"ok": False, "actor": actor, "status": exc.status})

        t1 = threading.Thread(target=confirm, args=("coord1", "merge-A"))
        t2 = threading.Thread(target=confirm, args=("coord2", "merge-B"))
        t1.start(); t2.start(); t1.join(); t2.join()
        self.assertEqual(1, sum(1 for o in outcomes if o["ok"]))
        loser = next(o for o in outcomes if not o["ok"])
        self.assertEqual(409, loser["status"])
        # 没有重复并入：主事件名下仍只有一个区域、一条线索
        primary_areas, primary_clues, batches = self._state_of(self.primary["id"])
        self.assertEqual(1, len(primary_areas))
        self.assertEqual(1, len(primary_clues))
        self.assertEqual(1, len([b for b in batches if b["status"] == "merged"]))

    def test_same_batch_retry_is_idempotent_and_never_doubles(self):
        first = self.service.confirm_duplicate_merge(
            "coord1", "coordinator", "merge-retry", self.dup["id"], self.primary["id"]
        )
        self.assertFalse(first["idempotent"])
        retry = self.service.confirm_duplicate_merge(
            "coord2", "coordinator", "merge-retry", self.dup["id"], self.primary["id"]
        )
        self.assertTrue(retry["idempotent"])
        primary_areas, primary_clues, batches = self._state_of(self.primary["id"])
        self.assertEqual(1, len(primary_areas))
        self.assertEqual(1, len(primary_clues))
        self.assertEqual(1, len(batches))
        # 不同批次重复确认同一报警应被拒绝
        with self.assertRaises(DomainError) as ctx:
            self.service.confirm_duplicate_merge(
                "coord2", "coordinator", "merge-other", self.dup["id"], self.primary["id"]
            )
        self.assertEqual(409, ctx.exception.status)

    def test_revert_returns_only_contents_still_on_primary(self):
        self.service.confirm_duplicate_merge(
            "coord1", "coordinator", "merge-r", self.dup["id"], self.primary["id"]
        )
        # 主事件在合并后又新增了自己的内容，撤销不能拆走
        own_area = self.service.create_search_area(
            "coord1", "coordinator", self.primary["id"], "OWN-A", "surface", 31.2, 122.2, 5, 3
        )
        own_clue = self.service.record_clue(
            "field1", "field", self.primary["id"], "own-clue", 31.2, 122.2, 0.5, "radio"
        )
        result = self.service.revert_duplicate_merge(
            "coord2", "coordinator", "merge-r", "报警来源不同，解除合并"
        )
        self.assertEqual(1, result["summary"]["restored_area_count"])
        self.assertEqual(1, result["summary"]["restored_clue_count"])
        dup_areas, dup_clues, batches = self._state_of(self.dup["id"])
        primary_areas, primary_clues, _ = self._state_of(self.primary["id"])
        self.assertEqual(["DUP-A1"], [a["code"] for a in dup_areas])
        self.assertEqual([self.clue["id"]], [c["id"] for c in dup_clues])
        self.assertEqual(["OWN-A"], [a["code"] for a in primary_areas])
        self.assertEqual([own_clue["id"]], [c["id"] for c in primary_clues])
        self.assertEqual(own_area["id"], primary_areas[0]["id"])
        dup_row = [i for i in self.service.state()["incidents"] if i["id"] == self.dup["id"]][0]
        self.assertEqual("duplicate", dup_row["status"])
        self.assertEqual("reverted", batches[0]["status"])
        # 分配关系跟随区域放回
        self.assertEqual(self.asset["id"], dup_areas[0]["assigned_asset_id"])
        # 已撤销的批次不能重放
        with self.assertRaises(DomainError) as ctx:
            self.service.confirm_duplicate_merge(
                "coord1", "coordinator", "merge-r", self.dup["id"], self.primary["id"]
            )
        self.assertEqual(409, ctx.exception.status)
        # 撤销需要原因，且不能重复撤销
        with self.assertRaises(DomainError):
            self.service.revert_duplicate_merge("coord1", "coordinator", "merge-r", "   ")
        with self.assertRaises(DomainError) as ctx2:
            self.service.revert_duplicate_merge("coord1", "coordinator", "merge-r", "再撤一次")
        self.assertEqual(409, ctx2.exception.status)

    def test_revert_reopens_closed_primary_as_pending(self):
        self.service.confirm_duplicate_merge(
            "coord1", "coordinator", "merge-c", self.dup["id"], self.primary["id"]
        )
        # 并入的区域已完成，主事件随后关闭
        merged_area = [a for a in self.service.state()["search_areas"]
                       if a["incident_id"] == self.primary["id"]][0]
        self.service.complete_area(
            "coord1", "coordinator", merged_area["id"], "completed", merged_area["version"]
        )
        current = [i for i in self.service.state()["incidents"] if i["id"] == self.primary["id"]][0]
        self.service.close_incident("coord1", "coordinator", self.primary["id"], "resolved", current["version"])
        result = self.service.revert_duplicate_merge(
            "coord1", "coordinator", "merge-c", "误判为重复"
        )
        self.assertTrue(result["summary"]["primary_reopened"])
        primary = [i for i in self.service.state()["incidents"] if i["id"] == self.primary["id"]][0]
        self.assertEqual("reported", primary["status"])

    def test_revert_skips_area_that_left_the_primary(self):
        self.service.confirm_duplicate_merge(
            "coord1", "coordinator", "merge-s", self.dup["id"], self.primary["id"]
        )
        # 模拟并入的区域后来被改挂到别的事件（不再属于主事件）
        with self.service.connect() as conn:
            other = conn.execute(
                "INSERT INTO incidents(code,vessel_name,latitude,longitude,uncertainty_km,sea_state,status,lead_org,created_by,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                ("SAR-900", "其他船", 30.0, 121.0, 5.0, 2, "coordinating", "南海中心", "coord1", "2026-01-01T00:00:00+00:00", "2026-01-01T00:00:00+00:00"),
            )
            other_id = other.lastrowid
            conn.execute("UPDATE search_areas SET incident_id=? WHERE id=?", (other_id, self.area["id"]))
        result = self.service.revert_duplicate_merge(
            "coord1", "coordinator", "merge-s", "区域已转其他事件"
        )
        self.assertEqual([], result["summary"]["restored_area_ids"])
        self.assertEqual(1, result["summary"]["restored_clue_count"])
        # 线索回到重复报警，但不再跨事件引用已属于其他事件的区域
        clue = [c for c in self.service.state()["clues"] if c["id"] == self.clue["id"]][0]
        self.assertEqual(self.dup["id"], clue["incident_id"])
        self.assertIsNone(clue["area_id"])
        area = [a for a in self.service.state()["search_areas"] if a["id"] == self.area["id"]][0]
        self.assertNotIn(area["incident_id"], (self.dup["id"], self.primary["id"]))

    def test_merge_validation_and_permission(self):
        # 只有协调员可以确认合并
        with self.assertRaises(DomainError) as ctx:
            self.service.confirm_duplicate_merge(
                "op1", "operator", "merge-x", self.dup["id"], self.primary["id"]
            )
        self.assertEqual(403, ctx.exception.status)
        # 主事件与重复归属不一致
        with self.assertRaises(DomainError):
            self.service.confirm_duplicate_merge(
                "coord1", "coordinator", "merge-x", self.dup["id"], self.dup["id"] + 999
            )
        # 合并后的重复报警冻结，不能再建区域或录线索
        self.service.confirm_duplicate_merge(
            "coord1", "coordinator", "merge-f", self.dup["id"], self.primary["id"]
        )
        with self.assertRaises(DomainError) as ctx_area:
            self.service.create_search_area(
                "coord1", "coordinator", self.dup["id"], "DUP-A2", "surface", 31.0, 122.0, 5
            )
        self.assertEqual(409, ctx_area.exception.status)
        with self.assertRaises(DomainError) as ctx_clue:
            self.service.record_clue(
                "field1", "field", self.dup["id"], "dup-clue-2", 31.0, 122.0, 0.5, "radio"
            )
        self.assertEqual(409, ctx_clue.exception.status)


if __name__ == "__main__":
    unittest.main()
