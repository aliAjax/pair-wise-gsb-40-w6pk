import json
import sys
import tempfile
import threading
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import DomainError, MaritimeSARService  # noqa: E402


class DuplicateMergeFlowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = MaritimeSARService(Path(self.tmp.name) / "test.db")
        self.main = self.service.create_incident(
            "coord1", "coordinator", "SAR-100", "海燕号", 31.0, 122.0, 10.0, 3, "东海中心"
        )
        self.asset = self.service.add_asset(
            "coord1", "coordinator", "海巡01", "vessel", ["surface", "night"], 31.0, 122.0, 20, 100, 5
        )
        self.dup = self.service.create_incident(
            "op1", "operator", "SAR-101", "海燕号", 31.02, 122.02, 6.0, 3, "东海中心"
        )
        self.assertEqual("duplicate", self.dup["status"])
        self.assertEqual(self.main["id"], self.dup["duplicate_of"])

    def tearDown(self):
        self.tmp.cleanup()

    def _state_rows(self, table, **filters):
        rows = [r for r in self.service.state()[table] if all(r[k] == v for k, v in filters.items())]
        return rows

    def _build_dup_contents(self, area_code="D-A1", clue_event="dup-clue-1"):
        area = self.service.create_search_area(
            "coord1", "coordinator", self.dup["id"], area_code, "surface", 31.03, 122.03, 5, 2
        )
        assigned = self.service.assign_area("coord1", "coordinator", area["id"], self.asset["id"])
        self.assertEqual("assigned", assigned["status"])
        clue = self.service.record_clue(
            "field1", "field", self.dup["id"], clue_event, 31.03, 122.03, 0.8, "visual", area["id"]
        )
        return area, clue

    def test_confirm_merge_moves_area_clue_and_resource_with_traceability(self):
        area, clue = self._build_dup_contents()
        result = self.service.confirm_duplicate_merge(
            "coord1", "coordinator", self.dup["id"], client_batch_id="merge-1",
            note="同船同位置，值班员确认重复",
        )
        self.assertEqual("merged", result["status"])
        self.assertIn(self.asset["id"], result["summary"]["assets_following"])

        moved_area = self._state_rows("search_areas", id=area["id"])[0]
        moved_clue = self._state_rows("clues", id=clue["id"])[0]
        self.assertEqual(self.main["id"], moved_area["incident_id"])
        self.assertEqual(self.dup["id"], moved_area["origin_incident_id"])
        self.assertEqual(self.main["id"], moved_clue["incident_id"])
        self.assertEqual(self.dup["id"], moved_clue["origin_incident_id"])
        self.assertEqual(area["id"], moved_clue["area_id"])
        # 分配随区域一起并入：资源仍挂在已并入主事件的区域上
        self.assertEqual(self.asset["id"], moved_area["assigned_asset_id"])
        self.assertEqual("assigned", self.service.list_assets()[0]["status"])
        # 原报警记录仍可追溯
        dup_row = self._state_rows("incidents", id=self.dup["id"])[0]
        self.assertEqual("merged", dup_row["status"])
        self.assertEqual("SAR-101", dup_row["code"])
        self.assertEqual("merge-1", dup_row["merge_batch_id"])
        actions = [e["action"] for e in self.service.incident_timeline(self.main["id"])]
        self.assertIn("area.merged", actions)
        self.assertIn("clue.merged", actions)

        # 同批次再次提交：幂等返回，内容不重复
        again = self.service.confirm_duplicate_merge(
            "coord1", "coordinator", self.dup["id"], client_batch_id="merge-1", note="重复点击"
        )
        self.assertTrue(again["idempotent"])
        self.assertEqual(1, len(self._state_rows("search_areas", id=area["id"])))
        self.assertEqual(1, len(self._state_rows("clues", id=clue["id"])))

    def test_two_operators_confirm_concurrently_only_one_succeeds(self):
        self._build_dup_contents(area_code="D-A2", clue_event="dup-clue-2")
        barrier = threading.Barrier(2)
        outcomes = []

        def confirm(batch_code, user):
            try:
                barrier.wait(timeout=10)
                self.service.confirm_duplicate_merge(
                    user, "operator", self.dup["id"], client_batch_id=batch_code, note="并发确认"
                )
                outcomes.append(("ok", batch_code, user))
            except DomainError as exc:
                outcomes.append(("err", batch_code, exc.status, str(exc)))

        t1 = threading.Thread(target=confirm, args=("merge-c1", "op1"))
        t2 = threading.Thread(target=confirm, args=("merge-c2", "op2"))
        t1.start(); t2.start(); t1.join(); t2.join()

        ok = [o for o in outcomes if o[0] == "ok"]
        err = [o for o in outcomes if o[0] == "err"]
        self.assertEqual(1, len(ok), outcomes)
        self.assertEqual(1, len(err))
        self.assertEqual(409, err[0][2])
        dup_row = self._state_rows("incidents", id=self.dup["id"])[0]
        self.assertEqual("merged", dup_row["status"])
        self.assertEqual(ok[0][1], dup_row["merge_batch_id"])
        area = self._state_rows("search_areas", code="D-A2")[0]
        self.assertEqual(self.main["id"], area["incident_id"])

    def test_failed_merge_retries_same_batch_without_duplicating(self):
        area, clue = self._build_dup_contents(area_code="D-A3", clue_event="dup-clue-3")
        third = self.service.create_incident(
            "coord1", "coordinator", "SAR-200", "北斗号", 35.0, 125.0, 8.0, 3, "南海中心"
        )
        # 模拟区域先被挂到另一起事件：线索因此不能并入，整批失败
        with self.service.connect() as conn:
            conn.execute("UPDATE search_areas SET incident_id=? WHERE id=?", (third["id"], area["id"]))

        first = self.service.confirm_duplicate_merge(
            "coord1", "coordinator", self.dup["id"], client_batch_id="merge-retry", note="首次尝试"
        )
        self.assertEqual("failed", first["status"])
        self.assertEqual([], [i for i in first["summary"]["items"] if i["status"] == "merged"])
        self.assertEqual("duplicate", self._state_rows("incidents", id=self.dup["id"])[0]["status"])
        # 线索仍留在重复报警下，未被错误并走
        self.assertEqual(self.dup["id"], self._state_rows("clues", id=clue["id"])[0]["incident_id"])

        # 排除故障：区域回到重复报警；按同一批次重试
        with self.service.connect() as conn:
            conn.execute("UPDATE search_areas SET incident_id=? WHERE id=?", (self.dup["id"], area["id"]))
        second = self.service.confirm_duplicate_merge(
            "coord1", "coordinator", self.dup["id"], client_batch_id="merge-retry", note="重试"
        )
        self.assertEqual("merged", second["status"])

        with self.service.connect() as conn:
            item_count = conn.execute(
                "SELECT COUNT(*) AS c FROM merge_items mi JOIN merge_batches mb ON mi.batch_id=mb.id "
                "WHERE mb.client_batch_id='merge-retry'"
            ).fetchone()["c"]
        # 区域、线索各只有一条批次明细，已并入的内容没有重复
        self.assertEqual(2, item_count)
        self.assertEqual(self.main["id"], self._state_rows("search_areas", id=area["id"])[0]["incident_id"])
        self.assertEqual(self.main["id"], self._state_rows("clues", id=clue["id"])[0]["incident_id"])

    def test_revert_restores_area_and_clue_and_is_idempotent(self):
        area, clue = self._build_dup_contents(area_code="D-A4", clue_event="dup-clue-4")
        self.service.confirm_duplicate_merge(
            "coord1", "coordinator", self.dup["id"], client_batch_id="merge-r1", note="确认"
        )
        reverted = self.service.revert_duplicate_merge(
            "coord2", "coordinator", "merge-r1", "误判为重复"
        )
        self.assertEqual("reverted", reverted["status"])
        self.assertEqual("duplicate", reverted["summary"]["restored_status"])

        dup_row = self._state_rows("incidents", id=self.dup["id"])[0]
        self.assertEqual("duplicate", dup_row["status"])
        self.assertEqual(self.main["id"], dup_row["duplicate_of"])
        self.assertEqual(self.dup["id"], self._state_rows("search_areas", id=area["id"])[0]["incident_id"])
        self.assertEqual(self.dup["id"], self._state_rows("clues", id=clue["id"])[0]["incident_id"])
        self.assertEqual(area["id"], self._state_rows("clues", id=clue["id"])[0]["area_id"])
        # 资源分配随区域一起放回，不被拆坏
        restored_area = self._state_rows("search_areas", id=area["id"])[0]
        self.assertEqual("assigned", restored_area["status"])
        self.assertEqual(self.asset["id"], restored_area["assigned_asset_id"])
        self.assertEqual("assigned", self.service.list_assets()[0]["status"])

        again = self.service.revert_duplicate_merge("coord2", "coordinator", "merge-r1", "再点一次")
        self.assertTrue(again["idempotent"])
        # 撤销后可用新批次重新确认
        remerge = self.service.confirm_duplicate_merge(
            "coord2", "coordinator", self.dup["id"], client_batch_id="merge-r1b", note="重新确认"
        )
        self.assertEqual("merged", remerge["status"])

    def test_revert_reopens_closed_main_incident_first(self):
        area, clue = self._build_dup_contents(area_code="D-A5", clue_event="dup-clue-5")
        self.service.confirm_duplicate_merge(
            "coord1", "coordinator", self.dup["id"], client_batch_id="merge-r2", note="确认"
        )
        # 主事件随后被结束
        self.service.complete_area("coord1", "coordinator", area["id"], "completed")
        current = self._state_rows("incidents", id=self.main["id"])[0]
        self.service.close_incident("coord1", "coordinator", self.main["id"], "resolved", current["version"])
        self.assertEqual("closed", self._state_rows("incidents", id=self.main["id"])[0]["status"])

        reverted = self.service.revert_duplicate_merge(
            "coord1", "coordinator", "merge-r2", "报警实为两船"
        )
        self.assertTrue(reverted["summary"]["main_reopened"])
        self.assertEqual("reported", self._state_rows("incidents", id=self.main["id"])[0]["status"])
        self.assertEqual("duplicate", self._state_rows("incidents", id=self.dup["id"])[0]["status"])
        self.assertEqual(self.dup["id"], self._state_rows("search_areas", id=area["id"])[0]["incident_id"])
        self.assertEqual(self.dup["id"], self._state_rows("clues", id=clue["id"])[0]["incident_id"])

    def test_revert_does_not_break_other_merged_alarm(self):
        area1, clue1 = self._build_dup_contents(area_code="D-A6", clue_event="dup-clue-6")
        other_dup = self.service.create_incident(
            "op1", "operator", "SAR-102", "海燕号", 31.04, 122.04, 6.0, 3, "东海中心"
        )
        area2 = self.service.create_search_area(
            "coord1", "coordinator", other_dup["id"], "D-A7", "surface", 31.05, 122.05, 5, 3
        )
        clue2 = self.service.record_clue(
            "field1", "field", other_dup["id"], "dup-clue-7", 31.05, 122.05, 0.6, "radio", area2["id"]
        )
        self.service.confirm_duplicate_merge(
            "coord1", "coordinator", self.dup["id"], client_batch_id="merge-b1", note="报警一"
        )
        self.service.confirm_duplicate_merge(
            "coord1", "coordinator", other_dup["id"], client_batch_id="merge-b2", note="报警二"
        )

        # 撤销第一起合并：第二起已并入的区域和线索保持不动
        self.service.revert_duplicate_merge("coord1", "coordinator", "merge-b1", "撤销报警一")
        self.assertEqual(self.dup["id"], self._state_rows("search_areas", id=area1["id"])[0]["incident_id"])
        self.assertEqual(self.dup["id"], self._state_rows("clues", id=clue1["id"])[0]["incident_id"])
        self.assertEqual("duplicate", self._state_rows("incidents", id=self.dup["id"])[0]["status"])

        self.assertEqual(self.main["id"], self._state_rows("search_areas", id=area2["id"])[0]["incident_id"])
        self.assertEqual(self.main["id"], self._state_rows("clues", id=clue2["id"])[0]["incident_id"])
        self.assertEqual("merged", self._state_rows("incidents", id=other_dup["id"])[0]["status"])

    def test_verified_clue_keeps_its_review_after_merge_and_permission(self):
        area, clue = self._build_dup_contents(area_code="D-A8", clue_event="dup-clue-8")
        self.service.verify_clue("analyst1", "analyst", clue["id"], "verified")
        result = self.service.confirm_duplicate_merge(
            "coord1", "coordinator", self.dup["id"], client_batch_id="merge-v", note="确认"
        )
        self.assertEqual("merged", result["status"])
        self.assertEqual("verified", self._state_rows("clues", id=clue["id"])[0]["status"])
        with self.assertRaises(DomainError) as ctx:
            self.service.confirm_duplicate_merge(
                "viewer1", "viewer", self.dup["id"], client_batch_id="merge-x", note="无权"
            )
        self.assertEqual(403, ctx.exception.status)
        with self.assertRaises(DomainError):
            self.service.revert_duplicate_merge("coord1", "coordinator", "missing-batch", "x")


if __name__ == "__main__":
    unittest.main()
