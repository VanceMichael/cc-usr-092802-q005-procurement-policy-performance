"""政策目标履约核算：守恒、幂等、权限、规则生效、更正、恢复与追溯测试。"""

from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path

from civicflow.application import CivicFlow
from civicflow.errors import ConflictError, PermissionDenied, ValidationError
from civicflow.security import AccessContext
from civicflow.targets import account_key_of


NOW = "2026-03-01T10:00:00+08:00"


def clerk(unit: str, actor: str = "clerk") -> AccessContext:
    return AccessContext(
        actor_id=actor,
        permissions=frozenset({"write:fulfillment", "read:fulfillment", "read:targets", "write:targets", "request:exceptions", "read:stage_results"}),
        scopes=frozenset({f"unit:{unit}"}),
    )


class PolicyTargetTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "test.sqlite3"
        self.app = CivicFlow.open(self.db, fixed_now=NOW)
        self.finance = AccessContext.system("finance")

    def tearDown(self):
        self.temp.cleanup()

    def reopen(self, now: str) -> CivicFlow:
        return CivicFlow.open(self.db, fixed_now=now)

    # ------------------------------------------------------------------
    # 工具
    # ------------------------------------------------------------------
    def establish_and_issue(self, app=None, *, year=2026, program="sme", category="goods", total="1000.00", issues=()):
        app = app or self.app
        app.targets.establish(self.finance, fiscal_year=year, program=program, category=category, amount=total, reason="年度总目标", request_key=f"establish-{year}-{program}-{category}-{total}")
        for unit, new_total in issues:
            app.targets.issue(self.finance, fiscal_year=year, program=program, category=category, unit_id=unit, new_total=new_total, reason="下达", request_key=f"issue-{year}-{program}-{category}-{unit}-{new_total}")

    def balance(self, app, year, program, unit, category):
        return app.targets.get_account(self.finance, account_key_of(year, program, unit, category))["balance_minor"]

    def total_balance(self, app) -> int:
        return sum(row["balance_minor"] for row in app.targets.list_accounts(self.finance))

    # ------------------------------------------------------------------
    # 目标下达、换版与守恒
    # ------------------------------------------------------------------
    def test_issue_and_reversion_conserve_total(self):
        self.establish_and_issue(total="1000.00", issues=(("org:a", "600.00"),))
        self.assertEqual(self.balance(self.app, 2026, "sme", "org:a", "goods"), 60000)
        self.assertEqual(self.balance(self.app, 2026, "sme", "pool", "goods"), 40000)
        # 换版：单位目标下调到 500，差额回到总目标池
        self.app.targets.issue(self.finance, fiscal_year=2026, program="sme", category="goods", unit_id="org:a", new_total="500.00", reason="年中换版", request_key="reissue-1")
        self.assertEqual(self.balance(self.app, 2026, "sme", "org:a", "goods"), 50000)
        self.assertEqual(self.balance(self.app, 2026, "sme", "pool", "goods"), 50000)
        history = self.app.targets.target_history(self.finance, account_key_of(2026, "sme", "org:a", "goods"))
        self.assertEqual([row["amount_minor"] for row in history], [60000, 50000])
        self.assertEqual(self.total_balance(self.app), 100000)

    def test_issue_cannot_overdraw_pool(self):
        self.establish_and_issue(total="1000.00", issues=(("org:a", "600.00"),))
        with self.assertRaises(ConflictError):
            self.app.targets.issue(self.finance, fiscal_year=2026, program="sme", category="goods", unit_id="org:b", new_total="500.00", reason="超发", request_key="issue-overdraw")
        self.assertEqual(self.total_balance(self.app), 100000)

    def test_issue_is_idempotent(self):
        self.establish_and_issue(total="1000.00")
        first = self.app.targets.issue(self.finance, fiscal_year=2026, program="sme", category="goods", unit_id="org:a", new_total="600.00", reason="下达", request_key="issue-same")
        second = self.app.targets.issue(self.finance, fiscal_year=2026, program="sme", category="goods", unit_id="org:a", new_total="600.00", reason="下达", request_key="issue-same")
        self.assertEqual(first["movement"]["movement_id"], second["movement"]["movement_id"])
        self.assertEqual(self.balance(self.app, 2026, "sme", "org:a", "goods"), 60000)

    # ------------------------------------------------------------------
    # 调剂：两阶段、守恒、过期
    # ------------------------------------------------------------------
    def test_transfer_two_phase_and_conservation(self):
        self.establish_and_issue(total="1000.00", issues=(("org:a", "600.00"), ("org:b", "100.00")))
        proposed = self.app.targets.propose_transfer(self.finance, fiscal_year=2026, program="sme", category="goods", from_unit="org:a", to_unit="org:b", amount="200.00", reason="年中调剂", confirm_by="2026-12-01T00:00:00+08:00", request_key="transfer-1")
        self.assertEqual(proposed["state"], "proposed")
        self.assertEqual(self.balance(self.app, 2026, "sme", "org:a", "goods"), 60000)  # 确认前不移动
        confirmed = self.app.targets.confirm_transfer(self.finance, proposed["transfer_id"], request_key="transfer-1-confirm")
        self.assertEqual(confirmed["state"], "confirmed")
        self.assertEqual(self.balance(self.app, 2026, "sme", "org:a", "goods"), 40000)
        self.assertEqual(self.balance(self.app, 2026, "sme", "org:b", "goods"), 30000)
        self.assertEqual(self.total_balance(self.app), 100000)
        with self.assertRaises(ConflictError):
            self.app.targets.confirm_transfer(self.finance, proposed["transfer_id"], request_key="transfer-1-again")

    def test_transfer_reject_and_expiry(self):
        self.establish_and_issue(total="1000.00", issues=(("org:a", "600.00"),))
        proposed = self.app.targets.propose_transfer(self.finance, fiscal_year=2026, program="sme", category="goods", from_unit="org:a", to_unit="org:b", amount="50.00", reason="调剂", confirm_by="2026-12-01T00:00:00+08:00", request_key="transfer-reject")
        rejected = self.app.targets.reject_transfer(self.finance, proposed["transfer_id"], reason="依据不足", request_key="transfer-reject-1")
        self.assertEqual(rejected["state"], "rejected")
        expiring = self.app.targets.propose_transfer(self.finance, fiscal_year=2026, program="sme", category="goods", from_unit="org:a", to_unit="org:b", amount="50.00", reason="调剂", confirm_by="2026-04-01T00:00:00+08:00", request_key="transfer-expire")
        later = self.reopen("2026-05-01T00:00:00+08:00")
        with self.assertRaises(ConflictError):
            later.targets.confirm_transfer(self.finance, expiring["transfer_id"], request_key="transfer-expire-confirm")
        summary = later.rectifications.recover(AccessContext.system("recovery"))
        self.assertIn(expiring["transfer_id"], summary["transfers_expired"])
        self.assertEqual(self.total_balance(later), 100000)

    def test_concurrent_release_never_overdraws(self):
        self.establish_and_issue(total="1000.00", issues=(("org:a", "100.00"),))
        barrier = threading.Barrier(5)
        outcomes = []

        def release(index):
            barrier.wait()
            try:
                self.app.targets.release(self.finance, fiscal_year=2026, program="sme", category="goods", unit_id="org:a", amount="30.00", reason="并发释放", request_key=f"release-{index}")
                outcomes.append("ok")
            except ConflictError:
                outcomes.append("conflict")

        threads = [threading.Thread(target=release, args=(index,)) for index in range(5)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(outcomes.count("ok"), 3)
        self.assertEqual(outcomes.count("conflict"), 2)
        self.assertEqual(self.balance(self.app, 2026, "sme", "org:a", "goods"), 1000)
        self.assertEqual(self.total_balance(self.app), 100000)

    # ------------------------------------------------------------------
    # 释放、结转与关账
    # ------------------------------------------------------------------
    def test_release_and_carry_over_conserve(self):
        self.establish_and_issue(total="1000.00", issues=(("org:a", "600.00"),))
        self.app.targets.release(self.finance, fiscal_year=2026, program="sme", category="goods", unit_id="org:a", amount="100.00", reason="调减", request_key="release-1")
        self.assertEqual(self.balance(self.app, 2026, "sme", "org:a", "goods"), 50000)
        self.assertEqual(self.balance(self.app, 2026, "sme", "pool", "goods"), 50000)
        self.app.targets.carry_over(self.finance, fiscal_year=2026, program="sme", category="goods", unit_id="org:a", amount="200.00", reason="提前结转", request_key="carry-1")
        self.assertEqual(self.balance(self.app, 2026, "sme", "org:a", "goods"), 30000)
        self.assertEqual(self.balance(self.app, 2027, "sme", "org:a", "goods"), 20000)
        self.assertEqual(self.total_balance(self.app), 100000)

    def test_close_fiscal_year_carries_remaining(self):
        self.establish_and_issue(year=2025, total="800.00", issues=(("org:a", "500.00"),))
        with self.assertRaises(ValidationError):
            self.app.targets.close_fiscal_year(self.finance, 2026, request_key="close-too-early")
        later = self.reopen("2026-02-01T00:00:00+08:00")
        closed = later.targets.close_fiscal_year(self.finance, 2025, request_key="close-2025")
        self.assertEqual(closed["closed_accounts"], 2)
        self.assertEqual(self.balance(later, 2026, "sme", "org:a", "goods"), 50000)
        self.assertEqual(self.balance(later, 2026, "sme", "pool", "goods"), 30000)
        self.assertEqual(later.targets.get_account(self.finance, account_key_of(2025, "sme", "org:a", "goods"))["state"], "closed")
        self.assertEqual(self.total_balance(later), 80000)
        again = later.targets.close_fiscal_year(self.finance, 2025, request_key="close-2025-bis")
        self.assertEqual(again["closed_accounts"], 0)

    # ------------------------------------------------------------------
    # 规则生效与计划承诺
    # ------------------------------------------------------------------
    def test_rules_apply_only_to_plans_after_effective(self):
        self.establish_and_issue(total="2000.00", issues=(("org:a", "1500.00"),))
        self.app.fulfillment.publish_rule(self.finance, program="sme", category="goods", version=1, min_ratio_bp=3000, catalog=["A01"], effective_from="2026-01-01T00:00:00+08:00", request_key="rule-v1")
        plan1 = self.app.fulfillment.record_plan(self.finance, fiscal_year=2026, program="sme", unit_id="org:a", category="goods", amount="200.00", catalog_code="A01", line_total="500.00", reference="plan-1", request_key="plan-1")
        self.app.fulfillment.publish_rule(self.finance, program="sme", category="goods", version=2, min_ratio_bp=5000, catalog=["B02"], effective_from="2026-06-01T00:00:00+08:00", request_key="rule-v2")
        # 新规则尚未生效：旧目录仍可用，仍适用 v1
        before = self.reopen("2026-03-15T00:00:00+08:00")
        plan2 = before.fulfillment.record_plan(self.finance, fiscal_year=2026, program="sme", unit_id="org:a", category="goods", amount="200.00", catalog_code="A01", line_total="500.00", reference="plan-2", request_key="plan-2")
        self.assertEqual(plan2["rule_id"], plan1["rule_id"])
        # 生效之后：旧目录被拒，比例不足被拒，满足新规则才入账
        after = self.reopen("2026-06-02T00:00:00+08:00")
        with self.assertRaises(ValidationError):
            after.fulfillment.record_plan(self.finance, fiscal_year=2026, program="sme", unit_id="org:a", category="goods", amount="200.00", catalog_code="A01", line_total="500.00", reference="plan-3", request_key="plan-3")
        with self.assertRaises(ValidationError):
            after.fulfillment.record_plan(self.finance, fiscal_year=2026, program="sme", unit_id="org:a", category="goods", amount="200.00", catalog_code="B02", line_total="500.00", reference="plan-4", request_key="plan-4")
        plan5 = after.fulfillment.record_plan(self.finance, fiscal_year=2026, program="sme", unit_id="org:a", category="goods", amount="250.00", catalog_code="B02", line_total="500.00", reference="plan-5", request_key="plan-5")
        self.assertNotEqual(plan5["rule_id"], plan1["rule_id"])
        events = after.fulfillment.replay(self.finance, account_key_of(2026, "sme", "org:a", "goods"))
        self.assertEqual([event["rule_id"] for event in events], [plan1["rule_id"], plan1["rule_id"], plan5["rule_id"]])

    def test_plan_commit_cannot_exceed_target(self):
        self.establish_and_issue(total="1000.00", issues=(("org:a", "100.00"),))
        with self.assertRaises(ConflictError):
            self.app.fulfillment.record_plan(self.finance, fiscal_year=2026, program="sme", unit_id="org:a", category="goods", amount="200.00", catalog_code="A01", line_total="500.00", reference="plan-x", request_key="plan-x")

    # ------------------------------------------------------------------
    # 执行依据：招标、流标、合同缩减、紧急采购
    # ------------------------------------------------------------------
    def test_pipeline_tender_reduce_and_emergency(self):
        self.establish_and_issue(total="1000.00", issues=(("org:a", "600.00"),))
        account = account_key_of(2026, "sme", "org:a", "goods")
        self.app.fulfillment.record_plan(self.finance, fiscal_year=2026, program="sme", unit_id="org:a", category="goods", amount="200.00", catalog_code="A01", line_total="400.00", reference="plan-1", request_key="plan-1")
        # 流标：承诺释放
        self.app.fulfillment.record_tender(self.finance, fiscal_year=2026, program="sme", unit_id="org:a", category="goods", outcome="failed", amount="200.00", reference="tender-1", request_key="tender-1")
        self.assertEqual(self.app.fulfillment.pipeline(self.finance, account)["committed"], 0)
        # 中标：承诺转为合同
        self.app.fulfillment.record_plan(self.finance, fiscal_year=2026, program="sme", unit_id="org:a", category="goods", amount="300.00", catalog_code="A01", line_total="600.00", reference="plan-2", request_key="plan-2")
        self.app.fulfillment.record_tender(self.finance, fiscal_year=2026, program="sme", unit_id="org:a", category="goods", outcome="awarded", amount="300.00", reference="tender-2", request_key="tender-2")
        pipeline = self.app.fulfillment.pipeline(self.finance, account)
        self.assertEqual((pipeline["committed"], pipeline["awarded"]), (0, 30000))
        # 合同缩减：释放长期占压的额度
        self.app.fulfillment.reduce_contract(self.finance, fiscal_year=2026, program="sme", unit_id="org:a", category="goods", amount="50.00", reference="tender-2", reason="缩减", request_key="reduce-1")
        self.assertEqual(self.app.fulfillment.pipeline(self.finance, account)["awarded"], 25000)
        # 紧急采购：绕过目录校验直接计入履约并留痕
        self.app.fulfillment.record_emergency(self.finance, fiscal_year=2026, program="sme", unit_id="org:a", category="goods", amount="20.00", reason="防汛物资", reference="emg-1", request_key="emg-1")
        pipeline = self.app.fulfillment.pipeline(self.finance, account)
        self.assertEqual(pipeline["fulfilled"], 2000)
        events = self.app.fulfillment.replay(self.finance, account)
        self.assertTrue(any(event["kind"] == "emergency" and event["detail"]["emergency"] for event in events))
        # 无承诺不能中标，无合同不能超额履约
        with self.assertRaises(ConflictError):
            self.app.fulfillment.record_tender(self.finance, fiscal_year=2026, program="sme", unit_id="org:a", category="goods", outcome="awarded", amount="1.00", reference="tender-9", request_key="tender-9")

    # ------------------------------------------------------------------
    # 进度回执：幂等复用与隔离
    # ------------------------------------------------------------------
    def test_progress_receipt_replay_and_quarantine(self):
        self.establish_and_issue(total="1000.00", issues=(("org:a", "600.00"),))
        account = account_key_of(2026, "sme", "org:a", "goods")
        self.app.fulfillment.record_plan(self.finance, fiscal_year=2026, program="sme", unit_id="org:a", category="goods", amount="100.00", catalog_code="A01", line_total="200.00", reference="plan-1", request_key="plan-1")
        self.app.fulfillment.record_tender(self.finance, fiscal_year=2026, program="sme", unit_id="org:a", category="goods", outcome="awarded", amount="100.00", reference="tender-1", request_key="tender-1")
        payload = {"fiscal_year": 2026, "program": "sme", "unit_id": "org:a", "category": "goods", "amount": "40.00", "contract_ref": "contract-1"}
        first = self.app.fulfillment.record_progress(self.finance, source="erp", receipt_no="rcpt-1", payload=payload)
        self.assertFalse(first["replayed"])
        second = self.app.fulfillment.record_progress(self.finance, source="erp", receipt_no="rcpt-1", payload=payload)
        self.assertTrue(second["replayed"])
        self.assertEqual(first["event_id"], second["event_id"])
        self.assertEqual(self.app.fulfillment.pipeline(self.finance, account)["fulfilled"], 4000)
        with self.assertRaises(ConflictError):
            self.app.fulfillment.record_progress(self.finance, source="erp", receipt_no="rcpt-1", payload={**payload, "amount": "45.00"})
        quarantined = self.app.fulfillment.quarantined(self.finance)
        self.assertEqual(len(quarantined), 1)
        self.assertEqual(quarantined[0]["receipt_no"], "rcpt-1")
        self.assertEqual(self.app.fulfillment.pipeline(self.finance, account)["fulfilled"], 4000)

    # ------------------------------------------------------------------
    # 例外决定：审批分离
    # ------------------------------------------------------------------
    def test_exception_requester_cannot_self_approve(self):
        self.establish_and_issue(total="1000.00", issues=(("org:a", "600.00"),))
        account = account_key_of(2026, "sme", "org:a", "goods")
        requester = clerk("org:a", actor="clerk-a")
        requested = self.app.fulfillment.request_exception(requester, fiscal_year=2026, program="sme", unit_id="org:a", category="goods", amount="100.00", reason="供应商停产", request_key="exc-1")
        approver = AccessContext(actor_id="clerk-a", permissions=frozenset({"decide:exceptions"}))
        with self.assertRaises(PermissionDenied):
            self.app.fulfillment.decide_exception(approver, requested["exception_id"], approve=True, reason="自审", request_key="exc-1-self")
        boss = AccessContext(actor_id="director-1", permissions=frozenset({"decide:exceptions"}))
        decided = self.app.fulfillment.decide_exception(boss, requested["exception_id"], approve=True, reason="情况属实", request_key="exc-1-decide")
        self.assertEqual(decided["state"], "approved")
        pipeline = self.app.fulfillment.pipeline(self.finance, account)
        self.assertEqual(pipeline["exempted"], 10000)
        self.assertEqual(pipeline["gap_minor"], 50000)
        with self.assertRaises(ConflictError):
            self.app.fulfillment.decide_exception(boss, requested["exception_id"], approve=False, reason="重复审批", request_key="exc-1-again")

    # ------------------------------------------------------------------
    # 阶段结果：发布与更正版本
    # ------------------------------------------------------------------
    def test_stage_result_correction_versions(self):
        self.establish_and_issue(total="1000.00", issues=(("org:a", "600.00"),))
        publisher = AccessContext.system("publisher")
        published = self.app.stage_results.publish(publisher, fiscal_year=2026, program="sme", unit_id="org:a", stage="midyear", request_key="report-1")
        self.assertEqual(published["version"], 1)
        with self.assertRaises(ConflictError):
            self.app.stage_results.publish(publisher, fiscal_year=2026, program="sme", unit_id="org:a", stage="midyear", request_key="report-1-dup")
        self.app.fulfillment.record_plan(self.finance, fiscal_year=2026, program="sme", unit_id="org:a", category="goods", amount="100.00", catalog_code="A01", line_total="200.00", reference="plan-1", request_key="plan-1")
        corrected = self.app.stage_results.correct(publisher, published["report_id"], reason="补录计划", request_key="report-1-correct")
        self.assertEqual(corrected["version"], 2)
        self.assertEqual(corrected["corrects_report_id"], published["report_id"])
        self.assertEqual(corrected["snapshot"]["categories"]["goods"]["committed"], 10000)
        chain = self.app.stage_results.list(self.finance, fiscal_year=2026, program="sme", unit_id="org:a", stage="midyear")
        self.assertEqual([(row["version"], row["state"]) for row in chain], [(1, "corrected"), (2, "published")])
        with self.assertRaises(ConflictError):
            self.app.stage_results.correct(publisher, published["report_id"], reason="旧版本不能再更正", request_key="report-1-correct-2")

    # ------------------------------------------------------------------
    # 权限：单位明细与汇总维度
    # ------------------------------------------------------------------
    def test_unit_scope_and_aggregate_dimensions(self):
        self.establish_and_issue(total="1000.00", issues=(("org:a", "600.00"), ("org:b", "300.00")))
        account_a = account_key_of(2026, "sme", "org:a", "goods")
        account_b = account_key_of(2026, "sme", "org:b", "goods")
        clerk_a = clerk("org:a", actor="clerk-a")
        self.assertEqual(self.app.fulfillment.pipeline(clerk_a, account_a)["target_minor"], 60000)
        with self.assertRaises(PermissionDenied):
            self.app.fulfillment.pipeline(clerk_a, account_b)
        visible = self.app.targets.list_accounts(clerk_a)
        self.assertEqual({row["unit_id"] for row in visible}, {"org:a"})
        auditor = AccessContext(actor_id="auditor", permissions=frozenset({"aggregate:fulfillment"}), scopes=frozenset({"aggregate:program"}))
        rows = self.app.stage_results.aggregate(auditor, dimension="program")
        self.assertEqual(rows, [{"program": "sme", "target_minor": 90000, "committed": 0, "reserved": 0, "awarded": 0, "fulfilled": 0, "exempted": 0, "gap_minor": 90000}])
        with self.assertRaises(PermissionDenied):
            self.app.stage_results.aggregate(auditor, dimension="unit")
        with self.assertRaises(PermissionDenied):
            self.app.fulfillment.pipeline(auditor, account_a)
        with self.assertRaises(PermissionDenied):
            self.app.stage_results.aggregate(clerk_a, dimension="program")

    # ------------------------------------------------------------------
    # 恢复接续：整改、待确认调剂、关账
    # ------------------------------------------------------------------
    def test_recovery_resumes_rectification_transfer_and_closing(self):
        old = self.reopen("2025-06-01T00:00:00+08:00")
        self.establish_and_issue(old, year=2025, total="500.00", issues=(("org:a", "500.00"),))
        old.fulfillment.record_plan(self.finance, fiscal_year=2025, program="sme", unit_id="org:a", category="goods", amount="100.00", catalog_code="A01", line_total="200.00", reference="plan-2025", request_key="plan-2025")
        old.fulfillment.record_tender(self.finance, fiscal_year=2025, program="sme", unit_id="org:a", category="goods", outcome="awarded", amount="100.00", reference="tender-2025", request_key="tender-2025")
        transfer = old.targets.propose_transfer(self.finance, fiscal_year=2025, program="sme", category="goods", from_unit="org:a", to_unit="org:b", amount="50.00", reason="调剂", confirm_by="2025-07-01T00:00:00+08:00", request_key="transfer-2025")
        rectification = old.rectifications.create(self.finance, fiscal_year=2025, program="sme", unit_id="org:a", category="goods", assignee="person:liu", due_at="2025-12-01T00:00:00+08:00", request_key="rect-2025")
        # 系统恢复：新实例、时间推进到次年
        resumed = self.reopen("2026-02-01T00:00:00+08:00")
        summary = resumed.rectifications.recover(AccessContext.system("recovery"))
        self.assertEqual(summary["rectifications_escalated"], [rectification["rectification_id"]])
        self.assertEqual(summary["transfers_expired"], [transfer["transfer_id"]])
        self.assertEqual(summary["years_closed"], [2025])
        leased = resumed.outbox.lease(owner="notifier")
        self.assertEqual([message["topic"] for message in leased], ["rectification.escalated"])
        self.assertEqual(self.balance(resumed, 2026, "sme", "org:a", "goods"), 50000)
        self.assertEqual(resumed.targets.get_account(self.finance, account_key_of(2025, "sme", "org:a", "goods"))["state"], "closed")
        # 跨年履约：已关账年度仍接收进度回执
        progress = resumed.fulfillment.record_progress(self.finance, source="erp", receipt_no="rcpt-2025", payload={"fiscal_year": 2025, "program": "sme", "unit_id": "org:a", "category": "goods", "amount": "100.00", "contract_ref": "contract-2025"})
        self.assertFalse(progress["replayed"])
        self.assertEqual(resumed.fulfillment.pipeline(self.finance, account_key_of(2025, "sme", "org:a", "goods"))["fulfilled"], 10000)
        # 再次恢复：全部幂等，没有重复效果
        second = resumed.rectifications.recover(AccessContext.system("recovery"))
        self.assertEqual(second["rectifications_escalated"], [])
        self.assertEqual(second["transfers_expired"], [])
        self.assertEqual(second["years_closed"], [])
        self.assertEqual(self.total_balance(resumed), 50000)

    def test_recovery_reminds_near_deadline_rectification_once(self):
        self.establish_and_issue(total="1000.00", issues=(("org:a", "600.00"),))
        rectification = self.app.rectifications.create(self.finance, fiscal_year=2026, program="sme", unit_id="org:a", category="goods", assignee="person:wang", due_at="2026-03-02T00:00:00+08:00", request_key="rect-remind")
        summary = self.app.rectifications.recover(AccessContext.system("recovery"), horizon_seconds=172800)
        self.assertEqual(summary["rectifications_reminded"], [rectification["rectification_id"]])
        again = self.app.rectifications.recover(AccessContext.system("recovery"), horizon_seconds=172800)
        self.assertEqual(again["rectifications_reminded"], [])

    # ------------------------------------------------------------------
    # 缺口追溯
    # ------------------------------------------------------------------
    def test_trace_gap_links_events_rules_and_next_owner(self):
        self.establish_and_issue(total="1000.00", issues=(("org:a", "600.00"),))
        account = account_key_of(2026, "sme", "org:a", "goods")
        self.app.fulfillment.publish_rule(self.finance, program="sme", category="goods", version=1, min_ratio_bp=3000, catalog=["A01"], effective_from="2026-01-01T00:00:00+08:00", request_key="rule-1")
        self.app.fulfillment.record_plan(self.finance, fiscal_year=2026, program="sme", unit_id="org:a", category="goods", amount="100.00", catalog_code="A01", line_total="200.00", reference="plan-1", request_key="plan-1")
        self.app.rectifications.create(self.finance, fiscal_year=2026, program="sme", unit_id="org:a", category="goods", assignee="person:zhao", due_at="2026-09-01T00:00:00+08:00", request_key="rect-1")
        trace = self.app.stage_results.trace_gap(self.finance, fiscal_year=2026, program="sme", unit_id="org:a", category="goods")
        self.assertEqual(trace["account_key"], account)
        self.assertEqual(trace["gap_minor"], 60000)
        self.assertEqual([event["kind"] for event in trace["events"]], ["plan_commit"])
        self.assertEqual(len(trace["rules"]), 1)
        self.assertEqual(trace["rules"][0]["version"], 1)
        self.assertEqual(len(trace["target_versions"]), 1)
        self.assertTrue(any(movement["kind"] == "issue" for movement in trace["movements"]))
        self.assertEqual(trace["next_owner"], "person:zhao")
        clerk_b = clerk("org:b", actor="clerk-b")
        with self.assertRaises(PermissionDenied):
            self.app.stage_results.trace_gap(clerk_b, fiscal_year=2026, program="sme", unit_id="org:a", category="goods")

    def test_verify_counts_new_tables(self):
        self.establish_and_issue(total="1000.00", issues=(("org:a", "600.00"),))
        result = self.app.verify()
        self.assertGreaterEqual(result["target_movements"], 2)
        self.assertEqual(result["fulfillment_quarantine"], 0)


if __name__ == "__main__":
    unittest.main()
