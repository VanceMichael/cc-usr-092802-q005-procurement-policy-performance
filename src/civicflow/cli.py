"""命令行入口。"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from .application import CivicFlow
from .cases import CaseService
from .security import AccessContext


def emit(value: object) -> None:
    print(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2))


def demo(app: CivicFlow) -> dict:
    context = AccessContext.system("demo-operator")
    cases = CaseService(app.repository)
    created = cases.create(context, {"case_type": "协同事项", "subject": "示例联合处置", "owner_org": "org:demo", "priority": "high", "opened_at": app.clock.now()}, request_key="demo-case")
    accepted = app.inbox.receive(source="demo", source_key=created["entity_id"], sequence=1, payload={"kind": "opened"}, occurred_at=app.clock.now())
    reservation = app.reservations.reserve(resource_id="room:joint", subject_id=created["entity_id"], quantity=2, capacity=10, start_at="2026-09-28T10:00:00+08:00", end_at="2026-09-28T11:00:00+08:00", actor=context.actor_id)
    debit = app.ledger.post(journal_key="demo", account="coordination", currency="CNY", amount="12.34", direction="debit", reference="demo-debit", actor=context.actor_id)
    credit = app.ledger.post(journal_key="demo", account="coordination", currency="CNY", amount="12.34", direction="credit", reference="demo-credit", actor=context.actor_id)
    message = app.outbox.enqueue(topic="case.opened", aggregate_id=created["entity_id"], payload={"case_id": created["entity_id"]})
    return {"case": created, "inbox": accepted, "reservation": reservation, "entries": [debit, credit], "balance": app.ledger.balance("demo", currency="CNY"), "message_id": message, "verification": app.verify()}


def demo_targets(app: CivicFlow) -> dict:
    """政策目标履约核算的端到端演示。"""
    finance = AccessContext.system("finance-demo")
    unit = "org:demo"
    established = app.targets.establish(finance, fiscal_year=2026, program="sme", category="goods", amount="1000.00", reason="年度总目标下达", request_key="demo-establish")
    issued = app.targets.issue(finance, fiscal_year=2026, program="sme", category="goods", unit_id=unit, new_total="600.00", reason="下达单位目标", request_key="demo-issue")
    rule = app.fulfillment.publish_rule(finance, program="sme", category="goods", version=1, min_ratio_bp=3000, catalog=["A01", "A02"], effective_from="2026-01-01T00:00:00+08:00", request_key="demo-rule")
    plan = app.fulfillment.record_plan(finance, fiscal_year=2026, program="sme", unit_id=unit, category="goods", amount="200.00", catalog_code="A01", line_total="500.00", reference="plan-1", request_key="demo-plan")
    tender = app.fulfillment.record_tender(finance, fiscal_year=2026, program="sme", unit_id=unit, category="goods", outcome="awarded", amount="200.00", reference="tender-1", request_key="demo-tender")
    progress = app.fulfillment.record_progress(finance, source="erp", receipt_no="rcpt-1", payload={"fiscal_year": 2026, "program": "sme", "unit_id": unit, "category": "goods", "amount": "50.00", "contract_ref": "contract-1"})
    report = app.stage_results.publish(finance, fiscal_year=2026, program="sme", unit_id=unit, stage="midyear", request_key="demo-report")
    trace = app.stage_results.trace_gap(finance, fiscal_year=2026, program="sme", unit_id=unit, category="goods")
    return {"established": established, "issued": issued, "rule": rule, "plan": plan, "tender": tender, "progress": progress, "report": {"report_id": report["report_id"], "version": report["version"]}, "gap": {key: trace[key] for key in ("target_minor", "committed", "awarded", "fulfilled", "gap_minor")}, "verification": app.verify()}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="协同事务平台")
    parser.add_argument("--db", default=os.getenv("CIVICFLOW_DB", "civicflow.sqlite3"))
    parser.add_argument("--now", default=None, help="测试或演示使用的固定时间")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("demo")
    commands.add_parser("demo-targets")
    commands.add_parser("verify")
    commands.add_parser("list-cases")
    args = parser.parse_args(argv)
    app = CivicFlow.open(Path(args.db), fixed_now=args.now)
    if args.command == "demo": emit(demo(app))
    elif args.command == "demo-targets": emit(demo_targets(app))
    elif args.command == "verify": emit(app.verify())
    elif args.command == "list-cases": emit(CaseService(app.repository).list_current(AccessContext.system("cli")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
