"""整改任务与恢复接续。

缺口出现后登记整改任务并指定纠偏责任人与截止时间。系统恢复后调用
recover() 接续三类工作：临近截止的整改（提醒与升级）、超过确认截止的
待确认调剂（过期处理）、已结束财政年度的关账结转。所有动作都是幂等的
守卫式更新，重复执行不会产生重复效果。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import timedelta

from .audit import AuditLog
from .database import Database
from .errors import ConflictError, NotFoundError, ValidationError
from .fulfillment import pipeline_counters
from .identifiers import new_id, require_safe
from .idempotency import IdempotencyStore
from .jobs import JobQueue
from .outbox import Outbox
from .security import AccessContext
from .targets import TargetLedger, account_balance, account_key_of, check_category, check_program, check_year, require_unit_scope
from .timeutil import Clock, canonical_instant, parse_instant


RECTIFICATION_STATES = ("open", "done", "escalated")


def _require_request_key(request_key: str) -> str:
    if not isinstance(request_key, str) or not request_key.strip():
        raise ValidationError("请求标识不能为空")
    return request_key.strip()


@dataclass(frozen=True)
class RectificationService:
    """整改任务登记、办结与恢复接续。"""

    database: Database
    clock: Clock
    audit: AuditLog
    idempotency: IdempotencyStore
    jobs: JobQueue
    outbox: Outbox
    targets: TargetLedger

    def create(self, context: AccessContext, *, fiscal_year: object, program: str, unit_id: str, category: str, assignee: str, due_at: str, request_key: str) -> dict:
        """登记整改任务：记录当时缺口，指定下一名纠偏责任人。"""
        context.require("write:rectifications")
        year = check_year(fiscal_year); check_program(program)
        unit_id = require_safe(unit_id, "采购单位")
        require_unit_scope(context, unit_id)
        check_category(category)
        assignee = require_safe(assignee, "纠偏责任人")
        due_at = canonical_instant(due_at)
        request_key = _require_request_key(request_key)
        account_key = account_key_of(year, program, unit_id, category)
        request = {"account_key": account_key, "assignee": assignee, "due_at": due_at}
        with self.database.transaction() as connection:
            def operation() -> dict:
                if not connection.execute("SELECT 1 FROM target_accounts WHERE account_key=?", (account_key,)).fetchone():
                    raise NotFoundError(f"目标账户 {account_key} 不存在")
                counters = pipeline_counters(connection, account_key)
                gap = account_balance(connection, account_key) - counters["fulfilled"] - counters["exempted"]
                rectification_id = new_id("rectification")
                now = self.clock.now()
                connection.execute(
                    "INSERT INTO rectifications(rectification_id,account_key,gap_minor,assignee,due_at,state,created_by,request_key,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (rectification_id, account_key, gap, assignee, due_at, "open", context.actor_id, request_key, now, now),
                )
                self.jobs.schedule_on(connection, job_type="rectification.due", subject_id=rectification_id, run_at=due_at, payload={"account_key": account_key})
                self.audit.append(connection, actor_id=context.actor_id, action="rectification.create", entity_type="rectification", entity_id=rectification_id, version=1, detail={**request, "gap_minor": gap})
                return {"rectification_id": rectification_id, "account_key": account_key, "gap_minor": gap, "assignee": assignee, "due_at": due_at, "state": "open"}
            return self.idempotency.execute(connection, scope="rectification:create", request_key=request_key, request=request, operation=operation)

    def complete(self, context: AccessContext, rectification_id: str, *, request_key: str) -> dict:
        context.require("write:rectifications")
        request_key = _require_request_key(request_key)
        with self.database.transaction() as connection:
            def operation() -> dict:
                row = connection.execute("SELECT * FROM rectifications WHERE rectification_id=?", (rectification_id,)).fetchone()
                if not row:
                    raise NotFoundError(f"整改任务 {rectification_id} 不存在")
                if row["state"] == "done":
                    raise ConflictError("整改任务已经办结")
                connection.execute("UPDATE rectifications SET state='done',updated_at=? WHERE rectification_id=?", (self.clock.now(), rectification_id))
                self.audit.append(connection, actor_id=context.actor_id, action="rectification.complete", entity_type="rectification", entity_id=rectification_id, version=2, detail={})
                return {"rectification_id": rectification_id, "state": "done"}
            return self.idempotency.execute(connection, scope=f"rectification:complete:{rectification_id}", request_key=request_key, request={}, operation=operation)

    def list_open(self, context: AccessContext, *, unit_id: str | None = None) -> list[dict]:
        context.require("read:rectifications")
        sql = "SELECT * FROM rectifications WHERE state IN ('open','escalated')"
        params: list[object] = []
        if unit_id is not None:
            unit_id = require_safe(unit_id, "采购单位")
            require_unit_scope(context, unit_id)
            sql += " AND account_key LIKE ?"; params.append(f"%|{unit_id}|%")
        sql += " ORDER BY due_at, rectification_id"
        with self.database.connect() as connection:
            rows = [dict(row) for row in connection.execute(sql, params)]
        if context.has_scope("*"):
            return rows
        allowed = {scope[len("unit:"):] for scope in context.scopes if scope.startswith("unit:")}
        return [row for row in rows if any(f"|{unit}|" in row["account_key"] for unit in allowed)]

    # ------------------------------------------------------------------
    # 恢复接续
    # ------------------------------------------------------------------
    def _escalate(self, connection, rectification_id: str, *, actor: str) -> bool:
        changed = connection.execute("UPDATE rectifications SET state='escalated',updated_at=? WHERE rectification_id=? AND state='open'", (self.clock.now(), rectification_id)).rowcount
        if changed:
            self.audit.append(connection, actor_id=actor, action="rectification.escalate", entity_type="rectification", entity_id=rectification_id, version=2, detail={})
            self.outbox.enqueue_on(connection, topic="rectification.escalated", aggregate_id=rectification_id, payload={"rectification_id": rectification_id})
        return changed == 1

    def recover(self, context: AccessContext, *, horizon_seconds: int = 86400, limit: int = 100) -> dict:
        """恢复后接续：先处理到期的定时任务，再扫描兜底，全部幂等。

        - 临近截止的整改：到期未办结的升级并通知，临近截止的提醒一次；
        - 待确认调剂：超过确认截止的标记过期；
        - 关账：已结束财政年度执行结转折旧并关闭账户。
        """
        context.require("recover:fulfillment")
        if horizon_seconds < 0 or limit < 1:
            raise ValidationError("恢复参数不合法")
        actor = context.actor_id
        summary = {"jobs_processed": 0, "rectifications_escalated": [], "rectifications_reminded": [], "transfers_expired": [], "years_closed": []}
        for job in self.jobs.claim_due(limit=limit):
            try:
                with self.database.transaction() as connection:
                    if job["job_type"] == "rectification.due":
                        if self._escalate(connection, job["subject_id"], actor=actor):
                            summary["rectifications_escalated"].append(job["subject_id"])
                    elif job["job_type"] == "transfer.confirm_by":
                        if self.targets.expire_transfer(connection, job["subject_id"], actor=actor):
                            summary["transfers_expired"].append(job["subject_id"])
                    elif job["job_type"] == "fiscal_year.close":
                        closed = self.targets._close_fiscal_year(connection, int(json.loads(job["payload_json"])["fiscal_year"]), actor=actor, request_key=f"recover:close:{job['job_id']}")
                        if closed["closed_accounts"]:
                            summary["years_closed"].append(closed["fiscal_year"])
                self.jobs.finish(job["job_id"])
                summary["jobs_processed"] += 1
            except Exception as exc:  # noqa: BLE001 - 恢复时单个任务失败不应阻断其他接续工作
                retry_at = (parse_instant(self.clock.now()) + timedelta(seconds=300)).isoformat().replace("+00:00", "Z")
                self.jobs.retry(job["job_id"], error=str(exc), retry_at=retry_at)
        now = parse_instant(self.clock.now())
        horizon = (now + timedelta(seconds=horizon_seconds)).isoformat().replace("+00:00", "Z")
        with self.database.transaction() as connection:
            for row in connection.execute("SELECT transfer_id FROM target_transfers WHERE state='proposed' AND confirm_by<=? ORDER BY confirm_by LIMIT ?", (self.clock.now(), limit)).fetchall():
                if row["transfer_id"] not in summary["transfers_expired"] and self.targets.expire_transfer(connection, row["transfer_id"], actor=actor):
                    summary["transfers_expired"].append(row["transfer_id"])
            for row in connection.execute("SELECT rectification_id FROM rectifications WHERE state='open' AND due_at<=? ORDER BY due_at LIMIT ?", (self.clock.now(), limit)).fetchall():
                if row["rectification_id"] not in summary["rectifications_escalated"] and self._escalate(connection, row["rectification_id"], actor=actor):
                    summary["rectifications_escalated"].append(row["rectification_id"])
            for row in connection.execute("SELECT rectification_id FROM rectifications WHERE state='open' AND reminded_at IS NULL AND due_at<=? ORDER BY due_at LIMIT ?", (horizon, limit)).fetchall():
                changed = connection.execute("UPDATE rectifications SET reminded_at=? WHERE rectification_id=? AND reminded_at IS NULL", (self.clock.now(), row["rectification_id"])).rowcount
                if changed:
                    self.outbox.enqueue_on(connection, topic="rectification.reminder", aggregate_id=row["rectification_id"], payload={"rectification_id": row["rectification_id"]})
                    summary["rectifications_reminded"].append(row["rectification_id"])
            current_year = now.year
            years = [int(r["fiscal_year"]) for r in connection.execute("SELECT DISTINCT fiscal_year FROM target_accounts WHERE state='open' AND fiscal_year<? ORDER BY fiscal_year", (current_year,))]
            for fiscal_year in years:
                if fiscal_year in summary["years_closed"]:
                    continue
                closed = self.targets._close_fiscal_year(connection, fiscal_year, actor=actor, request_key=f"recover:scan:close:{fiscal_year}")
                if closed["closed_accounts"]:
                    summary["years_closed"].append(fiscal_year)
        return summary
