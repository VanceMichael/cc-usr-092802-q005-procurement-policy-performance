"""阶段结果发布、更正版本、授权汇总与目标缺口追溯。

阶段结果一经对外发布即不可改写；需要调整时生成新的更正版本并保留指向
被更正版本的指针，形成可追溯的版本链。汇总岗位只能取得获授权的统计维度，
看不到单位明细。缺口追溯把目标版本、执行事件、适用规则、相关移动与下一名
纠偏责任人串成一份完整证据。
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from .audit import AuditLog
from .database import Database
from .errors import ConflictError, NotFoundError, PermissionDenied, ValidationError
from .fulfillment import pipeline_counters
from .identifiers import new_id, require_safe
from .idempotency import IdempotencyStore
from .jsonutil import canonical_json
from .security import AccessContext
from .targets import CATEGORIES, account_balance, account_key_of, check_category, check_program, check_year, require_unit_scope
from .timeutil import Clock


AGGREGATE_DIMENSIONS = ("unit", "program", "category", "fiscal_year")
REPORT_STATES = ("published", "corrected")


def _require_request_key(request_key: str) -> str:
    if not isinstance(request_key, str) or not request_key.strip():
        raise ValidationError("请求标识不能为空")
    return request_key.strip()


@dataclass(frozen=True)
class StageResultService:
    """阶段结果与汇总、追溯查询。"""

    database: Database
    clock: Clock
    audit: AuditLog
    idempotency: IdempotencyStore

    # ------------------------------------------------------------------
    # 发布与更正
    # ------------------------------------------------------------------
    def _snapshot(self, connection, fiscal_year: int, program: str, unit_id: str) -> dict:
        categories = {}
        totals = {"target_minor": 0, "committed": 0, "reserved": 0, "awarded": 0, "fulfilled": 0, "exempted": 0, "gap_minor": 0}
        for category in CATEGORIES:
            account_key = account_key_of(fiscal_year, program, unit_id, category)
            if not connection.execute("SELECT 1 FROM target_accounts WHERE account_key=?", (account_key,)).fetchone():
                continue
            counters = pipeline_counters(connection, account_key)
            target = account_balance(connection, account_key)
            gap = target - counters["fulfilled"] - counters["exempted"]
            categories[category] = {"target_minor": target, **counters, "gap_minor": gap}
            totals["target_minor"] += target
            for name in ("committed", "reserved", "awarded", "fulfilled", "exempted"):
                totals[name] += counters[name]
            totals["gap_minor"] += gap
        return {"fiscal_year": fiscal_year, "program": program, "unit_id": unit_id, "categories": categories, "totals": totals, "generated_at": self.clock.now()}

    def publish(self, context: AccessContext, *, fiscal_year: object, program: str, unit_id: str, stage: str, request_key: str) -> dict:
        """对外发布阶段结果；同一阶段只能发布一次，之后只能更正。"""
        context.require("publish:stage_results")
        year = check_year(fiscal_year); check_program(program)
        unit_id = require_safe(unit_id, "采购单位")
        stage = require_safe(stage, "阶段标识")
        request_key = _require_request_key(request_key)
        request = {"fiscal_year": year, "program": program, "unit_id": unit_id, "stage": stage}
        with self.database.transaction() as connection:
            def operation() -> dict:
                existing = connection.execute("SELECT COUNT(*) AS n FROM stage_reports WHERE fiscal_year=? AND program=? AND unit_id=? AND stage=?", (year, program, unit_id, stage)).fetchone()
                if int(existing["n"]) > 0:
                    raise ConflictError("该阶段结果已发布，请通过更正生成新版本")
                snapshot = self._snapshot(connection, year, program, unit_id)
                report_id = new_id("report")
                connection.execute(
                    "INSERT INTO stage_reports(report_id,fiscal_year,program,unit_id,stage,version,corrects_report_id,snapshot_json,state,published_by,published_at,request_key) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                    (report_id, year, program, unit_id, stage, 1, None, canonical_json(snapshot), "published", context.actor_id, self.clock.now(), request_key),
                )
                self.audit.append(connection, actor_id=context.actor_id, action="stage.publish", entity_type="stage_report", entity_id=report_id, version=1, detail=request)
                return {"report_id": report_id, "version": 1, "state": "published", "snapshot": snapshot}
            return self.idempotency.execute(connection, scope="stage:publish", request_key=request_key, request=request, operation=operation)

    def correct(self, context: AccessContext, report_id: str, *, reason: str, request_key: str) -> dict:
        """更正已发布的阶段结果：生成新的更正版本，原版本保留并标记为已更正。"""
        context.require("publish:stage_results")
        reason = (reason or "").strip()
        if not reason:
            raise ValidationError("必须说明更正原因")
        request_key = _require_request_key(request_key)
        with self.database.transaction() as connection:
            def operation() -> dict:
                row = connection.execute("SELECT * FROM stage_reports WHERE report_id=?", (report_id,)).fetchone()
                if not row:
                    raise NotFoundError(f"阶段结果 {report_id} 不存在")
                if row["state"] != "published":
                    raise ConflictError("只能更正当前生效的发布版本")
                snapshot = self._snapshot(connection, int(row["fiscal_year"]), row["program"], row["unit_id"])
                snapshot["correction_reason"] = reason
                new_id_value = new_id("report")
                version = int(row["version"]) + 1
                connection.execute(
                    "INSERT INTO stage_reports(report_id,fiscal_year,program,unit_id,stage,version,corrects_report_id,snapshot_json,state,published_by,published_at,request_key) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                    (new_id_value, row["fiscal_year"], row["program"], row["unit_id"], row["stage"], version, report_id, canonical_json(snapshot), "published", context.actor_id, self.clock.now(), request_key),
                )
                connection.execute("UPDATE stage_reports SET state='corrected' WHERE report_id=?", (report_id,))
                self.audit.append(connection, actor_id=context.actor_id, action="stage.correct", entity_type="stage_report", entity_id=new_id_value, version=version, detail={"corrects": report_id, "reason": reason})
                return {"report_id": new_id_value, "version": version, "state": "published", "corrects_report_id": report_id, "snapshot": snapshot}
            return self.idempotency.execute(connection, scope=f"stage:correct:{report_id}", request_key=request_key, request={"reason": reason}, operation=operation)

    def get(self, context: AccessContext, report_id: str) -> dict:
        context.require("read:stage_results")
        with self.database.connect() as connection:
            row = connection.execute("SELECT * FROM stage_reports WHERE report_id=?", (report_id,)).fetchone()
            if not row:
                raise NotFoundError(f"阶段结果 {report_id} 不存在")
            require_unit_scope(context, row["unit_id"])
            return self._to_dict(row)

    def list(self, context: AccessContext, *, fiscal_year: object, program: str, unit_id: str, stage: str | None = None) -> list[dict]:
        """单位阶段的发布与更正版本链，按版本排序。"""
        context.require("read:stage_results")
        year = check_year(fiscal_year); check_program(program)
        unit_id = require_safe(unit_id, "采购单位")
        require_unit_scope(context, unit_id)
        sql = "SELECT * FROM stage_reports WHERE fiscal_year=? AND program=? AND unit_id=?"
        params: list[object] = [year, program, unit_id]
        if stage is not None:
            sql += " AND stage=?"; params.append(require_safe(stage, "阶段标识"))
        sql += " ORDER BY stage, version"
        with self.database.connect() as connection:
            return [self._to_dict(row) for row in connection.execute(sql, params)]

    @staticmethod
    def _to_dict(row) -> dict:
        result = dict(row)
        result["snapshot"] = json.loads(result["snapshot_json"])
        del result["snapshot_json"]
        return result

    # ------------------------------------------------------------------
    # 授权汇总
    # ------------------------------------------------------------------
    def aggregate(self, context: AccessContext, *, dimension: str, fiscal_year: object | None = None, program: str | None = None) -> list[dict]:
        """汇总岗位只能取得获授权的统计维度，结果只含分组合计，不含单位明细。"""
        context.require("aggregate:fulfillment")
        if dimension not in AGGREGATE_DIMENSIONS:
            raise ValidationError("未知统计维度")
        if not context.has_scope(f"aggregate:{dimension}"):
            raise PermissionDenied(f"缺少统计维度授权: {dimension}")
        year = check_year(fiscal_year) if fiscal_year is not None else None
        if program is not None:
            check_program(program)
        sql = "SELECT * FROM target_accounts WHERE unit_id<>?"
        params: list[object] = ["pool"]
        if year is not None:
            sql += " AND fiscal_year=?"; params.append(year)
        if program is not None:
            sql += " AND program=?"; params.append(program)
        groups: dict[object, dict] = {}
        with self.database.connect() as connection:
            for row in connection.execute(sql, params):
                key = row[{"unit": "unit_id", "program": "program", "category": "category", "fiscal_year": "fiscal_year"}[dimension]]
                bucket = groups.setdefault(key, {"target_minor": 0, "committed": 0, "reserved": 0, "awarded": 0, "fulfilled": 0, "exempted": 0, "gap_minor": 0})
                counters = pipeline_counters(connection, row["account_key"])
                target = account_balance(connection, row["account_key"])
                bucket["target_minor"] += target
                for name in ("committed", "reserved", "awarded", "fulfilled", "exempted"):
                    bucket[name] += counters[name]
                bucket["gap_minor"] += target - counters["fulfilled"] - counters["exempted"]
        return [{dimension: key, **groups[key]} for key in sorted(groups, key=str)]

    # ------------------------------------------------------------------
    # 缺口追溯
    # ------------------------------------------------------------------
    def trace_gap(self, context: AccessContext, *, fiscal_year: object, program: str, unit_id: str, category: str) -> dict:
        """从目标缺口追到目标版本、执行事件、适用规则、相关移动与下一名纠偏责任人。"""
        context.require("read:fulfillment")
        year = check_year(fiscal_year); check_program(program)
        unit_id = require_safe(unit_id, "采购单位")
        require_unit_scope(context, unit_id)
        check_category(category)
        account_key = account_key_of(year, program, unit_id, category)
        with self.database.connect() as connection:
            account = connection.execute("SELECT * FROM target_accounts WHERE account_key=?", (account_key,)).fetchone()
            if not account:
                raise NotFoundError(f"目标账户 {account_key} 不存在")
            counters = pipeline_counters(connection, account_key)
            target = account_balance(connection, account_key)
            versions = [dict(row) for row in connection.execute("SELECT * FROM target_versions WHERE account_key=? ORDER BY version", (account_key,))]
            events = []
            rule_ids = []
            for row in connection.execute("SELECT * FROM fulfillment_events WHERE account_key=? ORDER BY seq", (account_key,)):
                item = dict(row)
                item["detail"] = json.loads(item["detail_json"])
                del item["detail_json"]
                events.append(item)
                if item["rule_id"] and item["rule_id"] not in rule_ids:
                    rule_ids.append(item["rule_id"])
            rules = []
            for rule_id in rule_ids:
                rule = connection.execute("SELECT * FROM ratio_rules WHERE rule_id=?", (rule_id,)).fetchone()
                if rule:
                    entry = dict(rule)
                    entry["catalog"] = json.loads(entry["catalog_json"])
                    del entry["catalog_json"]
                    rules.append(entry)
            movements = [dict(row) for row in connection.execute("SELECT * FROM target_movements WHERE from_account=? OR to_account=? ORDER BY occurred_at, movement_id", (account_key, account_key))]
            rectifications = [dict(row) for row in connection.execute("SELECT * FROM rectifications WHERE account_key=? AND state IN ('open','escalated') ORDER BY due_at, rectification_id", (account_key,))]
            reports = [dict(row) for row in connection.execute("SELECT report_id, stage, version, state, corrects_report_id FROM stage_reports WHERE fiscal_year=? AND program=? AND unit_id=? ORDER BY stage, version", (year, program, unit_id))]
        next_owner = rectifications[0]["assignee"] if rectifications else None
        return {
            "account_key": account_key,
            "account_state": account["state"],
            "target_minor": target,
            **counters,
            "gap_minor": target - counters["fulfilled"] - counters["exempted"],
            "target_versions": versions,
            "events": events,
            "rules": rules,
            "movements": movements,
            "open_rectifications": rectifications,
            "next_owner": next_owner,
            "stage_reports": reports,
        }
