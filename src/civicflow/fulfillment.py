"""政策目标履约核算：计划承诺、预算预留、招标结果、履约进度、紧急采购与例外决定。

所有执行动作按账户顺序落为不可变事件，可整体回放得到承诺、预留、中标、
履约、豁免五类计数。目录与比例规则按生效时间解析，只约束生效之后登记的
计划；事件上记录所适用的规则版本，保证事后可追溯。进度回执按来源编号幂等：
重复回执复用原处理结果，编号相同而口径不同的数据进入隔离。例外申请与审批
分离，申请人不能审批自己的申请。
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass

from .audit import AuditLog
from .database import Database
from .errors import ConflictError, NotFoundError, ValidationError
from .identifiers import new_id, require_safe
from .idempotency import IdempotencyStore
from .jsonutil import canonical_json, digest_json
from .ledger import to_minor
from .security import AccessContext, assert_distinct
from .targets import account_balance, account_key_of, check_category, check_program, check_year, require_unit_scope, split_account_key
from .timeutil import Clock, canonical_instant


COUNTERS = ("committed", "reserved", "awarded", "fulfilled", "exempted")

# 事件类型对履约管道计数的影响；同一事件可在阶段间搬移额度。
EVENT_DELTAS = {
    "plan_commit": (("committed", 1),),
    "plan_release": (("committed", -1),),
    "budget_reserve": (("reserved", 1),),
    "budget_release": (("reserved", -1),),
    "tender_award": (("committed", -1), ("awarded", 1)),
    "tender_failed": (("committed", -1),),
    "contract_reduce": (("awarded", -1),),
    "progress": (("awarded", -1), ("fulfilled", 1)),
    "emergency": (("fulfilled", 1),),
    "exception_grant": (("exempted", 1),),
}

# 新增义务类事件要求账户处于打开状态；冲减类事件允许落在已关账年度（跨年履约、合同缩减）。
REQUIRES_OPEN_ACCOUNT = {"plan_commit", "budget_reserve", "tender_award", "emergency"}

EXCEPTION_STATES = ("requested", "approved", "rejected")


def pipeline_counters(connection: sqlite3.Connection, account_key: str) -> dict:
    """按事件回放得到的履约管道计数，供本模块与目标账本、阶段结果共用。"""
    counters = {name: 0 for name in COUNTERS}
    for row in connection.execute("SELECT kind, COALESCE(SUM(amount_minor),0) AS total FROM fulfillment_events WHERE account_key=? GROUP BY kind", (account_key,)):
        for counter, sign in EVENT_DELTAS.get(row["kind"], ()):
            counters[counter] += sign * int(row["total"])
    return counters


def _positive_minor(amount: object, label: str = "金额") -> int:
    minor = to_minor(amount)  # type: ignore[arg-type]
    if minor <= 0:
        raise ValidationError(f"{label}必须大于零")
    return minor


def _require_request_key(request_key: str) -> str:
    if not isinstance(request_key, str) or not request_key.strip():
        raise ValidationError("请求标识不能为空")
    return request_key.strip()


def _require_reason(reason: str, label: str = "原因") -> str:
    reason = (reason or "").strip()
    if not reason:
        raise ValidationError(f"必须说明{label}")
    return reason


@dataclass(frozen=True)
class FulfillmentService:
    """履约事件、规则、回执与例外的写入和查询。"""

    database: Database
    clock: Clock
    audit: AuditLog
    idempotency: IdempotencyStore

    # ------------------------------------------------------------------
    # 目录与比例规则
    # ------------------------------------------------------------------
    def publish_rule(self, context: AccessContext, *, program: str, category: str, version: int, min_ratio_bp: int, catalog: list[str], effective_from: str, request_key: str) -> dict:
        """发布目录与比例规则版本；规则只约束生效时间之后登记的计划。"""
        context.require("write:rules")
        check_program(program)
        if category != "*":
            check_category(category)
        if isinstance(version, bool) or not isinstance(version, int) or version < 1:
            raise ValidationError("规则版本必须是正整数")
        if isinstance(min_ratio_bp, bool) or not isinstance(min_ratio_bp, int) or min_ratio_bp < 0 or min_ratio_bp > 10000:
            raise ValidationError("比例必须介于 0 与 10000 基点之间")
        if not isinstance(catalog, list) or any(not isinstance(code, str) or not code.strip() for code in catalog):
            raise ValidationError("目录必须是编码列表")
        catalog = sorted({code.strip() for code in catalog})
        effective_from = canonical_instant(effective_from)
        request_key = _require_request_key(request_key)
        request = {"program": program, "category": category, "version": version, "min_ratio_bp": min_ratio_bp, "catalog": catalog, "effective_from": effective_from}
        with self.database.transaction() as connection:
            def operation() -> dict:
                rule_id = new_id("rule")
                connection.execute(
                    "INSERT INTO ratio_rules(rule_id,program,category,version,min_ratio_bp,catalog_json,effective_from,actor_id,request_key,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (rule_id, program, category, version, min_ratio_bp, canonical_json(catalog), effective_from, context.actor_id, request_key, self.clock.now()),
                )
                self.audit.append(connection, actor_id=context.actor_id, action="rule.publish", entity_type="ratio_rule", entity_id=rule_id, version=version, detail=request)
                return {"rule_id": rule_id, "program": program, "category": category, "version": version, "min_ratio_bp": min_ratio_bp, "catalog": catalog, "effective_from": effective_from}
            try:
                return self.idempotency.execute(connection, scope="fulfillment:rule", request_key=request_key, request=request, operation=operation)
            except sqlite3.IntegrityError as exc:
                raise ConflictError("相同规则版本已经发布") from exc

    def resolve_rule_at(self, connection: sqlite3.Connection, program: str, category: str, at: str) -> dict | None:
        """解析指定时点生效的规则：优先品目精确匹配，其次通配，取最新生效版本。"""
        row = connection.execute(
            "SELECT * FROM ratio_rules WHERE program=? AND category IN (?, '*') AND effective_from<=? ORDER BY (category=?) DESC, effective_from DESC, version DESC LIMIT 1",
            (program, category, at, category),
        ).fetchone()
        if not row:
            return None
        result = dict(row)
        result["catalog"] = json.loads(result["catalog_json"])
        return result

    def list_rules(self, context: AccessContext, *, program: str | None = None) -> list[dict]:
        context.require("read:rules")
        sql = "SELECT * FROM ratio_rules"
        params: list[object] = []
        if program is not None:
            sql += " WHERE program=?"; params.append(check_program(program))
        sql += " ORDER BY program, category, effective_from, version"
        with self.database.connect() as connection:
            rows = []
            for row in connection.execute(sql, params):
                item = dict(row)
                item["catalog"] = json.loads(item["catalog_json"])
                rows.append(item)
            return rows

    # ------------------------------------------------------------------
    # 事件落账
    # ------------------------------------------------------------------
    def _apply_event(self, connection: sqlite3.Connection, *, account_key: str, kind: str, amount_minor: int, reference: str, rule_id: str | None, detail: dict, actor: str, request_key: str) -> dict:
        if kind not in EVENT_DELTAS:
            raise ValidationError("未知事件类型")
        account = connection.execute("SELECT state FROM target_accounts WHERE account_key=?", (account_key,)).fetchone()
        if not account:
            raise NotFoundError(f"目标账户 {account_key} 不存在，请先下达目标")
        if kind in REQUIRES_OPEN_ACCOUNT and account["state"] != "open":
            raise ConflictError("该年度目标账户已关账，不能登记新的执行动作")
        counters = pipeline_counters(connection, account_key)
        for counter, sign in EVENT_DELTAS[kind]:
            if counters[counter] + sign * amount_minor < 0:
                raise ConflictError(f"履约管道计数 {counter} 不足，事件与既有执行依据矛盾")
        if kind == "plan_commit" and counters["committed"] + amount_minor > account_balance(connection, account_key):
            raise ConflictError("计划承诺超出可用目标，不能透支")
        if kind == "exception_grant" and counters["exempted"] + amount_minor > account_balance(connection, account_key):
            raise ConflictError("豁免额度超出可用目标")
        row = connection.execute("SELECT COALESCE(MAX(seq),0) AS s FROM fulfillment_events WHERE account_key=?", (account_key,)).fetchone()
        seq = int(row["s"]) + 1
        event_id = new_id("event")
        occurred_at = self.clock.now()
        connection.execute(
            "INSERT INTO fulfillment_events(event_id,account_key,seq,kind,amount_minor,reference,rule_id,detail_json,actor_id,request_key,occurred_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (event_id, account_key, seq, kind, amount_minor, reference, rule_id, canonical_json(detail), actor, request_key, occurred_at),
        )
        self.audit.append(connection, actor_id=actor, action=f"fulfillment.{kind}", entity_type="fulfillment_event", entity_id=event_id, version=seq, detail={"account_key": account_key, "amount_minor": amount_minor, "reference": reference, **detail})
        return {"event_id": event_id, "account_key": account_key, "seq": seq, "kind": kind, "amount_minor": amount_minor, "reference": reference, "rule_id": rule_id, "occurred_at": occurred_at}

    def _account_parts(self, fiscal_year: object, program: str, unit_id: str, category: str) -> tuple[int, str, str, str]:
        year = check_year(fiscal_year)
        check_program(program)
        unit_id = require_safe(unit_id, "采购单位")
        check_category(category)
        return year, program, unit_id, category

    def _record(self, context: AccessContext, *, kind: str, fiscal_year: object, program: str, unit_id: str, category: str, amount_minor: int, reference: str, detail: dict, request_key: str, rule_id: str | None = None, scope: str) -> dict:
        year, program, unit_id, category = self._account_parts(fiscal_year, program, unit_id, category)
        require_unit_scope(context, unit_id)
        if amount_minor <= 0:
            raise ValidationError("金额必须大于零")
        reference = (reference or "").strip()
        if not reference:
            raise ValidationError("业务参考号不能为空")
        request_key = _require_request_key(request_key)
        account_key = account_key_of(year, program, unit_id, category)
        request = {"account_key": account_key, "kind": kind, "amount_minor": amount_minor, "reference": reference, "detail": detail, "rule_id": rule_id}
        with self.database.transaction() as connection:
            return self.idempotency.execute(connection, scope=scope, request_key=request_key, request=request, operation=lambda: self._apply_event(connection, account_key=account_key, kind=kind, amount_minor=amount_minor, reference=reference, rule_id=rule_id, detail=detail, actor=context.actor_id, request_key=request_key))

    # ------------------------------------------------------------------
    # 计划承诺与预算预留
    # ------------------------------------------------------------------
    def record_plan(self, context: AccessContext, *, fiscal_year: object, program: str, unit_id: str, category: str, amount: object, catalog_code: str, line_total: object, reference: str, request_key: str) -> dict:
        """登记计划承诺：按登记时点生效的目录与比例规则校验，并记录适用规则版本。"""
        context.require("write:fulfillment")
        catalog_code = require_safe(catalog_code, "目录编码")
        line_total_minor = _positive_minor(line_total, "预算行总额")
        minor = _positive_minor(amount)
        with self.database.connect() as connection:
            rule = self.resolve_rule_at(connection, check_program(program), check_category(category), self.clock.now())
        detail: dict = {"catalog_code": catalog_code, "line_total_minor": line_total_minor}
        rule_id = None
        if rule is not None:
            rule_id = rule["rule_id"]
            if rule["catalog"] and catalog_code not in rule["catalog"]:
                raise ValidationError(f"目录编码 {catalog_code} 不在生效目录内（规则版本 {rule['version']}）")
            if minor * 10000 < line_total_minor * int(rule["min_ratio_bp"]):
                raise ValidationError(f"承诺金额低于生效比例规则（版本 {rule['version']}，{rule['min_ratio_bp']} 基点）")
            detail["rule_version"] = rule["version"]
        return self._record(context, kind="plan_commit", fiscal_year=fiscal_year, program=program, unit_id=unit_id, category=category, amount_minor=minor, reference=reference, detail=detail, request_key=request_key, rule_id=rule_id, scope="fulfillment:plan")

    def release_plan(self, context: AccessContext, *, fiscal_year: object, program: str, unit_id: str, category: str, amount: object, reference: str, reason: str, request_key: str) -> dict:
        """撤回计划承诺，释放占用的目标额度。"""
        context.require("write:fulfillment")
        return self._record(context, kind="plan_release", fiscal_year=fiscal_year, program=program, unit_id=unit_id, category=category, amount_minor=_positive_minor(amount), reference=reference, detail={"reason": _require_reason(reason)}, request_key=request_key, scope="fulfillment:plan_release")

    def reserve_budget(self, context: AccessContext, *, fiscal_year: object, program: str, unit_id: str, category: str, amount: object, reference: str, request_key: str) -> dict:
        """登记预算预留。"""
        context.require("write:fulfillment")
        return self._record(context, kind="budget_reserve", fiscal_year=fiscal_year, program=program, unit_id=unit_id, category=category, amount_minor=_positive_minor(amount), reference=reference, detail={}, request_key=request_key, scope="fulfillment:budget_reserve")

    def release_budget(self, context: AccessContext, *, fiscal_year: object, program: str, unit_id: str, category: str, amount: object, reference: str, reason: str, request_key: str) -> dict:
        """释放预算预留。"""
        context.require("write:fulfillment")
        return self._record(context, kind="budget_release", fiscal_year=fiscal_year, program=program, unit_id=unit_id, category=category, amount_minor=_positive_minor(amount), reference=reference, detail={"reason": _require_reason(reason)}, request_key=request_key, scope="fulfillment:budget_release")

    # ------------------------------------------------------------------
    # 招标结果与合同缩减
    # ------------------------------------------------------------------
    def record_tender(self, context: AccessContext, *, fiscal_year: object, program: str, unit_id: str, category: str, outcome: str, amount: object, reference: str, request_key: str) -> dict:
        """登记招标结果：中标把承诺转为合同，流标释放承诺占用的额度。"""
        context.require("write:fulfillment")
        if outcome not in ("awarded", "failed"):
            raise ValidationError("招标结果必须是 awarded 或 failed")
        kind = "tender_award" if outcome == "awarded" else "tender_failed"
        return self._record(context, kind=kind, fiscal_year=fiscal_year, program=program, unit_id=unit_id, category=category, amount_minor=_positive_minor(amount), reference=reference, detail={"outcome": outcome}, request_key=request_key, scope=f"fulfillment:{kind}")

    def reduce_contract(self, context: AccessContext, *, fiscal_year: object, program: str, unit_id: str, category: str, amount: object, reference: str, reason: str, request_key: str) -> dict:
        """合同缩减：冲减中标占用，使额度不再被长期占压。"""
        context.require("write:fulfillment")
        return self._record(context, kind="contract_reduce", fiscal_year=fiscal_year, program=program, unit_id=unit_id, category=category, amount_minor=_positive_minor(amount), reference=reference, detail={"reason": _require_reason(reason)}, request_key=request_key, scope="fulfillment:contract_reduce")

    # ------------------------------------------------------------------
    # 履约进度回执（幂等复用 + 口径隔离）
    # ------------------------------------------------------------------
    def record_progress(self, context: AccessContext, *, source: str, receipt_no: str, payload: dict) -> dict:
        """接收履约进度回执。

        相同来源编号、相同内容的回执复用原处理结果；编号相同而内容（口径）
        不同的回执进入隔离区并拒绝入账。
        """
        context.require("write:fulfillment")
        source = require_safe(source, "回执来源")
        receipt_no = require_safe(receipt_no, "回执编号")
        if not isinstance(payload, dict):
            raise ValidationError("回执内容必须是对象")
        try:
            year, program, unit_id, category = self._account_parts(payload.get("fiscal_year"), payload.get("program", ""), payload.get("unit_id", ""), payload.get("category", ""))
        except ValidationError as exc:
            raise ValidationError(f"回执账户要素不完整：{exc}") from exc
        require_unit_scope(context, unit_id)
        minor = _positive_minor(payload.get("amount"), "履约金额")
        contract_ref = require_safe(str(payload.get("contract_ref", "")), "合同编号")
        digest = digest_json(payload)
        account_key = account_key_of(year, program, unit_id, category)
        mismatched_digest = None
        with self.database.transaction() as connection:
            row = connection.execute("SELECT * FROM fulfillment_receipts WHERE source=? AND receipt_no=?", (source, receipt_no)).fetchone()
            if row and row["payload_digest"] == digest:
                result = json.loads(row["result_json"])
                return {**result, "replayed": True}
            if row:
                mismatched_digest = row["payload_digest"]
            else:
                event = self._apply_event(connection, account_key=account_key, kind="progress", amount_minor=minor, reference=receipt_no, rule_id=None, detail={"contract_ref": contract_ref, "source": source}, actor=context.actor_id, request_key=f"receipt:{source}:{receipt_no}")
                connection.execute(
                    "INSERT INTO fulfillment_receipts(source,receipt_no,payload_digest,status,result_json,first_seen_at) VALUES(?,?,?,?,?,?)",
                    (source, receipt_no, digest, "processed", canonical_json(event), self.clock.now()),
                )
                return {**event, "replayed": False}
        # 口径不一致：隔离记录必须在独立事务中提交，不能随拒绝一起回滚。
        with self.database.transaction() as quarantine:
            duplicate = quarantine.execute("SELECT 1 FROM fulfillment_quarantine WHERE source=? AND receipt_no=? AND incoming_digest=?", (source, receipt_no, digest)).fetchone()
            if not duplicate:
                quarantine.execute(
                    "INSERT INTO fulfillment_quarantine(source,receipt_no,existing_digest,incoming_digest,payload_json,reason,received_at) VALUES(?,?,?,?,?,?,?)",
                    (source, receipt_no, mismatched_digest, digest, canonical_json(payload), "相同编号回执口径不一致", self.clock.now()),
                )
        raise ConflictError("相同编号回执口径不一致，已进入隔离")

    def quarantined(self, context: AccessContext) -> list[dict]:
        """隔离区清单：编号相同而口径不同、等待人工核定的数据。"""
        context.require("read:fulfillment")
        with self.database.connect() as connection:
            return [dict(row) for row in connection.execute("SELECT * FROM fulfillment_quarantine ORDER BY quarantine_id")]

    # ------------------------------------------------------------------
    # 紧急采购
    # ------------------------------------------------------------------
    def record_emergency(self, context: AccessContext, *, fiscal_year: object, program: str, unit_id: str, category: str, amount: object, reason: str, reference: str, request_key: str) -> dict:
        """紧急采购：绕过目录与比例校验直接计入履约，但必须说明依据并全程留痕。"""
        context.require("write:fulfillment")
        detail = {"emergency": True, "reason": _require_reason(reason, "紧急采购依据")}
        return self._record(context, kind="emergency", fiscal_year=fiscal_year, program=program, unit_id=unit_id, category=category, amount_minor=_positive_minor(amount), reference=reference, detail=detail, request_key=request_key, scope="fulfillment:emergency")

    # ------------------------------------------------------------------
    # 例外决定
    # ------------------------------------------------------------------
    def request_exception(self, context: AccessContext, *, fiscal_year: object, program: str, unit_id: str, category: str, amount: object, reason: str, request_key: str) -> dict:
        """申请例外：批准后相应额度从应履约口径中豁免。"""
        context.require("request:exceptions")
        year, program, unit_id, category = self._account_parts(fiscal_year, program, unit_id, category)
        require_unit_scope(context, unit_id)
        minor = _positive_minor(amount)
        reason = _require_reason(reason, "例外理由")
        request_key = _require_request_key(request_key)
        account_key = account_key_of(year, program, unit_id, category)
        request = {"account_key": account_key, "amount_minor": minor, "reason": reason}
        with self.database.transaction() as connection:
            def operation() -> dict:
                exception_id = new_id("exception")
                connection.execute(
                    "INSERT INTO exception_requests(exception_id,account_key,amount_minor,reason,state,requested_by,request_key,created_at) VALUES(?,?,?,?,?,?,?,?)",
                    (exception_id, account_key, minor, reason, "requested", context.actor_id, request_key, self.clock.now()),
                )
                self.audit.append(connection, actor_id=context.actor_id, action="exception.request", entity_type="exception_request", entity_id=exception_id, version=1, detail=request)
                return {"exception_id": exception_id, "account_key": account_key, "amount_minor": minor, "state": "requested"}
            return self.idempotency.execute(connection, scope="fulfillment:exception:request", request_key=request_key, request=request, operation=operation)

    def decide_exception(self, context: AccessContext, exception_id: str, *, approve: bool, reason: str, request_key: str) -> dict:
        """审批例外：申请例外的人不能审批自己的申请；批准即登记豁免事件。"""
        context.require("decide:exceptions")
        reason = _require_reason(reason, "审批意见")
        request_key = _require_request_key(request_key)
        with self.database.transaction() as connection:
            def operation() -> dict:
                row = connection.execute("SELECT * FROM exception_requests WHERE exception_id=?", (exception_id,)).fetchone()
                if not row:
                    raise NotFoundError(f"例外申请 {exception_id} 不存在")
                if row["state"] != "requested":
                    raise ConflictError(f"例外申请当前状态为 {row['state']}，不能审批")
                assert_distinct(row["requested_by"], context.actor_id)
                now = self.clock.now()
                state = "approved" if approve else "rejected"
                connection.execute("UPDATE exception_requests SET state=?,decided_by=?,decision_reason=?,decided_at=? WHERE exception_id=? AND state='requested'", (state, context.actor_id, reason, now, exception_id))
                event = None
                if approve:
                    event = self._apply_event(connection, account_key=row["account_key"], kind="exception_grant", amount_minor=int(row["amount_minor"]), reference=exception_id, rule_id=None, detail={"exception_id": exception_id, "decision_reason": reason}, actor=context.actor_id, request_key=request_key)
                self.audit.append(connection, actor_id=context.actor_id, action="exception.decide", entity_type="exception_request", entity_id=exception_id, version=2, detail={"approve": approve, "reason": reason})
                return {"exception_id": exception_id, "state": state, "event": event}
            return self.idempotency.execute(connection, scope=f"fulfillment:exception:decide:{exception_id}", request_key=request_key, request={"approve": approve, "reason": reason}, operation=operation)

    # ------------------------------------------------------------------
    # 查询与回放
    # ------------------------------------------------------------------
    def pipeline(self, context: AccessContext, account_key: str) -> dict:
        """账户履约管道：承诺、预留、中标、履约、豁免与目标缺口。"""
        context.require("read:fulfillment")
        _, _, unit_id, _ = split_account_key(account_key)
        require_unit_scope(context, unit_id)
        with self.database.connect() as connection:
            if not connection.execute("SELECT 1 FROM target_accounts WHERE account_key=?", (account_key,)).fetchone():
                raise NotFoundError(f"目标账户 {account_key} 不存在")
            counters = pipeline_counters(connection, account_key)
            target = account_balance(connection, account_key)
        return {"account_key": account_key, "target_minor": target, **counters, "gap_minor": target - counters["fulfilled"] - counters["exempted"]}

    def replay(self, context: AccessContext, account_key: str) -> list[dict]:
        """按序回放账户全部执行事件，重建任意时点的执行依据。"""
        context.require("read:fulfillment")
        _, _, unit_id, _ = split_account_key(account_key)
        require_unit_scope(context, unit_id)
        with self.database.connect() as connection:
            rows = []
            for row in connection.execute("SELECT * FROM fulfillment_events WHERE account_key=? ORDER BY seq", (account_key,)):
                item = dict(row)
                item["detail"] = json.loads(item["detail_json"])
                rows.append(item)
            return rows
