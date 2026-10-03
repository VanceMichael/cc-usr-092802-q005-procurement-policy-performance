"""政策目标账本：年度目标下达、换版、调剂、释放、结转与关账。

每个财政年度按采购单位及货物、工程、服务分类开设目标账户；账户余额由
双边移动（下达、调剂、释放、结转）累计而成，任何移动都在同一事务内校验
来源账户余额，保证并发处理不会透支总目标。每次移动为账户生成新的目标
版本，形成可回放的换版历史。
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Callable

from .audit import AuditLog
from .database import Database
from .errors import ConflictError, NotFoundError, PermissionDenied, ValidationError
from .identifiers import new_id, require_safe
from .idempotency import IdempotencyStore
from .jobs import JobQueue
from .ledger import to_minor
from .security import AccessContext
from .timeutil import Clock, canonical_instant, parse_instant


PROGRAMS = ("green", "sme")
CATEGORIES = ("goods", "works", "services")
POOL_UNIT = "pool"
EXTERNAL_ACCOUNT = "external"
ACCOUNT_STATES = ("open", "closed")
TRANSFER_STATES = ("proposed", "confirmed", "rejected", "expired")
MOVEMENT_KINDS = ("establish", "issue", "transfer", "release", "carry_over")


def check_program(program: str) -> str:
    if program not in PROGRAMS:
        raise ValidationError("未知政策目标类型，必须是 green 或 sme")
    return program


def check_category(category: str) -> str:
    if category not in CATEGORIES:
        raise ValidationError("未知品目分类，必须是 goods、works 或 services")
    return category


def check_year(fiscal_year: object) -> int:
    if isinstance(fiscal_year, bool) or not isinstance(fiscal_year, int):
        raise ValidationError("财政年度必须是整数")
    if fiscal_year < 2000 or fiscal_year > 2100:
        raise ValidationError("财政年度超出允许范围")
    return fiscal_year


def account_key_of(fiscal_year: int, program: str, unit_id: str, category: str) -> str:
    # 单位标识本身允许带冒号（如 org:a），账户键改用标识字符集之外的分隔符。
    return f"{fiscal_year}|{program}|{unit_id}|{category}"


def split_account_key(account_key: str) -> tuple[int, str, str, str]:
    parts = account_key.split("|")
    if len(parts) != 4:
        raise ValidationError("账户标识不合法")
    try:
        year = int(parts[0])
    except ValueError as exc:
        raise ValidationError("账户标识不合法") from exc
    return check_year(year), check_program(parts[1]), parts[2], check_category(parts[3])


def account_balance(connection: sqlite3.Connection, account_key: str) -> int:
    """账户当前目标余额：流入减流出，供本模块与履约核算共用。"""
    row = connection.execute(
        "SELECT COALESCE(SUM(CASE WHEN to_account=? THEN amount_minor WHEN from_account=? THEN -amount_minor ELSE 0 END),0) AS balance FROM target_movements WHERE from_account=? OR to_account=?",
        (account_key, account_key, account_key, account_key),
    ).fetchone()
    return int(row["balance"])


def _positive_minor(amount: object, label: str = "金额") -> int:
    minor = to_minor(amount)  # type: ignore[arg-type]
    if minor <= 0:
        raise ValidationError(f"{label}必须大于零")
    return minor


def _require_request_key(request_key: str) -> str:
    if not isinstance(request_key, str) or not request_key.strip():
        raise ValidationError("请求标识不能为空")
    return request_key.strip()


def require_unit_scope(context: AccessContext, unit_id: str) -> None:
    """明细级访问控制：各单位只能看、只能动本单位的账户。"""
    if not context.has_scope(f"unit:{unit_id}"):
        raise PermissionDenied("无权访问该采购单位的明细")


@dataclass(frozen=True)
class TargetLedger:
    """目标账户与移动的写入、查询；全部变更幂等且守恒。"""

    database: Database
    clock: Clock
    audit: AuditLog
    idempotency: IdempotencyStore
    jobs: JobQueue
    # 由装配层注入履约管道计数，用于释放/下调目标时保护已承诺额度。
    pipeline_fn: Callable[[sqlite3.Connection, str], dict] | None = None

    # ------------------------------------------------------------------
    # 内部工具
    # ------------------------------------------------------------------
    def _ensure_account(self, connection: sqlite3.Connection, fiscal_year: int, program: str, unit_id: str, category: str) -> str:
        key = account_key_of(fiscal_year, program, unit_id, category)
        row = connection.execute("SELECT account_key FROM target_accounts WHERE account_key=?", (key,)).fetchone()
        if row:
            return key
        now = self.clock.now()
        connection.execute(
            "INSERT INTO target_accounts(account_key,fiscal_year,program,unit_id,category,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
            (key, fiscal_year, program, unit_id, category, "open", now, now),
        )
        return key

    def _account_row(self, connection: sqlite3.Connection, account_key: str):
        row = connection.execute("SELECT * FROM target_accounts WHERE account_key=?", (account_key,)).fetchone()
        if not row:
            raise NotFoundError(f"目标账户 {account_key} 不存在")
        return row

    def _bump_version(self, connection: sqlite3.Connection, account_key: str, *, kind: str, reference: str, reason: str, actor: str, request_key: str) -> int:
        balance = account_balance(connection, account_key)
        row = connection.execute("SELECT COALESCE(MAX(version),0) AS v FROM target_versions WHERE account_key=?", (account_key,)).fetchone()
        version = int(row["v"]) + 1
        connection.execute(
            "INSERT INTO target_versions(account_key,version,amount_minor,kind,reference,reason,actor_id,request_key,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (account_key, version, balance, kind, reference, reason, actor, request_key, self.clock.now()),
        )
        connection.execute("UPDATE target_accounts SET updated_at=? WHERE account_key=?", (self.clock.now(), account_key))
        return version

    def _move(self, connection: sqlite3.Connection, *, kind: str, fiscal_year: int, program: str, category: str, from_account: str, to_account: str, amount_minor: int, reference: str, reason: str, actor: str, request_key: str) -> dict:
        if kind not in MOVEMENT_KINDS:
            raise ValidationError("未知移动类型")
        if amount_minor <= 0:
            raise ValidationError("移动金额必须大于零")
        if from_account == to_account:
            raise ValidationError("来源与去向账户不能相同")
        if from_account != EXTERNAL_ACCOUNT:
            balance = account_balance(connection, from_account)
            if balance < amount_minor:
                raise ConflictError(f"账户 {from_account} 可用目标不足，不能透支")
        movement_id = new_id("movement")
        occurred_at = self.clock.now()
        connection.execute(
            "INSERT INTO target_movements(movement_id,kind,fiscal_year,program,category,from_account,to_account,amount_minor,reference,reason,actor_id,request_key,occurred_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (movement_id, kind, fiscal_year, program, category, from_account, to_account, amount_minor, reference, reason, actor, request_key, occurred_at),
        )
        versions = {}
        for key in dict.fromkeys((from_account, to_account)):
            if key != EXTERNAL_ACCOUNT:
                versions[key] = self._bump_version(connection, key, kind=kind, reference=movement_id, reason=reason, actor=actor, request_key=request_key)
        self.audit.append(
            connection,
            actor_id=actor,
            action=f"target.{kind}",
            entity_type="target_movement",
            entity_id=movement_id,
            version=1,
            detail={"kind": kind, "from_account": from_account, "to_account": to_account, "amount_minor": amount_minor, "reference": reference, "reason": reason},
        )
        return {"movement_id": movement_id, "kind": kind, "from_account": from_account, "to_account": to_account, "amount_minor": amount_minor, "versions": versions, "occurred_at": occurred_at}

    def _guard_pipeline(self, connection: sqlite3.Connection, account_key: str, balance_after: int) -> None:
        """目标下调后不得低于已承诺额度，避免承诺悬空。"""
        if self.pipeline_fn is None:
            return
        committed = int(self.pipeline_fn(connection, account_key).get("committed", 0))
        if balance_after < committed:
            raise ConflictError("目标余额将低于已承诺计划，先释放承诺再下调")

    # ------------------------------------------------------------------
    # 目标下达与换版
    # ------------------------------------------------------------------
    def establish(self, context: AccessContext, *, fiscal_year: int, program: str, category: str, amount: object, reason: str, request_key: str) -> dict:
        """建立年度总目标：外部额度进入总目标池。"""
        context.require("write:targets")
        fiscal_year = check_year(fiscal_year); check_program(program); check_category(category)
        minor = _positive_minor(amount); reason = (reason or "").strip()
        if not reason:
            raise ValidationError("必须说明目标依据")
        request_key = _require_request_key(request_key)
        request = {"fiscal_year": fiscal_year, "program": program, "category": category, "amount_minor": minor, "reason": reason}
        with self.database.transaction() as connection:
            def operation() -> dict:
                pool = self._ensure_account(connection, fiscal_year, program, POOL_UNIT, category)
                if connection.execute("SELECT state FROM target_accounts WHERE account_key=?", (pool,)).fetchone()["state"] != "open":
                    raise ConflictError("该年度目标账户已关账")
                movement = self._move(connection, kind="establish", fiscal_year=fiscal_year, program=program, category=category, from_account=EXTERNAL_ACCOUNT, to_account=pool, amount_minor=minor, reference=f"establish:{fiscal_year}:{program}:{category}", reason=reason, actor=context.actor_id, request_key=request_key)
                # 年度结束时的关账任务随总目标一并登记，恢复后可接续。
                self.jobs.schedule_on(connection, job_type="fiscal_year.close", subject_id=f"fiscal_year:{fiscal_year}", run_at=f"{fiscal_year + 1}-01-01T00:00:00Z", payload={"fiscal_year": fiscal_year})
                return {"account_key": pool, "balance_minor": account_balance(connection, pool), "movement": movement}
            return self.idempotency.execute(connection, scope="targets:establish", request_key=request_key, request=request, operation=operation)

    def issue(self, context: AccessContext, *, fiscal_year: int, program: str, category: str, unit_id: str, new_total: object, reason: str, request_key: str) -> dict:
        """下达或换版单位目标：以新的目标总额替换旧版本，差额在总目标池与单位间移动。"""
        context.require("write:targets")
        fiscal_year = check_year(fiscal_year); check_program(program); check_category(category)
        unit_id = require_safe(unit_id, "采购单位")
        if unit_id == POOL_UNIT:
            raise ValidationError("不能向总目标池下达单位目标")
        minor = to_minor(new_total)  # type: ignore[arg-type]
        if minor < 0:
            raise ValidationError("目标总额不能为负")
        reason = (reason or "").strip()
        if not reason:
            raise ValidationError("必须说明下达或换版依据")
        request_key = _require_request_key(request_key)
        request = {"fiscal_year": fiscal_year, "program": program, "category": category, "unit_id": unit_id, "new_total_minor": minor, "reason": reason}
        with self.database.transaction() as connection:
            def operation() -> dict:
                pool = self._ensure_account(connection, fiscal_year, program, POOL_UNIT, category)
                account = self._ensure_account(connection, fiscal_year, program, unit_id, category)
                for key in (pool, account):
                    if connection.execute("SELECT state FROM target_accounts WHERE account_key=?", (key,)).fetchone()["state"] != "open":
                        raise ConflictError("该年度目标账户已关账")
                balance = account_balance(connection, account)
                delta = minor - balance
                if delta == 0:
                    raise ValidationError("目标总额没有变化，无需换版")
                if delta > 0:
                    movement = self._move(connection, kind="issue", fiscal_year=fiscal_year, program=program, category=category, from_account=pool, to_account=account, amount_minor=delta, reference=f"issue:{account}", reason=reason, actor=context.actor_id, request_key=request_key)
                else:
                    self._guard_pipeline(connection, account, minor)
                    movement = self._move(connection, kind="issue", fiscal_year=fiscal_year, program=program, category=category, from_account=account, to_account=pool, amount_minor=-delta, reference=f"issue:{account}", reason=reason, actor=context.actor_id, request_key=request_key)
                return {"account_key": account, "target_minor": minor, "previous_minor": balance, "movement": movement}
            return self.idempotency.execute(connection, scope="targets:issue", request_key=request_key, request=request, operation=operation)

    def release(self, context: AccessContext, *, fiscal_year: int, program: str, category: str, unit_id: str, amount: object, reason: str, request_key: str) -> dict:
        """单位释放未使用目标，退回总目标池。"""
        context.require("write:targets")
        fiscal_year = check_year(fiscal_year); check_program(program); check_category(category)
        unit_id = require_safe(unit_id, "采购单位")
        require_unit_scope(context, unit_id)
        minor = _positive_minor(amount); reason = (reason or "").strip()
        if not reason:
            raise ValidationError("必须说明释放原因")
        request_key = _require_request_key(request_key)
        request = {"fiscal_year": fiscal_year, "program": program, "category": category, "unit_id": unit_id, "amount_minor": minor, "reason": reason}
        with self.database.transaction() as connection:
            def operation() -> dict:
                account = account_key_of(fiscal_year, program, unit_id, category)
                self._account_row(connection, account)
                pool = self._ensure_account(connection, fiscal_year, program, POOL_UNIT, category)
                self._guard_pipeline(connection, account, account_balance(connection, account) - minor)
                movement = self._move(connection, kind="release", fiscal_year=fiscal_year, program=program, category=category, from_account=account, to_account=pool, amount_minor=minor, reference=f"release:{account}", reason=reason, actor=context.actor_id, request_key=request_key)
                return {"account_key": account, "balance_minor": account_balance(connection, account), "movement": movement}
            return self.idempotency.execute(connection, scope="targets:release", request_key=request_key, request=request, operation=operation)

    # ------------------------------------------------------------------
    # 单位间调剂（两阶段：提出 + 确认）
    # ------------------------------------------------------------------
    def propose_transfer(self, context: AccessContext, *, fiscal_year: int, program: str, category: str, from_unit: str, to_unit: str, amount: object, reason: str, confirm_by: str, request_key: str) -> dict:
        context.require("write:targets")
        fiscal_year = check_year(fiscal_year); check_program(program); check_category(category)
        from_unit = require_safe(from_unit, "调出单位"); to_unit = require_safe(to_unit, "调入单位")
        if from_unit == to_unit:
            raise ValidationError("调出与调入单位不能相同")
        if POOL_UNIT in (from_unit, to_unit):
            raise ValidationError("调剂发生在采购单位之间，请使用下达或释放调整总目标池")
        require_unit_scope(context, from_unit)
        minor = _positive_minor(amount); reason = (reason or "").strip()
        if not reason:
            raise ValidationError("必须说明调剂原因")
        confirm_by = canonical_instant(confirm_by)
        if self.clock.is_due(confirm_by):
            raise ValidationError("确认截止必须晚于当前时间")
        request_key = _require_request_key(request_key)
        request = {"fiscal_year": fiscal_year, "program": program, "category": category, "from_unit": from_unit, "to_unit": to_unit, "amount_minor": minor, "reason": reason, "confirm_by": confirm_by}
        with self.database.transaction() as connection:
            def operation() -> dict:
                transfer_id = new_id("transfer")
                now = self.clock.now()
                connection.execute(
                    "INSERT INTO target_transfers(transfer_id,fiscal_year,program,category,from_unit,to_unit,amount_minor,reason,state,proposed_by,confirm_by,request_key,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (transfer_id, fiscal_year, program, category, from_unit, to_unit, minor, reason, "proposed", context.actor_id, confirm_by, request_key, now, now),
                )
                self.jobs.schedule_on(connection, job_type="transfer.confirm_by", subject_id=transfer_id, run_at=confirm_by, payload={"transfer_id": transfer_id})
                self.audit.append(connection, actor_id=context.actor_id, action="transfer.propose", entity_type="target_transfer", entity_id=transfer_id, version=1, detail=request)
                return {"transfer_id": transfer_id, "state": "proposed", "amount_minor": minor, "confirm_by": confirm_by}
            return self.idempotency.execute(connection, scope="targets:transfer:propose", request_key=request_key, request=request, operation=operation)

    def _transfer_row(self, connection: sqlite3.Connection, transfer_id: str):
        row = connection.execute("SELECT * FROM target_transfers WHERE transfer_id=?", (transfer_id,)).fetchone()
        if not row:
            raise NotFoundError(f"调剂 {transfer_id} 不存在")
        return row

    def confirm_transfer(self, context: AccessContext, transfer_id: str, *, request_key: str) -> dict:
        """确认调剂：在同一事务内校验调出方余额并执行移动，并发确认不会透支。"""
        context.require("confirm:transfers")
        request_key = _require_request_key(request_key)
        with self.database.transaction() as connection:
            def operation() -> dict:
                row = self._transfer_row(connection, transfer_id)
                if row["state"] != "proposed":
                    raise ConflictError(f"调剂当前状态为 {row['state']}，不能确认")
                if self.clock.is_due(row["confirm_by"]):
                    raise ConflictError("调剂已超过确认截止，按过期处理")
                from_account = account_key_of(row["fiscal_year"], row["program"], row["from_unit"], row["category"])
                to_account = self._ensure_account(connection, row["fiscal_year"], row["program"], row["to_unit"], row["category"])
                self._account_row(connection, from_account)
                movement = self._move(connection, kind="transfer", fiscal_year=row["fiscal_year"], program=row["program"], category=row["category"], from_account=from_account, to_account=to_account, amount_minor=int(row["amount_minor"]), reference=transfer_id, reason=row["reason"], actor=context.actor_id, request_key=request_key)
                now = self.clock.now()
                connection.execute("UPDATE target_transfers SET state='confirmed',decided_by=?,updated_at=? WHERE transfer_id=? AND state='proposed'", (context.actor_id, now, transfer_id))
                self.audit.append(connection, actor_id=context.actor_id, action="transfer.confirm", entity_type="target_transfer", entity_id=transfer_id, version=2, detail={"movement_id": movement["movement_id"]})
                return {"transfer_id": transfer_id, "state": "confirmed", "movement": movement}
            return self.idempotency.execute(connection, scope=f"targets:transfer:confirm:{transfer_id}", request_key=request_key, request={"transfer_id": transfer_id}, operation=operation)

    def reject_transfer(self, context: AccessContext, transfer_id: str, *, reason: str, request_key: str) -> dict:
        context.require("confirm:transfers")
        reason = (reason or "").strip()
        if not reason:
            raise ValidationError("必须说明驳回原因")
        request_key = _require_request_key(request_key)
        with self.database.transaction() as connection:
            def operation() -> dict:
                row = self._transfer_row(connection, transfer_id)
                if row["state"] != "proposed":
                    raise ConflictError(f"调剂当前状态为 {row['state']}，不能驳回")
                now = self.clock.now()
                connection.execute("UPDATE target_transfers SET state='rejected',decided_by=?,updated_at=? WHERE transfer_id=? AND state='proposed'", (context.actor_id, now, transfer_id))
                self.audit.append(connection, actor_id=context.actor_id, action="transfer.reject", entity_type="target_transfer", entity_id=transfer_id, version=2, detail={"reason": reason})
                return {"transfer_id": transfer_id, "state": "rejected", "reason": reason}
            return self.idempotency.execute(connection, scope=f"targets:transfer:reject:{transfer_id}", request_key=request_key, request={"transfer_id": transfer_id, "reason": reason}, operation=operation)

    def expire_transfer(self, connection: sqlite3.Connection, transfer_id: str, *, actor: str) -> bool:
        """恢复接续使用：把超过确认截止的待确认调剂标记为过期，幂等。"""
        changed = connection.execute("UPDATE target_transfers SET state='expired',decided_by=?,updated_at=? WHERE transfer_id=? AND state='proposed'", (actor, self.clock.now(), transfer_id)).rowcount
        if changed:
            self.audit.append(connection, actor_id=actor, action="transfer.expire", entity_type="target_transfer", entity_id=transfer_id, version=2, detail={"confirm_by": self._transfer_row(connection, transfer_id)["confirm_by"]})
        return changed == 1

    def pending_transfers(self, context: AccessContext, *, fiscal_year: int | None = None) -> list[dict]:
        context.require("read:targets")
        sql = "SELECT * FROM target_transfers WHERE state='proposed'"
        params: list[object] = []
        if fiscal_year is not None:
            sql += " AND fiscal_year=?"; params.append(check_year(fiscal_year))
        sql += " ORDER BY confirm_by, transfer_id"
        with self.database.connect() as connection:
            rows = [dict(row) for row in connection.execute(sql, params)]
        if context.has_scope("*"):
            return rows
        allowed = {scope[len("unit:"):] for scope in context.scopes if scope.startswith("unit:")}
        return [row for row in rows if row["from_unit"] in allowed or row["to_unit"] in allowed]

    # ------------------------------------------------------------------
    # 结转与关账
    # ------------------------------------------------------------------
    def carry_over(self, context: AccessContext, *, fiscal_year: int, program: str, category: str, unit_id: str, amount: object, reason: str, request_key: str) -> dict:
        """把账户目标结转到下一财政年度的同名账户，总量守恒。"""
        context.require("write:targets")
        fiscal_year = check_year(fiscal_year); check_program(program); check_category(category)
        unit_id = require_safe(unit_id, "采购单位")
        require_unit_scope(context, unit_id)
        minor = _positive_minor(amount); reason = (reason or "").strip()
        if not reason:
            raise ValidationError("必须说明结转原因")
        request_key = _require_request_key(request_key)
        request = {"fiscal_year": fiscal_year, "program": program, "category": category, "unit_id": unit_id, "amount_minor": minor, "reason": reason}
        with self.database.transaction() as connection:
            return self.idempotency.execute(connection, scope="targets:carry_over", request_key=request_key, request=request, operation=lambda: self._carry_over(connection, fiscal_year=fiscal_year, program=program, category=category, unit_id=unit_id, amount_minor=minor, reason=reason, actor=context.actor_id, request_key=request_key))

    def _carry_over(self, connection: sqlite3.Connection, *, fiscal_year: int, program: str, category: str, unit_id: str, amount_minor: int, reason: str, actor: str, request_key: str) -> dict:
        account = account_key_of(fiscal_year, program, unit_id, category)
        self._account_row(connection, account)
        next_account = self._ensure_account(connection, fiscal_year + 1, program, unit_id, category)
        movement = self._move(connection, kind="carry_over", fiscal_year=fiscal_year, program=program, category=category, from_account=account, to_account=next_account, amount_minor=amount_minor, reference=f"carry:{account}", reason=reason, actor=actor, request_key=request_key)
        return {"from_account": account, "to_account": next_account, "amount_minor": amount_minor, "movement": movement}

    def close_fiscal_year(self, context: AccessContext, fiscal_year: int, *, request_key: str) -> dict:
        """年度关账：把全部未结转目标余额结转到下一年度并关闭账户。"""
        context.require("write:targets")
        fiscal_year = check_year(fiscal_year)
        request_key = _require_request_key(request_key)
        with self.database.transaction() as connection:
            return self.idempotency.execute(connection, scope=f"targets:close:{fiscal_year}", request_key=request_key, request={"fiscal_year": fiscal_year}, operation=lambda: self._close_fiscal_year(connection, fiscal_year, actor=context.actor_id, request_key=request_key))

    def _close_fiscal_year(self, connection: sqlite3.Connection, fiscal_year: int, *, actor: str, request_key: str) -> dict:
        year_end = parse_instant(f"{fiscal_year + 1}-01-01T00:00:00Z")
        if parse_instant(self.clock.now()) < year_end:
            raise ValidationError("财政年度尚未结束，不能关账")
        earlier = connection.execute("SELECT COUNT(*) AS n FROM target_accounts WHERE fiscal_year<? AND state='open'", (fiscal_year,)).fetchone()
        if int(earlier["n"]) > 0:
            raise ConflictError("存在更早年度尚未关账，请按年度顺序关账")
        rows = connection.execute("SELECT * FROM target_accounts WHERE fiscal_year=? AND state='open' ORDER BY account_key", (fiscal_year,)).fetchall()
        carried = []
        for row in rows:
            key = row["account_key"]
            balance = account_balance(connection, key)
            if balance > 0:
                self._carry_over(connection, fiscal_year=fiscal_year, program=row["program"], category=row["category"], unit_id=row["unit_id"], amount_minor=balance, reason="年度关账结转", actor=actor, request_key=f"{request_key}:{key}")
                carried.append({"account_key": key, "amount_minor": balance})
            connection.execute("UPDATE target_accounts SET state='closed',updated_at=? WHERE account_key=?", (self.clock.now(), key))
        if rows:
            self.audit.append(connection, actor_id=actor, action="targets.close_year", entity_type="fiscal_year", entity_id=str(fiscal_year), version=1, detail={"closed": [row["account_key"] for row in rows], "carried": carried})
        return {"fiscal_year": fiscal_year, "closed_accounts": len(rows), "carried": carried}

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------
    def get_account(self, context: AccessContext, account_key: str) -> dict:
        context.require("read:targets")
        fiscal_year, program, unit_id, category = split_account_key(account_key)
        if unit_id != POOL_UNIT:
            require_unit_scope(context, unit_id)
        elif not context.has_scope("*"):
            raise PermissionDenied("总目标池明细仅汇总岗位可见")
        with self.database.connect() as connection:
            row = self._account_row(connection, account_key)
            result = dict(row)
            result["balance_minor"] = account_balance(connection, account_key)
            row_v = connection.execute("SELECT COALESCE(MAX(version),0) AS v FROM target_versions WHERE account_key=?", (account_key,)).fetchone()
            result["target_version"] = int(row_v["v"])
            return result

    def list_accounts(self, context: AccessContext, *, fiscal_year: int | None = None, program: str | None = None) -> list[dict]:
        context.require("read:targets")
        sql = "SELECT * FROM target_accounts WHERE 1=1"
        params: list[object] = []
        if fiscal_year is not None:
            sql += " AND fiscal_year=?"; params.append(check_year(fiscal_year))
        if program is not None:
            sql += " AND program=?"; params.append(check_program(program))
        sql += " ORDER BY fiscal_year, program, unit_id, category"
        with self.database.connect() as connection:
            rows = [dict(row) for row in connection.execute(sql, params)]
            for row in rows:
                row["balance_minor"] = account_balance(connection, row["account_key"])
        if context.has_scope("*"):
            return rows
        allowed = {scope[len("unit:"):] for scope in context.scopes if scope.startswith("unit:")}
        return [row for row in rows if row["unit_id"] in allowed]

    def target_history(self, context: AccessContext, account_key: str) -> list[dict]:
        """目标换版历史：每次下达、调剂、释放、结转形成的版本链。"""
        context.require("read:targets")
        _, _, unit_id, _ = split_account_key(account_key)
        if unit_id != POOL_UNIT:
            require_unit_scope(context, unit_id)
        with self.database.connect() as connection:
            self._account_row(connection, account_key)
            return [dict(row) for row in connection.execute("SELECT * FROM target_versions WHERE account_key=? ORDER BY version", (account_key,))]

    def movements(self, context: AccessContext, account_key: str) -> list[dict]:
        context.require("read:targets")
        _, _, unit_id, _ = split_account_key(account_key)
        if unit_id != POOL_UNIT:
            require_unit_scope(context, unit_id)
        with self.database.connect() as connection:
            return [dict(row) for row in connection.execute("SELECT * FROM target_movements WHERE from_account=? OR to_account=? ORDER BY occurred_at, movement_id", (account_key, account_key))]
