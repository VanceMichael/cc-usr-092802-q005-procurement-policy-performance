"""SQLite 连接、事务和数据库初始化。"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


SCHEMA = r"""
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS entities (
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    state TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    created_by TEXT NOT NULL,
    updated_by TEXT NOT NULL,
    PRIMARY KEY(entity_type, entity_id)
);
CREATE TABLE IF NOT EXISTS entity_versions (
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    state TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    valid_from TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    request_key TEXT NOT NULL,
    PRIMARY KEY(entity_type, entity_id, version)
);
CREATE INDEX IF NOT EXISTS entity_versions_asof ON entity_versions(entity_type, entity_id, valid_from, version);
CREATE TABLE IF NOT EXISTS idempotency_keys (
    scope TEXT NOT NULL,
    request_key TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(scope, request_key)
);
CREATE TABLE IF NOT EXISTS audit_entries (
    audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
    occurred_at TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    action TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    detail_json TEXT NOT NULL,
    previous_digest TEXT NOT NULL,
    entry_digest TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS inbox_messages (
    source TEXT NOT NULL,
    source_key TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    payload_digest TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    received_at TEXT NOT NULL,
    status TEXT NOT NULL,
    PRIMARY KEY(source, source_key, sequence)
);
CREATE TABLE IF NOT EXISTS inbox_conflicts (
    conflict_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    source_key TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    existing_digest TEXT NOT NULL,
    incoming_digest TEXT NOT NULL,
    received_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS outbox_messages (
    message_id TEXT PRIMARY KEY,
    topic TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    available_at TEXT NOT NULL,
    lease_until TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    delivered_at TEXT
);
CREATE INDEX IF NOT EXISTS outbox_ready ON outbox_messages(status, available_at, lease_until);
CREATE TABLE IF NOT EXISTS journal_entries (
    entry_id TEXT PRIMARY KEY,
    journal_key TEXT NOT NULL,
    account TEXT NOT NULL,
    currency TEXT NOT NULL,
    amount_minor INTEGER NOT NULL,
    direction TEXT NOT NULL,
    reference TEXT NOT NULL,
    reversed_entry_id TEXT,
    occurred_at TEXT NOT NULL,
    posted_by TEXT NOT NULL,
    FOREIGN KEY(reversed_entry_id) REFERENCES journal_entries(entry_id)
);
CREATE INDEX IF NOT EXISTS journal_reference ON journal_entries(journal_key, reference, occurred_at);
CREATE TABLE IF NOT EXISTS resource_reservations (
    reservation_id TEXT PRIMARY KEY,
    resource_id TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    start_at TEXT NOT NULL,
    end_at TEXT NOT NULL,
    status TEXT NOT NULL,
    version INTEGER NOT NULL,
    created_by TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS reservation_window ON resource_reservations(resource_id, start_at, end_at, status);
CREATE TABLE IF NOT EXISTS scheduled_jobs (
    job_id TEXT PRIMARY KEY,
    job_type TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    run_at TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 0,
    lease_until TEXT,
    last_error TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS jobs_due ON scheduled_jobs(status, run_at, lease_until);
CREATE TABLE IF NOT EXISTS target_accounts (
    account_key TEXT PRIMARY KEY,
    fiscal_year INTEGER NOT NULL,
    program TEXT NOT NULL,
    unit_id TEXT NOT NULL,
    category TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS target_accounts_year ON target_accounts(fiscal_year, program, unit_id, category, state);
CREATE TABLE IF NOT EXISTS target_movements (
    movement_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    fiscal_year INTEGER NOT NULL,
    program TEXT NOT NULL,
    category TEXT NOT NULL,
    from_account TEXT NOT NULL,
    to_account TEXT NOT NULL,
    amount_minor INTEGER NOT NULL,
    reference TEXT NOT NULL,
    reason TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    request_key TEXT NOT NULL,
    occurred_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS movement_from ON target_movements(from_account, occurred_at);
CREATE INDEX IF NOT EXISTS movement_to ON target_movements(to_account, occurred_at);
CREATE TABLE IF NOT EXISTS target_versions (
    account_key TEXT NOT NULL,
    version INTEGER NOT NULL,
    amount_minor INTEGER NOT NULL,
    kind TEXT NOT NULL,
    reference TEXT NOT NULL,
    reason TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    request_key TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(account_key, version)
);
CREATE TABLE IF NOT EXISTS target_transfers (
    transfer_id TEXT PRIMARY KEY,
    fiscal_year INTEGER NOT NULL,
    program TEXT NOT NULL,
    category TEXT NOT NULL,
    from_unit TEXT NOT NULL,
    to_unit TEXT NOT NULL,
    amount_minor INTEGER NOT NULL,
    reason TEXT NOT NULL,
    state TEXT NOT NULL,
    proposed_by TEXT NOT NULL,
    decided_by TEXT,
    confirm_by TEXT NOT NULL,
    request_key TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS transfer_pending ON target_transfers(state, confirm_by);
CREATE TABLE IF NOT EXISTS ratio_rules (
    rule_id TEXT PRIMARY KEY,
    program TEXT NOT NULL,
    category TEXT NOT NULL,
    version INTEGER NOT NULL,
    min_ratio_bp INTEGER NOT NULL,
    catalog_json TEXT NOT NULL,
    effective_from TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    request_key TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(program, category, version)
);
CREATE INDEX IF NOT EXISTS rule_effective ON ratio_rules(program, category, effective_from);
CREATE TABLE IF NOT EXISTS fulfillment_events (
    event_id TEXT PRIMARY KEY,
    account_key TEXT NOT NULL,
    seq INTEGER NOT NULL,
    kind TEXT NOT NULL,
    amount_minor INTEGER NOT NULL,
    reference TEXT NOT NULL,
    rule_id TEXT,
    detail_json TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    request_key TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    UNIQUE(account_key, seq)
);
CREATE INDEX IF NOT EXISTS event_account ON fulfillment_events(account_key, seq);
CREATE TABLE IF NOT EXISTS fulfillment_receipts (
    source TEXT NOT NULL,
    receipt_no TEXT NOT NULL,
    payload_digest TEXT NOT NULL,
    status TEXT NOT NULL,
    result_json TEXT,
    first_seen_at TEXT NOT NULL,
    PRIMARY KEY(source, receipt_no)
);
CREATE TABLE IF NOT EXISTS fulfillment_quarantine (
    quarantine_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    receipt_no TEXT NOT NULL,
    existing_digest TEXT NOT NULL,
    incoming_digest TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    reason TEXT NOT NULL,
    received_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS exception_requests (
    exception_id TEXT PRIMARY KEY,
    account_key TEXT NOT NULL,
    amount_minor INTEGER NOT NULL,
    reason TEXT NOT NULL,
    state TEXT NOT NULL,
    requested_by TEXT NOT NULL,
    decided_by TEXT,
    decision_reason TEXT,
    request_key TEXT NOT NULL,
    created_at TEXT NOT NULL,
    decided_at TEXT
);
CREATE TABLE IF NOT EXISTS stage_reports (
    report_id TEXT PRIMARY KEY,
    fiscal_year INTEGER NOT NULL,
    program TEXT NOT NULL,
    unit_id TEXT NOT NULL,
    stage TEXT NOT NULL,
    version INTEGER NOT NULL,
    corrects_report_id TEXT,
    snapshot_json TEXT NOT NULL,
    state TEXT NOT NULL,
    published_by TEXT NOT NULL,
    published_at TEXT NOT NULL,
    request_key TEXT NOT NULL,
    UNIQUE(fiscal_year, program, unit_id, stage, version)
);
CREATE TABLE IF NOT EXISTS rectifications (
    rectification_id TEXT PRIMARY KEY,
    account_key TEXT NOT NULL,
    gap_minor INTEGER NOT NULL,
    assignee TEXT NOT NULL,
    due_at TEXT NOT NULL,
    state TEXT NOT NULL,
    reminded_at TEXT,
    created_by TEXT NOT NULL,
    request_key TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS rectification_open ON rectifications(state, due_at);
"""


class Database:
    def __init__(self, path: str | Path):
        self.path = str(path)

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA busy_timeout=30000")
        return connection

    def initialize(self) -> None:
        with self.connect() as connection:
            connection.executescript(SCHEMA)

    @contextmanager
    def transaction(self, *, immediate: bool = True) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()
