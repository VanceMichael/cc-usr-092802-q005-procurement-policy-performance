"""应用装配。"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .audit import AuditLog
from .database import Database
from .fulfillment import FulfillmentService, pipeline_counters
from .idempotency import IdempotencyStore
from .inbox import Inbox
from .jobs import JobQueue
from .ledger import Ledger
from .outbox import Outbox
from .rectifications import RectificationService
from .repository import EntityRepository
from .reservations import ReservationBook
from .stage_results import StageResultService
from .targets import TargetLedger
from .timeutil import Clock


@dataclass(frozen=True)
class CivicFlow:
    database: Database
    clock: Clock
    repository: EntityRepository
    inbox: Inbox
    outbox: Outbox
    ledger: Ledger
    reservations: ReservationBook
    jobs: JobQueue
    targets: TargetLedger
    fulfillment: FulfillmentService
    stage_results: StageResultService
    rectifications: RectificationService

    @classmethod
    def open(cls, path: str | Path, *, fixed_now: str | None = None) -> "CivicFlow":
        database = Database(path); database.initialize(); clock = Clock(fixed_now)
        audit = AuditLog(clock); idempotency = IdempotencyStore(clock)
        repository = EntityRepository(database, clock, audit, idempotency)
        inbox = Inbox(database, clock); outbox = Outbox(database, clock)
        ledger = Ledger(database, clock); reservations = ReservationBook(database)
        jobs = JobQueue(database, clock)
        fulfillment = FulfillmentService(database, clock, audit, idempotency)
        targets = TargetLedger(database, clock, audit, idempotency, jobs, pipeline_fn=pipeline_counters)
        stage_results = StageResultService(database, clock, audit, idempotency)
        rectifications = RectificationService(database, clock, audit, idempotency, jobs, outbox, targets)
        return cls(database, clock, repository, inbox, outbox, ledger, reservations, jobs, targets, fulfillment, stage_results, rectifications)

    def verify(self) -> dict:
        with self.database.connect() as connection:
            audit_count = AuditLog(self.clock).verify(connection)
            entity_count = connection.execute("SELECT COUNT(*) AS n FROM entities").fetchone()["n"]
            conflict_count = connection.execute("SELECT COUNT(*) AS n FROM inbox_conflicts").fetchone()["n"]
            movement_count = connection.execute("SELECT COUNT(*) AS n FROM target_movements").fetchone()["n"]
            event_count = connection.execute("SELECT COUNT(*) AS n FROM fulfillment_events").fetchone()["n"]
            quarantine_count = connection.execute("SELECT COUNT(*) AS n FROM fulfillment_quarantine").fetchone()["n"]
        return {"audit_entries": audit_count, "entities": entity_count, "inbox_conflicts": conflict_count, "target_movements": movement_count, "fulfillment_events": event_count, "fulfillment_quarantine": quarantine_count}
