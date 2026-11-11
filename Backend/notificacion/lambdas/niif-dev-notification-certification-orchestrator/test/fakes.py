from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Sequence

from builders import CONFIG_BUCKET, GLOBAL_KEY, global_json

from certification_orchestrator.application.ports import Dependencies, LogEvent
from certification_orchestrator.domain.errors import PlatformError
from certification_orchestrator.domain.models import (
    Alert,
    AlertSettings,
    LockAcquisition,
    NotificationRecord,
    RecordKey,
)

BOGOTA = timezone(timedelta(hours=-5))
NOW = datetime(2026, 11, 11, 12, 0, 0, tzinfo=BOGOTA)


class MemoryLog:
    def __init__(self) -> None:
        self.context: dict[str, Any] = {}
        self.records: list[tuple[LogEvent, str, dict[str, Any]]] = []

    def bind(self, **fields: Any) -> None:
        self.context.update(fields)

    def record(self, event: LogEvent, level: str = "INFO", **fields: Any) -> None:
        self.records.append((event, level, {**self.context, **fields}))

    def events(self) -> list[LogEvent]:
        return [event for event, _, _ in self.records]

    def fields(self, event: LogEvent) -> dict[str, Any]:
        return next(fields for recorded, _, fields in self.records if recorded == event)


class FakeConfigs:
    def __init__(self, documents: Mapping[tuple[str, str], Any] | None = None) -> None:
        self.documents = dict(documents or {(CONFIG_BUCKET, GLOBAL_KEY): global_json()})
        self.reads: list[tuple[str, str]] = []

    def load_json(self, bucket: str, key: str) -> Any:
        self.reads.append((bucket, key))
        return self.documents[(bucket, key)]


class FakeNotifications:
    def __init__(self) -> None:
        self.keys_by_cert: dict[str, list[RecordKey]] = {}
        self.pending: list[NotificationRecord] = []
        self.lock_available = True
        self.locks: list[LockAcquisition] = []
        self.released: list[tuple[tuple[RecordKey, ...], str]] = []
        self.status_changes: list[tuple[RecordKey, str, str]] = []
        self.status_change_result = True
        self.alerted: list[tuple[tuple[RecordKey, ...], str]] = []
        self.pending_queries: list[tuple[str, str, str, str]] = []

    def find_keys_by_cert_code(self, table: str, index: str, cert_code: str) -> Sequence[RecordKey]:
        return tuple(self.keys_by_cert.get(cert_code, []))

    def find_pending_before(self, table: str, index: str, status: str, before: str) -> Sequence[NotificationRecord]:
        self.pending_queries.append((table, index, status, before))
        return tuple(self.pending)

    def acquire_lock(self, table: str, acquisition: LockAcquisition) -> bool:
        self.locks.append(acquisition)
        return self.lock_available

    def release_lock(self, table: str, keys: Sequence[RecordKey], token: str) -> None:
        self.released.append((tuple(keys), token))

    def change_status(self, table: str, key: RecordKey, expected: str, new: str) -> bool:
        self.status_changes.append((key, expected, new))
        return self.status_change_result

    def mark_alerted(self, table: str, keys: Sequence[RecordKey], date: str) -> None:
        self.alerted.append((tuple(keys), date))


class FakeObjects:
    def __init__(self, existing: set[tuple[str, str]] | None = None) -> None:
        self.existing = set(existing or set())
        self.copies: list[tuple[str, str, str, str]] = []
        self.deletes: list[tuple[str, str]] = []

    def exists(self, bucket: str, key: str) -> bool:
        return (bucket, key) in self.existing

    def copy(self, source_bucket: str, source_key: str, target_bucket: str, target_key: str) -> None:
        self.copies.append((source_bucket, source_key, target_bucket, target_key))
        self.existing.add((target_bucket, target_key))

    def delete(self, bucket: str, key: str) -> None:
        self.deletes.append((bucket, key))
        self.existing.discard((bucket, key))


class FakeJobs:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.starts: list[tuple[str, dict[str, str]]] = []

    def start(self, job_name: str, arguments: Mapping[str, str]) -> str:
        if self.error is not None:
            raise self.error
        self.starts.append((job_name, dict(arguments)))
        return f"jr-{len(self.starts)}"


class FakeAlerts:
    def __init__(self) -> None:
        self.sent: list[tuple[AlertSettings, Alert]] = []

    def send(self, settings: AlertSettings, alert: Alert) -> None:
        self.sent.append((settings, alert))


class FixedClock:
    def __init__(self, now: datetime = NOW) -> None:
        self.now = now
        self.zones: list[str] = []

    def now_local(self, timezone_name: str) -> datetime:
        self.zones.append(timezone_name)
        return self.now


class World:
    def __init__(self) -> None:
        self.configs = FakeConfigs()
        self.notifications = FakeNotifications()
        self.objects = FakeObjects()
        self.jobs = FakeJobs()
        self.alerts = FakeAlerts()
        self.clock = FixedClock()
        self.log = MemoryLog()

    def dependencies(self) -> Dependencies:
        return Dependencies(
            configs=self.configs,
            notifications=self.notifications,
            objects=self.objects,
            jobs=self.jobs,
            alerts=self.alerts,
            clock=self.clock,
            log=self.log,
        )


def failing_platform() -> PlatformError:
    return PlatformError("ORQ-PLA-001", "Glue no disponible", recoverable=True)
