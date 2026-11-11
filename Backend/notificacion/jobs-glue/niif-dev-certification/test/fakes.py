import itertools
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Sequence

from certification_job.application.ports import LogEvent
from certification_job.domain.models import NotificationKey, ProbeResult, QueryDetails, QueryExecution

BOGOTA = timezone(timedelta(hours=-5))


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
    def __init__(self, documents: Mapping[tuple[str, str], Any]) -> None:
        self._documents = dict(documents)
        self.reads: list[tuple[str, str]] = []

    def load_json(self, bucket: str, key: str) -> Any:
        self.reads.append((bucket, key))
        return self._documents[(bucket, key)]


class FakeQueries:
    def __init__(self, probe: ProbeResult | None = None, details: QueryDetails | None = None) -> None:
        self.runs: list[tuple[str, str]] = []
        self._counter = itertools.count(1)
        self._probe = probe or ProbeResult(row_count=1, has_more=False, column_names=("a", "b"))
        self._details = details or QueryDetails(state="SUCCEEDED", manifest_location="s3://bucket-assets/manifest.csv")

    def run(self, sql: str, database: str) -> QueryExecution:
        self.runs.append((sql, database))
        return QueryExecution(query_execution_id=f"q-{next(self._counter)}", bytes_scanned=100)

    def describe(self, query_execution_id: str) -> QueryDetails:
        return self._details

    def probe(self, query_execution_id: str) -> ProbeResult:
        return self._probe


class FakeObjects:
    def __init__(self, files: Mapping[tuple[str, str], bytes] | None = None) -> None:
        self.files: dict[tuple[str, str], bytes] = dict(files or {})
        self.writes: list[tuple[str, str, bytes, str]] = []

    def list_keys(self, bucket: str, prefix: str) -> list[str]:
        return [key for stored_bucket, key in self.files if stored_bucket == bucket and key.startswith(prefix)]

    def read(self, bucket: str, key: str) -> bytes:
        return self.files[(bucket, key)]

    def write(self, bucket: str, key: str, body: bytes, content_type: str) -> None:
        self.writes.append((bucket, key, body, content_type))


class FakeNotifications:
    def __init__(self, visible_after: int = 1) -> None:
        self.items: list[tuple[str, dict[str, Any]]] = []
        self.searches: list[tuple[str, str, str, str]] = []
        self.pending: list[tuple[str, tuple[NotificationKey, ...], dict[str, str]]] = []
        self._visible_after = visible_after

    def put(self, table: str, item: Mapping[str, Any]) -> None:
        self.items.append((table, dict(item)))

    def find_by_cert_code(self, table: str, index: str, cert_code: str, use_case_id: str) -> Sequence[NotificationKey]:
        self.searches.append((table, index, cert_code, use_case_id))
        if len(self.searches) < self._visible_after:
            return ()
        return tuple(
            NotificationKey(item["useCaseId"], item["executionId"])
            for _, item in self.items
            if item.get("certCode") == cert_code and item["useCaseId"] == use_case_id
        )

    def mark_pending(self, table: str, keys: Sequence[NotificationKey], values: Mapping[str, str]) -> None:
        self.pending.append((table, tuple(keys), dict(values)))


class FixedClock:
    def __init__(self, now: datetime | None = None) -> None:
        self.now = now or datetime(2026, 11, 11, 9, 30, 15, tzinfo=BOGOTA)

    def now_local(self) -> datetime:
        return self.now


class SequentialIds:
    def __init__(self) -> None:
        self._counter = itertools.count(1)

    def execution_id(self) -> str:
        return f"exec-{next(self._counter)}"

    def unique_suffix(self) -> str:
        return "suffix"


class RecordingSleep:
    def __init__(self) -> None:
        self.calls: list[float] = []

    def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)
