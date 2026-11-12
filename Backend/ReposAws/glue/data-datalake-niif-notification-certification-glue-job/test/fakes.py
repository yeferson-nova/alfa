from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from certification_notifier.domain.models import CompositeRecord, QueryResult


class FixedClock:
    def __init__(self) -> None:
        self.local = datetime(2026, 11, 4, 10, 0, 0, tzinfo=ZoneInfo("America/Bogota"))
        self.utc = datetime(2026, 11, 4, 15, 0, 0, tzinfo=timezone.utc)

    def local_now(self) -> datetime:
        return self.local

    def utc_now(self) -> datetime:
        return self.utc


class SequentialTokenGenerator:
    def __init__(self) -> None:
        self.counter = 0

    def generate(self, length: int) -> str:
        self.counter += 1
        return str(self.counter).zfill(length)


class InMemoryEventLogger:
    def __init__(self) -> None:
        self.events: list[tuple[object, str, dict]] = []

    def log(self, event, level="INFO", **context) -> None:
        self.events.append((event, level, context))

    def names(self) -> list:
        return [event for event, _, _ in self.events]


class FakeConfigRepository:
    def __init__(self, documents: dict) -> None:
        self.documents = documents
        self.reads: list[tuple[str, str]] = []

    def load_json(self, bucket: str, key: str) -> dict:
        self.reads.append((bucket, key))
        return self.documents[(bucket, key)]

    def load_text(self, bucket: str, key: str) -> str:
        self.reads.append((bucket, key))
        content = self.documents[(bucket, key)]
        if isinstance(content, Exception):
            raise content
        return content


class FakeNotificationRepository:
    def __init__(self, records) -> None:
        self.records = tuple(records)
        self.searches: list[tuple] = []
        self.marked_records: list[dict] = []

    def find_by_cert_code(self, table, index_name, cert_code, use_case_id):
        self.searches.append((table, index_name, cert_code, use_case_id))
        return self.records

    def mark_sent(self, table, records, status, channel, sent_emails, sent_at) -> None:
        self.marked_records.append(
            {
                "table": table,
                "records": tuple(records),
                "status": status,
                "channel": channel,
                "sent_emails": tuple(sent_emails),
                "sent_at": sent_at,
            }
        )


class FakeLinkRepository:
    def __init__(self) -> None:
        self.saved: list[tuple[str, object]] = []

    def save(self, table, link) -> None:
        self.saved.append((table, link))


class FakeQueryRunner:
    def __init__(self, responses: dict[str, QueryResult | Exception]) -> None:
        self.responses = responses
        self.calls: list[tuple] = []

    def run(self, sql, database, query_environment) -> QueryResult:
        self.calls.append((sql, database, query_environment))
        response = self.responses[sql]
        if isinstance(response, Exception):
            raise response
        return response


class FakeNotifier:
    def __init__(self, ids: list[str] | None = None) -> None:
        self.ids = ids
        self.sent: list[tuple] = []

    def send(self, message, recipient) -> str:
        self.sent.append((message, recipient))
        if self.ids is not None:
            return self.ids[len(self.sent) - 1]
        return f"id-{len(self.sent)}"


class FakeCompositeRepository:
    def __init__(self, records: dict[tuple[str, str, str], CompositeRecord] | None = None) -> None:
        self.records = records or {}
        self.consumptions: list[tuple] = []

    def get(self, table, composite_id, member_id):
        return self.records.get((table, composite_id, member_id))

    def consume(self, reservation, message_id, occurred_at) -> None:
        self.consumptions.append((reservation, message_id, occurred_at))
