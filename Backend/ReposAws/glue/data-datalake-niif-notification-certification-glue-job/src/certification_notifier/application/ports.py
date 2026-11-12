from datetime import datetime
from enum import Enum
from typing import Any, Mapping, Protocol, Sequence

from certification_notifier.domain.models import (
    ShortLink,
    QueryEnvironment,
    SentEmail,
    EmailMessage,
    CompositeRecord,
    NotificationRecord,
    CompositeReservation,
    QueryResult,
)


class LogEvent(str, Enum):
    JOB_START = "NTF_INICIO_JOB"
    CONFIG_LOADED = "NTF_CONFIGURACION_CARGADA"
    NO_RECORDS = "NTF_SIN_REGISTROS"
    ALREADY_SENT = "NTF_YA_ENVIADO"
    COMPOSITE_ALREADY_COMPLETED = "NTF_COMPUESTO_YA_COMPLETADO"
    QUERY_EXECUTED = "NTF_CONSULTA_EJECUTADA"
    QUERY_FAILED = "NTF_CONSULTA_FALLIDA"
    LINK_CREATED = "NTF_ENLACE_CREADO"
    EMAIL_SENT = "NTF_CORREO_ENVIADO"
    RECORDS_MARKED = "NTF_REGISTROS_MARCADOS"
    COMPOSITE_CLOSED = "NTF_COMPUESTO_CERRADO"
    TEMPLATE_NOT_READ = "NTF_PLANTILLA_NO_LEIDA"
    TEMPLATE_WITHOUT_MARKER = "NTF_PLANTILLA_SIN_MARCADOR"
    SEND_FAILED = "NTF_ENVIO_FALLIDO"
    PARTIAL_SEND = "NTF_ENVIO_PARCIAL"
    JOB_END = "NTF_FIN_JOB"
    JOB_ERROR = "NTF_ERROR_JOB"


class ConfigRepository(Protocol):
    def load_json(self, bucket: str, key: str) -> Mapping[str, Any]: ...

    def load_text(self, bucket: str, key: str) -> str: ...


class NotificationRepository(Protocol):
    def find_by_cert_code(
        self, table: str, index_name: str, cert_code: str, use_case_id: str
    ) -> tuple[NotificationRecord, ...]: ...

    def mark_sent(
        self,
        table: str,
        records: Sequence[NotificationRecord],
        status: str,
        channel: str,
        sent_emails: Sequence[SentEmail],
        sent_at: str,
    ) -> None: ...


class LinkRepository(Protocol):
    def save(self, table: str, link: ShortLink) -> None: ...


class AnalyticsQueryRunner(Protocol):
    def run(self, sql: str, database: str, query_environment: QueryEnvironment) -> QueryResult: ...


class Notifier(Protocol):
    def send(self, message: EmailMessage, recipient: str) -> str: ...


class CompositeRepository(Protocol):
    def get(self, table: str, composite_id: str, member_id: str) -> CompositeRecord | None: ...

    def consume(self, reservation: CompositeReservation, message_id: str, occurred_at: str) -> None: ...


class Clock(Protocol):
    def local_now(self) -> datetime: ...

    def utc_now(self) -> datetime: ...


class TokenGenerator(Protocol):
    def generate(self, length: int) -> str: ...


class EventLogger(Protocol):
    def log(self, event: LogEvent, level: str = "INFO", **context: Any) -> None: ...
