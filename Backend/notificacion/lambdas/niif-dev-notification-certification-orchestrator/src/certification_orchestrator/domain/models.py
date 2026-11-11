from dataclasses import dataclass
from enum import Enum
from typing import Mapping


class Action(str, Enum):
    LAUNCH_NOTIFICATION = "LANZAR_NOTIFICACION"
    TRANSPORT = "TRANSPORTAR"
    ARCHIVE_REJECTED = "ARCHIVAR_NO_CERTIFICADO"
    IGNORE = "IGNORAR"


@dataclass(frozen=True)
class RecordKey:
    use_case_id: str
    execution_id: str


@dataclass(frozen=True)
class SendLock:
    token: str | None = None
    date: str | None = None
    attempts: int = 0
    alert_date: str | None = None


@dataclass(frozen=True)
class NotificationRecord:
    key: RecordKey
    status: str
    cert_code: str | None
    name_file: str | None
    email_type: str | None
    config_bucket: str | None
    config_key: str | None
    global_config_key: str | None
    regeneration_without_new_data: bool
    flow_execution_id: str | None
    period: str | None
    lock: SendLock


@dataclass(frozen=True)
class StreamChange:
    sequence_number: str
    event_name: str
    old_status: str | None
    record: NotificationRecord


@dataclass(frozen=True)
class ConfigPointer:
    bucket: str
    key: str


@dataclass(frozen=True)
class StatusNames:
    pending: str
    certificate: str
    no_certificate: str
    transport: str
    no_transport: str


@dataclass(frozen=True)
class OrchestratorSettings:
    notification_job_name: str
    transport_bucket: str
    transport_prefix: str
    rejected_folder: str
    use_historic_source_path: bool
    reprocess_after_minutes: int
    reprocess_max_attempts: int


@dataclass(frozen=True)
class AlertSettings:
    sender: str
    configuration_set: str | None
    recipients: tuple[str, ...]


@dataclass(frozen=True)
class GlobalConfig:
    environment: str
    timezone: str
    reports_bucket: str
    notification_table: str
    cert_code_index: str
    pending_index: str
    report_folders: Mapping[str, str]
    statuses: StatusNames
    orchestrator: OrchestratorSettings
    alerts: AlertSettings


@dataclass(frozen=True)
class LockRequest:
    key: RecordKey
    current_token: str | None


@dataclass(frozen=True)
class LockAcquisition:
    requests: tuple[LockRequest, ...]
    token: str
    date: str
    required_status: str | None

    @property
    def keys(self) -> tuple[RecordKey, ...]:
        return tuple(request.key for request in self.requests)


@dataclass(frozen=True)
class Alert:
    subject: str
    body: str


@dataclass(frozen=True)
class BatchResult:
    failures: tuple[str, ...]
    processed: int
    ignored: int


@dataclass(frozen=True)
class ReprocessResult:
    relaunched: int
    skipped: int
    alerted: int
    failed: int
