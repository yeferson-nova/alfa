from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class JobParameters:
    period: str
    config_bucket: str
    case_key: str
    global_key: str
    regeneration_without_new_data: bool
    flow_execution_id: str | None
    origin_execution_ids: tuple[str, ...]
    job_run_id: str | None = None


@dataclass(frozen=True)
class WaitPolicy:
    poll_seconds: float
    max_wait_seconds: float


@dataclass(frozen=True)
class RetryPolicy:
    attempts: int
    wait_seconds: float


@dataclass(frozen=True)
class ConsoleSettings:
    base_url: str
    account_id: str
    role_name: str
    region: str


@dataclass(frozen=True)
class ExportSettings:
    content_type: str
    write_bom: bool
    field_delimiter: str


@dataclass(frozen=True)
class CertificationSettings:
    default_user_email: str
    processed_status: str
    pending_status: str
    report_folders: Mapping[str, str]
    athena_wait: WaitPolicy
    index_visibility: RetryPolicy


@dataclass(frozen=True)
class GlobalConfig:
    environment: str
    catalog: str
    timezone: str
    assets_bucket: str
    reports_bucket: str
    notification_table: str
    cert_code_index: str
    athena_output_path: str
    athena_unload_path: str
    console: ConsoleSettings
    export: ExportSettings
    certification: CertificationSettings


@dataclass(frozen=True)
class DetailQuery:
    name: str
    database: str | None
    sql: str


@dataclass(frozen=True)
class HeaderSpec:
    query_name: str
    headers: tuple[str, ...]
    export_disabled: bool


@dataclass(frozen=True)
class CaseConfig:
    use_case_id: str
    email_type: str
    default_database: str | None
    detail_queries: tuple[DetailQuery, ...]
    header_specs: tuple[HeaderSpec, ...]


@dataclass(frozen=True)
class QueryExecution:
    query_execution_id: str
    bytes_scanned: int | None


@dataclass(frozen=True)
class QueryDetails:
    state: str
    manifest_location: str | None


@dataclass(frozen=True)
class ProbeResult:
    row_count: int
    has_more: bool
    column_names: tuple[str, ...]


@dataclass(frozen=True)
class ExportedFile:
    report_key: str
    console_url: str
    file_name: str
    data_rows: int | None
    no_data: bool


@dataclass(frozen=True)
class NotificationKey:
    use_case_id: str
    execution_id: str


@dataclass(frozen=True)
class CertificationResult:
    cert_code: str | None
    files: tuple[ExportedFile, ...]
