from dataclasses import dataclass, field
from typing import Any, Mapping


@dataclass(frozen=True)
class RunParameters:
    use_case_id: str
    cert_code: str
    email_type: str
    config_bucket: str
    case_key: str
    global_key: str
    redirect_url: str
    links_table: str
    sender: str
    configuration_set: str | None
    cert_code_index: str
    timezone_name: str
    link_validity_days: int
    link_token_length: int
    poll_interval_seconds: float
    max_query_wait_seconds: float
    regeneration_without_new_data: bool
    execution_id: str


@dataclass(frozen=True)
class Query:
    name: str
    database: str
    sql: str


@dataclass(frozen=True)
class GlobalConfig:
    assets_bucket: str
    notifications_table: str
    catalog: str
    environment_name: str
    athena_output_prefix: str
    sent_status: str


@dataclass(frozen=True)
class CompositeConfig:
    table: str
    group_id: str
    control_id: str
    member_ids: tuple[str, ...]


@dataclass(frozen=True)
class CaseConfig:
    use_case_id: str
    subject_template: str
    string_vars: Mapping[str, str]
    recipients: tuple[str, ...]
    summary_queries: tuple[Query, ...]
    total_queries: tuple[Query, ...]
    presence_queries: tuple[Query, ...]
    composite_config: CompositeConfig | None
    template_path: str | None = None


@dataclass(frozen=True)
class NotificationRecord:
    use_case_id: str
    execution_id: str
    period: str
    status: str
    user_email: str
    query_name: str
    file_name: str
    s3_url: str
    no_data_for_period: bool


@dataclass(frozen=True)
class ShortLink:
    token: str
    url: str
    use_case_id: str
    cert_code: str
    action: str
    created_at: str
    expires_at: int


@dataclass(frozen=True)
class LinkPolicy:
    url_base: str
    table: str
    validity_days: int
    token_length: int


@dataclass(frozen=True)
class QueryEnvironment:
    catalog: str
    output_location: str


@dataclass(frozen=True)
class QueryResult:
    headers: tuple[str, ...]
    rows: tuple[tuple[str, ...], ...]


@dataclass(frozen=True)
class Tables:
    text: str
    html: str


@dataclass(frozen=True)
class Notice:
    tone: str
    text: str


@dataclass(frozen=True)
class EmailMessage:
    subject: str
    text: str
    html: str | None = None
    tags: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class SentEmail:
    recipient: str
    message_id: str


@dataclass(frozen=True)
class CompositeRecord:
    member_id: str
    status: str
    period: Any
    cert_code: str
    source_execution_id: Any
    batch_id: Any
    updated_at: Any


@dataclass(frozen=True)
class CompositeReservation:
    table: str
    composite_id: str
    cert_code: str
    control: CompositeRecord
    members: tuple[CompositeRecord, ...]
    already_completed: bool
