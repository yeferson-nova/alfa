from dataclasses import dataclass
from enum import Enum
from typing import Mapping, Sequence

from certification_notifier.application.composite import CompositeService
from certification_notifier.application.queries import QueryService
from certification_notifier.application.links import LinkService
from certification_notifier.application.ports import (
    EventLogger,
    LogEvent,
    Notifier,
    Clock,
    ConfigRepository,
    NotificationRepository,
)
from certification_notifier.domain.config import (
    read_case_config,
    read_global_config,
)
from certification_notifier.domain.constants import (
    ACTION_CERTIFY,
    ACTION_DOWNLOAD,
    ACTION_NO_CERTIFY,
    EMAIL_CHANNEL,
    DETAIL_QUERY_PREFIX,
)
from certification_notifier.domain.email_html import (
    html_fragments,
    template_supports_notices,
    fill_template,
)
from certification_notifier.domain.errors import NotificationError, PlatformError
from certification_notifier.domain.models import (
    Notice,
    CaseConfig,
    GlobalConfig,
    QueryEnvironment,
    SentEmail,
    EmailMessage,
    RunParameters,
    NotificationRecord,
)
from certification_notifier.domain.templating import clean_url
from certification_notifier.domain.wording import (
    notice_from_records,
    email_notices,
    compose_text,
    build_subject,
    build_body,
    format_files,
    file_name_of,
    base_variables,
    final_variables,
)
from certification_notifier.domain.paths import normalize_key, query_output_path

IDS_SEPARATOR = ","
USE_CASE_TAG = "useCaseId"
EMAIL_TYPE_TAG = "emailType"
CERTIFY_LINK_VARIABLE = "certificarLink"
NO_CERTIFY_LINK_VARIABLE = "nocertificarLink"
USE_CASE_NAME_VARIABLE = "useCaseName"


class JobResult(str, Enum):
    NO_RECORDS = "SIN_REGISTROS"
    ALREADY_SENT = "YA_ENVIADO"
    ALREADY_COMPLETED = "YA_COMPLETADO"
    SENT = "ENVIADO"


@dataclass(frozen=True)
class Dependencies:
    config: ConfigRepository
    notifications: NotificationRepository
    notifier: Notifier
    queries: QueryService
    links: LinkService
    composite: CompositeService
    clock: Clock
    logger: EventLogger


class NotifyCertification:
    def __init__(self, parameters: RunParameters, dependencies: Dependencies) -> None:
        self._parameters = parameters
        self._dependencies = dependencies

    def run(self) -> JobResult:
        logger = self._dependencies.logger
        logger.log(
            LogEvent.JOB_START, use_case_id=self._parameters.use_case_id, cert_code=self._parameters.cert_code
        )
        global_config, case_config = self._load_configs()
        records = self._dependencies.notifications.find_by_cert_code(
            global_config.notifications_table,
            self._parameters.cert_code_index,
            self._parameters.cert_code,
            self._parameters.use_case_id,
        )
        skip = self._skip_reason(records, global_config)
        if skip is not None:
            return skip
        result = self._notify(global_config, case_config, records)
        logger.log(LogEvent.JOB_END, result=result.value)
        return result

    def _load_configs(self) -> tuple[GlobalConfig, CaseConfig]:
        parameters = self._parameters
        repository = self._dependencies.config
        global_config = read_global_config(
            repository.load_json(parameters.config_bucket, parameters.global_key)
        )
        case_config = read_case_config(
            repository.load_json(parameters.config_bucket, parameters.case_key),
            parameters.use_case_id,
        )
        self._dependencies.logger.log(LogEvent.CONFIG_LOADED)
        return global_config, case_config

    def _skip_reason(
        self,
        records: Sequence[NotificationRecord],
        global_config: GlobalConfig,
    ) -> JobResult | None:
        logger = self._dependencies.logger
        if not records:
            logger.log(LogEvent.NO_RECORDS, level="WARN", cert_code=self._parameters.cert_code)
            return JobResult.NO_RECORDS
        if all(record.status == global_config.sent_status for record in records):
            logger.log(LogEvent.ALREADY_SENT, cert_code=self._parameters.cert_code)
            return JobResult.ALREADY_SENT
        return None

    def _notify(
        self,
        global_config: GlobalConfig,
        case_config: CaseConfig,
        records: Sequence[NotificationRecord],
    ) -> JobResult:
        parameters = self._parameters
        reservation = self._dependencies.composite.prepare(
            case_config.composite_config,
            records[0].period,
            parameters.cert_code,
            parameters.regeneration_without_new_data,
        )
        if reservation is not None and reservation.already_completed:
            self._dependencies.logger.log(LogEvent.COMPOSITE_ALREADY_COMPLETED, cert_code=parameters.cert_code)
            return JobResult.ALREADY_COMPLETED
        message = self._build_message(global_config, case_config, records)
        sent_emails = self._send(message, case_config.recipients)
        self._mark_sent(global_config, records, sent_emails)
        identifiers = IDS_SEPARATOR.join(sent_email.message_id for sent_email in sent_emails)
        self._dependencies.composite.close(reservation, identifiers)
        return JobResult.SENT

    def _build_message(
        self,
        global_config: GlobalConfig,
        case_config: CaseConfig,
        records: Sequence[NotificationRecord],
    ) -> EmailMessage:
        parameters = self._parameters
        queries = self._dependencies.queries
        first = records[0]
        base = base_variables(global_config.environment_name, case_config, first, parameters.cert_code)
        final_vars = final_variables(base, case_config.string_vars)
        query_environment = self._query_environment(global_config)
        summary = queries.tables(case_config.summary_queries, base, query_environment)
        totals = queries.tables(case_config.total_queries, base, query_environment)
        member_notice = queries.availability(case_config.presence_queries, base, query_environment)
        certify_link = self._link(ACTION_CERTIFY, final_vars.get(CERTIFY_LINK_VARIABLE, ""))
        no_certify_link = self._link(ACTION_NO_CERTIFY, final_vars.get(NO_CERTIFY_LINK_VARIABLE, ""))
        files = self._files(records)
        email_body = build_body(
            final_vars.get(USE_CASE_NAME_VARIABLE, parameters.use_case_id),
            summary.text,
            totals.text,
            format_files(files),
            certify_link,
            no_certify_link,
        )
        notice = member_notice if member_notice is not None else notice_from_records(records, first.period)
        regenerated = parameters.regeneration_without_new_data
        fragments = html_fragments(summary.html, totals.html, files, certify_link, no_certify_link)
        return EmailMessage(
            subject=build_subject(case_config.subject_template, final_vars, regenerated),
            text=compose_text(email_body, notice, regenerated),
            html=self._html(
                global_config, case_config, final_vars, fragments, email_notices(notice, regenerated)
            ),
            tags={USE_CASE_TAG: parameters.use_case_id, EMAIL_TYPE_TAG: parameters.email_type},
        )

    @staticmethod
    def _query_environment(global_config: GlobalConfig) -> QueryEnvironment:
        return QueryEnvironment(
            catalog=global_config.catalog,
            output_location=query_output_path(
                global_config.assets_bucket, global_config.athena_output_prefix
            ),
        )

    def _html(
        self,
        global_config: GlobalConfig,
        case_config: CaseConfig,
        variables: Mapping[str, str],
        fragments: Mapping[str, str],
        notices: Sequence[Notice],
    ) -> str | None:
        path = case_config.template_path
        if not path:
            return None
        bucket = global_config.assets_bucket
        normalize_key(bucket, path)
        logger = self._dependencies.logger
        try:
            template = self._dependencies.config.load_text(bucket, path)
        except NotificationError as error:
            logger.log(LogEvent.TEMPLATE_NOT_READ, level="WARN", path=path, **error.as_dict())
            return None
        if not template_supports_notices(template, notices):
            logger.log(LogEvent.TEMPLATE_WITHOUT_MARKER, level="WARN", path=path)
            return None
        return fill_template(template, variables, fragments, notices)

    def _link(self, action: str, url: str) -> str:
        return self._dependencies.links.create(action, clean_url(url))

    def _files(self, records: Sequence[NotificationRecord]) -> list[tuple[str, str]]:
        details = sorted(
            (record for record in records if record.query_name.startswith(DETAIL_QUERY_PREFIX)),
            key=lambda record: record.query_name,
        )
        return [(file_name_of(record), self._link(ACTION_DOWNLOAD, record.s3_url)) for record in details]

    def _send(self, message: EmailMessage, recipients: Sequence[str]) -> tuple[SentEmail, ...]:
        logger = self._dependencies.logger
        total = len(recipients)
        sent_emails: list[SentEmail] = []
        last_error: NotificationError | None = None
        for position, recipient in enumerate(recipients, start=1):
            try:
                message_id = self._send_one(message, recipient)
            except NotificationError as error:
                last_error = error
                logger.log(
                    LogEvent.SEND_FAILED, level="ERROR", position=position, total=total, **error.as_dict()
                )
                continue
            sent_emails.append(SentEmail(recipient=recipient, message_id=message_id))
            logger.log(LogEvent.EMAIL_SENT, position=position, total=total, message_id=message_id)
        if not sent_emails and last_error is not None:
            raise last_error
        if len(sent_emails) < total:
            logger.log(
                LogEvent.PARTIAL_SEND, level="WARN", sent=len(sent_emails), failed=total - len(sent_emails)
            )
        return tuple(sent_emails)

    def _send_one(self, message: EmailMessage, recipient: str) -> str:
        message_id = self._dependencies.notifier.send(message, recipient)
        if not message_id:
            raise PlatformError("NTF-PLA-003", "El servicio de correo no confirmo el identificador del mensaje")
        return message_id

    def _mark_sent(
        self,
        global_config: GlobalConfig,
        records: Sequence[NotificationRecord],
        sent_emails: Sequence[SentEmail],
    ) -> None:
        self._dependencies.notifications.mark_sent(
            global_config.notifications_table,
            records,
            global_config.sent_status,
            EMAIL_CHANNEL,
            sent_emails,
            self._dependencies.clock.local_now().isoformat(),
        )
        self._dependencies.logger.log(LogEvent.RECORDS_MARKED, count=len(records))
