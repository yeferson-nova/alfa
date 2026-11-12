from string import Template
from typing import Mapping, Sequence

from certification_notifier.domain.constants import TONE_WARNING, TONE_INFO
from certification_notifier.domain.models import Notice, CaseConfig, NotificationRecord
from certification_notifier.domain.templating import substitute_vars
from certification_notifier.domain.period import period_range
from certification_notifier.domain.texts import (
    NOTICE_OTHER_MEMBERS,
    NOTICE_NO_MEMBER,
    REGENERATION_NOTICE,
    NO_DATA_PARTIAL_NOTICE,
    NO_DATA_TOTAL_NOTICE,
    AVAILABILITY_HEADER,
    MEMBER_NO_DATA_LINE,
    BODY_TEMPLATE,
    REGENERATED_SUBJECT_PREFIX,
    DEFAULT_FILE_NAME,
    NO_FILES_TEXT,
    NO_SUMMARY_TEXT,
    NO_TOTALS_TEXT,
)

NOTICE_SEPARATOR = "\n\n"


def base_variables(
    environment_name: str,
    case_config: CaseConfig,
    record: NotificationRecord,
    cert_code: str,
) -> dict[str, str]:
    return {
        "env": environment_name,
        "period": record.period,
        "periodRange": period_range(record.period),
        "useCaseId": case_config.use_case_id,
        "useCaseName": case_config.string_vars.get("useCaseName", case_config.use_case_id),
        "certCode": cert_code,
        "userEmail": record.user_email,
        "executionId": record.execution_id,
    }


def final_variables(base: Mapping[str, str], string_vars: Mapping[str, str]) -> dict[str, str]:
    resolved = {key: substitute_vars(value, base) for key, value in string_vars.items()}
    return {**base, **resolved}


def build_subject(template: str, variables: Mapping[str, str], regenerated: bool) -> str:
    subject = Template(template).safe_substitute(variables)
    subject = subject.replace("\n", " ").replace("\r", " ")
    return REGENERATED_SUBJECT_PREFIX + subject if regenerated else subject


def file_name_of(record: NotificationRecord) -> str:
    return record.file_name or DEFAULT_FILE_NAME


def format_files(entries: Sequence[tuple[str, str]]) -> str:
    if not entries:
        return NO_FILES_TEXT
    return "\n".join(f"- {name}\n  {link}" for name, link in entries)


def build_body(
    use_case_name: str,
    summary: str,
    totals: str,
    files: str,
    certify_link: str,
    no_certify_link: str,
) -> str:
    return BODY_TEMPLATE.format(
        use_case_name=use_case_name,
        summary=summary or NO_SUMMARY_TEXT,
        totals=totals or NO_TOTALS_TEXT,
        files=files,
        certify_link=certify_link,
        no_certify_link=no_certify_link,
    )


def notice_from_records(records: Sequence[NotificationRecord], period: str) -> str:
    empty_members = [record for record in records if record.no_data_for_period]
    if not empty_members:
        return ""
    if len(empty_members) == len(records):
        return NO_DATA_TOTAL_NOTICE.format(period=period)
    queries = ", ".join(record.query_name for record in empty_members)
    return NO_DATA_PARTIAL_NOTICE.format(period=period, queries=queries)


def availability_notice(period: str, empty_members: Sequence[str], total_members: int) -> str:
    if not empty_members:
        return ""
    lines = [AVAILABILITY_HEADER]
    lines.extend(MEMBER_NO_DATA_LINE.format(name=name, period=period) for name in empty_members)
    lines.append(NOTICE_NO_MEMBER if len(empty_members) == total_members else NOTICE_OTHER_MEMBERS)
    return "\n".join(lines)


def email_notices(notice: str, regenerated: bool) -> list[Notice]:
    notices = [Notice(tone=TONE_INFO, text=REGENERATION_NOTICE)] if regenerated else []
    if notice:
        notices.append(Notice(tone=TONE_WARNING, text=notice))
    return notices


def compose_text(email_body: str, notice: str, regenerated: bool) -> str:
    text = email_body
    if notice:
        text = notice + NOTICE_SEPARATOR + text
    if regenerated:
        text = REGENERATION_NOTICE + NOTICE_SEPARATOR + text
    return text
