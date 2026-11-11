from datetime import datetime
from typing import Mapping
from urllib.parse import quote

from certification_job.domain.errors import ConfigurationError
from certification_job.domain.models import ConsoleSettings

FILE_TIMESTAMP = "%Y%m%d-%H%M%S"
EXCLUDED_UNLOAD_MARKERS = ("manifest", "metadata")
S3_SCHEME = "s3://"


def report_folder(folders: Mapping[str, str], use_case_id: str) -> str:
    if use_case_id not in folders:
        raise ConfigurationError(
            "CER-CFG-003", f"El useCaseId {use_case_id} no tiene carpeta en certification.report_folders"
        )
    return folders[use_case_id]


def report_file_name(period: str, query_name: str, now: datetime) -> str:
    normalized = query_name.replace("#", "_").replace(" ", "_")
    return f"{period}-{normalized}-{now.strftime(FILE_TIMESTAMP)}.csv"


def unload_prefix(unload_path: str, execution_id: str) -> str:
    return f"{unload_path}/{execution_id}/"


def s3_uri(bucket: str, key: str) -> str:
    return f"{S3_SCHEME}{bucket}/{key}"


def split_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith(S3_SCHEME) or "/" not in uri[len(S3_SCHEME):]:
        raise ConfigurationError("CER-CFG-005", f"Ruta S3 invalida: {uri}")
    bucket, key = uri[len(S3_SCHEME):].split("/", 1)
    return bucket, key


def is_unload_data_key(key: str) -> bool:
    lowered = key.lower()
    return not key.endswith("/") and not any(marker in lowered for marker in EXCLUDED_UNLOAD_MARKERS)


def console_url(console: ConsoleSettings, reports_bucket: str, report_key: str) -> str:
    destination = (
        f"https://{console.region}.console.aws.amazon.com/s3/object/{reports_bucket}"
        f"?region={console.region}&prefix={quote(report_key, safe='/')}"
    )
    return (
        f"{console.base_url}?account_id={console.account_id}"
        f"&role_name={console.role_name}&destination={quote(destination, safe='')}"
    )
