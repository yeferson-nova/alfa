import copy
from typing import Any

from certification_job.domain.configuration import read_global_config
from certification_job.domain.models import GlobalConfig, JobParameters

GLOBAL_CONFIG: dict[str, Any] = {
    "environment": "dev",
    "catalog": "AwsDataCatalog",
    "timezone": "America/Bogota",
    "buckets": {"assets": "bucket-assets", "reports": "bucket-reports"},
    "dynamodb": {"notification_table": "table-notifications", "cert_code_index": "index-cert"},
    "athena": {"output_path": "athena/out", "unload_path": "athena/unload"},
    "console_url": {
        "base_url": "https://sso.example/start/#/console",
        "account_id": "ACCOUNT",
        "role_name": "ROLE",
        "region": "region-1",
    },
    "s3_export": {"csv_content_type": "text/csv; charset=utf-8", "csv_encoding_bom": True, "field_delimiter": "|"},
    "certification": {
        "default_user_email": "SYSTEM_GLUE_JOB",
        "processed_status": "PROCESSED",
        "pending_status": "PENDING",
        "report_folders": {"caseA": "folderA", "reserva": "folderReserva"},
        "athena_poll_seconds": 2,
        "athena_max_wait_seconds": 10,
        "index_visibility_attempts": 3,
        "index_visibility_wait_seconds": 2,
    },
}

DETAIL_SQL = (
    "UNLOAD (SELECT a, b FROM t WHERE p = '$period') "
    "TO 's3://$assets_bucket/$unload_path/$execution_id/' WITH (format = 'TEXTFILE')"
)

CASE_CONFIG: dict[str, Any] = {
    "useCaseId": "caseA",
    "emailType": "CERT",
    "athena": {"defaultDatabase": "db-default"},
    "queries": {
        "detail": [
            {"queryName": "detail#2", "database": "db_${env}", "query": DETAIL_SQL},
            {"queryName": "detail#1", "query": DETAIL_SQL},
        ]
    },
    "email": {
        "tableVars": {
            "tableDetail": {
                "queryName": "detail",
                "export": True,
                "columns": {"a": {"name": "Columna A"}, "b": "ignored"},
            },
            "tableTotal": {"queryName": "total", "columns": {"x": {}}},
        }
    },
}


def global_data(**changes: Any) -> dict[str, Any]:
    data = copy.deepcopy(GLOBAL_CONFIG)
    data.update(changes)
    return data


def case_data(**changes: Any) -> dict[str, Any]:
    data = copy.deepcopy(CASE_CONFIG)
    data.update(changes)
    return data


def global_config(**changes: Any) -> GlobalConfig:
    return read_global_config(global_data(**changes))


def parameters(**changes: Any) -> JobParameters:
    base: dict[str, Any] = {
        "period": "202606",
        "config_bucket": "bucket-config",
        "case_key": "cases/caseA.json",
        "global_key": "global.json",
        "regeneration_without_new_data": False,
        "flow_execution_id": "primasArl-202606-flow",
        "origin_execution_ids": (),
        "job_run_id": "jr_1",
    }
    base.update(changes)
    return JobParameters(**base)
