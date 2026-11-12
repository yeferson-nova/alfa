import copy
from typing import Any

from certification_notifier.domain.models import (
    RunParameters,
    CompositeRecord,
    NotificationRecord,
    QueryResult,
)

GLOBAL_CONFIG_DATA: dict[str, Any] = {
    "buckets": {"assets": "bucket-activos"},
    "dynamodb": {"notification_table": "tabla-notificaciones"},
    "environment": "dev",
    "athena": {"output_path": "athena/salida"},
    "certification": {"sent_status": "SENT"},
}

CASE_CONFIG_DATA: dict[str, Any] = {
    "useCaseId": "casoA",
    "email": {
        "subject": "NIIF 17 - Certificacion ${useCaseName} Periodo ${period}",
        "stringVars": {
            "useCaseName": "Caso A",
            "certificarLink": "https://api.example/certify?c=${certCode}&amp;p=${period}",
            "nocertificarLink": "https://api.example/no?c=${certCode}",
        },
        "destinations": ["Uno@Example.com", " dos@example.com ", "uno@example.com"],
    },
    "queries": {
        "summary": [
            {"queryName": "b_resumen", "database": "db_${env}", "query": "SELECT 'b' FROM t WHERE p='${period}'"},
            {"queryName": "a_resumen", "database": "db_${env}", "query": "SELECT 'a' FROM t WHERE p='${period}'"},
        ],
        "total": [
            {"queryName": "total", "database": "db_${env}", "query": "SELECT 'total' FROM t WHERE p='${period}'"},
        ],
    },
}

DEFAULT_RESPONSES: dict[str, QueryResult] = {
    "SELECT 'a' FROM t WHERE p='202610'": QueryResult(("periodo", "total"), (("202610", "5"),)),
    "SELECT 'b' FROM t WHERE p='202610'": QueryResult(("periodo", "total"), (("202610", "7"),)),
    "SELECT 'total' FROM t WHERE p='202610'": QueryResult(("registros", "valor"), (("12", "900"),)),
}


def build_parameters(**changes: Any) -> RunParameters:
    base: dict[str, Any] = {
        "use_case_id": "casoA",
        "cert_code": "CERT1",
        "email_type": "certification",
        "config_bucket": "bucket-config",
        "case_key": "casos/casoA.json",
        "global_key": "global.json",
        "redirect_url": "https://redir.example/r",
        "links_table": "tabla-enlaces",
        "sender": "no-responder@example.com",
        "configuration_set": None,
        "cert_code_index": "indice-cert",
        "timezone_name": "America/Bogota",
        "link_validity_days": 7,
        "link_token_length": 8,
        "poll_interval_seconds": 1.0,
        "max_query_wait_seconds": 60.0,
        "regeneration_without_new_data": False,
        "execution_id": "ejec-1",
    }
    base.update(changes)
    return RunParameters(**base)


def build_record(**changes: Any) -> NotificationRecord:
    base: dict[str, Any] = {
        "use_case_id": "casoA",
        "execution_id": "ej1",
        "period": "202610",
        "status": "PENDING",
        "user_email": "usuario@example.com",
        "query_name": "detail#a",
        "file_name": "a.csv",
        "s3_url": "https://s3.example/a.csv?x=1&amp;y=2",
        "no_data_for_period": False,
    }
    base.update(changes)
    return NotificationRecord(**base)


def build_composite_record(**changes: Any) -> CompositeRecord:
    base: dict[str, Any] = {
        "member_id": "m1",
        "status": "READY_TO_NOTIFY",
        "period": "202610",
        "cert_code": "",
        "source_execution_id": "origen-1",
        "batch_id": "lote-1",
        "updated_at": "2026-11-04T10:00:00",
    }
    base.update(changes)
    return CompositeRecord(**base)


def copy_case_config() -> dict[str, Any]:
    return copy.deepcopy(CASE_CONFIG_DATA)


def build_composite_config() -> dict[str, Any]:
    return {
        "enabled": True,
        "tableName": "tabla-compuesto",
        "groupId": "grupo",
        "memberIds": ["m1", "m2"],
    }
