from typing import Any, Mapping

from certification_job.domain.errors import ConfigurationError
from certification_job.domain.models import (
    CaseConfig,
    CertificationSettings,
    ConsoleSettings,
    DetailQuery,
    ExportSettings,
    GlobalConfig,
    HeaderSpec,
    RetryPolicy,
    WaitPolicy,
)

MISSING_KEY = "CER-CFG-001"
NO_DETAIL_QUERIES = "CER-CFG-002"
CERTIFICATION = "certification"


def _path(keys: tuple[str, ...]) -> str:
    return ".".join(keys)


def _find(data: Any, *keys: str) -> Any:
    current = data
    for key in keys:
        if not isinstance(current, Mapping) or key not in current:
            raise ConfigurationError(MISSING_KEY, f"Falta la clave de configuracion: {_path(keys)}")
        current = current[key]
    return current


def _text(data: Any, *keys: str) -> str:
    value = _find(data, *keys)
    if not isinstance(value, str) or not value.strip():
        raise ConfigurationError(MISSING_KEY, f"La clave debe ser texto no vacio: {_path(keys)}")
    return value.strip()


def _optional_text(data: Any, *keys: str) -> str | None:
    try:
        value = _find(data, *keys)
    except ConfigurationError:
        return None
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    return _text(data, *keys)


def _flag(data: Any, *keys: str) -> bool:
    value = _find(data, *keys)
    if not isinstance(value, bool):
        raise ConfigurationError(MISSING_KEY, f"La clave debe ser true o false: {_path(keys)}")
    return value


def _positive_number(data: Any, *keys: str) -> float:
    value = _find(data, *keys)
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        raise ConfigurationError(MISSING_KEY, f"La clave debe ser un numero mayor a cero: {_path(keys)}")
    return float(value)


def _positive_integer(data: Any, *keys: str) -> int:
    value = _find(data, *keys)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ConfigurationError(MISSING_KEY, f"La clave debe ser un entero mayor a cero: {_path(keys)}")
    return value


def _text_map(data: Any, *keys: str) -> dict[str, str]:
    value = _find(data, *keys)
    if not isinstance(value, Mapping) or not value:
        raise ConfigurationError(MISSING_KEY, f"La clave debe ser un objeto con valores: {_path(keys)}")
    return {str(name): _text(value, name) for name in value}


def _wait_policy(data: Any) -> WaitPolicy:
    poll = _positive_number(data, CERTIFICATION, "athena_poll_seconds")
    maximum = _positive_number(data, CERTIFICATION, "athena_max_wait_seconds")
    if maximum < poll:
        raise ConfigurationError(
            MISSING_KEY, "certification.athena_max_wait_seconds no puede ser menor a athena_poll_seconds"
        )
    return WaitPolicy(poll_seconds=poll, max_wait_seconds=maximum)


def _certification(data: Any) -> CertificationSettings:
    return CertificationSettings(
        default_user_email=_text(data, CERTIFICATION, "default_user_email"),
        processed_status=_text(data, CERTIFICATION, "processed_status"),
        pending_status=_text(data, CERTIFICATION, "pending_status"),
        report_folders=_text_map(data, CERTIFICATION, "report_folders"),
        athena_wait=_wait_policy(data),
        index_visibility=RetryPolicy(
            attempts=_positive_integer(data, CERTIFICATION, "index_visibility_attempts"),
            wait_seconds=_positive_number(data, CERTIFICATION, "index_visibility_wait_seconds"),
        ),
    )


def _console(data: Any) -> ConsoleSettings:
    return ConsoleSettings(
        base_url=_text(data, "console_url", "base_url"),
        account_id=_text(data, "console_url", "account_id"),
        role_name=_text(data, "console_url", "role_name"),
        region=_text(data, "console_url", "region"),
    )


def _export(data: Any) -> ExportSettings:
    delimiter = _find(data, "s3_export", "field_delimiter")
    if not isinstance(delimiter, str) or len(delimiter) != 1:
        raise ConfigurationError(MISSING_KEY, "s3_export.field_delimiter debe ser un solo caracter")
    return ExportSettings(
        content_type=_text(data, "s3_export", "csv_content_type"),
        write_bom=_flag(data, "s3_export", "csv_encoding_bom"),
        field_delimiter=delimiter,
    )


def read_global_config(data: Any) -> GlobalConfig:
    return GlobalConfig(
        environment=_text(data, "environment"),
        catalog=_text(data, "catalog"),
        timezone=_text(data, "timezone"),
        assets_bucket=_text(data, "buckets", "assets"),
        reports_bucket=_text(data, "buckets", "reports"),
        notification_table=_text(data, "dynamodb", "notification_table"),
        cert_code_index=_text(data, "dynamodb", "cert_code_index"),
        athena_output_path=_text(data, "athena", "output_path"),
        athena_unload_path=_text(data, "athena", "unload_path"),
        console=_console(data),
        export=_export(data),
        certification=_certification(data),
    )


def _detail_query(raw: Any) -> DetailQuery:
    if not isinstance(raw, Mapping):
        raise ConfigurationError(MISSING_KEY, "Cada consulta de queries.detail debe ser un objeto")
    return DetailQuery(
        name=_text(raw, "queryName"),
        database=_optional_text(raw, "database"),
        sql=_text(raw, "query"),
    )


def _detail_queries(data: Any) -> tuple[DetailQuery, ...]:
    queries = data.get("queries", {}).get("detail") if isinstance(data.get("queries"), Mapping) else None
    if not isinstance(queries, list) or not queries:
        raise ConfigurationError(NO_DETAIL_QUERIES, "El caso no tiene consultas en queries.detail")
    return tuple(sorted((_detail_query(raw) for raw in queries), key=lambda query: query.name))


def _header_names(columns: Mapping[str, Any]) -> tuple[str, ...]:
    return tuple(
        str(meta.get("name") or key) if isinstance(meta, Mapping) else str(key) for key, meta in columns.items()
    )


def _header_spec(raw: Any) -> HeaderSpec | None:
    if not isinstance(raw, Mapping) or not isinstance(raw.get("queryName"), str):
        return None
    columns = raw.get("columns") or {}
    if not isinstance(columns, Mapping):
        raise ConfigurationError(MISSING_KEY, "email.tableVars.*.columns debe ser un objeto")
    return HeaderSpec(
        query_name=raw["queryName"],
        headers=_header_names(columns),
        export_disabled=raw.get("export") is False,
    )


def _header_specs(data: Any) -> tuple[HeaderSpec, ...]:
    email = data.get("email")
    table_vars = email.get("tableVars") if isinstance(email, Mapping) else None
    if table_vars is None:
        return ()
    if not isinstance(table_vars, Mapping):
        raise ConfigurationError(MISSING_KEY, "email.tableVars debe ser un objeto")
    specs = (_header_spec(raw) for raw in table_vars.values())
    return tuple(spec for spec in specs if spec is not None)


def _default_database(data: Any) -> str | None:
    athena = data.get("athena")
    return _optional_text(athena, "defaultDatabase") if isinstance(athena, Mapping) else None


def read_case_config(data: Any) -> CaseConfig:
    if not isinstance(data, Mapping):
        raise ConfigurationError(MISSING_KEY, "El JSON del caso debe ser un objeto")
    return CaseConfig(
        use_case_id=_text(data, "useCaseId"),
        email_type=_text(data, "emailType").lower(),
        default_database=_default_database(data),
        detail_queries=_detail_queries(data),
        header_specs=_header_specs(data),
    )
