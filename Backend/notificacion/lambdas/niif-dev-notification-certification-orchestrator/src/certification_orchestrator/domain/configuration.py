from typing import Any, Mapping

from certification_orchestrator.domain.errors import ConfigurationError
from certification_orchestrator.domain.models import (
    AlertSettings,
    GlobalConfig,
    OrchestratorSettings,
    StatusNames,
)

INVALID_KEY = "ORQ-CFG-001"
CERTIFICATION = "certification"
ORCHESTRATOR = "orchestrator"
NOTIFICATION = "notification"


def _path(keys: tuple[str, ...]) -> str:
    return ".".join(keys)


def _find(data: Any, *keys: str) -> Any:
    current = data
    for key in keys:
        if not isinstance(current, Mapping) or key not in current:
            raise ConfigurationError(INVALID_KEY, f"Falta la clave de configuracion: {_path(keys)}")
        current = current[key]
    return current


def _text(data: Any, *keys: str) -> str:
    value = _find(data, *keys)
    if not isinstance(value, str) or not value.strip():
        raise ConfigurationError(INVALID_KEY, f"La clave debe ser texto no vacio: {_path(keys)}")
    return value.strip()


def _nullable_text(data: Any, *keys: str) -> str | None:
    value = _find(data, *keys)
    if value is None:
        return None
    return _text(data, *keys)


def _flag(data: Any, *keys: str) -> bool:
    value = _find(data, *keys)
    if not isinstance(value, bool):
        raise ConfigurationError(INVALID_KEY, f"La clave debe ser true o false: {_path(keys)}")
    return value


def _positive_integer(data: Any, *keys: str) -> int:
    value = _find(data, *keys)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ConfigurationError(INVALID_KEY, f"La clave debe ser un entero mayor a cero: {_path(keys)}")
    return value


def _text_map(data: Any, *keys: str) -> dict[str, str]:
    value = _find(data, *keys)
    if not isinstance(value, Mapping) or not value:
        raise ConfigurationError(INVALID_KEY, f"La clave debe ser un objeto con valores: {_path(keys)}")
    return {str(name): _text(value, name) for name in value}


def _text_list(data: Any, *keys: str) -> tuple[str, ...]:
    value = _find(data, *keys)
    if not isinstance(value, list) or not value:
        raise ConfigurationError(INVALID_KEY, f"La clave debe ser una lista con valores: {_path(keys)}")
    if any(not isinstance(item, str) or not item.strip() for item in value):
        raise ConfigurationError(INVALID_KEY, f"La lista solo admite texto no vacio: {_path(keys)}")
    return tuple(item.strip() for item in value)


def _folder(data: Any, *keys: str) -> str:
    value = _text(data, *keys).strip("/")
    if not value:
        raise ConfigurationError(INVALID_KEY, f"La carpeta no puede ser solo '/': {_path(keys)}")
    return value


def _statuses(data: Any) -> StatusNames:
    statuses = StatusNames(
        pending=_text(data, CERTIFICATION, "pending_status"),
        certificate=_text(data, CERTIFICATION, "certificate_status"),
        no_certificate=_text(data, CERTIFICATION, "no_certificate_status"),
        transport=_text(data, CERTIFICATION, "transport_status"),
        no_transport=_text(data, CERTIFICATION, "no_transport_status"),
    )
    values = list(vars(statuses).values())
    if len(set(values)) != len(values):
        raise ConfigurationError(INVALID_KEY, "Los estados de certification.*_status deben ser distintos entre si")
    return statuses


def _orchestrator(data: Any) -> OrchestratorSettings:
    return OrchestratorSettings(
        notification_job_name=_text(data, ORCHESTRATOR, "notification_job_name"),
        transport_bucket=_text(data, ORCHESTRATOR, "transport_bucket"),
        transport_prefix=_folder(data, ORCHESTRATOR, "transport_prefix"),
        rejected_folder=_folder(data, ORCHESTRATOR, "rejected_folder"),
        use_historic_source_path=_flag(data, ORCHESTRATOR, "use_historic_source_path"),
        reprocess_after_minutes=_positive_integer(data, ORCHESTRATOR, "reprocess_after_minutes"),
        reprocess_max_attempts=_positive_integer(data, ORCHESTRATOR, "reprocess_max_attempts"),
    )


def _alerts(data: Any) -> AlertSettings:
    return AlertSettings(
        sender=_text(data, NOTIFICATION, "ses_sender"),
        configuration_set=_nullable_text(data, NOTIFICATION, "ses_configuration_set"),
        recipients=_text_list(data, ORCHESTRATOR, "alert_recipients"),
    )


def read_global_config(data: Any) -> GlobalConfig:
    return GlobalConfig(
        environment=_text(data, "environment"),
        timezone=_text(data, "timezone"),
        reports_bucket=_text(data, "buckets", "reports"),
        notification_table=_text(data, "dynamodb", "notification_table"),
        cert_code_index=_text(data, "dynamodb", "cert_code_index"),
        pending_index=_text(data, "dynamodb", "pending_index"),
        report_folders=_text_map(data, CERTIFICATION, "report_folders"),
        statuses=_statuses(data),
        orchestrator=_orchestrator(data),
        alerts=_alerts(data),
    )
