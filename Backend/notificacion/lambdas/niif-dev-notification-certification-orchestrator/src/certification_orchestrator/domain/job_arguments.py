from certification_orchestrator.domain.errors import DataError
from certification_orchestrator.domain.models import NotificationRecord

FLAG_TRUE, FLAG_FALSE = "true", "false"
FLOW_ID_ARGUMENT = "--EJECUCION_ID"


def notification_arguments(record: NotificationRecord) -> dict[str, str]:
    required = {
        "--useCaseId": record.key.use_case_id,
        "--certCode": record.cert_code,
        "--emailType": record.email_type,
        "--CONFIG_BUCKET": record.config_bucket,
        "--CONFIG_KEY": record.config_key,
        "--GLOBAL_CONFIG_KEY": record.global_config_key,
    }
    missing = sorted(name.lstrip("-") for name, value in required.items() if not value)
    if missing:
        raise DataError(
            "ORQ-DAT-004", f"El registro no tiene los datos para lanzar la notificacion: {', '.join(missing)}"
        )
    arguments = {name: str(value) for name, value in required.items()}
    arguments["--REGENERACION_SIN_DATOS_NUEVOS"] = FLAG_TRUE if record.regeneration_without_new_data else FLAG_FALSE
    if record.flow_execution_id:
        arguments[FLOW_ID_ARGUMENT] = record.flow_execution_id
    return arguments
