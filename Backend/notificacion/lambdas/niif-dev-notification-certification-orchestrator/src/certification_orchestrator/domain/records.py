from typing import Any, Mapping

from certification_orchestrator.domain.errors import DataError, InputError
from certification_orchestrator.domain.models import (
    ConfigPointer,
    NotificationRecord,
    RecordKey,
    SendLock,
    StreamChange,
)

USE_CASE_ID = "useCaseId"
EXECUTION_ID = "executionId"
STATUS = "status"
CERT_CODE = "certCode"
NAME_FILE = "nameFile"
EMAIL_TYPE = "emailType"
CONFIG_BUCKET = "configBucket"
CONFIG_KEY = "configKey"
GLOBAL_CONFIG_KEY = "globalConfigKey"
REGENERATION = "regeneracionSinDatosNuevos"
FLOW_EXECUTION_ID = "ejecucionId"
PERIOD = "period"
PENDING_DATE = "pendingDate"
SEND_LOCK = "sendLock"
SEND_LOCK_DATE = "sendLockDate"
SEND_ATTEMPTS = "sendAttempts"
SEND_ALERT_DATE = "sendAlertDate"

TEXT, NUMBER, FLAG, NULL = "S", "N", "BOOL", "NULL"
BAD_RECORD = "ORQ-ENT-002"
STREAM_EVENT_NAMES = frozenset({"INSERT", "MODIFY"})
REMOVE_EVENT = "REMOVE"


def _attribute(image: Mapping[str, Any], name: str, kind: str) -> Any:
    attribute = image.get(name)
    if attribute is None:
        return None
    if not isinstance(attribute, Mapping):
        raise InputError(BAD_RECORD, f"El atributo {name} del registro no tiene formato de DynamoDB")
    if attribute.get(NULL) is True:
        return None
    if kind not in attribute:
        raise InputError(BAD_RECORD, f"El atributo {name} del registro no es del tipo {kind}")
    return attribute[kind]


def _text(image: Mapping[str, Any], name: str) -> str | None:
    value = _attribute(image, name, TEXT)
    if value is None or not str(value).strip():
        return None
    return str(value).strip()


def _required_text(image: Mapping[str, Any], name: str) -> str:
    value = _text(image, name)
    if value is None:
        raise InputError(BAD_RECORD, f"El registro no tiene {name}")
    return value


def _integer(image: Mapping[str, Any], name: str) -> int:
    value = _attribute(image, name, NUMBER)
    if value is None:
        return 0
    try:
        return int(str(value))
    except ValueError as error:
        raise InputError(BAD_RECORD, f"El atributo {name} no es un entero") from error


def _flag(image: Mapping[str, Any], name: str) -> bool:
    return _attribute(image, name, FLAG) is True


def _lock(image: Mapping[str, Any]) -> SendLock:
    return SendLock(
        token=_text(image, SEND_LOCK),
        date=_text(image, SEND_LOCK_DATE),
        attempts=_integer(image, SEND_ATTEMPTS),
        alert_date=_text(image, SEND_ALERT_DATE),
    )


def record_key(image: Mapping[str, Any]) -> RecordKey:
    return RecordKey(
        use_case_id=_required_text(image, USE_CASE_ID),
        execution_id=_required_text(image, EXECUTION_ID),
    )


def record_from_image(image: Mapping[str, Any]) -> NotificationRecord:
    if not isinstance(image, Mapping):
        raise InputError(BAD_RECORD, "La imagen del registro no es un objeto")
    return NotificationRecord(
        key=record_key(image),
        status=_required_text(image, STATUS),
        cert_code=_text(image, CERT_CODE),
        name_file=_text(image, NAME_FILE),
        email_type=_text(image, EMAIL_TYPE),
        config_bucket=_text(image, CONFIG_BUCKET),
        config_key=_text(image, CONFIG_KEY),
        global_config_key=_text(image, GLOBAL_CONFIG_KEY),
        regeneration_without_new_data=_flag(image, REGENERATION),
        flow_execution_id=_text(image, FLOW_EXECUTION_ID),
        period=_text(image, PERIOD),
        lock=_lock(image),
    )


def sequence_number(raw: Any) -> str:
    stream = raw.get("dynamodb") if isinstance(raw, Mapping) else None
    value = stream.get("SequenceNumber") if isinstance(stream, Mapping) else None
    if not isinstance(value, str) or not value.strip():
        raise InputError("ORQ-ENT-001", "Un registro del lote no tiene SequenceNumber")
    return value


def is_removal(raw: Mapping[str, Any]) -> bool:
    return raw.get("eventName") == REMOVE_EVENT


def parse_change(raw: Mapping[str, Any]) -> StreamChange:
    event_name = raw.get("eventName")
    if event_name not in STREAM_EVENT_NAMES:
        raise InputError(BAD_RECORD, f"eventName desconocido en el stream: {event_name}")
    stream = raw["dynamodb"]
    new_image = stream.get("NewImage")
    if not isinstance(new_image, Mapping):
        raise InputError(BAD_RECORD, "El registro del stream no trae NewImage (revisar StreamViewType)")
    old_image = stream.get("OldImage")
    return StreamChange(
        sequence_number=sequence_number(raw),
        event_name=str(event_name),
        old_status=_text(old_image, STATUS) if isinstance(old_image, Mapping) else None,
        record=record_from_image(new_image),
    )


def config_pointer(record: NotificationRecord) -> ConfigPointer:
    if not record.config_bucket or not record.global_config_key:
        raise DataError(
            "ORQ-DAT-001", "El registro no tiene configBucket o globalConfigKey: no se puede leer el JSON global"
        )
    return ConfigPointer(bucket=record.config_bucket, key=record.global_config_key)
