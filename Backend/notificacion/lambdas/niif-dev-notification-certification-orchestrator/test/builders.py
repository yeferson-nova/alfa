import copy
from typing import Any

CONFIG_BUCKET = "bucket-config"
GLOBAL_KEY = "configs/global.json"
CASE_KEY = "configs/caso.json"
TABLE = "tabla-notificaciones"
CERT_INDEX = "indice-cert"
PENDING_INDEX = "indice-pendientes"
REPORTS = "bucket-reportes"
TRANSPORT = "bucket-transporte"
JOB = "job-notificacion"
SENDER = "remitente@ejemplo.test"
RECIPIENT = "soporte@ejemplo.test"

GLOBAL_JSON: dict[str, Any] = {
    "environment": "test",
    "timezone": "America/Bogota",
    "buckets": {"reports": REPORTS},
    "dynamodb": {"notification_table": TABLE, "cert_code_index": CERT_INDEX, "pending_index": PENDING_INDEX},
    "certification": {
        "pending_status": "PENDING",
        "certificate_status": "CERTIFICATE",
        "no_certificate_status": "NO_CERTIFICATE",
        "transport_status": "TRANSPORT",
        "no_transport_status": "NO_TRANSPORT",
        "report_folders": {"primasArl": "primasArl", "auxilioFunerario": "auxiliosFunerario"},
    },
    "notification": {"ses_sender": SENDER, "ses_configuration_set": "conjunto"},
    "orchestrator": {
        "notification_job_name": JOB,
        "transport_bucket": TRANSPORT,
        "transport_prefix": "export",
        "rejected_folder": "NO-CERTIFICADOS",
        "use_historic_source_path": True,
        "reprocess_after_minutes": 30,
        "reprocess_max_attempts": 3,
        "alert_recipients": [RECIPIENT],
    },
}


def _parent(data: dict[str, Any], path: str) -> tuple[dict[str, Any], str]:
    *parents, last = path.split(".")
    target = data
    for parent in parents:
        target = target[parent]
    return target, last


def global_json(changes: dict[str, Any] | None = None, removed: tuple[str, ...] = ()) -> dict[str, Any]:
    data = copy.deepcopy(GLOBAL_JSON)
    for path, value in (changes or {}).items():
        target, last = _parent(data, path)
        target[last] = value
    for path in removed:
        target, last = _parent(data, path)
        target.pop(last)
    return data


def image(**overrides: Any) -> dict[str, Any]:
    values: dict[str, Any] = {
        "useCaseId": "primasArl",
        "executionId": "exec-1",
        "status": "PENDING",
        "certCode": "CERT-1",
        "nameFile": "archivo.csv",
        "emailType": "certification",
        "period": "202606",
        "configBucket": CONFIG_BUCKET,
        "configKey": CASE_KEY,
        "globalConfigKey": GLOBAL_KEY,
        "regeneracionSinDatosNuevos": False,
        "ejecucionId": "flujo-1",
    }
    values.update(overrides)
    return {name: _attribute(value) for name, value in values.items() if value is not ...}


def _attribute(value: Any) -> dict[str, Any]:
    if value is None:
        return {"NULL": True}
    if isinstance(value, bool):
        return {"BOOL": value}
    if isinstance(value, int):
        return {"N": str(value)}
    return {"S": str(value)}


def stream_record(
    new: dict[str, Any] | None = None, old_status: str | None = "PROCESSED", sequence: str = "1", event: str = "MODIFY"
) -> dict[str, Any]:
    stream: dict[str, Any] = {"SequenceNumber": sequence}
    if new is not None:
        stream["NewImage"] = new
    if old_status is not None:
        stream["OldImage"] = {"status": {"S": old_status}}
    return {"eventName": event, "dynamodb": stream}
