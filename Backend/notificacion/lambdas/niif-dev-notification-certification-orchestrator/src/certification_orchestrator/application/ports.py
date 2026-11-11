from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Mapping, Protocol, Sequence

from certification_orchestrator.domain.models import (
    Alert,
    AlertSettings,
    LockAcquisition,
    NotificationRecord,
    RecordKey,
)


class LogEvent(str, Enum):
    BATCH_START = "ORQ_INICIO_LOTE"
    RECORD_IGNORED = "ORQ_REGISTRO_IGNORADO"
    FLOW_ID_MISSING = "ORQ_ID_FLUJO_AUSENTE"
    LOCK_TAKEN = "ORQ_CANDADO_OCUPADO"
    NOTIFICATION_LAUNCHED = "ORQ_NOTIFICACION_LANZADA"
    LOCK_RELEASED = "ORQ_CANDADO_LIBERADO"
    FILE_TRANSPORTED = "ORQ_ARCHIVO_TRANSPORTADO"
    FILE_ARCHIVED = "ORQ_ARCHIVO_NO_CERTIFICADO"
    STATUS_ALREADY_CHANGED = "ORQ_ESTADO_YA_CAMBIADO"
    SCHEDULE_PENDING_D2 = "ORQ_PROGRAMACION_PENDIENTE_D2"
    RECORD_FAILED = "ORQ_ERROR_REGISTRO"
    BATCH_SUMMARY = "ORQ_RESUMEN_LOTE"
    REPROCESS_START = "ORQ_INICIO_REPROCESO"
    STUCK_FOUND = "ORQ_PENDIENTE_ATASCADO"
    ATTEMPTS_EXHAUSTED = "ORQ_REINTENTOS_AGOTADOS"
    ALERT_SENT = "ORQ_ALERTA_ENVIADA"
    GROUP_FAILED = "ORQ_ERROR_REPROCESO"
    REPROCESS_SUMMARY = "ORQ_RESUMEN_REPROCESO"
    INVOCATION_ERROR = "ORQ_ERROR_INVOCACION"


EVENT_MESSAGES: dict[LogEvent, str] = {
    LogEvent.BATCH_START: "Inicio del lote del stream de notificaciones",
    LogEvent.RECORD_IGNORED: "El cambio no requiere acción: el estado no cambió o no es de la cadena",
    LogEvent.FLOW_ID_MISSING: "El registro no tiene ejecucionId; el job se lanza sin EJECUCION_ID",
    LogEvent.LOCK_TAKEN: "Otro evento ya lanzó la notificación de este certCode; se omite",
    LogEvent.NOTIFICATION_LAUNCHED: "Job de notificación lanzado",
    LogEvent.LOCK_RELEASED: "No se pudo lanzar el job de notificación; se libera el candado para reintentar",
    LogEvent.FILE_TRANSPORTED: "Archivo certificado copiado al bucket de transporte",
    LogEvent.FILE_ARCHIVED: "Archivo no certificado movido a la carpeta de rechazados",
    LogEvent.STATUS_ALREADY_CHANGED: "El registro ya no estaba en el estado esperado; no se cambia",
    LogEvent.SCHEDULE_PENDING_D2: "No certificado: dejar la malla pendiente en la programación espera la decisión D2",
    LogEvent.RECORD_FAILED: "Falló el procesamiento del registro; se devuelve para reintento",
    LogEvent.BATCH_SUMMARY: "Resumen del lote del stream",
    LogEvent.REPROCESS_START: "Inicio del reprocesador de notificaciones pendientes",
    LogEvent.STUCK_FOUND: "certCode pendiente de notificación por más tiempo del permitido",
    LogEvent.ATTEMPTS_EXHAUSTED: "Se agotaron los intentos automáticos de notificación",
    LogEvent.ALERT_SENT: "Alerta de notificación sin enviar enviada a soporte",
    LogEvent.GROUP_FAILED: "Falló el reproceso de un certCode",
    LogEvent.REPROCESS_SUMMARY: "Resumen del reprocesador",
    LogEvent.INVOCATION_ERROR: "La invocación de la orquestadora falló",
}


class EventLog(Protocol):
    def bind(self, **fields: Any) -> None: ...

    def record(self, event: LogEvent, level: str = "INFO", **fields: Any) -> None: ...


class ConfigRepository(Protocol):
    def load_json(self, bucket: str, key: str) -> Any: ...


class NotificationRepository(Protocol):
    def find_keys_by_cert_code(self, table: str, index: str, cert_code: str) -> Sequence[RecordKey]: ...

    def find_pending_before(
        self, table: str, index: str, status: str, before: str
    ) -> Sequence[NotificationRecord]: ...

    def acquire_lock(self, table: str, acquisition: LockAcquisition) -> bool: ...

    def release_lock(self, table: str, keys: Sequence[RecordKey], token: str) -> None: ...

    def change_status(self, table: str, key: RecordKey, expected: str, new: str) -> bool: ...

    def mark_alerted(self, table: str, keys: Sequence[RecordKey], date: str) -> None: ...


class ObjectStore(Protocol):
    def exists(self, bucket: str, key: str) -> bool: ...

    def copy(self, source_bucket: str, source_key: str, target_bucket: str, target_key: str) -> None: ...

    def delete(self, bucket: str, key: str) -> None: ...


class JobLauncher(Protocol):
    def start(self, job_name: str, arguments: Mapping[str, str]) -> str: ...


class AlertSender(Protocol):
    def send(self, settings: AlertSettings, alert: Alert) -> None: ...


class Clock(Protocol):
    def now_local(self, timezone_name: str) -> datetime: ...


@dataclass(frozen=True)
class Dependencies:
    configs: ConfigRepository
    notifications: NotificationRepository
    objects: ObjectStore
    jobs: JobLauncher
    alerts: AlertSender
    clock: Clock
    log: EventLog
