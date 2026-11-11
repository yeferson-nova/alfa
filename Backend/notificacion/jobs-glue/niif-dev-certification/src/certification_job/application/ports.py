from datetime import datetime
from enum import Enum
from typing import Any, Mapping, Protocol, Sequence

from certification_job.domain.models import NotificationKey, ProbeResult, QueryDetails, QueryExecution


class LogEvent(str, Enum):
    JOB_START = "CER_INICIO_JOB"
    CONFIG_LOADED = "CER_CONFIGURACION_CARGADA"
    FLOW_ID_MISSING = "CER_ID_FLUJO_AUSENTE"
    CERT_CODE_GENERATED = "CER_CERT_CODE_GENERADO"
    QUERY_EXECUTED = "CER_CONSULTA_EJECUTADA"
    UNLOAD_EMPTY = "CER_UNLOAD_SIN_ARCHIVOS"
    NO_DATA_CONFIRMED = "CER_SIN_DATOS_CONFIRMADO"
    FILE_WRITTEN = "CER_ARCHIVO_GENERADO"
    ITEM_REGISTERED = "CER_REGISTRO_CREADO"
    INDEX_NOT_VISIBLE = "CER_INDICE_NO_VISIBLE"
    PENDING_MARKED = "CER_REGISTROS_PENDIENTES"
    JOB_END = "CER_FIN_JOB"
    JOB_ERROR = "CER_ERROR_JOB"
    JOB_SUMMARY = "CER_RESUMEN_JOB"


EVENT_MESSAGES: dict[LogEvent, str] = {
    LogEvent.JOB_START: "Inicio del job de certificación",
    LogEvent.CONFIG_LOADED: "Configuración global y del caso cargadas",
    LogEvent.FLOW_ID_MISSING: "No llegó el ID del flujo (EJECUCION_ID); los registros quedan sin ejecucionId",
    LogEvent.CERT_CODE_GENERATED: "Código de certificación generado",
    LogEvent.QUERY_EXECUTED: "Consulta de detalle ejecutada en Athena",
    LogEvent.UNLOAD_EMPTY: "El UNLOAD no dejó archivos; se confirma si el resultado está vacío",
    LogEvent.NO_DATA_CONFIRMED: "Resultado sin datos confirmado; se genera el CSV solo con encabezado",
    LogEvent.FILE_WRITTEN: "Archivo CSV de certificación generado",
    LogEvent.ITEM_REGISTERED: "Registro de notificación creado",
    LogEvent.INDEX_NOT_VISIBLE: "El certCode todavía no es visible en el índice; se reintenta",
    LogEvent.PENDING_MARKED: "Registros del certCode marcados como pendientes de notificación",
    LogEvent.JOB_END: "Fin del job de certificación",
    LogEvent.JOB_ERROR: "El job de certificación falló",
    LogEvent.JOB_SUMMARY: "Resumen de la ejecución del job",
}


class EventLog(Protocol):
    def bind(self, **fields: Any) -> None: ...

    def record(self, event: LogEvent, level: str = "INFO", **fields: Any) -> None: ...


class ConfigRepository(Protocol):
    def load_json(self, bucket: str, key: str) -> Any: ...


class QueryRunner(Protocol):
    def run(self, sql: str, database: str) -> QueryExecution: ...

    def describe(self, query_execution_id: str) -> QueryDetails: ...

    def probe(self, query_execution_id: str) -> ProbeResult: ...


class ObjectStore(Protocol):
    def list_keys(self, bucket: str, prefix: str) -> list[str]: ...

    def read(self, bucket: str, key: str) -> bytes: ...

    def write(self, bucket: str, key: str, body: bytes, content_type: str) -> None: ...


class NotificationRepository(Protocol):
    def put(self, table: str, item: Mapping[str, Any]) -> None: ...

    def find_by_cert_code(
        self, table: str, index: str, cert_code: str, use_case_id: str
    ) -> Sequence[NotificationKey]: ...

    def mark_pending(self, table: str, keys: Sequence[NotificationKey], values: Mapping[str, str]) -> None: ...


class Clock(Protocol):
    def now_local(self) -> datetime: ...


class IdGenerator(Protocol):
    def execution_id(self) -> str: ...

    def unique_suffix(self) -> str: ...


class Sleeper(Protocol):
    def __call__(self, seconds: float) -> None: ...
