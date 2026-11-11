from datetime import datetime
from enum import Enum
from typing import Any, Mapping, Protocol, Sequence

from notificacion_certificacion.domain.modelos import (
    EnlaceCorto,
    EntornoConsultas,
    EnvioRealizado,
    Mensaje,
    RegistroCompuesto,
    RegistroNotificacion,
    ReservaCompuesta,
    ResultadoConsulta,
)


class EventoLog(str, Enum):
    INICIO_JOB = "NTF_INICIO_JOB"
    CONFIGURACION_CARGADA = "NTF_CONFIGURACION_CARGADA"
    SIN_REGISTROS = "NTF_SIN_REGISTROS"
    YA_ENVIADO = "NTF_YA_ENVIADO"
    COMPUESTO_YA_COMPLETADO = "NTF_COMPUESTO_YA_COMPLETADO"
    CONSULTA_EJECUTADA = "NTF_CONSULTA_EJECUTADA"
    CONSULTA_FALLIDA = "NTF_CONSULTA_FALLIDA"
    ENLACE_CREADO = "NTF_ENLACE_CREADO"
    CORREO_ENVIADO = "NTF_CORREO_ENVIADO"
    REGISTROS_MARCADOS = "NTF_REGISTROS_MARCADOS"
    COMPUESTO_CERRADO = "NTF_COMPUESTO_CERRADO"
    FIN_JOB = "NTF_FIN_JOB"
    ERROR_JOB = "NTF_ERROR_JOB"
    ID_FLUJO_AUSENTE = "NTF_ID_FLUJO_AUSENTE"
    ID_FLUJO_DISTINTO = "NTF_ID_FLUJO_DISTINTO"
    AVISOS_AGREGADOS = "NTF_AVISOS_AGREGADOS"
    PLANTILLA_NO_DISPONIBLE = "NTF_PLANTILLA_NO_DISPONIBLE"
    FORMATO_CORREO = "NTF_FORMATO_CORREO"
    CORREO_FALLIDO = "NTF_CORREO_FALLIDO"
    ENVIO_PARCIAL = "NTF_ENVIO_PARCIAL"
    RESUMEN_JOB = "NTF_RESUMEN_JOB"


MENSAJES_EVENTO: dict[EventoLog, str] = {
    EventoLog.INICIO_JOB: "Inicia el job de notificación de certificación",
    EventoLog.CONFIGURACION_CARGADA: "Configuración global y del caso cargadas",
    EventoLog.SIN_REGISTROS: "No hay registros para el certCode; se termina sin enviar",
    EventoLog.YA_ENVIADO: "Todos los registros ya estaban enviados; no se reenvía",
    EventoLog.COMPUESTO_YA_COMPLETADO: "El grupo compuesto ya fue notificado con este certCode; no se reenvía",
    EventoLog.CONSULTA_EJECUTADA: "Consulta de Athena terminada",
    EventoLog.CONSULTA_FALLIDA: "Consulta de Athena fallida; el correo lo indica",
    EventoLog.ENLACE_CREADO: "Enlace corto creado",
    EventoLog.CORREO_ENVIADO: "Correo aceptado por SES para un destinatario",
    EventoLog.REGISTROS_MARCADOS: "Registros de notificación marcados como enviados",
    EventoLog.COMPUESTO_CERRADO: "Miembros del grupo compuesto consumidos y control completado",
    EventoLog.FIN_JOB: "Termina el job de notificación de certificación",
    EventoLog.ERROR_JOB: "El job falló con un error clasificado",
    EventoLog.ID_FLUJO_AUSENTE: "No llegó el ejecucion_id del flujo; no se puede unir con la Step Function",
    EventoLog.ID_FLUJO_DISTINTO: "El ejecucion_id recibido no coincide con el del registro de notificación",
    EventoLog.AVISOS_AGREGADOS: "Se agregaron avisos al correo",
    EventoLog.PLANTILLA_NO_DISPONIBLE: "No se pudo usar la plantilla HTML; el correo sale solo en texto",
    EventoLog.FORMATO_CORREO: "Formato del correo decidido",
    EventoLog.CORREO_FALLIDO: "SES no aceptó el correo para un destinatario; se sigue con los demás",
    EventoLog.ENVIO_PARCIAL: "El correo no salió para todos los destinatarios",
    EventoLog.RESUMEN_JOB: "Resumen de la corrida para métricas (filtros de métricas de CloudWatch Logs)",
}


class RepositorioConfiguracion(Protocol):
    def cargar_json(self, bucket: str, clave: str) -> Mapping[str, Any]: ...

    def cargar_texto(self, bucket: str, clave: str) -> str: ...


class RepositorioNotificaciones(Protocol):
    def buscar_por_cert_code(
        self, tabla: str, indice: str, cert_code: str, caso_uso: str
    ) -> tuple[RegistroNotificacion, ...]: ...

    def marcar_enviados(
        self,
        tabla: str,
        registros: Sequence[RegistroNotificacion],
        estado: str,
        canal: str,
        envios: Sequence[EnvioRealizado],
        fecha_envio: str,
    ) -> None: ...


class RepositorioEnlaces(Protocol):
    def guardar(self, tabla: str, enlace: EnlaceCorto) -> None: ...


class ConsultorAnalitico(Protocol):
    def ejecutar(self, sql: str, base_datos: str, entorno: EntornoConsultas) -> ResultadoConsulta: ...


class Notificador(Protocol):
    def enviar(self, mensaje: Mensaje, destinatario: str) -> str: ...


class RepositorioCompuesto(Protocol):
    def obtener(self, tabla: str, id_compuesto: str, id_miembro: str) -> RegistroCompuesto | None: ...

    def consumir(self, reserva: ReservaCompuesta, id_mensaje: str, fecha: str) -> None: ...


class Reloj(Protocol):
    def ahora_local(self) -> datetime: ...

    def ahora_utc(self) -> datetime: ...


class GeneradorTokens(Protocol):
    def generar(self, longitud: int) -> str: ...


class Bitacora(Protocol):
    def registrar(self, evento: EventoLog, nivel: str = "INFO", **contexto: Any) -> None: ...

    def fijar_contexto(self, **contexto: Any) -> None: ...
