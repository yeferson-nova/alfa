from datetime import datetime
from enum import Enum
from typing import Any, Mapping, Protocol, Sequence

from notificacion_certificacion.dominio.modelos import (
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


class RepositorioConfiguracion(Protocol):
    def cargar_json(self, bucket: str, clave: str) -> Mapping[str, Any]: ...


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
