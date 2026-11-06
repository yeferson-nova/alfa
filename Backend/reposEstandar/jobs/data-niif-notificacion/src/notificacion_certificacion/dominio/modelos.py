from dataclasses import dataclass, field
from typing import Any, Mapping


@dataclass(frozen=True)
class ParametrosEjecucion:
    caso_uso: str
    cert_code: str
    tipo_correo: str
    bucket_configuracion: str
    clave_caso: str
    clave_global: str
    url_redireccion: str
    tabla_enlaces: str
    remitente: str
    conjunto_configuracion: str | None
    indice_cert_code: str
    zona_horaria: str
    dias_vigencia_enlace: int
    longitud_token_enlace: int
    segundos_entre_consultas: float
    segundos_maximos_consulta: float
    regeneracion_sin_datos_nuevos: bool
    id_ejecucion: str


@dataclass(frozen=True)
class Consulta:
    nombre: str
    base_datos: str
    sql: str


@dataclass(frozen=True)
class ConfiguracionGlobal:
    bucket_activos: str
    tabla_notificaciones: str
    catalogo: str
    ambiente: str
    prefijo_salida_athena: str
    estado_enviado: str


@dataclass(frozen=True)
class ConfiguracionCompuesta:
    tabla: str
    id_grupo: str
    id_control: str
    ids_miembros: tuple[str, ...]


@dataclass(frozen=True)
class ConfiguracionCaso:
    caso_uso: str
    plantilla_asunto: str
    variables_texto: Mapping[str, str]
    destinatarios: tuple[str, ...]
    consultas_resumen: tuple[Consulta, ...]
    consultas_totales: tuple[Consulta, ...]
    consultas_presencia: tuple[Consulta, ...]
    compuesta: ConfiguracionCompuesta | None


@dataclass(frozen=True)
class RegistroNotificacion:
    caso_uso: str
    id_ejecucion: str
    periodo: str
    estado: str
    correo_usuario: str
    nombre_consulta: str
    nombre_archivo: str
    url_s3: str
    sin_datos_periodo: bool


@dataclass(frozen=True)
class EnlaceCorto:
    token: str
    url: str
    caso_uso: str
    cert_code: str
    accion: str
    fecha_creacion: str
    expira_en: int


@dataclass(frozen=True)
class PoliticaEnlaces:
    url_base: str
    tabla: str
    dias_vigencia: int
    longitud_token: int


@dataclass(frozen=True)
class EntornoConsultas:
    catalogo: str
    salida: str


@dataclass(frozen=True)
class ResultadoConsulta:
    encabezados: tuple[str, ...]
    filas: tuple[tuple[str, ...], ...]


@dataclass(frozen=True)
class Mensaje:
    asunto: str
    texto: str
    html: str | None = None
    etiquetas: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class EnvioRealizado:
    destinatario: str
    id_mensaje: str


@dataclass(frozen=True)
class RegistroCompuesto:
    id_miembro: str
    estado: str
    periodo: Any
    cert_code: str
    id_ejecucion_origen: Any
    id_lote: Any
    actualizado_en: Any


@dataclass(frozen=True)
class ReservaCompuesta:
    tabla: str
    id_compuesto: str
    cert_code: str
    control: RegistroCompuesto
    miembros: tuple[RegistroCompuesto, ...]
    ya_completada: bool
