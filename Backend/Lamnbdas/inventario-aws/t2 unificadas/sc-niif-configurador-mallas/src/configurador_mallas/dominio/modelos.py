"""Contratos de entrada y salida, y especificación de una malla.

Los prefijos de llave (``MALLA#``, ``EJC#``, ``EJECUCION#``) son el contrato con
las tablas y con la Step Function: no son configuración y no varían por malla.
"""
import re
from dataclasses import dataclass, field

from .errores import ErrorEntrada
from .periodo import validar_formato

PREFIJO_MALLA = "MALLA#"
PREFIJO_EJC = "EJC#"
PREFIJO_EJECUCION = "EJECUCION#"
ORIGEN_POR_DEFECTO = "INVOCACION_DIRECTA"
MODO_POR_DEFECTO = "NORMAL"

_CLAVE_IDEMPOTENCIA = re.compile(r"[A-Za-z0-9._:\-]{1,128}")


@dataclass(frozen=True)
class ReglasPeriodo:
    """Cómo se traslada el periodo a los filtros (`where`) y a los segmentos."""

    columnas_fecha: tuple[str, ...]
    columna_periodo: str | None
    patron_segmento: str | None


@dataclass(frozen=True)
class MallaSpec:
    """Lo que distingue a una malla. Viene del catálogo, no del código."""

    codigo: str
    nombre_caso: str
    prefijo_ejecucion: str
    state_machine: str
    prg_pk: str
    prg_sk: str
    total_ciclos: int
    estados_remediacion: tuple[str, ...]
    reglas_periodo: ReglasPeriodo
    incluir_rango_fechas: bool

    @property
    def pk(self):
        return f"{PREFIJO_MALLA}{self.codigo}"

    @property
    def slug(self):
        return self.codigo.lower().replace("_", "-")

    def ejc_pk(self, periodo):
        return f"{PREFIJO_EJC}{self.codigo}#{periodo}"


@dataclass(frozen=True)
class Invocacion:
    request_id: str
    tiempo_restante_ms: int


@dataclass(frozen=True)
class Evento:
    malla: str
    publicar: bool
    ejecutar: bool
    periodo_negocio: str | None
    modo_ejecucion: str
    origen: str
    clave_idempotencia: str | None

    @classmethod
    def desde_dict(cls, event, malla_por_defecto=None):
        if event is None:
            event = {}
        if not isinstance(event, dict):
            raise ErrorEntrada("El evento debe ser un objeto JSON", codigo="CFG-ENT-001")

        publicar = _booleano(event, "publicar", False)
        ejecutar = _booleano(event, "ejecutar", publicar)
        if ejecutar and not publicar:
            raise ErrorEntrada("Para ejecutar la Step Function se requiere publicar=true",
                               codigo="CFG-ENT-001")

        malla = event.get("malla") or malla_por_defecto
        if not isinstance(malla, str) or not malla.strip():
            raise ErrorEntrada("No se indicó la malla (campo 'malla' o variable MALLA_PK)",
                               codigo="CFG-ENT-004")

        periodo = event.get("periodoNegocio")
        if periodo is not None:
            periodo = validar_formato(periodo)

        clave = event.get("idempotencyKey")
        if clave is not None and (not isinstance(clave, str)
                                  or not _CLAVE_IDEMPOTENCIA.fullmatch(clave)):
            raise ErrorEntrada("idempotencyKey inválida (1-128 caracteres: letras, "
                               "números, punto, guion, dos puntos)", codigo="CFG-ENT-001")

        return cls(
            malla=malla.strip().removeprefix(PREFIJO_MALLA),
            publicar=publicar,
            ejecutar=ejecutar,
            periodo_negocio=periodo,
            modo_ejecucion=_texto(event, "modoEjecucion", MODO_POR_DEFECTO),
            origen=_texto(event, "origen", ORIGEN_POR_DEFECTO),
            clave_idempotencia=clave,
        )


@dataclass
class Resultado:
    malla: str
    publicar: bool
    ejecutar: bool
    periodo_negocio: str
    registros_cnf: int
    ejecucion_id: str
    estado: str = "VALIDADO"
    publicados: list = field(default_factory=list)
    execution_name: str | None = None
    execution_arn: str | None = None
    ejc_pk: str | None = None
    ejc_sk: str | None = None
    reutilizado: bool = False

    def to_dict(self):
        return {
            "estado": self.estado,
            "malla": self.malla,
            "publicar": self.publicar,
            "ejecutar": self.ejecutar,
            "periodoNegocio": self.periodo_negocio,
            "registrosCNF": self.registros_cnf,
            "ejecucionId": self.ejecucion_id,
            "publicados": self.publicados,
            "executionName": self.execution_name,
            "executionArn": self.execution_arn,
            "ejcPK": self.ejc_pk,
            "ejcSK": self.ejc_sk,
            "reutilizado": self.reutilizado,
        }

    @classmethod
    def desde_dict(cls, datos):
        return cls(
            estado=datos["estado"],
            malla=datos["malla"],
            publicar=datos["publicar"],
            ejecutar=datos["ejecutar"],
            periodo_negocio=datos["periodoNegocio"],
            registros_cnf=datos["registrosCNF"],
            ejecucion_id=datos["ejecucionId"],
            publicados=list(datos.get("publicados", [])),
            execution_name=datos.get("executionName"),
            execution_arn=datos.get("executionArn"),
            ejc_pk=datos.get("ejcPK"),
            ejc_sk=datos.get("ejcSK"),
            reutilizado=bool(datos.get("reutilizado", False)),
        )


def _booleano(event, nombre, por_defecto):
    if nombre not in event:
        return por_defecto
    valor = event[nombre]
    if not isinstance(valor, bool):  # evita que "false" (texto) se tome como verdadero
        raise ErrorEntrada(f"'{nombre}' debe ser true o false", codigo="CFG-ENT-001")
    return valor


def _texto(event, nombre, por_defecto):
    valor = event.get(nombre, por_defecto)
    if not isinstance(valor, str) or not valor.strip():
        raise ErrorEntrada(f"'{nombre}' debe ser un texto no vacío", codigo="CFG-ENT-001")
    return valor.strip()
