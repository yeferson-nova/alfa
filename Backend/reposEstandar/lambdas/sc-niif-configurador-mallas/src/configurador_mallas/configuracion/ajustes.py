"""Ajustes del entorno: se leen y validan una sola vez, en el arranque en frío.

No hay valores por defecto de negocio ("el motor no decide, obedece"): si falta
una variable requerida, la función falla con un error clasificado.
"""
import os
from dataclasses import dataclass

from ..dominio.errores import ErrorConfiguracion

_REQUERIDAS = (
    "AWS_REGION",
    "CNF_TABLE",
    "PRS_TABLE",
    "PRG_TABLE",
    "EJC_TABLE",
    "IDEMPOTENCIA_TABLE",
    "IDEMPOTENCIA_TTL_SEGUNDOS",
    "CATALOGO_MALLAS_URI",
    "METRICAS_NAMESPACE",
)
NIVELES_LOG = ("DEBUG", "INFO", "WARNING", "ERROR")
NIVEL_LOG_POR_DEFECTO = "INFO"


@dataclass(frozen=True)
class Ajustes:
    region: str
    cnf_table: str
    prs_table: str
    prg_table: str
    ejc_table: str
    idempotencia_table: str
    idempotencia_ttl_segundos: int
    catalogo_uri: str
    metricas_namespace: str
    malla_por_defecto: str | None
    nivel_log: str

    @classmethod
    def desde_entorno(cls, entorno=None):
        entorno = os.environ if entorno is None else entorno
        faltantes = [v for v in _REQUERIDAS if not entorno.get(v)]
        if faltantes:
            raise ErrorConfiguracion(
                "Variables de entorno faltantes",
                codigo="CFG-CNF-001",
                detalle={"variables": faltantes},
            )
        return cls(
            region=entorno["AWS_REGION"],
            cnf_table=entorno["CNF_TABLE"],
            prs_table=entorno["PRS_TABLE"],
            prg_table=entorno["PRG_TABLE"],
            ejc_table=entorno["EJC_TABLE"],
            idempotencia_table=entorno["IDEMPOTENCIA_TABLE"],
            idempotencia_ttl_segundos=_entero_positivo(entorno, "IDEMPOTENCIA_TTL_SEGUNDOS"),
            catalogo_uri=entorno["CATALOGO_MALLAS_URI"],
            metricas_namespace=entorno["METRICAS_NAMESPACE"],
            # Solo para el paso T2-A (una función por malla). En T2-B la malla llega en el evento.
            malla_por_defecto=entorno.get("MALLA_PK") or None,
            nivel_log=nivel_log(entorno),
        )


def nivel_log(entorno=None):
    entorno = os.environ if entorno is None else entorno
    nivel = (entorno.get("NIVEL_LOG") or NIVEL_LOG_POR_DEFECTO).upper()
    if nivel not in NIVELES_LOG:
        raise ErrorConfiguracion(
            "NIVEL_LOG inválido", codigo="CFG-CNF-001", detalle={"permitidos": list(NIVELES_LOG)}
        )
    return nivel


def _entero_positivo(entorno, nombre):
    try:
        valor = int(entorno[nombre])
    except ValueError:
        valor = 0
    if valor <= 0:
        raise ErrorConfiguracion(
            f"{nombre} debe ser un entero positivo", codigo="CFG-CNF-001"
        )
    return valor
