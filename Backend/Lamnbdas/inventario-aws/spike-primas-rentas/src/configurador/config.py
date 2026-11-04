"""Variables de entorno, leídas y validadas en un solo lugar.

Falla de inmediato si falta una variable: no hay valores por defecto de otro
ambiente (hoy otras lambdas caen a ``niif-qa-*`` si falta una variable).
"""
import os
from dataclasses import dataclass

from .errores import ConfiguracionError

_REQUERIDAS = (
    "AWS_REGION",
    "CNF_TABLE",
    "PRS_TABLE",
    "EJC_TABLE",
    "STATE_MACHINE_ARN",
    "MALLA_PK",
)


@dataclass(frozen=True)
class Ajustes:
    region: str
    cnf_table: str
    prs_table: str
    ejc_table: str
    state_machine_arn: str
    malla_por_defecto: str  # nombre de la malla, sin el prefijo MALLA#

    @classmethod
    def desde_entorno(cls, entorno=None):
        entorno = os.environ if entorno is None else entorno
        faltantes = [v for v in _REQUERIDAS if not entorno.get(v)]
        if faltantes:
            raise ConfiguracionError(f"Variables de entorno faltantes: {faltantes}")
        return cls(
            region=entorno["AWS_REGION"],
            cnf_table=entorno["CNF_TABLE"],
            prs_table=entorno["PRS_TABLE"],
            ejc_table=entorno["EJC_TABLE"],
            state_machine_arn=entorno["STATE_MACHINE_ARN"],
            malla_por_defecto=entorno["MALLA_PK"].removeprefix("MALLA#"),
        )
