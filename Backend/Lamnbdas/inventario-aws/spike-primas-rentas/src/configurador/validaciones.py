"""Validaciones de la configuración leída y de los JSON generados."""
import json
import re

from .errores import ConfiguracionError
from .json_utils import normalizar

_MARCADORES = re.compile(
    r"\$\{(?:PERIODO_NEGOCIO|FECHA_INICIO|FECHA_SIGUIENTE_MES|FECHA_FIN)\}"
)
_BASE = ("GENERAL", "EXTRACCION", "VALIDACION", "REMEDIACION", "SCD")


def validar_registros_cnf(cnf, spec):
    if len(cnf) != spec.total_registros_cnf:
        raise ConfiguracionError(
            f"CNF esperada={spec.total_registros_cnf}, encontrada={len(cnf)}"
        )
    requeridos = set(_BASE) | {f"CICLO#{i:03d}" for i in range(1, spec.total_ciclos + 1)}
    faltantes = sorted(requeridos - set(cnf))
    if faltantes:
        raise ConfiguracionError(f"Registros CNF faltantes: {faltantes}")


def validar_generados(generados):
    for nombre, contenido in generados.items():
        if not isinstance(contenido, dict) or not contenido:
            raise ConfiguracionError(f"JSON vacio o invalido: {nombre}")
        if _MARCADORES.search(json.dumps(normalizar(contenido))):
            raise ConfiguracionError(f"Marcadores de periodo sin resolver: {nombre}")

    extraccion = generados["EXTRACCION"]
    if extraccion["periodo_negocio"] != extraccion["control_extraccion"]["periodo_negocio"]:
        raise ConfiguracionError("Periodo inconsistente en extraccion")
