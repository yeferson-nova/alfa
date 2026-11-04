"""Validaciones de la configuración leída y de los JSON generados."""
import json
import re

from .errores import ErrorConfiguracion
from .serializacion import normalizar
from .periodo import MARCADORES

REGISTROS_BASE = ("GENERAL", "EXTRACCION", "VALIDACION", "REMEDIACION", "SCD")


def claves_ciclo(total):
    return [f"CICLO#{i:03d}" for i in range(1, total + 1)]


def validar_registros_cnf(cnf, spec):
    requeridos = set(REGISTROS_BASE) | set(claves_ciclo(spec.total_ciclos))
    faltantes = sorted(requeridos - set(cnf))
    if faltantes:
        raise ErrorConfiguracion("Registros CNF faltantes", codigo="CFG-CNF-004",
                                 detalle={"faltantes": faltantes})
    # Las lambdas anteriores exigían el número exacto de registros: tampoco se aceptan sobrantes.
    sobrantes = sorted(set(cnf) - requeridos)
    if sobrantes:
        raise ErrorConfiguracion("Hay registros CNF que no corresponden al catálogo",
                                 codigo="CFG-CNF-004", detalle={"sobrantes": sobrantes})
    nombre_caso = cnf["GENERAL"].get("nombreCaso")
    if nombre_caso != spec.nombre_caso:
        raise ErrorConfiguracion(
            "GENERAL.nombreCaso no coincide con el catálogo",
            codigo="CFG-CNF-010",
            detalle={"cnf": nombre_caso, "catalogo": spec.nombre_caso},
        )


def validar_generados(generados, spec):
    for nombre, contenido in generados.items():
        if not isinstance(contenido, dict) or not contenido:
            raise ErrorConfiguracion(f"JSON generado vacío o inválido: {nombre}",
                                     codigo="CFG-CNF-008")
        texto = json.dumps(normalizar(contenido))
        pendientes = [m for m in MARCADORES if m in texto]
        if pendientes:
            raise ErrorConfiguracion(f"Marcadores de periodo sin resolver en {nombre}",
                                     codigo="CFG-CNF-008", detalle={"marcadores": pendientes})

    extraccion = generados["EXTRACCION"]
    periodo = extraccion["periodo_negocio"]
    if periodo != extraccion["control_extraccion"]["periodo_negocio"]:
        raise ErrorConfiguracion("Periodo inconsistente en la extracción", codigo="CFG-CNF-008")
    if spec.reglas_periodo.ajustar_segmentacion:
        _validar_segmentos(extraccion, periodo, spec.reglas_periodo.patron_segmento)


def _validar_segmentos(extraccion, periodo, patron):
    for ciclo in extraccion.get("ciclos", []):
        for segmento in ciclo.get("segmentos", []):
            for tabla in segmento.get("tablas", []):
                segmentacion = tabla.get("segmentacion_horizontal")
                if not isinstance(segmentacion, dict) or not segmentacion:
                    continue
                if segmentacion.get("periodo_negocio") != periodo:
                    raise ErrorConfiguracion("Periodo de segmento inconsistente",
                                             codigo="CFG-CNF-008",
                                             detalle={"tabla": tabla.get("tabla_relativa")})
                if patron and any(
                        p != periodo
                        for valor in segmentacion.values() if isinstance(valor, str)
                        for p in re.findall(patron, valor)):
                    raise ErrorConfiguracion("Identificador o ruta de segmento inconsistente",
                                             codigo="CFG-CNF-008",
                                             detalle={"tabla": tabla.get("tabla_relativa")})
