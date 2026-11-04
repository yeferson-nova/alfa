"""Reglas de periodo de negocio. Funciones puras, sin AWS."""
import copy
import re
from datetime import date, timedelta

from .errores import ConfiguracionError

# Corte de pensión que el código original marca como "identificado en ARP".
# PENDIENTE: confirmar si aplica a Primas Rentas.
_CORTE_PENSION = re.compile(
    r"(\bFEC_APROBACION_PENSION\s*<=\s*DATE\s*)'\d{4}-\d{2}-\d{2}'", re.I
)


def validar_formato(valor):
    if not isinstance(valor, str) or not re.fullmatch(r"[0-9]{6}", valor):
        raise ConfiguracionError("periodoNegocio debe tener formato YYYYMM")
    return valor


def rango_mes(periodo):
    """Devuelve (primer día, primer día del mes siguiente, último día)."""
    try:
        inicio = date(int(periodo[:4]), int(periodo[4:]), 1)
        siguiente = (
            date(inicio.year + 1, 1, 1)
            if inicio.month == 12
            else date(inicio.year, inicio.month + 1, 1)
        )
    except ValueError:
        raise ConfiguracionError("periodoNegocio contiene mes o anio invalido") from None
    return inicio, siguiente, siguiente - timedelta(days=1)


def aplicar_periodo_runtime(contenido, periodo_anterior, periodo_nuevo):
    """Reemplaza el periodo en todos los textos de una estructura anidada."""
    if isinstance(contenido, dict):
        return {
            k: aplicar_periodo_runtime(v, periodo_anterior, periodo_nuevo)
            for k, v in contenido.items()
        }
    if isinstance(contenido, list):
        return [aplicar_periodo_runtime(v, periodo_anterior, periodo_nuevo) for v in contenido]
    if isinstance(contenido, str):
        return contenido.replace(periodo_anterior, periodo_nuevo)
    return contenido


def periodo_base(cnf):
    return str(cnf["EXTRACCION"]["controlExtraccion"]["periodo_negocio"])


def preparar_periodo(cnf, periodo_nuevo):
    """Aplica ``periodo_nuevo`` a la configuración.

    Devuelve ``(salida, antes)``: ``antes`` es copia intacta de la entrada, para
    la condición de la escritura optimista. ``cnf`` no se modifica.
    """
    viejo = periodo_base(cnf)
    nuevo = validar_formato(viejo if periodo_nuevo is None else periodo_nuevo)
    _, _, fin = rango_mes(nuevo)

    antes = copy.deepcopy(cnf)
    salida = copy.deepcopy(cnf)

    control = aplicar_periodo_runtime(salida["EXTRACCION"]["controlExtraccion"], viejo, nuevo)
    control["periodo_negocio"] = nuevo
    salida["EXTRACCION"]["controlExtraccion"] = control

    for clave, item in salida.items():
        if not clave.startswith("CICLO#"):
            continue
        for segmento in item.get("segmentos", []):
            for uso in segmento.get("estructuras", []):
                where = uso.get("where")
                if where is None:
                    continue
                if not isinstance(where, str):
                    raise ConfiguracionError(f"where debe ser texto: {clave}")
                where = aplicar_periodo_runtime(where, viejo, nuevo)
                uso["where"] = _CORTE_PENSION.sub(
                    lambda m: f"{m.group(1)}'{fin.isoformat()}'", where
                )
    return salida, antes
