"""Reglas del periodo de negocio. Funciones puras: no conocen AWS.

Trasladan un periodo ``YYYYMM`` a la configuración de la malla: control de
extracción, filtros ``where`` de cada estructura y segmentación horizontal. Las
columnas de fecha y el patrón de los segmentos vienen del catálogo de mallas.
"""
import copy
import re
from datetime import date, timedelta

from .errores import ErrorConfiguracion, ErrorEntrada

MARCADOR_PERIODO = "${PERIODO_NEGOCIO}"
MARCADOR_INICIO = "${FECHA_INICIO}"
MARCADOR_SIGUIENTE = "${FECHA_SIGUIENTE_MES}"
MARCADOR_FIN = "${FECHA_FIN}"
MARCADORES = (MARCADOR_PERIODO, MARCADOR_INICIO, MARCADOR_SIGUIENTE, MARCADOR_FIN)

_FORMATO = re.compile(r"[0-9]{6}")
_FECHA_O_MARCADOR = (
    r"'(?:\d{4}-\d{2}-\d{2}|\$\{FECHA_INICIO\}|\$\{FECHA_SIGUIENTE_MES\}|\$\{FECHA_FIN\})'"
)


def validar_formato(valor):
    if not isinstance(valor, str) or not _FORMATO.fullmatch(valor):
        raise ErrorEntrada("periodoNegocio debe tener formato YYYYMM", codigo="CFG-ENT-002")
    rango_mes(valor)
    return valor


def rango_mes(periodo):
    """(primer día, primer día del mes siguiente, último día) del periodo."""
    try:
        inicio = date(int(periodo[:4]), int(periodo[4:]), 1)
    except ValueError:
        raise ErrorEntrada("periodoNegocio contiene un mes inválido",
                           codigo="CFG-ENT-002") from None
    siguiente = date(inicio.year + 1, 1, 1) if inicio.month == 12 else date(
        inicio.year, inicio.month + 1, 1)
    return inicio, siguiente, siguiente - timedelta(days=1)


def resolver_texto(texto, viejo, nuevo):
    """Cambia el periodo anterior por el nuevo y resuelve los marcadores de fecha."""
    inicio, siguiente, fin = rango_mes(nuevo)
    resultado = texto.replace(str(viejo), nuevo)
    for marcador, valor in (
        (MARCADOR_PERIODO, nuevo),
        (MARCADOR_INICIO, inicio.isoformat()),
        (MARCADOR_SIGUIENTE, siguiente.isoformat()),
        (MARCADOR_FIN, fin.isoformat()),
    ):
        resultado = resultado.replace(marcador, valor)
    return resultado


def resolver_estructura(contenido, viejo, nuevo):
    """Aplica ``resolver_texto`` a todos los textos de una estructura anidada."""
    if isinstance(contenido, dict):
        return {k: resolver_estructura(v, viejo, nuevo) for k, v in contenido.items()}
    if isinstance(contenido, list):
        return [resolver_estructura(v, viejo, nuevo) for v in contenido]
    if isinstance(contenido, str):
        return resolver_texto(contenido, viejo, nuevo)
    return contenido


def resolver_where(where, viejo, nuevo, reglas):
    """Ajusta un filtro SQL al periodo.

    Para las columnas de fecha configuradas: ``>=`` toma el primer día, ``<`` el
    primer día del mes siguiente y ``<=`` el último día. La columna de periodo
    (``COL = 'YYYYMM'``) toma el periodo nuevo.
    """
    inicio, siguiente, fin = rango_mes(nuevo)
    resultado = where
    if reglas.columnas_fecha:
        columnas = "|".join(re.escape(c) for c in reglas.columnas_fecha)
        patron = (rf"(\b(?:[A-Za-z_][A-Za-z_0-9]*\.)?(?:{columnas})\s*)(>=|<=|<)(\s*DATE\s*)"
                  + _FECHA_O_MARCADOR)
        valores = {">=": inicio, "<": siguiente, "<=": fin}
        resultado = re.sub(
            patron,
            lambda m: f"{m.group(1)}{m.group(2)}{m.group(3)}'{valores[m.group(2)].isoformat()}'",
            resultado,
            flags=re.I,
        )
    if reglas.columna_periodo:
        resultado = re.sub(
            rf"(\b{re.escape(reglas.columna_periodo)}\s*=\s*)'\d{{6}}'",
            lambda m: f"{m.group(1)}'{nuevo}'",
            resultado,
            flags=re.I,
        )
    return resolver_texto(resultado, viejo, nuevo)


def periodo_base(cnf):
    try:
        return str(cnf["EXTRACCION"]["controlExtraccion"]["periodo_negocio"])
    except KeyError:
        raise ErrorConfiguracion("EXTRACCION.controlExtraccion.periodo_negocio no existe",
                                 codigo="CFG-CNF-004") from None


def preparar_periodo(cnf, periodo_nuevo, reglas):
    """Aplica el periodo a la configuración de la malla.

    Devuelve ``(salida, antes)``. ``antes`` es una copia intacta de la entrada, que
    se usa como condición de la escritura optimista. ``cnf`` no se modifica.
    """
    viejo = periodo_base(cnf)
    nuevo = validar_formato(periodo_nuevo)
    antes = copy.deepcopy(cnf)
    salida = copy.deepcopy(cnf)

    control = resolver_estructura(salida["EXTRACCION"]["controlExtraccion"], viejo, nuevo)
    control["periodo_negocio"] = nuevo
    salida["EXTRACCION"]["controlExtraccion"] = control

    for clave, item in salida.items():
        if not clave.startswith("CICLO#"):
            continue
        for segmento in item.get("segmentos", []):
            for uso in segmento.get("estructuras", []):
                _ajustar_uso(clave, uso, viejo, nuevo, reglas)
    return salida, antes


def _ajustar_uso(clave, uso, viejo, nuevo, reglas):
    where = uso.get("where")
    if where is not None:
        if not isinstance(where, str):
            raise ErrorConfiguracion(f"'where' debe ser texto en {clave}", codigo="CFG-CNF-004")
        uso["where"] = resolver_where(where, viejo, nuevo, reglas)

    segmentacion = uso.get("segmentacionHorizontal")
    if isinstance(segmentacion, dict) and segmentacion:
        uso["segmentacionHorizontal"] = _ajustar_segmentacion(segmentacion, nuevo, reglas)


def _ajustar_segmentacion(segmentacion, nuevo, reglas):
    """Cambia los periodos dentro de los identificadores del segmento.

    Solo toca la configuración del segmento; no renombra objetos en S3.
    """
    viejos = set()
    valor = segmentacion.get("periodo_negocio")
    if valor is not None and _FORMATO.fullmatch(str(valor)):
        viejos.add(str(valor))
    if reglas.patron_segmento:
        for texto in segmentacion.values():
            if isinstance(texto, str):
                viejos.update(re.findall(reglas.patron_segmento, texto))

    def actualizar(contenido):
        if isinstance(contenido, dict):
            return {k: actualizar(v) for k, v in contenido.items()}
        if isinstance(contenido, list):
            return [actualizar(v) for v in contenido]
        if isinstance(contenido, str):
            for viejo in sorted(viejos):
                contenido = re.sub(rf"(?<![0-9]){viejo}(?![0-9])", nuevo, contenido)
        return contenido

    salida = actualizar(segmentacion)
    salida["periodo_negocio"] = nuevo
    return salida
