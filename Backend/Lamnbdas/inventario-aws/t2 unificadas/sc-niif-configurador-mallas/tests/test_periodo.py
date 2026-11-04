from datetime import date

import pytest

from configurador_mallas.dominio import periodo
from configurador_mallas.dominio.errores import ErrorConfiguracion, ErrorEntrada
from configurador_mallas.dominio.modelos import ReglasPeriodo

REGLAS = ReglasPeriodo(("FEC_INICIAL", "FEC_APROBACION_PENSION"), "PERIODO", r"niif17_([0-9]{6})_")
SIN_REGLAS = ReglasPeriodo((), None, None)


def test_rango_de_diciembre_cruza_el_anio():
    assert periodo.rango_mes("202612") == (date(2026, 12, 1), date(2027, 1, 1), date(2026, 12, 31))


@pytest.mark.parametrize("valor", [None, 202610, "20261", "2026-10", "202600", "202613"])
def test_formato_invalido(valor):
    with pytest.raises(ErrorEntrada):
        periodo.validar_formato(valor)


def test_resuelve_marcadores_y_periodo_anterior():
    texto = "s3://x/${PERIODO_NEGOCIO}/${FECHA_INICIO}/${FECHA_SIGUIENTE_MES}/${FECHA_FIN}/202609"
    assert periodo.resolver_texto(texto, "202609", "202602") == (
        "s3://x/202602/2026-02-01/2026-03-01/2026-02-28/202602")


def test_resuelve_estructuras_anidadas_sin_tocar_otros_tipos():
    contenido = {"a": ["202609", {"b": "${PERIODO_NEGOCIO}"}], "n": 3, "v": None}
    assert periodo.resolver_estructura(contenido, "202609", "202610") == {
        "a": ["202610", {"b": "202610"}], "n": 3, "v": None}


def test_where_ajusta_cada_operador_de_las_columnas_configuradas():
    where = ("t.FEC_INICIAL >= DATE '2025-01-01' AND FEC_INICIAL < DATE '${FECHA_SIGUIENTE_MES}' "
             "AND fec_aprobacion_pension <= DATE '2025-01-31' AND OTRA_FECHA >= DATE '2020-01-01' "
             "AND PERIODO = '202501'")
    resultado = periodo.resolver_where(where, "202501", "202610", REGLAS)
    assert "t.FEC_INICIAL >= DATE '2026-10-01'" in resultado
    assert "FEC_INICIAL < DATE '2026-11-01'" in resultado
    assert "fec_aprobacion_pension <= DATE '2026-10-31'" in resultado
    assert "OTRA_FECHA >= DATE '2020-01-01'" in resultado  # columna no configurada: no cambia
    assert "PERIODO = '202610'" in resultado


def test_where_sin_reglas_solo_cambia_el_periodo():
    assert periodo.resolver_where("PERIODO = '202501'", "202501", "202610",
                                  SIN_REGLAS) == "PERIODO = '202610'"


def test_preparar_periodo_no_modifica_la_entrada(cnf):
    original = repr(cnf)
    salida, antes = periodo.preparar_periodo(cnf, "202610", REGLAS)
    assert repr(cnf) == original
    assert antes == cnf
    control = salida["EXTRACCION"]["controlExtraccion"]
    assert control["periodo_negocio"] == "202610"
    assert control["ruta_estado"] == "s3://assets/estado/202610/estado.json"


def test_preparar_periodo_ajusta_filtros_y_segmentos(cnf):
    salida, _ = periodo.preparar_periodo(cnf, "202610", REGLAS)
    uso = salida["CICLO#001"]["segmentos"][0]["estructuras"][1]
    assert "PERIODO = '202610'" in uso["where"]
    segmento = salida["CICLO#002"]["segmentos"][0]["estructuras"][0]
    assert segmento["where"] == "FEC_APROBACION_PENSION <= DATE '2026-10-31'"
    assert segmento["segmentacionHorizontal"] == {
        "periodo_negocio": "202610", "identificador": "niif17_202610_lote",
        "ruta": "s3://bronce/niif17_202610_x/"}


def test_segmentacion_sin_patron_usa_solo_el_periodo_declarado(cnf):
    reglas = ReglasPeriodo((), None, None)
    salida, _ = periodo.preparar_periodo(cnf, "202610", reglas)
    segmento = salida["CICLO#002"]["segmentos"][0]["estructuras"][0]["segmentacionHorizontal"]
    assert segmento["identificador"] == "niif17_202610_lote"


def test_where_que_no_es_texto(cnf):
    cnf["CICLO#001"]["segmentos"][0]["estructuras"][0]["where"] = ["no", "texto"]
    with pytest.raises(ErrorConfiguracion, match="where"):
        periodo.preparar_periodo(cnf, "202610", REGLAS)


def test_sin_periodo_base(cnf):
    del cnf["EXTRACCION"]["controlExtraccion"]["periodo_negocio"]
    with pytest.raises(ErrorConfiguracion):
        periodo.preparar_periodo(cnf, "202610", REGLAS)


def test_mismo_periodo_es_idempotente(cnf):
    primera, _ = periodo.preparar_periodo(cnf, "202610", REGLAS)
    segunda, antes = periodo.preparar_periodo(primera, "202610", REGLAS)
    assert segunda == antes
