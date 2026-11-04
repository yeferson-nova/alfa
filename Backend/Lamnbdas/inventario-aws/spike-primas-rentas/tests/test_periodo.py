import pytest

from configurador.errores import ConfiguracionError
from configurador.periodo import (
    aplicar_periodo_runtime,
    preparar_periodo,
    rango_mes,
    validar_formato,
)
from fakes import cnf_ejemplo


@pytest.mark.parametrize("valor", ["202609", "199912"])
def test_formato_valido(valor):
    assert validar_formato(valor) == valor


@pytest.mark.parametrize("valor", ["2026-09", "20269", "abcdef", None, 202609, "２０２６０９"])
def test_formato_invalido(valor):
    with pytest.raises(ConfiguracionError):
        validar_formato(valor)


def test_rango_mes_diciembre_cruza_anio():
    inicio, siguiente, fin = rango_mes("202612")
    assert (inicio.isoformat(), siguiente.isoformat(), fin.isoformat()) == (
        "2026-12-01",
        "2027-01-01",
        "2026-12-31",
    )


def test_rango_mes_invalido():
    with pytest.raises(ConfiguracionError):
        rango_mes("202613")


def test_aplicar_periodo_recorre_estructuras_anidadas():
    entrada = {"a": ["x202609", {"b": "202609"}], "n": 202609, "z": None}
    assert aplicar_periodo_runtime(entrada, "202609", "202610") == {
        "a": ["x202610", {"b": "202610"}],
        "n": 202609,
        "z": None,
    }


def test_preparar_periodo_cambia_control_y_where_sin_mutar_entrada():
    cnf = cnf_ejemplo()
    original = cnf_ejemplo()
    salida, antes = preparar_periodo(cnf, "202610")

    assert cnf == original  # no muta la entrada
    assert antes == original  # copia intacta para la condición optimista
    assert salida["EXTRACCION"]["controlExtraccion"]["periodo_negocio"] == "202610"
    assert "202610" in salida["EXTRACCION"]["controlExtraccion"]["ruta"]
    where = salida["CICLO#001"]["segmentos"][0]["estructuras"][0]["where"]
    assert where == "PERIODO = '202610' AND FEC_APROBACION_PENSION <= DATE '2026-10-31'"


def test_preparar_periodo_sin_nuevo_conserva_el_actual():
    salida, _ = preparar_periodo(cnf_ejemplo(), None)
    assert salida["EXTRACCION"]["controlExtraccion"]["periodo_negocio"] == "202609"


def test_where_no_texto_falla():
    cnf = cnf_ejemplo()
    cnf["CICLO#001"]["segmentos"][0]["estructuras"][0]["where"] = {"no": "texto"}
    with pytest.raises(ConfiguracionError, match="where debe ser texto"):
        preparar_periodo(cnf, "202610")
