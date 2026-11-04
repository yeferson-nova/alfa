import pytest

from configurador import validaciones
from configurador.errores import ConfiguracionError
from configurador.mallas import obtener_malla
from fakes import cnf_ejemplo

SPEC = obtener_malla("PRIMAS_RENTAS")


def test_cnf_completo_es_valido():
    validaciones.validar_registros_cnf(cnf_ejemplo(), SPEC)


def test_cnf_con_cantidad_incorrecta():
    cnf = cnf_ejemplo()
    cnf["EXTRA"] = {}
    with pytest.raises(ConfiguracionError, match="CNF esperada=9"):
        validaciones.validar_registros_cnf(cnf, SPEC)


def test_cnf_con_registro_faltante_se_detecta_antes_de_construir():
    cnf = cnf_ejemplo()
    del cnf["SCD"]
    cnf["OTRO"] = {}
    with pytest.raises(ConfiguracionError, match=r"faltantes: \['SCD'\]"):
        validaciones.validar_registros_cnf(cnf, SPEC)


def _generados(periodo="202609"):
    return {
        "EXTRACCION": {"periodo_negocio": periodo, "control_extraccion": {"periodo_negocio": periodo}},
        "VALIDACION": {"a": 1},
    }


def test_generados_validos():
    validaciones.validar_generados(_generados())


def test_json_vacio():
    g = _generados()
    g["VALIDACION"] = {}
    with pytest.raises(ConfiguracionError, match="vacio o invalido: VALIDACION"):
        validaciones.validar_generados(g)


def test_marcador_sin_resolver():
    g = _generados()
    g["VALIDACION"] = {"where": "FEC >= DATE '${FECHA_INICIO}'"}
    with pytest.raises(ConfiguracionError, match="sin resolver: VALIDACION"):
        validaciones.validar_generados(g)


def test_periodo_inconsistente():
    g = _generados()
    g["EXTRACCION"]["control_extraccion"]["periodo_negocio"] = "202610"
    with pytest.raises(ConfiguracionError, match="inconsistente"):
        validaciones.validar_generados(g)
