import pytest

from configurador import constructores
from configurador.errores import ConfiguracionError
from configurador.mallas import obtener_malla
from fakes import FakeAlmacen, FakePrs, cnf_ejemplo

SPEC = obtener_malla("PRIMAS_RENTAS")


def test_ciclos_y_tablas_ordenados_por_orden():
    ciclos = constructores.construir_ciclos(cnf_ejemplo(), FakePrs(), SPEC)
    assert [c["nombre"] for c in ciclos] == [f"ciclo {n}" for n in range(1, 5)]
    tablas = ciclos[0]["segmentos"][0]["tablas"]
    assert [t["orden"] for t in tablas] == [1, 2]
    assert tablas[0]["tabla_relativa"] == "sch/tabla_a1"
    assert "descripcion" not in ciclos[0]  # opcional: solo si existe


def test_ciclos_cantidad_incorrecta():
    cnf = cnf_ejemplo()
    del cnf["CICLO#004"]
    with pytest.raises(ConfiguracionError, match="Ciclos esperados=4"):
        constructores.construir_ciclos(cnf, FakePrs(), SPEC)


def test_estructura_inexistente_en_prs():
    with pytest.raises(ConfiguracionError, match="Estructura inexistente"):
        constructores.construir_ciclos(cnf_ejemplo(), FakePrs(existentes=set()), SPEC)


def test_extraccion_incluye_periodo_y_ciclos():
    x = constructores.construir_extraccion(cnf_ejemplo(), FakePrs(), SPEC)
    assert x["nombre_caso"] == "primas_rentas"
    assert x["periodo_negocio"] == "202609"
    assert len(x["ciclos"]) == 4


def test_scd_exige_estructura_gold():
    with pytest.raises(ConfiguracionError, match="Gold inexistente"):
        constructores.construir_scd(cnf_ejemplo(), FakePrs(existentes=set()))
    assert constructores.construir_scd(cnf_ejemplo(), FakePrs())["llaves"] == ["k"]


def test_remediacion_valida_caso_y_estado():
    cnf = cnf_ejemplo()
    assert constructores.construir_remediacion(cnf, FakeAlmacen(), SPEC)["nombre_caso"] == "primas_rentas"

    with pytest.raises(ConfiguracionError, match="no corresponde"):
        constructores.construir_remediacion(cnf, FakeAlmacen({"nombre_caso": "otra"}), SPEC)

    cnf["REMEDIACION"]["estadoDefinicion"] = "DEFINITIVA"
    with pytest.raises(ConfiguracionError, match="Estado de remediacion"):
        constructores.construir_remediacion(cnf, FakeAlmacen(), SPEC)
