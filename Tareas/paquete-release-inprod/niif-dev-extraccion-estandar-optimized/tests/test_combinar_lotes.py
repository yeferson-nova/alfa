import ast
from pathlib import Path

import pytest

RUTA = Path(__file__).resolve().parents[1] / "extraccion_estandar_optimized.py"
FUENTE = RUTA.read_text(encoding="utf-8")


def cargar_funcion(nombre):
    nodo = next(
        n for n in ast.parse(FUENTE).body
        if isinstance(n, ast.FunctionDef) and n.name == nombre
    )
    espacio = {}
    modulo = ast.Module(body=[nodo], type_ignores=[])
    exec(compile(modulo, str(RUTA), "exec"), espacio)
    return espacio[nombre]


@pytest.fixture(scope="module")
def combinar():
    return cargar_funcion("combinar_lotes_respaldados")


def test_une_respaldo_y_actual_cuando_hay_datos_nuevos(combinar):
    resultado = combinar([1793982916], 1793985760, True, "t")
    assert resultado == [1793982916, 1793985760]


def test_no_agrega_el_actual_si_no_hay_datos_nuevos(combinar):
    assert combinar([1793982916], 1793985760, False, "t") == [1793982916]


def test_ordena_y_elimina_duplicados(combinar):
    assert combinar(["30", 10, 10], 20, True, "t") == [10, 20, 30]


@pytest.mark.parametrize("previos", [None, [], set()])
def test_sin_respaldo_falla_nombrando_la_tabla(combinar, previos):
    with pytest.raises(ValueError) as error:
        combinar(previos, 1, True, "arpprod/par_endosos")
    assert "arpprod/par_endosos" in str(error.value)
