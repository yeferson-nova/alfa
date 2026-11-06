import ast
from pathlib import Path

import pytest

RUTA_SCD = Path(__file__).resolve().parents[1] / "scd.py"
FUENTE = RUTA_SCD.read_text(encoding="utf-8")
NOMBRES = {"FrenoApagadoMasivo", "leer_umbral_pct", "evaluar_freno_apagado"}


def cargar_freno():
    nodos = [
        n for n in ast.parse(FUENTE).body
        if isinstance(n, (ast.ClassDef, ast.FunctionDef)) and n.name in NOMBRES
    ]
    assert {n.name for n in nodos} == NOMBRES
    espacio = {}
    exec(compile(ast.Module(body=nodos, type_ignores=[]), str(RUTA_SCD), "exec"), espacio)
    return espacio


@pytest.fixture(scope="module")
def freno():
    return cargar_freno()


def test_incidente_primas_arl_detiene_el_job(freno):
    with pytest.raises(freno["FrenoApagadoMasivo"]) as error:
        freno["evaluar_freno_apagado"](1_203_789, 1_203_789, 20.0, False)
    assert "SCD_APAGADO_MASIVO" in str(error.value)
    assert "Gold no se modifico" in str(error.value)


def test_carga_incremental_sana_no_apaga_nada(freno):
    assert freno["evaluar_freno_apagado"](0, 1_203_789, 20.0, False) == 0.0


def test_baja_normal_por_debajo_del_umbral_continua(freno):
    assert freno["evaluar_freno_apagado"](2_000, 100_000, 20.0, False) == pytest.approx(2.0)


def test_justo_en_el_umbral_continua_y_apenas_encima_detiene(freno):
    assert freno["evaluar_freno_apagado"](20, 100, 20.0, False) == pytest.approx(20.0)
    with pytest.raises(freno["FrenoApagadoMasivo"]):
        freno["evaluar_freno_apagado"](21, 100, 20.0, False)


def test_autorizacion_manual_permite_el_apagado_masivo(freno):
    assert freno["evaluar_freno_apagado"](900, 1_000, 20.0, True) == pytest.approx(90.0)


def test_sin_activos_en_gold_no_divide_por_cero(freno):
    assert freno["evaluar_freno_apagado"](0, 0, 20.0, False) == 0.0


@pytest.mark.parametrize("valor", ["abc", None, "-1", "101", "nan"])
def test_umbral_invalido_falla_en_vez_de_ignorarse(freno, valor):
    with pytest.raises(ValueError):
        freno["leer_umbral_pct"](valor)


@pytest.mark.parametrize("valor,esperado", [("0", 0.0), ("20", 20.0), ("100", 100.0), (35.5, 35.5)])
def test_umbral_valido(freno, valor, esperado):
    assert freno["leer_umbral_pct"](valor) == esperado


def test_el_umbral_es_opcional_y_tiene_defecto():
    definicion = FUENTE[:FUENTE.index("args = getResolvedOptions(")]
    assert '"UMBRAL_APAGADO_PCT"' in definicion
    assert "UMBRAL_APAGADO_PCT_DEFECTO = 20.0" in definicion


def test_el_freno_se_evalua_antes_de_escribir_gold_en_el_camino_scd():
    llamada = FUENTE.index("evaluar_freno_apagado(\n                total_apagados")
    escritura = FUENTE.index("df_final.write")
    assert llamada < escritura


def test_al_frenar_se_registra_el_fallo_y_se_relanza():
    bloque = FUENTE[FUENTE.index("except FrenoApagadoMasivo"):FUENTE.index("porcentaje_apagado:.2f")]
    assert '"estado_gold_scd"] = "FAILED"' in bloque
    assert "guardar_json_s3(ruta_parametros_extraccion" in bloque
    assert bloque.rstrip().split("\n")[-2].strip() == "raise"
