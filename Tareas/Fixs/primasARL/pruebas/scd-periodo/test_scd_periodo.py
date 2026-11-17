import pytest

pytest.importorskip("pyspark")

from soporte.escenarios_scd import CONFIG_BASE, correr_scd, filas_base, preparar_silver
from soporte.instantanea import verificar


def test_periodo_nuevo_inactiva_el_anterior_sin_freno(entorno):
    preparar_silver(entorno, filas_base([(1, "A", "10.00", "202511", 1), (2, "A", "20.00", "202511", 1)]))
    pasos = [correr_scd(entorno, CONFIG_BASE, 1, periodo="202511")]
    preparar_silver(entorno, filas_base([(3, "A", "30.00", "202512", 2)]))
    pasos.append(correr_scd(entorno, CONFIG_BASE, 2, periodo="202512"))
    verificar("periodo_nuevo_inactiva_el_anterior_sin_freno", pasos)


def test_reejecucion_del_mismo_periodo_hace_merge(entorno):
    base = [(i, "A", f"{i}0.00", "202511", 1) for i in range(1, 11)]
    preparar_silver(entorno, filas_base(base))
    pasos = [correr_scd(entorno, CONFIG_BASE, 1, periodo="202511")]
    segunda = [(i, "A", f"{i}0.00", "202511", 2) for i in range(1, 10)]
    segunda[0] = (1, "A", "11.00", "202511", 2)
    segunda.append((11, "A", "110.00", "202511", 2))
    preparar_silver(entorno, filas_base(segunda))
    pasos.append(correr_scd(entorno, CONFIG_BASE, 2, periodo="202511"))
    verificar("periodo_reejecucion_del_mismo_periodo_hace_merge", pasos)


def test_reejecucion_del_mismo_periodo_con_perdida_masiva_aplica_el_freno(entorno):
    preparar_silver(entorno, filas_base([(1, "A", "10.00", "202511", 1), (2, "A", "20.00", "202511", 1)]))
    pasos = [correr_scd(entorno, CONFIG_BASE, 1, periodo="202511")]
    preparar_silver(entorno, filas_base([(1, "A", "10.00", "202511", 2)]))
    pasos.append(correr_scd(entorno, CONFIG_BASE, 2, periodo="202511"))
    verificar("periodo_reejecucion_con_perdida_masiva_aplica_el_freno", pasos)


def test_volver_a_un_periodo_ejecutado_reactiva_con_version_siguiente(entorno):
    anterior = [(1, "A", "10.00", "202511", 1), (2, "A", "20.00", "202511", 1)]
    preparar_silver(entorno, filas_base(anterior))
    pasos = [correr_scd(entorno, CONFIG_BASE, 1, periodo="202511")]
    preparar_silver(entorno, filas_base([(3, "A", "30.00", "202512", 2)]))
    pasos.append(correr_scd(entorno, CONFIG_BASE, 2, periodo="202512"))
    preparar_silver(entorno, filas_base([(1, "A", "10.00", "202511", 3), (2, "A", "21.00", "202511", 3), (4, "A", "40.00", "202511", 3)]))
    pasos.append(correr_scd(entorno, CONFIG_BASE, 3, periodo="202511"))
    verificar("periodo_volver_a_un_periodo_ejecutado", pasos)


def test_primera_corrida_sin_control_aplica_el_freno_hasta_permitirlo(entorno):
    preparar_silver(entorno, filas_base([(1, "A", "10.00", "202511", 1), (2, "A", "20.00", "202511", 1)]))
    pasos = [correr_scd(entorno, CONFIG_BASE, 1)]
    preparar_silver(entorno, filas_base([(3, "A", "30.00", "202512", 2)]))
    pasos.append(correr_scd(entorno, CONFIG_BASE, 2, periodo="202512"))
    pasos.append(correr_scd(entorno, CONFIG_BASE, 2, {"PERMITIR_APAGADO_MASIVO": "true"}, periodo="202512"))
    preparar_silver(entorno, filas_base([(4, "A", "40.00", "202601", 3)]))
    pasos.append(correr_scd(entorno, CONFIG_BASE, 3, periodo="202601"))
    verificar("periodo_primera_corrida_sin_control", pasos)
