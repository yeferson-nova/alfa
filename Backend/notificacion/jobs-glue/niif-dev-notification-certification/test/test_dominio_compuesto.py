import pytest

from constructores import construir_registro_compuesto
from notificacion_certificacion.domain.compuesto import (
    control_ya_completado,
    identificador_compuesto,
    validar_miembro,
)
from notificacion_certificacion.domain.errores import ErrorConcurrencia
from notificacion_certificacion.domain.modelos import ConfiguracionCompuesta


def test_identificador_compuesto_une_grupo_y_periodo():
    configuracion = ConfiguracionCompuesta("t", "grupo", "_CONTROL", ("m1",))
    assert identificador_compuesto(configuracion, "202610") == "grupo#202610"


def test_control_en_curso_no_esta_completado():
    control = construir_registro_compuesto(estado="NOTIFICATION_IN_PROGRESS")
    assert control_ya_completado(control, "C1") is False


def test_control_completado_con_el_mismo_cert_code_se_reconoce():
    control = construir_registro_compuesto(estado="NOTIFICATION_COMPLETED", cert_code="C1")
    assert control_ya_completado(control, "C1") is True


def test_control_completado_para_otra_solicitud_falla():
    control = construir_registro_compuesto(estado="NOTIFICATION_COMPLETED", cert_code="OTRO")
    with pytest.raises(ErrorConcurrencia) as error:
        control_ya_completado(control, "C1")
    assert error.value.codigo == "NTF-CON-001"


@pytest.mark.parametrize(
    "control",
    [
        None,
        construir_registro_compuesto(estado="READY_TO_NOTIFY"),
        construir_registro_compuesto(estado="NOTIFICATION_IN_PROGRESS", actualizado_en=""),
        construir_registro_compuesto(estado="NOTIFICATION_IN_PROGRESS", actualizado_en=None),
    ],
)
def test_control_no_reservado_falla(control):
    with pytest.raises(ErrorConcurrencia) as error:
        control_ya_completado(control, "C1")
    assert "reservado" in error.value.mensaje


def test_miembro_listo_se_acepta():
    miembro = construir_registro_compuesto()
    assert validar_miembro(miembro, "m1", "202610") is miembro


@pytest.mark.parametrize(
    "miembro",
    [
        None,
        construir_registro_compuesto(estado="CONSUMED"),
        construir_registro_compuesto(periodo="202609"),
    ],
)
def test_miembro_no_listo_falla(miembro):
    with pytest.raises(ErrorConcurrencia) as error:
        validar_miembro(miembro, "m1", "202610")
    assert "no listo" in error.value.mensaje


@pytest.mark.parametrize("campo", ["id_ejecucion_origen", "id_lote", "actualizado_en"])
def test_miembro_sin_identidad_completa_falla(campo):
    miembro = construir_registro_compuesto(**{campo: None})
    with pytest.raises(ErrorConcurrencia) as error:
        validar_miembro(miembro, "m1", "202610")
    assert "identidad" in error.value.mensaje
