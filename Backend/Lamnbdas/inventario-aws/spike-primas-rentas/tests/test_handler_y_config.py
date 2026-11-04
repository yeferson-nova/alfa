import pytest

from configurador import handler
from configurador.config import Ajustes
from configurador.errores import ConfiguracionError


class Ctx:
    aws_request_id = "req-12345678"


def test_handler_delega_en_el_servicio_y_devuelve_dict(monkeypatch, mundo):
    monkeypatch.setattr(handler.composicion, "crear_configurador", lambda: mundo.servicio)
    monkeypatch.setattr(handler, "_servicio", None)
    respuesta = handler.lambda_handler({"periodoNegocio": "202610"}, Ctx())
    assert respuesta["estado"] == "VALIDADO"
    assert respuesta["periodoNegocio"] == "202610"
    assert set(respuesta) == {
        "estado", "malla", "publicar", "ejecutar", "periodoNegocio", "registrosCNF",
        "publicados", "executionName", "executionArn", "ejcPK", "ejcSK",
    }


def test_ajustes_desde_entorno():
    a = Ajustes.desde_entorno(
        {
            "AWS_REGION": "us-east-1",
            "CNF_TABLE": "c",
            "PRS_TABLE": "p",
            "EJC_TABLE": "e",
            "STATE_MACHINE_ARN": "arn",
            "MALLA_PK": "MALLA#PRIMAS_RENTAS",
        }
    )
    assert a.malla_por_defecto == "PRIMAS_RENTAS"


def test_ajustes_fallan_si_falta_una_variable():
    with pytest.raises(ConfiguracionError, match=r"\['CNF_TABLE'"):
        Ajustes.desde_entorno({"AWS_REGION": "us-east-1"})
