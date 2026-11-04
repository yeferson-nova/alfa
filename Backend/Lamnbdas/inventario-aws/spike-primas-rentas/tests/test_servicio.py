import pytest

from configurador.errores import ConfiguracionError
from configurador.modelos import Evento


def test_solo_validar_no_escribe_ni_ejecuta(mundo):
    r = mundo.servicio.ejecutar(Evento.desde_dict({}), "req-12345678")
    assert r.estado == "VALIDADO"
    assert r.malla == "MALLA#PRIMAS_RENTAS"
    assert r.registros_cnf == 9
    assert mundo.cnf.persistido is None
    assert mundo.almacen.escrito == []
    assert mundo.orquestador.llamadas == []


def test_publicar_persiste_periodo_y_escribe_los_cuatro_json(mundo):
    r = mundo.servicio.ejecutar(
        Evento.desde_dict({"publicar": True, "ejecutar": False, "periodoNegocio": "202610"}),
        "req-12345678",
    )
    assert r.periodo_negocio == "202610"
    cnf, antes = mundo.cnf.persistido
    assert cnf["EXTRACCION"]["controlExtraccion"]["periodo_negocio"] == "202610"
    assert antes["EXTRACCION"]["controlExtraccion"]["periodo_negocio"] == "202609"

    uris = [u for u, _, _ in mundo.almacen.escrito]
    assert uris == [
        "s3://assets/rentas/config_extraccion.json",
        "s3://assets/rentas/config_val.json",
        "s3://assets/rentas/config_rem.json",
        "s3://assets/rentas/config_scd.json",
    ]
    assert mundo.almacen.escrito[0][2] == {"origen": "dynamodb-prs-cnf", "malla": "primas-rentas"}
    assert mundo.almacen.escrito[0][1]["periodo_negocio"] == "202610"
    assert mundo.orquestador.llamadas == []


def test_ejecutar_inicia_step_y_registra_ejecucion(mundo):
    r = mundo.servicio.ejecutar(
        Evento.desde_dict({"publicar": True, "periodoNegocio": "202610", "origen": "SCHEDULER"}),
        "abcdef12-3456",
    )
    nombre = "rentas-202610-auto-20261103T100000Z-abcdef12"
    assert r.execution_name == nombre
    assert r.ejc_pk == "EJC#PRIMAS_RENTAS#202610"
    assert r.ejc_sk == f"EJECUCION#{nombre}"

    (nombre_step, entrada), = mundo.orquestador.llamadas
    assert nombre_step == nombre
    assert entrada == {
        "ejcPK": "EJC#PRIMAS_RENTAS#202610",
        "ejcSK": f"EJECUCION#{nombre}",
        "malla": "PRIMAS_RENTAS",
        "periodoNegocio": "202610",
        "modoEjecucion": "NORMAL",
        "rutaConfigExtraccion": "s3://assets/rentas/config_extraccion.json",
        "origen": "SCHEDULER",
    }

    (item,) = mundo.ejecuciones.items
    assert item["estado"] == "INICIADA"
    assert item["executionArn"] == f"arn:exec:{nombre}"
    assert item["fechaInicio"] == "2026-11-03T10:00:00Z"


def test_ejecutar_sin_publicar_es_error():
    with pytest.raises(ConfiguracionError, match="publicar=true"):
        Evento.desde_dict({"ejecutar": True})


def test_periodo_invalido_se_rechaza_en_el_evento():
    with pytest.raises(ConfiguracionError, match="YYYYMM"):
        Evento.desde_dict({"periodoNegocio": "2026-10"})
    with pytest.raises(ConfiguracionError, match="YYYYMM"):
        Evento.desde_dict({"periodoNegocio": None})


def test_malla_desconocida(mundo):
    with pytest.raises(ConfiguracionError, match="Malla desconocida"):
        mundo.servicio.ejecutar(Evento.desde_dict({"malla": "NO_EXISTE"}), "r")


def test_si_falla_la_validacion_no_se_publica_nada(mundo):
    mundo.cnf.cnf["VALIDACION"]["datasets"] = []
    mundo.cnf.cnf["VALIDACION"]["salida"] = {"x": "${PERIODO_NEGOCIO}"}
    with pytest.raises(ConfiguracionError, match="sin resolver"):
        mundo.servicio.ejecutar(Evento.desde_dict({"publicar": True}), "r")
    assert mundo.cnf.persistido is None and mundo.almacen.escrito == []
