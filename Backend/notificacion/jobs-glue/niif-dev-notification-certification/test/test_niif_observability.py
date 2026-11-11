import json
from datetime import datetime, timezone

import pytest

from niif_observability import TRACE_FIELDS, Metric, StructuredLogger, emf_line

FIXED_TIME = datetime(2026, 11, 10, 15, 0, 0, tzinfo=timezone.utc)


def build_logger() -> tuple[StructuredLogger, list[str]]:
    lines: list[str] = []
    return StructuredLogger("componente", lines.append, lambda: FIXED_TIME), lines


def test_every_line_carries_all_trace_fields_even_before_binding():
    logger, lines = build_logger()
    logger.log("NTF_INICIO", "Inicio")
    line = json.loads(lines[0])
    assert all(field in line for field in TRACE_FIELDS)
    assert line["ejecucion_id"] is None
    assert line["marcaTiempo"] == FIXED_TIME.isoformat()
    assert (line["nivel"], line["evento"], line["mensaje"], line["funcion"]) == (
        "INFO",
        "NTF_INICIO",
        "Inicio",
        "componente",
    )


def test_bound_context_is_repeated_in_every_following_line():
    logger, lines = build_logger()
    logger.bind(ejecucion_id="arl-202606-auto-x-1", cert_code="C1", use_case_id="primasArl")
    logger.log("A", "uno")
    logger.bind(periodo_negocio="202606")
    logger.log("B", "dos", detalle=3)
    first, second = (json.loads(line) for line in lines)
    assert first["cert_code"] == second["cert_code"] == "C1"
    assert first["periodo_negocio"] is None
    assert second["periodo_negocio"] == "202606"
    assert second["detalle"] == 3
    assert logger.context["ejecucion_id"] == "arl-202606-auto-x-1"


def test_message_keeps_spanish_characters():
    logger, lines = build_logger()
    logger.log("A", "Configuración leída")
    assert "Configuración leída" in lines[0]


def test_reserved_fields_cannot_be_bound_or_overwritten():
    logger, lines = build_logger()
    with pytest.raises(ValueError):
        logger.bind(nivel="X")
    logger.log("A", "uno", evento="otro")
    assert json.loads(lines[0])["evento"] == "A"


def test_invalid_level_and_empty_component_are_rejected():
    logger, _ = build_logger()
    with pytest.raises(ValueError):
        logger.log("A", "uno", level="TRACE")
    with pytest.raises(ValueError):
        StructuredLogger(" ", print)


def test_default_clock_is_utc():
    lines: list[str] = []
    StructuredLogger("c", lines.append).log("A", "uno")
    assert json.loads(lines[0])["marcaTiempo"].endswith("+00:00")


def test_emf_line_follows_the_embedded_metric_format():
    document = json.loads(
        emf_line(
            "NIIF17/Notificacion",
            [Metric("CorreosEnviados", 2), Metric("DuracionMs", 1500, "Milliseconds")],
            {"Componente": "job", "CasoUso": "primasArl"},
            FIXED_TIME,
        )
    )
    directive = document["_aws"]["CloudWatchMetrics"][0]
    assert document["_aws"]["Timestamp"] == int(FIXED_TIME.timestamp() * 1000)
    assert directive["Namespace"] == "NIIF17/Notificacion"
    assert directive["Dimensions"] == [["CasoUso", "Componente"]]
    assert directive["Metrics"][1] == {"Name": "DuracionMs", "Unit": "Milliseconds"}
    assert (document["CorreosEnviados"], document["CasoUso"]) == (2, "primasArl")


@pytest.mark.parametrize(
    ("namespace", "metrics"),
    [(" ", [Metric("A", 1)]), ("N", []), ("N", [Metric("A", 1, "Parsecs")])],
)
def test_emf_line_rejects_invalid_input(namespace, metrics):
    with pytest.raises(ValueError):
        emf_line(namespace, metrics, {}, FIXED_TIME)
