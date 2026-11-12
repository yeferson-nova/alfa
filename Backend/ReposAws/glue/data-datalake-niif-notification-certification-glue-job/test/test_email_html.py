import pytest

from builders import DEFAULT_RESPONSES, build_record, copy_case_config
from scenario import build_scenario
from certification_notifier.application.ports import LogEvent
from certification_notifier.domain.config import read_case_config
from certification_notifier.domain.email_html import (
    notices_html,
    notice_box,
    detail_html,
    html_fragments,
    template_supports_notices,
    fill_template,
    table_html,
)
from certification_notifier.domain.errors import ConfigurationError, QueryFailedError, AccessDeniedError
from certification_notifier.domain.models import Notice, QueryResult
from certification_notifier.domain.wording import email_notices
from certification_notifier.domain.texts import REGENERATION_NOTICE

TEMPLATE_PATH = "email-template/caso_a.html"
TEMPLATE = (
    "<html>$avisoHtml|$useCaseName|$period|$tableSummary|$tableTotal|"
    "$tableDetail|$certificarLink|$nocertificarLink|$desconocida</html>"
)


def _config_with_template():
    data = copy_case_config()
    data["email"]["templatePath"] = TEMPLATE_PATH
    return data


def test_table_html_escapes_headers_values_and_fills_cells():
    html = table_html(QueryResult(("a<b", "c"), (("<x>",),)))

    assert "<th" in html and "a&lt;b" in html
    assert "&lt;x&gt;" in html
    assert html.count("<td") == 2


def test_table_html_without_rows_shows_no_data():
    assert ">No hay datos</p>" in table_html(QueryResult(("a",), ()))


def test_detail_html_escapes_name_and_link():
    html = detail_html([("a&b.csv", "https://x/r?t=1&u=2")])

    assert 'href="https://x/r?t=1&amp;u=2"' in html
    assert ">a&amp;b.csv</a>" in html
    assert 'rel="noopener noreferrer"' in html


def test_detail_html_without_files_shows_default_text():
    assert "No se encontraron archivos detalle." in detail_html([])


def test_notice_box_joins_non_empty_lines_and_uses_tone():
    html = notice_box(Notice(tone="info", text="uno\n\n<dos>"))

    assert "uno<br>&lt;dos&gt;" in html
    assert "#174ea6" in html


def test_empty_notices_html_generates_no_row():
    assert notices_html([]) == ""
    assert notices_html([Notice("warn", "x")]).startswith('<tr><td class="pad"')


def test_template_supports_notices_only_with_marker_or_no_notices():
    assert template_supports_notices("sin marcador", [])
    assert not template_supports_notices("sin marcador", [Notice("warn", "x")])
    assert template_supports_notices("con $avisoHtml", [Notice("warn", "x")])


def test_fill_template_escapes_variables_and_keeps_unknown_markers():
    fragments = html_fragments("<r/>", "<t/>", [], "https://c?a=1&b=2", "https://n")

    html = fill_template(TEMPLATE, {"useCaseName": "A<B", "period": "202610"}, fragments, [])

    assert "|A&lt;B|202610|<r/>|<t/>|" in html
    assert "|https://c?a=1&amp;b=2|https://n|$desconocida" in html
    assert html.startswith("<html>|")


def test_email_notices_put_regeneration_first():
    assert email_notices("", False) == []
    assert email_notices("falta", True) == [Notice("info", REGENERATION_NOTICE), Notice("warn", "falta")]


def test_config_reads_optional_template_path():
    assert read_case_config(copy_case_config(), "casoA").template_path is None
    assert read_case_config(_config_with_template(), "casoA").template_path == TEMPLATE_PATH


def test_message_has_html_from_assets_bucket_template():
    scenario = build_scenario(raw_case_config=_config_with_template(), templates={TEMPLATE_PATH: TEMPLATE})

    scenario.case_config.run()

    message, _ = scenario.notifier.sent[0]
    assert ("bucket-activos", TEMPLATE_PATH) in scenario.config.reads
    assert "|Caso A|202610|" in message.html
    assert "periodo</th>" in message.html and ">5</td>" in message.html and ">900</td>" in message.html
    assert ">a.csv</a>" in message.html
    assert "|https://redir.example/r?t=00000001|https://redir.example/r?t=00000002|" in message.html
    assert message.html.startswith("<html>|")


def test_notices_appear_in_html_when_template_has_marker():
    records = [build_record(no_data_for_period=True)]
    scenario = build_scenario(raw_case_config=_config_with_template(), templates={TEMPLATE_PATH: TEMPLATE}, records=records)

    scenario.case_config.run()

    message, _ = scenario.notifier.sent[0]
    assert "No se encontraron datos procesados para el periodo seleccionado: 202610." in message.html
    assert "#8a4b00" in message.html


def test_notices_without_template_marker_send_text_only():
    records = [build_record(no_data_for_period=True)]
    scenario = build_scenario(
        raw_case_config=_config_with_template(), templates={TEMPLATE_PATH: "<html>$tableTotal</html>"}, records=records
    )

    scenario.case_config.run()

    message, _ = scenario.notifier.sent[0]
    assert message.html is None
    assert LogEvent.TEMPLATE_WITHOUT_MARKER in scenario.logger.names()


def test_unreadable_template_sends_text_only_and_logs():
    for error in (ConfigurationError("NTF-CFG-008", "no existe"), AccessDeniedError("NTF-PER-001", "denegado")):
        scenario = build_scenario(raw_case_config=_config_with_template(), templates={TEMPLATE_PATH: error})

        scenario.case_config.run()

        message, _ = scenario.notifier.sent[0]
        assert message.html is None
        event, level, context = next(e for e in scenario.logger.events if e[0] == LogEvent.TEMPLATE_NOT_READ)
        assert (level, context["codigo"], context["path"]) == ("WARN", error.code, TEMPLATE_PATH)


def test_failed_query_appears_as_error_in_html():
    responses = dict(DEFAULT_RESPONSES)
    responses["SELECT 'total' FROM t WHERE p='202610'"] = QueryFailedError("NTF-PLA-002", "fallo")
    scenario = build_scenario(
        raw_case_config=_config_with_template(), templates={TEMPLATE_PATH: TEMPLATE}, responses=responses
    )

    scenario.case_config.run()

    message, _ = scenario.notifier.sent[0]
    assert '<div style="margin:0 0 12px 0;"><p' in message.html
    assert "Error ejecutando consulta</p></div>|" in message.html


def test_template_from_other_bucket_stops_job_like_previous_script():
    data = _config_with_template()
    data["email"]["templatePath"] = "s3://otro-bucket/email-template/a.html"
    scenario = build_scenario(raw_case_config=data)

    with pytest.raises(ConfigurationError) as error:
        scenario.case_config.run()

    assert error.value.code == "NTF-CFG-006"
    assert scenario.notifier.sent == []
