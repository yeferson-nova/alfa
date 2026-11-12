from builders import build_record, copy_case_config
from certification_notifier.domain.config import read_case_config
from certification_notifier.domain.wording import (
    availability_notice,
    notice_from_records,
    compose_text,
    build_subject,
    build_body,
    format_files,
    file_name_of,
    base_variables,
    final_variables,
)
from certification_notifier.domain.texts import (
    REGENERATION_NOTICE,
    DEFAULT_FILE_NAME,
    NO_FILES_TEXT,
    NO_SUMMARY_TEXT,
    NO_TOTALS_TEXT,
)


def _case():
    return read_case_config(copy_case_config(), "casoA")


def test_base_variables_include_template_contract():
    base = base_variables("dev", _case(), build_record(), "CERT1")
    assert base == {
        "env": "dev",
        "period": "202610",
        "periodRange": "01/10/2026 - 31/10/2026",
        "useCaseId": "casoA",
        "useCaseName": "Caso A",
        "certCode": "CERT1",
        "userEmail": "usuario@example.com",
        "executionId": "ej1",
    }


def test_base_variables_use_id_without_configured_name():
    data = copy_case_config()
    del data["email"]["stringVars"]["useCaseName"]
    case_config = read_case_config(data, "casoA")
    assert base_variables("dev", case_config, build_record(), "C")["useCaseName"] == "casoA"


def test_final_variables_resolve_string_vars_with_base():
    base = base_variables("dev", _case(), build_record(), "CERT1")
    final_vars = final_variables(base, _case().string_vars)
    assert final_vars["certificarLink"] == "https://api.example/certify?c=CERT1&amp;p=202610"
    assert final_vars["useCaseName"] == "Caso A"
    assert final_vars["env"] == "dev"


def test_build_subject_normal_and_regenerated():
    variables = {"useCaseName": "Caso A", "period": "202610"}
    template = "Cert ${useCaseName}\n${period}\r"
    assert build_subject(template, variables, False) == "Cert Caso A 202610 "
    assert build_subject(template, variables, True) == "[REGENERADO] Cert Caso A 202610 "


def test_file_name_uses_default_text_when_empty():
    assert file_name_of(build_record(file_name="")) == DEFAULT_FILE_NAME
    assert file_name_of(build_record(file_name="x.csv")) == "x.csv"


def test_format_files_lists_each_link_or_reports_none():
    assert format_files([]) == NO_FILES_TEXT
    assert format_files([("a.csv", "http://r/1"), ("b.csv", "http://r/2")]) == (
        "- a.csv\n  http://r/1\n- b.csv\n  http://r/2"
    )


def test_build_body_includes_all_sections():
    email_body = build_body("Caso A", "resumen", "totales", "archivos", "http://ok", "http://no")
    assert email_body == (
        "\nSolicitud de certificación NIIF 17\n\n"
        "Caso de uso:\nCaso A\n\n"
        "Resumen:\nresumen\n\n"
        "Totales:\ntotales\n\n"
        "Archivos generados:\narchivos\n\n"
        "Certificar:\nhttp://ok\n\n"
        "No certificar:\nhttp://no\n\n"
        "Este mensaje fue generado automáticamente por el flujo NIIF 17.\n"
    )


def test_build_body_uses_default_texts_for_empty_summary_and_totals():
    email_body = build_body("Caso A", "", "", "archivos", "a", "b")
    assert NO_SUMMARY_TEXT in email_body
    assert NO_TOTALS_TEXT in email_body


def test_records_notice_without_empty_records_is_empty():
    assert notice_from_records([build_record()], "202610") == ""


def test_records_notice_when_all_are_empty():
    records = [build_record(no_data_for_period=True), build_record(no_data_for_period=True)]
    assert notice_from_records(records, "202610") == (
        "No se encontraron datos procesados para el periodo seleccionado: 202610."
    )


def test_partial_records_notice_lists_empty_queries():
    records = [
        build_record(query_name="q1", no_data_for_period=True),
        build_record(query_name="q2"),
        build_record(query_name="q3", no_data_for_period=True),
    ]
    assert notice_from_records(records, "202610") == (
        "No se encontraron datos procesados para el periodo 202610 en las consultas: q1, q3. "
        "Las demas consultas conservan sus datos."
    )


def test_availability_notice_without_empty_members_is_empty():
    assert availability_notice("202610", [], 2) == ""


def test_availability_notice_partial_and_total():
    partial_send = availability_notice("202610", ["A"], 2)
    total = availability_notice("202610", ["A", "B"], 2)
    assert partial_send.splitlines() == [
        "Disponibilidad por caso de uso:",
        "A: no se encontraron datos activos para el periodo 202610.",
        "El archivo y los resumenes incluyen los datos disponibles de los demas casos.",
    ]
    assert total.splitlines()[-1] == (
        "Ninguno de los casos del compuesto tiene datos activos para el periodo solicitado."
    )


def test_compose_text_orders_regeneration_notice_and_body():
    assert compose_text("cuerpo", "", False) == "cuerpo"
    assert compose_text("cuerpo", "aviso", False) == "aviso\n\ncuerpo"
    assert compose_text("cuerpo", "aviso", True) == f"{REGENERATION_NOTICE}\n\naviso\n\ncuerpo"
    assert compose_text("cuerpo", "", True) == f"{REGENERATION_NOTICE}\n\ncuerpo"
