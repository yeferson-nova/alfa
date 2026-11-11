import gzip
from datetime import datetime
from urllib.parse import parse_qs, unquote, urlparse

import pytest

from certification_job.domain import csv_report, naming
from certification_job.domain.cert_code import build_cert_code
from certification_job.domain.errors import ConfigurationError, DataError, InputError
from certification_job.domain.models import ConsoleSettings, HeaderSpec
from certification_job.domain.periods import period_range, validate_period
from certification_job.domain.templating import render
from certification_job.domain.unload import parse_unload, probe_query

NOW = datetime(2026, 11, 11, 9, 5, 7)
BOM = b"\xef\xbb\xbf"


@pytest.mark.parametrize("value, expected", [("202606", "202606"), (" 202612 ", "202612"), ("202401", "202401")])
def test_validate_period_accepts_yyyymm(value, expected):
    assert validate_period(value) == expected


@pytest.mark.parametrize("value", ["2026-06", "202613", "202600", "20266", "abcdef", ""])
def test_validate_period_rejects_other_formats(value):
    with pytest.raises(InputError) as error:
        validate_period(value)
    assert error.value.code == "CER-ENT-002"


@pytest.mark.parametrize(
    "period, expected",
    [
        ("202602", "01/02/2026 - 28/02/2026"),
        ("202402", "01/02/2024 - 29/02/2024"),
        ("202612", "01/12/2026 - 31/12/2026"),
    ],
)
def test_period_range_covers_the_whole_month(period, expected):
    assert period_range(period) == expected


def test_cert_code_keeps_the_deployed_formats():
    assert build_cert_code("202606", "cert", NOW, False, "abc") == "202606-20261111-0905"
    assert build_cert_code("202606", "CERT", NOW, True, "abc") == "202606-20261111-090507-abc"


def test_cert_code_is_only_generated_for_certification_emails():
    assert build_cert_code("202606", "report", NOW, False, "abc") is None


def test_render_unescapes_html_and_substitutes_strictly():
    assert render("a &gt; $x and ${y}", {"x": "1", "y": "2"}, "query") == "a > 1 and 2"
    assert render("cost $$5", {}, "query") == "cost $5"


def test_render_fails_on_missing_variables_or_invalid_placeholders():
    with pytest.raises(ConfigurationError) as missing:
        render("WHERE p = '$period'", {}, "query de detail")
    assert missing.value.code == "CER-CFG-004"
    assert "period" in missing.value.message
    with pytest.raises(ConfigurationError) as invalid:
        render("precio $ 5", {}, "query de detail")
    assert invalid.value.code == "CER-CFG-004"


def test_report_folder_comes_only_from_configuration():
    assert naming.report_folder({"primasArl": "primasArl"}, "primasArl") == "primasArl"
    with pytest.raises(ConfigurationError) as error:
        naming.report_folder({"primasArl": "primasArl"}, "otro")
    assert error.value.code == "CER-CFG-003"


def test_report_file_name_normalizes_the_query_name():
    assert naming.report_file_name("202606", "detail#1 a", NOW) == "202606-detail_1_a-20261111-090507.csv"


def test_s3_helpers():
    assert naming.unload_prefix("athena/unload", "e1") == "athena/unload/e1/"
    assert naming.s3_uri("b", "k/x") == "s3://b/k/x"
    assert naming.split_s3_uri("s3://b/k/x") == ("b", "k/x")
    for invalid in ("https://b/k", "s3://solo-bucket"):
        with pytest.raises(ConfigurationError):
            naming.split_s3_uri(invalid)


@pytest.mark.parametrize(
    "key, expected",
    [("p/e/part-0.gz", True), ("p/e/", False), ("p/e/abc-manifest.csv", False), ("p/e/METADATA", False)],
)
def test_unload_data_keys_skip_folders_manifest_and_metadata(key, expected):
    assert naming.is_unload_data_key(key) is expected


def test_console_url_matches_the_deployed_format():
    console = ConsoleSettings(
        base_url="https://sso.example/start/#/console", account_id="1", role_name="R", region="r-1"
    )
    url = naming.console_url(console, "reports", "folder/202606-detail_1-x.csv")
    assert url.startswith("https://sso.example/start/#/console?account_id=1&role_name=R&destination=")
    destination = unquote(url.split("destination=", 1)[1])
    parsed = urlparse(destination)
    assert parsed.netloc == "r-1.console.aws.amazon.com"
    assert parsed.path == "/s3/object/reports"
    assert parse_qs(parsed.query) == {"region": ["r-1"], "prefix": ["folder/202606-detail_1-x.csv"]}


@pytest.mark.parametrize(
    "file_name, expected",
    [("f/202606-detail_1-20261111.csv", "detail"), ("202606-total-x.csv", "total"), ("singuion.csv", None)],
)
def test_query_group_comes_from_the_file_name(file_name, expected):
    assert csv_report.query_group(file_name) == expected


SPECS = (
    HeaderSpec("detail", ("A", "B"), False),
    HeaderSpec("total", (), False),
    HeaderSpec("summary", ("S",), True),
)


@pytest.mark.parametrize(
    "group, expected",
    [("detail", ("A", "B")), ("total", None), ("summary", None), ("otro", None), (None, None)],
)
def test_headers_for_group_follow_the_deployed_rules(group, expected):
    assert csv_report.headers_for_group(SPECS, group) == expected


def test_reserva_query_names_with_suffix_do_not_match_the_group():
    specs = (HeaderSpec("detail#1", ("A",), False),)
    assert csv_report.headers_for_group(specs, csv_report.query_group("202606-detail_1-x.csv")) is None


def test_assemble_writes_bom_prefix_and_decompresses_chunks():
    body = csv_report.assemble(True, b"h\n", [gzip.compress(b"1|2\n"), b"3|4\n"])
    assert body == BOM + b"h\n1|2\n3|4\n"
    assert csv_report.assemble(False, b"", []) == b""


def test_add_headers_cases():
    assert csv_report.add_headers(BOM + b"1|2\n", ("A", "B"), "|") == BOM + b"A|B\n1|2\n"
    assert csv_report.add_headers(b"1|2\n", ("A", "B"), "|") == b"A|B\n1|2\n"
    assert csv_report.add_headers(BOM, ("A", "B"), "|") == BOM + b"A|B\n"
    assert csv_report.add_headers(BOM + b"A|B\n1|2\n", ("A", "B"), "|") == BOM + b"A|B\n1|2\n"
    assert csv_report.add_headers(b"1|2\n", None, "|") == b"1|2\n"


def test_count_data_rows():
    assert csv_report.count_data_rows(BOM + b"1|2\n\n3|4\n", ("A", "B"), "|") == 2
    assert csv_report.count_data_rows(BOM, ("A", "B"), "|") == 0
    assert csv_report.count_data_rows(BOM + b"1|2\n", None, "|") is None


def test_column_header_csv_uses_the_deployed_csv_writer_format():
    assert csv_report.column_header_csv(("a", "b")) == b"a,b\r\n"


UNLOAD = (
    "UNLOAD (SELECT ')' AS x, \"c(\" FROM t -- )\n /* ) */ WHERE (a = 1)) "
    "TO 's3://b/p/e1/' WITH (format='TEXTFILE')"
)


def test_parse_unload_ignores_parentheses_in_literals_identifiers_and_comments():
    statement = parse_unload(UNLOAD)
    assert statement.target == "s3://b/p/e1/"
    assert statement.inner_query.startswith("SELECT ')' AS x")
    assert statement.inner_query.endswith("WHERE (a = 1)")
    assert probe_query("SELECT 1") == "SELECT * FROM (\nSELECT 1\n) AS niif_sin_datos LIMIT 1"


def test_parse_unload_unescapes_quotes_in_the_target():
    assert parse_unload("UNLOAD (SELECT 1) TO 's3://b/o''k/'").target == "s3://b/o'k/"


@pytest.mark.parametrize(
    "sql, reason",
    [
        ("SELECT 1", "no reconocido"),
        ("UNLOAD (SELECT (1) TO 's3://b/'", "sin cierre"),
        ("UNLOAD (SELECT 1) WITH (x)", "sin destino"),
    ],
)
def test_parse_unload_rejects_unknown_shapes(sql, reason):
    with pytest.raises(DataError) as error:
        parse_unload(sql)
    assert error.value.code == "CER-DAT-001"
    assert reason in error.value.message
