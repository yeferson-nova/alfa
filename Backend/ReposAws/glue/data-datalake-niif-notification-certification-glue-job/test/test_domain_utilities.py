import pytest

from certification_notifier.domain.recipients import normalize_recipients
from certification_notifier.domain.availability import extract_count
from certification_notifier.domain.errors import (
    ConcurrencyError,
    ConfigurationError,
    InputError,
    NotificationError,
    PlatformError,
)
from certification_notifier.domain.models import QueryResult
from certification_notifier.domain.templating import clean_url, substitute_vars
from certification_notifier.domain.period import period_range
from certification_notifier.domain.paths import normalize_key, query_output_path
from certification_notifier.domain.tables import format_result, join_blocks
from certification_notifier.domain.texts import NO_DATA_TEXT


def test_clean_url_unescapes_until_stable():
    assert clean_url(" https://x.example/a?b=1&amp;amp;c=2 ") == "https://x.example/a?b=1&c=2"


def test_clean_url_with_empty_value_returns_empty_text():
    assert clean_url(None) == ""


def test_substitute_vars_replaces_known_and_keeps_unknown():
    assert substitute_vars("${a}-${b}", {"a": "uno"}) == "uno-${b}"


def test_substitute_vars_with_unescape_cleans_before_substituting():
    assert substitute_vars("x&amp;${a}", {"a": "1"}, unescape=True) == "x&1"


def test_substitute_vars_converts_non_text_values():
    assert substitute_vars(15, {}) == "15"


@pytest.mark.parametrize(
    ("period", "expected"),
    [
        ("202602", "01/02/2026 - 28/02/2026"),
        ("202402", "01/02/2024 - 29/02/2024"),
        ("202512", "01/12/2025 - 31/12/2025"),
    ],
)
def test_period_range_computes_first_and_last_day(period, expected):
    assert period_range(period) == expected


@pytest.mark.parametrize("period", ["2026", "20261", "2026101", "abcdef", "202613", "202600"])
def test_period_range_rejects_invalid_formats(period):
    with pytest.raises(InputError) as error:
        period_range(period)
    assert error.value.code == "NTF-ENT-002"


def test_normalize_key_strips_same_bucket_prefix_and_leading_slash():
    assert normalize_key("bucket", "s3://bucket/a/b.json") == "a/b.json"
    assert normalize_key("bucket", "/a/b.json") == "a/b.json"
    assert normalize_key("bucket", None) == ""


def test_normalize_key_rejects_other_bucket_paths():
    with pytest.raises(ConfigurationError) as error:
        normalize_key("bucket", "s3://otro/a.json")
    assert error.value.code == "NTF-CFG-006"


def test_query_output_path_ends_with_slash():
    assert query_output_path("b", "athena/salida") == "s3://b/athena/salida/"


def test_format_result_without_rows_returns_no_data_text():
    assert format_result(QueryResult(("a",), ())) == NO_DATA_TEXT


def test_format_result_joins_columns_and_fills_missing():
    result = QueryResult(("a", "b", "c"), (("1", "2", "3"), ("4",)))
    assert format_result(result) == "a: 1 | b: 2 | c: 3\na: 4 | b:  | c: "


def test_join_blocks_separates_with_blank_line_and_trims():
    assert join_blocks(["uno", "dos", ""]) == "uno\n\ndos"


def test_normalize_recipients_trims_lowercases_and_dedupes():
    assert normalize_recipients([" A@x.com ", "a@X.com", "", "b@x.com"]) == ("a@x.com", "b@x.com")


def test_normalize_recipients_without_values_fails():
    with pytest.raises(ConfigurationError) as error:
        normalize_recipients(["", "  "])
    assert error.value.code == "NTF-CFG-003"


def test_extract_count_reads_single_cell():
    assert extract_count(QueryResult(("registros",), (("42",),)), "m") == 42


@pytest.mark.parametrize(
    "result",
    [
        QueryResult(("otra",), (("1",),)),
        QueryResult(("registros",), ()),
        QueryResult(("registros",), (("1",), ("2",))),
        QueryResult(("registros",), (("1", "2"),)),
        QueryResult(("registros",), (("abc",),)),
        QueryResult(("registros",), (("-1",),)),
    ],
)
def test_extract_count_rejects_results_outside_contract(result):
    with pytest.raises(ConfigurationError) as error:
        extract_count(result, "member")
    assert error.value.code == "NTF-CFG-007"
    assert "member" in error.value.message


def test_error_exposes_full_classification():
    error = ConcurrencyError("NTF-CON-001", "detail")
    assert error.as_dict() == {
        "codigo": "NTF-CON-001",
        "dominio": "CONCURRENCIA",
        "severidad": "ALTA",
        "recuperable": False,
        "accion": error.action,
        "destinatario": "SOPORTE",
        "mensaje": "detail",
    }


def test_error_allows_recoverable_only_on_instance():
    recoverable = PlatformError("NTF-PLA-001", "x", recoverable=True)
    normal = PlatformError("NTF-PLA-004", "y")
    assert recoverable.recoverable is True
    assert normal.recoverable is False
    assert isinstance(recoverable, NotificationError)
