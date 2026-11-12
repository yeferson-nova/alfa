import copy

import pytest

from builders import GLOBAL_CONFIG_DATA, build_composite_config, copy_case_config
from certification_notifier.domain.config import (
    read_case_config,
    read_global_config,
)
from certification_notifier.domain.errors import ConfigurationError


def _global(**changes):
    data = copy.deepcopy(GLOBAL_CONFIG_DATA)
    data.update(changes)
    return data


def test_global_config_is_read_completely():
    config = read_global_config(GLOBAL_CONFIG_DATA)
    assert config.assets_bucket == "bucket-activos"
    assert config.notifications_table == "tabla-notificaciones"
    assert config.environment_name == "dev"
    assert config.athena_output_prefix == "athena/salida"
    assert config.sent_status == "SENT"
    assert config.catalog == "AwsDataCatalog"


def test_global_config_respects_configured_catalog_and_status():
    data = _global(catalog="otro_catalogo")
    data["certification"] = {"sent_status": "ENVIADO"}
    config = read_global_config(data)
    assert config.catalog == "otro_catalogo"
    assert config.sent_status == "ENVIADO"


def test_global_config_uses_default_status_without_section():
    data = _global()
    del data["certification"]
    assert read_global_config(data).sent_status == "SENT"


@pytest.mark.parametrize("key", ["buckets", "dynamodb", "environment", "athena"])
def test_global_config_requires_mandatory_keys(key):
    data = _global()
    del data[key]
    with pytest.raises(ConfigurationError) as error:
        read_global_config(data)
    assert error.value.code == "NTF-CFG-001"
    assert key in error.value.message


def test_global_config_rejects_non_text_values():
    with pytest.raises(ConfigurationError):
        read_global_config(_global(environment="  "))
    with pytest.raises(ConfigurationError):
        read_global_config(_global(environment=5))


def test_global_config_rejects_non_object_document():
    with pytest.raises(ConfigurationError):
        read_global_config([])


def test_case_config_normalizes_recipients_and_sorts_queries():
    case_config = read_case_config(copy_case_config(), "casoA")
    assert case_config.recipients == ("uno@example.com", "dos@example.com")
    assert [query.name for query in case_config.summary_queries] == ["a_resumen", "b_resumen"]
    assert [query.name for query in case_config.total_queries] == ["total"]
    assert case_config.presence_queries == ()
    assert case_config.composite_config is None
    assert case_config.subject_template.startswith("NIIF 17")


def test_case_config_rejects_a_different_use_case_id():
    with pytest.raises(ConfigurationError) as error:
        read_case_config(copy_case_config(), "otro")
    assert error.value.code == "NTF-CFG-002"


def test_case_config_rejects_non_object_documents():
    with pytest.raises(ConfigurationError) as error:
        read_case_config([], "casoA")
    assert error.value.code == "NTF-CFG-002"


def test_case_config_requires_email_and_subject():
    without_email = copy_case_config()
    del without_email["email"]
    without_subject = copy_case_config()
    del without_subject["email"]["subject"]
    for data in (without_email, without_subject):
        with pytest.raises(ConfigurationError) as error:
            read_case_config(data, "casoA")
        assert error.value.code == "NTF-CFG-001"


def test_case_config_without_recipients_fails_on_read():
    data = copy_case_config()
    data["email"]["destinations"] = []
    with pytest.raises(ConfigurationError) as error:
        read_case_config(data, "casoA")
    assert error.value.code == "NTF-CFG-003"


def test_case_config_without_recipients_key_fails():
    data = copy_case_config()
    del data["email"]["destinations"]
    with pytest.raises(ConfigurationError) as error:
        read_case_config(data, "casoA")
    assert error.value.code == "NTF-CFG-003"


def test_case_config_requires_recipients_list():
    data = copy_case_config()
    data["email"]["destinations"] = "a@x.com"
    with pytest.raises(ConfigurationError) as error:
        read_case_config(data, "casoA")
    assert error.value.code == "NTF-CFG-003"


def test_case_config_accepts_missing_string_vars():
    data = copy_case_config()
    del data["email"]["stringVars"]
    assert read_case_config(data, "casoA").string_vars == {}


def test_case_config_requires_string_vars_object():
    data = copy_case_config()
    data["email"]["stringVars"] = ["x"]
    with pytest.raises(ConfigurationError):
        read_case_config(data, "casoA")


def test_case_config_without_queries_leaves_them_empty():
    data = copy_case_config()
    del data["queries"]
    case_config = read_case_config(data, "casoA")
    assert case_config.summary_queries == ()
    assert case_config.total_queries == ()


def test_case_config_validates_query_shape():
    data = copy_case_config()
    data["queries"]["summary"] = ["no es objeto"]
    with pytest.raises(ConfigurationError):
        read_case_config(data, "casoA")
    data["queries"]["summary"] = {"no": "lista"}
    with pytest.raises(ConfigurationError):
        read_case_config(data, "casoA")
    data["queries"]["summary"] = [{"queryName": "x", "database": "d"}]
    with pytest.raises(ConfigurationError):
        read_case_config(data, "casoA")


def test_case_config_reads_data_presence_per_member():
    data = copy_case_config()
    data["email"]["dataPresence"] = [
        {"name": "A", "database": "d", "query": "q1"},
        {"name": "B", "database": "d", "query": "q2"},
    ]
    case_config = read_case_config(data, "casoA")
    assert [query.name for query in case_config.presence_queries] == ["A", "B"]


@pytest.mark.parametrize(
    "presence",
    [
        "no lista",
        [{"database": "d", "query": "q"}],
        [{"name": " ", "database": "d", "query": "q"}],
        ["text"],
        [{"name": "A", "database": "d", "query": "q"}, {"name": "A", "database": "d", "query": "q"}],
    ],
)
def test_case_config_rejects_invalid_presence(presence):
    data = copy_case_config()
    data["email"]["dataPresence"] = presence
    with pytest.raises(ConfigurationError) as error:
        read_case_config(data, "casoA")
    assert error.value.code == "NTF-CFG-005"


def test_case_config_reads_composite_with_default_control():
    data = copy_case_config()
    data["compositeNotification"] = build_composite_config()
    composite_config = read_case_config(data, "casoA").composite_config
    assert composite_config.table == "tabla-compuesto"
    assert composite_config.group_id == "grupo"
    assert composite_config.control_id == "_CONTROL"
    assert composite_config.member_ids == ("m1", "m2")


def test_case_config_respects_configured_control_member():
    data = copy_case_config()
    data["compositeNotification"] = {**build_composite_config(), "controlMemberId": "control"}
    assert read_case_config(data, "casoA").composite_config.control_id == "control"


def test_case_config_ignores_disabled_composite():
    data = copy_case_config()
    data["compositeNotification"] = {**build_composite_config(), "enabled": False}
    assert read_case_config(data, "casoA").composite_config is None


@pytest.mark.parametrize(
    "changes",
    [
        {"memberIds": []},
        {"memberIds": ["m1", "m1"]},
        {"memberIds": ["m1", "_CONTROL"]},
        {"memberIds": "m1"},
    ],
)
def test_case_config_rejects_invalid_composites(changes):
    data = copy_case_config()
    data["compositeNotification"] = {**build_composite_config(), **changes}
    with pytest.raises(ConfigurationError) as error:
        read_case_config(data, "casoA")
    assert error.value.code == "NTF-CFG-004"


def test_case_config_requires_composite_table_and_group():
    data = copy_case_config()
    composite = build_composite_config()
    del composite["tableName"]
    data["compositeNotification"] = composite
    with pytest.raises(ConfigurationError) as error:
        read_case_config(data, "casoA")
    assert error.value.code == "NTF-CFG-001"
