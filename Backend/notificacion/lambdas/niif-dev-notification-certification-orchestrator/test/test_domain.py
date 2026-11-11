import pytest
from builders import RECIPIENT, global_json, image, stream_record

from certification_orchestrator.domain.alerts import exhausted_attempts_alert
from certification_orchestrator.domain.configuration import read_global_config
from certification_orchestrator.domain.decisions import decide
from certification_orchestrator.domain.errors import (
    ConfigurationError,
    DataError,
    InputError,
    PlatformError,
    classify,
)
from certification_orchestrator.domain.job_arguments import notification_arguments
from certification_orchestrator.domain.models import Action, LockAcquisition, LockRequest, RecordKey
from certification_orchestrator.domain.naming import (
    join_key,
    rejected_key,
    report_folder,
    source_candidates,
    transport_key,
    valid_file_name,
)
from certification_orchestrator.domain.records import (
    config_pointer,
    is_removal,
    parse_change,
    record_from_image,
    sequence_number,
)


def _config(**kwargs):
    return read_global_config(global_json(**kwargs))


def test_global_config_reads_every_key():
    config = _config()
    assert config.environment == "test"
    assert config.pending_index == "indice-pendientes"
    assert config.statuses.no_transport == "NO_TRANSPORT"
    assert config.orchestrator.transport_prefix == "export"
    assert config.orchestrator.use_historic_source_path is True
    assert config.alerts.recipients == (RECIPIENT,)
    assert config.alerts.configuration_set == "conjunto"
    assert config.report_folders["auxilioFunerario"] == "auxiliosFunerario"


@pytest.mark.parametrize(
    "path",
    [
        "dynamodb.pending_index",
        "orchestrator.notification_job_name",
        "orchestrator.alert_recipients",
        "certification.transport_status",
        "notification.ses_sender",
        "notification.ses_configuration_set",
        "orchestrator",
    ],
)
def test_global_config_without_a_key_fails_naming_it(path):
    with pytest.raises(ConfigurationError) as error:
        _config(removed=(path,))
    assert error.value.code == "ORQ-CFG-001"
    assert path.split(".")[-1] in error.value.message or path in error.value.message


@pytest.mark.parametrize(
    "changes",
    [
        {"orchestrator.reprocess_after_minutes": 0},
        {"orchestrator.reprocess_max_attempts": True},
        {"orchestrator.use_historic_source_path": "si"},
        {"orchestrator.alert_recipients": []},
        {"orchestrator.alert_recipients": ["ok@x.test", " "]},
        {"orchestrator.transport_prefix": "/"},
        {"certification.report_folders": {}},
        {"certification.transport_status": "CERTIFICATE"},
        {"timezone": ""},
    ],
)
def test_global_config_rejects_invalid_values(changes):
    with pytest.raises(ConfigurationError):
        _config(changes=changes)


def test_configuration_set_may_be_null():
    assert _config(changes={"notification.ses_configuration_set": None}).alerts.configuration_set is None


def test_record_from_image_reads_fields_and_lock():
    record = record_from_image(
        image(sendLock="tok", sendLockDate="2026-11-11T10:00:00-05:00", sendAttempts=2, sendAlertDate=None)
    )
    assert record.key == RecordKey("primasArl", "exec-1")
    assert record.cert_code == "CERT-1"
    assert record.regeneration_without_new_data is False
    assert record.lock.token == "tok"
    assert record.lock.attempts == 2
    assert record.lock.alert_date is None


def test_record_without_optional_fields_has_defaults():
    record = record_from_image(image(certCode=..., ejecucionId=..., regeneracionSinDatosNuevos=..., nameFile=" "))
    assert record.cert_code is None
    assert record.flow_execution_id is None
    assert record.name_file is None
    assert record.lock.attempts == 0


@pytest.mark.parametrize(
    "bad",
    [
        image(status=...),
        image(useCaseId=...),
        {**image(), "certCode": {"N": "1"}},
        {**image(), "certCode": "texto"},
        {**image(), "sendAttempts": {"N": "1.5"}},
    ],
)
def test_record_with_bad_attributes_is_an_input_error(bad):
    with pytest.raises(InputError):
        record_from_image(bad)


def test_record_image_must_be_an_object():
    with pytest.raises(InputError):
        record_from_image([])


def test_parse_change_reads_old_status_and_sequence():
    change = parse_change(stream_record(image(), old_status="PROCESSED", sequence="77"))
    assert change.sequence_number == "77"
    assert change.old_status == "PROCESSED"
    assert change.event_name == "MODIFY"


def test_parse_change_on_insert_has_no_old_status():
    assert parse_change(stream_record(image(), old_status=None, event="INSERT")).old_status is None


@pytest.mark.parametrize(
    "raw",
    [stream_record(image(), event="OTRO"), stream_record(None)],
)
def test_parse_change_rejects_unknown_events_and_missing_images(raw):
    with pytest.raises(InputError):
        parse_change(raw)


@pytest.mark.parametrize("raw", [{}, {"dynamodb": {}}, {"dynamodb": {"SequenceNumber": " "}}, "x"])
def test_sequence_number_is_required(raw):
    with pytest.raises(InputError) as error:
        sequence_number(raw)
    assert error.value.code == "ORQ-ENT-001"


def test_removal_is_detected():
    assert is_removal({"eventName": "REMOVE"}) is True
    assert is_removal({"eventName": "MODIFY"}) is False


def test_config_pointer_needs_bucket_and_global_key():
    assert config_pointer(record_from_image(image())).key == "configs/global.json"
    with pytest.raises(DataError):
        config_pointer(record_from_image(image(globalConfigKey=...)))


@pytest.mark.parametrize(
    "new_status, old_status, expected",
    [
        ("PENDING", "PROCESSED", Action.LAUNCH_NOTIFICATION),
        ("PENDING", None, Action.LAUNCH_NOTIFICATION),
        ("CERTIFICATE", "SENT", Action.TRANSPORT),
        ("NO_CERTIFICATE", "SENT", Action.ARCHIVE_REJECTED),
        ("PENDING", "PENDING", Action.IGNORE),
        ("SENT", "PENDING", Action.IGNORE),
        ("TRANSPORT", "CERTIFICATE", Action.IGNORE),
    ],
)
def test_decide_acts_only_on_status_changes(new_status, old_status, expected):
    change = parse_change(stream_record(image(status=new_status), old_status=old_status))
    assert decide(change, _config().statuses) is expected


def test_report_folder_comes_from_config():
    assert report_folder({"a": "/carpeta/"}, "a") == "carpeta"
    with pytest.raises(ConfigurationError) as error:
        report_folder({}, "otro")
    assert error.value.code == "ORQ-CFG-002"


@pytest.mark.parametrize("name", [None, "", " ", ".", "..", "a/b.csv", "a\\b.csv"])
def test_invalid_file_names_are_rejected(name):
    with pytest.raises(DataError):
        valid_file_name(name)


def test_paths_are_joined_without_double_slashes():
    assert join_key("/a/", "", "b") == "a/b"
    assert transport_key("export", "primasArl", "f.csv") == "export/primasArl/f.csv"
    assert rejected_key("primasArl", "NO-CERTIFICADOS", "f.csv") == "primasArl/NO-CERTIFICADOS/f.csv"


def test_source_candidates_include_historic_path_only_when_enabled():
    assert source_candidates("auxiliosFunerario", "auxilioFunerario", "f.csv", True) == (
        "auxiliosFunerario/f.csv",
        "auxilioFunerario/f.csv",
    )
    assert source_candidates("primasArl", "primasArl", "f.csv", True) == ("primasArl/f.csv",)
    assert source_candidates("x", "y", "f.csv", False) == ("x/f.csv",)


def test_notification_arguments_carry_the_flow_id():
    arguments = notification_arguments(record_from_image(image(regeneracionSinDatosNuevos=True)))
    assert arguments["--EJECUCION_ID"] == "flujo-1"
    assert arguments["--REGENERACION_SIN_DATOS_NUEVOS"] == "true"
    assert arguments["--certCode"] == "CERT-1"
    assert "--REDIRECT_BASE_URL" not in arguments
    assert "--SHORT_LINKS_TABLE" not in arguments


def test_notification_arguments_without_flow_id_omit_it():
    arguments = notification_arguments(record_from_image(image(ejecucionId=...)))
    assert "--EJECUCION_ID" not in arguments
    assert arguments["--REGENERACION_SIN_DATOS_NUEVOS"] == "false"


def test_notification_arguments_list_missing_fields():
    with pytest.raises(DataError) as error:
        notification_arguments(record_from_image(image(certCode=..., emailType=...)))
    assert "certCode" in error.value.message and "emailType" in error.value.message


def test_alert_has_no_personal_data_and_names_the_case():
    alert = exhausted_attempts_alert("dev", record_from_image(image()), 3)
    assert "primasArl" in alert.subject and "CERT-1" in alert.subject
    assert "Intentos: 3" in alert.body
    assert "@" not in alert.body


def test_alert_without_optional_values_says_not_available():
    alert = exhausted_attempts_alert("dev", record_from_image(image(certCode=..., ejecucionId=..., period=...)), 1)
    assert alert.body.count("no disponible") == 3


def test_errors_classify_and_describe_themselves():
    error = classify(ValueError("x"))
    assert isinstance(error, PlatformError)
    assert error.code == "ORQ-PLA-999"
    fields = DataError("ORQ-DAT-001", "detalle", recoverable=True).as_log_fields()
    assert fields["dominio"] == "DATOS" and fields["recuperable"] is True
    same = InputError("ORQ-ENT-001", "x")
    assert classify(same) is same


def test_lock_acquisition_exposes_its_keys():
    key = RecordKey("a", "b")
    acquisition = LockAcquisition((LockRequest(key, None),), "tok", "fecha", None)
    assert acquisition.keys == (key,)
