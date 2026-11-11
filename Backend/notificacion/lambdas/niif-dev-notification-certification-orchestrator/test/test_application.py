import pytest
from builders import CONFIG_BUCKET, GLOBAL_KEY, JOB, REPORTS, TRANSPORT, global_json, image, stream_record
from fakes import NOW, World, failing_platform

from certification_orchestrator.application.ports import LogEvent
from certification_orchestrator.application.process_stream import ProcessStream
from certification_orchestrator.application.reprocess_pending import ReprocessPending, stuck_groups
from certification_orchestrator.domain.errors import PlatformError
from certification_orchestrator.domain.models import ConfigPointer, RecordKey
from certification_orchestrator.domain.records import record_from_image

TOKEN = "solicitud-1"


@pytest.fixture
def world():
    return World()


def _stream(world, *records):
    return ProcessStream(world.dependencies(), TOKEN).run(list(records))


def test_pending_launches_the_job_once_with_lock_and_flow_id(world):
    world.notifications.keys_by_cert["CERT-1"] = [RecordKey("primasArl", "exec-2"), RecordKey("otro", "exec-9")]
    result = _stream(world, stream_record(image()))
    assert result.failures == () and result.processed == 1
    job, arguments = world.jobs.starts[0]
    assert job == JOB
    assert arguments["--EJECUCION_ID"] == "flujo-1"
    lock = world.notifications.locks[0]
    assert lock.keys == (RecordKey("primasArl", "exec-1"), RecordKey("primasArl", "exec-2"))
    assert lock.token == TOKEN and lock.required_status is None
    assert all(request.current_token is None for request in lock.requests)
    assert lock.date == NOW.isoformat()
    fields = world.log.fields(LogEvent.NOTIFICATION_LAUNCHED)
    assert fields["job_run_id"] == "jr-1"
    assert fields["ejecucion_id"] == "flujo-1" and fields["cert_code"] == "CERT-1"


def test_pending_does_not_depend_on_the_query_name(world):
    _stream(world, stream_record(image(queryName="detail#7")))
    assert len(world.jobs.starts) == 1


def test_pending_with_lock_taken_is_skipped(world):
    world.notifications.lock_available = False
    result = _stream(world, stream_record(image()))
    assert result.failures == ()
    assert world.jobs.starts == []
    assert LogEvent.LOCK_TAKEN in world.log.events()


def test_pending_without_flow_id_warns_and_still_launches(world):
    _stream(world, stream_record(image(ejecucionId=...)))
    assert "--EJECUCION_ID" not in world.jobs.starts[0][1]
    assert LogEvent.FLOW_ID_MISSING in world.log.events()


def test_launch_failure_releases_the_lock_and_reports_the_record(world):
    world.jobs.error = failing_platform()
    result = _stream(world, stream_record(image(), sequence="5"), stream_record(image(), sequence="6"))
    assert result.failures == ("5",)
    assert world.notifications.released == (
        [((RecordKey("primasArl", "exec-1"),), TOKEN)]
    )
    assert LogEvent.LOCK_RELEASED in world.log.events()
    fields = world.log.fields(LogEvent.RECORD_FAILED)
    assert fields["codigo"] == "ORQ-PLA-001" and fields["secuencia"] == "5"


def test_pending_without_job_data_fails_before_locking(world):
    result = _stream(world, stream_record(image(emailType=...)))
    assert result.failures == ("1",)
    assert world.notifications.locks == []


def test_too_many_records_for_one_transaction_fail(world):
    world.notifications.keys_by_cert["CERT-1"] = [RecordKey("primasArl", f"e{index}") for index in range(101)]
    result = _stream(world, stream_record(image()))
    assert result.failures == ("1",)
    assert world.log.fields(LogEvent.RECORD_FAILED)["codigo"] == "ORQ-DAT-005"


def test_unchanged_status_and_removals_are_ignored(world):
    result = _stream(
        world,
        stream_record(image(), old_status="PENDING", sequence="1"),
        stream_record(None, event="REMOVE", sequence="2"),
        stream_record(image(status="SENT"), old_status="PENDING", sequence="3"),
    )
    assert result.ignored == 3 and result.processed == 0
    assert world.jobs.starts == [] and world.notifications.locks == []


def test_certificate_copies_only_its_own_file_and_marks_transport(world):
    world.objects.existing.add((REPORTS, "primasArl/archivo.csv"))
    result = _stream(world, stream_record(image(status="CERTIFICATE"), old_status="SENT"))
    assert result.processed == 1
    assert world.objects.copies == [(REPORTS, "primasArl/archivo.csv", TRANSPORT, "export/primasArl/archivo.csv")]
    assert world.notifications.status_changes == [(RecordKey("primasArl", "exec-1"), "CERTIFICATE", "TRANSPORT")]
    assert world.log.fields(LogEvent.FILE_TRANSPORTED)["estado_actualizado"] is True


def test_certificate_uses_historic_path_when_official_is_missing(world):
    world.objects.existing.add((REPORTS, "auxilioFunerario/archivo.csv"))
    _stream(world, stream_record(image(status="CERTIFICATE", useCaseId="auxilioFunerario"), old_status="SENT"))
    assert world.objects.copies[0][1] == "auxilioFunerario/archivo.csv"
    assert world.objects.copies[0][3] == "export/auxiliosFunerario/archivo.csv"


def test_certificate_already_moved_on_does_not_fail(world):
    world.objects.existing.add((REPORTS, "primasArl/archivo.csv"))
    world.notifications.status_change_result = False
    result = _stream(world, stream_record(image(status="CERTIFICATE"), old_status="SENT"))
    assert result.failures == ()
    assert LogEvent.STATUS_ALREADY_CHANGED in world.log.events()


def test_certificate_without_source_file_fails(world):
    result = _stream(world, stream_record(image(status="CERTIFICATE"), old_status="SENT"))
    assert result.failures == ("1",)
    assert world.log.fields(LogEvent.RECORD_FAILED)["codigo"] == "ORQ-DAT-002"


def test_certificate_for_unknown_case_is_a_configuration_error(world):
    result = _stream(world, stream_record(image(status="CERTIFICATE", useCaseId="nuevo"), old_status="SENT"))
    assert result.failures == ("1",)
    assert world.log.fields(LogEvent.RECORD_FAILED)["dominio"] == "CONFIGURACION"


def test_no_certificate_moves_the_file_and_marks_no_transport(world):
    world.objects.existing.add((REPORTS, "primasArl/archivo.csv"))
    _stream(world, stream_record(image(status="NO_CERTIFICATE"), old_status="SENT"))
    target = "primasArl/NO-CERTIFICADOS/archivo.csv"
    assert world.objects.copies == [(REPORTS, "primasArl/archivo.csv", REPORTS, target)]
    assert world.objects.deletes == [(REPORTS, "primasArl/archivo.csv")]
    assert world.notifications.status_changes[0][1:] == ("NO_CERTIFICATE", "NO_TRANSPORT")
    assert LogEvent.SCHEDULE_PENDING_D2 in world.log.events()


def test_no_certificate_is_idempotent_after_the_move(world):
    world.objects.existing.add((REPORTS, "primasArl/NO-CERTIFICADOS/archivo.csv"))
    result = _stream(world, stream_record(image(status="NO_CERTIFICATE"), old_status="SENT"))
    assert result.failures == ()
    assert world.objects.copies == [] and world.objects.deletes == []
    assert world.log.fields(LogEvent.FILE_ARCHIVED)["ya_archivado"] is True


def test_no_certificate_after_copy_but_before_delete_finishes_the_move(world):
    world.objects.existing.update(
        {(REPORTS, "primasArl/archivo.csv"), (REPORTS, "primasArl/NO-CERTIFICADOS/archivo.csv")}
    )
    _stream(world, stream_record(image(status="NO_CERTIFICATE"), old_status="SENT"))
    assert world.objects.copies == []
    assert world.objects.deletes == [(REPORTS, "primasArl/archivo.csv")]


def test_no_certificate_without_any_file_fails(world):
    result = _stream(world, stream_record(image(status="NO_CERTIFICATE"), old_status="SENT"))
    assert result.failures == ("1",)


def test_batch_stops_at_the_first_failure(world):
    result = _stream(
        world,
        stream_record(image(globalConfigKey=...), sequence="10"),
        stream_record(image(), sequence="11"),
    )
    assert result.failures == ("10",)
    assert world.jobs.starts == []


def test_global_config_is_read_once_per_batch(world):
    world.notifications.lock_available = False
    _stream(world, stream_record(image(), sequence="1"), stream_record(image(), sequence="2"))
    assert world.configs.reads == [(CONFIG_BUCKET, GLOBAL_KEY)]


def test_each_record_uses_the_global_config_it_points_to(world):
    other = global_json(changes={"orchestrator.notification_job_name": "job-previsional"})
    world.configs.documents[(CONFIG_BUCKET, "previsional/global.json")] = other
    _stream(world, stream_record(image(globalConfigKey="previsional/global.json")))
    assert world.jobs.starts[0][0] == "job-previsional"


def test_record_without_sequence_number_fails_the_whole_batch(world):
    with pytest.raises(Exception) as error:
        _stream(world, {"eventName": "MODIFY", "dynamodb": {}})
    assert error.value.code == "ORQ-ENT-001"


def _pending(**overrides):
    return record_from_image(image(**overrides))


def _reprocess(world):
    return ReprocessPending(world.dependencies(), TOKEN, ConfigPointer(CONFIG_BUCKET, GLOBAL_KEY)).run()


def test_reprocess_relaunches_stuck_records_taking_over_their_lock(world):
    world.notifications.pending = [
        _pending(sendLock="viejo", sendLockDate="2026-11-11T09:00:00-05:00", sendAttempts=1),
        _pending(executionId="exec-2", sendLock="viejo", sendAttempts=1),
    ]
    result = _reprocess(world)
    assert result.relaunched == 1 and result.failed == 0
    table, index, status, before = world.notifications.pending_queries[0]
    assert (index, status, before) == ("indice-pendientes", "PENDING", "2026-11-11T11:30:00-05:00")
    lock = world.notifications.locks[0]
    assert [request.current_token for request in lock.requests] == ["viejo", "viejo"]
    assert lock.required_status == "PENDING"
    assert world.jobs.starts[0][1]["--EJECUCION_ID"] == "flujo-1"


def test_reprocess_skips_records_whose_lock_is_recent(world):
    world.notifications.pending = [_pending(sendLock="t", sendLockDate="2026-11-11T11:50:00-05:00", sendAttempts=1)]
    result = _reprocess(world)
    assert result == result.__class__(relaunched=0, skipped=0, alerted=0, failed=0)
    assert world.jobs.starts == []


def test_reprocess_alerts_once_when_attempts_are_exhausted(world):
    world.notifications.pending = [_pending(sendLock="t", sendAttempts=3)]
    result = _reprocess(world)
    assert result.alerted == 1
    settings, alert = world.alerts.sent[0]
    assert settings.recipients and "CERT-1" in alert.subject
    assert world.notifications.alerted == [((RecordKey("primasArl", "exec-1"),), NOW.isoformat())]
    assert world.jobs.starts == []


def test_reprocess_does_not_repeat_an_alert(world):
    world.notifications.pending = [_pending(sendAttempts=3, sendAlertDate="2026-11-10T10:00:00-05:00")]
    result = _reprocess(world)
    assert result.skipped == 1 and world.alerts.sent == []


def test_reprocess_reports_lock_lost_as_skipped(world):
    world.notifications.pending = [_pending(sendAttempts=1)]
    world.notifications.lock_available = False
    assert _reprocess(world).skipped == 1


def test_reprocess_continues_after_a_group_fails_and_then_raises(world):
    world.notifications.pending = [_pending(certCode="A", emailType=...), _pending(certCode="B", executionId="e2")]
    with pytest.raises(PlatformError) as error:
        _reprocess(world)
    assert error.value.code == "ORQ-PLA-005"
    assert len(world.jobs.starts) == 1
    assert LogEvent.GROUP_FAILED in world.log.events()


def test_stuck_groups_group_by_case_and_cert_code():
    records = [
        _pending(executionId="b"),
        _pending(executionId="a"),
        _pending(certCode="OTRO", executionId="c"),
        _pending(certCode=..., executionId="d"),
    ]
    groups = stuck_groups(records, "2026-11-11T11:30:00-05:00")
    assert [[record.key.execution_id for record in group] for group in groups] == [["d"], ["a", "b"], ["c"]]
