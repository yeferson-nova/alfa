import pytest

from builders import (
    build_composite_config,
    build_parameters,
    build_record,
    build_composite_record,
    copy_case_config,
)
from scenario import build_scenario
from fakes import FakeNotifier, FakeCompositeRepository
from certification_notifier.application.notify_certification import JobResult
from certification_notifier.application.ports import LogEvent
from certification_notifier.domain.errors import ConfigurationError, PlatformError
from certification_notifier.domain.models import QueryResult


def test_normal_flow_sends_one_email_per_recipient_and_marks_records():
    scenario = build_scenario()

    result = scenario.case_config.run()

    assert result == JobResult.SENT
    assert [recipient for _, recipient in scenario.notifier.sent] == [
        "uno@example.com",
        "dos@example.com",
    ]
    marked = scenario.notifications.marked_records[0]
    assert marked["table"] == "tabla-notificaciones"
    assert marked["status"] == "SENT"
    assert marked["channel"] == "SES_EMAIL"
    assert [sent_email.message_id for sent_email in marked["sent_emails"]] == ["id-1", "id-2"]
    assert marked["sent_at"] == scenario.clock.local.isoformat()


def test_normal_flow_finds_records_with_received_parameters():
    scenario = build_scenario()
    scenario.case_config.run()
    assert scenario.notifications.searches == [("tabla-notificaciones", "indice-cert", "CERT1", "casoA")]
    assert scenario.config.reads == [("bucket-config", "global.json"), ("bucket-config", "casos/casoA.json")]


def test_message_has_subject_body_tags_and_short_links():
    scenario = build_scenario()
    scenario.case_config.run()

    message, _ = scenario.notifier.sent[0]

    assert message.subject == "NIIF 17 - Certificacion Caso A Periodo 202610"
    assert dict(message.tags) == {"useCaseId": "casoA", "emailType": "certification"}
    assert message.html is None
    assert "Resumen:\nperiodo: 202610 | total: 5\n\nperiodo: 202610 | total: 7" in message.text
    assert "Totales:\nregistros: 12 | valor: 900" in message.text
    assert "- a.csv\n  https://redir.example/r?t=00000003" in message.text
    assert "Certificar:\nhttps://redir.example/r?t=00000001" in message.text
    assert "No certificar:\nhttps://redir.example/r?t=00000002" in message.text


def test_links_are_saved_with_clean_original_url():
    scenario = build_scenario()
    scenario.case_config.run()

    urls = {link.action: link.url for _, link in scenario.links.saved}

    assert urls == {
        "DOWNLOAD": "https://s3.example/a.csv?x=1&y=2",
        "CERTIFICATE": "https://api.example/certify?c=CERT1&p=202610",
        "NO_CERTIFICATE": "https://api.example/no?c=CERT1",
    }


def test_only_detail_records_create_download_links_in_order():
    records = [
        build_record(query_name="detail#b", file_name="b.csv", execution_id="e2"),
        build_record(query_name="summary#x", file_name="x.csv", execution_id="e3"),
        build_record(query_name="detail#a", file_name="a.csv", execution_id="e1"),
    ]
    scenario = build_scenario(records=records)
    scenario.case_config.run()

    downloads = [link.url for _, link in scenario.links.saved if link.action == "DOWNLOAD"]
    text = scenario.notifier.sent[0][0].text

    assert len(downloads) == 2
    assert text.index("a.csv") < text.index("b.csv")
    assert "x.csv" not in text


def test_body_reports_missing_detail_files():
    scenario = build_scenario(records=[build_record(query_name="summary#x")])
    scenario.case_config.run()
    assert "No se encontraron archivos detalle." in scenario.notifier.sent[0][0].text


def test_without_records_nothing_is_sent():
    scenario = build_scenario(records=[])

    result = scenario.case_config.run()

    assert result == JobResult.NO_RECORDS
    assert scenario.notifier.sent == []
    assert scenario.notifications.marked_records == []
    assert LogEvent.NO_RECORDS in scenario.logger.names()


def test_already_sent_records_are_not_resent():
    scenario = build_scenario(records=[build_record(status="SENT"), build_record(status="SENT")])

    result = scenario.case_config.run()

    assert result == JobResult.ALREADY_SENT
    assert scenario.notifier.sent == []
    assert scenario.links.saved == []


def test_partially_sent_batch_is_processed():
    scenario = build_scenario(records=[build_record(status="SENT"), build_record(status="PENDING")])
    assert scenario.case_config.run() == JobResult.SENT


def test_regeneration_adds_prefix_notice_and_skips_composite():
    data = copy_case_config()
    data["compositeNotification"] = build_composite_config()
    parameters = build_parameters(regeneration_without_new_data=True)
    scenario = build_scenario(parameters=parameters, raw_case_config=data)

    result = scenario.case_config.run()

    message, _ = scenario.notifier.sent[0]
    assert result == JobResult.SENT
    assert message.subject.startswith("[REGENERADO] ")
    assert message.text.startswith("CASO DE USO REEJECUTADO SIN REGISTROS NUEVOS")
    assert scenario.composite.consumptions == []


def test_records_without_data_notice_is_prepended_to_body():
    scenario = build_scenario(records=[build_record(no_data_for_period=True)])
    scenario.case_config.run()
    text = scenario.notifier.sent[0][0].text
    assert text.startswith("No se encontraron datos procesados para el periodo seleccionado: 202610.")


def test_member_availability_notice_replaces_records_notice():
    data = copy_case_config()
    data["email"]["dataPresence"] = [
        {"name": "A", "database": "db", "query": "SELECT presencia"},
    ]
    responses = {
        "SELECT 'a' FROM t WHERE p='202610'": QueryResult(("p",), (("1",),)),
        "SELECT 'b' FROM t WHERE p='202610'": QueryResult(("p",), (("1",),)),
        "SELECT 'total' FROM t WHERE p='202610'": QueryResult(("p",), (("1",),)),
        "SELECT presencia": QueryResult(("registros",), (("0",),)),
    }
    scenario = build_scenario(
        raw_case_config=data, responses=responses, records=[build_record(no_data_for_period=True)]
    )

    scenario.case_config.run()

    text = scenario.notifier.sent[0][0].text
    assert text.startswith("Disponibilidad por caso de uso:")
    assert "No se encontraron datos procesados" not in text


def test_failed_summary_query_is_reported_without_stopping_send():
    from certification_notifier.domain.errors import QueryFailedError

    responses = {
        "SELECT 'a' FROM t WHERE p='202610'": QueryFailedError("NTF-PLA-002", "fallo"),
        "SELECT 'b' FROM t WHERE p='202610'": QueryResult(("p",), (("1",),)),
        "SELECT 'total' FROM t WHERE p='202610'": QueryResult(("p",), ()),
    }
    scenario = build_scenario(responses=responses)

    assert scenario.case_config.run() == JobResult.SENT

    text = scenario.notifier.sent[0][0].text
    assert "Error ejecutando consulta" in text
    assert "Totales:\nNo hay datos" in text


def test_case_without_queries_uses_default_texts():
    data = copy_case_config()
    del data["queries"]
    scenario = build_scenario(raw_case_config=data, responses={})
    scenario.case_config.run()
    text = scenario.notifier.sent[0][0].text
    assert "No hay resumen configurado." in text
    assert "No hay totales configurados." in text


def test_case_with_other_use_case_id_fails_before_reading_records():
    data = copy_case_config()
    data["useCaseId"] = "otro"
    scenario = build_scenario(raw_case_config=data)
    with pytest.raises(ConfigurationError) as error:
        scenario.case_config.run()
    assert error.value.code == "NTF-CFG-002"
    assert scenario.notifications.searches == []


def _ready_composite():
    return FakeCompositeRepository(
        {
            ("tabla-compuesto", "grupo#202610", "_CONTROL"): build_composite_record(
                member_id="_CONTROL", status="NOTIFICATION_IN_PROGRESS"
            ),
            ("tabla-compuesto", "grupo#202610", "m1"): build_composite_record(member_id="m1"),
            ("tabla-compuesto", "grupo#202610", "m2"): build_composite_record(member_id="m2"),
        }
    )


def test_with_composite_consumes_members_after_send_and_mark():
    data = copy_case_config()
    data["compositeNotification"] = build_composite_config()
    scenario = build_scenario(raw_case_config=data, composite=_ready_composite())

    assert scenario.case_config.run() == JobResult.SENT

    reservation, message_id, _ = scenario.composite.consumptions[0]
    assert message_id == "id-1,id-2"
    assert reservation.cert_code == "CERT1"
    assert scenario.logger.names().index(LogEvent.RECORDS_MARKED) < scenario.logger.names().index(
        LogEvent.COMPOSITE_CLOSED
    )


def test_with_composite_already_completed_does_not_resend():
    data = copy_case_config()
    data["compositeNotification"] = build_composite_config()
    repository = FakeCompositeRepository(
        {
            ("tabla-compuesto", "grupo#202610", "_CONTROL"): build_composite_record(
                member_id="_CONTROL", status="NOTIFICATION_COMPLETED", cert_code="CERT1"
            ),
        }
    )
    scenario = build_scenario(raw_case_config=data, composite=repository)

    assert scenario.case_config.run() == JobResult.ALREADY_COMPLETED

    assert scenario.notifier.sent == []
    assert LogEvent.COMPOSITE_ALREADY_COMPLETED in scenario.logger.names()


def test_unconfirmed_email_is_neither_marked_nor_consumed():
    data = copy_case_config()
    data["compositeNotification"] = build_composite_config()
    scenario = build_scenario(
        raw_case_config=data, composite=_ready_composite(), notifier=FakeNotifier(ids=["", ""])
    )

    with pytest.raises(PlatformError) as error:
        scenario.case_config.run()

    assert error.value.code == "NTF-PLA-003"
    assert scenario.notifications.marked_records == []
    assert scenario.composite.consumptions == []


def test_failed_recipient_does_not_stop_others_and_logs_partial_send():
    class FailingNotifier(FakeNotifier):
        def send(self, message, recipient):
            if self.sent:
                self.sent.append((message, recipient))
                raise PlatformError("NTF-PLA-004", "rechazado")
            return super().send(message, recipient)

    data = copy_case_config()
    data["compositeNotification"] = build_composite_config()
    scenario = build_scenario(raw_case_config=data, composite=_ready_composite(), notifier=FailingNotifier())

    assert scenario.case_config.run().value == "ENVIADO"

    sent_emails = scenario.notifications.marked_records[0]["sent_emails"]
    assert [sent_email.message_id for sent_email in sent_emails] == ["id-1"]
    assert len(scenario.composite.consumptions) == 1
    events = {event: (level, context) for event, level, context in scenario.logger.events}
    assert events[LogEvent.SEND_FAILED][1]["codigo"] == "NTF-PLA-004"
    assert events[LogEvent.PARTIAL_SEND] == ("WARN", {"sent": 1, "failed": 1})


def test_all_recipients_failing_reraises_without_marking():
    class AlwaysFailingNotifier(FakeNotifier):
        def send(self, message, recipient):
            self.sent.append((message, recipient))
            raise PlatformError("NTF-PLA-004", "rechazado")

    scenario = build_scenario(notifier=AlwaysFailingNotifier())

    with pytest.raises(PlatformError) as error:
        scenario.case_config.run()

    assert error.value.code == "NTF-PLA-004"
    assert len(scenario.notifier.sent) == 2
    assert scenario.notifications.marked_records == []


def test_normal_flow_logs_start_and_end_without_personal_data():
    scenario = build_scenario()
    scenario.case_config.run()

    names = scenario.logger.names()
    contexts = [context for _, _, context in scenario.logger.events]

    assert names[0] == LogEvent.JOB_START
    assert names[-1] == LogEvent.JOB_END
    assert not any("example.com" in str(value) for context in contexts for value in context.values())
