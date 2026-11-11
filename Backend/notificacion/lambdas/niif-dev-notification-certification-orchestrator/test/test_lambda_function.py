import json
from types import SimpleNamespace

import pytest
from builders import CONFIG_BUCKET, GLOBAL_KEY, image, stream_record
from fakes import World

import lambda_function
from certification_orchestrator.adapters.inbound import lambda_handler
from certification_orchestrator.application.ports import LogEvent
from certification_orchestrator.application.process_stream import ProcessStream
from certification_orchestrator.application.reprocess_pending import ReprocessPending
from certification_orchestrator.config import composition
from certification_orchestrator.config.composition import Context, build_context
from certification_orchestrator.config.settings import read_reprocess_pointer
from certification_orchestrator.domain.errors import ConfigurationError, InputError
from certification_orchestrator.domain.models import ConfigPointer

CONTEXT = SimpleNamespace(aws_request_id="req-1")


def _builder(world, environ=None):
    pointer = ConfigPointer(CONFIG_BUCKET, GLOBAL_KEY)

    def build(request_id):
        world.log.bind(request_id=request_id)
        dependencies = world.dependencies()
        return Context(
            log=world.log,
            stream=ProcessStream(dependencies, request_id),
            reprocess=lambda: ReprocessPending(dependencies, request_id, pointer),
        )

    return build


def test_stream_event_returns_batch_item_failures():
    world = World()
    world.jobs.error = RuntimeError("x")
    response = lambda_handler.handle(
        {"Records": [stream_record(image(), sequence="9")]}, CONTEXT, build=_builder(world)
    )
    assert response == {"batchItemFailures": [{"itemIdentifier": "9"}]}
    summary = world.log.fields(LogEvent.BATCH_SUMMARY)
    assert summary["fallidos"] == 1 and summary["request_id"] == "req-1"


def test_stream_event_without_failures_returns_empty_list():
    world = World()
    response = lambda_handler.handle({"Records": [stream_record(image())]}, CONTEXT, build=_builder(world))
    assert response == {"batchItemFailures": []}
    assert world.jobs.starts


def test_scheduled_event_runs_the_reprocessor():
    world = World()
    response = lambda_handler.handle({}, CONTEXT, build=_builder(world))
    assert response == {"relanzados": 0, "omitidos": 0, "alertados": 0}
    assert LogEvent.REPROCESS_SUMMARY in world.log.events()


def test_invalid_records_field_is_logged_and_raised():
    world = World()
    with pytest.raises(InputError):
        lambda_handler.handle({"Records": "x"}, CONTEXT, build=_builder(world))
    assert world.log.fields(LogEvent.INVOCATION_ERROR)["codigo"] == "ORQ-ENT-001"


def test_startup_error_is_logged_with_request_id_and_raised():
    lines = []

    def broken(_):
        raise ConfigurationError("ORQ-CFG-003", "falta")

    with pytest.raises(ConfigurationError):
        lambda_handler.handle({}, CONTEXT, build=broken, write=lines.append)
    line = json.loads(lines[0])
    assert line["evento"] == "ORQ_ERROR_INVOCACION" and line["request_id"] == "req-1" and line["fase"] == "arranque"


def test_default_writer_prints(capsys):
    lambda_handler._write("linea")
    assert capsys.readouterr().out == "linea\n"


def test_entry_point_delegates_to_the_inbound_adapter(monkeypatch):
    monkeypatch.setattr(lambda_function, "handle", lambda event, context: {"ok": event})
    assert lambda_function.lambda_handler({"a": 1}, CONTEXT) == {"ok": {"a": 1}}


def test_reprocess_pointer_requires_both_variables():
    pointer = read_reprocess_pointer({"CONFIG_BUCKET": " b ", "GLOBAL_CONFIG_KEY": "k"})
    assert pointer == ConfigPointer("b", "k")
    with pytest.raises(ConfigurationError) as error:
        read_reprocess_pointer({"CONFIG_BUCKET": "b"})
    assert "GLOBAL_CONFIG_KEY" in error.value.message


def test_build_context_wires_real_adapters(monkeypatch):
    composition._clients.cache_clear()
    context = build_context("req-2", {"CONFIG_BUCKET": "b", "GLOBAL_CONFIG_KEY": "k"})
    assert isinstance(context.stream, ProcessStream)
    assert isinstance(context.reprocess(), ReprocessPending)
    assert build_context(None, {}).stream is not None
    with pytest.raises(ConfigurationError):
        build_context("req-3", {}).reprocess()
