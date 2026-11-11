import time
from typing import Any, Callable, Mapping

from niif_observability import StructuredLogger

from certification_orchestrator.application.ports import EVENT_MESSAGES, LogEvent
from certification_orchestrator.config.composition import COMPONENT, Context, build_context
from certification_orchestrator.domain.errors import InputError, classify

STREAM_RECORDS = "Records"
MILLISECONDS_PER_SECOND = 1000


def _write(line: str) -> None:
    print(line, flush=True)


def _elapsed_ms(started: float) -> int:
    return round((time.monotonic() - started) * MILLISECONDS_PER_SECOND)


def _log_startup_error(write: Callable[[str], None], error: Exception, request_id: str | None) -> None:
    logger = StructuredLogger(COMPONENT, write)
    logger.bind(request_id=request_id)
    logger.log(
        LogEvent.INVOCATION_ERROR.value,
        EVENT_MESSAGES[LogEvent.INVOCATION_ERROR],
        "ERROR",
        fase="arranque",
        **classify(error).as_log_fields(),
    )


def _handle_stream(app: Context, records: Any, started: float) -> dict[str, Any]:
    if not isinstance(records, list):
        raise InputError("ORQ-ENT-001", "Records del evento del stream no es una lista")
    result = app.stream.run(records)
    app.log.record(
        LogEvent.BATCH_SUMMARY,
        registros=len(records),
        procesados=result.processed,
        ignorados=result.ignored,
        fallidos=len(result.failures),
        duracion_ms=_elapsed_ms(started),
    )
    return {"batchItemFailures": [{"itemIdentifier": sequence} for sequence in result.failures]}


def _handle_reprocess(app: Context, started: float) -> dict[str, Any]:
    result = app.reprocess().run()
    app.log.record(
        LogEvent.REPROCESS_SUMMARY,
        relanzados=result.relaunched,
        omitidos=result.skipped,
        alertados=result.alerted,
        fallidos=result.failed,
        duracion_ms=_elapsed_ms(started),
    )
    return {"relanzados": result.relaunched, "omitidos": result.skipped, "alertados": result.alerted}


def handle(
    event: Mapping[str, Any],
    context: Any,
    build: Callable[[str | None], Context] = build_context,
    write: Callable[[str], None] = _write,
) -> dict[str, Any]:
    started = time.monotonic()
    request_id = getattr(context, "aws_request_id", None)
    try:
        app = build(request_id)
    except Exception as error:
        _log_startup_error(write, error, request_id)
        raise
    try:
        if isinstance(event, Mapping) and STREAM_RECORDS in event:
            return _handle_stream(app, event[STREAM_RECORDS], started)
        return _handle_reprocess(app, started)
    except Exception as error:
        app.log.record(LogEvent.INVOCATION_ERROR, level="ERROR", **classify(error).as_log_fields())
        raise
