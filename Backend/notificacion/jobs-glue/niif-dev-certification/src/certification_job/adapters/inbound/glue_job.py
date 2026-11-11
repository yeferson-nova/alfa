import sys
import time
from typing import Callable, Sequence

from niif_observability import StructuredLogger

from certification_job.application.ports import EVENT_MESSAGES, LogEvent
from certification_job.config.composition import COMPONENT, Context, build_context
from certification_job.config.settings import read_parameters
from certification_job.domain.errors import CertificationError, PlatformError
from certification_job.domain.models import CertificationResult, JobParameters

UNCLASSIFIED_ERROR = "CER-PLA-999"
TRACE_ARGUMENTS = {"--EJECUCION_ID": "ejecucion_id", "--JOB_RUN_ID": "request_id"}
MILLISECONDS_PER_SECOND = 1000


def _write(line: str) -> None:
    print(line, flush=True)


def classify(error: Exception) -> CertificationError:
    if isinstance(error, CertificationError):
        return error
    return PlatformError(UNCLASSIFIED_ERROR, f"{type(error).__name__}: {error}")


def _trace_from_argv(argv: Sequence[str]) -> dict[str, str]:
    values = list(argv)
    return {
        field: values[position + 1]
        for position, name in enumerate(values[:-1])
        for argument, field in TRACE_ARGUMENTS.items()
        if name == argument
    }


def _log_startup_error(write: Callable[[str], None], error: Exception, argv: Sequence[str]) -> None:
    logger = StructuredLogger(COMPONENT, write)
    logger.bind(**_trace_from_argv(argv))
    logger.log(
        LogEvent.JOB_ERROR.value,
        EVENT_MESSAGES[LogEvent.JOB_ERROR],
        "ERROR",
        fase="arranque",
        **classify(error).as_log_fields(),
    )


def _summarize(context: Context, succeeded: bool, started: float) -> None:
    files = context.use_case.files
    context.log.record(
        LogEvent.JOB_SUMMARY,
        job_exitoso=1 if succeeded else 0,
        job_fallido=0 if succeeded else 1,
        archivos_generados=len(files),
        archivos_sin_datos=sum(1 for exported in files if exported.no_data),
        duracion_ms=round((time.monotonic() - started) * MILLISECONDS_PER_SECOND),
    )


def run(
    argv: Sequence[str] | None = None,
    read: Callable[[Sequence[str]], JobParameters] = read_parameters,
    build: Callable[[JobParameters], Context] = build_context,
    write: Callable[[str], None] = _write,
) -> CertificationResult:
    started = time.monotonic()
    arguments = sys.argv[1:] if argv is None else argv
    try:
        context = build(read(arguments))
    except Exception as error:
        _log_startup_error(write, error, arguments)
        raise
    try:
        result = context.use_case.run()
    except Exception as error:
        context.log.record(LogEvent.JOB_ERROR, level="ERROR", **classify(error).as_log_fields())
        _summarize(context, succeeded=False, started=started)
        raise
    _summarize(context, succeeded=True, started=started)
    return result
