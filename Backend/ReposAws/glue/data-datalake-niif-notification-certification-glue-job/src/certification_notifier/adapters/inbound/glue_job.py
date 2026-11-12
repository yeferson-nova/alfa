import sys
from typing import Callable, Sequence

from certification_notifier.application.notify_certification import JobResult
from certification_notifier.application.ports import LogEvent
from certification_notifier.config.settings import read_parameters
from certification_notifier.config.composition import Context, build_context
from certification_notifier.domain.errors import NotificationError, PlatformError
from certification_notifier.domain.models import RunParameters

UNCLASSIFIED_ERROR_CODE = "NTF-PLA-999"


def run(
    argv: Sequence[str] | None = None,
    read: Callable[[Sequence[str]], RunParameters] = read_parameters,
    build: Callable[[RunParameters], Context] = build_context,
) -> JobResult:
    arguments = sys.argv[1:] if argv is None else argv
    context = build(read(arguments))
    try:
        return context.use_case.run()
    except NotificationError as error:
        context.logger.log(LogEvent.JOB_ERROR, level="ERROR", **error.as_dict())
        raise
    except Exception as error:
        classified = PlatformError(UNCLASSIFIED_ERROR_CODE, str(error))
        context.logger.log(LogEvent.JOB_ERROR, level="ERROR", **classified.as_dict())
        raise
