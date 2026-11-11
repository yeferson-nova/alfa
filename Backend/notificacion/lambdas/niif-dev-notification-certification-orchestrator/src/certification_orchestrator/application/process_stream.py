from typing import Any, Callable, Mapping, Sequence

from certification_orchestrator.application.global_configs import GlobalConfigs
from certification_orchestrator.application.launch_notification import LaunchNotification
from certification_orchestrator.application.move_files import ArchiveRejected, TransportCertified
from certification_orchestrator.application.ports import Dependencies, LogEvent
from certification_orchestrator.application.tracing import bind_record
from certification_orchestrator.domain.decisions import decide
from certification_orchestrator.domain.errors import classify
from certification_orchestrator.domain.models import Action, BatchResult, GlobalConfig, NotificationRecord
from certification_orchestrator.domain.records import config_pointer, is_removal, parse_change, sequence_number

Handler = Callable[[GlobalConfig, NotificationRecord], None]


class ProcessStream:
    def __init__(self, dependencies: Dependencies, token: str) -> None:
        self._deps = dependencies
        self._configs = GlobalConfigs(dependencies.configs)
        self._handlers: dict[Action, Handler] = {
            Action.LAUNCH_NOTIFICATION: LaunchNotification(dependencies, token).run,
            Action.TRANSPORT: TransportCertified(dependencies).run,
            Action.ARCHIVE_REJECTED: ArchiveRejected(dependencies).run,
        }

    def run(self, raw_records: Sequence[Mapping[str, Any]]) -> BatchResult:
        sequences = [sequence_number(raw) for raw in raw_records]
        self._deps.log.record(LogEvent.BATCH_START, registros=len(raw_records))
        processed = ignored = 0
        for raw, sequence in zip(raw_records, sequences):
            outcome = self._process(raw, sequence)
            if outcome is None:
                return BatchResult(failures=(sequence,), processed=processed, ignored=ignored)
            processed += outcome
            ignored += 1 - outcome
        bind_record(self._deps.log, None)
        return BatchResult(failures=(), processed=processed, ignored=ignored)

    def _process(self, raw: Mapping[str, Any], sequence: str) -> int | None:
        bind_record(self._deps.log, None)
        try:
            return self._apply(raw, sequence)
        except Exception as error:
            self._deps.log.record(
                LogEvent.RECORD_FAILED, level="ERROR", secuencia=sequence, **classify(error).as_log_fields()
            )
            return None

    def _apply(self, raw: Mapping[str, Any], sequence: str) -> int:
        if is_removal(raw):
            self._deps.log.record(LogEvent.RECORD_IGNORED, secuencia=sequence, evento_stream=raw.get("eventName"))
            return 0
        change = parse_change(raw)
        bind_record(self._deps.log, change.record)
        config = self._configs.get(config_pointer(change.record))
        action = decide(change, config.statuses)
        if action is Action.IGNORE:
            self._deps.log.record(
                LogEvent.RECORD_IGNORED,
                secuencia=sequence,
                estado_anterior=change.old_status,
                estado_nuevo=change.record.status,
            )
            return 0
        self._handlers[action](config, change.record)
        return 1
