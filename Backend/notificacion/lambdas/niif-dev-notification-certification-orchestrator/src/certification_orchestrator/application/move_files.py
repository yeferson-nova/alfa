from dataclasses import dataclass

from certification_orchestrator.application.ports import Dependencies, LogEvent
from certification_orchestrator.domain.errors import DataError
from certification_orchestrator.domain.models import GlobalConfig, NotificationRecord
from certification_orchestrator.domain.naming import (
    rejected_key,
    report_folder,
    source_candidates,
    transport_key,
    valid_file_name,
)


@dataclass(frozen=True)
class _FileContext:
    name_file: str
    folder: str
    candidates: tuple[str, ...]


def _file_context(config: GlobalConfig, record: NotificationRecord) -> _FileContext:
    name_file = valid_file_name(record.name_file)
    folder = report_folder(config.report_folders, record.key.use_case_id)
    candidates = source_candidates(
        folder, record.key.use_case_id, name_file, config.orchestrator.use_historic_source_path
    )
    return _FileContext(name_file=name_file, folder=folder, candidates=candidates)


def _missing_source(candidates: tuple[str, ...]) -> DataError:
    return DataError("ORQ-DAT-002", f"No se encontro el archivo de certificacion en: {', '.join(candidates)}")


class _FileMover:
    def __init__(self, dependencies: Dependencies) -> None:
        self._deps = dependencies

    def _find_source(self, bucket: str, candidates: tuple[str, ...]) -> str | None:
        return next((key for key in candidates if self._deps.objects.exists(bucket, key)), None)

    def _change_status(self, config: GlobalConfig, record: NotificationRecord, expected: str, new: str) -> bool:
        changed = self._deps.notifications.change_status(config.notification_table, record.key, expected, new)
        if not changed:
            self._deps.log.record(LogEvent.STATUS_ALREADY_CHANGED, estado_esperado=expected, estado_nuevo=new)
        return changed


class TransportCertified(_FileMover):
    def run(self, config: GlobalConfig, record: NotificationRecord) -> None:
        files = _file_context(config, record)
        source = self._find_source(config.reports_bucket, files.candidates)
        if source is None:
            raise _missing_source(files.candidates)
        settings = config.orchestrator
        target = transport_key(settings.transport_prefix, files.folder, files.name_file)
        self._deps.objects.copy(config.reports_bucket, source, settings.transport_bucket, target)
        statuses = config.statuses
        changed = self._change_status(config, record, statuses.certificate, statuses.transport)
        self._deps.log.record(
            LogEvent.FILE_TRANSPORTED, archivo=files.name_file, destino=target, estado_actualizado=changed
        )


class ArchiveRejected(_FileMover):
    def run(self, config: GlobalConfig, record: NotificationRecord) -> None:
        files = _file_context(config, record)
        bucket = config.reports_bucket
        target = rejected_key(files.folder, config.orchestrator.rejected_folder, files.name_file)
        source = self._find_source(bucket, files.candidates)
        already_archived = self._deps.objects.exists(bucket, target)
        if not already_archived:
            if source is None:
                raise _missing_source(files.candidates)
            self._deps.objects.copy(bucket, source, bucket, target)
        if source is not None:
            self._deps.objects.delete(bucket, source)
        statuses = config.statuses
        changed = self._change_status(config, record, statuses.no_certificate, statuses.no_transport)
        self._deps.log.record(
            LogEvent.FILE_ARCHIVED,
            archivo=files.name_file,
            destino=target,
            ya_archivado=already_archived,
            estado_actualizado=changed,
        )
        self._deps.log.record(LogEvent.SCHEDULE_PENDING_D2, level="WARN")
