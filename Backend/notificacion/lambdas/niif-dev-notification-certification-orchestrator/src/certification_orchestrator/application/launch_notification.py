from certification_orchestrator.application.ports import Dependencies, LogEvent
from certification_orchestrator.domain.errors import DataError
from certification_orchestrator.domain.job_arguments import notification_arguments
from certification_orchestrator.domain.models import (
    GlobalConfig,
    LockAcquisition,
    LockRequest,
    NotificationRecord,
    RecordKey,
)

MAX_LOCKED_RECORDS = 100


def launch_with_lock(
    dependencies: Dependencies, config: GlobalConfig, acquisition: LockAcquisition, record: NotificationRecord
) -> bool:
    arguments = notification_arguments(record)
    if len(acquisition.requests) > MAX_LOCKED_RECORDS:
        raise DataError(
            "ORQ-DAT-005", f"El certCode tiene mas de {MAX_LOCKED_RECORDS} registros: no cabe en una transaccion"
        )
    table = config.notification_table
    if not dependencies.notifications.acquire_lock(table, acquisition):
        dependencies.log.record(LogEvent.LOCK_TAKEN)
        return False
    if not record.flow_execution_id:
        dependencies.log.record(LogEvent.FLOW_ID_MISSING, level="WARN")
    try:
        run_id = dependencies.jobs.start(config.orchestrator.notification_job_name, arguments)
    except Exception:
        dependencies.notifications.release_lock(table, acquisition.keys, acquisition.token)
        dependencies.log.record(LogEvent.LOCK_RELEASED, level="WARN")
        raise
    dependencies.log.record(
        LogEvent.NOTIFICATION_LAUNCHED,
        job_run_id=run_id,
        registros_bloqueados=len(acquisition.requests),
        job=config.orchestrator.notification_job_name,
    )
    return True


class LaunchNotification:
    def __init__(self, dependencies: Dependencies, token: str) -> None:
        self._deps = dependencies
        self._token = token

    def run(self, config: GlobalConfig, record: NotificationRecord) -> None:
        notification_arguments(record)
        keys = self._siblings(config, record)
        acquisition = LockAcquisition(
            requests=tuple(LockRequest(key=key, current_token=None) for key in keys),
            token=self._token,
            date=self._deps.clock.now_local(config.timezone).isoformat(),
            required_status=None,
        )
        launch_with_lock(self._deps, config, acquisition, record)

    def _siblings(self, config: GlobalConfig, record: NotificationRecord) -> tuple[RecordKey, ...]:
        found = self._deps.notifications.find_keys_by_cert_code(
            config.notification_table, config.cert_code_index, str(record.cert_code)
        )
        keys = [key for key in found if key.use_case_id == record.key.use_case_id]
        return tuple(sorted(set(keys) | {record.key}, key=lambda key: key.execution_id))
