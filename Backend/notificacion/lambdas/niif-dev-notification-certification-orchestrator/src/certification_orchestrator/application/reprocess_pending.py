from datetime import datetime, timedelta
from typing import Sequence

from certification_orchestrator.application.global_configs import GlobalConfigs
from certification_orchestrator.application.launch_notification import launch_with_lock
from certification_orchestrator.application.ports import Dependencies, LogEvent
from certification_orchestrator.application.tracing import bind_record
from certification_orchestrator.domain.alerts import exhausted_attempts_alert
from certification_orchestrator.domain.errors import PlatformError, classify
from certification_orchestrator.domain.models import (
    ConfigPointer,
    GlobalConfig,
    LockAcquisition,
    LockRequest,
    NotificationRecord,
    ReprocessResult,
)

RELAUNCHED, SKIPPED, ALERTED = "relanzado", "omitido", "alertado"
Group = tuple[NotificationRecord, ...]


def stuck_groups(records: Sequence[NotificationRecord], cutoff: str) -> list[Group]:
    groups: dict[tuple[str, str], list[NotificationRecord]] = {}
    for record in records:
        if record.lock.date and record.lock.date >= cutoff:
            continue
        groups.setdefault((record.key.use_case_id, record.cert_code or ""), []).append(record)
    return [tuple(sorted(items, key=lambda item: item.key.execution_id)) for _, items in sorted(groups.items())]


class ReprocessPending:
    def __init__(self, dependencies: Dependencies, token: str, pointer: ConfigPointer) -> None:
        self._deps = dependencies
        self._token = token
        self._pointer = pointer

    def run(self) -> ReprocessResult:
        config = GlobalConfigs(self._deps.configs).get(self._pointer)
        now = self._deps.clock.now_local(config.timezone)
        cutoff = (now - timedelta(minutes=config.orchestrator.reprocess_after_minutes)).isoformat()
        self._deps.log.record(LogEvent.REPROCESS_START, antes_de=cutoff)
        records = self._deps.notifications.find_pending_before(
            config.notification_table, config.pending_index, config.statuses.pending, cutoff
        )
        outcomes = [self._handle(config, group, now) for group in stuck_groups(records, cutoff)]
        bind_record(self._deps.log, None)
        result = ReprocessResult(
            relaunched=outcomes.count(RELAUNCHED),
            skipped=outcomes.count(SKIPPED),
            alerted=outcomes.count(ALERTED),
            failed=outcomes.count(None),
        )
        if result.failed:
            raise PlatformError("ORQ-PLA-005", f"Fallaron {result.failed} certCode en el reproceso", recoverable=True)
        return result

    def _handle(self, config: GlobalConfig, group: Group, now: datetime) -> str | None:
        bind_record(self._deps.log, group[0])
        try:
            return self._reprocess(config, group, now)
        except Exception as error:
            self._deps.log.record(LogEvent.GROUP_FAILED, level="ERROR", **classify(error).as_log_fields())
            return None

    def _reprocess(self, config: GlobalConfig, group: Group, now: datetime) -> str:
        attempts = max(record.lock.attempts for record in group)
        self._deps.log.record(LogEvent.STUCK_FOUND, level="WARN", intentos=attempts, registros=len(group))
        if attempts >= config.orchestrator.reprocess_max_attempts:
            return self._exhausted(config, group, attempts, now)
        acquisition = LockAcquisition(
            requests=tuple(LockRequest(key=record.key, current_token=record.lock.token) for record in group),
            token=self._token,
            date=now.isoformat(),
            required_status=config.statuses.pending,
        )
        return RELAUNCHED if launch_with_lock(self._deps, config, acquisition, group[0]) else SKIPPED

    def _exhausted(self, config: GlobalConfig, group: Group, attempts: int, now: datetime) -> str:
        already_alerted = any(record.lock.alert_date for record in group)
        self._deps.log.record(
            LogEvent.ATTEMPTS_EXHAUSTED, level="ERROR", intentos=attempts, ya_alertado=already_alerted
        )
        if already_alerted:
            return SKIPPED
        alert = exhausted_attempts_alert(config.environment, group[0], attempts)
        self._deps.alerts.send(config.alerts, alert)
        keys = tuple(record.key for record in group)
        self._deps.notifications.mark_alerted(config.notification_table, keys, now.isoformat())
        self._deps.log.record(LogEvent.ALERT_SENT, destinatarios=len(config.alerts.recipients))
        return ALERTED
