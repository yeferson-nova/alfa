from certification_orchestrator.application.ports import EventLog
from certification_orchestrator.domain.models import NotificationRecord


def bind_record(log: EventLog, record: NotificationRecord | None) -> None:
    log.bind(
        ejecucion_id=record.flow_execution_id if record else None,
        cert_code=record.cert_code if record else None,
        use_case_id=record.key.use_case_id if record else None,
        periodo_negocio=record.period if record else None,
    )
