from certification_orchestrator.domain.models import Alert, NotificationRecord

NOT_AVAILABLE = "no disponible"


def exhausted_attempts_alert(environment: str, record: NotificationRecord, attempts: int) -> Alert:
    lines = (
        "La notificacion de certificacion no se envio despues de todos los intentos automaticos.",
        "",
        f"Ambiente: {environment}",
        f"Caso de uso: {record.key.use_case_id}",
        f"certCode: {record.cert_code or NOT_AVAILABLE}",
        f"Periodo: {record.period or NOT_AVAILABLE}",
        f"ejecucionId: {record.flow_execution_id or NOT_AVAILABLE}",
        f"Intentos: {attempts}",
        "",
        "Accion: revisar los logs del job de notificacion y relanzarlo a mano cuando se corrija la causa.",
    )
    subject = f"[NIIF {environment}] Notificacion sin enviar: {record.key.use_case_id} {record.cert_code or ''}"
    return Alert(subject=subject.strip(), body="\n".join(lines))
