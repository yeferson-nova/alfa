from certification_notifier.domain.constants import (
    STATUS_READY_TO_NOTIFY,
    STATUS_NOTIFICATION_COMPLETED,
    STATUS_NOTIFICATION_IN_PROGRESS,
)
from certification_notifier.domain.errors import ConcurrencyError
from certification_notifier.domain.models import CompositeConfig, CompositeRecord

ID_SEPARATOR = "#"


def composite_identifier(config: CompositeConfig, period: str) -> str:
    return f"{config.group_id}{ID_SEPARATOR}{period}"


def control_already_completed(control: CompositeRecord | None, cert_code: str) -> bool:
    if control is not None and control.status == STATUS_NOTIFICATION_COMPLETED:
        if control.cert_code == cert_code:
            return True
        raise ConcurrencyError("NTF-CON-001", "Control ya completado para otra solicitud; no se reenvia")
    reserved = control is not None and control.status == STATUS_NOTIFICATION_IN_PROGRESS
    if not reserved or not control.updated_at:
        raise ConcurrencyError("NTF-CON-001", "Control no esta reservado para notificacion")
    return False


def validate_member(member: CompositeRecord | None, member_id: str, period: str) -> CompositeRecord:
    ready = member is not None and member.status == STATUS_READY_TO_NOTIFY
    if not ready or str(member.period) != str(period):
        raise ConcurrencyError("NTF-CON-001", f"Miembro no listo: {member_id}")
    if not (member.source_execution_id and member.batch_id and member.updated_at):
        raise ConcurrencyError("NTF-CON-001", f"Miembro sin identidad completa: {member_id}")
    return member
