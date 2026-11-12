from certification_notifier.application.ports import EventLogger, LogEvent, Clock, CompositeRepository
from certification_notifier.domain.composite import (
    control_already_completed,
    composite_identifier,
    validate_member,
)
from certification_notifier.domain.errors import PlatformError
from certification_notifier.domain.models import (
    CompositeConfig,
    CompositeRecord,
    CompositeReservation,
)


class CompositeService:
    def __init__(self, repository: CompositeRepository, clock: Clock, logger: EventLogger) -> None:
        self._repository = repository
        self._clock = clock
        self._logger = logger

    def prepare(
        self,
        config: CompositeConfig | None,
        period: str,
        cert_code: str,
        regeneration: bool,
    ) -> CompositeReservation | None:
        if regeneration or config is None:
            return None
        composite_id = composite_identifier(config, period)
        control = self._repository.get(config.table, composite_id, config.control_id)
        completed = control_already_completed(control, cert_code)
        members = () if completed else self._members(config, composite_id, period)
        return CompositeReservation(
            table=config.table,
            composite_id=composite_id,
            cert_code=cert_code,
            control=control,
            members=members,
            already_completed=completed,
        )

    def close(self, reservation: CompositeReservation | None, message_id: str) -> None:
        if reservation is None or reservation.already_completed:
            return
        if not message_id:
            raise PlatformError("NTF-PLA-003", "El envio no devolvio identificador; no se consumen los miembros")
        self._repository.consume(reservation, message_id, self._clock.utc_now().isoformat())
        self._logger.log(
            LogEvent.COMPOSITE_CLOSED, composite=reservation.composite_id, cert_code=reservation.cert_code
        )

    def _members(
        self,
        config: CompositeConfig,
        composite_id: str,
        period: str,
    ) -> tuple[CompositeRecord, ...]:
        return tuple(
            validate_member(
                self._repository.get(config.table, composite_id, member_id), member_id, period
            )
            for member_id in config.member_ids
        )
