from datetime import timedelta

from certification_notifier.application.ports import (
    EventLogger,
    LogEvent,
    TokenGenerator,
    Clock,
    LinkRepository,
)
from certification_notifier.domain.models import ShortLink, LinkPolicy


class LinkService:
    def __init__(
        self,
        repository: LinkRepository,
        clock: Clock,
        generator: TokenGenerator,
        logger: EventLogger,
        policy: LinkPolicy,
        use_case_id: str,
        cert_code: str,
    ) -> None:
        self._repository = repository
        self._clock = clock
        self._generator = generator
        self._logger = logger
        self._policy = policy
        self._use_case_id = use_case_id
        self._cert_code = cert_code

    def create(self, action: str, long_url: str) -> str:
        current_time = self._clock.local_now()
        expiration = current_time + timedelta(days=self._policy.validity_days)
        link = ShortLink(
            token=self._generator.generate(self._policy.token_length),
            url=long_url,
            use_case_id=self._use_case_id,
            cert_code=self._cert_code,
            action=action,
            created_at=current_time.isoformat(),
            expires_at=int(expiration.timestamp()),
        )
        self._repository.save(self._policy.table, link)
        self._logger.log(LogEvent.LINK_CREATED, action=action, token=link.token)
        return f"{self._policy.url_base}?t={link.token}"
