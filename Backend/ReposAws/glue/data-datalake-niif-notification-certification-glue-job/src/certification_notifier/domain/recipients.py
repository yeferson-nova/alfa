from typing import Iterable

from certification_notifier.domain.errors import ConfigurationError


def normalize_recipients(values: Iterable[object]) -> tuple[str, ...]:
    cleaned = (str(value).strip().lower() for value in values)
    unique = dict.fromkeys(value for value in cleaned if value)
    if not unique:
        raise ConfigurationError("NTF-CFG-003", "No hay destinatarios configurados en email.destinations")
    return tuple(unique)
