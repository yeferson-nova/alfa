import re

from certification_notifier.domain.constants import RECORDS_COLUMN
from certification_notifier.domain.errors import ConfigurationError
from certification_notifier.domain.models import QueryResult

COUNT_PATTERN = re.compile(r"[0-9]+")


def extract_count(result: QueryResult, name: str) -> int:
    is_single_cell = len(result.rows) == 1 and len(result.rows[0]) == 1
    if result.headers != (RECORDS_COLUMN,) or not is_single_cell:
        raise ConfigurationError("NTF-CFG-007", f"No se pudo confirmar disponibilidad de {name}")
    value = result.rows[0][0].strip()
    if not COUNT_PATTERN.fullmatch(value):
        raise ConfigurationError("NTF-CFG-007", f"No se pudo confirmar disponibilidad de {name}")
    return int(value)
