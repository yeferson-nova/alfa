import calendar
import re
from datetime import date

from certification_notifier.domain.errors import InputError

PERIOD_PATTERN = re.compile(r"[0-9]{6}")
DATE_FORMAT = "%d/%m/%Y"
MIN_MONTH = 1
MAX_MONTH = 12


def period_range(period: str) -> str:
    text = str(period)
    if not PERIOD_PATTERN.fullmatch(text):
        raise InputError("NTF-ENT-002", f"El periodo debe tener formato AAAAMM: {text}")
    year, month = int(text[:4]), int(text[4:])
    if not MIN_MONTH <= month <= MAX_MONTH:
        raise InputError("NTF-ENT-002", f"El mes del periodo es invalido: {text}")
    start = date(year, month, 1)
    end = date(year, month, calendar.monthrange(year, month)[1])
    return f"{start.strftime(DATE_FORMAT)} - {end.strftime(DATE_FORMAT)}"
