import calendar
import re
from datetime import date

from certification_job.domain.errors import InputError

PERIOD_PATTERN = re.compile(r"^(\d{4})(\d{2})$")
FIRST_MONTH = 1
LAST_MONTH = 12
DATE_FORMAT = "%d/%m/%Y"


def validate_period(value: str) -> str:
    match = PERIOD_PATTERN.match(value.strip())
    if not match or not FIRST_MONTH <= int(match.group(2)) <= LAST_MONTH:
        raise InputError("CER-ENT-002", f"PERIOD debe tener el formato AAAAMM con un mes valido: {value!r}")
    return match.group(0)


def period_range(period: str) -> str:
    year, month = int(period[:4]), int(period[4:6])
    last_day = calendar.monthrange(year, month)[1]
    first = date(year, month, 1).strftime(DATE_FORMAT)
    last = date(year, month, last_day).strftime(DATE_FORMAT)
    return f"{first} - {last}"
