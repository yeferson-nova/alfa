from typing import Iterable

from certification_notifier.domain.models import QueryResult
from certification_notifier.domain.texts import NO_DATA_TEXT

COLUMN_SEPARATOR = " | "
BLOCK_SEPARATOR = "\n\n"


def _format_row(headers: tuple[str, ...], row: tuple[str, ...]) -> str:
    values = (row[i] if i < len(row) else "" for i in range(len(headers)))
    return COLUMN_SEPARATOR.join(f"{header}: {value}" for header, value in zip(headers, values))


def format_result(result: QueryResult) -> str:
    if not result.rows:
        return NO_DATA_TEXT
    return "\n".join(_format_row(result.headers, row) for row in result.rows)


def join_blocks(blocks: Iterable[str]) -> str:
    return BLOCK_SEPARATOR.join(blocks).strip()
