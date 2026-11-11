import re
from dataclasses import dataclass

from certification_job.domain.errors import DataError

UNLOAD_REJECTED = "CER-DAT-001"
OPENING = re.compile(r"\s*UNLOAD\s*\(", re.IGNORECASE)
TARGET = re.compile(r"\s*TO\s+'((?:''|[^'])*)'", re.IGNORECASE)
SQL_TOKEN = re.compile(
    r"'(?:''|[^'])*'"
    r'|"(?:""|[^"])*"'
    r"|--[^\n]*(?:\n|$)"
    r"|/\*.*?\*/"
    r"|[^'\"()/\-]+"
    r"|.",
    re.DOTALL,
)
PROBE_ALIAS = "niif_sin_datos"


@dataclass(frozen=True)
class UnloadStatement:
    inner_query: str
    target: str


def _closing_index(sql: str, start: int) -> int:
    depth = 1
    for match in SQL_TOKEN.finditer(sql, start):
        if match.group() == "(":
            depth += 1
        elif match.group() == ")":
            depth -= 1
            if depth == 0:
                return match.start()
    raise DataError(UNLOAD_REJECTED, "UNLOAD sin cierre reconocido")


def parse_unload(sql: str) -> UnloadStatement:
    opening = OPENING.match(sql)
    if not opening:
        raise DataError(UNLOAD_REJECTED, "Formato UNLOAD no reconocido; no se modifica el resultado")
    closing = _closing_index(sql, opening.end())
    target = TARGET.match(sql[closing + 1:])
    if not target:
        raise DataError(UNLOAD_REJECTED, "UNLOAD sin destino TO reconocido")
    return UnloadStatement(
        inner_query=sql[opening.end():closing].strip(),
        target=target.group(1).replace("''", "'"),
    )


def probe_query(inner_query: str) -> str:
    return f"SELECT * FROM (\n{inner_query}\n) AS {PROBE_ALIAS} LIMIT 1"
