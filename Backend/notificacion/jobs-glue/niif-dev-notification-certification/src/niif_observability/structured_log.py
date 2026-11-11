import json
from datetime import datetime, timezone
from typing import Any, Callable

TRACE_FIELDS = ("ejecucion_id", "request_id", "cert_code", "use_case_id", "periodo_negocio")
LEVELS = frozenset({"DEBUG", "INFO", "WARN", "ERROR"})
RESERVED_FIELDS = frozenset({"marcaTiempo", "nivel", "evento", "mensaje", "funcion"})


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class StructuredLogger:
    def __init__(
        self,
        component: str,
        write: Callable[[str], None],
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        if not component.strip():
            raise ValueError("component must not be empty")
        self._component = component
        self._write = write
        self._clock = clock
        self._context: dict[str, Any] = dict.fromkeys(TRACE_FIELDS)

    @property
    def context(self) -> dict[str, Any]:
        return dict(self._context)

    def bind(self, **fields: Any) -> None:
        clashes = RESERVED_FIELDS.intersection(fields)
        if clashes:
            raise ValueError(f"reserved log fields: {sorted(clashes)}")
        self._context.update(fields)

    def log(self, event: str, message: str, level: str = "INFO", **fields: Any) -> None:
        if level not in LEVELS:
            raise ValueError(f"unknown log level: {level}")
        line = {
            "marcaTiempo": self._clock().isoformat(),
            "nivel": level,
            "evento": event,
            "mensaje": message,
            "funcion": self._component,
            **self._context,
            **{key: value for key, value in fields.items() if key not in RESERVED_FIELDS},
        }
        self._write(json.dumps(line, ensure_ascii=False, default=str))
