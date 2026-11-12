import json
from typing import Any, Callable

from certification_notifier.application.ports import LogEvent, Clock


class JsonEventLogger:
    def __init__(
        self,
        clock: Clock,
        execution_id: str,
        component: str,
        write: Callable[[str], None],
    ) -> None:
        self._clock = clock
        self._execution_id = execution_id
        self._component = component
        self._write = write

    def log(self, event: LogEvent, level: str = "INFO", **context: Any) -> None:
        line = {
            "marcaTiempo": self._clock.utc_now().isoformat(),
            "nivel": level,
            "evento": event.value,
            "funcion": self._component,
            "ejecucion_id": self._execution_id,
            **context,
        }
        self._write(json.dumps(line, ensure_ascii=False, default=str))
