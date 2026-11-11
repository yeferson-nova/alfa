from typing import Mapping

from certification_orchestrator.domain.errors import ConfigurationError
from certification_orchestrator.domain.models import ConfigPointer

CONFIG_BUCKET_VARIABLE = "CONFIG_BUCKET"
GLOBAL_CONFIG_KEY_VARIABLE = "GLOBAL_CONFIG_KEY"


def _required(environ: Mapping[str, str], name: str) -> str:
    value = (environ.get(name) or "").strip()
    if not value:
        raise ConfigurationError("ORQ-CFG-003", f"Falta la variable de entorno {name} (la usa el reprocesador)")
    return value


def read_reprocess_pointer(environ: Mapping[str, str]) -> ConfigPointer:
    return ConfigPointer(
        bucket=_required(environ, CONFIG_BUCKET_VARIABLE),
        key=_required(environ, GLOBAL_CONFIG_KEY_VARIABLE),
    )
