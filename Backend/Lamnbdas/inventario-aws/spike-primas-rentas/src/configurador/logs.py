"""Log estructurado en JSON (una línea por evento), para CloudWatch."""
import json
import logging

_LOGGER = logging.getLogger("configurador")
_LOGGER.setLevel(logging.INFO)


def registrar(mensaje, nivel=logging.INFO, **campos):
    _LOGGER.log(
        nivel,
        json.dumps({"mensaje": mensaje, **campos}, ensure_ascii=False, default=str),
    )
