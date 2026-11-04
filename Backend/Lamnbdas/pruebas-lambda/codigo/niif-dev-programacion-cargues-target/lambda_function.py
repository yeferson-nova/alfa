import json
import logging
from datetime import datetime, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    resultado = {
        "estado": "EVENTO_RECIBIDO",
        "fechaRecepcion": datetime.now(timezone.utc).isoformat(),
        "requestId": context.aws_request_id,
        "evento": event
    }

    logger.info(
        "EVENTO PROGRAMADO RECIBIDO: %s",
        json.dumps(resultado, ensure_ascii=False, default=str)
    )

    return resultado
