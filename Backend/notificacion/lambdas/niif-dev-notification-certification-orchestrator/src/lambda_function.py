from typing import Any, Mapping

from certification_orchestrator.adapters.inbound.lambda_handler import handle


def lambda_handler(event: Mapping[str, Any], context: Any) -> dict[str, Any]:
    return handle(event, context)
