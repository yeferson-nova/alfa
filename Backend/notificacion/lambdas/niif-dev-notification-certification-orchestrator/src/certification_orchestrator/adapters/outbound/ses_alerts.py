from typing import Any

from certification_orchestrator.adapters.outbound.aws_errors import translating
from certification_orchestrator.domain.models import Alert, AlertSettings

ENCODING = "UTF-8"


class SesAlertSender:
    def __init__(self, client: Any) -> None:
        self._client = client

    def send(self, settings: AlertSettings, alert: Alert) -> None:
        request: dict[str, Any] = {
            "FromEmailAddress": settings.sender,
            "Destination": {"ToAddresses": list(settings.recipients)},
            "Content": {
                "Simple": {
                    "Subject": {"Data": alert.subject, "Charset": ENCODING},
                    "Body": {"Text": {"Data": alert.body, "Charset": ENCODING}},
                }
            },
        }
        if settings.configuration_set:
            request["ConfigurationSetName"] = settings.configuration_set
        with translating("SES send_email"):
            self._client.send_email(**request)
