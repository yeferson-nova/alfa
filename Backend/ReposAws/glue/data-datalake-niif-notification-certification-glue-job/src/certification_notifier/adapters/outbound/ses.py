from typing import Any

from certification_notifier.adapters.outbound.aws_errors import translating
from certification_notifier.domain.constants import ENCODING
from certification_notifier.domain.models import EmailMessage


class SesNotifier:
    def __init__(self, client: Any, sender: str, configuration_set: str | None) -> None:
        self._client = client
        self._sender = sender
        self._configuration_set = configuration_set

    def send(self, message: EmailMessage, recipient: str) -> str:
        with translating("SES send_email"):
            response = self._client.send_email(**self._request(message, recipient))
        return str(response.get("MessageId", ""))

    def _request(self, message: EmailMessage, recipient: str) -> dict[str, Any]:
        request: dict[str, Any] = {
            "FromEmailAddress": self._sender,
            "Destination": {"ToAddresses": [recipient]},
            "Content": {"Simple": {"Subject": self._text(message.subject), "Body": self._body(message)}},
        }
        if message.tags:
            request["EmailTags"] = [{"Name": name, "Value": value} for name, value in message.tags.items()]
        if self._configuration_set:
            request["ConfigurationSetName"] = self._configuration_set
        return request

    @staticmethod
    def _text(content: str) -> dict[str, str]:
        return {"Data": content, "Charset": ENCODING}

    def _body(self, message: EmailMessage) -> dict[str, dict[str, str]]:
        email_body = {"Text": self._text(message.text)}
        if message.html:
            email_body["Html"] = self._text(message.html)
        return email_body
