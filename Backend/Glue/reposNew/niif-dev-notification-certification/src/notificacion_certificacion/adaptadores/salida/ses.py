from typing import Any

from notificacion_certificacion.adaptadores.salida.errores_aws import traduciendo
from notificacion_certificacion.dominio.constantes import CODIFICACION
from notificacion_certificacion.dominio.modelos import Mensaje


class NotificadorSes:
    def __init__(self, cliente: Any, remitente: str, conjunto_configuracion: str | None) -> None:
        self._cliente = cliente
        self._remitente = remitente
        self._conjunto = conjunto_configuracion

    def enviar(self, mensaje: Mensaje, destinatario: str) -> str:
        with traduciendo("SES send_email"):
            respuesta = self._cliente.send_email(**self._solicitud(mensaje, destinatario))
        return str(respuesta.get("MessageId", ""))

    def _solicitud(self, mensaje: Mensaje, destinatario: str) -> dict[str, Any]:
        solicitud: dict[str, Any] = {
            "FromEmailAddress": self._remitente,
            "Destination": {"ToAddresses": [destinatario]},
            "Content": {"Simple": {"Subject": self._texto(mensaje.asunto), "Body": self._cuerpo(mensaje)}},
        }
        if mensaje.etiquetas:
            solicitud["EmailTags"] = [{"Name": nombre, "Value": valor} for nombre, valor in mensaje.etiquetas.items()]
        if self._conjunto:
            solicitud["ConfigurationSetName"] = self._conjunto
        return solicitud

    @staticmethod
    def _texto(contenido: str) -> dict[str, str]:
        return {"Data": contenido, "Charset": CODIFICACION}

    def _cuerpo(self, mensaje: Mensaje) -> dict[str, dict[str, str]]:
        cuerpo = {"Text": self._texto(mensaje.texto)}
        if mensaje.html:
            cuerpo["Html"] = self._texto(mensaje.html)
        return cuerpo
