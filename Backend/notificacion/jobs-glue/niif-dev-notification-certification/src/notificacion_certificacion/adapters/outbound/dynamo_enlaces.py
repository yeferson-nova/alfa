from typing import Any

from notificacion_certificacion.adapters.outbound.dynamo_tipos import serializar
from notificacion_certificacion.adapters.outbound.errores_aws import traduciendo
from notificacion_certificacion.domain.modelos import EnlaceCorto

CONDICION_TOKEN_NUEVO = "attribute_not_exists(#tk)"


class RepositorioEnlacesDynamo:
    def __init__(self, cliente: Any) -> None:
        self._cliente = cliente

    def guardar(self, tabla: str, enlace: EnlaceCorto) -> None:
        item = serializar(
            {
                "token": enlace.token,
                "url": enlace.url,
                "useCaseId": enlace.caso_uso,
                "certCode": enlace.cert_code,
                "action": enlace.accion,
                "creationDate": enlace.fecha_creacion,
                "expiresAt": enlace.expira_en,
            }
        )
        with traduciendo(f"DynamoDB put_item {tabla}"):
            self._cliente.put_item(
                TableName=tabla,
                Item=item,
                ConditionExpression=CONDICION_TOKEN_NUEVO,
                ExpressionAttributeNames={"#tk": "token"},
            )
