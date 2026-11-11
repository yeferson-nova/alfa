from typing import Any, Mapping, Sequence

from notificacion_certificacion.adaptadores.salida.dynamo_tipos import deserializar, serializar
from notificacion_certificacion.adaptadores.salida.errores_aws import traduciendo
from notificacion_certificacion.dominio.errores import ErrorConfiguracion
from notificacion_certificacion.dominio.modelos import EnvioRealizado, RegistroNotificacion

SEPARADOR_IDENTIFICADORES = ","
ACTUALIZAR_ENVIO = (
    "SET #st = :s, sentDate = :d, notificationChannel = :ch, sesMessageId = :mid, emailDestination = :dest"
)


class RepositorioNotificacionesDynamo:
    def __init__(self, cliente: Any) -> None:
        self._cliente = cliente

    def buscar_por_cert_code(
        self, tabla: str, indice: str, cert_code: str, caso_uso: str
    ) -> tuple[RegistroNotificacion, ...]:
        items = self._consultar(tabla, indice, cert_code)
        return tuple(self._a_registro(item) for item in items if item.get("useCaseId") == caso_uso)

    def marcar_enviados(
        self,
        tabla: str,
        registros: Sequence[RegistroNotificacion],
        estado: str,
        canal: str,
        envios: Sequence[EnvioRealizado],
        fecha_envio: str,
    ) -> None:
        valores = serializar(
            {
                ":s": estado,
                ":d": fecha_envio,
                ":ch": canal,
                ":mid": SEPARADOR_IDENTIFICADORES.join(envio.id_mensaje for envio in envios),
                ":dest": [envio.destinatario for envio in envios],
            }
        )
        for registro in registros:
            with traduciendo(f"DynamoDB update_item {tabla}"):
                self._cliente.update_item(
                    TableName=tabla,
                    Key=serializar({"useCaseId": registro.caso_uso, "executionId": registro.id_ejecucion}),
                    UpdateExpression=ACTUALIZAR_ENVIO,
                    ExpressionAttributeNames={"#st": "status"},
                    ExpressionAttributeValues=valores,
                )

    def _consultar(self, tabla: str, indice: str, cert_code: str) -> list[dict[str, Any]]:
        argumentos: dict[str, Any] = {
            "TableName": tabla,
            "IndexName": indice,
            "KeyConditionExpression": "certCode = :cert",
            "ExpressionAttributeValues": serializar({":cert": cert_code}),
        }
        items: list[dict[str, Any]] = []
        while True:
            with traduciendo(f"DynamoDB query {tabla}/{indice}"):
                respuesta = self._cliente.query(**argumentos)
            items.extend(deserializar(item) for item in respuesta.get("Items", []))
            siguiente = respuesta.get("LastEvaluatedKey")
            if not siguiente:
                return items
            argumentos["ExclusiveStartKey"] = siguiente

    @staticmethod
    def _requerido(item: Mapping[str, Any], clave: str) -> str:
        if clave not in item:
            raise ErrorConfiguracion("NTF-CFG-001", f"Registro de notificacion sin el campo {clave}")
        return str(item[clave])

    def _a_registro(self, item: Mapping[str, Any]) -> RegistroNotificacion:
        return RegistroNotificacion(
            caso_uso=self._requerido(item, "useCaseId"),
            id_ejecucion=self._requerido(item, "executionId"),
            periodo=self._requerido(item, "period"),
            estado=str(item.get("status", "")),
            correo_usuario=str(item.get("userEmail", "")),
            nombre_consulta=str(item.get("queryName", "")),
            nombre_archivo=str(item.get("nameFile", "")),
            url_s3=str(item.get("s3_url", "")),
            sin_datos_periodo=item.get("sinDatosPeriodo") is True,
            ejecucion_flujo=str(item.get("ejecucionId", "")),
        )
