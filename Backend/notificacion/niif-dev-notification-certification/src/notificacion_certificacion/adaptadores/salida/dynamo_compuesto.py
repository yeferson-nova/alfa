from typing import Any

from notificacion_certificacion.adaptadores.salida.dynamo_tipos import deserializar, serializar
from notificacion_certificacion.adaptadores.salida.errores_aws import traduciendo
from notificacion_certificacion.dominio.constantes import (
    ESTADO_CONSUMIDO,
    ESTADO_LISTO_PARA_NOTIFICAR,
    ESTADO_NOTIFICACION_COMPLETADA,
    ESTADO_NOTIFICACION_EN_CURSO,
)
from notificacion_certificacion.dominio.modelos import RegistroCompuesto, ReservaCompuesta

CONSUMIR_MIEMBRO = (
    "SET #st = :consumed, notificationReleased = :yes, completedAt = :now, updatedAt = :now, "
    "certCode = :cert, sesMessageId = :mid"
)
CONDICION_MIEMBRO = "#st = :ready AND sourceExecutionId = :exec AND loteCargueId = :lot AND updatedAt = :previous"
COMPLETAR_CONTROL = (
    "SET #st = :done, completedAt = :now, updatedAt = :now, certCode = :cert, sesMessageId = :mid"
)
CONDICION_CONTROL = "#st = :progress AND updatedAt = :previous"
NOMBRE_ESTADO = {"#st": "status"}


class RepositorioCompuestoDynamo:
    def __init__(self, cliente: Any) -> None:
        self._cliente = cliente

    def obtener(self, tabla: str, id_compuesto: str, id_miembro: str) -> RegistroCompuesto | None:
        with traduciendo(f"DynamoDB get_item {tabla}"):
            respuesta = self._cliente.get_item(
                TableName=tabla,
                Key=serializar({"compositeId": id_compuesto, "memberId": id_miembro}),
                ConsistentRead=True,
            )
        item = respuesta.get("Item")
        return None if item is None else self._a_registro(id_miembro, deserializar(item))

    def consumir(self, reserva: ReservaCompuesta, id_mensaje: str, fecha: str) -> None:
        transacciones = [self._consumir_miembro(reserva, miembro, id_mensaje, fecha) for miembro in reserva.miembros]
        transacciones.append(self._completar_control(reserva, id_mensaje, fecha))
        with traduciendo(f"DynamoDB transact_write_items {reserva.tabla}"):
            self._cliente.transact_write_items(TransactItems=transacciones)

    @staticmethod
    def _a_registro(id_miembro: str, item: dict[str, Any]) -> RegistroCompuesto:
        return RegistroCompuesto(
            id_miembro=id_miembro,
            estado=str(item.get("status", "")),
            periodo=item.get("period"),
            cert_code=str(item.get("certCode", "")),
            id_ejecucion_origen=item.get("sourceExecutionId"),
            id_lote=item.get("loteCargueId"),
            actualizado_en=item.get("updatedAt"),
        )

    @staticmethod
    def _consumir_miembro(
        reserva: ReservaCompuesta, miembro: RegistroCompuesto, id_mensaje: str, fecha: str
    ) -> dict[str, Any]:
        return {
            "Update": {
                "TableName": reserva.tabla,
                "Key": serializar({"compositeId": reserva.id_compuesto, "memberId": miembro.id_miembro}),
                "UpdateExpression": CONSUMIR_MIEMBRO,
                "ConditionExpression": CONDICION_MIEMBRO,
                "ExpressionAttributeNames": NOMBRE_ESTADO,
                "ExpressionAttributeValues": serializar(
                    {
                        ":consumed": ESTADO_CONSUMIDO,
                        ":yes": True,
                        ":now": fecha,
                        ":cert": reserva.cert_code,
                        ":mid": id_mensaje,
                        ":ready": ESTADO_LISTO_PARA_NOTIFICAR,
                        ":exec": miembro.id_ejecucion_origen,
                        ":lot": miembro.id_lote,
                        ":previous": miembro.actualizado_en,
                    }
                ),
            }
        }

    @staticmethod
    def _completar_control(reserva: ReservaCompuesta, id_mensaje: str, fecha: str) -> dict[str, Any]:
        return {
            "Update": {
                "TableName": reserva.tabla,
                "Key": serializar({"compositeId": reserva.id_compuesto, "memberId": reserva.control.id_miembro}),
                "UpdateExpression": COMPLETAR_CONTROL,
                "ConditionExpression": CONDICION_CONTROL,
                "ExpressionAttributeNames": NOMBRE_ESTADO,
                "ExpressionAttributeValues": serializar(
                    {
                        ":done": ESTADO_NOTIFICACION_COMPLETADA,
                        ":now": fecha,
                        ":cert": reserva.cert_code,
                        ":mid": id_mensaje,
                        ":progress": ESTADO_NOTIFICACION_EN_CURSO,
                        ":previous": reserva.control.actualizado_en,
                    }
                ),
            }
        }
