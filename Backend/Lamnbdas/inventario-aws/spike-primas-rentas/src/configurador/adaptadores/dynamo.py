"""Adaptadores de DynamoDB: CNF, PRS y registro de ejecuciones."""
import json
from decimal import Decimal

from boto3.dynamodb.conditions import Key
from boto3.dynamodb.types import TypeSerializer

from ..errores import ConfiguracionError
from ..json_utils import normalizar

_MAX_ACCIONES_TRANSACCION = 100
_serializer = TypeSerializer()


def _serializar(valor):
    return _serializer.serialize(json.loads(json.dumps(valor, default=str), parse_float=Decimal))


def _atributo_de_periodo(clave):
    """Atributo que cambia al cambiar el periodo, o ``None`` si el registro no cambia."""
    if clave == "EXTRACCION":
        return "controlExtraccion"
    if clave.startswith("CICLO#"):
        return "segmentos"
    return None


class DynamoCnf:
    def __init__(self, recurso, cliente, tabla):
        self._tabla = recurso.Table(tabla)
        self._cliente = cliente
        self._nombre = tabla

    def consultar(self, pk):
        respuesta = self._tabla.query(KeyConditionExpression=Key("PK").eq(pk), ConsistentRead=True)
        return {item["SK"]: normalizar(item) for item in respuesta.get("Items", [])}

    def persistir_periodo(self, cnf, antes):
        acciones = []
        for clave, item in cnf.items():
            atributo = _atributo_de_periodo(clave)
            if atributo is None or item[atributo] == antes[clave][atributo]:
                continue
            acciones.append(
                {
                    "Update": {
                        "TableName": self._nombre,
                        "Key": {"PK": {"S": item["PK"]}, "SK": {"S": item["SK"]}},
                        "UpdateExpression": "SET #a = :nuevo",
                        "ConditionExpression": "#a = :anterior",
                        "ExpressionAttributeNames": {"#a": atributo},
                        "ExpressionAttributeValues": {
                            ":nuevo": _serializar(item[atributo]),
                            ":anterior": _serializar(antes[clave][atributo]),
                        },
                    }
                }
            )
        if not acciones:
            return
        if len(acciones) > _MAX_ACCIONES_TRANSACCION:
            raise ConfiguracionError("Demasiados registros CNF para una transaccion")
        self._cliente.transact_write_items(TransactItems=acciones)


class DynamoPrs:
    def __init__(self, recurso, tabla):
        self._tabla = recurso.Table(tabla)

    def obtener(self, referencia, sk):
        respuesta = self._tabla.get_item(Key={"PK": referencia, "SK": sk}, ConsistentRead=True)
        item = respuesta.get("Item")
        return normalizar(item) if item else None


class DynamoEjecuciones:
    def __init__(self, cliente, tabla):
        self._cliente = cliente
        self._tabla = tabla

    def registrar(self, item):
        self._cliente.put_item(
            TableName=self._tabla,
            Item={campo: {"S": valor} for campo, valor in item.items()},
        )
