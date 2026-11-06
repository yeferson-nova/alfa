"""Adaptadores de DynamoDB: CNF, PRS, PRG, ejecuciones e idempotencia.

Usan el cliente de bajo nivel con serialización explícita, para controlar las
condiciones de escritura (idempotencia y concurrencia optimista).
"""
import json
from datetime import datetime, timezone
from decimal import Decimal

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

from ...dominio.errores import ErrorConcurrencia, ErrorConfiguracion
from ...aplicacion.idempotencia import EN_PROCESO, FALLIDO
from ...dominio.serializacion import decimal_default, normalizar
from .errores_aws import codigo_aws, protegido

_MAX_ACCIONES_TRANSACCION = 100
_serializador = TypeSerializer()
_deserializador = TypeDeserializer()


def a_dynamo(valor):
    """Valor Python a formato DynamoDB (los float pasan a Decimal)."""
    return _serializador.serialize(
        json.loads(json.dumps(valor, default=decimal_default), parse_float=Decimal))


def desde_dynamo(item):
    return normalizar({k: _deserializador.deserialize(v) for k, v in item.items()})


def _llave(pk, sk):
    return {"PK": {"S": pk}, "SK": {"S": sk}}


def _atributo_de_periodo(clave):
    """Atributo que cambia con el periodo, o ``None`` si el registro no cambia."""
    if clave == "EXTRACCION":
        return "controlExtraccion"
    if clave.startswith("CICLO#"):
        return "segmentos"
    return None


class DynamoCnf:
    def __init__(self, cliente, tabla):
        self._cliente = cliente
        self._tabla = tabla

    @protegido("cnf.consultar")
    def consultar(self, pk):
        registros, inicio = {}, None
        while True:
            parametros = {
                "TableName": self._tabla,
                "KeyConditionExpression": "PK = :pk",
                "ExpressionAttributeValues": {":pk": {"S": pk}},
                "ConsistentRead": True,
            }
            if inicio:
                parametros["ExclusiveStartKey"] = inicio
            respuesta = self._cliente.query(**parametros)
            for item in respuesta.get("Items", []):
                registro = desde_dynamo(item)
                registros[registro["SK"]] = registro
            inicio = respuesta.get("LastEvaluatedKey")
            if not inicio:
                return registros

    @protegido("cnf.persistir_periodo")
    def persistir_periodo(self, cnf, antes):
        acciones = []
        for clave, item in cnf.items():
            atributo = _atributo_de_periodo(clave)
            if atributo is None or item[atributo] == antes[clave][atributo]:
                continue
            acciones.append({"Update": {
                "TableName": self._tabla,
                "Key": _llave(item["PK"], item["SK"]),
                "UpdateExpression": "SET #a = :nuevo",
                "ConditionExpression": "#a = :anterior",
                "ExpressionAttributeNames": {"#a": atributo},
                "ExpressionAttributeValues": {
                    ":nuevo": a_dynamo(item[atributo]),
                    ":anterior": a_dynamo(antes[clave][atributo]),
                },
            }})
        if not acciones:
            return 0
        if len(acciones) > _MAX_ACCIONES_TRANSACCION:
            raise ErrorConfiguracion("Demasiados registros CNF para una transacción",
                                     codigo="CFG-CNF-004")
        try:
            self._cliente.transact_write_items(TransactItems=acciones)
        except Exception as exc:
            if codigo_aws(exc) == "TransactionCanceledException":
                raise ErrorConcurrencia("La configuración CNF cambió durante la corrida",
                                        codigo="CFG-CON-002") from exc
            raise
        return len(acciones)


class DynamoLectura:
    """Lectura por llave (PRS y PRG comparten el mismo esquema PK/SK)."""

    def __init__(self, cliente, tabla):
        self._cliente = cliente
        self._tabla = tabla

    @protegido("dynamo.obtener")
    def obtener(self, pk, sk):
        respuesta = self._cliente.get_item(TableName=self._tabla, Key=_llave(pk, sk),
                                           ConsistentRead=True)
        item = respuesta.get("Item")
        return desde_dynamo(item) if item else None


class DynamoEjecuciones:
    def __init__(self, cliente, tabla):
        self._cliente = cliente
        self._tabla = tabla

    @protegido("ejecuciones.registrar")
    def registrar(self, item):
        try:
            self._cliente.put_item(
                TableName=self._tabla,
                Item={campo: a_dynamo(valor) for campo, valor in item.items()},
                ConditionExpression="attribute_not_exists(PK)",
            )
        except Exception as exc:
            if codigo_aws(exc) == "ConditionalCheckFailedException":
                return False  # reintento: el registro ya existe y no se sobrescribe
            raise
        return True


class DynamoIdempotencia:
    """Registros ``PK = IDEMPOTENCIA#<malla>``, ``SK = <clave>``.

    ``expiraEn`` (epoch) sirve para el TTL de DynamoDB si se activa en la tabla.
    """

    PREFIJO = "IDEMPOTENCIA#"

    def __init__(self, cliente, tabla):
        self._cliente = cliente
        self._tabla = tabla

    @protegido("idempotencia.reclamar")
    def reclamar(self, ambito, clave, ahora, bloqueo_hasta, expira_en):
        try:
            self._cliente.put_item(
                TableName=self._tabla,
                Item={
                    **_llave(self.PREFIJO + ambito, clave),
                    "estado": {"S": EN_PROCESO},
                    "bloqueoHasta": {"N": str(bloqueo_hasta)},
                    "expiraEn": {"N": str(expira_en)},
                    "actualizado": {"S": _ahora_iso()},
                },
                ConditionExpression=("attribute_not_exists(PK) OR estado = :fallido OR "
                                     "(estado = :en_proceso AND bloqueoHasta < :ahora)"),
                ExpressionAttributeValues={
                    ":fallido": {"S": FALLIDO},
                    ":en_proceso": {"S": EN_PROCESO},
                    ":ahora": {"N": str(ahora)},
                },
            )
            return None
        except Exception as exc:
            if codigo_aws(exc) != "ConditionalCheckFailedException":
                raise
        respuesta = self._cliente.get_item(TableName=self._tabla,
                                           Key=_llave(self.PREFIJO + ambito, clave),
                                           ConsistentRead=True)
        existente = desde_dynamo(respuesta.get("Item", {}))
        if isinstance(existente.get("resultado"), str):
            existente["resultado"] = json.loads(existente["resultado"])
        return existente

    @protegido("idempotencia.guardar")
    def guardar(self, ambito, clave, estado, expira_en, resultado=None, error=None):
        valores = {":estado": {"S": estado}, ":expira": {"N": str(expira_en)},
                   ":actualizado": {"S": _ahora_iso()}}
        asignaciones = ["estado = :estado", "expiraEn = :expira", "actualizado = :actualizado"]
        for nombre, contenido in (("resultado", resultado), ("error", error)):
            if contenido is not None:
                asignaciones.append(f"{nombre} = :{nombre}")
                valores[f":{nombre}"] = {"S": json.dumps(contenido, ensure_ascii=False,
                                                         default=decimal_default)}
        self._cliente.update_item(
            TableName=self._tabla,
            Key=_llave(self.PREFIJO + ambito, clave),
            UpdateExpression="SET " + ", ".join(asignaciones),
            ExpressionAttributeValues=valores,
        )


def _ahora_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
