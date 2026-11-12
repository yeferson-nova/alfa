from typing import Any, Mapping

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

_SERIALIZADOR = TypeSerializer()
_DESERIALIZADOR = TypeDeserializer()


def serializar(valores: Mapping[str, Any]) -> dict[str, Any]:
    return {clave: _SERIALIZADOR.serialize(valor) for clave, valor in valores.items()}


def deserializar(item: Mapping[str, Any]) -> dict[str, Any]:
    return {clave: _DESERIALIZADOR.deserialize(valor) for clave, valor in item.items()}
