from typing import Any, Mapping

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

_SERIALIZER = TypeSerializer()
_DESERIALIZER = TypeDeserializer()


def serialize(values: Mapping[str, Any]) -> dict[str, Any]:
    return {key: _SERIALIZER.serialize(value) for key, value in values.items()}


def deserialize(item: Mapping[str, Any]) -> dict[str, Any]:
    return {key: _DESERIALIZER.deserialize(value) for key, value in item.items()}
