"""Serialización de valores de DynamoDB (Decimal) a tipos JSON."""
import json
from decimal import Decimal


def decimal_default(valor):
    if isinstance(valor, Decimal):
        return int(valor) if valor % 1 == 0 else float(valor)
    raise TypeError(f"Tipo no serializable: {type(valor)}")


def normalizar(valor):
    """Devuelve una copia del valor con los Decimal convertidos a int o float."""
    return json.loads(json.dumps(valor, default=decimal_default))
