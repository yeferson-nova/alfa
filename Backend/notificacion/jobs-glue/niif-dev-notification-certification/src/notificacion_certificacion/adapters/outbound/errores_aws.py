from contextlib import contextmanager
from typing import Iterator

from botocore.exceptions import BotoCoreError, ClientError

from notificacion_certificacion.domain.errores import (
    ErrorConcurrencia,
    ErrorConfiguracion,
    ErrorNotificacion,
    ErrorPermisos,
    ErrorPlataforma,
)

CODIGOS_PERMISOS = frozenset({"AccessDenied", "AccessDeniedException", "UnrecognizedClientException"})
CODIGOS_NO_ENCONTRADO = frozenset(
    {"NoSuchKey", "NoSuchBucket", "ResourceNotFoundException", "NotFoundException"}
)
CODIGOS_CONCURRENCIA = frozenset({"ConditionalCheckFailedException", "TransactionCanceledException"})
CODIGOS_LIMITE = frozenset(
    {
        "ThrottlingException",
        "Throttling",
        "TooManyRequestsException",
        "ProvisionedThroughputExceededException",
        "RequestLimitExceeded",
        "LimitExceededException",
        "SlowDown",
    }
)


def traducir(error: ClientError, contexto: str) -> ErrorNotificacion:
    codigo = error.response.get("Error", {}).get("Code", "")
    detalle = f"{contexto}: {codigo}"
    if codigo in CODIGOS_PERMISOS:
        return ErrorPermisos("NTF-PER-001", detalle)
    if codigo in CODIGOS_NO_ENCONTRADO:
        return ErrorConfiguracion("NTF-CFG-008", detalle)
    if codigo in CODIGOS_CONCURRENCIA:
        return ErrorConcurrencia("NTF-CON-002", detalle)
    if codigo in CODIGOS_LIMITE:
        return ErrorPlataforma("NTF-PLA-001", detalle, recuperable=True)
    return ErrorPlataforma("NTF-PLA-004", detalle)


@contextmanager
def traduciendo(contexto: str) -> Iterator[None]:
    try:
        yield
    except ClientError as error:
        raise traducir(error, contexto) from error
    except BotoCoreError as error:
        raise ErrorPlataforma("NTF-PLA-001", f"{contexto}: {error}", recuperable=True) from error
