"""Traducción de los errores de AWS a la taxonomía del configurador."""
import functools

from ...dominio.errores import (ErrorConcurrencia, ErrorConfiguracion, ErrorConfigurador,
                       ErrorPermisos, ErrorPlataforma)

_PERMISOS = {"AccessDenied", "AccessDeniedException", "UnauthorizedOperation",
             "KMSAccessDeniedException", "KMSDisabledException", "AuthorizationErrorException"}
_TRANSITORIOS = {"ThrottlingException", "Throttling", "TooManyRequestsException",
                 "ProvisionedThroughputExceededException", "RequestLimitExceeded",
                 "ServiceUnavailable", "ServiceUnavailableException", "InternalServerError",
                 "InternalFailure", "InternalServerErrorException", "SlowDown", "RequestTimeout",
                 "TransactionInProgressException"}
_NO_ENCONTRADO = {"ResourceNotFoundException", "NoSuchKey", "NoSuchBucket",
                  "StateMachineDoesNotExist", "StateMachineDeleting"}
_CONFLICTO = {"ConditionalCheckFailedException", "TransactionCanceledException",
              "TransactionConflictException"}


def codigo_aws(exc):
    """Código de error de una ``ClientError`` de botocore, o ``None``."""
    respuesta = getattr(exc, "response", None)
    if isinstance(respuesta, dict):
        return respuesta.get("Error", {}).get("Code")
    return None


def traducir(exc, operacion):
    codigo = codigo_aws(exc)
    detalle = {"operacion": operacion, "codigoAws": codigo, "tipo": type(exc).__name__}
    if codigo in _PERMISOS or (codigo and "AccessDenied" in codigo):
        return ErrorPermisos(f"Permiso denegado en {operacion}", codigo="CFG-PER-001",
                             detalle=detalle)
    if codigo in _NO_ENCONTRADO:
        return ErrorConfiguracion(f"Recurso no encontrado en {operacion}", codigo="CFG-CNF-009",
                                  detalle=detalle)
    if codigo in _CONFLICTO:
        return ErrorConcurrencia(f"Conflicto de escritura en {operacion}", codigo="CFG-CON-002",
                                 detalle=detalle)
    if codigo in _TRANSITORIOS or codigo is None:
        # Sin código: fallas de red o de conexión de botocore; también son transitorias.
        return ErrorPlataforma(f"Falla transitoria de AWS en {operacion}", codigo="CFG-PLT-001",
                               detalle=detalle)
    return ErrorPlataforma(f"Error de AWS no clasificado en {operacion}", codigo="CFG-PLT-000",
                           detalle=detalle)


def protegido(operacion):
    """Decorador: deja pasar los errores propios y traduce los de AWS."""
    def decorador(funcion):
        @functools.wraps(funcion)
        def envoltura(*args, **kwargs):
            try:
                return funcion(*args, **kwargs)
            except ErrorConfigurador:
                raise
            except Exception as exc:
                raise traducir(exc, operacion) from exc
        return envoltura
    return decorador
