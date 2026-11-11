from typing import Any

UNCLASSIFIED_ERROR = "ORQ-PLA-999"


class OrchestratorError(Exception):
    domain = "PLATAFORMA"
    severity = "ALTA"
    recoverable = False
    action = "Revisar el detalle del error"
    recipient = "SOPORTE"

    def __init__(self, code: str, message: str, recoverable: bool | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        if recoverable is not None:
            self.recoverable = recoverable

    def as_log_fields(self) -> dict[str, Any]:
        return {
            "codigo": self.code,
            "dominio": self.domain,
            "severidad": self.severity,
            "recuperable": self.recoverable,
            "accion": self.action,
            "destinatario": self.recipient,
            "detalle": self.message,
        }


class InputError(OrchestratorError):
    domain = "ENTRADA"
    severity = "MEDIA"
    action = "Revisar el evento del stream o del disparo programado"
    recipient = "DESARROLLO"


class ConfigurationError(OrchestratorError):
    domain = "CONFIGURACION"
    action = "Corregir el JSON global o las variables de entorno de la lambda"
    recipient = "CONFIGURACION"


class DataError(OrchestratorError):
    domain = "DATOS"
    action = "Revisar el registro de la tabla de notificaciones y el archivo de certificacion"
    recipient = "SOPORTE"


class ConcurrencyError(OrchestratorError):
    domain = "CONCURRENCIA"
    severity = "MEDIA"
    recoverable = True
    action = "Se reintenta solo; si persiste, revisar quien modifica el registro"
    recipient = "SOPORTE"


class AccessDeniedError(OrchestratorError):
    domain = "PERMISOS"
    action = "Revisar los permisos del rol de la lambda"
    recipient = "SEGURIDAD"


class PlatformError(OrchestratorError):
    domain = "PLATAFORMA"
    action = "Reintentar y, si persiste, escalar a soporte"
    recipient = "SOPORTE"


def classify(error: Exception) -> OrchestratorError:
    if isinstance(error, OrchestratorError):
        return error
    return PlatformError(UNCLASSIFIED_ERROR, f"{type(error).__name__}: {error}")
