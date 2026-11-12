from typing import Any


class NotificationError(Exception):
    domain = "PLATAFORMA"
    severity = "ALTA"
    recoverable = False
    action = "Revisar el detalle del error y la configuracion del job"
    recipient = "SOPORTE"

    def __init__(self, code: str, message: str, recoverable: bool | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        if recoverable is not None:
            self.recoverable = recoverable

    def as_dict(self) -> dict[str, Any]:
        return {
            "codigo": self.code,
            "dominio": self.domain,
            "severidad": self.severity,
            "recuperable": self.recoverable,
            "accion": self.action,
            "destinatario": self.recipient,
            "mensaje": self.message,
        }


class InputError(NotificationError):
    domain = "ENTRADA"
    severity = "MEDIA"
    action = "Corregir los argumentos con los que se lanza el job"
    recipient = "DESARROLLO"


class ConfigurationError(NotificationError):
    domain = "CONFIGURACION"
    severity = "ALTA"
    action = "Corregir la configuracion del caso de uso o la global"
    recipient = "CONFIGURACION"


class ConcurrencyError(NotificationError):
    domain = "CONCURRENCIA"
    severity = "ALTA"
    action = "Revisar el estado del grupo compuesto antes de reintentar"
    recipient = "SOPORTE"


class AccessDeniedError(NotificationError):
    domain = "PERMISOS"
    severity = "ALTA"
    action = "Revisar los permisos del rol del job"
    recipient = "SEGURIDAD"


class PlatformError(NotificationError):
    domain = "PLATAFORMA"
    severity = "ALTA"
    action = "Reintentar y, si persiste, escalar a soporte"
    recipient = "SOPORTE"


class QueryFailedError(PlatformError):
    pass
