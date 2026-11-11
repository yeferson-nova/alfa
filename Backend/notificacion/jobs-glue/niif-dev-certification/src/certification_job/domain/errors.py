from typing import Any


class CertificationError(Exception):
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


class InputError(CertificationError):
    domain = "ENTRADA"
    severity = "MEDIA"
    action = "Corregir los argumentos con los que se lanza el job"
    recipient = "DESARROLLO"


class ConfigurationError(CertificationError):
    domain = "CONFIGURACION"
    action = "Corregir el JSON global o el JSON del caso"
    recipient = "CONFIGURACION"


class DataError(CertificationError):
    domain = "DATOS"
    action = "Revisar el resultado de la consulta en Athena antes de reintentar"
    recipient = "SOPORTE"


class AccessDeniedError(CertificationError):
    domain = "PERMISOS"
    action = "Revisar los permisos del rol del job"
    recipient = "SEGURIDAD"


class PlatformError(CertificationError):
    domain = "PLATAFORMA"
    action = "Reintentar y, si persiste, escalar a soporte"
    recipient = "SOPORTE"
