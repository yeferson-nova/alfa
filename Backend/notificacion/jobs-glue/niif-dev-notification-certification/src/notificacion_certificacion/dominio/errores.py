from typing import Any


class ErrorNotificacion(Exception):
    dominio = "PLATAFORMA"
    severidad = "ALTA"
    recuperable = False
    accion = "Revisar el detalle del error y la configuracion del job"
    destinatario = "SOPORTE"

    def __init__(self, codigo: str, mensaje: str, recuperable: bool | None = None) -> None:
        super().__init__(mensaje)
        self.codigo = codigo
        self.mensaje = mensaje
        if recuperable is not None:
            self.recuperable = recuperable

    def como_diccionario(self) -> dict[str, Any]:
        return {
            "codigo": self.codigo,
            "dominio": self.dominio,
            "severidad": self.severidad,
            "recuperable": self.recuperable,
            "accion": self.accion,
            "destinatario": self.destinatario,
            "mensaje": self.mensaje,
        }


class ErrorEntrada(ErrorNotificacion):
    dominio = "ENTRADA"
    severidad = "MEDIA"
    accion = "Corregir los argumentos con los que se lanza el job"
    destinatario = "DESARROLLO"


class ErrorConfiguracion(ErrorNotificacion):
    dominio = "CONFIGURACION"
    severidad = "ALTA"
    accion = "Corregir la configuracion del caso de uso o la global"
    destinatario = "CONFIGURACION"


class ErrorConcurrencia(ErrorNotificacion):
    dominio = "CONCURRENCIA"
    severidad = "ALTA"
    accion = "Revisar el estado del grupo compuesto antes de reintentar"
    destinatario = "SOPORTE"


class ErrorPermisos(ErrorNotificacion):
    dominio = "PERMISOS"
    severidad = "ALTA"
    accion = "Revisar los permisos del rol del job"
    destinatario = "SEGURIDAD"


class ErrorPlataforma(ErrorNotificacion):
    dominio = "PLATAFORMA"
    severidad = "ALTA"
    accion = "Reintentar y, si persiste, escalar a soporte"
    destinatario = "SOPORTE"


class ErrorConsultaFallida(ErrorPlataforma):
    pass
