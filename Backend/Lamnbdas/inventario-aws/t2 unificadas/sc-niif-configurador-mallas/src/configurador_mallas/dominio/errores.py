"""Errores del configurador con la taxonomía corporativa (HU-GI-06).

Cada error lleva dominio, severidad, si es recuperable, la acción esperada y el
destinatario. El handler lo registra en CloudWatch con esos campos y lo vuelve a
lanzar, para que Lambda cuente el error y aplique su política de reintentos.
"""
from dataclasses import dataclass, replace


@dataclass(frozen=True)
class Clasificacion:
    codigo: str
    dominio: str
    severidad: str
    recuperable: bool
    accion: str
    destinatario: str


class ErrorConfigurador(Exception):
    """Error base. Las subclases fijan la clasificación por defecto."""

    CLASIFICACION = Clasificacion(
        "CFG-PLT-000", "PLATAFORMA", "ALTA", True,
        "Revisar el log y reintentar; si persiste, escalar", "Soporte",
    )

    def __init__(self, mensaje, *, codigo=None, detalle=None):
        super().__init__(mensaje)
        base = self.CLASIFICACION
        self.clasificacion = base if codigo is None else replace(base, codigo=codigo)
        self.detalle = dict(detalle or {})

    def to_dict(self):
        c = self.clasificacion
        return {
            "codigo": c.codigo,
            "dominio": c.dominio,
            "severidad": c.severidad,
            "recuperable": c.recuperable,
            "accion": c.accion,
            "destinatario": c.destinatario,
            "mensaje": str(self),
            "detalle": self.detalle,
        }


class ErrorEntrada(ErrorConfigurador):
    """El evento de invocación no cumple el contrato. No se reintenta."""

    CLASIFICACION = Clasificacion(
        "CFG-ENT-001", "ENTRADA", "MEDIA", False,
        "Corregir el evento de invocación", "Equipo de desarrollo",
    )


class ErrorConfiguracion(ErrorConfigurador):
    """Falta o es inválida la metadata (entorno, catálogo, CNF, PRS o PRG)."""

    CLASIFICACION = Clasificacion(
        "CFG-CNF-001", "CONFIGURACION", "ALTA", False,
        "Corregir la metadata de la malla (entorno, catálogo, cnf-mallas, "
        "prs-estructuras o prg-programaciones)",
        "Gestor de configuración",
    )


class ErrorConcurrencia(ErrorConfigurador):
    """Otra invocación trabaja sobre lo mismo. Se reintenta."""

    CLASIFICACION = Clasificacion(
        "CFG-CON-001", "CONCURRENCIA", "MEDIA", True,
        "Reintentar: hay otra invocación en curso o la configuración cambió",
        "Soporte",
    )


class ErrorPermisos(ErrorConfigurador):
    """El rol no tiene permiso. Se detiene sin reintentar."""

    CLASIFICACION = Clasificacion(
        "CFG-PER-001", "PERMISOS", "CRITICA", False,
        "Detener y revisar la política del rol", "Seguridad de la información",
    )


class ErrorPlataforma(ErrorConfigurador):
    """Falla transitoria de AWS (límites, indisponibilidad). Se reintenta."""

    CLASIFICACION = Clasificacion(
        "CFG-PLT-001", "PLATAFORMA", "ALTA", True,
        "Reintentar con espera; si persiste, escalar", "Soporte",
    )


class ErrorOrquestacion(ErrorConfigurador):
    """La Step Function rechazó el inicio por un conflicto. No se reintenta."""

    CLASIFICACION = Clasificacion(
        "CFG-ORQ-001", "ORQUESTACION", "ALTA", False,
        "Revisar la ejecución existente de la Step Function", "Soporte",
    )
