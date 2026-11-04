"""Catálogo de mallas: lo que hoy está quemado en cada copia del configurador.

Para sumar una malla se agrega una entrada aquí (o, más adelante, se lee de
``cnf-mallas``). No se copia código.
"""
from dataclasses import dataclass

from .errores import ConfiguracionError


@dataclass(frozen=True)
class MallaSpec:
    nombre: str               # PRIMAS_RENTAS
    nombre_caso: str          # primas_rentas (campo nombre_caso de las plantillas)
    prefijo_ejecucion: str    # rentas (nombre de la ejecución de la Step Function)
    total_registros_cnf: int = 9
    total_ciclos: int = 4
    estado_remediacion: str = "PROVISIONAL_SIN_REGLAS_TRANSFORMATIVAS"

    @property
    def pk(self):
        return f"MALLA#{self.nombre}"

    @property
    def slug(self):
        return self.nombre.lower().replace("_", "-")


CATALOGO = {
    "PRIMAS_RENTAS": MallaSpec(
        nombre="PRIMAS_RENTAS",
        nombre_caso="primas_rentas",
        prefijo_ejecucion="rentas",
    ),
}


def obtener_malla(nombre):
    try:
        return CATALOGO[nombre]
    except KeyError:
        raise ConfiguracionError(
            f"Malla desconocida: {nombre}. Disponibles: {sorted(CATALOGO)}"
        ) from None
