"""Contrato de entrada y salida del configurador."""
from dataclasses import dataclass, field

from .errores import ConfiguracionError
from .periodo import validar_formato


@dataclass(frozen=True)
class Evento:
    publicar: bool = False
    ejecutar: bool = False
    periodo_negocio: str | None = None
    modo_ejecucion: str = "NORMAL"
    origen: str = "CONFIGURADOR"
    malla: str | None = None  # si no llega, se usa la malla por defecto del entorno

    @classmethod
    def desde_dict(cls, event):
        event = event or {}
        publicar = bool(event.get("publicar", False))
        ejecutar = bool(event.get("ejecutar", publicar))
        if ejecutar and not publicar:
            raise ConfiguracionError("Para ejecutar la Step se requiere publicar=true")

        periodo = None
        if "periodoNegocio" in event:
            periodo = validar_formato(event["periodoNegocio"])

        return cls(
            publicar=publicar,
            ejecutar=ejecutar,
            periodo_negocio=periodo,
            modo_ejecucion=event.get("modoEjecucion", "NORMAL"),
            origen=event.get("origen", "CONFIGURADOR"),
            malla=event.get("malla"),
        )


@dataclass
class Resultado:
    malla: str
    publicar: bool
    ejecutar: bool
    periodo_negocio: str
    registros_cnf: int
    publicados: list = field(default_factory=list)
    execution_name: str | None = None
    execution_arn: str | None = None
    ejc_pk: str | None = None
    ejc_sk: str | None = None
    estado: str = "VALIDADO"

    def to_dict(self):
        return {
            "estado": self.estado,
            "malla": self.malla,
            "publicar": self.publicar,
            "ejecutar": self.ejecutar,
            "periodoNegocio": self.periodo_negocio,
            "registrosCNF": self.registros_cnf,
            "publicados": self.publicados,
            "executionName": self.execution_name,
            "executionArn": self.execution_arn,
            "ejcPK": self.ejc_pk,
            "ejcSK": self.ejc_sk,
        }
