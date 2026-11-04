"""Interfaces hacia AWS. El servicio depende de estas, no de boto3.

En producción las implementan los adaptadores (``adaptadores/``); en pruebas,
dobles en memoria (``tests/fakes.py``).
"""
from typing import Protocol


class RepositorioCnf(Protocol):
    def consultar(self, pk: str) -> dict:
        """Registros de la malla, indexados por SK (valores ya normalizados)."""

    def persistir_periodo(self, cnf: dict, antes: dict) -> None:
        """Escribe los cambios de periodo en una transacción con condición optimista."""


class RepositorioPrs(Protocol):
    def obtener(self, referencia: str, sk: str) -> dict | None:
        """Estructura PRS (normalizada) o ``None`` si no existe."""


class AlmacenJson(Protocol):
    def leer(self, uri: str) -> dict: ...

    def escribir(self, uri: str, contenido: dict, metadatos: dict) -> dict: ...


class Orquestador(Protocol):
    def iniciar(self, nombre: str, entrada: dict) -> str:
        """Inicia la Step Function y devuelve el ARN de la ejecución."""


class RegistroEjecuciones(Protocol):
    def registrar(self, item: dict) -> None: ...
