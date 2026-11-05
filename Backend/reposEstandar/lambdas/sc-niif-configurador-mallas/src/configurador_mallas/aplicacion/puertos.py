"""Puertos de salida: lo que el caso de uso necesita del exterior.

El caso de uso depende de estas interfaces, no de boto3. En producción las
implementan los adaptadores de ``adaptadores/salida``; en las pruebas, dobles
en memoria (``tests/fakes.py``).
"""
from enum import Enum
from typing import Protocol


class EventoLog(str, Enum):
    """Catálogo cerrado de eventos de log (HU-GI-07)."""

    CORRIDA_INICIADA = "CORRIDA_INICIADA"
    IDEMPOTENCIA_REUTILIZADA = "IDEMPOTENCIA_REUTILIZADA"
    CONFIGURACION_LEIDA = "CONFIGURACION_LEIDA"
    CONFIGURACION_PUBLICADA = "CONFIGURACION_PUBLICADA"
    EJECUCION_INICIADA = "EJECUCION_INICIADA"
    EJECUCION_YA_REGISTRADA = "EJECUCION_YA_REGISTRADA"
    CORRIDA_FINALIZADA = "CORRIDA_FINALIZADA"
    CORRIDA_FALLIDA = "CORRIDA_FALLIDA"


class Metrica(str, Enum):
    CORRIDA_EXITOSA = "CorridaExitosa"
    CORRIDA_FALLIDA = "CorridaFallida"
    CORRIDA_REUTILIZADA = "CorridaReutilizada"
    DURACION = "DuracionMs"


class Observador(Protocol):
    def usar_namespace(self, namespace: str) -> None: ...

    def con_contexto(self, **campos) -> None:
        """Agrega campos (malla, ejecucion_id, ...) a todos los registros siguientes."""

    def evento(self, evento: EventoLog, **campos) -> None: ...

    def error(self, error) -> None:
        """Registra un ``ErrorConfigurador`` con su clasificación y cuenta la métrica."""

    def metrica(self, metrica: Metrica, valor, unidad: str, **dimensiones) -> None: ...


class RepositorioCnf(Protocol):
    def consultar(self, pk: str) -> dict:
        """Registros de la malla indexados por SK, con valores normalizados."""

    def persistir_periodo(self, cnf: dict, antes: dict) -> int:
        """Escribe los cambios de periodo con condición optimista; devuelve cuántos cambió."""


class RepositorioPrs(Protocol):
    def obtener(self, referencia: str, sk: str) -> dict | None: ...


class RepositorioProgramaciones(Protocol):
    def obtener(self, pk: str, sk: str) -> dict | None: ...


class AlmacenJson(Protocol):
    def leer(self, uri: str) -> dict: ...

    def escribir(self, uri: str, contenido: dict, metadatos: dict) -> dict: ...


class Orquestador(Protocol):
    def iniciar(self, maquina: str, nombre: str, entrada: dict) -> str:
        """Inicia la Step Function y devuelve el ARN de la ejecución.

        Con el mismo nombre y la misma entrada, Step Functions devuelve la ejecución
        existente en vez de crear otra.
        """


class RegistroEjecuciones(Protocol):
    def registrar(self, item: dict) -> bool:
        """Crea el registro; devuelve False si ya existía (no lo sobrescribe)."""


class RepositorioIdempotencia(Protocol):
    def reclamar(self, ambito: str, clave: str, ahora: int, bloqueo_hasta: int,
                 expira_en: int) -> dict | None:
        """Toma la clave si está libre, falló antes o su bloqueo venció.

        Devuelve ``None`` si la tomó; si no, el registro existente
        (``estado``, ``resultado``, ``bloqueoHasta``).
        """

    def guardar(self, ambito: str, clave: str, estado: str, expira_en: int,
                resultado: dict | None = None, error: dict | None = None) -> None: ...
