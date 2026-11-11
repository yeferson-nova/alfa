from typing import Iterable

from notificacion_certificacion.domain.modelos import ResultadoConsulta
from notificacion_certificacion.domain.textos import TEXTO_SIN_DATOS

SEPARADOR_COLUMNAS = " | "
SEPARADOR_BLOQUES = "\n\n"


def _formatear_fila(encabezados: tuple[str, ...], fila: tuple[str, ...]) -> str:
    valores = (fila[i] if i < len(fila) else "" for i in range(len(encabezados)))
    return SEPARADOR_COLUMNAS.join(f"{encabezado}: {valor}" for encabezado, valor in zip(encabezados, valores))


def formatear_resultado(resultado: ResultadoConsulta) -> str:
    if not resultado.filas:
        return TEXTO_SIN_DATOS
    return "\n".join(_formatear_fila(resultado.encabezados, fila) for fila in resultado.filas)


def unir_bloques(bloques: Iterable[str]) -> str:
    return SEPARADOR_BLOQUES.join(bloques).strip()
