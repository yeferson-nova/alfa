import re

from notificacion_certificacion.domain.constantes import COLUMNA_REGISTROS
from notificacion_certificacion.domain.errores import ErrorConfiguracion
from notificacion_certificacion.domain.modelos import ResultadoConsulta

FORMATO_CANTIDAD = re.compile(r"[0-9]+")


def extraer_cantidad(resultado: ResultadoConsulta, nombre: str) -> int:
    es_una_celda = len(resultado.filas) == 1 and len(resultado.filas[0]) == 1
    if resultado.encabezados != (COLUMNA_REGISTROS,) or not es_una_celda:
        raise ErrorConfiguracion("NTF-CFG-007", f"No se pudo confirmar disponibilidad de {nombre}")
    valor = resultado.filas[0][0].strip()
    if not FORMATO_CANTIDAD.fullmatch(valor):
        raise ErrorConfiguracion("NTF-CFG-007", f"No se pudo confirmar disponibilidad de {nombre}")
    return int(valor)
