import html
from string import Template
from typing import Any, Mapping

from notificacion_certificacion.domain.errores import ErrorConfiguracion

REPETICIONES_DESESCAPE = 3


def limpiar_url(url: Any) -> str:
    texto = str(url or "")
    for _ in range(REPETICIONES_DESESCAPE):
        siguiente = html.unescape(texto)
        if siguiente == texto:
            break
        texto = siguiente
    return texto.strip()


def parametrizar(texto: Any, variables: Mapping[str, Any], desescapar: bool = False) -> str:
    valor = str(texto)
    if desescapar:
        valor = limpiar_url(valor)
    return Template(valor).safe_substitute(variables)


def parametrizar_estricto(texto: Any, variables: Mapping[str, Any], origen: str) -> str:
    valor = limpiar_url(str(texto))
    try:
        return Template(valor).substitute(variables)
    except KeyError as error:
        raise ErrorConfiguracion("NTF-CFG-009", f"{origen}: variable sin valor {error.args[0]}") from error
    except ValueError as error:
        raise ErrorConfiguracion("NTF-CFG-009", f"{origen}: marcador invalido en el texto") from error
