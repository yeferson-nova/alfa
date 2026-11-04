import html
from string import Template
from typing import Any, Mapping

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
