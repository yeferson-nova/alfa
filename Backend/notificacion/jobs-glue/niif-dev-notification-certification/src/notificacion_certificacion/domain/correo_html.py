import html
from string import Template
from typing import Mapping, Sequence

from notificacion_certificacion.domain.modelos import Aviso, ResultadoConsulta
from notificacion_certificacion.domain.textos import TEXTO_ERROR_CONSULTA, TEXTO_SIN_ARCHIVOS, TEXTO_SIN_DATOS

MARCADOR_AVISOS = "$avisoHtml"
VARIABLE_AVISOS = "avisoHtml"
TONO_INFORMATIVO = "info"
TONO_ADVERTENCIA = "warn"
ESTILO_TEXTO = (
    "margin:0 0 8px 0; font-family:Arial, Helvetica, sans-serif; font-size:14px; line-height:20px; color:#1f2a27;"
)
ESTILO_TABLA = (
    "border-collapse:collapse; border:1px solid #d9e1de; font-family:Arial, Helvetica, sans-serif; font-size:14px;"
)
ESTILO_ENCABEZADO = (
    "background-color:#eef2f0; color:#5b6763; font-size:12px; font-weight:bold; text-transform:uppercase; "
    "text-align:left; padding:10px 12px; border:1px solid #d9e1de; white-space:nowrap;"
)
ESTILO_CELDA = "color:#1f2a27; text-align:left; padding:10px 12px; border:1px solid #d9e1de; white-space:nowrap;"
ESTILO_ENLACE = "color:#00875f; text-decoration:underline;"
ESTILO_CELDA_AVISOS = "padding:20px 24px 0 24px;"
ESTILO_BLOQUE_TABLA = "margin:0 0 12px 0;"
COLORES_TONO = {
    TONO_INFORMATIVO: ("#174ea6", "#e8f0fe", "#a8c3f5"),
    TONO_ADVERTENCIA: ("#8a4b00", "#fff4e0", "#f1c27d"),
}


def escapar(texto: str) -> str:
    return html.escape(texto)


def parrafo(texto: str) -> str:
    return f'<p style="{ESTILO_TEXTO}">{html.escape(texto)}</p>'


def resultado_a_html(resultado: ResultadoConsulta) -> str:
    if not resultado.filas:
        return parrafo(TEXTO_SIN_DATOS)
    encabezados = "".join(
        f'<th style="{ESTILO_ENCABEZADO}">{html.escape(nombre)}</th>' for nombre in resultado.encabezados
    )
    filas = "".join(
        "<tr>"
        + "".join(
            f'<td style="{ESTILO_CELDA}">{html.escape(fila[i] if i < len(fila) else "")}</td>'
            for i in range(len(resultado.encabezados))
        )
        + "</tr>"
        for fila in resultado.filas
    )
    return (
        '<table role="presentation" border="0" cellpadding="0" cellspacing="0" '
        f'style="{ESTILO_TABLA}"><tr>{encabezados}</tr>{filas}</table>'
    )


def error_a_html() -> str:
    return parrafo(TEXTO_ERROR_CONSULTA)


def bloque_tabla(contenido: str) -> str:
    return f'<div style="{ESTILO_BLOQUE_TABLA}">{contenido}</div>'


def archivos_a_html(archivos: Sequence[tuple[str, str]]) -> str:
    if not archivos:
        return parrafo(TEXTO_SIN_ARCHIVOS)
    return "".join(
        f'<p style="{ESTILO_TEXTO} word-break:break-all;"><a href="{html.escape(url)}" target="_blank" '
        f'rel="noopener noreferrer" style="{ESTILO_ENLACE}">{html.escape(nombre)}</a></p>'
        for nombre, url in archivos
    )


def recuadro_aviso(aviso: Aviso) -> str:
    color, fondo, borde = COLORES_TONO[aviso.tono]
    lineas = "<br>".join(html.escape(linea) for linea in aviso.texto.splitlines() if linea.strip())
    return (
        '<div style="margin:0 0 8px 0; padding:12px 16px; '
        "font-family:Arial, Helvetica, sans-serif; font-size:14px; line-height:20px; "
        f"color:{color}; background-color:{fondo}; border:1px solid {borde}; "
        f'border-radius:8px;">{lineas}</div>'
    )


def avisos_a_html(avisos: Sequence[Aviso]) -> str:
    if not avisos:
        return ""
    recuadros = "".join(recuadro_aviso(aviso) for aviso in avisos)
    return f'<tr><td class="pad" style="{ESTILO_CELDA_AVISOS}">{recuadros}</td></tr>'


def plantilla_admite_avisos(plantilla: str, avisos: Sequence[Aviso]) -> bool:
    return not avisos or MARCADOR_AVISOS in plantilla


def componer_html(
    plantilla: str,
    variables: Mapping[str, str],
    fragmentos: Mapping[str, str],
    avisos: Sequence[Aviso],
) -> str:
    seguras = {clave: html.escape(str(valor)) for clave, valor in variables.items()}
    return Template(plantilla).safe_substitute(
        {**seguras, **fragmentos, VARIABLE_AVISOS: avisos_a_html(avisos)}
    )
