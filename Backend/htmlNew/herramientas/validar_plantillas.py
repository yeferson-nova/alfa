"""Valida plantillas y correos de ejemplo contra el estandar de HTML para correo.

Reglas basadas en la practica comun para clientes de correo (Outlook de escritorio con motor Word,
Outlook web y movil, Gmail web y app, Apple Mail): maquetacion con tablas, estilos en linea,
sin JavaScript ni CSS moderno de maquetacion, botones VML para Outlook, tamano menor a 102 KB.

Uso:  python herramientas/validar_plantillas.py            (valida newUx, anterior y previews)
      python herramientas/validar_plantillas.py ruta.html   (valida archivos concretos)
Codigo de salida 1 si hay algun ERROR.
"""
import re
import sys
from html.parser import HTMLParser
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]

ETIQUETAS_VACIAS = {"meta", "br", "img", "hr", "link", "input", "col", "area", "base", "wbr", "source", "param"}
ETIQUETAS_PROHIBIDAS = {"script", "iframe", "form", "object", "embed", "video", "audio", "canvas", "svg", "button", "input", "frame"}
ESQUEMAS_PERMITIDOS = ("https://", "mailto:", "tel:")

CSS_PROHIBIDO = {
    "display flex/grid": re.compile(r"display\s*:\s*(inline-)?(flex|grid)", re.I),
    "position absoluto/fijo": re.compile(r"position\s*:\s*(absolute|fixed|sticky)", re.I),
    "float": re.compile(r"(?<![-\w])float\s*:", re.I),
    "variables o calc": re.compile(r"var\(|calc\(", re.I),
    "unidades vw/vh": re.compile(r"\d\s*(vw|vh|vmin|vmax)\b", re.I),
    "@import / @font-face": re.compile(r"@import|@font-face", re.I),
    "gap / grid-*": re.compile(r"(?<![-\w])gap\s*:|grid-", re.I),
    "animaciones": re.compile(r"@keyframes|animation\s*:|transition\s*:", re.I),
}
CSS_ADVERTENCIA = {
    "background-image / url()": re.compile(r"background-image|url\(", re.I),
    "box-shadow": re.compile(r"box-shadow", re.I),
    "transform / filter": re.compile(r"(?<![-\w])transform\s*:|(?<![-\w])filter\s*:", re.I),
}

MARCADORES_PERMITIDOS = {
    "useCaseName", "periodRange", "tableTotal", "tableSummary", "tableDetail",
    "certificarLink", "nocertificarLink", "avisoHtml",
}
MARCADORES_OBLIGATORIOS = {"useCaseName", "tableTotal", "tableDetail", "certificarLink", "nocertificarLink"}

EMOJIS = re.compile(
    "[\U0001F300-\U0001FAFF\u2600-\u27BF\u23E9-\u23F3]|&#(?:9203|9888|9989|10060|9940|128\\d{3}|129\\d{3});",
)
LIMITE_GMAIL = 102_000
AVISO_TAMANO = 90_000
ANCHO_MAXIMO = 700


class Hallazgos:
    def __init__(self):
        self.errores = []
        self.advertencias = []

    def error(self, mensaje):
        self.errores.append(mensaje)

    def advertencia(self, mensaje):
        self.advertencias.append(mensaje)


def _luminancia(hex_color):
    texto = hex_color.lstrip("#")
    if len(texto) == 3:
        texto = "".join(c * 2 for c in texto)
    canales = [int(texto[i:i + 2], 16) / 255 for i in (0, 2, 4)]
    lineales = [c / 12.92 if c <= 0.03928 else ((c + 0.055) / 1.055) ** 2.4 for c in canales]
    return 0.2126 * lineales[0] + 0.7152 * lineales[1] + 0.0722 * lineales[2]


def contraste(color_a, color_b):
    claro, oscuro = sorted((_luminancia(color_a), _luminancia(color_b)), reverse=True)
    return (claro + 0.05) / (oscuro + 0.05)


class Analizador(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.pila = []
        self.desbalance = []
        self.doctype = False
        self.html_lang = None
        self.metas = []
        self.titulo = ""
        self._en_titulo = False
        self.en_style = False
        self.bloques_style = []
        self.estilos_en_linea = []
        self.tablas = []
        self.pila_tablas = []
        self.enlaces = []
        self.enlaces_blank_sin_rel = 0
        self.etiquetas_prohibidas = []
        self.comentarios = []
        self.tiene_img_sin_alt = False
        self.pares_color = []

    def handle_decl(self, decl):
        if decl.lower().startswith("doctype"):
            self.doctype = True

    def handle_comment(self, data):
        self.comentarios.append(data)

    def handle_starttag(self, etiqueta, atributos):
        attrs = dict(atributos)
        if etiqueta == "html":
            self.html_lang = attrs.get("lang")
        if etiqueta == "meta":
            self.metas.append(attrs)
        if etiqueta == "title":
            self._en_titulo = True
        if etiqueta == "style":
            self.en_style = True
            self.bloques_style.append("")
        if etiqueta in ETIQUETAS_PROHIBIDAS:
            self.etiquetas_prohibidas.append(etiqueta)
        if etiqueta == "link" and "stylesheet" in (attrs.get("rel") or "").lower():
            self.etiquetas_prohibidas.append("link rel=stylesheet")
        if etiqueta == "img" and not attrs.get("alt") and attrs.get("alt") != "":
            self.tiene_img_sin_alt = True
        if etiqueta == "a":
            self.enlaces.append(attrs.get("href"))
            rel = (attrs.get("rel") or "").lower().split()
            if attrs.get("target") == "_blank" and not {"noopener", "noreferrer"} <= set(rel):
                self.enlaces_blank_sin_rel += 1
        if "style" in attrs:
            self.estilos_en_linea.append(attrs["style"])
            self._registrar_color(attrs["style"])
        if etiqueta == "table":
            info = {"role": attrs.get("role"), "tiene_th": False, "celdas_sin_estilo": 0}
            self.tablas.append(info)
            self.pila_tablas.append(info)
        if etiqueta in ("td", "th") and self.pila_tablas:
            if etiqueta == "th":
                self.pila_tablas[-1]["tiene_th"] = True
            if "style" not in attrs:
                self.pila_tablas[-1]["celdas_sin_estilo"] += 1
        if etiqueta not in ETIQUETAS_VACIAS:
            self.pila.append(etiqueta)

    def handle_startendtag(self, etiqueta, atributos):
        self.handle_starttag(etiqueta, atributos)
        if etiqueta not in ETIQUETAS_VACIAS and self.pila and self.pila[-1] == etiqueta:
            self.pila.pop()

    def handle_endtag(self, etiqueta):
        if etiqueta == "title":
            self._en_titulo = False
        if etiqueta == "style":
            self.en_style = False
        if etiqueta == "table" and self.pila_tablas:
            self.pila_tablas.pop()
        if etiqueta in ETIQUETAS_VACIAS:
            return
        if self.pila and self.pila[-1] == etiqueta:
            self.pila.pop()
        else:
            self.desbalance.append(etiqueta)
            if etiqueta in self.pila:
                while self.pila and self.pila[-1] != etiqueta:
                    self.pila.pop()
                if self.pila:
                    self.pila.pop()

    def handle_data(self, datos):
        if self._en_titulo:
            self.titulo += datos
        if self.en_style:
            self.bloques_style[-1] += datos

    def _registrar_color(self, estilo):
        texto = re.search(r"(?<![-\w])color\s*:\s*(#[0-9a-fA-F]{3,6})\b", estilo)
        fondo = re.search(r"background-color\s*:\s*(#[0-9a-fA-F]{3,6})\b", estilo)
        if texto and fondo:
            self.pares_color.append((texto.group(1), fondo.group(1)))


def _sin_comentarios(contenido):
    return re.sub(r"<!--.*?-->", "", contenido, flags=re.S)


def validar(ruta, conjunto):
    contenido = ruta.read_text(encoding="utf-8")
    hallazgos = Hallazgos()
    analizador = Analizador()
    analizador.feed(contenido)
    analizador.close()
    es_correo_final = conjunto == "previews"
    visible = _sin_comentarios(contenido)

    if not analizador.doctype:
        hallazgos.error("Falta DOCTYPE")
    if not analizador.html_lang:
        hallazgos.error("Falta lang en <html>")
    tiene_charset = any(m.get("charset") or "charset=" in (m.get("content") or "").lower() for m in analizador.metas)
    if not tiene_charset:
        hallazgos.error("Falta meta charset / Content-Type UTF-8")
    if not any((m.get("name") or "").lower() == "viewport" for m in analizador.metas):
        hallazgos.error("Falta meta viewport (movil)")
    if not analizador.titulo.strip():
        hallazgos.error("Falta <title>")
    if not any((m.get("name") or "").lower() == "x-apple-disable-message-reformatting" for m in analizador.metas):
        hallazgos.advertencia("Falta meta x-apple-disable-message-reformatting")
    if not any((m.get("name") or "").lower() == "color-scheme" for m in analizador.metas):
        hallazgos.advertencia("Falta meta color-scheme (modo oscuro)")

    if analizador.desbalance or analizador.pila:
        hallazgos.error(f"Etiquetas desbalanceadas: cierres sobrantes={analizador.desbalance[:5]} sin cerrar={analizador.pila[:5]}")
    for etiqueta in sorted(set(analizador.etiquetas_prohibidas)):
        hallazgos.error(f"Etiqueta no soportada en correo: <{etiqueta}>")
    if re.search(r"javascript:|\bon[a-z]+\s*=\s*[\"']", visible, re.I):
        hallazgos.error("Contiene javascript: o manejadores de eventos (onclick, etc.)")
    if analizador.tiene_img_sin_alt:
        hallazgos.error("Hay <img> sin atributo alt")

    sin_role = [t for t in analizador.tablas if t["role"] != "presentation"]
    if sin_role:
        hallazgos.error(f"{len(sin_role)} tablas sin role=\"presentation\"")
    for tabla in analizador.tablas:
        if tabla["tiene_th"] and tabla["celdas_sin_estilo"]:
            hallazgos.error(f"Tabla de datos con {tabla['celdas_sin_estilo']} celdas sin estilo en linea (se pierde en Gmail sin <style>)")

    estilo_total = " ".join(analizador.bloques_style + analizador.estilos_en_linea)
    for nombre, patron in CSS_PROHIBIDO.items():
        if patron.search(estilo_total):
            hallazgos.error(f"CSS no soportado en correo: {nombre}")
    for nombre, patron in CSS_ADVERTENCIA.items():
        if patron.search(estilo_total):
            hallazgos.advertencia(f"CSS de soporte parcial: {nombre}")
    if "table-layout:fixed" not in estilo_total.replace(" ", ""):
        informe_fijo = hallazgos.advertencia if conjunto == "anterior" else hallazgos.error
        informe_fijo("Sin table-layout:fixed en la tabla contenedora: una tabla ancha ensancha todo el correo en movil")
    if "@media" not in estilo_total:
        hallazgos.error("No hay @media para movil")
    if "Arial" not in contenido:
        hallazgos.advertencia("No declara fuente web segura (Arial)")

    for href in analizador.enlaces:
        if not href or not href.strip():
            hallazgos.error("Enlace <a> sin href")
        elif not es_correo_final and href.startswith("$"):
            continue
        elif not href.lower().startswith(ESQUEMAS_PERMITIDOS) and not href.startswith("#"):
            hallazgos.error(f"Enlace con esquema no permitido: {href[:40]}")

    if analizador.enlaces_blank_sin_rel:
        hallazgos.error(f"{analizador.enlaces_blank_sin_rel} enlace(s) con target=\"_blank\" sin rel=\"noopener noreferrer\" (hotspot de SonarQube)")

    comentarios = " ".join(analizador.comentarios)
    if "[if mso" not in comentarios:
        hallazgos.error("Sin comentarios condicionales de Outlook (tabla fantasma / VML)")
    if "v:roundrect" not in comentarios:
        hallazgos.error("Sin botones VML para Outlook de escritorio")
    if 'xmlns:v="urn:schemas-microsoft-com:vml"' not in contenido:
        hallazgos.error("Falta el espacio de nombres VML en <html>")
    anchos = [int(a) for a in re.findall(r"max-width\s*:\s*(\d+)px", estilo_total)]
    if anchos and max(anchos) > ANCHO_MAXIMO:
        hallazgos.advertencia(f"Ancho maximo {max(anchos)} px > {ANCHO_MAXIMO} px")
    if "display:none" not in contenido.replace(" ", "") or "mso-hide" not in contenido:
        hallazgos.advertencia("Sin texto de vista previa (preheader) oculto")
    if "-webkit-overflow-scrolling" in contenido or "overflow-x" in estilo_total:
        hallazgos.advertencia("overflow-x en tablas anchas: soporte parcial en clientes de correo; verificar en Gmail y Outlook")

    peores = sorted(((contraste(t, f), t, f) for t, f in analizador.pares_color))[:1]
    for razon, texto, fondo in peores:
        if razon < 4.5:
            hallazgos.advertencia(f"Contraste bajo {razon:.1f}:1 ({texto} sobre {fondo}); WCAG pide 4.5:1")

    if EMOJIS.search(visible):
        hallazgos.advertencia("Contiene emojis: se dibujan distinto en Outlook, Gmail y movil; usar glifos o texto")

    tamano = len(contenido.encode("utf-8"))
    if tamano >= LIMITE_GMAIL:
        hallazgos.error(f"Pesa {tamano / 1024:.0f} KB: Gmail recorta el mensaje desde ~102 KB")
    elif tamano >= AVISO_TAMANO:
        hallazgos.advertencia(f"Pesa {tamano / 1024:.0f} KB, cerca del limite de Gmail (102 KB)")

    marcadores = set(re.findall(r"\$([A-Za-z_][A-Za-z0-9_]*)", contenido))
    if es_correo_final:
        if marcadores:
            hallazgos.error(f"Marcadores sin reemplazar: {sorted(marcadores)}")
    else:
        desconocidos = marcadores - MARCADORES_PERMITIDOS
        if desconocidos:
            (hallazgos.advertencia if conjunto == "anterior" else hallazgos.error)(f"Marcadores no reconocidos por el job: {sorted(desconocidos)}")
        faltantes = MARCADORES_OBLIGATORIOS - marcadores
        if faltantes:
            hallazgos.error(f"Faltan marcadores obligatorios: {sorted(faltantes)}")
        if "avisoHtml" not in marcadores:
            informe = hallazgos.advertencia if conjunto == "anterior" else hallazgos.error
            informe("Falta $avisoHtml: el job enviaria solo texto cuando haya avisos")
        elif len(re.findall(r"\$avisoHtml", contenido)) != 1:
            hallazgos.error("$avisoHtml debe aparecer una sola vez")
    return hallazgos, tamano


def descubrir():
    conjuntos = {
        "newUx": sorted((RAIZ.parent / "notificacion" / "email-template").glob("*.html")),
        "anterior": sorted((RAIZ / "anterior").glob("*.html")),
        "previews": sorted(p for p in (RAIZ / "previews").rglob("*.html") if p.name != "index.html"),
    }
    return conjuntos


def main(argumentos):
    if argumentos:
        conjuntos = {"personalizado": [Path(a) for a in argumentos]}
    else:
        conjuntos = descubrir()
    total_errores = 0
    total_advertencias = 0
    for conjunto, archivos in conjuntos.items():
        print(f"\n=== {conjunto} ({len(archivos)} archivos) ===")
        for ruta in archivos:
            hallazgos, tamano = validar(ruta, conjunto)
            estado = "OK " if not hallazgos.errores else "ERR"
            resuelta = ruta.resolve()
            nombre = resuelta.relative_to(RAIZ) if RAIZ in resuelta.parents else ruta
            print(f"[{estado}] {nombre} ({tamano / 1024:.1f} KB) errores={len(hallazgos.errores)} avisos={len(hallazgos.advertencias)}")
            for mensaje in hallazgos.errores:
                print(f"       ERROR: {mensaje}")
            for mensaje in hallazgos.advertencias:
                print(f"       aviso: {mensaje}")
            total_errores += len(hallazgos.errores)
            total_advertencias += len(hallazgos.advertencias)
    print(f"\nTOTAL errores={total_errores} avisos={total_advertencias}")
    return 1 if total_errores else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
