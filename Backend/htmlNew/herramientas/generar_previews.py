"""Genera correos de ejemplo (HTML ya lleno) a partir de las plantillas newUx.

Usa las mismas funciones del job de Glue (build_html_body, result_to_html,
details_to_html, notices_to_html), de modo que lo que se ve aqui es lo que SES envia.
Todos los datos son inventados. No usa AWS.

Uso:  python herramientas/generar_previews.py
"""
import html
import importlib
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock, patch

RAIZ_HTML = Path(__file__).resolve().parents[1]
DIRECTORIO_PLANTILLAS = RAIZ_HTML.parent / "notificacion" / "email-template"
DIRECTORIO_PREVIEWS = RAIZ_HTML / "previews"
DIRECTORIO_JOB = RAIZ_HTML.parent / "Glue" / "niif-dev-notification-certification"

PERIODO = "01/10/2026 - 31/10/2026"
ENLACE_CORTO = "https://redir.example/r?t=a1b2c3d4"
AVISO_REGENERACION = (
    "CASO DE USO REEJECUTADO SIN REGISTROS NUEVOS\n"
    "Archivos regenerados desde Gold existente del período.\n"
    "Silver y Gold no se ejecutaron nuevamente.\n"
    "No se verificaron cambios en registros ya procesados."
)

CASOS = {
    "primas_arl_cert": "Primas ARL",
    "primas_rentas_cert": "Primas Rentas y Endosos",
    "auxilio_funerario_cert": "Auxilios Funerarios",
    "previsional": "Previsional",
    "rentas_vitalicias_cert": "Rentas Vitalicias",
    "reservas_matematicas_cert": "Reservas Matemáticas",
}

VARIANTES = (
    ("01-normal", "Normal"),
    ("02-regenerado", "Regenerado"),
    ("03-sin-datos", "Sin datos"),
    ("04-regenerado-sin-datos", "Regenerado y sin datos"),
    ("05-disponibilidad-parcial", "Disponibilidad parcial"),
    ("06-estres", "Estrés"),
)


def _argumentos_falsos():
    nombres = (
        "useCaseId", "certCode", "emailType", "CONFIG_BUCKET", "CONFIG_KEY",
        "GLOBAL_CONFIG_KEY", "SES_SENDER", "REDIRECT_BASE_URL", "SHORT_LINKS_TABLE",
    )
    argv = ["job.py"]
    for nombre in nombres:
        argv += [f"--{nombre}", "x"]
    return argv


def _resolver(argv, nombres):
    return {nombre: argv[argv.index(f"--{nombre}") + 1] for nombre in nombres}


def cargar_job():
    paquete = types.ModuleType("awsglue")
    utilidades = types.ModuleType("awsglue.utils")
    utilidades.getResolvedOptions = _resolver
    paquete.utils = utilidades
    sys.modules["awsglue"] = paquete
    sys.modules["awsglue.utils"] = utilidades
    sys.path.insert(0, str(DIRECTORIO_JOB))
    with patch.object(sys, "argv", _argumentos_falsos()), patch("boto3.client"), patch("boto3.resource"):
        return importlib.import_module("notification_certification")


def tabla(encabezados, filas):
    return {"error": False, "headers": list(encabezados), "rows": [list(fila) for fila in filas]}


def datos_normales():
    return {
        "total": tabla(["registros", "valor_total"], [["1.203.789", "$ 45.210.330.120"]]),
        "resumen": tabla(
            ["origen_renta", "registros", "valor"],
            [["Pensión de vejez", "820.114", "$ 30.100.250.000"], ["Pensión de invalidez", "383.675", "$ 15.110.080.120"]],
        ),
        "archivos": [
            ("detalle_202610_parte_1.csv", ENLACE_CORTO),
            ("detalle_202610_parte_2.csv", ENLACE_CORTO),
        ],
        "avisos": [],
    }


def datos_regenerado():
    datos = datos_normales()
    datos["avisos"] = [("info", AVISO_REGENERACION)]
    return datos


def datos_sin_datos():
    return {
        "total": tabla(["registros", "valor_total"], []),
        "resumen": tabla(["origen_renta", "registros", "valor"], []),
        "archivos": [],
        "avisos": [("warn", "No se encontraron datos procesados para el periodo seleccionado: 202610.")],
    }


def datos_regenerado_sin_datos():
    datos = datos_sin_datos()
    datos["avisos"] = [("info", AVISO_REGENERACION)] + datos["avisos"]
    return datos


def datos_disponibilidad_parcial():
    datos = datos_normales()
    datos["avisos"] = [
        (
            "warn",
            "Disponibilidad por caso de uso:\n"
            "Rentas Vitalicias: no se encontraron datos activos para el periodo 202610.\n"
            "El archivo y los resumenes incluyen los datos disponibles de los demas casos.",
        )
    ]
    return datos


def datos_estres():
    columnas = [f"columna_muy_larga_numero_{numero}" for numero in range(1, 13)]
    filas = [
        [f"Valor largo de ejemplo con acentos áéíóú ñ y símbolos & < > \" ' fila {fila} col {col}" for col in range(12)]
        for fila in range(1, 4)
    ]
    resumen_columnas = [f"campo_{numero}" for numero in range(1, 11)]
    resumen_filas = [[f"{fila * 1000 + col:,}".replace(",", ".") for col in range(10)] for fila in range(1, 9)]
    archivos = [
        (f"archivo_detalle_con_un_nombre_extremadamente_largo_para_probar_el_ajuste_de_linea_{numero}_202610.csv", ENLACE_CORTO)
        for numero in range(1, 7)
    ]
    return {
        "total": tabla(columnas, filas),
        "resumen": tabla(resumen_columnas, resumen_filas),
        "archivos": archivos,
        "avisos": [("info", AVISO_REGENERACION), ("warn", "Línea de aviso muy larga " + "con texto repetido " * 12)],
    }


GENERADORES = {
    "01-normal": datos_normales,
    "02-regenerado": datos_regenerado,
    "03-sin-datos": datos_sin_datos,
    "04-regenerado-sin-datos": datos_regenerado_sin_datos,
    "05-disponibilidad-parcial": datos_disponibilidad_parcial,
    "06-estres": datos_estres,
}


def construir(job, plantilla, nombre_caso, datos):
    nombre = nombre_caso
    job.load_text = lambda bucket, clave: plantilla
    variables = {"useCaseName": nombre, "periodRange": PERIODO, "certificarLink": "ignorado"}
    fragmentos = {
        "tableTotal": job.result_to_html(datos["total"]),
        "tableSummary": job.result_to_html(datos["resumen"]),
        "tableDetail": job.details_to_html(datos["archivos"]),
        "certificarLink": html.escape(ENLACE_CORTO),
        "nocertificarLink": html.escape(ENLACE_CORTO + "&accion=no"),
    }
    return job.build_html_body("bucket", "plantilla.html", variables, fragmentos, datos["avisos"])


def escribir_indice(generados):
    filas = []
    for caso, archivos in generados.items():
        enlaces = " &middot; ".join(f'<a href="{caso}/{archivo}">{etiqueta}</a>' for archivo, etiqueta in archivos)
        filas.append(f"<tr><td><b>{CASOS[caso]}</b><br><small>{caso}</small></td><td>{enlaces}</td></tr>")
    pagina = (
        "<!DOCTYPE html><html lang=\"es\"><head><meta charset=\"utf-8\"><title>Vistas previas de correo</title>"
        "<style>body{font-family:Arial,sans-serif;margin:24px;color:#1f2a27}td{padding:10px 16px;border-bottom:1px solid #d9e1de}"
        "a{color:#00875f}</style></head><body><h1>Vistas previas de correo NIIF 17</h1>"
        "<p>Generadas con <code>herramientas/generar_previews.py</code>. Datos inventados. Para fidelidad real, "
        "enviar una prueba a una direcci&oacute;n verificada y abrirla en Outlook, Gmail y m&oacute;vil.</p>"
        f"<table>{''.join(filas)}</table></body></html>"
    )
    (DIRECTORIO_PREVIEWS / "index.html").write_text(pagina, encoding="utf-8")


def main():
    job = cargar_job()
    generados = {}
    for caso, nombre in CASOS.items():
        plantilla = (DIRECTORIO_PLANTILLAS / f"{caso}.html").read_text(encoding="utf-8")
        carpeta = DIRECTORIO_PREVIEWS / caso
        carpeta.mkdir(parents=True, exist_ok=True)
        generados[caso] = []
        for variante, etiqueta in VARIANTES:
            nombre_mostrado = nombre
            datos = GENERADORES[variante]()
            if variante == "06-estres":
                nombre_mostrado = f"{nombre} - Régimen de Prima Media y Ahorro Individual con un nombre excesivamente largo"
            cuerpo = construir(job, plantilla, nombre_mostrado, datos)
            if cuerpo is None:
                raise SystemExit(f"La plantilla {caso} no admite los avisos de la variante {variante}")
            archivo = f"{variante}.html"
            (carpeta / archivo).write_text(cuerpo, encoding="utf-8")
            generados[caso].append((archivo, etiqueta))
            print(f"OK {caso}/{archivo} ({len(cuerpo.encode('utf-8')) / 1024:.1f} KB)")
    escribir_indice(generados)


if __name__ == "__main__":
    main()
