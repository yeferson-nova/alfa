import importlib.util
from pathlib import Path

import pytest

HERRAMIENTAS = Path(__file__).resolve().parents[3] / "htmlNew" / "herramientas"
PLANTILLAS = sorted((HERRAMIENTAS.parent / "newUx").glob("*.html"))
PREVIEWS = sorted(p for p in (HERRAMIENTAS.parent / "previews").rglob("*.html") if p.name != "index.html")


def _cargar_validador():
    especificacion = importlib.util.spec_from_file_location("validar_plantillas", HERRAMIENTAS / "validar_plantillas.py")
    modulo = importlib.util.module_from_spec(especificacion)
    especificacion.loader.exec_module(modulo)
    return modulo


@pytest.mark.parametrize("archivo", PLANTILLAS, ids=lambda ruta: ruta.name)
def test_las_plantillas_cumplen_el_estandar_de_html_para_correo(archivo):
    hallazgos, _ = _cargar_validador().validar(archivo, "newUx")

    assert hallazgos.errores == []


@pytest.mark.parametrize("archivo", PREVIEWS, ids=lambda ruta: f"{ruta.parent.name}/{ruta.name}")
def test_los_correos_de_ejemplo_cumplen_el_estandar_de_html_para_correo(archivo):
    hallazgos, _ = _cargar_validador().validar(archivo, "previews")

    assert hallazgos.errores == []
