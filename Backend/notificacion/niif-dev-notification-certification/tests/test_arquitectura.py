import ast
from pathlib import Path

import pytest

PAQUETE = "notificacion_certificacion"
RAIZ = Path(__file__).resolve().parents[1] / "src" / PAQUETE
EXTERNOS_AWS = ("boto3", "botocore")

PROHIBIDOS = {
    "dominio": EXTERNOS_AWS + (f"{PAQUETE}.aplicacion", f"{PAQUETE}.adaptadores", f"{PAQUETE}.configuracion"),
    "aplicacion": EXTERNOS_AWS + (f"{PAQUETE}.adaptadores", f"{PAQUETE}.configuracion"),
    "adaptadores/entrada": EXTERNOS_AWS + (f"{PAQUETE}.adaptadores.salida",),
    "adaptadores/salida": (f"{PAQUETE}.configuracion", f"{PAQUETE}.adaptadores.entrada"),
}


def _modulos_importados(ruta: Path) -> set[str]:
    arbol = ast.parse(ruta.read_text(encoding="utf-8"))
    modulos: set[str] = set()
    for nodo in ast.walk(arbol):
        if isinstance(nodo, ast.Import):
            modulos.update(alias.name for alias in nodo.names)
        elif isinstance(nodo, ast.ImportFrom) and nodo.module:
            modulos.add(nodo.module)
    return modulos


def _viola(modulo: str, prohibido: str) -> bool:
    return modulo == prohibido or modulo.startswith(prohibido + ".")


@pytest.mark.parametrize("capa", sorted(PROHIBIDOS))
def test_cada_capa_respeta_la_regla_de_dependencias(capa):
    archivos = sorted((RAIZ / capa).glob("*.py"))
    assert archivos
    violaciones = [
        f"{archivo.name} importa {modulo}"
        for archivo in archivos
        for modulo in _modulos_importados(archivo)
        for prohibido in PROHIBIDOS[capa]
        if _viola(modulo, prohibido)
    ]
    assert violaciones == []


def test_solo_la_configuracion_y_los_adaptadores_de_salida_usan_boto3():
    carpetas_con_boto3 = {
        "/".join(archivo.relative_to(RAIZ).parts[:-1])
        for archivo in RAIZ.rglob("*.py")
        if any(_viola(modulo, externo) for modulo in _modulos_importados(archivo) for externo in EXTERNOS_AWS)
    }
    assert carpetas_con_boto3 <= {"adaptadores/salida", "configuracion"}
