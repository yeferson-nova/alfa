"""Hace cumplir la regla de dependencias de la arquitectura hexagonal.

dominio      → no importa ninguna otra capa ni AWS
aplicacion   → solo dominio
adaptadores  → aplicacion y dominio (entrada no importa salida)
configuracion→ puede importar todo (es la raíz de composición)
"""
import ast
from pathlib import Path

import pytest

PAQUETE = Path(__file__).resolve().parents[1] / "src" / "configurador_mallas"
CAPAS = ("dominio", "aplicacion", "adaptadores", "configuracion")
PROHIBIDO = {
    "dominio": {"aplicacion", "adaptadores", "configuracion", "boto3", "botocore"},
    "aplicacion": {"adaptadores", "configuracion", "boto3", "botocore"},
    "adaptadores/entrada": {"adaptadores/salida", "boto3", "botocore"},
    "adaptadores/salida": {"adaptadores/entrada", "configuracion"},
}


def _destinos(archivo):
    """Capas y librerías que importa un módulo (resolviendo imports relativos)."""
    relativo = archivo.relative_to(PAQUETE).with_suffix("")
    partes_modulo = list(relativo.parts[:-1])
    for nodo in ast.walk(ast.parse(archivo.read_text(encoding="utf-8"))):
        if isinstance(nodo, ast.Import):
            for alias in nodo.names:
                yield alias.name.split(".")[0]
        elif isinstance(nodo, ast.ImportFrom):
            if nodo.level == 0:
                modulo = (nodo.module or "").split(".")
                if modulo[0] == "configurador_mallas":
                    yield "/".join(modulo[1:3])
                else:
                    yield modulo[0]
                continue
            base = partes_modulo[: len(partes_modulo) - (nodo.level - 1)]
            destino = base + (nodo.module.split(".") if nodo.module else [])
            nombres = [] if nodo.module else [a.name for a in nodo.names]
            for ruta in ([destino] if nodo.module else [destino + [n] for n in nombres]):
                yield "/".join(ruta[:2])


def _capa(archivo):
    partes = archivo.relative_to(PAQUETE).parts
    return "/".join(partes[:2]) if partes[0] == "adaptadores" and len(partes) > 2 else partes[0]


ARCHIVOS = [a for a in PAQUETE.rglob("*.py") if a.relative_to(PAQUETE).parts[0] in CAPAS]


@pytest.mark.parametrize("archivo", ARCHIVOS, ids=lambda a: str(a.relative_to(PAQUETE)))
def test_respeta_la_regla_de_dependencias(archivo):
    capa = _capa(archivo)
    prohibidos = PROHIBIDO.get(capa, set())
    violaciones = sorted({d for d in _destinos(archivo)
                          if any(d == p or d.startswith(p + "/") or d.split("/")[0] == p
                                 for p in prohibidos)})
    assert not violaciones, f"{capa} no debe importar {violaciones}"


def test_todas_las_capas_existen():
    assert all((PAQUETE / capa).is_dir() for capa in CAPAS)
    assert (PAQUETE / "adaptadores" / "entrada").is_dir()
    assert (PAQUETE / "adaptadores" / "salida").is_dir()
