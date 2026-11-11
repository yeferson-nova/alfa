import ast
import io
import re
import tokenize
from pathlib import Path

import pytest

PROYECTO = Path(__file__).resolve().parents[1]
FUENTES = sorted((PROYECTO / "src").rglob("*.py"))
PRUEBAS = sorted((PROYECTO / "test").glob("*.py"))
TODOS = FUENTES + PRUEBAS
MAXIMO_LINEAS_FUNCION = 40
MAXIMO_PARAMETROS = 8

PATRONES_QUEMADOS = {
    "cuenta de AWS": re.compile(r"(?<![0-9])[0-9]{12}(?![0-9])"),
    "ARN": re.compile(r"arn:aws"),
    "region": re.compile(r"\b[a-z]{2}-(?:east|west|south|north|central)-[0-9]\b"),
    "recurso del proyecto": re.compile(r"niif-(?:dev|qa|stg|prd)-"),
    "dominio de AWS": re.compile(r"amazonaws\.com"),
    "correo": re.compile(r"[\w.+-]+@[\w-]+\.[\w.]+"),
}


def _nombre(ruta: Path) -> str:
    return str(ruta.relative_to(PROYECTO)).replace("\\", "/")


@pytest.mark.parametrize("ruta", TODOS, ids=_nombre)
def test_el_codigo_no_tiene_comentarios(ruta):
    texto = ruta.read_text(encoding="utf-8")
    comentarios = [
        token.string
        for token in tokenize.generate_tokens(io.StringIO(texto).readline)
        if token.type == tokenize.COMMENT
    ]
    assert comentarios == []


@pytest.mark.parametrize("ruta", TODOS, ids=_nombre)
def test_el_codigo_no_tiene_docstrings(ruta):
    arbol = ast.parse(ruta.read_text(encoding="utf-8"))
    nodos = [arbol] + [
        nodo for nodo in ast.walk(arbol) if isinstance(nodo, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
    ]
    assert [nodo for nodo in nodos if ast.get_docstring(nodo) is not None] == []


@pytest.mark.parametrize("ruta", FUENTES, ids=_nombre)
def test_el_codigo_fuente_no_tiene_valores_quemados_de_infraestructura(ruta):
    texto = ruta.read_text(encoding="utf-8")
    encontrados = [descripcion for descripcion, patron in PATRONES_QUEMADOS.items() if patron.search(texto)]
    assert encontrados == []


@pytest.mark.parametrize("ruta", FUENTES, ids=_nombre)
def test_las_funciones_son_cortas_y_con_pocos_parametros(ruta):
    arbol = ast.parse(ruta.read_text(encoding="utf-8"))
    funciones = [nodo for nodo in ast.walk(arbol) if isinstance(nodo, (ast.FunctionDef, ast.AsyncFunctionDef))]
    largas = [f.name for f in funciones if f.end_lineno - f.lineno + 1 > MAXIMO_LINEAS_FUNCION]
    con_muchos_parametros = [
        f.name
        for f in funciones
        if len(f.args.args) + len(f.args.kwonlyargs) - (1 if f.args.args and f.args.args[0].arg == "self" else 0)
        > MAXIMO_PARAMETROS
    ]
    assert largas == []
    assert con_muchos_parametros == []


@pytest.mark.parametrize("ruta", FUENTES, ids=_nombre)
def test_el_codigo_no_usa_prints_ni_excepciones_silenciadas(ruta):
    arbol = ast.parse(ruta.read_text(encoding="utf-8"))
    silenciadas = [
        nodo
        for nodo in ast.walk(arbol)
        if isinstance(nodo, ast.ExceptHandler) and all(isinstance(cuerpo, ast.Pass) for cuerpo in nodo.body)
    ]
    assert silenciadas == []


def test_el_proyecto_incluye_los_archivos_de_calidad_exigidos():
    for nombre in ("pyproject.toml", "sonar-project.properties", "requirements-dev.txt", "README.md", "tox.ini"):
        assert (PROYECTO / nombre).is_file(), nombre
