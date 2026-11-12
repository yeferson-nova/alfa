import ast
import io
import re
import tokenize
from pathlib import Path

import pytest

PROJECT = Path(__file__).resolve().parents[1]
SOURCES = sorted((PROJECT / "src").rglob("*.py"))
TESTS = sorted((PROJECT / "test").glob("*.py"))
SCRIPTS = sorted((PROJECT / "scripts").glob("*.py"))
ALL_FILES = SOURCES + TESTS + SCRIPTS
MAX_FUNCTION_LINES = 40
MAX_PARAMETERS = 8

HARDCODED_PATTERNS = {
    "cuenta de AWS": re.compile(r"(?<![0-9])[0-9]{12}(?![0-9])"),
    "ARN": re.compile(r"arn:aws"),
    "region": re.compile(r"\b[a-z]{2}-(?:east|west|south|north|central)-[0-9]\b"),
    "recurso del proyecto": re.compile(r"niif-(?:dev|qa|stg|prd)-"),
    "dominio de AWS": re.compile(r"amazonaws\.com"),
    "email": re.compile(r"[\w.+-]+@[\w-]+\.[\w.]+"),
}


def _name(path: Path) -> str:
    return str(path.relative_to(PROJECT)).replace("\\", "/")


@pytest.mark.parametrize("path", ALL_FILES, ids=_name)
def test_code_has_no_comments(path):
    text = path.read_text(encoding="utf-8")
    comments = [
        token.string
        for token in tokenize.generate_tokens(io.StringIO(text).readline)
        if token.type == tokenize.COMMENT
    ]
    assert comments == []


@pytest.mark.parametrize("path", ALL_FILES, ids=_name)
def test_code_has_no_docstrings(path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    nodes = [tree] + [
        node for node in ast.walk(tree) if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
    ]
    assert [node for node in nodes if ast.get_docstring(node) is not None] == []


@pytest.mark.parametrize("path", SOURCES, ids=_name)
def test_source_has_no_hardcoded_infrastructure_values(path):
    text = path.read_text(encoding="utf-8")
    found = [description for description, pattern in HARDCODED_PATTERNS.items() if pattern.search(text)]
    assert found == []


@pytest.mark.parametrize("path", SOURCES, ids=_name)
def test_functions_are_short_with_few_parameters(path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    functions = [node for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))]
    long_functions = [f.name for f in functions if f.end_lineno - f.lineno + 1 > MAX_FUNCTION_LINES]
    too_many_parameters = [
        f.name
        for f in functions
        if len(f.args.args) + len(f.args.kwonlyargs) - (1 if f.args.args and f.args.args[0].arg == "self" else 0)
        > MAX_PARAMETERS
    ]
    assert long_functions == []
    assert too_many_parameters == []


@pytest.mark.parametrize("path", SOURCES, ids=_name)
def test_code_has_no_prints_or_silenced_exceptions(path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    silenced = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.ExceptHandler) and all(isinstance(email_body, ast.Pass) for email_body in node.body)
    ]
    assert silenced == []


def test_project_includes_required_quality_files():
    for name in ("pyproject.toml", "sonar-project.properties", "requirements-dev.txt", "README.md"):
        assert (PROJECT / name).is_file(), name
