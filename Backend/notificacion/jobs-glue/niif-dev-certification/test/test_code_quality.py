import ast
import io
import re
import tokenize
from pathlib import Path

import pytest

PROJECT = Path(__file__).resolve().parents[1]
SOURCES = sorted((PROJECT / "src").rglob("*.py"))
TESTS = sorted((PROJECT / "test").glob("*.py"))
ALL_FILES = SOURCES + TESTS
MAX_FUNCTION_LINES = 40
MAX_PARAMETERS = 8
MAX_LINE_LENGTH = 120
SNAKE_CASE = re.compile(r"^_{0,2}[a-z][a-z0-9_]*$")
PASCAL_CASE = re.compile(r"^_?[A-Z][A-Za-z0-9]*$")

HARDCODED_PATTERNS = {
    "cuenta de AWS": re.compile(r"(?<![0-9])[0-9]{12}(?![0-9])"),
    "ARN": re.compile(r"arn:aws"),
    "region": re.compile(r"\b[a-z]{2}-(?:east|west|south|north|central)-[0-9]\b"),
    "recurso del proyecto": re.compile(r"niif-(?:dev|qa|stg|prd)-"),
    "dominio de AWS": re.compile(r"amazonaws\.com"),
    "correo": re.compile(r"[\w.+-]+@[\w-]+\.[\w.]+"),
    "portal SSO": re.compile(r"awsapps\.com"),
}


def _name(path: Path) -> str:
    return str(path.relative_to(PROJECT)).replace("\\", "/")


def _functions(path: Path) -> list[ast.FunctionDef]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return [node for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))]


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


@pytest.mark.parametrize("path", ALL_FILES, ids=_name)
def test_lines_are_at_most_120_characters(path):
    lines = path.read_text(encoding="utf-8").splitlines()
    long_lines = [number for number, line in enumerate(lines, 1) if len(line) > MAX_LINE_LENGTH]
    assert long_lines == []


@pytest.mark.parametrize("path", SOURCES, ids=_name)
def test_source_has_no_hardcoded_infrastructure(path):
    text = path.read_text(encoding="utf-8")
    assert [label for label, pattern in HARDCODED_PATTERNS.items() if pattern.search(text)] == []


@pytest.mark.parametrize("path", SOURCES, ids=_name)
def test_functions_are_short_with_few_parameters(path):
    functions = _functions(path)
    long_functions = [node.name for node in functions if node.end_lineno - node.lineno + 1 > MAX_FUNCTION_LINES]
    many_parameters = [
        node.name
        for node in functions
        if len([arg for arg in node.args.args if arg.arg != "self"]) + len(node.args.kwonlyargs) > MAX_PARAMETERS
    ]
    assert long_functions == []
    assert many_parameters == []


@pytest.mark.parametrize("path", SOURCES, ids=_name)
def test_names_follow_pep8(path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    bad_functions = [node.name for node in _functions(path) if not SNAKE_CASE.match(node.name)]
    classes = [node.name for node in ast.walk(tree) if isinstance(node, ast.ClassDef)]
    bad_classes = [name for name in classes if not PASCAL_CASE.match(name)]
    assert bad_functions == []
    assert bad_classes == []


@pytest.mark.parametrize("path", SOURCES, ids=_name)
def test_source_has_no_silenced_exceptions(path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    silenced = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.ExceptHandler) and all(isinstance(statement, ast.Pass) for statement in node.body)
    ]
    assert silenced == []


def test_project_has_the_required_quality_files():
    for name in ("pyproject.toml", "sonar-project.properties", "requirements-dev.txt", "README.md", "tox.ini"):
        assert (PROJECT / name).is_file(), name
