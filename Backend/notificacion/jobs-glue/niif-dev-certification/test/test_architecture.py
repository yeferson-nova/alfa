import ast
from pathlib import Path

import pytest

PACKAGE = "certification_job"
ROOT = Path(__file__).resolve().parents[1] / "src" / PACKAGE
AWS_LIBRARIES = ("boto3", "botocore")

FORBIDDEN = {
    "domain": AWS_LIBRARIES
    + (f"{PACKAGE}.application", f"{PACKAGE}.adapters", f"{PACKAGE}.config", "niif_observability"),
    "application": AWS_LIBRARIES + (f"{PACKAGE}.adapters", f"{PACKAGE}.config", "niif_observability"),
    "adapters/inbound": AWS_LIBRARIES + (f"{PACKAGE}.adapters.outbound",),
    "adapters/outbound": (f"{PACKAGE}.config", f"{PACKAGE}.adapters.inbound"),
}


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
    return modules


def _violates(module: str, forbidden: str) -> bool:
    return module == forbidden or module.startswith(forbidden + ".")


@pytest.mark.parametrize("layer", sorted(FORBIDDEN))
def test_each_layer_respects_the_dependency_rule(layer):
    files = sorted((ROOT / layer).glob("*.py"))
    assert files
    violations = [
        f"{path.name} imports {module}"
        for path in files
        for module in _imports(path)
        for forbidden in FORBIDDEN[layer]
        if _violates(module, forbidden)
    ]
    assert violations == []


def test_only_outbound_adapters_and_config_use_aws_libraries():
    folders = {
        "/".join(path.relative_to(ROOT).parts[:-1])
        for path in ROOT.rglob("*.py")
        if any(_violates(module, library) for module in _imports(path) for library in AWS_LIBRARIES)
    }
    assert folders <= {"adapters/outbound", "config"}


def test_entry_script_only_imports_the_inbound_adapter():
    entry = ROOT.parent / "niif_certification.py"
    assert _imports(entry) == {f"{PACKAGE}.adapters.inbound.glue_job"}
