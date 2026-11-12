import ast
from pathlib import Path

import pytest

PACKAGE = "certification_notifier"
ROOT = Path(__file__).resolve().parents[1] / "src" / PACKAGE
AWS_PACKAGES = ("boto3", "botocore")

FORBIDDEN = {
    "domain": AWS_PACKAGES + (f"{PACKAGE}.aplicacion", f"{PACKAGE}.adaptadores", f"{PACKAGE}.configuracion"),
    "application": AWS_PACKAGES + (f"{PACKAGE}.adaptadores", f"{PACKAGE}.configuracion"),
    "adapters/inbound": AWS_PACKAGES + (f"{PACKAGE}.adaptadores.salida",),
    "adapters/outbound": (f"{PACKAGE}.configuracion", f"{PACKAGE}.adaptadores.entrada"),
}


def _imported_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    module_names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            module_names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            module_names.add(node.module)
    return module_names


def _violates(module_name: str, forbidden: str) -> bool:
    return module_name == forbidden or module_name.startswith(forbidden + ".")


@pytest.mark.parametrize("layer", sorted(FORBIDDEN))
def test_each_layer_respects_the_dependency_rule(layer):
    files = sorted((ROOT / layer).glob("*.py"))
    assert files
    violations = [
        f"{source_file.name} importa {module_name}"
        for source_file in files
        for module_name in _imported_modules(source_file)
        for forbidden in FORBIDDEN[layer]
        if _violates(module_name, forbidden)
    ]
    assert violations == []


def test_only_config_and_outbound_adapters_use_boto3():
    boto3_folders = {
        "/".join(source_file.relative_to(ROOT).parts[:-1])
        for source_file in ROOT.rglob("*.py")
        if any(_violates(module_name, external) for module_name in _imported_modules(source_file) for external in AWS_PACKAGES)
    }
    assert boto3_folders <= {"adapters/outbound", "config"}
