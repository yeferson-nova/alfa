import pytest


@pytest.fixture(autouse=True)
def credenciales_ficticias(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "clave-ficticia")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secreto-ficticio")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
