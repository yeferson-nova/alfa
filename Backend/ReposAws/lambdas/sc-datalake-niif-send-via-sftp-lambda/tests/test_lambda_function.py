"""
Pruebas unitarias para src/lambda_function.py

El modulo lee variables de entorno al importarse (HOST, PORT, SECRET_ID, ...),
por lo que se definen ANTES de importar el modulo. Secrets Manager y S3 se
mockean con moto, y la capa SFTP (paramiko / socket) con mocks.
"""

import io
import os
import json
import importlib

import boto3
import pytest
from moto import mock_secretsmanager, mock_s3

REGION = "us-east-1"
SECRET_ID = "test/sftp/creds"
BUCKET = "test-bucket"


# ---------------------------------------------------------------------------
# Entorno requerido por el modulo (se setea antes de importarlo)
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("SFTP_HOST", "sftp.example.com")
    monkeypatch.setenv("SFTP_PORT", "22")
    monkeypatch.setenv("SFTP_BASE_DIR", "/upload")
    monkeypatch.setenv("CREDS_SECRET_ID", SECRET_ID)
    monkeypatch.setenv("ENABLE_SSH_RSA_FALLBACK", "true")


def _load_module():
    """Importa (o recarga) el modulo con el entorno ya definido."""
    import lambda_function
    return importlib.reload(lambda_function)


def _seed_secret():
    client = boto3.client("secretsmanager", region_name=REGION)
    client.create_secret(
        Name=SECRET_ID,
        SecretString=json.dumps({"username": "user", "password": "pass"}),
    )


def _seed_s3_object(key, content=b"hello world"):
    s3 = boto3.client("s3", region_name=REGION)
    s3.create_bucket(Bucket=BUCKET)
    s3.put_object(Bucket=BUCKET, Key=key, Body=content)


def _s3_event(key):
    return {
        "Records": [
            {
                "eventSource": "aws:s3",
                "s3": {
                    "bucket": {"name": BUCKET},
                    "object": {"key": key},
                },
            }
        ]
    }


class FakeSFTP:
    """SFTP falso que registra directorios creados y archivos subidos."""

    def __init__(self):
        self.dirs = set()
        self.uploaded = {}

    def listdir(self, path):
        if path not in self.dirs:
            raise IOError("no existe")
        return []

    def mkdir(self, path):
        self.dirs.add(path)

    def putfo(self, fo, remote_path, callback=None):
        data = fo.read()
        self.uploaded[remote_path] = data
        if callback:
            callback(len(data), len(data))

    def stat(self, remote_path):
        class _St:
            st_size = len(self.uploaded.get(remote_path, b""))
        return _St()

    def close(self):
        pass


class FakeClient:
    def close(self):
        pass


@pytest.fixture
def patch_sftp(monkeypatch):
    """Reemplaza _connect_sftp para no abrir conexiones reales."""
    fake_sftp = FakeSFTP()
    fake_client = FakeClient()

    def _fake_connect(host, port, username, password):
        return fake_client, fake_sftp

    return fake_sftp, _fake_connect


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
@mock_secretsmanager
@mock_s3
def test_lambda_handler_sube_archivo_ok(monkeypatch, patch_sftp):
    _seed_secret()
    _seed_s3_object("export/OUT/archivo.csv", b"col1,col2\n1,2\n")

    mod = _load_module()
    fake_sftp, fake_connect = patch_sftp
    monkeypatch.setattr(mod, "_connect_sftp", fake_connect)

    result = mod.lambda_handler(_s3_event("export/OUT/archivo.csv"), None)

    assert result["status"] == "ok"
    assert result["successes"] == 1
    assert result["failures"] == 0
    # Se descarta el prefijo "export" y se conserva la jerarquia bajo BASE_DIR
    assert "/upload/OUT/archivo.csv" in fake_sftp.uploaded
    assert fake_sftp.uploaded["/upload/OUT/archivo.csv"] == b"col1,col2\n1,2\n"


@mock_secretsmanager
@mock_s3
def test_lambda_handler_mapea_caso_de_uso(monkeypatch, patch_sftp):
    """export/<casoDeUso>/archivo -> BASE_DIR/<casoDeUso>/archivo"""
    _seed_secret()
    _seed_s3_object("export/primasArl/detalle.csv", b"data")

    mod = _load_module()
    fake_sftp, fake_connect = patch_sftp
    monkeypatch.setattr(mod, "_connect_sftp", fake_connect)

    result = mod.lambda_handler(_s3_event("export/primasArl/detalle.csv"), None)

    assert result["status"] == "ok"
    assert result["successes"] == 1
    assert "/upload/primasArl/detalle.csv" in fake_sftp.uploaded


@mock_secretsmanager
@mock_s3
def test_lambda_handler_mapea_caso_de_uso_con_subcarpetas(monkeypatch, patch_sftp):
    """Conserva toda la jerarquia intermedia bajo el caso de uso."""
    _seed_secret()
    _seed_s3_object("export/reservaMatematica/2026/05/val.csv", b"data")

    mod = _load_module()
    fake_sftp, fake_connect = patch_sftp
    monkeypatch.setattr(mod, "_connect_sftp", fake_connect)

    result = mod.lambda_handler(_s3_event("export/reservaMatematica/2026/05/val.csv"), None)

    assert result["status"] == "ok"
    assert result["successes"] == 1
    assert "/upload/reservaMatematica/2026/05/val.csv" in fake_sftp.uploaded


@mock_secretsmanager
@mock_s3
def test_lambda_handler_ignora_evento_de_carpeta(monkeypatch, patch_sftp):
    """Un objeto cuyo key termina en '/' (carpeta) se ignora, no se sube nada."""
    _seed_secret()
    _seed_s3_object("export/", b"")

    mod = _load_module()
    fake_sftp, fake_connect = patch_sftp
    monkeypatch.setattr(mod, "_connect_sftp", fake_connect)

    result = mod.lambda_handler(_s3_event("export/"), None)

    assert result["status"] == "ok"
    assert result["successes"] == 0
    assert result["failures"] == 0
    assert fake_sftp.uploaded == {}


@mock_secretsmanager
@mock_s3
def test_lambda_handler_sin_records(monkeypatch, patch_sftp):
    _seed_secret()

    mod = _load_module()
    fake_sftp, fake_connect = patch_sftp
    monkeypatch.setattr(mod, "_connect_sftp", fake_connect)

    result = mod.lambda_handler({"Records": []}, None)

    assert result == {"status": "ok", "processed": 0}


@mock_secretsmanager
@mock_s3
def test_lambda_handler_ignora_record_no_s3(monkeypatch, patch_sftp):
    _seed_secret()

    mod = _load_module()
    fake_sftp, fake_connect = patch_sftp
    monkeypatch.setattr(mod, "_connect_sftp", fake_connect)

    event = {"Records": [{"eventSource": "aws:sqs"}]}
    result = mod.lambda_handler(event, None)

    assert result["status"] == "ok"
    assert result["successes"] == 0
    assert result["failures"] == 0
    assert fake_sftp.uploaded == {}


@mock_secretsmanager
@mock_s3
def test_lambda_handler_error_en_secreto(monkeypatch, patch_sftp):
    # No se crea el secreto -> get_secret_value falla
    mod = _load_module()
    fake_sftp, fake_connect = patch_sftp
    monkeypatch.setattr(mod, "_connect_sftp", fake_connect)

    result = mod.lambda_handler(_s3_event("a/b/c.csv"), None)

    assert result["status"] == "error"
    assert result["stage"] == "secrets"


@mock_secretsmanager
@mock_s3
def test_lambda_handler_error_en_conexion(monkeypatch):
    _seed_secret()

    mod = _load_module()

    def _boom(host, port, username, password):
        raise OSError("no reachability")

    monkeypatch.setattr(mod, "_connect_sftp", _boom)

    result = mod.lambda_handler(_s3_event("a/b/c.csv"), None)

    assert result["status"] == "error"
    assert result["stage"] == "connect"


@mock_secretsmanager
@mock_s3
def test_lambda_handler_key_sin_subdir(monkeypatch, patch_sftp):
    _seed_secret()
    _seed_s3_object("archivo_raiz.csv", b"abc")

    mod = _load_module()
    fake_sftp, fake_connect = patch_sftp
    monkeypatch.setattr(mod, "_connect_sftp", fake_connect)

    result = mod.lambda_handler(_s3_event("archivo_raiz.csv"), None)

    assert result["status"] == "ok"
    assert result["successes"] == 1
    assert "/upload/archivo_raiz.csv" in fake_sftp.uploaded


def test_mkdirs_sftp_crea_arbol(monkeypatch):
    mod = _load_module()
    fake_sftp = FakeSFTP()

    mod._mkdirs_sftp(fake_sftp, "/upload/a/b")

    assert "/upload" in fake_sftp.dirs
    assert "/upload/a" in fake_sftp.dirs
    assert "/upload/a/b" in fake_sftp.dirs


def test_mkdirs_sftp_raiz_no_hace_nada():
    mod = _load_module()
    fake_sftp = FakeSFTP()

    mod._mkdirs_sftp(fake_sftp, "/")

    assert fake_sftp.dirs == set()
