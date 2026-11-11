import importlib
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

DIRECTORIO_JOB = Path(__file__).resolve().parents[1]
DIRECTORIO_PLANTILLAS = Path(__file__).resolve().parents[3] / "notificacion" / "email-template"

ARGUMENTOS = {
    "useCaseId": "casoA",
    "certCode": "CERT1",
    "emailType": "certification",
    "CONFIG_BUCKET": "bucket-config",
    "CONFIG_KEY": "casos/casoA.json",
    "GLOBAL_CONFIG_KEY": "global.json",
    "SES_SENDER": "remitente@example.com",
    "REDIRECT_BASE_URL": "https://redir.example/r",
    "SHORT_LINKS_TABLE": "tabla-enlaces",
}


def _obtener_opciones_resueltas(argv, nombres):
    resueltas = {}
    for nombre in nombres:
        marca = f"--{nombre}"
        if marca not in argv:
            raise ValueError(f"Falta el argumento {nombre}")
        resueltas[nombre] = argv[argv.index(marca) + 1]
    return resueltas


def _instalar_awsglue_falso():
    paquete = types.ModuleType("awsglue")
    utilidades = types.ModuleType("awsglue.utils")
    utilidades.getResolvedOptions = _obtener_opciones_resueltas
    paquete.utils = utilidades
    sys.modules["awsglue"] = paquete
    sys.modules["awsglue.utils"] = utilidades


def _argv():
    argv = ["job.py"]
    for nombre, valor in ARGUMENTOS.items():
        argv += [f"--{nombre}", valor]
    return argv


@pytest.fixture
def job():
    _instalar_awsglue_falso()
    sys.modules.pop("notification_certification", None)
    sys.path.insert(0, str(DIRECTORIO_JOB))
    with patch.object(sys, "argv", _argv()), patch("boto3.client"), patch("boto3.resource"):
        modulo = importlib.import_module("notification_certification")
    modulo.sesv2 = MagicMock()
    modulo.s3 = MagicMock()
    modulo.athena = MagicMock()
    modulo.dynamodb = MagicMock()
    yield modulo
    sys.path.remove(str(DIRECTORIO_JOB))
    sys.modules.pop("notification_certification", None)
