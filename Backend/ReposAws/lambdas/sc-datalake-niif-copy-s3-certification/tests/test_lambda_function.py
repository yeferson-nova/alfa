import os
import pytest
from unittest.mock import patch, MagicMock
from copy_s3 import lambda_handler, niif_carpeta, niif_origen
from botocore.exceptions import ClientError

# ===================== ENV =====================

os.environ["REPORT_BUCKET"] = "bucket-reporte"
os.environ["TRANSPORT_BUCKET"] = "bucket-transporte"
os.environ["NOTIFICATIONS_TABLE"] = "tabla-notificaciones"
os.environ["AWS_REGION"] = "us-east-1"

# ===================== FIXTURES =====================

@pytest.fixture
def mock_aws():
    mock_s3 = MagicMock()
    mock_dynamodb = MagicMock()

    with patch("copy_s3.get_s3") as mock_get_s3, \
         patch("copy_s3.get_dynamodb") as mock_get_dynamodb:
        mock_get_s3.return_value = mock_s3
        mock_get_dynamodb.return_value = mock_dynamodb
        yield mock_s3, mock_dynamodb


@pytest.fixture
def stream_event():
    # useCaseId debe existir en NIIF_CARPETAS (copy_s3.niif_carpeta); de lo
    # contrario niif_origen lanza ValueError antes de copiar.
    return {
        "Records": [
            {
                "eventName": "MODIFY",
                "dynamodb": {
                    "OldImage": {"status": {"S": "PENDING"}},
                    "NewImage": {
                        "status": {"S": "CERTIFICATE"},
                        "useCaseId": {"S": "Previsional"},
                        "certCode": {"S": "CERT-123"}
                    }
                }
            }
        ]
    }

# ===================== TESTS =====================

def test_lambda_handler_success(mock_aws, stream_event):
    mock_s3, mock_dyn = mock_aws

    mock_table = MagicMock()
    mock_table.query.return_value = {
        "Items": [
            {"executionId": "exec-1", "useCaseId": "Previsional", "nameFile": "archivo.pdf"}
        ]
    }

    mock_dyn.Table.return_value = mock_table

    # Origen resuelto al primer candidato (head_object no lanza error).
    mock_s3.head_object.return_value = {}

    response = lambda_handler(stream_event, None)

    assert response["status"] == "success"
    mock_s3.copy_object.assert_called_once()
    mock_table.update_item.assert_called_once()


def test_lambda_handler_skip_invalid_event(mock_aws):
    mock_s3, mock_dyn = mock_aws

    event = {
        "Records": [
            {"eventName": "INSERT", "dynamodb": {"NewImage": {"status": {"S": "CERTIFICATE"}}}}
        ]
    }

    lambda_handler(event, None)

    mock_dyn.Table.assert_not_called()
    mock_s3.copy_object.assert_not_called()


def test_lambda_handler_skip_already_certified(mock_aws):
    mock_s3, mock_dyn = mock_aws

    event = {
        "Records": [
            {
                "eventName": "MODIFY",
                "dynamodb": {
                    "OldImage": {"status": {"S": "CERTIFICATE"}},
                    "NewImage": {"status": {"S": "CERTIFICATE"}}
                }
            }
        ]
    }

    lambda_handler(event, None)
    mock_s3.copy_object.assert_not_called()


def test_lambda_handler_missing_file_name(mock_aws, stream_event):
    mock_s3, mock_dyn = mock_aws

    mock_table = MagicMock()
    mock_table.query.return_value = {
        "Items": [{"executionId": "exec-x", "useCaseId": "UC-001"}]
    }

    mock_dyn.Table.return_value = mock_table

    lambda_handler(stream_event, None)
    mock_s3.copy_object.assert_not_called()


def test_lambda_handler_s3_error(mock_aws, stream_event):
    mock_s3, mock_dyn = mock_aws

    mock_table = MagicMock()
    mock_table.query.return_value = {
        "Items": [{"executionId": "exec", "useCaseId": "Previsional", "nameFile": "f.txt"}]
    }

    mock_dyn.Table.return_value = mock_table
    mock_s3.head_object.return_value = {}
    # El fallo en copy_object es capturado por el handler; la ejecucion no cae.
    mock_s3.copy_object.side_effect = Exception("S3 error")

    response = lambda_handler(stream_event, None)
    assert response["status"] == "success"
    mock_table.update_item.assert_not_called()


# ===================== UNIT: niif_carpeta / niif_origen =====================

def test_niif_carpeta_alias_se_normaliza():
    assert niif_carpeta("auxilioFunerario") == "auxiliosFunerario"
    assert niif_carpeta("reservaMatematicaRvi") == "reservaMatematicaRviArp"


def test_niif_carpeta_invalida_lanza_valueerror():
    with pytest.raises(ValueError):
        niif_carpeta("UC-INEXISTENTE")


def test_niif_origen_usa_ruta_oficial():
    mock_s3 = MagicMock()
    mock_s3.head_object.return_value = {}

    key = niif_origen(mock_s3, "bucket", "Previsional", "archivo.json")

    assert key == "Previsional/archivo.json"
    mock_s3.head_object.assert_called_once()


def test_niif_origen_cae_a_ruta_historica():
    # Alias (auxilioFunerario -> auxiliosFunerario): oficial 404, historica existe.
    mock_s3 = MagicMock()
    mock_s3.head_object.side_effect = [
        ClientError(
            {"Error": {"Code": "404"}, "ResponseMetadata": {"HTTPStatusCode": 404}},
            "HeadObject",
        ),
        {},
    ]

    key = niif_origen(mock_s3, "bucket", "auxilioFunerario", "archivo.json")

    assert key == "auxilioFunerario/archivo.json"
    assert mock_s3.head_object.call_count == 2


def test_niif_origen_error_no_404_se_propaga():
    mock_s3 = MagicMock()
    mock_s3.head_object.side_effect = ClientError(
        {"Error": {"Code": "403"}, "ResponseMetadata": {"HTTPStatusCode": 403}},
        "HeadObject",
    )

    with pytest.raises(ClientError):
        niif_origen(mock_s3, "bucket", "primasArl", "archivo.json")