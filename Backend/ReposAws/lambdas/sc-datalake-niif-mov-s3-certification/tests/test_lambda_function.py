import os
import pytest
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError
from move_s3 import lambda_handler, niif_carpeta, niif_origen

# ===================== ENV =====================

os.environ["REPORT_BUCKET"] = "bucket-reportes"
os.environ["NOTIFICATIONS_TABLE"] = "tabla-notificaciones"
os.environ["AWS_REGION"] = "us-east-1"

# ===================== FIXTURES =====================

@pytest.fixture
def mock_aws():
    mock_s3 = MagicMock()
    mock_dynamodb = MagicMock()

    with patch("move_s3.get_s3") as mock_get_s3, \
         patch("move_s3.get_dynamodb") as mock_get_dynamo:
        mock_get_s3.return_value = mock_s3
        mock_get_dynamo.return_value = mock_dynamodb
        yield mock_s3, mock_dynamodb


@pytest.fixture
def stream_event_no_cert():
    # useCaseId debe existir en NIIF_CARPETAS (move_s3.niif_carpeta), de lo
    # contrario niif_origen lanza ValueError antes de tocar S3.
    return {
        "Records": [
            {
                "eventName": "MODIFY",
                "dynamodb": {
                    "OldImage": {"status": {"S": "PENDING"}},
                    "NewImage": {
                        "status": {"S": "NO_CERTIFICATE"},
                        "useCaseId": {"S": "Previsional"},
                        "certCode": {"S": "CERT-456"}
                    }
                }
            }
        ]
    }

# ===================== TESTS =====================

def test_lambda_handler_success_move(mock_aws, stream_event_no_cert):
    mock_s3, mock_dyn = mock_aws

    mock_table = MagicMock()
    mock_table.query.return_value = {
        "Items": [
            {"executionId": "exec-100", "useCaseId": "Previsional", "nameFile": "reporte.json"}
        ]
    }
    mock_dyn.Table.return_value = mock_table

    # Origen oficial "Previsional/reporte.json" existe; destino
    # "Previsional/NO-CERTIFICADOS/reporte.json" no existe (404) para forzar la copia.
    def head_object(Bucket, Key):
        if "NO-CERTIFICADOS" in Key:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {}

    mock_s3.head_object.side_effect = head_object

    response = lambda_handler(stream_event_no_cert, None)

    assert response["status"] == "success"
    mock_s3.copy_object.assert_called_once()
    mock_s3.delete_object.assert_called_once()
    mock_table.update_item.assert_called_once()


def test_lambda_handler_skip_if_destination_exists(mock_aws, stream_event_no_cert):
    mock_s3, mock_dyn = mock_aws
    mock_table = MagicMock()
    mock_table.query.return_value = {
        "Items": [{"executionId": "ex", "useCaseId": "Previsional", "nameFile": "f.json"}]
    }
    mock_dyn.Table.return_value = mock_table

    mock_s3.head_object.return_value = {}  # origen y destino existen

    lambda_handler(stream_event_no_cert, None)

    mock_s3.copy_object.assert_not_called()
    mock_s3.delete_object.assert_not_called()


def test_lambda_handler_origin_not_found(mock_aws, stream_event_no_cert):
    mock_s3, mock_dyn = mock_aws
    mock_table = MagicMock()
    mock_table.query.return_value = {
        "Items": [{"executionId": "ex", "useCaseId": "Previsional", "nameFile": "x.json"}]
    }
    mock_dyn.Table.return_value = mock_table

    # Ningun candidato de origen existe: niif_origen lanza FileNotFoundError,
    # capturado por el handler. No se copia ni se borra.
    mock_s3.head_object.side_effect = ClientError(
        {"Error": {"Code": "404"}}, "HeadObject"
    )

    lambda_handler(stream_event_no_cert, None)

    mock_s3.copy_object.assert_not_called()
    mock_s3.delete_object.assert_not_called()


def test_lambda_handler_ignore_wrong_status(mock_aws):
    event = {
        "Records": [
            {
                "eventName": "MODIFY",
                "dynamodb": {
                    "OldImage": {"status": {"S": "PENDING"}},
                    "NewImage": {"status": {"S": "CERTIFICATE"}}
                }
            }
        ]
    }

    lambda_handler(event, None)


def test_lambda_handler_missing_fields(mock_aws):
    event = {
        "Records": [
            {
                "eventName": "MODIFY",
                "dynamodb": {
                    "NewImage": {"status": {"S": "NO_CERTIFICATE"}}
                }
            }
        ]
    }

    response = lambda_handler(event, None)
    assert response["status"] == "success"


# ===================== UNIT: niif_carpeta =====================

def test_niif_carpeta_alias_se_normaliza():
    # Alias historico debe mapear a la carpeta oficial.
    assert niif_carpeta("auxilioFunerario") == "auxiliosFunerario"
    assert niif_carpeta("reservaMatematicaRvi") == "reservaMatematicaRviArp"


def test_niif_carpeta_invalida_lanza_valueerror():
    with pytest.raises(ValueError):
        niif_carpeta("UC-INEXISTENTE")


# ===================== UNIT: niif_origen =====================

def test_niif_origen_usa_ruta_oficial():
    # Si la ruta oficial existe, se usa y no se prueba la historica.
    mock_s3 = MagicMock()
    mock_s3.head_object.return_value = {}

    key = niif_origen(mock_s3, "bucket", "Previsional", "archivo.json")

    assert key == "Previsional/archivo.json"
    mock_s3.head_object.assert_called_once()


def test_niif_origen_cae_a_ruta_historica():
    # Con un alias (auxilioFunerario -> carpeta oficial auxiliosFunerario) los
    # dos candidatos son distintos: primero la oficial (404) y luego la historica.
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
    # Un error distinto de 404 (p.ej. AccessDenied) debe propagarse.
    mock_s3 = MagicMock()
    mock_s3.head_object.side_effect = ClientError(
        {"Error": {"Code": "403"}, "ResponseMetadata": {"HTTPStatusCode": 403}},
        "HeadObject",
    )

    with pytest.raises(ClientError):
        niif_origen(mock_s3, "bucket", "primasArl", "archivo.json")