import os
import pytest
from unittest.mock import patch, MagicMock

# Variables de entorno para tests
os.environ["GLUE_JOB_NAME"] = "niif-qa-notification-certification"
os.environ["SNS_TOPIC_ARN"] = "arn:aws:sns:us-east-1:533267205627:niif-qa-certification-email"
os.environ["REDIRECT_BASE_URL"] = "https://d5qmm9fi64.execute-api.us-east-1.amazonaws.com/qa/r"
os.environ["SHORT_LINKS_TABLE"] = "niif-qa-short-links"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
os.environ["AWS_REGION"] = "us-east-1"

# Mock boto3.client antes de importar el módulo
with patch("boto3.client") as mock_boto_client:
    mock_boto_client.return_value = MagicMock()
    from notification_certification import lambda_handler
    import notification_certification


@pytest.fixture
def mock_glue():
    """Mock del cliente glue usado en el módulo."""
    mock = MagicMock()
    mock.start_job_run.return_value = {"JobRunId": "jr_test123"}
    with patch.object(notification_certification, "glue", mock):
        yield mock


def test_lambda_handler_success(mock_glue):
    """Test caso exitoso: evento válido dispara Glue."""
    event = {
        "Records": [
            {
                "eventName": "MODIFY",
                "dynamodb": {
                    "NewImage": {
                        "status": {"S": "PENDING"},
                            "useCaseId": {"S": "UC-001"},
                            "certCode": {"S": "CERT-99"},
                            "emailType": {"S": "Welcome"},
                            "queryName": {"S": "detail#1"},
                            "configBucket": {"S": "test-configBucket-value"},
                            "configKey": {"S": "test-configKey-value"},
                            "globalConfigKey": {"S": "test-globalConfigKey-value"}
                    }
                }
            }
        ]
    }

    lambda_handler(event, None)

    mock_glue.start_job_run.assert_called_once_with(
        JobName=os.environ["GLUE_JOB_NAME"],
        Arguments={
            "--useCaseId": "UC-001",
            "--certCode": "CERT-99",
            "--emailType": "Welcome",
            "--CONFIG_BUCKET": "test-configBucket-value",
            "--CONFIG_KEY": "test-configKey-value",
            "--GLOBAL_CONFIG_KEY": "test-globalConfigKey-value",
            "--SNS_TOPIC_ARN": os.environ["SNS_TOPIC_ARN"],
            "--REDIRECT_BASE_URL": os.environ["REDIRECT_BASE_URL"],
            "--SHORT_LINKS_TABLE": os.environ["SHORT_LINKS_TABLE"],
            "--REGENERACION_SIN_DATOS_NUEVOS": "false",
        }
    )


def test_lambda_handler_regeneracion_sin_datos_nuevos(mock_glue):
    """Test: cuando regeneracionSinDatosNuevos=True el argumento va en 'true'."""
    event = {
        "Records": [
            {
                "eventName": "MODIFY",
                "dynamodb": {
                    "NewImage": {
                        "status": {"S": "PENDING"},
                        "useCaseId": {"S": "UC-001"},
                        "certCode": {"S": "CERT-99"},
                        "emailType": {"S": "Welcome"},
                        "queryName": {"S": "detail#1"},
                        "configBucket": {"S": "test-configBucket-value"},
                        "configKey": {"S": "test-configKey-value"},
                        "globalConfigKey": {"S": "test-globalConfigKey-value"},
                        "regeneracionSinDatosNuevos": {"BOOL": True},
                    }
                }
            }
        ]
    }

    lambda_handler(event, None)

    args = mock_glue.start_job_run.call_args.kwargs["Arguments"]
    assert args["--REGENERACION_SIN_DATOS_NUEVOS"] == "true"


def test_lambda_handler_ignore_not_pending(mock_glue):
    """Test: ignora eventos con status diferente a PENDING."""
    event = {
        "Records": [
            {
                "eventName": "MODIFY",
                "dynamodb": {
                    "NewImage": {
                        "status": {"S": "COMPLETED"}
                    }
                }
            }
        ]
    }

    lambda_handler(event, None)
    mock_glue.start_job_run.assert_not_called()


def test_lambda_handler_with_sendlock(mock_glue):
    """Test: ignora eventos con sendLock presente."""
    event = {
        "Records": [
            {
                "eventName": "MODIFY",
                "dynamodb": {
                    "NewImage": {
                        "status": {"S": "PENDING"},
                        "sendLock": {"S": "active"}
                    }
                }
            }
        ]
    }

    lambda_handler(event, None)
    mock_glue.start_job_run.assert_not_called()


def test_lambda_handler_ignore_wrong_query_name(mock_glue):
    """Test: ignora queryName diferente al esperado."""
    event = {
        "Records": [
            {
                "eventName": "MODIFY",
                "dynamodb": {
                    "NewImage": {
                        "status": {"S": "PENDING"},
                        "useCaseId": {"S": "UC-001"},
                        "certCode": {"S": "CERT-99"},
                        "emailType": {"S": "Welcome"},
                        "queryName": {"S": "other_query"}
                    }
                }
            }
        ]
    }

    lambda_handler(event, None)
    mock_glue.start_job_run.assert_not_called()


def test_lambda_handler_ignore_missing_config(mock_glue):
    """Test: ignora sin campos de configuración requeridos."""
    event = {
        "Records": [
            {
                "eventName": "MODIFY",
                "dynamodb": {
                    "NewImage": {
                        "status": {"S": "PENDING"},
                        "useCaseId": {"S": "UC-001"},
                        "certCode": {"S": "CERT-99"},
                        "emailType": {"S": "Welcome"},
                        "queryName": {"S": "detail#1"}
                    }
                }
            }
        ]
    }

    lambda_handler(event, None)
    mock_glue.start_job_run.assert_not_called()


def test_lambda_handler_ignore_non_modify(mock_glue):
    """Test: ignora eventos que no son MODIFY."""
    event = {
        "Records": [
            {
                "eventName": "INSERT",
                "dynamodb": {
                    "NewImage": {
                        "status": {"S": "PENDING"}
                    }
                }
            }
        ]
    }

    lambda_handler(event, None)
    mock_glue.start_job_run.assert_not_called()
