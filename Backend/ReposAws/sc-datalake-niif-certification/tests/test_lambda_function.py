import os
import json
import pytest
from decimal import Decimal
from unittest.mock import patch, MagicMock, call

# ============================================================
# Variables de entorno antes de importar el módulo
# ============================================================
os.environ["NOTIFICATIONS_TABLE"] = "niif-qa-notification-certification"
os.environ["APPROVERS_LOG_TABLE"] = "niif-qa-approval-audit-log"
os.environ["CONFIG_BUCKET"] = "niif-qa-assets"
os.environ["GLOBAL_CONFIG_KEY"] = "configs/certification/json_global_notif_cert.json"
os.environ["CONFIG_KEY"] = "primas-arl/configs/certification/primasArl.json"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
os.environ["AWS_REGION"] = "us-east-1"

# Mock boto3 antes de importar para evitar llamadas reales a AWS
with patch("boto3.resource"), patch("boto3.client"):
    import certification
    from certification import lambda_handler


# ============================================================
# Fixtures base
# ============================================================

@pytest.fixture
def event_certify():
    """Evento API Gateway para ruta /certify con claims Cognito."""
    return {
        "requestContext": {
            "authorizer": {
                "claims": {
                    "email": "approver@example.com"
                }
            }
        },
        "path": "/certify",
        "body": json.dumps({
            "useCaseId": "UC-001",
            "certCode": "CERT-123"
        }),
        "isBase64Encoded": False
    }


@pytest.fixture
def event_not_certify():
    """Evento API Gateway para ruta /not_certify con claims Cognito."""
    return {
        "requestContext": {
            "authorizer": {
                "claims": {
                    "email": "approver@example.com"
                }
            }
        },
        "path": "/not_certify",
        "body": json.dumps({
            "useCaseId": "UC-001",
            "certCode": "CERT-123"
        }),
        "isBase64Encoded": False
    }


@pytest.fixture
def mock_aws(monkeypatch):
    """
    Mock centralizado de dynamodb (resource) y s3 (client) a nivel de módulo.
    Retorna un dict con las piezas mockeadas para configurar en cada test.
    """
    mock_dynamo = MagicMock()
    mock_s3 = MagicMock()
    monkeypatch.setattr(certification, "dynamodb", mock_dynamo)
    monkeypatch.setattr(certification, "s3", mock_s3)
    return {"dynamo": mock_dynamo, "s3": mock_s3}


def _make_s3_json_response(obj):
    """Helper: simula la respuesta de s3.get_object para un JSON."""
    body_mock = MagicMock()
    body_mock.read.return_value = json.dumps(obj).encode("utf-8")
    return {"Body": body_mock}


def _setup_s3_configs(mock_s3, approvers=None, required=1):
    """
    Configura el mock de S3 para devolver global_config y case_config.
    approvers: lista de emails habilitados (default: ["approver@example.com"])
    """
    if approvers is None:
        approvers = ["approver@example.com"]

    global_config = {
        "paths": {
            "use_cases_base": "configs/certification/use-cases"
        }
    }

    case_config = {
        "useCaseId": "UC-001",
        "approvers": {
            "users": approvers,
            "required": required
        }
    }

    # get_object se llama dos veces: primero global, luego case config
    mock_s3.get_object.side_effect = [
        _make_s3_json_response(global_config),
        _make_s3_json_response(case_config)
    ]


# ============================================================
# Tests: validaciones de entrada (sin AWS)
# ============================================================

def test_unauthenticated_user_returns_401():
    """Sin email en claims retorna 401."""
    event = {
        "requestContext": {"authorizer": {"claims": {}}},
        "path": "/certify",
        "body": json.dumps({"useCaseId": "UC-001", "certCode": "CERT-123"})
    }
    result = lambda_handler(event, None)
    assert result["statusCode"] == 401
    assert "no autenticado" in json.loads(result["body"])["message"].lower()


def test_invalid_path_returns_400(event_certify):
    """Ruta no reconocida retorna 400."""
    event_certify["path"] = "/ruta_invalida"
    result = lambda_handler(event_certify, None)
    assert result["statusCode"] == 400
    assert "Ruta no válida" in json.loads(result["body"])["message"]


def test_missing_params_returns_400(event_certify):
    """Body sin certCode retorna 400."""
    event_certify["body"] = json.dumps({"useCaseId": "UC-001"})
    result = lambda_handler(event_certify, None)
    assert result["statusCode"] == 400
    assert "Parámetros incompletos" in json.loads(result["body"])["message"]


def test_missing_use_case_id_returns_400(event_certify):
    """Body sin useCaseId retorna 400."""
    event_certify["body"] = json.dumps({"certCode": "CERT-123"})
    result = lambda_handler(event_certify, None)
    assert result["statusCode"] == 400


# ============================================================
# Tests: flujo con DynamoDB y S3 mockeados
# ============================================================

def test_cert_code_not_found_returns_404(mock_aws, event_certify):
    """certCode sin registros en DynamoDB retorna 404."""
    mock_table = MagicMock()
    mock_table.query.return_value = {"Items": []}
    mock_aws["dynamo"].Table.return_value = mock_table

    result = lambda_handler(event_certify, None)
    assert result["statusCode"] == 404
    body = json.loads(result["body"])
    assert "no encontrados" in body["message"].lower()


def test_unauthorized_approver_returns_403(mock_aws, event_certify):
    """Email no está en la lista de approvers del JSON de S3 retorna 403."""
    mock_table = MagicMock()
    mock_table.query.return_value = {
        "Items": [
            {
                "useCaseId": "UC-001",
                "executionId": "exec-001",
                "status": "PENDING",
                "configBucket": "niif-qa-assets",
                "configKey": "primas-arl/configs/certification/primasArl.json"
            }
        ]
    }
    mock_aws["dynamo"].Table.return_value = mock_table

    # approvers lista sin el email del evento
    _setup_s3_configs(mock_aws["s3"], approvers=["otro@example.com"])

    result = lambda_handler(event_certify, None)
    assert result["statusCode"] == 403
    assert "no autorizado" in json.loads(result["body"])["message"].lower()


def test_already_processed_returns_409(mock_aws, event_certify):
    """Registro con status CERTIFICATE retorna 409."""
    mock_table = MagicMock()
    mock_table.query.return_value = {
        "Items": [
            {
                "useCaseId": "UC-001",
                "executionId": "exec-001",
                "status": "CERTIFICATE",
                "configBucket": "niif-qa-assets",
                "configKey": "primas-arl/configs/certification/primasArl.json",
                "userEmail": "approver@example.com",
                "certificationDate": "2026-01-01T10:00:00"
            }
        ]
    }
    mock_aws["dynamo"].Table.return_value = mock_table
    _setup_s3_configs(mock_aws["s3"])

    result = lambda_handler(event_certify, None)
    assert result["statusCode"] == 409
    body = json.loads(result["body"])
    assert "procesado" in body["message"].lower()
    assert body["status"] == "CERTIFICATE"


def test_already_processed_no_certificate_returns_409(mock_aws, event_not_certify):
    """Registro con status NO_CERTIFICATE retorna 409."""
    mock_table = MagicMock()
    mock_table.query.return_value = {
        "Items": [
            {
                "useCaseId": "UC-001",
                "executionId": "exec-001",
                "status": "NO_CERTIFICATE",
                "configBucket": "niif-qa-assets",
                "configKey": "primas-arl/configs/certification/primasArl.json",
                "userEmail": "approver@example.com"
            }
        ]
    }
    mock_aws["dynamo"].Table.return_value = mock_table
    _setup_s3_configs(mock_aws["s3"])

    result = lambda_handler(event_not_certify, None)
    assert result["statusCode"] == 409


def test_single_approver_certify_returns_200_certificate(mock_aws, event_certify):
    """
    Un solo aprobador requerido: al certificar queda CERTIFICATE (no PENDING_APPROVERS).
    """
    mock_notif_table = MagicMock()
    mock_notif_table.query.return_value = {
        "Items": [
            {
                "useCaseId": "UC-001",
                "executionId": "exec-001",
                "status": "PENDING",
                "configBucket": "niif-qa-assets",
                "configKey": "primas-arl/configs/certification/primasArl.json"
            }
        ]
    }

    mock_log_table = MagicMock()
    # No existe log previo
    mock_log_table.get_item.return_value = {}

    def table_side_effect(name):
        if name == "niif-qa-notification-certification":
            return mock_notif_table
        if name == "niif-qa-approval-audit-log":
            return mock_log_table
        return MagicMock()

    mock_aws["dynamo"].Table.side_effect = table_side_effect
    # 1 aprobador requerido
    _setup_s3_configs(mock_aws["s3"], approvers=["approver@example.com"], required=1)

    result = lambda_handler(event_certify, None)
    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    assert body["finalStatus"] == "CERTIFICATE"
    assert body["pendingApprovals"] == 0
    assert body["updatedItems"] == 1
    mock_log_table.put_item.assert_called_once()
    mock_notif_table.update_item.assert_called_once()


def test_two_approvers_first_vote_returns_pending(mock_aws, event_certify):
    """
    Dos aprobadores requeridos: primer voto deja status PENDING_APPROVERS.
    """
    mock_notif_table = MagicMock()
    mock_notif_table.query.return_value = {
        "Items": [
            {
                "useCaseId": "UC-001",
                "executionId": "exec-001",
                "status": "PENDING",
                "configBucket": "niif-qa-assets",
                "configKey": "primas-arl/configs/certification/primasArl.json"
            }
        ]
    }

    mock_log_table = MagicMock()
    mock_log_table.get_item.return_value = {}

    def table_side_effect(name):
        if name == "niif-qa-notification-certification":
            return mock_notif_table
        if name == "niif-qa-approval-audit-log":
            return mock_log_table
        return MagicMock()

    mock_aws["dynamo"].Table.side_effect = table_side_effect
    _setup_s3_configs(
        mock_aws["s3"],
        approvers=["approver@example.com", "boss@example.com"],
        required=2
    )

    result = lambda_handler(event_certify, None)
    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    assert body["finalStatus"] == "PENDING_APPROVERS"
    assert body["pendingApprovals"] == 1
    mock_log_table.put_item.assert_called_once()


def test_already_voted_returns_200_with_existing_decision(mock_aws, event_certify):
    """
    El mismo approver ya votó: retorna 200 con alreadyVoted info,
    sin modificar DynamoDB nuevamente.
    """
    mock_notif_table = MagicMock()
    mock_notif_table.query.return_value = {
        "Items": [
            {
                "useCaseId": "UC-001",
                "executionId": "exec-001",
                "status": "PENDING_APPROVERS",
                "configBucket": "niif-qa-assets",
                "configKey": "primas-arl/configs/certification/primasArl.json"
            }
        ]
    }

    mock_log_table = MagicMock()
    # Ya existe un log con el email del aprobador
    mock_log_table.get_item.return_value = {
        "Item": {
            "useCaseId": "UC-001",
            "certCode": "CERT-123",
            "pendingApprovals": 1,
            "status": "PENDING",
            "emails": {
                "approver@example.com": {
                    "decision": "CERTIFICATE",
                    "decisionDate": "2026-01-01T10:00:00"
                }
            }
        }
    }

    def table_side_effect(name):
        if name == "niif-qa-notification-certification":
            return mock_notif_table
        if name == "niif-qa-approval-audit-log":
            return mock_log_table
        return MagicMock()

    mock_aws["dynamo"].Table.side_effect = table_side_effect
    _setup_s3_configs(
        mock_aws["s3"],
        approvers=["approver@example.com", "boss@example.com"],
        required=2
    )

    result = lambda_handler(event_certify, None)
    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    assert "ya registró" in body["message"].lower()
    assert body["existingDecision"] == "CERTIFICATE"
    assert body["updatedItems"] == 0
    # No debe haber intentado actualizar la tabla de notificaciones
    mock_notif_table.update_item.assert_not_called()


def test_no_certify_single_approver_returns_no_certificate(mock_aws, event_not_certify):
    """
    Ruta /not_certify con único aprobador: finalStatus = NO_CERTIFICATE.
    """
    mock_notif_table = MagicMock()
    mock_notif_table.query.return_value = {
        "Items": [
            {
                "useCaseId": "UC-001",
                "executionId": "exec-001",
                "status": "PENDING",
                "configBucket": "niif-qa-assets",
                "configKey": "primas-arl/configs/certification/primasArl.json"
            }
        ]
    }

    mock_log_table = MagicMock()
    mock_log_table.get_item.return_value = {}

    def table_side_effect(name):
        if name == "niif-qa-notification-certification":
            return mock_notif_table
        if name == "niif-qa-approval-audit-log":
            return mock_log_table
        return MagicMock()

    mock_aws["dynamo"].Table.side_effect = table_side_effect
    _setup_s3_configs(mock_aws["s3"], approvers=["approver@example.com"], required=1)

    result = lambda_handler(event_not_certify, None)
    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    assert body["finalStatus"] == "NO_CERTIFICATE"
    assert body["decision"] == "NO_CERTIFICATE"


# ============================================================
# Tests: helpers unitarios (líneas sin cobertura)
# ============================================================

def test_formatear_fecha_none_returns_no_disponible():
    """formatear_fecha_sin_microsegundos con None retorna 'No disponible'."""
    from certification import formatear_fecha_sin_microsegundos
    assert formatear_fecha_sin_microsegundos(None) == "No disponible"


def test_formatear_fecha_empty_returns_no_disponible():
    """formatear_fecha_sin_microsegundos con string vacío retorna 'No disponible'."""
    from certification import formatear_fecha_sin_microsegundos
    assert formatear_fecha_sin_microsegundos("") == "No disponible"


def test_formatear_fecha_with_microseconds():
    """formatear_fecha_sin_microsegundos elimina la parte de microsegundos."""
    from certification import formatear_fecha_sin_microsegundos
    result = formatear_fecha_sin_microsegundos("2026-06-05T16:26:25.227409-05:00")
    assert result == "2026-06-05T16:26:25"


def test_formatear_fecha_without_microseconds():
    """formatear_fecha_sin_microsegundos pasa bien una fecha ya sin microsegundos."""
    from certification import formatear_fecha_sin_microsegundos
    result = formatear_fecha_sin_microsegundos("2026-06-05T16:26:25")
    assert result == "2026-06-05T16:26:25"


def test_formatear_fecha_short_string():
    """formatear_fecha_sin_microsegundos con string corto lo retorna tal cual."""
    from certification import formatear_fecha_sin_microsegundos
    result = formatear_fecha_sin_microsegundos("2026")
    assert result == "2026"


def test_json_default_decimal_integer():
    """json_default convierte Decimal entero a int."""
    from certification import json_default
    result = json_default(Decimal("7"))
    assert result == 7
    assert isinstance(result, int)


def test_json_default_decimal_float():
    """json_default convierte Decimal con decimales a float."""
    from certification import json_default
    result = json_default(Decimal("3.14"))
    assert isinstance(result, float)


def test_json_default_unsupported_type_raises():
    """json_default lanza TypeError para tipos no soportados."""
    from certification import json_default
    with pytest.raises(TypeError):
        json_default(object())


def test_normalizar_s3_key_strips_full_s3_prefix(mock_aws):
    """normalizar_s3_key elimina el prefijo s3://bucket/ completo."""
    from certification import normalizar_s3_key
    result = normalizar_s3_key("niif-qa-assets", "s3://niif-qa-assets/configs/file.json")
    assert result == "configs/file.json"


def test_normalizar_s3_key_raises_on_wrong_bucket(mock_aws):
    """normalizar_s3_key lanza ValueError si la ruta s3:// apunta a otro bucket."""
    from certification import normalizar_s3_key
    with pytest.raises(ValueError):
        normalizar_s3_key("niif-qa-assets", "s3://otro-bucket/configs/file.json")


def test_obtener_config_caso_fallback_legacy(mock_aws):
    """
    obtener_config_caso sin config_key usa el CONFIG_KEY global como fallback.
    Cubre la rama elif CONFIG_KEY (línea 130).
    """
    from certification import obtener_config_caso
    import certification as cert_mod

    global_config = {"paths": {"use_cases_base": "configs/use-cases"}}
    case_config = {"useCaseId": "UC-001", "approvers": {"users": ["a@b.com"], "required": 1}}

    body1 = MagicMock()
    body1.read.return_value = json.dumps(global_config).encode()
    body2 = MagicMock()
    body2.read.return_value = json.dumps(case_config).encode()
    mock_aws["s3"].get_object.side_effect = [{"Body": body1}, {"Body": body2}]

    # config_key=None → rama elif CONFIG_KEY (ya tiene valor en os.environ)
    result = obtener_config_caso("UC-001", config_bucket="niif-qa-assets", config_key=None)
    assert result["useCaseId"] == "UC-001"


def test_obtener_config_caso_no_config_key_uses_use_cases_base(mock_aws, monkeypatch):
    """
    obtener_config_caso sin config_key y sin CONFIG_KEY usa paths.use_cases_base.
    Cubre las líneas 127-130 (rama else).
    """
    import certification as cert_mod
    monkeypatch.setattr(cert_mod, "CONFIG_KEY", "")

    global_config = {"paths": {"use_cases_base": "configs/use-cases"}}
    case_config = {"useCaseId": "UC-002", "approvers": {"users": ["a@b.com"], "required": 1}}

    body1 = MagicMock()
    body1.read.return_value = json.dumps(global_config).encode()
    body2 = MagicMock()
    body2.read.return_value = json.dumps(case_config).encode()
    mock_aws["s3"].get_object.side_effect = [{"Body": body1}, {"Body": body2}]

    from certification import obtener_config_caso
    result = obtener_config_caso("UC-002", config_bucket="niif-qa-assets", config_key=None)
    assert result["useCaseId"] == "UC-002"


def test_obtener_approvers_no_users_raises(mock_aws):
    """
    obtener_approvers_desde_json lanza RuntimeError cuando approvers.users está vacío.
    Cubre línea 141-142.
    """
    import certification as cert_mod

    global_config = {"paths": {"use_cases_base": "configs/use-cases"}}
    case_config = {"useCaseId": "UC-001", "approvers": {"users": [], "required": 1}}

    body1 = MagicMock()
    body1.read.return_value = json.dumps(global_config).encode()
    body2 = MagicMock()
    body2.read.return_value = json.dumps(case_config).encode()
    mock_aws["s3"].get_object.side_effect = [{"Body": body1}, {"Body": body2}]

    from certification import obtener_approvers_desde_json
    with pytest.raises(RuntimeError, match="No hay approvers"):
        obtener_approvers_desde_json("UC-001", config_bucket="niif-qa-assets",
                                     config_key="some/key.json")


def test_obtener_approvers_required_less_than_1(mock_aws):
    """
    required < 1 se normaliza a 1. Cubre línea 144-145.
    """
    global_config = {"paths": {"use_cases_base": "configs/use-cases"}}
    case_config = {"useCaseId": "UC-001", "approvers": {"users": ["a@b.com"], "required": 0}}

    body1 = MagicMock()
    body1.read.return_value = json.dumps(global_config).encode()
    body2 = MagicMock()
    body2.read.return_value = json.dumps(case_config).encode()
    mock_aws["s3"].get_object.side_effect = [{"Body": body1}, {"Body": body2}]

    from certification import obtener_approvers_desde_json
    approvers, required = obtener_approvers_desde_json(
        "UC-001", config_bucket="niif-qa-assets", config_key="some/key.json"
    )
    assert required == 1


def test_obtener_approvers_required_exceeds_users_raises(mock_aws):
    """
    required > len(users) lanza RuntimeError. Cubre líneas 147-149.
    """
    global_config = {"paths": {"use_cases_base": "configs/use-cases"}}
    case_config = {"useCaseId": "UC-001", "approvers": {"users": ["a@b.com"], "required": 5}}

    body1 = MagicMock()
    body1.read.return_value = json.dumps(global_config).encode()
    body2 = MagicMock()
    body2.read.return_value = json.dumps(case_config).encode()
    mock_aws["s3"].get_object.side_effect = [{"Body": body1}, {"Body": body2}]

    from certification import obtener_approvers_desde_json
    with pytest.raises(RuntimeError, match="approvers.required"):
        obtener_approvers_desde_json(
            "UC-001", config_bucket="niif-qa-assets", config_key="some/key.json"
        )


def test_build_update_expressions_no_certificate():
    """
    build_update_expressions con cert_status=NO_CERTIFICATE genera condition sin :transport.
    Cubre líneas 166-169.
    """
    from certification import build_update_expressions
    update_expr, cond_expr, names, values = build_update_expressions(
        "NO_CERTIFICATE", "NO_CERTIFICATE", "2026-01-01T00:00:00", "user@test.com"
    )
    assert ":certificate" in values
    assert ":transport" not in values
    assert "attribute_not_exists" in cond_expr


def test_build_update_expressions_certificate():
    """
    build_update_expressions con cert_status=CERTIFICATE agrega :transport.
    Cubre línea 172.
    """
    from certification import build_update_expressions
    update_expr, cond_expr, names, values = build_update_expressions(
        "CERTIFICATE", "CERTIFICATE", "2026-01-01T00:00:00", "user@test.com"
    )
    assert ":transport" in values
    assert "transport" in cond_expr.lower()


def test_process_approval_audit_second_voter(mock_aws):
    """
    process_approval_audit con registro existente y email nuevo → update_item.
    Cubre líneas 278-306.
    """
    from certification import process_approval_audit

    mock_log_table = MagicMock()
    mock_log_table.get_item.return_value = {
        "Item": {
            "useCaseId": "UC-001",
            "certCode": "CERT-123",
            "pendingApprovals": 1,
            "status": "PENDING",
            "emails": {
                "first@example.com": {
                    "decision": "CERTIFICATE",
                    "decisionDate": "2026-01-01T10:00:00"
                }
            }
        }
    }
    mock_aws["dynamo"].Table.return_value = mock_log_table

    result = process_approval_audit(
        mock_aws["dynamo"],
        "niif-qa-approval-audit-log",
        "UC-001",
        "CERT-123",
        "second@example.com",
        "CERTIFICATE",
        2
    )

    assert result["pendingApprovals"] == 0
    mock_log_table.update_item.assert_called_once()


def test_process_approval_audit_second_voter_no_certificate(mock_aws):
    """
    process_approval_audit segundo votante con NO_CERTIFICATE → status CANCELED.
    Cubre la rama cert_status == NO_CERTIFICATE en el bloque update (líneas 280-282).
    """
    from certification import process_approval_audit

    mock_log_table = MagicMock()
    mock_log_table.get_item.return_value = {
        "Item": {
            "useCaseId": "UC-001",
            "certCode": "CERT-123",
            "pendingApprovals": 1,
            "status": "PENDING",
            "emails": {
                "first@example.com": {
                    "decision": "CERTIFICATE",
                    "decisionDate": "2026-01-01T10:00:00"
                }
            }
        }
    }
    mock_aws["dynamo"].Table.return_value = mock_log_table

    result = process_approval_audit(
        mock_aws["dynamo"],
        "niif-qa-approval-audit-log",
        "UC-001",
        "CERT-123",
        "second@example.com",
        "NO_CERTIFICATE",
        2
    )

    assert result["pendingApprovals"] == 0
    mock_log_table.update_item.assert_called_once()


def test_base64_encoded_body_is_decoded(mock_aws):
    """
    isBase64Encoded=True decodifica el body correctamente.
    Cubre líneas 347-348.
    """
    import base64

    body_data = json.dumps({"useCaseId": "UC-001", "certCode": "CERT-123"})
    encoded_body = base64.b64encode(body_data.encode()).decode()

    mock_table = MagicMock()
    mock_table.query.return_value = {"Items": []}
    mock_aws["dynamo"].Table.return_value = mock_table

    event = {
        "requestContext": {"authorizer": {"claims": {"email": "approver@example.com"}}},
        "path": "/certify",
        "body": encoded_body,
        "isBase64Encoded": True
    }

    result = lambda_handler(event, None)
    # Items vacío → 404, pero el body fue decodificado correctamente
    assert result["statusCode"] == 404


def test_already_voted_audit_status_canceled(mock_aws, event_certify):
    """
    alreadyVoted con auditStatus=CANCELED → finalStatus NO_CERTIFICATE.
    Cubre línea 479.
    """
    mock_notif_table = MagicMock()
    mock_notif_table.query.return_value = {
        "Items": [
            {
                "useCaseId": "UC-001",
                "executionId": "exec-001",
                "status": "PENDING_APPROVERS",
                "configBucket": "niif-qa-assets",
                "configKey": "primas-arl/configs/certification/primasArl.json"
            }
        ]
    }

    mock_log_table = MagicMock()
    mock_log_table.get_item.return_value = {
        "Item": {
            "useCaseId": "UC-001",
            "certCode": "CERT-123",
            "pendingApprovals": 0,
            "status": "CANCELED",
            "emails": {
                "approver@example.com": {
                    "decision": "NO_CERTIFICATE",
                    "decisionDate": "2026-01-01T10:00:00"
                }
            }
        }
    }

    def table_side_effect(name):
        if name == "niif-qa-notification-certification":
            return mock_notif_table
        if name == "niif-qa-approval-audit-log":
            return mock_log_table
        return MagicMock()

    mock_aws["dynamo"].Table.side_effect = table_side_effect
    _setup_s3_configs(mock_aws["s3"], approvers=["approver@example.com", "boss@example.com"],
                      required=2)

    result = lambda_handler(event_certify, None)
    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    assert body["finalStatus"] == "NO_CERTIFICATE"


def test_already_voted_audit_status_pending(mock_aws, event_certify):
    """
    alreadyVoted con auditStatus=PENDING → finalStatus PENDING_APPROVERS.
    Cubre línea 481.
    """
    mock_notif_table = MagicMock()
    mock_notif_table.query.return_value = {
        "Items": [
            {
                "useCaseId": "UC-001",
                "executionId": "exec-001",
                "status": "PENDING_APPROVERS",
                "configBucket": "niif-qa-assets",
                "configKey": "primas-arl/configs/certification/primasArl.json"
            }
        ]
    }

    mock_log_table = MagicMock()
    mock_log_table.get_item.return_value = {
        "Item": {
            "useCaseId": "UC-001",
            "certCode": "CERT-123",
            "pendingApprovals": 1,
            "status": "PENDING",
            "emails": {
                "approver@example.com": {
                    "decision": "CERTIFICATE",
                    "decisionDate": "2026-01-01T10:00:00"
                }
            }
        }
    }

    def table_side_effect(name):
        if name == "niif-qa-notification-certification":
            return mock_notif_table
        if name == "niif-qa-approval-audit-log":
            return mock_log_table
        return MagicMock()

    mock_aws["dynamo"].Table.side_effect = table_side_effect
    _setup_s3_configs(mock_aws["s3"], approvers=["approver@example.com", "boss@example.com"],
                      required=2)

    result = lambda_handler(event_certify, None)
    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    assert body["finalStatus"] == "PENDING_APPROVERS"


def test_already_voted_audit_status_unknown_uses_resolve(mock_aws, event_certify):
    """
    alreadyVoted con auditStatus desconocido → usa resolve_final_status.
    Cubre línea 485.
    """
    mock_notif_table = MagicMock()
    mock_notif_table.query.return_value = {
        "Items": [
            {
                "useCaseId": "UC-001",
                "executionId": "exec-001",
                "status": "PENDING_APPROVERS",
                "configBucket": "niif-qa-assets",
                "configKey": "primas-arl/configs/certification/primasArl.json"
            }
        ]
    }

    mock_log_table = MagicMock()
    mock_log_table.get_item.return_value = {
        "Item": {
            "useCaseId": "UC-001",
            "certCode": "CERT-123",
            "pendingApprovals": 0,
            "status": "UNKNOWN_STATUS",
            "emails": {
                "approver@example.com": {
                    "decision": "CERTIFICATE",
                    "decisionDate": "2026-01-01T10:00:00"
                }
            }
        }
    }

    def table_side_effect(name):
        if name == "niif-qa-notification-certification":
            return mock_notif_table
        if name == "niif-qa-approval-audit-log":
            return mock_log_table
        return MagicMock()

    mock_aws["dynamo"].Table.side_effect = table_side_effect
    _setup_s3_configs(mock_aws["s3"], approvers=["approver@example.com", "boss@example.com"],
                      required=2)

    result = lambda_handler(event_certify, None)
    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    # pendingApprovals=0 → resolve_final_status retorna la existingDecision
    assert body["finalStatus"] == "CERTIFICATE"


def test_update_item_condition_exception_returns_500(mock_aws, event_certify):
    """
    update_item lanza excepción durante el loop → capturado por except → 500.
    Cubre líneas 561-563.
    """
    from botocore.exceptions import ClientError

    mock_notif_table = MagicMock()
    mock_notif_table.query.return_value = {
        "Items": [
            {
                "useCaseId": "UC-001",
                "executionId": "exec-001",
                "status": "PENDING",
                "configBucket": "niif-qa-assets",
                "configKey": "primas-arl/configs/certification/primasArl.json"
            }
        ]
    }
    mock_notif_table.update_item.side_effect = Exception("DynamoDB error inesperado")
import os
import json
import pytest
from decimal import Decimal
from unittest.mock import patch, MagicMock, call

# ============================================================
# Variables de entorno antes de importar el módulo
# ============================================================
os.environ["NOTIFICATIONS_TABLE"] = "niif-qa-notification-certification"
os.environ["APPROVERS_LOG_TABLE"] = "niif-qa-approval-audit-log"
os.environ["CONFIG_BUCKET"] = "niif-qa-assets"
os.environ["GLOBAL_CONFIG_KEY"] = "configs/certification/json_global_notif_cert.json"
os.environ["CONFIG_KEY"] = "primas-arl/configs/certification/primasArl.json"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
os.environ["AWS_REGION"] = "us-east-1"

# Mock boto3 antes de importar para evitar llamadas reales a AWS
with patch("boto3.resource"), patch("boto3.client"):
    import certification
    from certification import lambda_handler


# ============================================================
# Fixtures base
# ============================================================

@pytest.fixture
def event_certify():
    """Evento API Gateway para ruta /certify con claims Cognito."""
    return {
        "requestContext": {
            "authorizer": {
                "claims": {
                    "email": "approver@example.com"
                }
            }
        },
        "path": "/certify",
        "body": json.dumps({
            "useCaseId": "UC-001",
            "certCode": "CERT-123"
        }),
        "isBase64Encoded": False
    }


@pytest.fixture
def event_not_certify():
    """Evento API Gateway para ruta /not_certify con claims Cognito."""
    return {
        "requestContext": {
            "authorizer": {
                "claims": {
                    "email": "approver@example.com"
                }
            }
        },
        "path": "/not_certify",
        "body": json.dumps({
            "useCaseId": "UC-001",
            "certCode": "CERT-123"
        }),
        "isBase64Encoded": False
    }


@pytest.fixture
def mock_aws(monkeypatch):
    """
    Mock centralizado de dynamodb (resource) y s3 (client) a nivel de módulo.
    Retorna un dict con las piezas mockeadas para configurar en cada test.
    """
    mock_dynamo = MagicMock()
    mock_s3 = MagicMock()
    monkeypatch.setattr(certification, "dynamodb", mock_dynamo)
    monkeypatch.setattr(certification, "s3", mock_s3)
    return {"dynamo": mock_dynamo, "s3": mock_s3}


def _make_s3_json_response(obj):
    """Helper: simula la respuesta de s3.get_object para un JSON."""
    body_mock = MagicMock()
    body_mock.read.return_value = json.dumps(obj).encode("utf-8")
    return {"Body": body_mock}


def _setup_s3_configs(mock_s3, approvers=None, required=1):
    """
    Configura el mock de S3 para devolver global_config y case_config.
    approvers: lista de emails habilitados (default: ["approver@example.com"])
    """
    if approvers is None:
        approvers = ["approver@example.com"]

    global_config = {
        "paths": {
            "use_cases_base": "configs/certification/use-cases"
        }
    }

    case_config = {
        "useCaseId": "UC-001",
        "approvers": {
            "users": approvers,
            "required": required
        }
    }

    # get_object se llama dos veces: primero global, luego case config
    mock_s3.get_object.side_effect = [
        _make_s3_json_response(global_config),
        _make_s3_json_response(case_config)
    ]


# ============================================================
# Tests: validaciones de entrada (sin AWS)
# ============================================================

def test_unauthenticated_user_returns_401():
    """Sin email en claims retorna 401."""
    event = {
        "requestContext": {"authorizer": {"claims": {}}},
        "path": "/certify",
        "body": json.dumps({"useCaseId": "UC-001", "certCode": "CERT-123"})
    }
    result = lambda_handler(event, None)
    assert result["statusCode"] == 401
    assert "no autenticado" in json.loads(result["body"])["message"].lower()


def test_invalid_path_returns_400(event_certify):
    """Ruta no reconocida retorna 400."""
    event_certify["path"] = "/ruta_invalida"
    result = lambda_handler(event_certify, None)
    assert result["statusCode"] == 400
    assert "Ruta no válida" in json.loads(result["body"])["message"]


def test_missing_params_returns_400(event_certify):
    """Body sin certCode retorna 400."""
    event_certify["body"] = json.dumps({"useCaseId": "UC-001"})
    result = lambda_handler(event_certify, None)
    assert result["statusCode"] == 400
    assert "Parámetros incompletos" in json.loads(result["body"])["message"]


def test_missing_use_case_id_returns_400(event_certify):
    """Body sin useCaseId retorna 400."""
    event_certify["body"] = json.dumps({"certCode": "CERT-123"})
    result = lambda_handler(event_certify, None)
    assert result["statusCode"] == 400


# ============================================================
# Tests: flujo con DynamoDB y S3 mockeados
# ============================================================

def test_cert_code_not_found_returns_404(mock_aws, event_certify):
    """certCode sin registros en DynamoDB retorna 404."""
    mock_table = MagicMock()
    mock_table.query.return_value = {"Items": []}
    mock_aws["dynamo"].Table.return_value = mock_table

    result = lambda_handler(event_certify, None)
    assert result["statusCode"] == 404
    body = json.loads(result["body"])
    assert "no encontrados" in body["message"].lower()


def test_unauthorized_approver_returns_403(mock_aws, event_certify):
    """Email no está en la lista de approvers del JSON de S3 retorna 403."""
    mock_table = MagicMock()
    mock_table.query.return_value = {
        "Items": [
            {
                "useCaseId": "UC-001",
                "executionId": "exec-001",
                "status": "PENDING",
                "configBucket": "niif-qa-assets",
                "configKey": "primas-arl/configs/certification/primasArl.json"
            }
        ]
    }
    mock_aws["dynamo"].Table.return_value = mock_table

    # approvers lista sin el email del evento
    _setup_s3_configs(mock_aws["s3"], approvers=["otro@example.com"])

    result = lambda_handler(event_certify, None)
    assert result["statusCode"] == 403
    assert "no autorizado" in json.loads(result["body"])["message"].lower()


def test_already_processed_returns_409(mock_aws, event_certify):
    """Registro con status CERTIFICATE retorna 409."""
    mock_table = MagicMock()
    mock_table.query.return_value = {
        "Items": [
            {
                "useCaseId": "UC-001",
                "executionId": "exec-001",
                "status": "CERTIFICATE",
                "configBucket": "niif-qa-assets",
                "configKey": "primas-arl/configs/certification/primasArl.json",
                "userEmail": "approver@example.com",
                "certificationDate": "2026-01-01T10:00:00"
            }
        ]
    }
    mock_aws["dynamo"].Table.return_value = mock_table
    _setup_s3_configs(mock_aws["s3"])

    result = lambda_handler(event_certify, None)
    assert result["statusCode"] == 409
    body = json.loads(result["body"])
    assert "procesado" in body["message"].lower()
    assert body["status"] == "CERTIFICATE"


def test_already_processed_no_certificate_returns_409(mock_aws, event_not_certify):
    """Registro con status NO_CERTIFICATE retorna 409."""
    mock_table = MagicMock()
    mock_table.query.return_value = {
        "Items": [
            {
                "useCaseId": "UC-001",
                "executionId": "exec-001",
                "status": "NO_CERTIFICATE",
                "configBucket": "niif-qa-assets",
                "configKey": "primas-arl/configs/certification/primasArl.json",
                "userEmail": "approver@example.com"
            }
        ]
    }
    mock_aws["dynamo"].Table.return_value = mock_table
    _setup_s3_configs(mock_aws["s3"])

    result = lambda_handler(event_not_certify, None)
    assert result["statusCode"] == 409


def test_single_approver_certify_returns_200_certificate(mock_aws, event_certify):
    """
    Un solo aprobador requerido: al certificar queda CERTIFICATE (no PENDING_APPROVERS).
    """
    mock_notif_table = MagicMock()
    mock_notif_table.query.return_value = {
        "Items": [
            {
                "useCaseId": "UC-001",
                "executionId": "exec-001",
                "status": "PENDING",
                "configBucket": "niif-qa-assets",
                "configKey": "primas-arl/configs/certification/primasArl.json"
            }
        ]
    }

    mock_log_table = MagicMock()
    # No existe log previo
    mock_log_table.get_item.return_value = {}

    def table_side_effect(name):
        if name == "niif-qa-notification-certification":
            return mock_notif_table
        if name == "niif-qa-approval-audit-log":
            return mock_log_table
        return MagicMock()

    mock_aws["dynamo"].Table.side_effect = table_side_effect
    # 1 aprobador requerido
    _setup_s3_configs(mock_aws["s3"], approvers=["approver@example.com"], required=1)

    result = lambda_handler(event_certify, None)
    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    assert body["finalStatus"] == "CERTIFICATE"
    assert body["pendingApprovals"] == 0
    assert body["updatedItems"] == 1
    mock_log_table.put_item.assert_called_once()
    mock_notif_table.update_item.assert_called_once()


def test_two_approvers_first_vote_returns_pending(mock_aws, event_certify):
    """
    Dos aprobadores requeridos: primer voto deja status PENDING_APPROVERS.
    """
    mock_notif_table = MagicMock()
    mock_notif_table.query.return_value = {
        "Items": [
            {
                "useCaseId": "UC-001",
                "executionId": "exec-001",
                "status": "PENDING",
                "configBucket": "niif-qa-assets",
                "configKey": "primas-arl/configs/certification/primasArl.json"
            }
        ]
    }

    mock_log_table = MagicMock()
    mock_log_table.get_item.return_value = {}

    def table_side_effect(name):
        if name == "niif-qa-notification-certification":
            return mock_notif_table
        if name == "niif-qa-approval-audit-log":
            return mock_log_table
        return MagicMock()

    mock_aws["dynamo"].Table.side_effect = table_side_effect
    _setup_s3_configs(
        mock_aws["s3"],
        approvers=["approver@example.com", "boss@example.com"],
        required=2
    )

    result = lambda_handler(event_certify, None)
    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    assert body["finalStatus"] == "PENDING_APPROVERS"
    assert body["pendingApprovals"] == 1
    mock_log_table.put_item.assert_called_once()


def test_already_voted_returns_200_with_existing_decision(mock_aws, event_certify):
    """
    El mismo approver ya votó: retorna 200 con alreadyVoted info,
    sin modificar DynamoDB nuevamente.
    """
    mock_notif_table = MagicMock()
    mock_notif_table.query.return_value = {
        "Items": [
            {
                "useCaseId": "UC-001",
                "executionId": "exec-001",
                "status": "PENDING_APPROVERS",
                "configBucket": "niif-qa-assets",
                "configKey": "primas-arl/configs/certification/primasArl.json"
            }
        ]
    }

    mock_log_table = MagicMock()
    # Ya existe un log con el email del aprobador
    mock_log_table.get_item.return_value = {
        "Item": {
            "useCaseId": "UC-001",
            "certCode": "CERT-123",
            "pendingApprovals": 1,
            "status": "PENDING",
            "emails": {
                "approver@example.com": {
                    "decision": "CERTIFICATE",
                    "decisionDate": "2026-01-01T10:00:00"
                }
            }
        }
    }

    def table_side_effect(name):
        if name == "niif-qa-notification-certification":
            return mock_notif_table
        if name == "niif-qa-approval-audit-log":
            return mock_log_table
        return MagicMock()

    mock_aws["dynamo"].Table.side_effect = table_side_effect
    _setup_s3_configs(
        mock_aws["s3"],
        approvers=["approver@example.com", "boss@example.com"],
        required=2
    )

    result = lambda_handler(event_certify, None)
    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    assert "ya registró" in body["message"].lower()
    assert body["existingDecision"] == "CERTIFICATE"
    assert body["updatedItems"] == 0
    # No debe haber intentado actualizar la tabla de notificaciones
    mock_notif_table.update_item.assert_not_called()


def test_no_certify_single_approver_returns_no_certificate(mock_aws, event_not_certify):
    """
    Ruta /not_certify con único aprobador: finalStatus = NO_CERTIFICATE.
    """
    mock_notif_table = MagicMock()
    mock_notif_table.query.return_value = {
        "Items": [
            {
                "useCaseId": "UC-001",
                "executionId": "exec-001",
                "status": "PENDING",
                "configBucket": "niif-qa-assets",
                "configKey": "primas-arl/configs/certification/primasArl.json"
            }
        ]
    }

    mock_log_table = MagicMock()
    mock_log_table.get_item.return_value = {}

    def table_side_effect(name):
        if name == "niif-qa-notification-certification":
            return mock_notif_table
        if name == "niif-qa-approval-audit-log":
            return mock_log_table
        return MagicMock()

    mock_aws["dynamo"].Table.side_effect = table_side_effect
    _setup_s3_configs(mock_aws["s3"], approvers=["approver@example.com"], required=1)

    result = lambda_handler(event_not_certify, None)
    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    assert body["finalStatus"] == "NO_CERTIFICATE"
    assert body["decision"] == "NO_CERTIFICATE"


# ============================================================
# Tests: helpers unitarios (líneas sin cobertura)
# ============================================================

def test_formatear_fecha_none_returns_no_disponible():
    """formatear_fecha_sin_microsegundos con None retorna 'No disponible'."""
    from certification import formatear_fecha_sin_microsegundos
    assert formatear_fecha_sin_microsegundos(None) == "No disponible"


def test_formatear_fecha_empty_returns_no_disponible():
    """formatear_fecha_sin_microsegundos con string vacío retorna 'No disponible'."""
    from certification import formatear_fecha_sin_microsegundos
    assert formatear_fecha_sin_microsegundos("") == "No disponible"


def test_formatear_fecha_with_microseconds():
    """formatear_fecha_sin_microsegundos elimina la parte de microsegundos."""
    from certification import formatear_fecha_sin_microsegundos
    result = formatear_fecha_sin_microsegundos("2026-06-05T16:26:25.227409-05:00")
    assert result == "2026-06-05T16:26:25"


def test_formatear_fecha_without_microseconds():
    """formatear_fecha_sin_microsegundos pasa bien una fecha ya sin microsegundos."""
    from certification import formatear_fecha_sin_microsegundos
    result = formatear_fecha_sin_microsegundos("2026-06-05T16:26:25")
    assert result == "2026-06-05T16:26:25"


def test_formatear_fecha_short_string():
    """formatear_fecha_sin_microsegundos con string corto lo retorna tal cual."""
    from certification import formatear_fecha_sin_microsegundos
    result = formatear_fecha_sin_microsegundos("2026")
    assert result == "2026"


def test_json_default_decimal_integer():
    """json_default convierte Decimal entero a int."""
    from certification import json_default
    result = json_default(Decimal("7"))
    assert result == 7
    assert isinstance(result, int)


def test_json_default_decimal_float():
    """json_default convierte Decimal con decimales a float."""
    from certification import json_default
    result = json_default(Decimal("3.14"))
    assert isinstance(result, float)


def test_json_default_unsupported_type_raises():
    """json_default lanza TypeError para tipos no soportados."""
    from certification import json_default
    with pytest.raises(TypeError):
        json_default(object())


def test_normalizar_s3_key_strips_full_s3_prefix(mock_aws):
    """normalizar_s3_key elimina el prefijo s3://bucket/ completo."""
    from certification import normalizar_s3_key
    result = normalizar_s3_key("niif-qa-assets", "s3://niif-qa-assets/configs/file.json")
    assert result == "configs/file.json"


def test_normalizar_s3_key_raises_on_wrong_bucket(mock_aws):
    """normalizar_s3_key lanza ValueError si la ruta s3:// apunta a otro bucket."""
    from certification import normalizar_s3_key
    with pytest.raises(ValueError):
        normalizar_s3_key("niif-qa-assets", "s3://otro-bucket/configs/file.json")


def test_obtener_config_caso_fallback_legacy(mock_aws):
    """
    obtener_config_caso sin config_key usa el CONFIG_KEY global como fallback.
    Cubre la rama elif CONFIG_KEY (línea 130).
    """
    from certification import obtener_config_caso
    import certification as cert_mod

    global_config = {"paths": {"use_cases_base": "configs/use-cases"}}
    case_config = {"useCaseId": "UC-001", "approvers": {"users": ["a@b.com"], "required": 1}}

    body1 = MagicMock()
    body1.read.return_value = json.dumps(global_config).encode()
    body2 = MagicMock()
    body2.read.return_value = json.dumps(case_config).encode()
    mock_aws["s3"].get_object.side_effect = [{"Body": body1}, {"Body": body2}]

    # config_key=None → rama elif CONFIG_KEY (ya tiene valor en os.environ)
    result = obtener_config_caso("UC-001", config_bucket="niif-qa-assets", config_key=None)
    assert result["useCaseId"] == "UC-001"


def test_obtener_config_caso_no_config_key_uses_use_cases_base(mock_aws, monkeypatch):
    """
    obtener_config_caso sin config_key y sin CONFIG_KEY usa paths.use_cases_base.
    Cubre las líneas 127-130 (rama else).
    """
    import certification as cert_mod
    monkeypatch.setattr(cert_mod, "CONFIG_KEY", "")

    global_config = {"paths": {"use_cases_base": "configs/use-cases"}}
    case_config = {"useCaseId": "UC-002", "approvers": {"users": ["a@b.com"], "required": 1}}

    body1 = MagicMock()
    body1.read.return_value = json.dumps(global_config).encode()
    body2 = MagicMock()
    body2.read.return_value = json.dumps(case_config).encode()
    mock_aws["s3"].get_object.side_effect = [{"Body": body1}, {"Body": body2}]

    from certification import obtener_config_caso
    result = obtener_config_caso("UC-002", config_bucket="niif-qa-assets", config_key=None)
    assert result["useCaseId"] == "UC-002"


def test_obtener_approvers_no_users_raises(mock_aws):
    """
    obtener_approvers_desde_json lanza RuntimeError cuando approvers.users está vacío.
    Cubre línea 141-142.
    """
    import certification as cert_mod

    global_config = {"paths": {"use_cases_base": "configs/use-cases"}}
    case_config = {"useCaseId": "UC-001", "approvers": {"users": [], "required": 1}}

    body1 = MagicMock()
    body1.read.return_value = json.dumps(global_config).encode()
    body2 = MagicMock()
    body2.read.return_value = json.dumps(case_config).encode()
    mock_aws["s3"].get_object.side_effect = [{"Body": body1}, {"Body": body2}]

    from certification import obtener_approvers_desde_json
    with pytest.raises(RuntimeError, match="No hay approvers"):
        obtener_approvers_desde_json("UC-001", config_bucket="niif-qa-assets",
                                     config_key="some/key.json")


def test_obtener_approvers_required_less_than_1(mock_aws):
    """
    required < 1 se normaliza a 1. Cubre línea 144-145.
    """
    global_config = {"paths": {"use_cases_base": "configs/use-cases"}}
    case_config = {"useCaseId": "UC-001", "approvers": {"users": ["a@b.com"], "required": 0}}

    body1 = MagicMock()
    body1.read.return_value = json.dumps(global_config).encode()
    body2 = MagicMock()
    body2.read.return_value = json.dumps(case_config).encode()
    mock_aws["s3"].get_object.side_effect = [{"Body": body1}, {"Body": body2}]

    from certification import obtener_approvers_desde_json
    approvers, required = obtener_approvers_desde_json(
        "UC-001", config_bucket="niif-qa-assets", config_key="some/key.json"
    )
    assert required == 1


def test_obtener_approvers_required_exceeds_users_raises(mock_aws):
    """
    required > len(users) lanza RuntimeError. Cubre líneas 147-149.
    """
    global_config = {"paths": {"use_cases_base": "configs/use-cases"}}
    case_config = {"useCaseId": "UC-001", "approvers": {"users": ["a@b.com"], "required": 5}}

    body1 = MagicMock()
    body1.read.return_value = json.dumps(global_config).encode()
    body2 = MagicMock()
    body2.read.return_value = json.dumps(case_config).encode()
    mock_aws["s3"].get_object.side_effect = [{"Body": body1}, {"Body": body2}]

    from certification import obtener_approvers_desde_json
    with pytest.raises(RuntimeError, match="approvers.required"):
        obtener_approvers_desde_json(
            "UC-001", config_bucket="niif-qa-assets", config_key="some/key.json"
        )


def test_build_update_expressions_no_certificate():
    """
    build_update_expressions con cert_status=NO_CERTIFICATE genera condition sin :transport.
    Cubre líneas 166-169.
    """
    from certification import build_update_expressions
    update_expr, cond_expr, names, values = build_update_expressions(
        "NO_CERTIFICATE", "NO_CERTIFICATE", "2026-01-01T00:00:00", "user@test.com"
    )
    assert ":certificate" in values
    assert ":transport" not in values
    assert "attribute_not_exists" in cond_expr


def test_build_update_expressions_certificate():
    """
    build_update_expressions con cert_status=CERTIFICATE agrega :transport.
    Cubre línea 172.
    """
    from certification import build_update_expressions
    update_expr, cond_expr, names, values = build_update_expressions(
        "CERTIFICATE", "CERTIFICATE", "2026-01-01T00:00:00", "user@test.com"
    )
    assert ":transport" in values
    assert "transport" in cond_expr.lower()


def test_process_approval_audit_second_voter(mock_aws):
    """
    process_approval_audit con registro existente y email nuevo → update_item.
    Cubre líneas 278-306.
    """
    from certification import process_approval_audit

    mock_log_table = MagicMock()
    mock_log_table.get_item.return_value = {
        "Item": {
            "useCaseId": "UC-001",
            "certCode": "CERT-123",
            "pendingApprovals": 1,
            "status": "PENDING",
            "emails": {
                "first@example.com": {
                    "decision": "CERTIFICATE",
                    "decisionDate": "2026-01-01T10:00:00"
                }
            }
        }
    }
    mock_aws["dynamo"].Table.return_value = mock_log_table

    result = process_approval_audit(
        mock_aws["dynamo"],
        "niif-qa-approval-audit-log",
        "UC-001",
        "CERT-123",
        "second@example.com",
        "CERTIFICATE",
        2
    )

    assert result["pendingApprovals"] == 0
    mock_log_table.update_item.assert_called_once()


def test_process_approval_audit_second_voter_no_certificate(mock_aws):
    """
    process_approval_audit segundo votante con NO_CERTIFICATE → status CANCELED.
    Cubre la rama cert_status == NO_CERTIFICATE en el bloque update (líneas 280-282).
    """
    from certification import process_approval_audit

    mock_log_table = MagicMock()
    mock_log_table.get_item.return_value = {
        "Item": {
            "useCaseId": "UC-001",
            "certCode": "CERT-123",
            "pendingApprovals": 1,
            "status": "PENDING",
            "emails": {
                "first@example.com": {
                    "decision": "CERTIFICATE",
                    "decisionDate": "2026-01-01T10:00:00"
                }
            }
        }
    }
    mock_aws["dynamo"].Table.return_value = mock_log_table

    result = process_approval_audit(
        mock_aws["dynamo"],
        "niif-qa-approval-audit-log",
        "UC-001",
        "CERT-123",
        "second@example.com",
        "NO_CERTIFICATE",
        2
    )

    assert result["pendingApprovals"] == 0
    mock_log_table.update_item.assert_called_once()


def test_base64_encoded_body_is_decoded(mock_aws):
    """
    isBase64Encoded=True decodifica el body correctamente.
    Cubre líneas 347-348.
    """
    import base64

    body_data = json.dumps({"useCaseId": "UC-001", "certCode": "CERT-123"})
    encoded_body = base64.b64encode(body_data.encode()).decode()

    mock_table = MagicMock()
    mock_table.query.return_value = {"Items": []}
    mock_aws["dynamo"].Table.return_value = mock_table

    event = {
        "requestContext": {"authorizer": {"claims": {"email": "approver@example.com"}}},
        "path": "/certify",
        "body": encoded_body,
        "isBase64Encoded": True
    }

    result = lambda_handler(event, None)
    # Items vacío → 404, pero el body fue decodificado correctamente
    assert result["statusCode"] == 404


def test_already_voted_audit_status_canceled(mock_aws, event_certify):
    """
    alreadyVoted con auditStatus=CANCELED → finalStatus NO_CERTIFICATE.
    Cubre línea 479.
    """
    mock_notif_table = MagicMock()
    mock_notif_table.query.return_value = {
        "Items": [
            {
                "useCaseId": "UC-001",
                "executionId": "exec-001",
                "status": "PENDING_APPROVERS",
                "configBucket": "niif-qa-assets",
                "configKey": "primas-arl/configs/certification/primasArl.json"
            }
        ]
    }

    mock_log_table = MagicMock()
    mock_log_table.get_item.return_value = {
        "Item": {
            "useCaseId": "UC-001",
            "certCode": "CERT-123",
            "pendingApprovals": 0,
            "status": "CANCELED",
            "emails": {
                "approver@example.com": {
                    "decision": "NO_CERTIFICATE",
                    "decisionDate": "2026-01-01T10:00:00"
                }
            }
        }
    }

    def table_side_effect(name):
        if name == "niif-qa-notification-certification":
            return mock_notif_table
        if name == "niif-qa-approval-audit-log":
            return mock_log_table
        return MagicMock()

    mock_aws["dynamo"].Table.side_effect = table_side_effect
    _setup_s3_configs(mock_aws["s3"], approvers=["approver@example.com", "boss@example.com"],
                      required=2)

    result = lambda_handler(event_certify, None)
    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    assert body["finalStatus"] == "NO_CERTIFICATE"


def test_already_voted_audit_status_pending(mock_aws, event_certify):
    """
    alreadyVoted con auditStatus=PENDING → finalStatus PENDING_APPROVERS.
    Cubre línea 481.
    """
    mock_notif_table = MagicMock()
    mock_notif_table.query.return_value = {
        "Items": [
            {
                "useCaseId": "UC-001",
                "executionId": "exec-001",
                "status": "PENDING_APPROVERS",
                "configBucket": "niif-qa-assets",
                "configKey": "primas-arl/configs/certification/primasArl.json"
            }
        ]
    }

    mock_log_table = MagicMock()
    mock_log_table.get_item.return_value = {
        "Item": {
            "useCaseId": "UC-001",
            "certCode": "CERT-123",
            "pendingApprovals": 1,
            "status": "PENDING",
            "emails": {
                "approver@example.com": {
                    "decision": "CERTIFICATE",
                    "decisionDate": "2026-01-01T10:00:00"
                }
            }
        }
    }

    def table_side_effect(name):
        if name == "niif-qa-notification-certification":
            return mock_notif_table
        if name == "niif-qa-approval-audit-log":
            return mock_log_table
        return MagicMock()

    mock_aws["dynamo"].Table.side_effect = table_side_effect
    _setup_s3_configs(mock_aws["s3"], approvers=["approver@example.com", "boss@example.com"],
                      required=2)

    result = lambda_handler(event_certify, None)
    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    assert body["finalStatus"] == "PENDING_APPROVERS"


def test_already_voted_audit_status_unknown_uses_resolve(mock_aws, event_certify):
    """
    alreadyVoted con auditStatus desconocido → usa resolve_final_status.
    Cubre línea 485.
    """
    mock_notif_table = MagicMock()
    mock_notif_table.query.return_value = {
        "Items": [
            {
                "useCaseId": "UC-001",
                "executionId": "exec-001",
                "status": "PENDING_APPROVERS",
                "configBucket": "niif-qa-assets",
                "configKey": "primas-arl/configs/certification/primasArl.json"
            }
        ]
    }

    mock_log_table = MagicMock()
    mock_log_table.get_item.return_value = {
        "Item": {
            "useCaseId": "UC-001",
            "certCode": "CERT-123",
            "pendingApprovals": 0,
            "status": "UNKNOWN_STATUS",
            "emails": {
                "approver@example.com": {
                    "decision": "CERTIFICATE",
                    "decisionDate": "2026-01-01T10:00:00"
                }
            }
        }
    }

    def table_side_effect(name):
        if name == "niif-qa-notification-certification":
            return mock_notif_table
        if name == "niif-qa-approval-audit-log":
            return mock_log_table
        return MagicMock()

    mock_aws["dynamo"].Table.side_effect = table_side_effect
    _setup_s3_configs(mock_aws["s3"], approvers=["approver@example.com", "boss@example.com"],
                      required=2)

    result = lambda_handler(event_certify, None)
    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    # pendingApprovals=0 → resolve_final_status retorna la existingDecision
    assert body["finalStatus"] == "CERTIFICATE"


def test_update_item_condition_exception_returns_500(mock_aws, event_certify):
    """
    update_item lanza excepción durante el loop → capturado por except → 500.
    Cubre líneas 561-563.
    """
    from botocore.exceptions import ClientError

    mock_notif_table = MagicMock()
    mock_notif_table.query.return_value = {
        "Items": [
            {
                "useCaseId": "UC-001",
                "executionId": "exec-001",
                "status": "PENDING",
                "configBucket": "niif-qa-assets",
                "configKey": "primas-arl/configs/certification/primasArl.json"
            }
        ]
    }
    mock_notif_table.update_item.side_effect = Exception("DynamoDB error inesperado")

    mock_log_table = MagicMock()
    mock_log_table.get_item.return_value = {}

    def table_side_effect(name):
        if name == "niif-qa-notification-certification":
            return mock_notif_table
        if name == "niif-qa-approval-audit-log":
            return mock_log_table
        return MagicMock()

    mock_aws["dynamo"].Table.side_effect = table_side_effect
    _setup_s3_configs(mock_aws["s3"], approvers=["approver@example.com"], required=1)

    result = lambda_handler(event_certify, None)
    assert result["statusCode"] == 500
    body = json.loads(result["body"])
    assert "Error interno" in body["message"]
    mock_log_table = MagicMock()
    mock_log_table.get_item.return_value = {}

    def table_side_effect(name):
        if name == "niif-qa-notification-certification":
            return mock_notif_table
        if name == "niif-qa-approval-audit-log":
            return mock_log_table
        return MagicMock()

    mock_aws["dynamo"].Table.side_effect = table_side_effect
    _setup_s3_configs(mock_aws["s3"], approvers=["approver@example.com"], required=1)

    result = lambda_handler(event_certify, None)
    assert result["statusCode"] == 500
    body = json.loads(result["body"])
    assert "Error interno" in body["message"]
