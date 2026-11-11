# -*- coding: utf-8 -*-
import os
import json
import pytest
from unittest.mock import patch, MagicMock, call
from botocore.exceptions import ClientError

# ============================================================
# Variables de entorno requeridas por el modulo
# ============================================================
os.environ["TABLE_NAME"] = "niif-qa-composite-tracker"
os.environ["CONFIG_BUCKET"] = "niif-qa-assets"
os.environ["COMPOSITE_GROUPS_KEY"] = "configs/composite/composite_groups.json"
os.environ["GLOBAL_CONFIG_KEY"] = "configs/certification/json_global_notif_cert.json"
os.environ["GLUE_JOB_NAME"] = "niif-qa-notification-job"
os.environ["SNS_TOPIC_ARN"] = "arn:aws:sns:us-east-1:123456789012:niif-qa-topic"
os.environ["REDIRECT_BASE_URL"] = "https://niif.example.com/r"
os.environ["SHORT_LINKS_TABLE"] = "niif-qa-short-links"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
os.environ["AWS_REGION"] = "us-east-1"

# Mock boto3 antes de importar el modulo para evitar llamadas reales a AWS
with patch("boto3.resource"), patch("boto3.client"):
    import composite_notification_evaluator as cne
    from composite_notification_evaluator import lambda_handler


# ============================================================
# Helpers
# ============================================================

def _s3_body(obj):
    """Simula la respuesta de s3.get_object con un JSON."""
    body = MagicMock()
    body.read.return_value = json.dumps(obj).encode("utf-8")
    return {"Body": body}


def _composite_groups_cfg(
    group_id="GRP#001#UC",
    members=None,
    required=2,
    notification_config_key="configs/composite/notif.json",
    notification_use_case_id="UC-COMPOSITE"
):
    """Genera una config de grupos compuestos estandar para tests."""
    if members is None:
        members = [
            {"useCaseId": "UC-001"},
            {"useCaseId": "UC-002"}
        ]
    return {
        "groups": [
            {
                "grupoId": group_id,
                "nombreGrupo": "Grupo Test",
                "status": "ACTIVE",
                "members": members,
                "notificationConfigBucket": "niif-qa-assets",
                "notificationConfigKey": notification_config_key,
                "notificationUseCaseId": notification_use_case_id
            }
        ]
    }


@pytest.fixture
def mock_aws(monkeypatch):
    """
    Mock centralizado de dynamodb, s3 y glue a nivel de modulo.
    La tabla global `table` tambien se parchea directamente.
    """
    mock_dynamo = MagicMock()
    mock_s3 = MagicMock()
    mock_glue = MagicMock()
    mock_table = MagicMock()

    monkeypatch.setattr(cne, "dynamodb", mock_dynamo)
    monkeypatch.setattr(cne, "s3", mock_s3)
    monkeypatch.setattr(cne, "glue", mock_glue)
    monkeypatch.setattr(cne, "table", mock_table)

    return {
        "dynamo": mock_dynamo,
        "s3": mock_s3,
        "glue": mock_glue,
        "table": mock_table
    }


# ============================================================
# Tests: lambda_handler - rutas de entrada
# ============================================================

def test_handler_stream_event_returns_skipped():
    """Evento DynamoDB Stream es ignorado: retorna STREAM_SKIPPED."""
    event = {
        "Records": [
            {"eventName": "MODIFY", "dynamodb": {"NewImage": {}}}
        ]
    }
    result = lambda_handler(event, None)
    assert result["statusCode"] == 200
    assert result["body"] == "STREAM_SKIPPED"


def test_handler_unknown_event_returns_ok():
    """Evento sin mode ni Records retorna OK generico."""
    result = lambda_handler({}, None)
    assert result["statusCode"] == 200
    assert result["body"] == "OK"


# ============================================================
# Tests: RESOLVE_NOTIFICATION - modo simple (sin grupo compuesto)
# ============================================================

def test_resolve_notification_simple_mode(mock_aws):
    """
    useCaseId no pertenece a ningun grupo compuesto:
    retorna notificationMode=SIMPLE con shouldStartCertification=True.
    """
    # S3 devuelve config sin grupos que contengan UC-SIMPLE
    mock_aws["s3"].get_object.return_value = _s3_body({"groups": []})

    event = {
        "mode": "RESOLVE_NOTIFICATION",
        "useCaseId": "UC-SIMPLE",
        "period": "2026-01",
        "loteCargueId": "LOTE-001",
        "configBucket": "niif-qa-assets",
        "configKey": "configs/uc-simple.json"
    }

    result = lambda_handler(event, None)

    assert result["notificationMode"] == "SIMPLE"
    assert result["shouldStartCertification"] is True
    assert result["useCaseId"] == "UC-SIMPLE"
    assert result["period"] == "2026-01"


def test_resolve_notification_missing_use_case_id_raises(mock_aws):
    """Falta useCaseId: debe lanzar ValueError y retornar error 500."""
    mock_aws["s3"].get_object.return_value = _s3_body({"groups": []})

    event = {
        "mode": "RESOLVE_NOTIFICATION",
        "period": "2026-01",
        "configKey": "configs/uc.json"
    }

    # El lambda_handler captura la excepcion internamente --> no hay try/except aqui,
    # se propaga. Verificamos que la funcion lanza el error.
    with pytest.raises(ValueError, match="useCaseId"):
        lambda_handler(event, None)


def test_resolve_notification_missing_period_raises(mock_aws):
    """Falta period: debe lanzar ValueError."""
    mock_aws["s3"].get_object.return_value = _s3_body({"groups": []})

    event = {
        "mode": "RESOLVE_NOTIFICATION",
        "useCaseId": "UC-001",
        "configKey": "configs/uc.json"
    }

    with pytest.raises(ValueError, match="period"):
        lambda_handler(event, None)


def test_resolve_notification_missing_config_key_raises(mock_aws):
    """Falta configKey: debe lanzar ValueError."""
    mock_aws["s3"].get_object.return_value = _s3_body({"groups": []})

    event = {
        "mode": "RESOLVE_NOTIFICATION",
        "useCaseId": "UC-001",
        "period": "2026-01"
    }

    with pytest.raises(ValueError, match="configKey"):
        lambda_handler(event, None)


# ============================================================
# Tests: RESOLVE_NOTIFICATION - modo compuesto, esperando miembros
# ============================================================

def test_resolve_notification_composite_waiting(mock_aws):
    """
    UC-001 pertenece a un grupo de 2 miembros.
    Solo UC-001 esta READY: retorna COMPOSITE_WAITING.
    """
    groups_cfg = _composite_groups_cfg(
        group_id="GRP#001#UC",
        members=[{"useCaseId": "UC-001"}, {"useCaseId": "UC-002"}]
    )
    mock_aws["s3"].get_object.return_value = _s3_body(groups_cfg)

    # Solo UC-001 en la tabla (UC-002 todavia no llego)
    mock_aws["table"].query.return_value = {
        "Items": [
            {"compositeId": "GRP#001#UC#2026-01", "memberId": "UC-001", "status": "READY_TO_NOTIFY"}
        ]
    }

    event = {
        "mode": "RESOLVE_NOTIFICATION",
        "useCaseId": "UC-001",
        "period": "2026-01",
        "loteCargueId": "LOTE-001",
        "configBucket": "niif-qa-assets",
        "configKey": "configs/uc-001.json"
    }

    result = lambda_handler(event, None)

    assert result["notificationMode"] == "COMPOSITE_WAITING"
    assert result["shouldStartCertification"] is False
    assert result["compositeId"] == "GRP#001#UC#2026-01"
    # Se registro el miembro como READY_TO_NOTIFY
    mock_aws["table"].update_item.assert_called_once()


def test_resolve_notification_composite_ready_starts_job(mock_aws):
    """
    Todos los miembros estan READY y el lock se obtiene:
    retorna COMPOSITE_READY con shouldStartCertification=True.
    En este modo la Step Function es quien dispara el Glue job,
    no la Lambda directamente.
    """
    groups_cfg = _composite_groups_cfg(
        group_id="GRP#001#UC",
        members=[{"useCaseId": "UC-001"}, {"useCaseId": "UC-002"}]
    )
    mock_aws["s3"].get_object.return_value = _s3_body(groups_cfg)

    # Ambos miembros listos en la tabla
    mock_aws["table"].query.return_value = {
        "Items": [
            {"compositeId": "GRP#001#UC#2026-01", "memberId": "UC-001", "status": "READY_TO_NOTIFY"},
            {"compositeId": "GRP#001#UC#2026-01", "memberId": "UC-002", "status": "READY_TO_NOTIFY"}
        ]
    }

    # Lock exitoso (put_item no lanza excepcion)
    mock_aws["table"].put_item.return_value = {}

    event = {
        "mode": "RESOLVE_NOTIFICATION",
        "useCaseId": "UC-001",
        "period": "2026-01",
        "loteCargueId": "LOTE-001",
        "configBucket": "niif-qa-assets",
        "configKey": "configs/uc-001.json"
    }

    result = lambda_handler(event, None)

    assert result["notificationMode"] == "COMPOSITE_READY"
    assert result["shouldStartCertification"] is True
    assert result["compositeId"] == "GRP#001#UC#2026-01"
    assert result["compositeGroupId"] == "GRP#001#UC"
    assert result["compositeGroupName"] == "Grupo Test"
    # El lock debe haberse creado
    mock_aws["table"].put_item.assert_called_once()
    # El miembro fue registrado como READY_TO_NOTIFY (update_item)
    mock_aws["table"].update_item.assert_called_once()


def test_resolve_notification_composite_already_locked(mock_aws):
    """
    Todos los miembros listos pero el lock ya existe:
    retorna COMPOSITE_ALREADY_LOCKED.
    """
    groups_cfg = _composite_groups_cfg(
        group_id="GRP#001#UC",
        members=[{"useCaseId": "UC-001"}, {"useCaseId": "UC-002"}]
    )
    mock_aws["s3"].get_object.return_value = _s3_body(groups_cfg)

    mock_aws["table"].query.return_value = {
        "Items": [
            {"compositeId": "GRP#001#UC#2026-01", "memberId": "UC-001", "status": "READY_TO_NOTIFY"},
            {"compositeId": "GRP#001#UC#2026-01", "memberId": "UC-002", "status": "READY_TO_NOTIFY"}
        ]
    }

    # Simular que el lock ya existe (ConditionalCheckFailedException)
    error_response = {"Error": {"Code": "ConditionalCheckFailedException", "Message": "exists"}}
    mock_aws["table"].put_item.side_effect = ClientError(error_response, "PutItem")

    event = {
        "mode": "RESOLVE_NOTIFICATION",
        "useCaseId": "UC-001",
        "period": "2026-01",
        "loteCargueId": "LOTE-001",
        "configBucket": "niif-qa-assets",
        "configKey": "configs/uc-001.json"
    }

    result = lambda_handler(event, None)

    assert result["notificationMode"] == "COMPOSITE_ALREADY_LOCKED"
    assert result["shouldStartCertification"] is False
    # No se disparo el job de Glue
    mock_aws["glue"].start_job_run.assert_not_called()


# ============================================================
# Tests: helpers internos
# ============================================================

def test_get_group_id_from_composite_id_valid():
    """Extrae el group_id correctamente de un compositeId con 3+ partes."""
    result = cne.get_group_id_from_composite_id("GRP#001#UC#2026-01")
    assert result == "GRP#001#UC"


def test_get_group_id_from_composite_id_invalid():
    """compositeId con menos de 3 partes lanza ValueError."""
    with pytest.raises(ValueError, match="compositeId"):
        cne.get_group_id_from_composite_id("GRP#001")


def test_find_group_returns_active_group():
    """find_group devuelve el grupo activo que coincide con el grupoId."""
    groups_cfg = _composite_groups_cfg(group_id="GRP#001#UC")
    result = cne.find_group(groups_cfg, "GRP#001#UC")
    assert result is not None
    assert result["grupoId"] == "GRP#001#UC"


def test_find_group_returns_none_for_inactive():
    """find_group ignora grupos con status INACTIVE."""
    groups_cfg = {
        "groups": [
            {"grupoId": "GRP#001#UC", "status": "INACTIVE", "members": []}
        ]
    }
    result = cne.find_group(groups_cfg, "GRP#001#UC")
    assert result is None


def test_find_group_returns_none_when_not_found():
    """find_group retorna None si no existe el grupoId."""
    groups_cfg = _composite_groups_cfg(group_id="GRP#OTHER#UC")
    result = cne.find_group(groups_cfg, "GRP#001#UC")
    assert result is None


def test_all_members_ready_true():
    """all_members_ready retorna True cuando todos los miembros estan READY_TO_NOTIFY."""
    expected = [{"useCaseId": "UC-001"}, {"useCaseId": "UC-002"}]
    items = [
        {"memberId": "UC-001", "status": "READY_TO_NOTIFY"},
        {"memberId": "UC-002", "status": "READY_TO_NOTIFY"}
    ]
    assert cne.all_members_ready(expected, items) is True


def test_all_members_ready_false_missing_member():
    """all_members_ready retorna False cuando falta un miembro."""
    expected = [{"useCaseId": "UC-001"}, {"useCaseId": "UC-002"}]
    items = [
        {"memberId": "UC-001", "status": "READY_TO_NOTIFY"}
    ]
    assert cne.all_members_ready(expected, items) is False


def test_all_members_ready_false_wrong_status():
    """all_members_ready retorna False cuando un miembro no tiene status READY_TO_NOTIFY."""
    expected = [{"useCaseId": "UC-001"}, {"useCaseId": "UC-002"}]
    items = [
        {"memberId": "UC-001", "status": "READY_TO_NOTIFY"},
        {"memberId": "UC-002", "status": "PENDING"}
    ]
    assert cne.all_members_ready(expected, items) is False


def test_create_control_lock_success(mock_aws):
    """create_control_lock retorna True cuando el put_item es exitoso."""
    mock_aws["table"].put_item.return_value = {}
    result = cne.create_control_lock("GRP#001#UC#2026-01", "GRP#001#UC", "Grupo Test")
    assert result is True
    mock_aws["table"].put_item.assert_called_once()


def test_create_control_lock_already_exists(mock_aws):
    """create_control_lock retorna False cuando el lock ya existe."""
    error_response = {"Error": {"Code": "ConditionalCheckFailedException", "Message": "exists"}}
    mock_aws["table"].put_item.side_effect = ClientError(error_response, "PutItem")

    result = cne.create_control_lock("GRP#001#UC#2026-01", "GRP#001#UC", "Grupo Test")
    assert result is False


def test_find_group_for_use_case_found():
    """find_group_for_use_case encuentra el grupo que contiene el useCaseId."""
    groups_cfg = _composite_groups_cfg(
        group_id="GRP#001#UC",
        members=[{"useCaseId": "UC-001"}, {"useCaseId": "UC-002"}]
    )
    result = cne.find_group_for_use_case(groups_cfg, "UC-002")
    assert result is not None
    assert result["grupoId"] == "GRP#001#UC"


def test_find_group_for_use_case_not_found():
    """find_group_for_use_case retorna None cuando el useCaseId no esta en ningun grupo."""
    groups_cfg = _composite_groups_cfg(
        group_id="GRP#001#UC",
        members=[{"useCaseId": "UC-001"}]
    )
    result = cne.find_group_for_use_case(groups_cfg, "UC-999")
    assert result is None


def test_query_members_excludes_control(mock_aws):
    """query_members excluye el item con memberId=_CONTROL."""
    mock_aws["table"].query.return_value = {
        "Items": [
            {"compositeId": "GRP#001#UC#2026-01", "memberId": "_CONTROL", "status": "NOTIFICATION_IN_PROGRESS"},
            {"compositeId": "GRP#001#UC#2026-01", "memberId": "UC-001", "status": "READY_TO_NOTIFY"}
        ]
    }
    result = cne.query_members("GRP#001#UC#2026-01")
    assert len(result) == 1
    assert result[0]["memberId"] == "UC-001"


# ============================================================
# Tests: unmarshall_image
# ============================================================

def test_unmarshall_image():
    """unmarshall_image convierte correctamente una imagen DynamoDB."""
    image = {
        "compositeId": {"S": "GRP#001#UC#2026-01"},
        "memberId": {"S": "UC-001"},
        "status": {"S": "READY_TO_NOTIFY"}
    }
    result = cne.unmarshall_image(image)
    assert result["compositeId"] == "GRP#001#UC#2026-01"
    assert result["memberId"] == "UC-001"
    assert result["status"] == "READY_TO_NOTIFY"


# ============================================================
# Tests: create_control_lock - escenarios avanzados
# ============================================================

def test_create_control_lock_existing_in_progress_not_expired(mock_aws):
    """
    Si existe un control con status NOTIFICATION_IN_PROGRESS y no ha expirado,
    retorna False (skip).
    """
    from datetime import datetime as dt

    recent_time = dt.now().strftime("%Y-%m-%dT%H:%M:%S")

    mock_aws["table"].get_item.return_value = {
        "Item": {
            "compositeId": "GRP#001#UC#2026-01",
            "memberId": "_CONTROL",
            "status": "NOTIFICATION_IN_PROGRESS",
            "updatedAt": recent_time,
            "createdAt": recent_time
        }
    }

    result = cne.create_control_lock("GRP#001#UC#2026-01", "GRP#001#UC", "Grupo Test")
    assert result is False
    # No se debe intentar crear un nuevo lock
    mock_aws["table"].put_item.assert_not_called()


def test_create_control_lock_existing_in_progress_expired(mock_aws):
    """
    Si existe un control con status NOTIFICATION_IN_PROGRESS y ya expiro,
    se elimina y se crea uno nuevo.
    """
    # Fecha de hace 2 horas (definitivamente expirada con timeout_minutes=30)
    expired_time = "2020-01-01T00:00:00"

    mock_aws["table"].get_item.return_value = {
        "Item": {
            "compositeId": "GRP#001#UC#2026-01",
            "memberId": "_CONTROL",
            "status": "NOTIFICATION_IN_PROGRESS",
            "updatedAt": expired_time,
            "createdAt": expired_time
        }
    }
    mock_aws["table"].put_item.return_value = {}

    result = cne.create_control_lock("GRP#001#UC#2026-01", "GRP#001#UC", "Grupo Test")
    assert result is True
    mock_aws["table"].delete_item.assert_called_once()
    mock_aws["table"].put_item.assert_called_once()


def test_create_control_lock_existing_in_progress_invalid_date(mock_aws):
    """
    Si el updatedAt tiene formato invalido, se considera expirado.
    """
    mock_aws["table"].get_item.return_value = {
        "Item": {
            "compositeId": "GRP#001#UC#2026-01",
            "memberId": "_CONTROL",
            "status": "NOTIFICATION_IN_PROGRESS",
            "updatedAt": "INVALID-DATE",
            "createdAt": "INVALID-DATE"
        }
    }
    mock_aws["table"].put_item.return_value = {}

    result = cne.create_control_lock("GRP#001#UC#2026-01", "GRP#001#UC", "Grupo Test")
    assert result is True
    mock_aws["table"].delete_item.assert_called_once()


def test_create_control_lock_existing_completed(mock_aws):
    """
    Si existe un control con status NOTIFICATION_COMPLETED,
    se elimina y se crea uno nuevo.
    """
    mock_aws["table"].get_item.return_value = {
        "Item": {
            "compositeId": "GRP#001#UC#2026-01",
            "memberId": "_CONTROL",
            "status": "NOTIFICATION_COMPLETED",
            "updatedAt": "2026-01-01T00:00:00",
            "createdAt": "2026-01-01T00:00:00"
        }
    }
    mock_aws["table"].put_item.return_value = {}

    result = cne.create_control_lock("GRP#001#UC#2026-01", "GRP#001#UC", "Grupo Test")
    assert result is True
    mock_aws["table"].delete_item.assert_called_once()
    mock_aws["table"].put_item.assert_called_once()


def test_create_control_lock_existing_unknown_status(mock_aws):
    """
    Si existe un control con un status desconocido,
    se elimina y se crea uno nuevo (recovery).
    """
    mock_aws["table"].get_item.return_value = {
        "Item": {
            "compositeId": "GRP#001#UC#2026-01",
            "memberId": "_CONTROL",
            "status": "SOME_UNKNOWN_STATUS",
            "updatedAt": "2026-01-01T00:00:00",
            "createdAt": "2026-01-01T00:00:00"
        }
    }
    mock_aws["table"].put_item.return_value = {}

    result = cne.create_control_lock("GRP#001#UC#2026-01", "GRP#001#UC", "Grupo Test")
    assert result is True
    mock_aws["table"].delete_item.assert_called_once()


def test_create_control_lock_no_existing_control(mock_aws):
    """
    Si no existe control previo, se crea directamente.
    """
    mock_aws["table"].get_item.return_value = {}
    mock_aws["table"].put_item.return_value = {}

    result = cne.create_control_lock("GRP#001#UC#2026-01", "GRP#001#UC", "Grupo Test")
    assert result is True
    mock_aws["table"].delete_item.assert_not_called()
    mock_aws["table"].put_item.assert_called_once()


def test_create_control_lock_race_condition_after_delete(mock_aws):
    """
    Si existe un control expirado, se elimina, pero al hacer put_item
    otro proceso ya lo creo (ConditionalCheckFailedException).
    """
    mock_aws["table"].get_item.return_value = {
        "Item": {
            "compositeId": "GRP#001#UC#2026-01",
            "memberId": "_CONTROL",
            "status": "NOTIFICATION_COMPLETED",
            "updatedAt": "2026-01-01T00:00:00",
            "createdAt": "2026-01-01T00:00:00"
        }
    }
    error_response = {"Error": {"Code": "ConditionalCheckFailedException", "Message": "exists"}}
    mock_aws["table"].put_item.side_effect = ClientError(error_response, "PutItem")

    result = cne.create_control_lock("GRP#001#UC#2026-01", "GRP#001#UC", "Grupo Test")
    assert result is False


def test_create_control_lock_unexpected_error_raises(mock_aws):
    """
    Si put_item lanza un error diferente a ConditionalCheckFailedException,
    se re-lanza.
    """
    mock_aws["table"].get_item.return_value = {}
    error_response = {"Error": {"Code": "InternalServerError", "Message": "unexpected"}}
    mock_aws["table"].put_item.side_effect = ClientError(error_response, "PutItem")

    with pytest.raises(ClientError):
        cne.create_control_lock("GRP#001#UC#2026-01", "GRP#001#UC", "Grupo Test")


# ============================================================
# Tests: start_composite_job
# ============================================================

def test_start_composite_job(mock_aws):
    """start_composite_job invoca glue.start_job_run y retorna el JobRunId."""
    mock_aws["glue"].start_job_run.return_value = {"JobRunId": "jr_123"}

    result = cne.start_composite_job(
        composite_id="GRP#001#UC#2026-01",
        group_id="GRP#001#UC",
        group_name="Grupo Test",
        period="2026-01",
        lote_cargue_id="LOTE-001",
        notification_config_bucket="niif-qa-assets",
        notification_config_key="configs/composite/notif.json",
        notification_use_case_id="UC-COMPOSITE"
    )

    assert result == "jr_123"
    mock_aws["glue"].start_job_run.assert_called_once()
    call_args = mock_aws["glue"].start_job_run.call_args
    assert call_args[1]["JobName"] == "niif-qa-notification-job"
    assert call_args[1]["Arguments"]["--COMPOSITE_MODE"] == "true"
    assert call_args[1]["Arguments"]["--COMPOSITE_ID"] == "GRP#001#UC#2026-01"


def test_start_composite_job_with_none_use_case_id(mock_aws):
    """start_composite_job maneja notification_use_case_id=None correctamente."""
    mock_aws["glue"].start_job_run.return_value = {"JobRunId": "jr_456"}

    result = cne.start_composite_job(
        composite_id="GRP#001#UC#2026-01",
        group_id="GRP#001#UC",
        group_name="Grupo Test",
        period="2026-01",
        lote_cargue_id="LOTE-001",
        notification_config_bucket="niif-qa-assets",
        notification_config_key="configs/composite/notif.json",
        notification_use_case_id=None
    )

    assert result == "jr_456"
    call_args = mock_aws["glue"].start_job_run.call_args
    assert call_args[1]["Arguments"]["--NOTIFICATION_USE_CASE_ID"] == ""


# ============================================================
# Tests: update_control_with_job_run
# ============================================================

def test_update_control_with_job_run(mock_aws):
    """update_control_with_job_run actualiza el item de control con el job run id."""
    cne.update_control_with_job_run("GRP#001#UC#2026-01", "jr_123")

    mock_aws["table"].update_item.assert_called_once()
    call_args = mock_aws["table"].update_item.call_args
    assert call_args[1]["Key"]["compositeId"] == "GRP#001#UC#2026-01"
    assert call_args[1]["Key"]["memberId"] == "_CONTROL"
    assert call_args[1]["ExpressionAttributeValues"][":j"] == "jr_123"


# ============================================================
# Tests: mark_members_in_progress
# ============================================================

def test_mark_members_in_progress(mock_aws):
    """mark_members_in_progress actualiza todos los miembros a NOTIFICATION_IN_PROGRESS."""
    items = [
        {"compositeId": "GRP#001#UC#2026-01", "memberId": "UC-001"},
        {"compositeId": "GRP#001#UC#2026-01", "memberId": "UC-002"}
    ]

    cne.mark_members_in_progress("GRP#001#UC#2026-01", items, "jr_123")

    assert mock_aws["table"].update_item.call_count == 2

    # Verificar que se actualizo cada miembro
    calls = mock_aws["table"].update_item.call_args_list
    member_ids_updated = [c[1]["Key"]["memberId"] for c in calls]
    assert "UC-001" in member_ids_updated
    assert "UC-002" in member_ids_updated


def test_mark_members_in_progress_empty_list(mock_aws):
    """mark_members_in_progress con lista vacia no hace nada."""
    cne.mark_members_in_progress("GRP#001#UC#2026-01", [], "jr_123")
    mock_aws["table"].update_item.assert_not_called()


# ============================================================
# Tests: process_ready_event
# ============================================================

def test_process_ready_event_not_ready_status(mock_aws):
    """process_ready_event no hace nada si status no es READY_TO_NOTIFY."""
    item = {"compositeId": "GRP#001#UC#2026-01", "status": "PENDING"}
    cne.process_ready_event(item)
    mock_aws["s3"].get_object.assert_not_called()


def test_process_ready_event_no_group(mock_aws):
    """process_ready_event retorna sin accion si no hay grupo activo."""
    mock_aws["s3"].get_object.return_value = _s3_body({"groups": []})

    item = {
        "compositeId": "GRP#001#UC#2026-01",
        "status": "READY_TO_NOTIFY",
        "grupoId": "GRP#001#UC"
    }
    cne.process_ready_event(item)
    # No se debe crear lock ni disparar job
    mock_aws["table"].put_item.assert_not_called()
    mock_aws["glue"].start_job_run.assert_not_called()


def test_process_ready_event_missing_notification_config_key(mock_aws):
    """process_ready_event lanza ValueError si el grupo no tiene notificationConfigKey."""
    groups_cfg = {
        "groups": [
            {
                "grupoId": "GRP#001#UC",
                "nombreGrupo": "Grupo Test",
                "status": "ACTIVE",
                "members": [{"useCaseId": "UC-001"}]
                # Sin notificationConfigKey
            }
        ]
    }
    mock_aws["s3"].get_object.return_value = _s3_body(groups_cfg)

    item = {
        "compositeId": "GRP#001#UC#2026-01",
        "status": "READY_TO_NOTIFY",
        "grupoId": "GRP#001#UC"
    }

    with pytest.raises(ValueError, match="notificationConfigKey"):
        cne.process_ready_event(item)


def test_process_ready_event_waiting_members(mock_aws):
    """process_ready_event retorna sin accion si faltan miembros."""
    groups_cfg = _composite_groups_cfg(
        group_id="GRP#001#UC",
        members=[{"useCaseId": "UC-001"}, {"useCaseId": "UC-002"}]
    )
    mock_aws["s3"].get_object.return_value = _s3_body(groups_cfg)

    # Solo un miembro esta listo
    mock_aws["table"].query.return_value = {
        "Items": [
            {"compositeId": "GRP#001#UC#2026-01", "memberId": "UC-001", "status": "READY_TO_NOTIFY"}
        ]
    }

    item = {
        "compositeId": "GRP#001#UC#2026-01",
        "status": "READY_TO_NOTIFY",
        "grupoId": "GRP#001#UC"
    }
    cne.process_ready_event(item)
    mock_aws["glue"].start_job_run.assert_not_called()


def test_process_ready_event_lock_fails(mock_aws):
    """process_ready_event retorna sin disparar job si el lock falla."""
    groups_cfg = _composite_groups_cfg(
        group_id="GRP#001#UC",
        members=[{"useCaseId": "UC-001"}]
    )
    mock_aws["s3"].get_object.return_value = _s3_body(groups_cfg)

    mock_aws["table"].query.return_value = {
        "Items": [
            {"compositeId": "GRP#001#UC#2026-01", "memberId": "UC-001", "status": "READY_TO_NOTIFY"}
        ]
    }

    # Simular lock activo (get_item devuelve control reciente)
    from datetime import datetime as dt
    recent_time = dt.now().strftime("%Y-%m-%dT%H:%M:%S")
    mock_aws["table"].get_item.return_value = {
        "Item": {
            "compositeId": "GRP#001#UC#2026-01",
            "memberId": "_CONTROL",
            "status": "NOTIFICATION_IN_PROGRESS",
            "updatedAt": recent_time
        }
    }

    item = {
        "compositeId": "GRP#001#UC#2026-01",
        "status": "READY_TO_NOTIFY",
        "grupoId": "GRP#001#UC"
    }
    cne.process_ready_event(item)
    mock_aws["glue"].start_job_run.assert_not_called()


def test_process_ready_event_full_success(mock_aws):
    """process_ready_event dispara el job cuando todos los miembros estan listos y se obtiene lock."""
    groups_cfg = _composite_groups_cfg(
        group_id="GRP#001#UC",
        members=[{"useCaseId": "UC-001"}]
    )
    mock_aws["s3"].get_object.return_value = _s3_body(groups_cfg)

    mock_aws["table"].query.return_value = {
        "Items": [
            {"compositeId": "GRP#001#UC#2026-01", "memberId": "UC-001", "status": "READY_TO_NOTIFY"}
        ]
    }

    # Lock exitoso: no hay control previo
    mock_aws["table"].get_item.return_value = {}
    mock_aws["table"].put_item.return_value = {}
    mock_aws["glue"].start_job_run.return_value = {"JobRunId": "jr_789"}

    item = {
        "compositeId": "GRP#001#UC#2026-01",
        "status": "READY_TO_NOTIFY",
        "grupoId": "GRP#001#UC",
        "period": "2026-01",
        "loteCargueId": "LOTE-001"
    }
    cne.process_ready_event(item)

    mock_aws["glue"].start_job_run.assert_called_once()
    # Se actualizo el control con el job run id y se marcaron los miembros
    assert mock_aws["table"].update_item.call_count >= 1


def test_process_ready_event_extracts_group_id_from_composite_id(mock_aws):
    """process_ready_event extrae el group_id del compositeId si no tiene grupoId."""
    groups_cfg = _composite_groups_cfg(
        group_id="GRP#001#UC",
        members=[{"useCaseId": "UC-001"}]
    )
    mock_aws["s3"].get_object.return_value = _s3_body(groups_cfg)

    mock_aws["table"].query.return_value = {
        "Items": [
            {"compositeId": "GRP#001#UC#2026-01", "memberId": "UC-001", "status": "READY_TO_NOTIFY"}
        ]
    }
    mock_aws["table"].get_item.return_value = {}
    mock_aws["table"].put_item.return_value = {}
    mock_aws["glue"].start_job_run.return_value = {"JobRunId": "jr_abc"}

    item = {
        "compositeId": "GRP#001#UC#2026-01",
        "status": "READY_TO_NOTIFY",
        # Sin grupoId - debe extraerlo del compositeId
        "period": "2026-01",
        "loteCargueId": "LOTE-002"
    }
    cne.process_ready_event(item)
    mock_aws["glue"].start_job_run.assert_called_once()
