import boto3
import pytest
from botocore.stub import Stubber

from constructores import construir_registro, construir_registro_compuesto
from notificacion_certificacion.adaptadores.salida.dynamo_compuesto import RepositorioCompuestoDynamo
from notificacion_certificacion.adaptadores.salida.dynamo_enlaces import RepositorioEnlacesDynamo
from notificacion_certificacion.adaptadores.salida.dynamo_notificaciones import RepositorioNotificacionesDynamo
from notificacion_certificacion.adaptadores.salida.dynamo_tipos import deserializar, serializar
from notificacion_certificacion.dominio.errores import ErrorConcurrencia, ErrorConfiguracion
from notificacion_certificacion.dominio.modelos import EnlaceCorto, EnvioRealizado, ReservaCompuesta


def _cliente():
    cliente = boto3.client("dynamodb", region_name="us-east-1")
    return cliente, Stubber(cliente)


def _item(**extra):
    base = {
        "useCaseId": {"S": "casoA"},
        "executionId": {"S": "ej1"},
        "period": {"S": "202610"},
        "status": {"S": "PENDING"},
        "userEmail": {"S": "usuario@example.com"},
        "queryName": {"S": "detail#a"},
        "nameFile": {"S": "a.csv"},
        "s3_url": {"S": "https://s3.example/a.csv"},
        "sinDatosPeriodo": {"BOOL": True},
    }
    base.update(extra)
    return base


def _consulta_esperada(**extra):
    base = {
        "TableName": "tabla",
        "IndexName": "indice",
        "KeyConditionExpression": "certCode = :cert",
        "ExpressionAttributeValues": {":cert": {"S": "CERT1"}},
    }
    base.update(extra)
    return base


def test_serializar_y_deserializar_son_inversos():
    valores = {"texto": "a", "entero": 3, "bandera": True, "lista": ["x", "y"]}
    assert deserializar(serializar(valores))["texto"] == "a"
    assert serializar({"n": 3}) == {"n": {"N": "3"}}


def test_buscar_por_cert_code_filtra_el_caso_de_uso_y_mapea_los_campos():
    cliente, stub = _cliente()
    stub.add_response(
        "query",
        {"Items": [_item(), _item(useCaseId={"S": "otro"}, executionId={"S": "ej9"})]},
        _consulta_esperada(),
    )
    with stub:
        registros = RepositorioNotificacionesDynamo(cliente).buscar_por_cert_code("tabla", "indice", "CERT1", "casoA")

    assert registros == (
        construir_registro(
            estado="PENDING",
            correo_usuario="usuario@example.com",
            url_s3="https://s3.example/a.csv",
            sin_datos_periodo=True,
        ),
    )


def test_buscar_por_cert_code_pagina_hasta_agotar_los_resultados():
    cliente, stub = _cliente()
    llave = {"useCaseId": {"S": "casoA"}, "executionId": {"S": "ej1"}}
    stub.add_response("query", {"Items": [_item()], "LastEvaluatedKey": llave}, _consulta_esperada())
    stub.add_response(
        "query",
        {"Items": [_item(executionId={"S": "ej2"})]},
        _consulta_esperada(ExclusiveStartKey=llave),
    )
    with stub:
        registros = RepositorioNotificacionesDynamo(cliente).buscar_por_cert_code("tabla", "indice", "CERT1", "casoA")

    assert [registro.id_ejecucion for registro in registros] == ["ej1", "ej2"]


def test_buscar_por_cert_code_usa_valores_por_defecto_en_campos_opcionales():
    cliente, stub = _cliente()
    minimo = {"useCaseId": {"S": "casoA"}, "executionId": {"S": "ej1"}, "period": {"S": "202610"}}
    stub.add_response("query", {"Items": [minimo]}, _consulta_esperada())
    with stub:
        (registro,) = RepositorioNotificacionesDynamo(cliente).buscar_por_cert_code("tabla", "indice", "CERT1", "casoA")

    assert (registro.estado, registro.nombre_archivo, registro.sin_datos_periodo) == ("", "", False)


def test_buscar_por_cert_code_rechaza_registros_sin_campos_obligatorios():
    cliente, stub = _cliente()
    sin_periodo = {"useCaseId": {"S": "casoA"}, "executionId": {"S": "ej1"}}
    stub.add_response("query", {"Items": [sin_periodo]}, _consulta_esperada())
    with stub, pytest.raises(ErrorConfiguracion) as error:
        RepositorioNotificacionesDynamo(cliente).buscar_por_cert_code("tabla", "indice", "CERT1", "casoA")
    assert "period" in error.value.mensaje


def test_marcar_enviados_actualiza_cada_registro_con_los_datos_del_envio():
    cliente, stub = _cliente()
    esperado = {
        "TableName": "tabla",
        "Key": {"useCaseId": {"S": "casoA"}, "executionId": {"S": "ej1"}},
        "UpdateExpression": (
            "SET #st = :s, sentDate = :d, notificationChannel = :ch, "
            "sesMessageId = :mid, emailDestination = :dest"
        ),
        "ExpressionAttributeNames": {"#st": "status"},
        "ExpressionAttributeValues": {
            ":s": {"S": "SENT"},
            ":d": {"S": "2026-11-04T10:00:00-05:00"},
            ":ch": {"S": "SES_EMAIL"},
            ":mid": {"S": "m1,m2"},
            ":dest": {"L": [{"S": "a@example.com"}, {"S": "b@example.com"}]},
        },
    }
    segundo = {**esperado, "Key": {"useCaseId": {"S": "casoA"}, "executionId": {"S": "ej2"}}}
    stub.add_response("update_item", {}, esperado)
    stub.add_response("update_item", {}, segundo)
    envios = [EnvioRealizado("a@example.com", "m1"), EnvioRealizado("b@example.com", "m2")]
    with stub:
        RepositorioNotificacionesDynamo(cliente).marcar_enviados(
            "tabla",
            [construir_registro(), construir_registro(id_ejecucion="ej2")],
            "SENT",
            "SES_EMAIL",
            envios,
            "2026-11-04T10:00:00-05:00",
        )
    stub.assert_no_pending_responses()


def test_marcar_enviados_traduce_los_errores_de_aws():
    cliente, stub = _cliente()
    stub.add_client_error("update_item", service_error_code="AccessDeniedException")
    with stub, pytest.raises(Exception) as error:
        RepositorioNotificacionesDynamo(cliente).marcar_enviados(
            "tabla", [construir_registro()], "SENT", "SES_EMAIL", [EnvioRealizado("a@x.com", "m")], "f"
        )
    assert error.value.codigo == "NTF-PER-001"


def _enlace():
    return EnlaceCorto("tk000001", "https://largo.example/x", "casoA", "CERT1", "CERTIFICATE", "2026-11-04T10:00:00", 1790000000)


def test_guardar_enlace_escribe_el_item_solo_si_el_token_no_existe():
    cliente, stub = _cliente()
    stub.add_response(
        "put_item",
        {},
        {
            "TableName": "enlaces",
            "Item": {
                "token": {"S": "tk000001"},
                "url": {"S": "https://largo.example/x"},
                "useCaseId": {"S": "casoA"},
                "certCode": {"S": "CERT1"},
                "action": {"S": "CERTIFICATE"},
                "creationDate": {"S": "2026-11-04T10:00:00"},
                "expiresAt": {"N": "1790000000"},
            },
            "ConditionExpression": "attribute_not_exists(#tk)",
            "ExpressionAttributeNames": {"#tk": "token"},
        },
    )
    with stub:
        RepositorioEnlacesDynamo(cliente).guardar("enlaces", _enlace())
    stub.assert_no_pending_responses()


def test_guardar_enlace_con_token_repetido_falla_como_concurrencia():
    cliente, stub = _cliente()
    stub.add_client_error("put_item", service_error_code="ConditionalCheckFailedException")
    with stub, pytest.raises(ErrorConcurrencia):
        RepositorioEnlacesDynamo(cliente).guardar("enlaces", _enlace())


def _llave_compuesto(miembro: str) -> dict:
    return {"compositeId": {"S": "grupo#202610"}, "memberId": {"S": miembro}}


def test_obtener_miembro_mapea_el_item_conservando_los_tipos_originales():
    cliente, stub = _cliente()
    item = {
        "status": {"S": "READY_TO_NOTIFY"},
        "period": {"S": "202610"},
        "sourceExecutionId": {"S": "origen-1"},
        "loteCargueId": {"N": "7"},
        "updatedAt": {"S": "2026-11-04T10:00:00"},
    }
    stub.add_response(
        "get_item",
        {"Item": item},
        {"TableName": "t", "Key": _llave_compuesto("m1"), "ConsistentRead": True},
    )
    with stub:
        registro = RepositorioCompuestoDynamo(cliente).obtener("t", "grupo#202610", "m1")

    assert registro.estado == "READY_TO_NOTIFY"
    assert registro.id_lote == 7
    assert registro.cert_code == ""


def test_obtener_miembro_inexistente_devuelve_none():
    cliente, stub = _cliente()
    stub.add_response("get_item", {}, {"TableName": "t", "Key": _llave_compuesto("m1"), "ConsistentRead": True})
    with stub:
        assert RepositorioCompuestoDynamo(cliente).obtener("t", "grupo#202610", "m1") is None


def _reserva():
    return ReservaCompuesta(
        tabla="t",
        id_compuesto="grupo#202610",
        cert_code="CERT1",
        control=construir_registro_compuesto(id_miembro="_CONTROL", estado="NOTIFICATION_IN_PROGRESS"),
        miembros=(construir_registro_compuesto(id_miembro="m1"),),
        ya_completada=False,
    )


def test_consumir_actualiza_miembros_y_control_en_una_sola_transaccion():
    cliente, stub = _cliente()
    miembro = {
        "Update": {
            "TableName": "t",
            "Key": _llave_compuesto("m1"),
            "UpdateExpression": (
                "SET #st = :consumed, notificationReleased = :yes, completedAt = :now, "
                "updatedAt = :now, certCode = :cert, sesMessageId = :mid"
            ),
            "ConditionExpression": (
                "#st = :ready AND sourceExecutionId = :exec AND loteCargueId = :lot AND updatedAt = :previous"
            ),
            "ExpressionAttributeNames": {"#st": "status"},
            "ExpressionAttributeValues": {
                ":consumed": {"S": "CONSUMED"},
                ":yes": {"BOOL": True},
                ":now": {"S": "2026-11-04T15:00:00+00:00"},
                ":cert": {"S": "CERT1"},
                ":mid": {"S": "m1,m2"},
                ":ready": {"S": "READY_TO_NOTIFY"},
                ":exec": {"S": "origen-1"},
                ":lot": {"S": "lote-1"},
                ":previous": {"S": "2026-11-04T10:00:00"},
            },
        }
    }
    control = {
        "Update": {
            "TableName": "t",
            "Key": _llave_compuesto("_CONTROL"),
            "UpdateExpression": (
                "SET #st = :done, completedAt = :now, updatedAt = :now, certCode = :cert, sesMessageId = :mid"
            ),
            "ConditionExpression": "#st = :progress AND updatedAt = :previous",
            "ExpressionAttributeNames": {"#st": "status"},
            "ExpressionAttributeValues": {
                ":done": {"S": "NOTIFICATION_COMPLETED"},
                ":now": {"S": "2026-11-04T15:00:00+00:00"},
                ":cert": {"S": "CERT1"},
                ":mid": {"S": "m1,m2"},
                ":progress": {"S": "NOTIFICATION_IN_PROGRESS"},
                ":previous": {"S": "2026-11-04T10:00:00"},
            },
        }
    }
    stub.add_response("transact_write_items", {}, {"TransactItems": [miembro, control]})
    with stub:
        RepositorioCompuestoDynamo(cliente).consumir(_reserva(), "m1,m2", "2026-11-04T15:00:00+00:00")
    stub.assert_no_pending_responses()


def test_consumir_con_condicion_incumplida_falla_como_concurrencia():
    cliente, stub = _cliente()
    stub.add_client_error("transact_write_items", service_error_code="TransactionCanceledException")
    with stub, pytest.raises(ErrorConcurrencia):
        RepositorioCompuestoDynamo(cliente).consumir(_reserva(), "m1", "fecha")
