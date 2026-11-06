"""Adaptadores de salida con clientes reales de boto3 y respuestas simuladas (Stubber)."""
import io
import json

import boto3
import pytest
from botocore.response import StreamingBody
from botocore.stub import ANY, Stubber

from configurador_mallas.adaptadores.salida import errores_aws
from configurador_mallas.adaptadores.salida.dynamo import (DynamoCnf, DynamoEjecuciones,
                                                           DynamoIdempotencia, DynamoLectura,
                                                           a_dynamo)
from configurador_mallas.adaptadores.salida.s3 import AlmacenS3, cargar_catalogo, separar_uri
from configurador_mallas.adaptadores.salida.stepfunctions import OrquestadorStepFunctions
from configurador_mallas.dominio.errores import (ErrorConcurrencia, ErrorConfiguracion,
                                                 ErrorOrquestacion, ErrorPermisos,
                                                 ErrorPlataforma)
from conftest import CATALOGO_DOC


@pytest.fixture(autouse=True)
def credenciales_falsas(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "prueba")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "prueba")


def _cliente(servicio):
    cliente = boto3.client(servicio, region_name="us-east-1")
    return cliente, Stubber(cliente)


def _cuerpo(texto):
    datos = texto.encode("utf-8")
    return StreamingBody(io.BytesIO(datos), len(datos))


class TestDynamoCnf:
    def test_consulta_pagina_y_normaliza(self):
        cliente, stub = _cliente("dynamodb")
        item = {"PK": {"S": "MALLA#X1"}, "SK": {"S": "GENERAL"}, "orden": {"N": "1"}}
        stub.add_response("query", {"Items": [item], "LastEvaluatedKey": {"PK": {"S": "x"}}},
                          {"TableName": "cnf", "KeyConditionExpression": "PK = :pk",
                           "ExpressionAttributeValues": {":pk": {"S": "MALLA#X1"}},
                           "ConsistentRead": True})
        stub.add_response("query", {"Items": [{**item, "SK": {"S": "SCD"}}]},
                          {"TableName": "cnf", "KeyConditionExpression": "PK = :pk",
                           "ExpressionAttributeValues": {":pk": {"S": "MALLA#X1"}},
                           "ConsistentRead": True, "ExclusiveStartKey": {"PK": {"S": "x"}}})
        with stub:
            registros = DynamoCnf(cliente, "cnf").consultar("MALLA#X1")
        assert set(registros) == {"GENERAL", "SCD"}
        assert registros["GENERAL"]["orden"] == 1 and isinstance(registros["GENERAL"]["orden"], int)

    def test_sin_cambios_no_escribe(self, cnf):
        cliente, stub = _cliente("dynamodb")
        with stub:
            assert DynamoCnf(cliente, "cnf").persistir_periodo(cnf, cnf) == 0

    def test_escribe_solo_los_atributos_que_cambian(self, cnf):
        cliente, stub = _cliente("dynamodb")
        nuevo = json.loads(json.dumps(cnf))
        nuevo["EXTRACCION"]["controlExtraccion"]["periodo_negocio"] = "202610"
        stub.add_response("transact_write_items", {}, {"TransactItems": ANY})
        with stub:
            assert DynamoCnf(cliente, "cnf").persistir_periodo(nuevo, cnf) == 1

    def test_transaccion_cancelada_es_concurrencia(self, cnf):
        cliente, stub = _cliente("dynamodb")
        nuevo = json.loads(json.dumps(cnf))
        nuevo["CICLO#001"]["segmentos"] = []
        stub.add_client_error("transact_write_items", "TransactionCanceledException")
        with stub, pytest.raises(ErrorConcurrencia) as error:
            DynamoCnf(cliente, "cnf").persistir_periodo(nuevo, cnf)
        assert error.value.clasificacion.codigo == "CFG-CON-002"

    def test_otro_error_de_transaccion_se_traduce(self, cnf):
        cliente, stub = _cliente("dynamodb")
        nuevo = json.loads(json.dumps(cnf))
        nuevo["CICLO#001"]["segmentos"] = []
        stub.add_client_error("transact_write_items", "ThrottlingException")
        with stub, pytest.raises(ErrorPlataforma):
            DynamoCnf(cliente, "cnf").persistir_periodo(nuevo, cnf)

    def test_limite_de_la_transaccion(self):
        cliente, _ = _cliente("dynamodb")
        antes = {f"CICLO#{i:03d}": {"PK": "P", "SK": f"CICLO#{i:03d}", "segmentos": []}
                 for i in range(101)}
        despues = {k: {**v, "segmentos": [1]} for k, v in antes.items()}
        with pytest.raises(ErrorConfiguracion, match="Demasiados"):
            DynamoCnf(cliente, "cnf").persistir_periodo(despues, antes)


class TestDynamoLecturaYEjecuciones:
    def test_obtener_existente_e_inexistente(self):
        cliente, stub = _cliente("dynamodb")
        parametros = {"TableName": "prs", "Key": {"PK": {"S": "SRC#A"}, "SK": {"S": "SRC"}},
                      "ConsistentRead": True}
        stub.add_response("get_item", {"Item": {"schema": {"S": "ALFAPRD"}}}, parametros)
        stub.add_response("get_item", {}, parametros)
        lectura = DynamoLectura(cliente, "prs")
        with stub:
            assert lectura.obtener("SRC#A", "SRC") == {"schema": "ALFAPRD"}
            assert lectura.obtener("SRC#A", "SRC") is None

    def test_registrar_es_condicional(self):
        cliente, stub = _cliente("dynamodb")
        stub.add_response("put_item", {}, {"TableName": "ejc", "Item": ANY,
                                           "ConditionExpression": "attribute_not_exists(PK)"})
        stub.add_client_error("put_item", "ConditionalCheckFailedException")
        stub.add_client_error("put_item", "AccessDeniedException")
        ejecuciones = DynamoEjecuciones(cliente, "ejc")
        item = {"PK": "EJC#X#202610", "SK": "EJECUCION#n", "estado": "INICIADA"}
        with stub:
            assert ejecuciones.registrar(item) is True
            assert ejecuciones.registrar(item) is False
            with pytest.raises(ErrorPermisos):
                ejecuciones.registrar(item)


class TestDynamoIdempotencia:
    def test_reclama_una_clave_libre(self):
        cliente, stub = _cliente("dynamodb")
        stub.add_response("put_item", {}, {
            "TableName": "idem", "Item": ANY, "ConditionExpression": ANY,
            "ExpressionAttributeValues": {":fallido": {"S": "FALLIDO"},
                                          ":en_proceso": {"S": "EN_PROCESO"},
                                          ":ahora": {"N": "100"}}})
        with stub:
            assert DynamoIdempotencia(cliente, "idem").reclamar("M1", "k", 100, 130, 900) is None

    def test_clave_tomada_devuelve_el_registro(self):
        cliente, stub = _cliente("dynamodb")
        stub.add_client_error("put_item", "ConditionalCheckFailedException")
        stub.add_response("get_item", {"Item": {
            "estado": {"S": "COMPLETADO"},
            "resultado": {"S": json.dumps({"estado": "VALIDADO"})}}},
            {"TableName": "idem", "Key": {"PK": {"S": "IDEMPOTENCIA#M1"}, "SK": {"S": "k"}},
             "ConsistentRead": True})
        with stub:
            existente = DynamoIdempotencia(cliente, "idem").reclamar("M1", "k", 100, 130, 900)
        assert existente == {"estado": "COMPLETADO", "resultado": {"estado": "VALIDADO"}}

    def test_error_distinto_a_la_condicion_se_traduce(self):
        cliente, stub = _cliente("dynamodb")
        stub.add_client_error("put_item", "ProvisionedThroughputExceededException")
        with stub, pytest.raises(ErrorPlataforma):
            DynamoIdempotencia(cliente, "idem").reclamar("M1", "k", 100, 130, 900)

    def test_guardar_resultado_y_error(self):
        cliente, stub = _cliente("dynamodb")
        stub.add_response("update_item", {}, {
            "TableName": "idem", "Key": {"PK": {"S": "IDEMPOTENCIA#M1"}, "SK": {"S": "k"}},
            "UpdateExpression": ("SET estado = :estado, expiraEn = :expira, "
                                 "actualizado = :actualizado, resultado = :resultado, "
                                 "error = :error"),
            "ExpressionAttributeValues": ANY})
        with stub:
            DynamoIdempotencia(cliente, "idem").guardar("M1", "k", "FALLIDO", 900,
                                                        resultado={"a": 1}, error={"b": 2})


def test_a_dynamo_convierte_float_a_numero():
    assert a_dynamo({"x": 1.5}) == {"M": {"x": {"N": "1.5"}}}


class TestS3:
    def test_leer_y_escribir_json(self):
        cliente, stub = _cliente("s3")
        stub.add_response("get_object", {"Body": _cuerpo('{"a": 1}')},
                          {"Bucket": "b", "Key": "c/d.json"})
        stub.add_response("put_object", {"ETag": '"abc"', "VersionId": "v1"},
                          {"Bucket": "b", "Key": "c/d.json", "Body": ANY,
                           "ContentType": "application/json", "Metadata": {"malla": "x"}})
        almacen = AlmacenS3(cliente)
        with stub:
            assert almacen.leer("s3://b/c/d.json") == {"a": 1}
            assert almacen.escribir("s3://b/c/d.json", {"a": 1}, {"malla": "x"}) == {
                "uri": "s3://b/c/d.json", "etag": "abc", "versionId": "v1"}

    def test_archivo_inexistente_o_invalido(self):
        cliente, stub = _cliente("s3")
        stub.add_client_error("get_object", "NoSuchKey")
        stub.add_response("get_object", {"Body": _cuerpo("no es json")})
        almacen = AlmacenS3(cliente)
        with stub:
            with pytest.raises(ErrorConfiguracion) as error:
                almacen.leer("s3://b/x.json")
            assert error.value.clasificacion.codigo == "CFG-CNF-009"
            with pytest.raises(ErrorConfiguracion, match="JSON"):
                almacen.leer("s3://b/x.json")

    @pytest.mark.parametrize("uri", [None, "", "https://b/x", "s3://b", "s3:///x.json"])
    def test_uri_invalida(self, uri):
        with pytest.raises(ErrorConfiguracion):
            separar_uri(uri)

    def test_cargar_catalogo(self):
        cliente, stub = _cliente("s3")
        stub.add_response("get_object", {"Body": _cuerpo(json.dumps(CATALOGO_DOC))})
        with stub:
            assert "PRIMAS_PRUEBA" in cargar_catalogo(AlmacenS3(cliente), "s3://b/c.json").mallas


class TestStepFunctions:
    def test_inicia_con_el_arn_armado_desde_la_cuenta(self):
        cliente, stub = _cliente("stepfunctions")
        arn = "arn:aws:states:us-east-1:111111111111:execution:maquina:nombre"
        stub.add_response("start_execution", {"executionArn": arn, "startDate": 0}, {
            "stateMachineArn": "arn:aws:states:us-east-1:111111111111:stateMachine:maquina",
            "name": "nombre", "input": json.dumps({"b": 2, "a": 1}, sort_keys=True)})
        orquestador = OrquestadorStepFunctions(cliente, "aws", "us-east-1", "111111111111")
        with stub:
            assert orquestador.iniciar("maquina", "nombre", {"b": 2, "a": 1}) == arn

    @pytest.mark.parametrize("codigo, tipo", [("ExecutionAlreadyExists", ErrorOrquestacion),
                                              ("StateMachineDoesNotExist", ErrorConfiguracion),
                                              ("AccessDeniedException", ErrorPermisos)])
    def test_errores(self, codigo, tipo):
        cliente, stub = _cliente("stepfunctions")
        stub.add_client_error("start_execution", codigo)
        with stub, pytest.raises(tipo):
            OrquestadorStepFunctions(cliente, "aws", "us-east-1", "1").iniciar("m", "n", {})


class TestTraduccionDeErrores:
    class _ErrorAws(Exception):
        def __init__(self, codigo):
            super().__init__(codigo)
            self.response = {"Error": {"Code": codigo}}

    @pytest.mark.parametrize("codigo, tipo, cod", [
        ("AccessDenied", ErrorPermisos, "CFG-PER-001"),
        ("KMS.AccessDeniedException", ErrorPermisos, "CFG-PER-001"),
        ("ResourceNotFoundException", ErrorConfiguracion, "CFG-CNF-009"),
        ("ConditionalCheckFailedException", ErrorConcurrencia, "CFG-CON-002"),
        ("ThrottlingException", ErrorPlataforma, "CFG-PLT-001"),
        ("ValidationException", ErrorPlataforma, "CFG-PLT-000"),
    ])
    def test_por_codigo(self, codigo, tipo, cod):
        error = errores_aws.traducir(self._ErrorAws(codigo), "op")
        assert isinstance(error, tipo) and error.clasificacion.codigo == cod
        assert error.detalle["codigoAws"] == codigo

    def test_sin_codigo_es_falla_transitoria(self):
        error = errores_aws.traducir(ConnectionError("sin red"), "op")
        assert isinstance(error, ErrorPlataforma) and error.clasificacion.recuperable

    def test_protegido_deja_pasar_errores_propios(self):
        @errores_aws.protegido("op")
        def falla():
            raise ErrorConfiguracion("propio")
        with pytest.raises(ErrorConfiguracion, match="propio"):
            falla()
