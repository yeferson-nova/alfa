import io
import json

import boto3
import pytest
from botocore.exceptions import ClientError, EndpointConnectionError
from botocore.response import StreamingBody
from botocore.stub import Stubber

from notificacion_certificacion.adapters.outbound.errores_aws import traducir, traduciendo
from notificacion_certificacion.adapters.outbound.s3_configuracion import RepositorioConfiguracionS3
from notificacion_certificacion.domain.errores import (
    ErrorConcurrencia,
    ErrorConfiguracion,
    ErrorPermisos,
    ErrorPlataforma,
)


def _cuerpo(texto: str) -> StreamingBody:
    datos = texto.encode("utf-8")
    return StreamingBody(io.BytesIO(datos), len(datos))


def _repositorio():
    cliente = boto3.client("s3", region_name="us-east-1")
    return RepositorioConfiguracionS3(cliente), Stubber(cliente)


def test_cargar_json_lee_el_objeto_normalizando_la_clave():
    repositorio, stub = _repositorio()
    stub.add_response(
        "get_object",
        {"Body": _cuerpo(json.dumps({"a": "ñ"}))},
        {"Bucket": "bucket", "Key": "ruta/archivo.json"},
    )
    with stub:
        assert repositorio.cargar_json("bucket", "s3://bucket/ruta/archivo.json") == {"a": "ñ"}


def test_cargar_json_con_contenido_invalido_falla_como_configuracion():
    repositorio, stub = _repositorio()
    stub.add_response("get_object", {"Body": _cuerpo("{no es json")}, {"Bucket": "b", "Key": "k"})
    with stub, pytest.raises(ErrorConfiguracion) as error:
        repositorio.cargar_json("b", "k")
    assert error.value.codigo == "NTF-CFG-001"


def test_cargar_json_exige_que_el_documento_sea_un_objeto():
    repositorio, stub = _repositorio()
    stub.add_response("get_object", {"Body": _cuerpo("[1, 2]")}, {"Bucket": "b", "Key": "k"})
    with stub, pytest.raises(ErrorConfiguracion):
        repositorio.cargar_json("b", "k")


def test_cargar_json_traduce_la_ausencia_del_objeto_a_configuracion():
    repositorio, stub = _repositorio()
    stub.add_client_error("get_object", service_error_code="NoSuchKey")
    with stub, pytest.raises(ErrorConfiguracion) as error:
        repositorio.cargar_json("b", "k")
    assert error.value.codigo == "NTF-CFG-008"


def test_cargar_json_rechaza_rutas_de_otro_bucket_sin_llamar_a_aws():
    repositorio, stub = _repositorio()
    with stub, pytest.raises(ErrorConfiguracion) as error:
        repositorio.cargar_json("b", "s3://otro/k")
    assert error.value.codigo == "NTF-CFG-006"


def _error_cliente(codigo: str) -> ClientError:
    return ClientError({"Error": {"Code": codigo, "Message": "x"}}, "Operacion")


@pytest.mark.parametrize(
    ("codigo", "clase", "codigo_error", "recuperable"),
    [
        ("AccessDenied", ErrorPermisos, "NTF-PER-001", False),
        ("AccessDeniedException", ErrorPermisos, "NTF-PER-001", False),
        ("NoSuchBucket", ErrorConfiguracion, "NTF-CFG-008", False),
        ("ResourceNotFoundException", ErrorConfiguracion, "NTF-CFG-008", False),
        ("ConditionalCheckFailedException", ErrorConcurrencia, "NTF-CON-002", False),
        ("TransactionCanceledException", ErrorConcurrencia, "NTF-CON-002", False),
        ("ThrottlingException", ErrorPlataforma, "NTF-PLA-001", True),
        ("TooManyRequestsException", ErrorPlataforma, "NTF-PLA-001", True),
        ("MessageRejected", ErrorPlataforma, "NTF-PLA-004", False),
    ],
)
def test_traducir_clasifica_los_errores_de_aws(codigo, clase, codigo_error, recuperable):
    traducido = traducir(_error_cliente(codigo), "contexto")
    assert isinstance(traducido, clase)
    assert traducido.codigo == codigo_error
    assert traducido.recuperable is recuperable
    assert codigo in traducido.mensaje


def test_traducir_sin_codigo_de_error_se_clasifica_como_plataforma():
    sin_codigo = ClientError({}, "Operacion")
    assert traducir(sin_codigo, "ctx").codigo == "NTF-PLA-004"


def test_traduciendo_convierte_errores_de_red_en_plataforma_recuperable():
    with pytest.raises(ErrorPlataforma) as error:
        with traduciendo("ctx"):
            raise EndpointConnectionError(endpoint_url="https://ejemplo.invalid")
    assert error.value.codigo == "NTF-PLA-001"
    assert error.value.recuperable is True


def test_traduciendo_no_altera_otros_errores():
    with pytest.raises(ValueError):
        with traduciendo("ctx"):
            raise ValueError("otro")
