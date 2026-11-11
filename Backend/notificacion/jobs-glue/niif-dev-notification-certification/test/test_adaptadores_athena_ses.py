import boto3
import pytest
from botocore.stub import Stubber

from notificacion_certificacion.adaptadores.salida.athena import ConsultorAthena
from notificacion_certificacion.adaptadores.salida.ses import NotificadorSes
from notificacion_certificacion.dominio.errores import (
    ErrorConsultaFallida,
    ErrorPermisos,
    ErrorPlataforma,
)
from notificacion_certificacion.dominio.modelos import EntornoConsultas, Mensaje, ResultadoConsulta

ENTORNO = EntornoConsultas(catalogo="catalogo", salida="s3://bucket/salida/")


class Espera:
    def __init__(self) -> None:
        self.segundos: list[float] = []

    def __call__(self, segundos: float) -> None:
        self.segundos.append(segundos)


def _athena(intervalo=1.0, maximo=10.0):
    cliente = boto3.client("athena", region_name="us-east-1")
    espera = Espera()
    return ConsultorAthena(cliente, espera, intervalo, maximo), Stubber(cliente), espera


def _inicio(stub):
    stub.add_response(
        "start_query_execution",
        {"QueryExecutionId": "q1"},
        {
            "QueryString": "SELECT 1",
            "QueryExecutionContext": {"Catalog": "catalogo", "Database": "db"},
            "ResultConfiguration": {"OutputLocation": "s3://bucket/salida/"},
        },
    )


def _estado(stub, nombre, motivo=None, escaneados=None):
    estado = {"State": nombre}
    if motivo:
        estado["StateChangeReason"] = motivo
    ejecucion = {"Status": estado}
    if escaneados is not None:
        ejecucion["Statistics"] = {"DataScannedInBytes": escaneados}
    stub.add_response("get_query_execution", {"QueryExecution": ejecucion}, {"QueryExecutionId": "q1"})


def _filas(*filas):
    return [{"Data": [{"VarCharValue": valor} for valor in fila]} for fila in filas]


def test_athena_espera_hasta_que_la_consulta_termina_y_lee_el_resultado():
    consultor, stub, espera = _athena()
    _inicio(stub)
    _estado(stub, "QUEUED")
    _estado(stub, "RUNNING")
    _estado(stub, "SUCCEEDED", escaneados=2048)
    stub.add_response(
        "get_query_results",
        {"ResultSet": {"Rows": _filas(("a", "b"), ("1", "2"), ("3", "4"))}},
        {"QueryExecutionId": "q1"},
    )
    with stub:
        resultado = consultor.ejecutar("SELECT 1", "db", ENTORNO)

    assert resultado == ResultadoConsulta(("a", "b"), (("1", "2"), ("3", "4")), "q1", 2048)
    assert espera.segundos == [1.0, 1.0]


def test_athena_lee_todas_las_paginas_y_toma_los_encabezados_solo_de_la_primera():
    consultor, stub, _ = _athena()
    _inicio(stub)
    _estado(stub, "SUCCEEDED")
    stub.add_response(
        "get_query_results",
        {"ResultSet": {"Rows": _filas(("a",), ("1",))}, "NextToken": "pagina2"},
        {"QueryExecutionId": "q1"},
    )
    stub.add_response(
        "get_query_results",
        {"ResultSet": {"Rows": _filas(("2",), ("3",))}},
        {"QueryExecutionId": "q1", "NextToken": "pagina2"},
    )
    with stub:
        resultado = consultor.ejecutar("SELECT 1", "db", ENTORNO)

    assert resultado == ResultadoConsulta(("a",), (("1",), ("2",), ("3",)), "q1")


def test_athena_con_solo_encabezados_devuelve_resultado_sin_filas():
    consultor, stub, _ = _athena()
    _inicio(stub)
    _estado(stub, "SUCCEEDED")
    stub.add_response("get_query_results", {"ResultSet": {"Rows": _filas(("a",))}}, {"QueryExecutionId": "q1"})
    with stub:
        assert consultor.ejecutar("SELECT 1", "db", ENTORNO) == ResultadoConsulta(("a",), (), "q1")


def test_athena_sin_filas_devuelve_encabezados_vacios():
    consultor, stub, _ = _athena()
    _inicio(stub)
    _estado(stub, "SUCCEEDED")
    stub.add_response("get_query_results", {"ResultSet": {"Rows": []}}, {"QueryExecutionId": "q1"})
    with stub:
        assert consultor.ejecutar("SELECT 1", "db", ENTORNO) == ResultadoConsulta((), (), "q1")


def test_athena_completa_celdas_vacias_con_texto_vacio():
    consultor, stub, _ = _athena()
    _inicio(stub)
    _estado(stub, "SUCCEEDED")
    filas = [{"Data": [{"VarCharValue": "a"}]}, {"Data": [{}]}]
    stub.add_response("get_query_results", {"ResultSet": {"Rows": filas}}, {"QueryExecutionId": "q1"})
    with stub:
        assert consultor.ejecutar("SELECT 1", "db", ENTORNO).filas == (("",),)


@pytest.mark.parametrize("estado", ["FAILED", "CANCELLED"])
def test_athena_con_consulta_fallida_o_cancelada_informa_el_motivo(estado):
    consultor, stub, _ = _athena()
    _inicio(stub)
    _estado(stub, estado, "tabla inexistente")
    with stub, pytest.raises(ErrorConsultaFallida) as error:
        consultor.ejecutar("SELECT 1", "db", ENTORNO)
    assert error.value.codigo == "NTF-PLA-002"
    assert "tabla inexistente" in error.value.mensaje
    assert estado in error.value.mensaje


def test_athena_agota_el_tiempo_maximo_cancela_la_consulta_y_falla_como_recuperable():
    consultor, stub, espera = _athena(intervalo=1.0, maximo=2.0)
    _inicio(stub)
    _estado(stub, "RUNNING")
    _estado(stub, "RUNNING")
    stub.add_response("stop_query_execution", {}, {"QueryExecutionId": "q1"})
    with stub, pytest.raises(ErrorPlataforma) as error:
        consultor.ejecutar("SELECT 1", "db", ENTORNO)
    stub.assert_no_pending_responses()
    assert error.value.codigo == "NTF-PLA-005"
    assert error.value.recuperable is True
    assert "consulta cancelada en Athena" in error.value.mensaje
    assert not isinstance(error.value, ErrorConsultaFallida)
    assert len(espera.segundos) == 2


def test_athena_si_no_puede_cancelar_igual_falla_por_tiempo_e_indica_por_que():
    consultor, stub, _ = _athena(intervalo=1.0, maximo=1.0)
    _inicio(stub)
    _estado(stub, "RUNNING")
    stub.add_client_error("stop_query_execution", service_error_code="AccessDeniedException")
    with stub, pytest.raises(ErrorPlataforma) as error:
        consultor.ejecutar("SELECT 1", "db", ENTORNO)
    assert error.value.codigo == "NTF-PLA-005"
    assert "no se pudo cancelar (NTF-PER-001)" in error.value.mensaje


def test_athena_traduce_los_errores_de_acceso():
    consultor, stub, _ = _athena()
    stub.add_client_error("start_query_execution", service_error_code="AccessDeniedException")
    with stub, pytest.raises(ErrorPermisos):
        consultor.ejecutar("SELECT 1", "db", ENTORNO)


def _ses(conjunto=None):
    cliente = boto3.client("sesv2", region_name="us-east-1")
    return NotificadorSes(cliente, "remitente@example.com", conjunto), Stubber(cliente)


def _solicitud(**extra):
    base = {
        "FromEmailAddress": "remitente@example.com",
        "Destination": {"ToAddresses": ["destino@example.com"]},
        "Content": {
            "Simple": {
                "Subject": {"Data": "Asunto ñ", "Charset": "UTF-8"},
                "Body": {"Text": {"Data": "Texto", "Charset": "UTF-8"}},
            }
        },
    }
    base.update(extra)
    return base


def test_ses_envia_el_correo_de_texto_a_un_destinatario():
    notificador, stub = _ses()
    stub.add_response("send_email", {"MessageId": "mid-1"}, _solicitud())
    with stub:
        assert notificador.enviar(Mensaje("Asunto ñ", "Texto"), "destino@example.com") == "mid-1"


def test_ses_incluye_etiquetas_conjunto_de_configuracion_y_cuerpo_html():
    notificador, stub = _ses("conjunto")
    solicitud = _solicitud(
        EmailTags=[{"Name": "useCaseId", "Value": "casoA"}],
        ConfigurationSetName="conjunto",
    )
    solicitud["Content"]["Simple"]["Body"]["Html"] = {"Data": "<p>Texto</p>", "Charset": "UTF-8"}
    stub.add_response("send_email", {"MessageId": "mid-2"}, solicitud)
    mensaje = Mensaje("Asunto ñ", "Texto", html="<p>Texto</p>", etiquetas={"useCaseId": "casoA"})
    with stub:
        assert notificador.enviar(mensaje, "destino@example.com") == "mid-2"


def test_ses_sin_identificador_en_la_respuesta_devuelve_texto_vacio():
    notificador, stub = _ses()
    stub.add_response("send_email", {}, _solicitud())
    with stub:
        assert notificador.enviar(Mensaje("Asunto ñ", "Texto"), "destino@example.com") == ""


@pytest.mark.parametrize(
    ("codigo", "clase", "codigo_error"),
    [
        ("MessageRejected", ErrorPlataforma, "NTF-PLA-004"),
        ("TooManyRequestsException", ErrorPlataforma, "NTF-PLA-001"),
        ("AccessDeniedException", ErrorPermisos, "NTF-PER-001"),
    ],
)
def test_ses_traduce_los_rechazos_del_servicio(codigo, clase, codigo_error):
    notificador, stub = _ses()
    stub.add_client_error("send_email", service_error_code=codigo)
    with stub, pytest.raises(clase) as error:
        notificador.enviar(Mensaje("Asunto ñ", "Texto"), "destino@example.com")
    assert error.value.codigo == codigo_error
