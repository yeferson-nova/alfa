import io
import re
from pathlib import Path

import boto3
import pytest
from botocore.response import StreamingBody
from botocore.stub import Stubber

from constructores import construir_parametros, construir_registro, copiar_config_caso
from escenario import armar_escenario
from fakes import BitacoraEnMemoria, ConsultorFalso
from notificacion_certificacion.adapters.outbound.s3_configuracion import RepositorioConfiguracionS3
from notificacion_certificacion.application.consultas import ServicioConsultas
from notificacion_certificacion.application.notificar_certificacion import ResultadoJob
from notificacion_certificacion.application.puertos import EventoLog
from notificacion_certificacion.domain.configuracion import leer_configuracion_caso
from notificacion_certificacion.domain.correo_html import avisos_a_html, componer_html, resultado_a_html
from notificacion_certificacion.domain.errores import ErrorConfiguracion, ErrorPermisos
from notificacion_certificacion.domain.modelos import Aviso, Consulta, EntornoConsultas, ResultadoConsulta
from notificacion_certificacion.domain.parametrizacion import parametrizar_estricto

PLANTILLAS_REALES = Path(__file__).resolve().parents[3] / "email-template"
RUTA_PLANTILLA = "email-template/caso.html"
PLANTILLA = (
    "<html><body><h1>$useCaseName</h1><p>$periodRange</p><table>$avisoHtml</table>"
    "$tableSummary $tableTotal $tableDetail <a href=\"$certificarLink\">C</a>"
    "<a href=\"$nocertificarLink\">N</a></body></html>"
)
ENTORNO = EntornoConsultas(catalogo="AwsDataCatalog", salida="s3://b/athena/")


def _con_plantilla(**cambios):
    datos = copiar_config_caso()
    datos["email"]["templatePath"] = RUTA_PLANTILLA
    datos["email"]["stringVars"]["useCaseName"] = "Caso <A>"
    plantillas = {("bucket-activos", RUTA_PLANTILLA): cambios.pop("plantilla", PLANTILLA)}
    return armar_escenario(config_caso=datos, plantillas=plantillas, **cambios)


def _eventos(escenario, evento):
    return [contexto for nombre, _, contexto in escenario.bitacora.eventos if nombre == evento]


def test_con_plantilla_el_correo_lleva_html_con_tablas_enlaces_y_variables_escapadas():
    escenario = _con_plantilla()
    assert escenario.caso.ejecutar() == ResultadoJob.ENVIADO

    mensaje = escenario.notificador.enviados[0][0]
    assert mensaje.html is not None
    assert "Caso &lt;A&gt;" in mensaje.html
    assert "01/10/2026 - 31/10/2026" in mensaje.html
    assert mensaje.html.count("<table role=\"presentation\"") == 3
    assert 'rel="noopener noreferrer"' in mensaje.html
    assert "https://redir.example/r?t=00000001" in mensaje.html
    assert "$" not in mensaje.html
    assert _eventos(escenario, EventoLog.FORMATO_CORREO) == [{"formato": "HTML"}]
    assert ("bucket-activos", RUTA_PLANTILLA) in escenario.configuracion.lecturas


def test_con_regeneracion_y_sin_datos_el_html_muestra_los_dos_recuadros_en_orden():
    escenario = _con_plantilla(
        parametros=construir_parametros(regeneracion_sin_datos_nuevos=True),
        registros=[construir_registro(sin_datos_periodo=True)],
    )
    escenario.caso.ejecutar()

    html = escenario.notificador.enviados[0][0].html
    azul, ambar = html.index("#e8f0fe"), html.index("#fff4e0")
    assert azul < ambar
    assert _eventos(escenario, EventoLog.AVISOS_AGREGADOS)[0]["tonos"] == ["info", "warn"]


def test_sin_ruta_de_plantilla_el_correo_sale_solo_en_texto():
    escenario = armar_escenario()
    escenario.caso.ejecutar()
    assert escenario.notificador.enviados[0][0].html is None
    assert _eventos(escenario, EventoLog.FORMATO_CORREO) == [{"formato": "TEXTO"}]


def test_si_la_plantilla_no_se_puede_leer_sale_en_texto_y_lo_registra():
    escenario = _con_plantilla(plantilla=ErrorPermisos("NTF-PER-001", "S3 get_object: AccessDenied"))
    assert escenario.caso.ejecutar() == ResultadoJob.ENVIADO

    assert escenario.notificador.enviados[0][0].html is None
    (evento,) = _eventos(escenario, EventoLog.PLANTILLA_NO_DISPONIBLE)
    assert (evento["codigo"], evento["ruta"]) == ("NTF-PER-001", RUTA_PLANTILLA)


def test_si_hay_avisos_y_la_plantilla_no_tiene_el_marcador_sale_en_texto_para_no_ocultarlos():
    escenario = _con_plantilla(
        plantilla="<p>$useCaseName</p>", registros=[construir_registro(sin_datos_periodo=True)]
    )
    escenario.caso.ejecutar()

    assert escenario.notificador.enviados[0][0].html is None
    assert _eventos(escenario, EventoLog.PLANTILLA_NO_DISPONIBLE)[0]["motivo"] == "SIN_MARCADOR_AVISOS"


@pytest.mark.parametrize("plantilla", sorted(PLANTILLAS_REALES.glob("*.html")), ids=lambda ruta: ruta.name)
def test_las_plantillas_de_produccion_quedan_sin_marcadores_pendientes(plantilla):
    texto = plantilla.read_text(encoding="utf-8")
    fragmentos = {
        "tableTotal": resultado_a_html(ResultadoConsulta(("total",), (("1",),))),
        "tableSummary": "",
        "tableDetail": "",
        "certificarLink": "https://r/?t=1",
        "nocertificarLink": "https://r/?t=2",
    }
    variables = {"useCaseName": "Caso", "periodRange": "01/06/2026 - 30/06/2026"}
    avisos = [Aviso("warn", "Sin datos")]

    html = componer_html(texto, variables, fragmentos, avisos)

    assert re.search(r"\$\{?[A-Za-z_]+\}?", html) is None
    assert "#fff4e0" in html


def test_avisos_vacios_no_agregan_fila():
    assert avisos_a_html([]) == ""


def test_parametrizacion_estricta_falla_si_falta_una_variable_o_el_marcador_es_invalido():
    assert parametrizar_estricto("p='${period}'", {"period": "202606"}, "q") == "p='202606'"
    with pytest.raises(ErrorConfiguracion) as faltante:
        parametrizar_estricto("p='$period'", {}, "total#1.query")
    assert faltante.value.codigo == "NTF-CFG-009"
    assert "total#1.query" in faltante.value.mensaje
    with pytest.raises(ErrorConfiguracion):
        parametrizar_estricto("costo $ 5", {}, "q")


def test_una_consulta_con_variable_desconocida_detiene_el_job_en_vez_de_devolver_cero_filas():
    datos = copiar_config_caso()
    datos["queries"]["total"][0]["query"] = "SELECT 1 WHERE p='$periodo'"
    escenario = armar_escenario(config_caso=datos)
    with pytest.raises(ErrorConfiguracion):
        escenario.caso.ejecutar()
    assert escenario.notificador.enviados == []


def test_consulta_ejecutada_registra_id_de_athena_y_bytes_escaneados():
    bitacora = BitacoraEnMemoria()
    consultor = ConsultorFalso({"SELECT 1": ResultadoConsulta(("a",), (("1",),), "q-9", 4096)})
    ServicioConsultas(consultor, bitacora).tabla([Consulta("total#1", "db", "SELECT 1")], {}, ENTORNO)
    _, _, contexto = bitacora.eventos[0]
    assert (contexto["id_consulta"], contexto["bytes_escaneados"]) == ("q-9", 4096)


def test_el_contexto_de_trazabilidad_queda_fijado_con_el_id_que_llega_por_argumento():
    escenario = armar_escenario(parametros=construir_parametros(id_ejecucion="arl-1", id_corrida="jr_1"))
    escenario.caso.ejecutar()
    assert escenario.bitacora.contexto == {
        "ejecucion_id": "arl-1",
        "request_id": "jr_1",
        "cert_code": "CERT1",
        "use_case_id": "casoA",
        "periodo_negocio": "202610",
    }
    assert _eventos(escenario, EventoLog.ID_FLUJO_AUSENTE) == []


def test_sin_argumento_toma_el_id_del_flujo_del_registro():
    escenario = armar_escenario(
        parametros=construir_parametros(id_ejecucion=None),
        registros=[construir_registro(ejecucion_flujo="arl-2")],
    )
    escenario.caso.ejecutar()
    assert escenario.bitacora.contexto["ejecucion_id"] == "arl-2"


def test_sin_id_del_flujo_avisa_y_no_inventa_uno():
    escenario = armar_escenario(parametros=construir_parametros(id_ejecucion=None))
    escenario.caso.ejecutar()
    assert escenario.bitacora.contexto["ejecucion_id"] is None
    assert len(_eventos(escenario, EventoLog.ID_FLUJO_AUSENTE)) == 1


def test_si_el_id_recibido_no_coincide_con_el_del_registro_lo_advierte():
    escenario = armar_escenario(
        parametros=construir_parametros(id_ejecucion="arl-1"),
        registros=[construir_registro(ejecucion_flujo="arl-9")],
    )
    escenario.caso.ejecutar()
    assert _eventos(escenario, EventoLog.ID_FLUJO_DISTINTO) == [{"ejecucion_registro": "arl-9"}]


def test_la_configuracion_lee_la_ruta_de_plantilla_si_existe():
    datos = copiar_config_caso()
    assert leer_configuracion_caso(datos, "casoA").ruta_plantilla is None
    datos["email"]["templatePath"] = " email-template/x.html "
    assert leer_configuracion_caso(datos, "casoA").ruta_plantilla == "email-template/x.html"
    datos["email"]["templatePath"] = ""
    with pytest.raises(ErrorConfiguracion):
        leer_configuracion_caso(datos, "casoA")


def test_s3_cargar_texto_lee_la_plantilla_normalizando_la_clave():
    cliente = boto3.client("s3", region_name="us-east-1")
    stub = Stubber(cliente)
    datos = "<p>$useCaseName</p>".encode("utf-8")
    stub.add_response(
        "get_object",
        {"Body": StreamingBody(io.BytesIO(datos), len(datos))},
        {"Bucket": "b", "Key": "email-template/x.html"},
    )
    with stub:
        assert RepositorioConfiguracionS3(cliente).cargar_texto("b", "s3://b/email-template/x.html") == "<p>$useCaseName</p>"


def test_sin_archivos_de_detalle_el_html_lo_indica():
    from notificacion_certificacion.domain.correo_html import archivos_a_html

    assert "No se encontraron archivos detalle." in archivos_a_html([])


def test_la_salida_por_defecto_escribe_en_la_consola(capsys):
    from notificacion_certificacion.adapters.inbound.job_glue import _escribir

    _escribir('{"a": 1}')
    assert capsys.readouterr().out == '{"a": 1}\n'
