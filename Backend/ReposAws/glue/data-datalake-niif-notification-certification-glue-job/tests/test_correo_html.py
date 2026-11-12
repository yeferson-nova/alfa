import pytest

from constructores import RESPUESTAS_POR_DEFECTO, construir_registro, copiar_config_caso
from escenario import armar_escenario
from notificacion_certificacion.aplicacion.puertos import EventoLog
from notificacion_certificacion.dominio.configuracion import leer_configuracion_caso
from notificacion_certificacion.dominio.correo_html import (
    avisos_html,
    caja_aviso,
    detalle_html,
    fragmentos_html,
    plantilla_admite_avisos,
    rellenar_plantilla,
    tabla_html,
)
from notificacion_certificacion.dominio.errores import ErrorConfiguracion, ErrorConsultaFallida, ErrorPermisos
from notificacion_certificacion.dominio.modelos import Aviso, ResultadoConsulta
from notificacion_certificacion.dominio.redaccion import avisos_del_correo
from notificacion_certificacion.dominio.textos import AVISO_REGENERACION

RUTA = "email-template/caso_a.html"
PLANTILLA = (
    "<html>$avisoHtml|$useCaseName|$period|$tableSummary|$tableTotal|"
    "$tableDetail|$certificarLink|$nocertificarLink|$desconocida</html>"
)


def _config_con_plantilla():
    datos = copiar_config_caso()
    datos["email"]["templatePath"] = RUTA
    return datos


def test_tabla_html_escapa_encabezados_y_valores_y_completa_celdas_faltantes():
    html = tabla_html(ResultadoConsulta(("a<b", "c"), (("<x>",),)))

    assert "<th" in html and "a&lt;b" in html
    assert "&lt;x&gt;" in html
    assert html.count("<td") == 2


def test_tabla_html_sin_filas_muestra_no_hay_datos():
    assert ">No hay datos</p>" in tabla_html(ResultadoConsulta(("a",), ()))


def test_detalle_html_escapa_nombre_y_enlace():
    html = detalle_html([("a&b.csv", "https://x/r?t=1&u=2")])

    assert 'href="https://x/r?t=1&amp;u=2"' in html
    assert ">a&amp;b.csv</a>" in html
    assert 'rel="noopener noreferrer"' in html


def test_detalle_html_sin_archivos_muestra_el_texto_por_defecto():
    assert "No se encontraron archivos detalle." in detalle_html([])


def test_caja_aviso_une_lineas_no_vacias_y_usa_el_tono():
    html = caja_aviso(Aviso(tono="info", texto="uno\n\n<dos>"))

    assert "uno<br>&lt;dos&gt;" in html
    assert "#174ea6" in html


def test_avisos_html_vacio_no_genera_fila():
    assert avisos_html([]) == ""
    assert avisos_html([Aviso("warn", "x")]).startswith('<tr><td class="pad"')


def test_plantilla_admite_avisos_solo_si_tiene_el_marcador_o_no_hay_avisos():
    assert plantilla_admite_avisos("sin marcador", [])
    assert not plantilla_admite_avisos("sin marcador", [Aviso("warn", "x")])
    assert plantilla_admite_avisos("con $avisoHtml", [Aviso("warn", "x")])


def test_rellenar_plantilla_escapa_variables_y_no_toca_marcadores_desconocidos():
    fragmentos = fragmentos_html("<r/>", "<t/>", [], "https://c?a=1&b=2", "https://n")

    html = rellenar_plantilla(PLANTILLA, {"useCaseName": "A<B", "period": "202610"}, fragmentos, [])

    assert "|A&lt;B|202610|<r/>|<t/>|" in html
    assert "|https://c?a=1&amp;b=2|https://n|$desconocida" in html
    assert html.startswith("<html>|")


def test_avisos_del_correo_pone_la_regeneracion_primero():
    assert avisos_del_correo("", False) == []
    assert avisos_del_correo("falta", True) == [Aviso("info", AVISO_REGENERACION), Aviso("warn", "falta")]


def test_la_configuracion_lee_template_path_opcional():
    assert leer_configuracion_caso(copiar_config_caso(), "casoA").ruta_plantilla is None
    assert leer_configuracion_caso(_config_con_plantilla(), "casoA").ruta_plantilla == RUTA


def test_el_mensaje_lleva_html_de_la_plantilla_del_bucket_de_activos():
    escenario = armar_escenario(config_caso=_config_con_plantilla(), plantillas={RUTA: PLANTILLA})

    escenario.caso.ejecutar()

    mensaje, _ = escenario.notificador.enviados[0]
    assert ("bucket-activos", RUTA) in escenario.configuracion.lecturas
    assert "|Caso A|202610|" in mensaje.html
    assert "periodo</th>" in mensaje.html and ">5</td>" in mensaje.html and ">900</td>" in mensaje.html
    assert ">a.csv</a>" in mensaje.html
    assert "|https://redir.example/r?t=00000001|https://redir.example/r?t=00000002|" in mensaje.html
    assert mensaje.html.startswith("<html>|")


def test_los_avisos_aparecen_en_el_html_cuando_la_plantilla_tiene_marcador():
    registros = [construir_registro(sin_datos_periodo=True)]
    escenario = armar_escenario(config_caso=_config_con_plantilla(), plantillas={RUTA: PLANTILLA}, registros=registros)

    escenario.caso.ejecutar()

    mensaje, _ = escenario.notificador.enviados[0]
    assert "No se encontraron datos procesados para el periodo seleccionado: 202610." in mensaje.html
    assert "#8a4b00" in mensaje.html


def test_si_hay_avisos_y_la_plantilla_no_tiene_marcador_se_envia_solo_texto():
    registros = [construir_registro(sin_datos_periodo=True)]
    escenario = armar_escenario(
        config_caso=_config_con_plantilla(), plantillas={RUTA: "<html>$tableTotal</html>"}, registros=registros
    )

    escenario.caso.ejecutar()

    mensaje, _ = escenario.notificador.enviados[0]
    assert mensaje.html is None
    assert EventoLog.PLANTILLA_SIN_MARCADOR in escenario.bitacora.nombres()


def test_si_no_se_puede_leer_la_plantilla_se_envia_solo_texto_y_se_registra():
    for error in (ErrorConfiguracion("NTF-CFG-008", "no existe"), ErrorPermisos("NTF-PER-001", "denegado")):
        escenario = armar_escenario(config_caso=_config_con_plantilla(), plantillas={RUTA: error})

        escenario.caso.ejecutar()

        mensaje, _ = escenario.notificador.enviados[0]
        assert mensaje.html is None
        evento, nivel, contexto = next(e for e in escenario.bitacora.eventos if e[0] == EventoLog.PLANTILLA_NO_LEIDA)
        assert (nivel, contexto["codigo"], contexto["ruta"]) == ("WARN", error.codigo, RUTA)


def test_una_consulta_fallida_aparece_como_error_en_el_html():
    respuestas = dict(RESPUESTAS_POR_DEFECTO)
    respuestas["SELECT 'total' FROM t WHERE p='202610'"] = ErrorConsultaFallida("NTF-PLA-002", "fallo")
    escenario = armar_escenario(
        config_caso=_config_con_plantilla(), plantillas={RUTA: PLANTILLA}, respuestas=respuestas
    )

    escenario.caso.ejecutar()

    mensaje, _ = escenario.notificador.enviados[0]
    assert '<div style="margin:0 0 12px 0;"><p' in mensaje.html
    assert "Error ejecutando consulta</p></div>|" in mensaje.html


def test_una_plantilla_de_otro_bucket_detiene_el_job_como_el_script_anterior():
    datos = _config_con_plantilla()
    datos["email"]["templatePath"] = "s3://otro-bucket/email-template/a.html"
    escenario = armar_escenario(config_caso=datos)

    with pytest.raises(ErrorConfiguracion) as error:
        escenario.caso.ejecutar()

    assert error.value.codigo == "NTF-CFG-006"
    assert escenario.notificador.enviados == []
