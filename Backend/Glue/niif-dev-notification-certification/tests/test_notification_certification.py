from unittest.mock import patch

import pytest
from botocore.exceptions import ClientError

from conftest import DIRECTORIO_PLANTILLAS

PLANTILLA = (
    "<h1>$useCaseName</h1>$avisoHtml<table>$tableTotal</table>$tableSummary$tableDetail"
    '<a href="$certificarLink">ok</a><a href="$nocertificarLink">no</a>$sinDefinir'
)

RESULTADO = {"error": False, "headers": ["registros", "valor"], "rows": [["12", "900"]]}


def _error_ses(codigo="MessageRejected"):
    return ClientError({"Error": {"Code": codigo, "Message": "rechazado"}}, "SendEmail")


# ---------------- Athena: texto y HTML ----------------

def test_result_to_text_mantiene_el_formato_columna_valor(job):
    resultado = {"error": False, "headers": ["a", "b"], "rows": [["1", "2"], ["3"]]}

    assert job.result_to_text(resultado) == "a: 1 | b: 2\na: 3 | b: "


def test_result_to_text_informa_error_y_ausencia_de_datos(job):
    assert job.result_to_text({"error": True, "headers": [], "rows": []}) == "Error ejecutando consulta"
    assert job.result_to_text({"error": False, "headers": ["a"], "rows": []}) == "No hay datos"


def test_result_to_html_genera_encabezados_y_celdas_con_estilos_en_linea(job):
    html = job.result_to_html({"error": False, "headers": ["a", "b"], "rows": [["1"]]})

    assert html.count("<th style=") == 2
    assert html.count("<td style=") == 2


def test_result_to_html_escapa_los_valores_de_las_consultas(job):
    html = job.result_to_html({"error": False, "headers": ["a"], "rows": [["<script>x</script>"]]})

    assert "<script>" not in html
    assert "&lt;script&gt;" in html


def test_result_to_html_informa_error_y_ausencia_de_datos(job):
    assert "Error ejecutando consulta" in job.result_to_html({"error": True, "headers": [], "rows": []})
    assert "No hay datos" in job.result_to_html({"error": False, "headers": ["a"], "rows": []})


def test_query_athena_devuelve_error_cuando_la_consulta_falla(job):
    job.athena.start_query_execution.return_value = {"QueryExecutionId": "q1"}
    job.athena.get_query_execution.return_value = {
        "QueryExecution": {"Status": {"State": "FAILED", "StateChangeReason": "sintaxis"}}
    }

    resultado = job.query_athena("db", "SELECT 1", {}, "s3://b/salida/", "AwsDataCatalog")

    assert resultado["error"] is True


def test_query_athena_separa_encabezados_y_filas(job):
    job.athena.start_query_execution.return_value = {"QueryExecutionId": "q1"}
    job.athena.get_query_execution.return_value = {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}
    job.athena.get_query_results.return_value = {
        "ResultSet": {
            "Rows": [
                {"Data": [{"VarCharValue": "registros"}]},
                {"Data": [{"VarCharValue": "3"}]},
                {"Data": [{}]},
            ]
        }
    }

    resultado = job.query_athena("db", "SELECT 1", {}, "s3://b/salida/", "AwsDataCatalog")

    assert resultado == {"error": False, "headers": ["registros"], "rows": [["3"], [""]]}


def test_ejecutar_athena_conserva_el_contrato_de_disponibilidad_por_miembro(job):
    job.athena.start_query_execution.return_value = {"QueryExecutionId": "q1"}
    job.athena.get_query_execution.return_value = {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}
    job.athena.get_query_results.return_value = {
        "ResultSet": {"Rows": [{"Data": [{"VarCharValue": "registros"}]}, {"Data": [{"VarCharValue": "0"}]}]}
    }

    assert job.ejecutar_athena("db", "SELECT 1", {}, "s3://b/s/", "cat") == "registros: 0"


def test_generar_tablas_devuelve_texto_y_html_con_una_sola_ejecucion_por_consulta(job):
    job.query_athena = lambda *argumentos: RESULTADO
    config = {"queries": {"total": [{"queryName": "b", "database": "d", "query": "q"}, {"queryName": "a", "database": "d", "query": "q"}]}}

    texto, html = job.generar_tablas(config, "total", {}, "s3://b/s/", "cat")

    assert texto == "registros: 12 | valor: 900\n\nregistros: 12 | valor: 900"
    assert html.count("<table") == 2


def test_generar_tablas_sin_consultas_devuelve_vacios(job):
    assert job.generar_tablas({}, "total", {}, "s3://b/s/", "cat") == ("", "")


# ---------------- HTML del correo ----------------

def test_details_to_html_lista_los_enlaces_escapados(job):
    html = job.details_to_html([("a<b.csv", "https://r.example/?a=1&b=2")])

    assert 'href="https://r.example/?a=1&amp;b=2"' in html
    assert "a&lt;b.csv" in html


def test_details_to_html_sin_archivos_lo_indica(job):
    assert "No se encontraron archivos detalle." in job.details_to_html([])


def test_notices_to_html_sin_avisos_es_vacio_y_con_avisos_usa_saltos_de_linea(job):
    assert job.notices_to_html([]) == ""
    assert "uno<br>dos<br>tres" in job.notices_to_html(["uno\n\ndos", "tres"])


def _construir(job, plantilla=PLANTILLA, notices=(), variables=None):
    job.load_text = lambda bucket, key: plantilla
    return job.build_html_body(
        "bucket",
        "email-templates/caso.html",
        variables or {"useCaseName": "Caso <A>", "certificarLink": "https://largo.example"},
        {"tableTotal": "<b>T</b>", "tableSummary": "", "tableDetail": "<i>D</i>", "certificarLink": "https://c.example/?t=1&amp;x=2", "nocertificarLink": "https://n.example"},
        list(notices),
    )


def test_build_html_body_sin_plantilla_configurada_devuelve_none(job):
    assert job.build_html_body("bucket", None, {}, {}, []) is None
    assert job.build_html_body("bucket", "", {}, {}, []) is None


def test_build_html_body_sustituye_variables_escapadas_y_fragmentos(job):
    cuerpo = _construir(job)

    assert "<h1>Caso &lt;A&gt;</h1>" in cuerpo
    assert "<b>T</b>" in cuerpo and "<i>D</i>" in cuerpo
    assert 'href="https://c.example/?t=1&amp;x=2"' in cuerpo
    assert "$sinDefinir" in cuerpo


def test_build_html_body_incluye_el_recuadro_de_avisos(job):
    cuerpo = _construir(job, notices=["CASO DE USO REEJECUTADO"])

    assert "CASO DE USO REEJECUTADO" in cuerpo
    assert "$avisoHtml" not in cuerpo


def test_build_html_body_sin_avisos_deja_el_marcador_vacio(job):
    assert "$avisoHtml" not in _construir(job)


def test_build_html_body_no_oculta_avisos_cuando_la_plantilla_no_tiene_marcador(job):
    assert _construir(job, plantilla="<p>$tableTotal</p>", notices=["aviso"]) is None


def test_build_html_body_sin_avisos_acepta_una_plantilla_sin_marcador(job):
    assert _construir(job, plantilla="<p>$tableTotal</p>") == "<p><b>T</b></p>"


def test_build_html_body_si_no_puede_leer_la_plantilla_devuelve_none(job):
    def fallar(bucket, key):
        raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")

    job.load_text = fallar

    assert job.build_html_body("bucket", "email-templates/x.html", {}, {}, []) is None


def test_load_text_lee_la_clave_normalizada(job):
    job.s3.get_object.return_value = {"Body": type("Cuerpo", (), {"read": lambda self: "<p>ñ</p>".encode("utf-8")})()}

    assert job.load_text("bucket", "s3://bucket/email-templates/a.html") == "<p>ñ</p>"
    job.s3.get_object.assert_called_once_with(Bucket="bucket", Key="email-templates/a.html")


@pytest.mark.parametrize("archivo", sorted(DIRECTORIO_PLANTILLAS.glob("*.html")), ids=lambda ruta: ruta.name)
def test_las_plantillas_nuevas_tienen_un_solo_marcador_de_avisos_antes_del_total(archivo):
    texto = archivo.read_text(encoding="utf-8")

    assert texto.count("$avisoHtml") == 1
    assert texto.index("$avisoHtml") < texto.index("$tableTotal")


@pytest.mark.parametrize("archivo", sorted(DIRECTORIO_PLANTILLAS.glob("*.html")), ids=lambda ruta: ruta.name)
def test_las_plantillas_nuevas_se_llenan_sin_dejar_marcadores_pendientes(job, archivo):
    plantilla = archivo.read_text(encoding="utf-8")

    cuerpo = _construir(job, plantilla=plantilla, notices=["aviso de prueba"], variables={"useCaseName": "Caso", "periodRange": "01/10/2026 - 31/10/2026"})

    assert "$tableTotal" not in cuerpo
    assert "$certificarLink" not in cuerpo
    assert "$nocertificarLink" not in cuerpo
    assert "$useCaseName" not in cuerpo
    assert "aviso de prueba" in cuerpo


# ---------------- Envío por SES ----------------

def test_send_ses_arma_la_solicitud_solo_texto_con_etiquetas(job):
    job.sesv2.send_email.return_value = {"MessageId": "m-1"}

    assert job.send_ses("dest@example.com", "Asunto ñ", "Texto") == "m-1"

    solicitud = job.sesv2.send_email.call_args.kwargs
    assert solicitud["FromEmailAddress"] == "remitente@example.com"
    assert solicitud["Destination"] == {"ToAddresses": ["dest@example.com"]}
    assert solicitud["Content"]["Simple"]["Subject"] == {"Data": "Asunto ñ", "Charset": "UTF-8"}
    assert solicitud["Content"]["Simple"]["Body"] == {"Text": {"Data": "Texto", "Charset": "UTF-8"}}
    assert solicitud["EmailTags"] == [{"Name": "useCaseId", "Value": "casoA"}, {"Name": "emailType", "Value": "certification"}]
    assert "ConfigurationSetName" not in solicitud


def test_send_ses_agrega_html_y_conjunto_de_configuracion_cuando_existen(job):
    job.sesv2.send_email.return_value = {"MessageId": "m-2"}
    job.SES_CONFIGURATION_SET = "conjunto"

    job.send_ses("dest@example.com", "Asunto", "Texto", "<p>Hola</p>")

    solicitud = job.sesv2.send_email.call_args.kwargs
    assert solicitud["Content"]["Simple"]["Body"]["Html"] == {"Data": "<p>Hola</p>", "Charset": "UTF-8"}
    assert solicitud["ConfigurationSetName"] == "conjunto"


def test_send_ses_devuelve_cadena_vacia_si_ses_no_responde_el_id(job):
    job.sesv2.send_email.return_value = {}

    assert job.send_ses("dest@example.com", "Asunto", "Texto") == ""


# ---------------- DynamoDB ----------------

def test_marcar_sent_registra_el_canal_y_el_id_de_ses(job):
    tabla = job.dynamodb.Table.return_value
    items = [{"useCaseId": "casoA", "executionId": "E1"}]

    job.marcar_sent("tabla", items, "SENT", {"MessageId": "m-1"}, ["dest@example.com"])

    argumentos = tabla.update_item.call_args.kwargs
    assert "sesMessageId = :mid" in argumentos["UpdateExpression"]
    assert "snsMessageId" not in argumentos["UpdateExpression"]
    assert argumentos["ExpressionAttributeValues"][":ch"] == "SES_EMAIL"
    assert argumentos["ExpressionAttributeValues"][":dest"] == ["dest@example.com"]


def test_cerrar_compuesto_consumed_guarda_el_id_de_ses_en_ambas_actualizaciones(job):
    snapshot = {
        "tableName": "t",
        "compositeId": "g#202610",
        "certCode": "CERT1",
        "control": {"memberId": "_CONTROL", "updatedAt": "u0"},
        "members": [{"memberId": "m1", "sourceExecutionId": "e", "loteCargueId": "l", "updatedAt": "u1"}],
    }

    with patch.object(job.boto3, "client") as fabrica:
        job.cerrar_compuesto_consumed(snapshot, {"MessageId": "m-1"})

    transacciones = fabrica.return_value.transact_write_items.call_args.kwargs["TransactItems"]
    expresiones = [t["Update"]["UpdateExpression"] for t in transacciones]
    assert all("sesMessageId = :mid" in e and "snsMessageId" not in e for e in expresiones)


def test_cerrar_compuesto_consumed_exige_el_id_del_mensaje(job):
    snapshot = {"tableName": "t", "compositeId": "c", "certCode": "C", "control": {}, "members": []}

    with pytest.raises(RuntimeError, match="SES no devolvio MessageId"):
        job.cerrar_compuesto_consumed(snapshot, {})


# ---------------- Flujo completo (main) ----------------

CONFIG_GLOBAL = {
    "buckets": {"assets": "bucket-activos"},
    "dynamodb": {"notification_table": "tabla-notificaciones"},
    "environment": "dev",
    "athena": {"output_path": "athena/salida"},
    "certification": {"sent_status": "SENT"},
}


def _config_caso(plantilla="email-templates/caso.html"):
    correo = {
        "subject": "NIIF 17 - Certificacion ${useCaseName}",
        "stringVars": {"useCaseName": "Caso A", "certificarLink": "https://a.example/c", "nocertificarLink": "https://a.example/n"},
        "destinations": ["Uno@Example.com", "dos@example.com", "tres@example.com"],
    }
    if plantilla:
        correo["templatePath"] = plantilla
    return {"useCaseId": "casoA", "email": correo}


class Escenario:
    def __init__(self, job, config_caso, enviar=None):
        self.enviados = []
        self.marcados = []
        self.cierres = []
        self.html_enviado = {}
        job.cargar_json = lambda bucket, key: CONFIG_GLOBAL if key == "global.json" else config_caso
        job.obtener_regs = lambda tabla: [
            {"useCaseId": "casoA", "executionId": "E1", "period": "202610", "queryName": "detail#1", "s3_url": "s3://b/a.csv", "nameFile": "a.csv"}
        ]
        job.preparar_cierre_compuesto = lambda *argumentos: {"tableName": "t"}
        job.generar_tablas = lambda *argumentos: ("texto", "<table>HTML</table>")
        job.aviso_sin_datos_por_miembro = lambda *argumentos: None
        job.crear_short_link = lambda accion, url: f"https://redir.example/r?t={accion}"
        job.load_text = lambda bucket, key: PLANTILLA
        job.marcar_sent = lambda tabla, items, estado, respuesta, destinos: self.marcados.append((respuesta, destinos))
        job.cerrar_compuesto_consumed = lambda snapshot, respuesta: self.cierres.append(respuesta)
        job.send_ses = enviar or self._enviar

    def _enviar(self, destinatario, asunto, texto, cuerpo_html=None):
        self.enviados.append(destinatario)
        self.html_enviado[destinatario] = cuerpo_html
        return f"id-{len(self.enviados)}"


def test_main_envia_un_correo_por_destinatario_con_texto_y_html(job):
    escenario = Escenario(job, _config_caso())

    job.main()

    assert escenario.enviados == ["uno@example.com", "dos@example.com", "tres@example.com"]
    html = escenario.html_enviado["uno@example.com"]
    assert "<h1>Caso A</h1>" in html and "<table>HTML</table>" in html
    assert "https://redir.example/r?t=CERTIFICATE" in html
    respuesta, destinos = escenario.marcados[0]
    assert respuesta["MessageId"] == "id-1,id-2,id-3"
    assert destinos == escenario.enviados
    assert escenario.cierres == [respuesta]


def test_main_sin_plantilla_configurada_envia_solo_texto(job):
    escenario = Escenario(job, _config_caso(plantilla=None))

    job.main()

    assert set(escenario.html_enviado.values()) == {None}


def test_main_con_un_destinatario_rechazado_envia_a_los_demas_y_marca_solo_los_enviados(job):
    llamadas = []

    def enviar(destinatario, asunto, texto, cuerpo_html=None):
        llamadas.append(destinatario)
        if destinatario == "dos@example.com":
            raise _error_ses()
        return f"id-{len(llamadas)}"

    escenario = Escenario(job, _config_caso(), enviar=enviar)

    job.main()

    assert llamadas == ["uno@example.com", "dos@example.com", "tres@example.com"]
    respuesta, destinos = escenario.marcados[0]
    assert destinos == ["uno@example.com", "tres@example.com"]
    assert respuesta["MessageId"] == "id-1,id-3"
    assert len(escenario.cierres) == 1


def test_main_si_ses_no_devuelve_id_ese_destinatario_cuenta_como_fallido(job):
    resultados = iter(["", "id-2", "id-3"])
    escenario = Escenario(job, _config_caso(), enviar=lambda *argumentos: next(resultados))

    job.main()

    assert escenario.marcados[0][1] == ["dos@example.com", "tres@example.com"]


def test_main_si_fallan_todos_los_destinatarios_no_marca_ni_cierra_el_compuesto(job):
    def enviar(*argumentos):
        raise _error_ses()

    escenario = Escenario(job, _config_caso(), enviar=enviar)

    with pytest.raises(RuntimeError, match="ningun destinatario"):
        job.main()

    assert escenario.marcados == []
    assert escenario.cierres == []


def test_main_sin_destinatarios_falla_antes_de_enviar(job):
    config = _config_caso()
    config["email"]["destinations"] = []
    escenario = Escenario(job, config)

    with pytest.raises(ValueError, match="destinatarios"):
        job.main()

    assert escenario.enviados == []


def test_main_con_regeneracion_antepone_el_aviso_en_texto_asunto_y_html(job):
    capturado = {}

    def enviar(destinatario, asunto, texto, cuerpo_html=None):
        capturado.update(asunto=asunto, texto=texto, html=cuerpo_html)
        return "id-1"

    Escenario(job, _config_caso(), enviar=enviar)
    job.REGENERACION_SIN_DATOS_NUEVOS = True

    job.main()

    assert capturado["asunto"].startswith("[REGENERADO] ")
    assert capturado["texto"].startswith("CASO DE USO REEJECUTADO SIN REGISTROS NUEVOS")
    assert "CASO DE USO REEJECUTADO SIN REGISTROS NUEVOS" in capturado["html"]


def test_main_con_aviso_de_miembros_lo_incluye_en_texto_y_html(job):
    capturado = {}

    def enviar(destinatario, asunto, texto, cuerpo_html=None):
        capturado.update(texto=texto, html=cuerpo_html)
        return "id-1"

    Escenario(job, _config_caso(), enviar=enviar)
    job.aviso_sin_datos_por_miembro = lambda *argumentos: "Disponibilidad por caso de uso:\nM1: sin datos"

    job.main()

    assert capturado["texto"].startswith("Disponibilidad por caso de uso:")
    assert "M1: sin datos" in capturado["html"]


def test_main_sin_registros_no_envia_nada(job):
    escenario = Escenario(job, _config_caso())
    job.obtener_regs = lambda tabla: []

    job.main()

    assert escenario.enviados == []
    assert escenario.marcados == []


def test_main_con_un_useCaseId_distinto_falla(job):
    config = _config_caso()
    config["useCaseId"] = "otro"
    Escenario(job, config)

    with pytest.raises(RuntimeError, match="useCaseId"):
        job.main()
