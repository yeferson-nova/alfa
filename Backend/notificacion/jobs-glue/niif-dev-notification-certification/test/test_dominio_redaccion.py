from constructores import construir_registro, copiar_config_caso
from notificacion_certificacion.domain.configuracion import leer_configuracion_caso
from notificacion_certificacion.domain.redaccion import (
    aviso_disponibilidad,
    aviso_por_registros,
    componer_texto,
    construir_asunto,
    construir_cuerpo,
    formatear_archivos,
    nombre_de_archivo,
    variables_base,
    variables_finales,
)
from notificacion_certificacion.domain.textos import (
    AVISO_REGENERACION,
    TEXTO_ARCHIVO_POR_DEFECTO,
    TEXTO_SIN_ARCHIVOS,
    TEXTO_SIN_RESUMEN,
    TEXTO_SIN_TOTALES,
)


def _caso():
    return leer_configuracion_caso(copiar_config_caso(), "casoA")


def test_variables_base_incluye_el_contrato_de_plantillas():
    base = variables_base("dev", _caso(), construir_registro(), "CERT1")
    assert base == {
        "env": "dev",
        "period": "202610",
        "periodRange": "01/10/2026 - 31/10/2026",
        "useCaseId": "casoA",
        "useCaseName": "Caso A",
        "certCode": "CERT1",
        "userEmail": "usuario@example.com",
        "executionId": "ej1",
    }


def test_variables_base_usa_el_identificador_si_no_hay_nombre_configurado():
    datos = copiar_config_caso()
    del datos["email"]["stringVars"]["useCaseName"]
    caso = leer_configuracion_caso(datos, "casoA")
    assert variables_base("dev", caso, construir_registro(), "C")["useCaseName"] == "casoA"


def test_variables_finales_resuelve_las_variables_de_texto_con_las_base():
    base = variables_base("dev", _caso(), construir_registro(), "CERT1")
    finales = variables_finales(base, _caso().variables_texto)
    assert finales["certificarLink"] == "https://api.example/certify?c=CERT1&amp;p=202610"
    assert finales["useCaseName"] == "Caso A"
    assert finales["env"] == "dev"


def test_construir_asunto_normal_y_regenerado():
    variables = {"useCaseName": "Caso A", "period": "202610"}
    plantilla = "Cert ${useCaseName}\n${period}\r"
    assert construir_asunto(plantilla, variables, False) == "Cert Caso A 202610 "
    assert construir_asunto(plantilla, variables, True) == "[REGENERADO] Cert Caso A 202610 "


def test_nombre_de_archivo_usa_texto_por_defecto_si_esta_vacio():
    assert nombre_de_archivo(construir_registro(nombre_archivo="")) == TEXTO_ARCHIVO_POR_DEFECTO
    assert nombre_de_archivo(construir_registro(nombre_archivo="x.csv")) == "x.csv"


def test_formatear_archivos_lista_cada_enlace_o_avisa_que_no_hay():
    assert formatear_archivos([]) == TEXTO_SIN_ARCHIVOS
    assert formatear_archivos([("a.csv", "http://r/1"), ("b.csv", "http://r/2")]) == (
        "- a.csv\n  http://r/1\n- b.csv\n  http://r/2"
    )


def test_construir_cuerpo_incluye_todas_las_secciones():
    cuerpo = construir_cuerpo("Caso A", "resumen", "totales", "archivos", "http://ok", "http://no")
    assert cuerpo == (
        "\nSolicitud de certificación NIIF 17\n\n"
        "Caso de uso:\nCaso A\n\n"
        "Resumen:\nresumen\n\n"
        "Totales:\ntotales\n\n"
        "Archivos generados:\narchivos\n\n"
        "Certificar:\nhttp://ok\n\n"
        "No certificar:\nhttp://no\n\n"
        "Este mensaje fue generado automáticamente por el flujo NIIF 17.\n"
    )


def test_construir_cuerpo_usa_textos_por_defecto_para_resumen_y_totales_vacios():
    cuerpo = construir_cuerpo("Caso A", "", "", "archivos", "a", "b")
    assert TEXTO_SIN_RESUMEN in cuerpo
    assert TEXTO_SIN_TOTALES in cuerpo


def test_aviso_por_registros_sin_vacios_devuelve_vacio():
    assert aviso_por_registros([construir_registro()], "202610") == ""


def test_aviso_por_registros_cuando_todos_estan_vacios():
    registros = [construir_registro(sin_datos_periodo=True), construir_registro(sin_datos_periodo=True)]
    assert aviso_por_registros(registros, "202610") == (
        "No se encontraron datos procesados para el periodo seleccionado: 202610."
    )


def test_aviso_por_registros_parcial_lista_las_consultas_vacias():
    registros = [
        construir_registro(nombre_consulta="q1", sin_datos_periodo=True),
        construir_registro(nombre_consulta="q2"),
        construir_registro(nombre_consulta="q3", sin_datos_periodo=True),
    ]
    assert aviso_por_registros(registros, "202610") == (
        "No se encontraron datos procesados para el periodo 202610 en las consultas: q1, q3. "
        "Las demas consultas conservan sus datos."
    )


def test_aviso_disponibilidad_sin_vacios_devuelve_vacio():
    assert aviso_disponibilidad("202610", [], 2) == ""


def test_aviso_disponibilidad_parcial_y_total():
    parcial = aviso_disponibilidad("202610", ["A"], 2)
    total = aviso_disponibilidad("202610", ["A", "B"], 2)
    assert parcial.splitlines() == [
        "Disponibilidad por caso de uso:",
        "A: no se encontraron datos activos para el periodo 202610.",
        "El archivo y los resumenes incluyen los datos disponibles de los demas casos.",
    ]
    assert total.splitlines()[-1] == (
        "Ninguno de los casos del compuesto tiene datos activos para el periodo solicitado."
    )


def test_componer_texto_ordena_regeneracion_aviso_y_cuerpo():
    assert componer_texto("cuerpo", "", False) == "cuerpo"
    assert componer_texto("cuerpo", "aviso", False) == "aviso\n\ncuerpo"
    assert componer_texto("cuerpo", "aviso", True) == f"{AVISO_REGENERACION}\n\naviso\n\ncuerpo"
    assert componer_texto("cuerpo", "", True) == f"{AVISO_REGENERACION}\n\ncuerpo"
