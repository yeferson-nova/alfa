import pytest

from notificacion_certificacion.dominio.destinatarios import normalizar_destinatarios
from notificacion_certificacion.dominio.disponibilidad import extraer_cantidad
from notificacion_certificacion.dominio.errores import (
    ErrorConcurrencia,
    ErrorConfiguracion,
    ErrorEntrada,
    ErrorNotificacion,
    ErrorPlataforma,
)
from notificacion_certificacion.dominio.modelos import ResultadoConsulta
from notificacion_certificacion.dominio.parametrizacion import limpiar_url, parametrizar
from notificacion_certificacion.dominio.periodo import rango_periodo
from notificacion_certificacion.dominio.rutas import normalizar_clave, ruta_salida_consultas
from notificacion_certificacion.dominio.tablas import formatear_resultado, unir_bloques
from notificacion_certificacion.dominio.textos import TEXTO_SIN_DATOS


def test_limpiar_url_desescapa_hasta_estabilizar():
    assert limpiar_url(" https://x.example/a?b=1&amp;amp;c=2 ") == "https://x.example/a?b=1&c=2"


def test_limpiar_url_con_valor_vacio_devuelve_texto_vacio():
    assert limpiar_url(None) == ""


def test_parametrizar_reemplaza_variables_y_deja_las_desconocidas():
    assert parametrizar("${a}-${b}", {"a": "uno"}) == "uno-${b}"


def test_parametrizar_con_desescape_limpia_antes_de_sustituir():
    assert parametrizar("x&amp;${a}", {"a": "1"}, desescapar=True) == "x&1"


def test_parametrizar_convierte_valores_no_texto():
    assert parametrizar(15, {}) == "15"


@pytest.mark.parametrize(
    ("periodo", "esperado"),
    [
        ("202602", "01/02/2026 - 28/02/2026"),
        ("202402", "01/02/2024 - 29/02/2024"),
        ("202512", "01/12/2025 - 31/12/2025"),
    ],
)
def test_rango_periodo_calcula_primer_y_ultimo_dia(periodo, esperado):
    assert rango_periodo(periodo) == esperado


@pytest.mark.parametrize("periodo", ["2026", "20261", "2026101", "abcdef", "202613", "202600"])
def test_rango_periodo_rechaza_formatos_invalidos(periodo):
    with pytest.raises(ErrorEntrada) as error:
        rango_periodo(periodo)
    assert error.value.codigo == "NTF-ENT-002"


def test_normalizar_clave_quita_prefijo_del_mismo_bucket_y_barra_inicial():
    assert normalizar_clave("bucket", "s3://bucket/a/b.json") == "a/b.json"
    assert normalizar_clave("bucket", "/a/b.json") == "a/b.json"
    assert normalizar_clave("bucket", None) == ""


def test_normalizar_clave_rechaza_rutas_de_otro_bucket():
    with pytest.raises(ErrorConfiguracion) as error:
        normalizar_clave("bucket", "s3://otro/a.json")
    assert error.value.codigo == "NTF-CFG-006"


def test_ruta_salida_consultas_termina_en_barra():
    assert ruta_salida_consultas("b", "athena/salida") == "s3://b/athena/salida/"


def test_formatear_resultado_sin_filas_devuelve_texto_sin_datos():
    assert formatear_resultado(ResultadoConsulta(("a",), ())) == TEXTO_SIN_DATOS


def test_formatear_resultado_une_columnas_y_completa_las_que_faltan():
    resultado = ResultadoConsulta(("a", "b", "c"), (("1", "2", "3"), ("4",)))
    assert formatear_resultado(resultado) == "a: 1 | b: 2 | c: 3\na: 4 | b:  | c: "


def test_unir_bloques_separa_con_linea_en_blanco_y_recorta():
    assert unir_bloques(["uno", "dos", ""]) == "uno\n\ndos"


def test_normalizar_destinatarios_limpia_pasa_a_minusculas_y_quita_repetidos():
    assert normalizar_destinatarios([" A@x.com ", "a@X.com", "", "b@x.com"]) == ("a@x.com", "b@x.com")


def test_normalizar_destinatarios_sin_valores_falla():
    with pytest.raises(ErrorConfiguracion) as error:
        normalizar_destinatarios(["", "  "])
    assert error.value.codigo == "NTF-CFG-003"


def test_extraer_cantidad_lee_la_unica_celda():
    assert extraer_cantidad(ResultadoConsulta(("registros",), (("42",),)), "m") == 42


@pytest.mark.parametrize(
    "resultado",
    [
        ResultadoConsulta(("otra",), (("1",),)),
        ResultadoConsulta(("registros",), ()),
        ResultadoConsulta(("registros",), (("1",), ("2",))),
        ResultadoConsulta(("registros",), (("1", "2"),)),
        ResultadoConsulta(("registros",), (("abc",),)),
        ResultadoConsulta(("registros",), (("-1",),)),
    ],
)
def test_extraer_cantidad_rechaza_resultados_que_no_cumplen_el_contrato(resultado):
    with pytest.raises(ErrorConfiguracion) as error:
        extraer_cantidad(resultado, "miembro")
    assert error.value.codigo == "NTF-CFG-007"
    assert "miembro" in error.value.mensaje


def test_error_expone_su_clasificacion_completa():
    error = ErrorConcurrencia("NTF-CON-001", "detalle")
    assert error.como_diccionario() == {
        "codigo": "NTF-CON-001",
        "dominio": "CONCURRENCIA",
        "severidad": "ALTA",
        "recuperable": False,
        "accion": error.accion,
        "destinatario": "SOPORTE",
        "mensaje": "detalle",
    }


def test_error_permite_marcar_recuperable_solo_en_la_instancia():
    recuperable = ErrorPlataforma("NTF-PLA-001", "x", recuperable=True)
    normal = ErrorPlataforma("NTF-PLA-004", "y")
    assert recuperable.recuperable is True
    assert normal.recuperable is False
    assert isinstance(recuperable, ErrorNotificacion)
