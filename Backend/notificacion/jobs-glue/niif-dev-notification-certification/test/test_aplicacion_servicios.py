from datetime import timedelta

import pytest

from constructores import construir_registro_compuesto
from fakes import (
    BitacoraEnMemoria,
    ConsultorFalso,
    GeneradorSecuencial,
    RelojFijo,
    RepositorioCompuestoFalso,
    RepositorioEnlacesFalso,
)
from notificacion_certificacion.application.compuesto import ServicioCompuesto
from notificacion_certificacion.application.consultas import ServicioConsultas
from notificacion_certificacion.application.enlaces import ServicioEnlaces
from notificacion_certificacion.application.puertos import EventoLog
from notificacion_certificacion.domain.errores import (
    ErrorConcurrencia,
    ErrorConfiguracion,
    ErrorConsultaFallida,
    ErrorPlataforma,
)
from notificacion_certificacion.domain.modelos import (
    ConfiguracionCompuesta,
    Consulta,
    EntornoConsultas,
    PoliticaEnlaces,
    ResultadoConsulta,
)

ENTORNO = EntornoConsultas(catalogo="catalogo", salida="s3://b/salida/")
VARIABLES = {"period": "202610", "env": "dev"}


def _consulta(nombre="q", sql="SELECT ${period}", base="db_${env}"):
    return Consulta(nombre=nombre, base_datos=base, sql=sql)


def test_servicio_enlaces_guarda_el_enlace_con_vigencia_y_devuelve_la_url_corta():
    repositorio = RepositorioEnlacesFalso()
    reloj = RelojFijo()
    politica = PoliticaEnlaces("https://redir.example/r", "tabla", 7, 8)
    bitacora = BitacoraEnMemoria()
    servicio = ServicioEnlaces(repositorio, reloj, GeneradorSecuencial(), bitacora, politica, "casoA", "C1")

    url = servicio.crear("CERTIFICATE", "https://largo.example/x")

    tabla, enlace = repositorio.guardados[0]
    assert url == "https://redir.example/r?t=00000001"
    assert tabla == "tabla"
    assert enlace.token == "00000001"
    assert enlace.url == "https://largo.example/x"
    assert (enlace.caso_uso, enlace.cert_code, enlace.accion) == ("casoA", "C1", "CERTIFICATE")
    assert enlace.fecha_creacion == reloj.local.isoformat()
    assert enlace.expira_en == int((reloj.local + timedelta(days=7)).timestamp())
    assert bitacora.nombres() == [EventoLog.ENLACE_CREADO]


def test_servicio_consultas_ejecuta_con_variables_y_une_los_bloques():
    consultor = ConsultorFalso(
        {
            "SELECT 202610": ResultadoConsulta(("a",), (("1",),)),
            "SELECT otro": ResultadoConsulta(("b",), (("2",),)),
        }
    )
    servicio = ServicioConsultas(consultor, BitacoraEnMemoria())

    tabla = servicio.tabla([_consulta(), _consulta("q2", "SELECT otro")], VARIABLES, ENTORNO)

    assert tabla.texto == "a: 1\n\nb: 2"
    assert tabla.html.count("<table") == 2
    assert tabla.html.startswith('<div style="margin:0 0 12px 0;">')
    assert consultor.llamadas[0] == ("SELECT 202610", "db_dev", ENTORNO)


def test_servicio_consultas_sin_consultas_devuelve_texto_vacio():
    tabla = ServicioConsultas(ConsultorFalso({}), BitacoraEnMemoria()).tabla([], VARIABLES, ENTORNO)
    assert (tabla.texto, tabla.html) == ("", "")


def test_servicio_consultas_convierte_una_consulta_fallida_en_texto_y_la_registra():
    bitacora = BitacoraEnMemoria()
    consultor = ConsultorFalso({"SELECT 202610": ErrorConsultaFallida("NTF-PLA-002", "fallo")})

    tabla = ServicioConsultas(consultor, bitacora).tabla([_consulta()], VARIABLES, ENTORNO)

    assert tabla.texto == "Error ejecutando consulta"
    assert "Error ejecutando consulta" in tabla.html
    evento, nivel, contexto = bitacora.eventos[0]
    assert (evento, nivel, contexto["codigo"]) == (EventoLog.CONSULTA_FALLIDA, "ERROR", "NTF-PLA-002")


def test_servicio_consultas_no_oculta_errores_que_no_son_de_consulta():
    consultor = ConsultorFalso({"SELECT 202610": ErrorPlataforma("NTF-PLA-005", "tiempo")})
    with pytest.raises(ErrorPlataforma):
        ServicioConsultas(consultor, BitacoraEnMemoria()).tabla([_consulta()], VARIABLES, ENTORNO)


def test_disponibilidad_sin_consultas_devuelve_none():
    assert ServicioConsultas(ConsultorFalso({}), BitacoraEnMemoria()).disponibilidad([], VARIABLES, ENTORNO) is None


def test_disponibilidad_informa_los_miembros_sin_datos():
    consultor = ConsultorFalso(
        {
            "SELECT 202610": ResultadoConsulta(("registros",), (("0",),)),
            "SELECT otro": ResultadoConsulta(("registros",), (("3",),)),
        }
    )
    servicio = ServicioConsultas(consultor, BitacoraEnMemoria())

    aviso = servicio.disponibilidad(
        [_consulta("A"), _consulta("B", "SELECT otro")], VARIABLES, ENTORNO
    )

    assert "A: no se encontraron datos activos para el periodo 202610." in aviso
    assert "B:" not in aviso


def test_disponibilidad_con_todos_con_datos_devuelve_vacio():
    consultor = ConsultorFalso({"SELECT 202610": ResultadoConsulta(("registros",), (("1",),))})
    assert ServicioConsultas(consultor, BitacoraEnMemoria()).disponibilidad([_consulta()], VARIABLES, ENTORNO) == ""


def test_disponibilidad_no_interpreta_un_fallo_tecnico_como_cero():
    consultor = ConsultorFalso({"SELECT 202610": ErrorConsultaFallida("NTF-PLA-002", "fallo")})
    with pytest.raises(ErrorConsultaFallida):
        ServicioConsultas(consultor, BitacoraEnMemoria()).disponibilidad([_consulta()], VARIABLES, ENTORNO)


def test_disponibilidad_rechaza_resultados_fuera_del_contrato():
    consultor = ConsultorFalso({"SELECT 202610": ResultadoConsulta(("x",), (("1",),))})
    with pytest.raises(ErrorConfiguracion):
        ServicioConsultas(consultor, BitacoraEnMemoria()).disponibilidad([_consulta()], VARIABLES, ENTORNO)


CONFIG_COMPUESTA = ConfiguracionCompuesta("tabla", "grupo", "_CONTROL", ("m1", "m2"))


def _registros_compuesto(control_estado="NOTIFICATION_IN_PROGRESS", **cambios_control):
    control = construir_registro_compuesto(id_miembro="_CONTROL", estado=control_estado, **cambios_control)
    return {
        ("tabla", "grupo#202610", "_CONTROL"): control,
        ("tabla", "grupo#202610", "m1"): construir_registro_compuesto(id_miembro="m1"),
        ("tabla", "grupo#202610", "m2"): construir_registro_compuesto(id_miembro="m2"),
    }


def _servicio_compuesto(registros):
    repositorio = RepositorioCompuestoFalso(registros)
    return ServicioCompuesto(repositorio, RelojFijo(), BitacoraEnMemoria()), repositorio


def test_compuesto_no_se_prepara_sin_configuracion_o_en_regeneracion():
    servicio, _ = _servicio_compuesto({})
    assert servicio.preparar(None, "202610", "C1", False) is None
    assert servicio.preparar(CONFIG_COMPUESTA, "202610", "C1", True) is None


def test_compuesto_prepara_la_reserva_con_control_y_miembros():
    servicio, _ = _servicio_compuesto(_registros_compuesto())

    reserva = servicio.preparar(CONFIG_COMPUESTA, "202610", "C1", False)

    assert reserva.id_compuesto == "grupo#202610"
    assert reserva.ya_completada is False
    assert [miembro.id_miembro for miembro in reserva.miembros] == ["m1", "m2"]
    assert reserva.control.id_miembro == "_CONTROL"


def test_compuesto_ya_completado_para_el_mismo_cert_code_no_carga_miembros():
    registros = _registros_compuesto("NOTIFICATION_COMPLETED", cert_code="C1")
    servicio, _ = _servicio_compuesto(registros)

    reserva = servicio.preparar(CONFIG_COMPUESTA, "202610", "C1", False)

    assert reserva.ya_completada is True
    assert reserva.miembros == ()


def test_compuesto_con_miembro_no_listo_falla():
    registros = _registros_compuesto()
    registros[("tabla", "grupo#202610", "m2")] = construir_registro_compuesto(id_miembro="m2", estado="CONSUMED")
    servicio, _ = _servicio_compuesto(registros)
    with pytest.raises(ErrorConcurrencia):
        servicio.preparar(CONFIG_COMPUESTA, "202610", "C1", False)


def test_compuesto_sin_control_falla():
    servicio, _ = _servicio_compuesto({})
    with pytest.raises(ErrorConcurrencia):
        servicio.preparar(CONFIG_COMPUESTA, "202610", "C1", False)


def test_cerrar_no_hace_nada_sin_reserva_o_si_ya_estaba_completada():
    registros = _registros_compuesto("NOTIFICATION_COMPLETED", cert_code="C1")
    servicio, repositorio = _servicio_compuesto(registros)
    reserva = servicio.preparar(CONFIG_COMPUESTA, "202610", "C1", False)

    servicio.cerrar(None, "id")
    servicio.cerrar(reserva, "id")

    assert repositorio.consumos == []


def test_cerrar_consume_los_miembros_con_el_identificador_y_la_fecha_utc():
    servicio, repositorio = _servicio_compuesto(_registros_compuesto())
    reserva = servicio.preparar(CONFIG_COMPUESTA, "202610", "C1", False)

    servicio.cerrar(reserva, "id-1,id-2")

    consumida, id_mensaje, fecha = repositorio.consumos[0]
    assert consumida is reserva
    assert id_mensaje == "id-1,id-2"
    assert fecha == RelojFijo().utc.isoformat()


def test_cerrar_sin_identificador_no_consume_y_falla():
    servicio, repositorio = _servicio_compuesto(_registros_compuesto())
    reserva = servicio.preparar(CONFIG_COMPUESTA, "202610", "C1", False)

    with pytest.raises(ErrorPlataforma) as error:
        servicio.cerrar(reserva, "")

    assert error.value.codigo == "NTF-PLA-003"
    assert repositorio.consumos == []
