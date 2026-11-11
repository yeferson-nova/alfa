import pytest

from constructores import (
    config_compuesta,
    construir_parametros,
    construir_registro,
    construir_registro_compuesto,
    copiar_config_caso,
)
from escenario import armar_escenario
from fakes import NotificadorFalso, RepositorioCompuestoFalso
from notificacion_certificacion.aplicacion.notificar_certificacion import ResultadoJob
from notificacion_certificacion.aplicacion.puertos import EventoLog
from notificacion_certificacion.dominio.errores import ErrorConfiguracion, ErrorPlataforma
from notificacion_certificacion.dominio.modelos import ResultadoConsulta


def test_flujo_normal_envia_un_correo_por_destinatario_y_marca_los_registros():
    escenario = armar_escenario()

    resultado = escenario.caso.ejecutar()

    assert resultado == ResultadoJob.ENVIADO
    assert [destinatario for _, destinatario in escenario.notificador.enviados] == [
        "uno@example.com",
        "dos@example.com",
    ]
    marcado = escenario.notificaciones.marcados[0]
    assert marcado["tabla"] == "tabla-notificaciones"
    assert marcado["estado"] == "SENT"
    assert marcado["canal"] == "SES_EMAIL"
    assert [envio.id_mensaje for envio in marcado["envios"]] == ["id-1", "id-2"]
    assert marcado["fecha_envio"] == escenario.reloj.local.isoformat()


def test_flujo_normal_busca_los_registros_con_los_parametros_recibidos():
    escenario = armar_escenario()
    escenario.caso.ejecutar()
    assert escenario.notificaciones.busquedas == [("tabla-notificaciones", "indice-cert", "CERT1", "casoA")]
    assert escenario.configuracion.lecturas == [("bucket-config", "global.json"), ("bucket-config", "casos/casoA.json")]


def test_el_mensaje_lleva_asunto_cuerpo_etiquetas_y_enlaces_cortos():
    escenario = armar_escenario()
    escenario.caso.ejecutar()

    mensaje, _ = escenario.notificador.enviados[0]

    assert mensaje.asunto == "NIIF 17 - Certificacion Caso A Periodo 202610"
    assert dict(mensaje.etiquetas) == {"useCaseId": "casoA", "emailType": "certification"}
    assert mensaje.html is None
    assert "Resumen:\nperiodo: 202610 | total: 5\n\nperiodo: 202610 | total: 7" in mensaje.texto
    assert "Totales:\nregistros: 12 | valor: 900" in mensaje.texto
    assert "- a.csv\n  https://redir.example/r?t=00000001" in mensaje.texto
    assert "Certificar:\nhttps://redir.example/r?t=00000002" in mensaje.texto
    assert "No certificar:\nhttps://redir.example/r?t=00000003" in mensaje.texto


def test_los_enlaces_se_guardan_con_la_url_original_limpia():
    escenario = armar_escenario()
    escenario.caso.ejecutar()

    urls = {enlace.accion: enlace.url for _, enlace in escenario.enlaces.guardados}

    assert urls == {
        "DOWNLOAD": "https://s3.example/a.csv?x=1&y=2",
        "CERTIFICATE": "https://api.example/certify?c=CERT1&p=202610",
        "NO_CERTIFICATE": "https://api.example/no?c=CERT1",
    }


def test_solo_los_registros_de_detalle_generan_enlaces_de_descarga_y_en_orden():
    registros = [
        construir_registro(nombre_consulta="detail#b", nombre_archivo="b.csv", id_ejecucion="e2"),
        construir_registro(nombre_consulta="summary#x", nombre_archivo="x.csv", id_ejecucion="e3"),
        construir_registro(nombre_consulta="detail#a", nombre_archivo="a.csv", id_ejecucion="e1"),
    ]
    escenario = armar_escenario(registros=registros)
    escenario.caso.ejecutar()

    descargas = [enlace.url for _, enlace in escenario.enlaces.guardados if enlace.accion == "DOWNLOAD"]
    texto = escenario.notificador.enviados[0][0].texto

    assert len(descargas) == 2
    assert texto.index("a.csv") < texto.index("b.csv")
    assert "x.csv" not in texto


def test_sin_archivos_de_detalle_el_cuerpo_lo_indica():
    escenario = armar_escenario(registros=[construir_registro(nombre_consulta="summary#x")])
    escenario.caso.ejecutar()
    assert "No se encontraron archivos detalle." in escenario.notificador.enviados[0][0].texto


def test_sin_registros_no_envia_nada():
    escenario = armar_escenario(registros=[])

    resultado = escenario.caso.ejecutar()

    assert resultado == ResultadoJob.SIN_REGISTROS
    assert escenario.notificador.enviados == []
    assert escenario.notificaciones.marcados == []
    assert EventoLog.SIN_REGISTROS in escenario.bitacora.nombres()


def test_si_todos_los_registros_ya_estan_enviados_no_reenvia():
    escenario = armar_escenario(registros=[construir_registro(estado="SENT"), construir_registro(estado="SENT")])

    resultado = escenario.caso.ejecutar()

    assert resultado == ResultadoJob.YA_ENVIADO
    assert escenario.notificador.enviados == []
    assert escenario.enlaces.guardados == []


def test_si_solo_algunos_estan_enviados_se_procesa_el_lote():
    escenario = armar_escenario(registros=[construir_registro(estado="SENT"), construir_registro(estado="PENDING")])
    assert escenario.caso.ejecutar() == ResultadoJob.ENVIADO


def test_regeneracion_agrega_prefijo_aviso_y_no_toca_el_compuesto():
    datos = copiar_config_caso()
    datos["compositeNotification"] = config_compuesta()
    parametros = construir_parametros(regeneracion_sin_datos_nuevos=True)
    escenario = armar_escenario(parametros=parametros, config_caso=datos)

    resultado = escenario.caso.ejecutar()

    mensaje, _ = escenario.notificador.enviados[0]
    assert resultado == ResultadoJob.ENVIADO
    assert mensaje.asunto.startswith("[REGENERADO] ")
    assert mensaje.texto.startswith("CASO DE USO REEJECUTADO SIN REGISTROS NUEVOS")
    assert escenario.compuesto.consumos == []


def test_aviso_por_registros_sin_datos_se_antepone_al_cuerpo():
    escenario = armar_escenario(registros=[construir_registro(sin_datos_periodo=True)])
    escenario.caso.ejecutar()
    texto = escenario.notificador.enviados[0][0].texto
    assert texto.startswith("No se encontraron datos procesados para el periodo seleccionado: 202610.")


def test_aviso_de_disponibilidad_por_miembro_reemplaza_al_de_registros():
    datos = copiar_config_caso()
    datos["email"]["dataPresence"] = [
        {"name": "A", "database": "db", "query": "SELECT presencia"},
    ]
    respuestas = {
        "SELECT 'a' FROM t WHERE p='202610'": ResultadoConsulta(("p",), (("1",),)),
        "SELECT 'b' FROM t WHERE p='202610'": ResultadoConsulta(("p",), (("1",),)),
        "SELECT 'total' FROM t WHERE p='202610'": ResultadoConsulta(("p",), (("1",),)),
        "SELECT presencia": ResultadoConsulta(("registros",), (("0",),)),
    }
    escenario = armar_escenario(
        config_caso=datos, respuestas=respuestas, registros=[construir_registro(sin_datos_periodo=True)]
    )

    escenario.caso.ejecutar()

    texto = escenario.notificador.enviados[0][0].texto
    assert texto.startswith("Disponibilidad por caso de uso:")
    assert "No se encontraron datos procesados" not in texto


def test_una_consulta_de_resumen_fallida_se_informa_en_el_cuerpo_sin_detener_el_envio():
    from notificacion_certificacion.dominio.errores import ErrorConsultaFallida

    respuestas = {
        "SELECT 'a' FROM t WHERE p='202610'": ErrorConsultaFallida("NTF-PLA-002", "fallo"),
        "SELECT 'b' FROM t WHERE p='202610'": ResultadoConsulta(("p",), (("1",),)),
        "SELECT 'total' FROM t WHERE p='202610'": ResultadoConsulta(("p",), ()),
    }
    escenario = armar_escenario(respuestas=respuestas)

    assert escenario.caso.ejecutar() == ResultadoJob.ENVIADO

    texto = escenario.notificador.enviados[0][0].texto
    assert "Error ejecutando consulta" in texto
    assert "Totales:\nNo hay datos" in texto


def test_un_caso_sin_consultas_usa_los_textos_por_defecto():
    datos = copiar_config_caso()
    del datos["queries"]
    escenario = armar_escenario(config_caso=datos, respuestas={})
    escenario.caso.ejecutar()
    texto = escenario.notificador.enviados[0][0].texto
    assert "No hay resumen configurado." in texto
    assert "No hay totales configurados." in texto


def test_un_caso_con_otro_useCaseId_falla_antes_de_consultar_registros():
    datos = copiar_config_caso()
    datos["useCaseId"] = "otro"
    escenario = armar_escenario(config_caso=datos)
    with pytest.raises(ErrorConfiguracion) as error:
        escenario.caso.ejecutar()
    assert error.value.codigo == "NTF-CFG-002"
    assert escenario.notificaciones.busquedas == []


def _compuesto_listo():
    return RepositorioCompuestoFalso(
        {
            ("tabla-compuesto", "grupo#202610", "_CONTROL"): construir_registro_compuesto(
                id_miembro="_CONTROL", estado="NOTIFICATION_IN_PROGRESS"
            ),
            ("tabla-compuesto", "grupo#202610", "m1"): construir_registro_compuesto(id_miembro="m1"),
            ("tabla-compuesto", "grupo#202610", "m2"): construir_registro_compuesto(id_miembro="m2"),
        }
    )


def test_con_compuesto_consume_los_miembros_despues_de_enviar_y_marcar():
    datos = copiar_config_caso()
    datos["compositeNotification"] = config_compuesta()
    escenario = armar_escenario(config_caso=datos, compuesto=_compuesto_listo())

    assert escenario.caso.ejecutar() == ResultadoJob.ENVIADO

    reserva, id_mensaje, _ = escenario.compuesto.consumos[0]
    assert id_mensaje == "id-1,id-2"
    assert reserva.cert_code == "CERT1"
    assert escenario.bitacora.nombres().index(EventoLog.REGISTROS_MARCADOS) < escenario.bitacora.nombres().index(
        EventoLog.COMPUESTO_CERRADO
    )


def test_con_compuesto_ya_completado_no_reenvia():
    datos = copiar_config_caso()
    datos["compositeNotification"] = config_compuesta()
    repositorio = RepositorioCompuestoFalso(
        {
            ("tabla-compuesto", "grupo#202610", "_CONTROL"): construir_registro_compuesto(
                id_miembro="_CONTROL", estado="NOTIFICATION_COMPLETED", cert_code="CERT1"
            ),
        }
    )
    escenario = armar_escenario(config_caso=datos, compuesto=repositorio)

    assert escenario.caso.ejecutar() == ResultadoJob.YA_COMPLETADO

    assert escenario.notificador.enviados == []
    assert EventoLog.COMPUESTO_YA_COMPLETADO in escenario.bitacora.nombres()


def test_si_ningun_destinatario_recibe_el_correo_no_se_marca_ni_se_consume():
    datos = copiar_config_caso()
    datos["compositeNotification"] = config_compuesta()
    escenario = armar_escenario(
        config_caso=datos, compuesto=_compuesto_listo(), notificador=NotificadorFalso(ids=["", ""])
    )

    with pytest.raises(ErrorPlataforma) as error:
        escenario.caso.ejecutar()

    assert error.value.codigo == "NTF-PLA-006"
    assert escenario.notificaciones.marcados == []
    assert escenario.compuesto.consumos == []
    fallidos = [contexto for evento, _, contexto in escenario.bitacora.eventos if evento == EventoLog.CORREO_FALLIDO]
    assert [contexto["codigo"] for contexto in fallidos] == ["NTF-PLA-003", "NTF-PLA-003"]
    assert (escenario.caso.enviados, escenario.caso.fallidos) == (0, 2)


def test_si_falla_un_destinatario_sigue_con_los_demas_y_marca_solo_los_enviados():
    class NotificadorQueFallaPrimero(NotificadorFalso):
        def enviar(self, mensaje, destinatario):
            if not self.enviados:
                self.enviados.append((mensaje, destinatario))
                raise ErrorPlataforma("NTF-PLA-004", "rechazado")
            return super().enviar(mensaje, destinatario)

    datos = copiar_config_caso()
    datos["compositeNotification"] = config_compuesta()
    escenario = armar_escenario(
        config_caso=datos, compuesto=_compuesto_listo(), notificador=NotificadorQueFallaPrimero()
    )

    assert escenario.caso.ejecutar() == ResultadoJob.ENVIADO

    marcado = escenario.notificaciones.marcados[0]
    assert [envio.destinatario for envio in marcado["envios"]] == ["dos@example.com"]
    assert len(escenario.compuesto.consumos) == 1
    nombres = escenario.bitacora.nombres()
    assert EventoLog.CORREO_FALLIDO in nombres and EventoLog.ENVIO_PARCIAL in nombres
    assert (escenario.caso.enviados, escenario.caso.fallidos) == (1, 1)


def test_el_flujo_normal_registra_inicio_y_fin_sin_datos_personales():
    escenario = armar_escenario()
    escenario.caso.ejecutar()

    nombres = escenario.bitacora.nombres()
    contextos = [contexto for _, _, contexto in escenario.bitacora.eventos]

    assert nombres[0] == EventoLog.INICIO_JOB
    assert nombres[-1] == EventoLog.FIN_JOB
    assert not any("example.com" in str(valor) for contexto in contextos for valor in contexto.values())
