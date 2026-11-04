import copy

import pytest

from configurador_mallas.dominio.errores import (ErrorConcurrencia, ErrorConfiguracion, ErrorEntrada,
                                         ErrorPlataforma)
from configurador_mallas.aplicacion.idempotencia import (COMPLETADO, EN_PROCESO, FALLIDO,
                                              ControlIdempotencia, ejecucion_id)
from configurador_mallas.dominio.modelos import Evento, Invocacion
from conftest import AHORA, MALLA, PRG, Entorno
from fakes import FakeIdempotencia

INVOCACION = Invocacion("req-0001", 60_000)


def _evento(**campos):
    return Evento.desde_dict({"malla": MALLA, **campos})


def _ejecutar(entorno, capturador, evento, invocacion=INVOCACION):
    return entorno.servicio.ejecutar(evento, invocacion, capturador.observador)


class TestFlujo:
    def test_validacion_no_escribe_ni_ejecuta(self, entorno, capturador):
        resultado = _ejecutar(entorno, capturador, _evento())
        assert resultado.estado == "VALIDADO"
        assert resultado.periodo_negocio == "202610"
        assert resultado.registros_cnf == 7
        assert entorno.almacen.escrituras == []
        assert entorno.cnf.persistencias == []
        assert entorno.orquestador.llamadas == 0
        assert capturador.eventos() == ["CORRIDA_INICIADA", "CONFIGURACION_LEIDA",
                                        "CORRIDA_FINALIZADA"]

    def test_publicar_actualiza_cnf_y_escribe_los_cuatro_json(self, entorno, capturador):
        resultado = _ejecutar(entorno, capturador, _evento(publicar=True, ejecutar=False))
        assert len(resultado.publicados) == 4
        assert entorno.cnf.persistencias == [3]  # EXTRACCION y los dos ciclos
        uri, metadatos = entorno.almacen.escrituras[0]
        assert uri == "s3://assets/primas-prueba/config_extraccion.json"
        assert metadatos == {"origen": "dynamodb-prs-cnf", "malla": "primas-prueba",
                             "ejecucion-id": resultado.ejecucion_id}
        assert entorno.almacen.archivos[uri]["periodo_negocio"] == "202610"
        assert entorno.orquestador.llamadas == 0

    def test_ejecutar_inicia_la_maquina_y_registra_la_ejecucion(self, entorno, capturador):
        resultado = _ejecutar(entorno, capturador, _evento(publicar=True,
                                                           origen="PROGRAMACION_GESTOR"))
        nombre = f"prueba-202610-auto-{ejecucion_id(MALLA, 'req-0001')}"
        assert resultado.execution_name == nombre
        maquina, entrada = entorno.orquestador.ejecuciones[nombre]
        assert maquina == "niif-dev-primas-prueba"
        assert entrada == {
            "ejcPK": "EJC#PRIMAS_PRUEBA#202610", "ejcSK": f"EJECUCION#{nombre}",
            "malla": MALLA, "periodoNegocio": "202610", "modoEjecucion": "NORMAL",
            "rutaConfigExtraccion": "s3://assets/primas-prueba/config_extraccion.json",
            "origen": "PROGRAMACION_GESTOR", "ejecucionId": resultado.ejecucion_id,
            "fecDesde": "2026-10-01", "fecHasta": "2026-10-31",
        }
        item = entorno.ejecuciones.items[(resultado.ejc_pk, resultado.ejc_sk)]
        assert item["estado"] == "INICIADA"
        assert item["executionArn"] == resultado.execution_arn
        assert "EJECUCION_INICIADA" in capturador.eventos()

    def test_sin_rango_de_fechas_si_el_catalogo_no_lo_pide(self, documento_catalogo, capturador):
        from configurador_mallas.dominio import catalogo
        documento_catalogo["mallas"][MALLA]["incluirRangoFechasEnEntrada"] = False
        entorno = Entorno(catalogo.parsear(documento_catalogo))
        resultado = _ejecutar(entorno, capturador, _evento(publicar=True))
        _, entrada = entorno.orquestador.ejecuciones[resultado.execution_name]
        assert "fecDesde" not in entrada and "fecHasta" not in entrada

    def test_el_periodo_del_evento_debe_coincidir_con_la_programacion(self, entorno, capturador):
        assert _ejecutar(entorno, capturador, _evento(periodoNegocio="202610")).periodo_negocio \
            == "202610"
        with pytest.raises(ErrorEntrada) as error:
            _ejecutar(entorno, capturador, _evento(periodoNegocio="202611",
                                                   idempotencyKey="otra"))
        assert error.value.clasificacion.codigo == "CFG-ENT-003"

    @pytest.mark.parametrize("prg", [{}, {(f"PROGRAMACION#{MALLA}", "METADATA"):
                                          {"periodoNegocio": "2026-10"}}])
    def test_programacion_inexistente_o_invalida(self, cat, capturador, prg):
        entorno = Entorno(cat, prg=prg)
        with pytest.raises(ErrorConfiguracion) as error:
            _ejecutar(entorno, capturador, _evento())
        assert error.value.clasificacion.codigo == "CFG-CNF-006"

    def test_modo_de_ejecucion_no_permitido(self, entorno, capturador):
        with pytest.raises(ErrorEntrada) as error:
            _ejecutar(entorno, capturador, _evento(modoEjecucion="OTRO"))
        assert error.value.clasificacion.codigo == "CFG-ENT-005"

    def test_malla_que_no_esta_en_el_catalogo(self, entorno, capturador):
        with pytest.raises(ErrorConfiguracion):
            entorno.servicio.ejecutar(Evento.desde_dict({"malla": "OTRA"}), INVOCACION,
                                      capturador.observador)

    def test_los_logs_llevan_ejecucion_id_y_malla(self, entorno, capturador):
        resultado = _ejecutar(entorno, capturador, _evento())
        eventos = [linea for linea in capturador.lineas() if "evento" in linea]
        assert all(linea["ejecucion_id"] == resultado.ejecucion_id for linea in eventos)
        assert all(linea["malla"] == MALLA for linea in eventos)
        assert eventos[-1]["periodo_negocio"] == "202610"


class TestIdempotencia:
    def test_mismo_evento_dos_veces_no_repite_efectos(self, entorno, capturador):
        evento = _evento(publicar=True, idempotencyKey="sched-1")
        primero = _ejecutar(entorno, capturador, evento)
        segundo = _ejecutar(entorno, capturador, evento, Invocacion("req-0002", 60_000))
        assert segundo.reutilizado is True
        assert segundo.execution_arn == primero.execution_arn
        assert entorno.orquestador.llamadas == 1
        assert len(entorno.almacen.escrituras) == 4
        assert "IDEMPOTENCIA_REUTILIZADA" in capturador.eventos()

    def test_otra_clave_es_otra_corrida(self, entorno, capturador):
        _ejecutar(entorno, capturador, _evento(publicar=True, idempotencyKey="a"))
        _ejecutar(entorno, capturador, _evento(publicar=True, idempotencyKey="b"))
        assert entorno.orquestador.llamadas == 2
        assert entorno.cnf.persistencias == [3, 0]  # el segundo ya encuentra el periodo aplicado

    def test_reintento_tras_falla_a_medias_no_duplica(self, entorno, capturador):
        evento = _evento(publicar=True, idempotencyKey="k1")
        original = entorno.ejecuciones.registrar

        def falla_una_vez(item):
            entorno.ejecuciones.registrar = original
            original(item)
            raise RuntimeError("se cayó la red después de escribir")

        entorno.ejecuciones.registrar = falla_una_vez
        with pytest.raises(ErrorPlataforma):
            _ejecutar(entorno, capturador, evento)
        assert entorno.idempotencia.estado(MALLA, "k1") == FALLIDO

        resultado = _ejecutar(entorno, capturador, evento)
        assert resultado.reutilizado is False
        assert len(entorno.orquestador.ejecuciones) == 1
        assert len(entorno.ejecuciones.items) == 1
        assert "EJECUCION_YA_REGISTRADA" in capturador.eventos()
        assert entorno.idempotencia.estado(MALLA, "k1") == COMPLETADO

    def test_error_clasificado_queda_como_fallido(self, entorno, capturador):
        entorno.prg.datos.clear()
        with pytest.raises(ErrorConfiguracion):
            _ejecutar(entorno, capturador, _evento(idempotencyKey="k2"))
        registro = entorno.idempotencia.registros[(MALLA, "k2")]
        assert registro["estado"] == FALLIDO
        assert registro["error"]["codigo"] == "CFG-CNF-006"

    def test_otra_invocacion_en_curso(self, entorno, capturador):
        entorno.idempotencia.registros[(MALLA, "k3")] = {
            "estado": EN_PROCESO, "bloqueoHasta": AHORA + 100, "expiraEn": AHORA + 3600}
        with pytest.raises(ErrorConcurrencia) as error:
            _ejecutar(entorno, capturador, _evento(idempotencyKey="k3"))
        assert error.value.clasificacion.recuperable is True

    def test_bloqueo_vencido_se_retoma(self, entorno, capturador):
        entorno.idempotencia.registros[(MALLA, "k4")] = {
            "estado": EN_PROCESO, "bloqueoHasta": AHORA - 1, "expiraEn": AHORA + 3600}
        assert _ejecutar(entorno, capturador, _evento(idempotencyKey="k4")).reutilizado is False


class TestControlIdempotencia:
    def test_ejecucion_id_es_estable_y_depende_de_malla_y_clave(self):
        assert ejecucion_id("A1", "k") == ejecucion_id("A1", "k")
        assert ejecucion_id("A1", "k") != ejecucion_id("B1", "k")
        assert len(ejecucion_id("A1", "k")) == 12

    def test_bloqueo_usa_el_tiempo_restante_de_la_lambda(self):
        repositorio = FakeIdempotencia()
        control = ControlIdempotencia(repositorio, 600, lambda: AHORA)
        assert control.iniciar("M1", "k", 30_000) is None
        registro = repositorio.registros[("M1", "k")]
        assert registro["bloqueoHasta"] == AHORA + 30 + 5
        assert registro["expiraEn"] == AHORA + 600

    def test_completado_sin_resultado_se_trata_como_en_curso(self):
        repositorio = FakeIdempotencia()
        repositorio.registros[("M1", "k")] = {"estado": COMPLETADO, "bloqueoHasta": 0}
        with pytest.raises(ErrorConcurrencia):
            ControlIdempotencia(repositorio, 600, lambda: AHORA).iniciar("M1", "k", 1000)


def test_prg_original_no_se_modifica():
    assert PRG == {(f"PROGRAMACION#{MALLA}", "METADATA"): {"periodoNegocio": "202610"}}
    assert copy.deepcopy(PRG) == PRG
