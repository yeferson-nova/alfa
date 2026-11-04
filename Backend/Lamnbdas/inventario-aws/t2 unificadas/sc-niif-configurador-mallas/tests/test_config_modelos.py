import pytest

from configurador_mallas.configuracion.ajustes import Ajustes, nivel_log
from configurador_mallas.dominio.errores import ErrorConfiguracion, ErrorEntrada
from configurador_mallas.dominio.modelos import Evento, Resultado

ENTORNO = {
    "AWS_REGION": "us-east-1", "CNF_TABLE": "cnf", "PRS_TABLE": "prs", "PRG_TABLE": "prg",
    "EJC_TABLE": "ejc", "IDEMPOTENCIA_TABLE": "idem", "IDEMPOTENCIA_TTL_SEGUNDOS": "86400",
    "CATALOGO_MALLAS_URI": "s3://assets/catalogo.json", "METRICAS_NAMESPACE": "NIIF/Config",
}


class TestAjustes:
    def test_lee_todas_las_variables(self):
        ajustes = Ajustes.desde_entorno({**ENTORNO, "MALLA_PK": "MALLA#PRIMAS_ARL",
                                         "NIVEL_LOG": "debug"})
        assert ajustes.cnf_table == "cnf"
        assert ajustes.idempotencia_ttl_segundos == 86400
        assert ajustes.malla_por_defecto == "MALLA#PRIMAS_ARL"
        assert ajustes.nivel_log == "DEBUG"

    def test_sin_malla_por_defecto_y_nivel_info(self):
        ajustes = Ajustes.desde_entorno(dict(ENTORNO))
        assert ajustes.malla_por_defecto is None
        assert ajustes.nivel_log == "INFO"

    def test_falla_si_faltan_variables_y_las_nombra(self):
        entorno = {k: v for k, v in ENTORNO.items() if k not in ("CNF_TABLE", "EJC_TABLE")}
        with pytest.raises(ErrorConfiguracion) as error:
            Ajustes.desde_entorno(entorno)
        assert error.value.detalle["variables"] == ["CNF_TABLE", "EJC_TABLE"]
        assert error.value.clasificacion.recuperable is False

    @pytest.mark.parametrize("valor", ["0", "-5", "uno"])
    def test_ttl_debe_ser_entero_positivo(self, valor):
        with pytest.raises(ErrorConfiguracion, match="IDEMPOTENCIA_TTL_SEGUNDOS"):
            Ajustes.desde_entorno({**ENTORNO, "IDEMPOTENCIA_TTL_SEGUNDOS": valor})

    def test_nivel_de_log_invalido(self):
        with pytest.raises(ErrorConfiguracion, match="NIVEL_LOG"):
            nivel_log({"NIVEL_LOG": "TODO"})


class TestEvento:
    def test_valores_por_defecto_de_validacion(self):
        evento = Evento.desde_dict({"malla": "PRIMAS_ARL"})
        assert (evento.publicar, evento.ejecutar) == (False, False)
        assert evento.modo_ejecucion == "NORMAL"
        assert evento.origen == "INVOCACION_DIRECTA"
        assert evento.clave_idempotencia is None

    def test_publicar_implica_ejecutar_salvo_que_se_indique(self):
        assert Evento.desde_dict({"malla": "X1", "publicar": True}).ejecutar is True
        assert Evento.desde_dict({"malla": "X1", "publicar": True,
                                  "ejecutar": False}).ejecutar is False

    def test_contrato_completo_del_scheduler(self):
        evento = Evento.desde_dict({
            "malla": "MALLA#PRIMAS_RENTAS", "publicar": True, "ejecutar": True,
            "periodoNegocio": "202610", "modoEjecucion": "REPROCESO_COMPLETO",
            "origen": "PROGRAMACION_GESTOR", "idempotencyKey": "abc-123:1",
        })
        assert evento.malla == "PRIMAS_RENTAS"
        assert evento.periodo_negocio == "202610"
        assert evento.clave_idempotencia == "abc-123:1"

    def test_toma_la_malla_del_entorno(self):
        assert Evento.desde_dict(None, "MALLA#AUXILIOS_FUNERARIOS").malla == "AUXILIOS_FUNERARIOS"

    @pytest.mark.parametrize("evento, codigo", [
        ({"malla": "X1", "publicar": "false"}, "CFG-ENT-001"),
        ({"malla": "X1", "ejecutar": True, "publicar": False}, "CFG-ENT-001"),
        ({}, "CFG-ENT-004"),
        ({"malla": "  "}, "CFG-ENT-004"),
        ({"malla": "X1", "periodoNegocio": "2026-10"}, "CFG-ENT-002"),
        ({"malla": "X1", "periodoNegocio": "202613"}, "CFG-ENT-002"),
        ({"malla": "X1", "idempotencyKey": "con espacios"}, "CFG-ENT-001"),
        ({"malla": "X1", "idempotencyKey": 7}, "CFG-ENT-001"),
        ({"malla": "X1", "origen": ""}, "CFG-ENT-001"),
        ({"malla": "X1", "modoEjecucion": 1}, "CFG-ENT-001"),
    ])
    def test_rechaza_eventos_invalidos(self, evento, codigo):
        with pytest.raises(ErrorEntrada) as error:
            Evento.desde_dict(evento)
        assert error.value.clasificacion.codigo == codigo

    def test_rechaza_evento_que_no_es_objeto(self):
        with pytest.raises(ErrorEntrada):
            Evento.desde_dict(["publicar"])


def test_resultado_ida_y_vuelta():
    resultado = Resultado(malla="MALLA#X", publicar=True, ejecutar=True, periodo_negocio="202610",
                          registros_cnf=7, ejecucion_id="abc", publicados=[{"uri": "s3://a/b"}],
                          execution_name="n", execution_arn="arn", ejc_pk="p", ejc_sk="s")
    assert Resultado.desde_dict(resultado.to_dict()) == resultado


def test_propiedades_de_la_malla(spec):
    assert spec.pk == "MALLA#PRIMAS_PRUEBA"
    assert spec.slug == "primas-prueba"
    assert spec.ejc_pk("202610") == "EJC#PRIMAS_PRUEBA#202610"
