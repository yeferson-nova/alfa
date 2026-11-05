import copy
from dataclasses import replace

import pytest

from configurador_mallas.dominio import constructores, validaciones
from configurador_mallas.dominio.errores import ErrorConfiguracion
from configurador_mallas.dominio.modelos import FormatoTablaExtraccion, ReglasRemediacion
from conftest import PLANTILLA, PRS, URI_PLANTILLA
from fakes import FakeAlmacen, FakeLectura

SIN_REGLAS = "PROVISIONAL_SIN_REGLAS_TRANSFORMATIVAS"
CON_REGLAS = "DEFINIDA_CON_REGLAS_TRANSFORMATIVAS"
FORMATO_RESERVAS = FormatoTablaExtraccion(
    (("where", "where"), ("dependencia", "dependencia"),
     ("reutilizacionCompartida", "reutilizacion_compartida")),
    omitir_nulos=True,
)


@pytest.fixture
def prs():
    return FakeLectura(PRS)


@pytest.fixture
def almacen():
    return FakeAlmacen({URI_PLANTILLA: PLANTILLA})


def _generados(cnf, prs, almacen, spec):
    return constructores.construir_todos(cnf, prs, almacen, spec)


class TestConstructores:
    def test_extraccion_ordena_ciclos_segmentos_y_tablas(self, cnf, prs, almacen, spec):
        extraccion = _generados(cnf, prs, almacen, spec)["EXTRACCION"]
        assert [c["nombre"] for c in extraccion["ciclos"]] == ["base", "complemento"]
        assert extraccion["ciclos"][1]["descripcion"] == "pensiones aprobadas"
        segmentos = extraccion["ciclos"][1]["segmentos"]
        assert [s["nombre"] for s in segmentos] == ["primero", "segundo"]
        assert segmentos[1]["descripcion"] == "por lote"
        tablas = extraccion["ciclos"][0]["segmentos"][0]["tablas"]
        assert [t["tabla_relativa"] for t in tablas] == ["alfaprd/pagos", "alfaprd/pensiones"]
        assert tablas[1]["dependencia"] is None

    def test_validacion_scd_y_remediacion(self, cnf, prs, almacen, spec):
        generados = _generados(cnf, prs, almacen, spec)
        assert generados["VALIDACION"]["datasets"] == cnf["VALIDACION"]["datasets"]
        assert generados["SCD"]["llaves"] == ["ID"]
        assert generados["REMEDIACION"] == PLANTILLA

    def test_estructura_de_origen_inexistente(self, cnf, almacen, spec):
        with pytest.raises(ErrorConfiguracion) as error:
            constructores.construir_extraccion(cnf, FakeLectura({}), spec)
        assert error.value.clasificacion.codigo == "CFG-CNF-005"

    def test_estructura_gold_inexistente(self, cnf):
        sin_gold = {k: v for k, v in PRS.items() if k[1] != "GLD"}
        with pytest.raises(ErrorConfiguracion, match="Gold"):
            constructores.construir_scd(cnf, FakeLectura(sin_gold))

    def test_estado_de_remediacion_no_permitido(self, cnf, almacen, spec):
        cnf["REMEDIACION"]["estadoDefinicion"] = "DEFINITIVO"
        with pytest.raises(ErrorConfiguracion, match="remediación"):
            constructores.construir_remediacion(cnf, almacen, spec)

    def test_plantilla_de_otra_malla(self, cnf, spec):
        almacen = FakeAlmacen({URI_PLANTILLA: {"nombre_caso": "otra_malla"}})
        with pytest.raises(ErrorConfiguracion, match="plantilla"):
            constructores.construir_remediacion(cnf, almacen, spec)

    @pytest.mark.parametrize("reglas, estado", [([], SIN_REGLAS), ([{"r": 1}], CON_REGLAS)])
    def test_remediacion_con_estado_segun_las_reglas(self, cnf, almacen, spec, reglas, estado):
        spec = replace(spec, remediacion=ReglasRemediacion(SIN_REGLAS, CON_REGLAS))
        cnf["REMEDIACION"].update(reglasTransformativas=reglas, estadoDefinicion=estado)
        assert constructores.construir_remediacion(cnf, almacen, spec) == PLANTILLA

    def test_con_reglas_exige_el_estado_definido(self, cnf, almacen, spec):
        spec = replace(spec, remediacion=ReglasRemediacion(SIN_REGLAS, CON_REGLAS))
        cnf["REMEDIACION"]["reglasTransformativas"] = [{"r": 1}]
        with pytest.raises(ErrorConfiguracion) as error:
            constructores.construir_remediacion(cnf, almacen, spec)
        assert error.value.detalle == {"esperado": CON_REGLAS, "actual": SIN_REGLAS}

    def test_sin_estado_con_reglas_las_reglas_no_cambian_lo_esperado(self, cnf, almacen, spec):
        cnf["REMEDIACION"]["reglasTransformativas"] = [{"r": 1}]
        assert constructores.construir_remediacion(cnf, almacen, spec) == PLANTILLA

    def test_tablas_por_defecto_llevan_los_opcionales_aunque_sean_nulos(self, cnf, prs, spec):
        tabla = constructores.construir_extraccion(cnf, prs, spec)["ciclos"][0][
            "segmentos"][0]["tablas"][1]
        assert tabla["where"] is None and tabla["dependencia"] is None
        assert "reutilizacion_compartida" not in tabla

    def test_tablas_de_reservas_omiten_nulos_y_copian_reutilizacion(self, cnf, prs, spec):
        uso = cnf["CICLO#001"]["segmentos"][0]["estructuras"][1]
        uso.update(dependencia="SRC#PENSIONES", reutilizacionCompartida=True)
        spec = replace(spec, formato_tabla=FORMATO_RESERVAS)
        tablas = constructores.construir_extraccion(cnf, prs, spec)["ciclos"][0][
            "segmentos"][0]["tablas"]
        assert tablas[0]["reutilizacion_compartida"] is True
        assert tablas[0]["dependencia"] == "SRC#PENSIONES"
        assert not {"where", "dependencia", "reutilizacion_compartida"} & set(tablas[1])

    def test_cuenta_las_estructuras_distintas_mas_la_gold(self, cnf, spec):
        assert constructores.contar_estructuras_prs(cnf, spec) == 3


class TestValidaciones:
    def test_registros_completos(self, cnf, spec):
        validaciones.validar_registros_cnf(cnf, spec)

    def test_registros_faltantes(self, cnf, spec):
        del cnf["SCD"], cnf["CICLO#002"]
        with pytest.raises(ErrorConfiguracion) as error:
            validaciones.validar_registros_cnf(cnf, spec)
        assert error.value.detalle["faltantes"] == ["CICLO#002", "SCD"]

    @pytest.mark.parametrize("sobrante", ["CICLO#003", "OTRO"])
    def test_registros_de_mas(self, cnf, spec, sobrante):
        cnf[sobrante] = copy.deepcopy(cnf["CICLO#002"])
        with pytest.raises(ErrorConfiguracion) as error:
            validaciones.validar_registros_cnf(cnf, spec)
        assert error.value.detalle["sobrantes"] == [sobrante]

    def test_nombre_de_caso_distinto_al_catalogo(self, cnf, spec):
        cnf["GENERAL"]["nombreCaso"] = "otra"
        with pytest.raises(ErrorConfiguracion) as error:
            validaciones.validar_registros_cnf(cnf, spec)
        assert error.value.clasificacion.codigo == "CFG-CNF-010"

    @pytest.fixture
    def generados(self, cnf, prs, almacen, spec):
        from configurador_mallas.dominio import periodo
        preparado, _ = periodo.preparar_periodo(cnf, "202610", spec.reglas_periodo)
        resultado = _generados(preparado, prs, almacen, spec)
        resultado["EXTRACCION"] = periodo.resolver_estructura(resultado["EXTRACCION"],
                                                              "202610", "202610")
        return resultado

    def test_generados_validos(self, generados, spec):
        validaciones.validar_generados(generados, spec)

    def test_json_vacio(self, generados, spec):
        generados["SCD"] = {}
        with pytest.raises(ErrorConfiguracion, match="vacío"):
            validaciones.validar_generados(generados, spec)

    def test_marcadores_sin_resolver(self, generados, spec):
        generados["VALIDACION"]["salida"] = {"ruta": "s3://x/${FECHA_FIN}"}
        with pytest.raises(ErrorConfiguracion) as error:
            validaciones.validar_generados(generados, spec)
        assert error.value.detalle["marcadores"] == ["${FECHA_FIN}"]

    def test_periodo_inconsistente(self, generados, spec):
        generados["EXTRACCION"]["control_extraccion"]["periodo_negocio"] = "202609"
        with pytest.raises(ErrorConfiguracion, match="Periodo inconsistente"):
            validaciones.validar_generados(generados, spec)

    def test_segmento_con_otro_periodo(self, generados, spec):
        tabla = generados["EXTRACCION"]["ciclos"][1]["segmentos"][1]["tablas"][0]
        tabla["segmentacion_horizontal"]["periodo_negocio"] = "202609"
        with pytest.raises(ErrorConfiguracion, match="segmento"):
            validaciones.validar_generados(generados, spec)

    def test_identificador_de_segmento_con_otro_periodo(self, generados, spec):
        tabla = generados["EXTRACCION"]["ciclos"][1]["segmentos"][1]["tablas"][0]
        tabla["segmentacion_horizontal"]["ruta"] = "s3://bronce/niif17_202501_x/"
        with pytest.raises(ErrorConfiguracion, match="Identificador"):
            validaciones.validar_generados(generados, spec)

    def test_sin_ajuste_de_segmentacion_no_se_validan_los_segmentos(self, generados, spec):
        tabla = generados["EXTRACCION"]["ciclos"][1]["segmentos"][1]["tablas"][0]
        tabla["segmentacion_horizontal"]["periodo_negocio"] = "202609"
        spec = replace(spec, reglas_periodo=replace(spec.reglas_periodo,
                                                    ajustar_segmentacion=False))
        validaciones.validar_generados(generados, spec)
