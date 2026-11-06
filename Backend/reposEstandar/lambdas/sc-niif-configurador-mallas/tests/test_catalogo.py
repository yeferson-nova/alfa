import json
from pathlib import Path

import pytest

from configurador_mallas.dominio import catalogo
from configurador_mallas.dominio.errores import ErrorConfiguracion
from conftest import MALLA

CATALOGO_DEV = Path(__file__).resolve().parents[1] / "config" / "catalogo-mallas.dev.json"


def test_convierte_el_documento_en_especificaciones(cat):
    spec = cat.obtener(MALLA)
    assert cat.modos_ejecucion == ("NORMAL", "REPROCESO_COMPLETO")
    assert (cat.modo_por_defecto, cat.origen_por_defecto) == ("NORMAL", "CONFIGURADOR")
    assert spec.state_machine == "niif-dev-primas-prueba"
    assert spec.total_ciclos == 2
    assert spec.reglas_periodo.columnas_fecha == ("FEC_INICIAL", "FEC_APROBACION_PENSION")
    assert spec.reglas_periodo.operadores_fecha == (">=", "<", "<=")
    assert spec.reglas_periodo.ajustar_segmentacion is True
    assert spec.remediacion.estado_con_reglas is None
    assert spec.formato_tabla.campos_opcionales == (("where", "where"),
                                                     ("dependencia", "dependencia"))
    assert spec.entrada_orquestacion.incluir_rango_fechas is True


def test_malla_desconocida_lista_las_disponibles(cat):
    with pytest.raises(ErrorConfiguracion) as error:
        cat.obtener("NO_EXISTE")
    assert error.value.clasificacion.codigo == "CFG-CNF-003"
    assert error.value.detalle["disponibles"] == [MALLA]


def test_el_catalogo_de_dev_del_repositorio_es_valido():
    cat = catalogo.parsear(json.loads(CATALOGO_DEV.read_text(encoding="utf-8")))
    assert len(cat.mallas) == 6
    assert cat.origen_por_defecto == "CONFIGURADOR"  # el mismo que usaban las seis lambdas
    con_rango = {c for c, s in cat.mallas.items() if s.entrada_orquestacion.incluir_rango_fechas}
    assert con_rango == {"AUXILIOS_FUNERARIOS"}
    reservas = {"RESERVA_MATEMATICA_RVI", "RESERVA_MATEMATICA_ARP"}
    con_reglas = {c for c, s in cat.mallas.items() if s.remediacion.estado_con_reglas}
    assert con_reglas == reservas
    sin_ruta = {c for c, s in cat.mallas.items()
                if not s.entrada_orquestacion.incluir_ruta_config_extraccion}
    assert sin_ruta == reservas
    omiten_nulos = {c for c, s in cat.mallas.items() if s.formato_tabla.omitir_nulos}
    assert omiten_nulos == reservas


def _romper(documento, ruta, valor):
    destino = documento["mallas"][MALLA]
    for clave in ruta[:-1]:
        destino = destino[clave]
    if valor is _BORRAR:
        del destino[ruta[-1]]
    else:
        destino[ruta[-1]] = valor


_BORRAR = object()


@pytest.mark.parametrize("ruta, valor", [
    (("prefijoEjecucion",), "Con Mayusculas"),
    (("stateMachineName",), "nombre con espacios"),
    (("nombreCaso",), ""),
    (("totalCiclos",), 0),
    (("totalCiclos",), True),
    (("programacion",), _BORRAR),
    (("programacion", "sk"), _BORRAR),
    (("remediacion",), _BORRAR),
    (("remediacion", "estadoSinReglas"), ""),
    (("remediacion", "estadoConReglas"), ""),
    (("periodo",), _BORRAR),
    (("periodo", "columnasFecha"), ["FEC VALIDA"]),
    (("periodo", "columnasFecha"), "FEC_INICIAL"),
    (("periodo", "operadoresFecha"), _BORRAR),
    (("periodo", "operadoresFecha"), ["="]),
    (("periodo", "operadoresFecha"), []),
    (("periodo", "columnaPeriodo"), "1PERIODO"),
    (("periodo", "ajustarSegmentacion"), "si"),
    (("periodo", "patronIdentificadorSegmento"), "niif17_(["),
    (("periodo", "patronIdentificadorSegmento"), "sin_grupo"),
    (("tablaExtraccion",), _BORRAR),
    (("tablaExtraccion", "camposOpcionales"), ["where"]),
    (("tablaExtraccion", "camposOpcionales"), {"where": "con espacio"}),
    (("tablaExtraccion", "camposOpcionales"), {"where": 1}),
    (("tablaExtraccion", "omitirNulos"), _BORRAR),
    (("entradaStepFunction",), _BORRAR),
    (("entradaStepFunction", "incluirRangoFechas"), "si"),
    (("entradaStepFunction", "incluirRutaConfigExtraccion"), _BORRAR),
])
def test_rechaza_definiciones_invalidas(documento_catalogo, ruta, valor):
    _romper(documento_catalogo, ruta, valor)
    with pytest.raises(ErrorConfiguracion) as error:
        catalogo.parsear(documento_catalogo)
    assert error.value.clasificacion.codigo == "CFG-CNF-002"


_DEFECTOS = {"modoEjecucion": "NORMAL", "origen": "CONFIGURADOR"}


@pytest.mark.parametrize("documento", [
    [],
    {"version": "1", "modosEjecucion": ["NORMAL"], "valoresPorDefecto": _DEFECTOS, "mallas": {}},
    {"version": "1", "modosEjecucion": [], "valoresPorDefecto": _DEFECTOS, "mallas": {"X1": {}}},
    {"modosEjecucion": ["NORMAL"], "valoresPorDefecto": _DEFECTOS, "mallas": {"X1": {}}},
    {"version": "1", "modosEjecucion": ["NORMAL"], "valoresPorDefecto": _DEFECTOS,
     "mallas": {"minusculas": {}}},
    {"version": "1", "modosEjecucion": ["NORMAL"], "valoresPorDefecto": _DEFECTOS,
     "mallas": {"X1": []}},
])
def test_rechaza_documentos_mal_formados(documento):
    with pytest.raises(ErrorConfiguracion):
        catalogo.parsear(documento)


@pytest.mark.parametrize("defectos", [
    None,
    {"origen": "CONFIGURADOR"},
    {"modoEjecucion": "NORMAL"},
    {"modoEjecucion": "OTRO", "origen": "CONFIGURADOR"},
])
def test_valores_por_defecto_obligatorios_y_validos(documento_catalogo, defectos):
    if defectos is None:
        del documento_catalogo["valoresPorDefecto"]
    else:
        documento_catalogo["valoresPorDefecto"] = defectos
    with pytest.raises(ErrorConfiguracion) as error:
        catalogo.parsear(documento_catalogo)
    assert error.value.clasificacion.codigo == "CFG-CNF-002"


def test_patron_y_columna_de_periodo_son_opcionales(documento_catalogo):
    periodo = documento_catalogo["mallas"][MALLA]["periodo"]
    del periodo["patronIdentificadorSegmento"]
    del periodo["columnaPeriodo"]
    reglas = catalogo.parsear(documento_catalogo).obtener(MALLA).reglas_periodo
    assert reglas.patron_segmento is None
    assert reglas.columna_periodo is None


def test_sin_columnas_de_fecha_los_operadores_pueden_ir_vacios(documento_catalogo):
    documento_catalogo["mallas"][MALLA]["periodo"].update(columnasFecha=[], operadoresFecha=[])
    reglas = catalogo.parsear(documento_catalogo).obtener(MALLA).reglas_periodo
    assert reglas.columnas_fecha == () and reglas.operadores_fecha == ()


def test_estado_con_reglas_opcional(documento_catalogo):
    documento_catalogo["mallas"][MALLA]["remediacion"]["estadoConReglas"] = "DEFINIDA"
    spec = catalogo.parsear(documento_catalogo).obtener(MALLA)
    assert spec.remediacion.estado_con_reglas == "DEFINIDA"
