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
    assert spec.state_machine == "niif-dev-primas-prueba"
    assert spec.total_ciclos == 2
    assert spec.reglas_periodo.columnas_fecha == ("FEC_INICIAL", "FEC_APROBACION_PENSION")
    assert spec.incluir_rango_fechas is True


def test_malla_desconocida_lista_las_disponibles(cat):
    with pytest.raises(ErrorConfiguracion) as error:
        cat.obtener("NO_EXISTE")
    assert error.value.clasificacion.codigo == "CFG-CNF-003"
    assert error.value.detalle["disponibles"] == [MALLA]


def test_el_catalogo_de_dev_del_repositorio_es_valido():
    cat = catalogo.parsear(json.loads(CATALOGO_DEV.read_text(encoding="utf-8")))
    assert len(cat.mallas) == 6
    assert cat.obtener("AUXILIOS_FUNERARIOS").incluir_rango_fechas is True


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
    (("incluirRangoFechasEnEntrada",), "si"),
    (("programacion",), _BORRAR),
    (("programacion", "sk"), _BORRAR),
    (("estadosRemediacionPermitidos",), []),
    (("periodo", "columnasFecha"), ["FEC VALIDA"]),
    (("periodo", "columnasFecha"), "FEC_INICIAL"),
    (("periodo", "columnaPeriodo"), "1PERIODO"),
    (("periodo", "patronIdentificadorSegmento"), "niif17_(["),
    (("periodo", "patronIdentificadorSegmento"), "sin_grupo"),
])
def test_rechaza_definiciones_invalidas(documento_catalogo, ruta, valor):
    _romper(documento_catalogo, ruta, valor)
    with pytest.raises(ErrorConfiguracion) as error:
        catalogo.parsear(documento_catalogo)
    assert error.value.clasificacion.codigo == "CFG-CNF-002"


@pytest.mark.parametrize("documento", [
    [],
    {"version": "1", "modosEjecucion": ["NORMAL"], "mallas": {}},
    {"version": "1", "modosEjecucion": [], "mallas": {"X1": {}}},
    {"modosEjecucion": ["NORMAL"], "mallas": {"X1": {}}},
    {"version": "1", "modosEjecucion": ["NORMAL"], "mallas": {"minusculas": {}}},
    {"version": "1", "modosEjecucion": ["NORMAL"], "mallas": {"X1": []}},
])
def test_rechaza_documentos_mal_formados(documento):
    with pytest.raises(ErrorConfiguracion):
        catalogo.parsear(documento)


def test_patron_y_columna_de_periodo_son_opcionales(documento_catalogo):
    periodo = documento_catalogo["mallas"][MALLA]["periodo"]
    del periodo["patronIdentificadorSegmento"]
    del periodo["columnaPeriodo"]
    reglas = catalogo.parsear(documento_catalogo).obtener(MALLA).reglas_periodo
    assert reglas.patron_segmento is None
    assert reglas.columna_periodo is None
