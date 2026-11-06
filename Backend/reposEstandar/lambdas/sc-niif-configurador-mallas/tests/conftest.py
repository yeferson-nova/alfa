"""Datos de prueba: una malla ficticia de dos ciclos, con la forma real de CNF y PRS."""
import copy
import io
import json
import logging

import pytest

from configurador_mallas.dominio import catalogo
from configurador_mallas.aplicacion.idempotencia import ControlIdempotencia
from configurador_mallas.adaptadores.salida.cloudwatch import Observador, crear_logger
from configurador_mallas.aplicacion.configurar_malla import ConfiguradorMallas
from fakes import (FakeAlmacen, FakeCnf, FakeEjecuciones, FakeIdempotencia, FakeLectura,
                   FakeOrquestador)

MALLA = "PRIMAS_PRUEBA"
PK = f"MALLA#{MALLA}"
AHORA = 1_790_000_000  # instante fijo (2026-09) para pruebas deterministas
URI_PLANTILLA = "s3://assets/plantillas/remediacion.json"

CATALOGO_DOC = {
    "version": "1.0.0",
    "modosEjecucion": ["NORMAL", "REPROCESO_COMPLETO"],
    "valoresPorDefecto": {"modoEjecucion": "NORMAL", "origen": "CONFIGURADOR"},
    "mallas": {
        MALLA: {
            "nombreCaso": "primas_prueba",
            "prefijoEjecucion": "prueba",
            "stateMachineName": "niif-dev-primas-prueba",
            "programacion": {"pk": f"PROGRAMACION#{MALLA}", "sk": "METADATA"},
            "totalCiclos": 2,
            "remediacion": {"estadoSinReglas": "PROVISIONAL_SIN_REGLAS_TRANSFORMATIVAS",
                            "estadoConReglas": None},
            "periodo": {
                "columnasFecha": ["FEC_INICIAL", "FEC_APROBACION_PENSION"],
                "operadoresFecha": [">=", "<", "<="],
                "columnaPeriodo": "PERIODO",
                "ajustarSegmentacion": True,
                "patronIdentificadorSegmento": "niif17_([0-9]{6})_",
            },
            "tablaExtraccion": {
                "camposOpcionales": {"where": "where", "dependencia": "dependencia"},
                "omitirNulos": False,
            },
            "entradaStepFunction": {"incluirRutaConfigExtraccion": True,
                                    "incluirRangoFechas": True},
        }
    },
}


def _registro(sk, **campos):
    return {"PK": PK, "SK": sk, **campos}


CNF = {
    "GENERAL": _registro("GENERAL", nombreCaso="primas_prueba"),
    "EXTRACCION": _registro(
        "EXTRACCION",
        archivoActivoUri="s3://assets/primas-prueba/config_extraccion.json",
        fuente={"conexion": "oracle"},
        rutas={"bronce": "s3://bronce/niif/"},
        controlExtraccion={"periodo_negocio": "202609",
                           "ruta_estado": "s3://assets/estado/${PERIODO_NEGOCIO}/estado.json"},
        salidaRuntime={"ruta": "s3://assets/primas-prueba/parametros_extraccion.json"},
    ),
    "VALIDACION": _registro(
        "VALIDACION",
        archivoActivoUri="s3://assets/primas-prueba/config_val_tec.json",
        parametrosExtraccion={"ruta": "s3://assets/primas-prueba/parametros_extraccion.json"},
        salida={"ruta": "s3://silver/validacion/"},
        datasets=[{"tabla": "alfaprd/pagos", "reglas": []}],
    ),
    "REMEDIACION": _registro(
        "REMEDIACION",
        archivoActivoUri="s3://assets/primas-prueba/config_remediacion.json",
        plantillaUri=URI_PLANTILLA,
        estadoDefinicion="PROVISIONAL_SIN_REGLAS_TRANSFORMATIVAS",
    ),
    "SCD": _registro(
        "SCD",
        archivoActivoUri="s3://assets/primas-prueba/config_scd.json",
        estructuraGoldRef="GLD#SCD_PRIMAS",
        salida={"ruta": "s3://gold/niif/scd_primas_prueba/"},
        llaves=["ID"],
        columnasCambio=["VALOR"],
        columnasFijas=["ID"],
        columnasAdministrativas=["FEC_CARGA"],
        metadatos={"version": 1},
    ),
    "CICLO#001": _registro(
        "CICLO#001", orden=1, nombre="base",
        segmentos=[{
            "orden": 1, "nombre": "pagos", "forma": "COMPLETA",
            "estructuras": [
                {"orden": 2, "estructuraRef": "SRC#PENSIONES", "tipoTabla": "DIMENSION",
                 "where": None, "segmentacionHorizontal": {}},
                {"orden": 1, "estructuraRef": "SRC#PAGOS", "tipoTabla": "HECHO",
                 "where": ("p.FEC_INICIAL >= DATE '2026-09-01' AND FEC_INICIAL < DATE "
                           "'2026-10-01' AND PERIODO = '202609'"),
                 "dependencia": None, "segmentacionHorizontal": {}},
            ],
        }],
    ),
    "CICLO#002": _registro(
        "CICLO#002", orden=2, nombre="complemento", descripcion="pensiones aprobadas",
        segmentos=[
            {"orden": 2, "nombre": "segundo", "forma": "SEGMENTADA", "descripcion": "por lote",
             "estructuras": [{
                 "orden": 1, "estructuraRef": "SRC#PENSIONES", "tipoTabla": "DIMENSION",
                 "where": "FEC_APROBACION_PENSION <= DATE '2026-09-30'",
                 "segmentacionHorizontal": {"periodo_negocio": "202609",
                                            "identificador": "niif17_202609_lote",
                                            "ruta": "s3://bronce/niif17_202609_x/"},
             }]},
            {"orden": 1, "nombre": "primero", "forma": "COMPLETA",
             "estructuras": [{"orden": 1, "estructuraRef": "SRC#PAGOS", "tipoTabla": "HECHO",
                              "where": None, "segmentacionHorizontal": {}}]},
        ],
    ),
}

PRS = {
    ("SRC#PAGOS", "SRC"): {"schema": "ALFAPRD", "tabla": "PAGOS"},
    ("SRC#PENSIONES", "SRC"): {"schema": "ALFAPRD", "tabla": "PENSIONES"},
    ("GLD#SCD_PRIMAS", "GLD"): {"schema": "GOLD", "tabla": "SCD_PRIMAS"},
}
PRG = {(f"PROGRAMACION#{MALLA}", "METADATA"): {"periodoNegocio": "202610"}}
PLANTILLA = {"nombre_caso": "primas_prueba", "reglas": []}


@pytest.fixture
def documento_catalogo():
    return copy.deepcopy(CATALOGO_DOC)


@pytest.fixture
def cat():
    return catalogo.parsear(copy.deepcopy(CATALOGO_DOC))


@pytest.fixture
def spec(cat):
    return cat.obtener(MALLA)


@pytest.fixture
def cnf():
    return copy.deepcopy(CNF)


class Capturador:
    """Observador real con salida a memoria, para revisar los logs JSON."""

    def __init__(self):
        self.salida = io.StringIO()
        self.observador = Observador(crear_logger(logging.DEBUG, self.salida),
                                     "Pruebas/Configurador", "funcion-prueba",
                                     reloj=lambda: AHORA)

    def lineas(self):
        return [json.loads(linea) for linea in self.salida.getvalue().splitlines()]

    def eventos(self):
        return [linea["evento"] for linea in self.lineas() if "evento" in linea]


@pytest.fixture
def capturador():
    return Capturador()


class Entorno:
    """Servicio armado con dobles en memoria."""

    def __init__(self, cat, cnf=CNF, prg=PRG):
        self.cnf = FakeCnf(cnf)
        self.prs = FakeLectura(PRS)
        self.prg = FakeLectura(prg)
        self.almacen = FakeAlmacen({URI_PLANTILLA: PLANTILLA})
        self.orquestador = FakeOrquestador()
        self.ejecuciones = FakeEjecuciones()
        self.idempotencia = FakeIdempotencia()
        self.servicio = ConfiguradorMallas(
            catalogo=cat, cnf=self.cnf, prs=self.prs, programaciones=self.prg,
            almacen=self.almacen, orquestador=self.orquestador, ejecuciones=self.ejecuciones,
            idempotencia=ControlIdempotencia(self.idempotencia, 3600, lambda: AHORA),
            reloj=lambda: AHORA,
        )


@pytest.fixture
def entorno(cat):
    return Entorno(cat)
