"""Equivalencia con las seis lambdas ``niif-dev-configuration-<malla>`` actuales.

Ejecuta el código de cada lambda actual con AWS simulado y la lambda nueva con los
mismos datos, y compara todos sus efectos: los cuatro JSON y sus metadatos en S3,
la escritura del periodo en CNF, la entrada de la Step Function, el registro EJC y
la respuesta. Así se comprueba que la lambda nueva hace todo lo que hacen las seis.

- Auxilios funerarios se compara con el catálogo de DEV tal cual (su código es el
  desplegado).
- Las otras cinco son copias de la versión anterior (ver su ``ORIGEN.md``), con
  reglas de periodo más simples que las desplegadas. Se comparan con su entrada del
  catálogo de DEV cambiando solo el bloque ``periodo`` por el de la copia: eso
  prueba que el resto del catálogo es fiel y que el motor reproduce sus reglas.

La carpeta de las lambdas actuales se toma de ``LAMBDAS_ACTUALES_DIR`` (por
defecto, la del repositorio). Si no existe, las pruebas se omiten.
"""
import copy
import importlib.util
import io
import json
import os
from pathlib import Path

import boto3
import pytest

from configurador_mallas.adaptadores.salida.dynamo import DynamoCnf
from configurador_mallas.aplicacion.configurar_malla import ConfiguradorMallas
from configurador_mallas.aplicacion.idempotencia import ControlIdempotencia
from configurador_mallas.dominio import catalogo
from configurador_mallas.dominio.modelos import Evento, Invocacion
from conftest import AHORA
from fakes import FakeAlmacen, FakeEjecuciones, FakeIdempotencia, FakeLectura, FakeOrquestador

RAIZ = Path(__file__).resolve().parents[1]
DIRECTORIO = Path(os.environ.get("LAMBDAS_ACTUALES_DIR", RAIZ.parents[2]))
CATALOGO_DEV = json.loads((RAIZ / "config" / "catalogo-mallas.dev.json").read_text("utf-8"))

PERIODO_CNF, PERIODO_PRG = "202609", "202610"
TABLAS = {"CNF_TABLE": "cnf", "PRS_TABLE": "prs", "PRG_TABLE": "prg", "EJC_TABLE": "ejc"}
SIN_REGLAS = "PROVISIONAL_SIN_REGLAS_TRANSFORMATIVAS"
CON_REGLAS = "DEFINIDA_CON_REGLAS_TRANSFORMATIVAS"

# Reglas de periodo de las copias anteriores (solo el corte de pensión; ARP, el rango completo).
_PERIODO_COPIA = {"columnasFecha": ["FEC_APROBACION_PENSION"], "operadoresFecha": ["<="],
                  "columnaPeriodo": None, "ajustarSegmentacion": False,
                  "patronIdentificadorSegmento": None}
_PERIODO_COPIA_ARP = {**_PERIODO_COPIA, "operadoresFecha": [">=", "<", "<="]}

# malla -> (archivo de la lambda actual, bloque periodo que la reproduce, reglas en REMEDIACION)
LAMBDAS = {
    "AUXILIOS_FUNERARIOS": ("niif-dev-configuration-auxilios-funerarios/conf_auxilios.py",
                            None, False),
    "PRIMAS_RENTAS": ("niif-dev-configuration-primas-rentas/lambda_function.py",
                      _PERIODO_COPIA, True),
    "PRIMAS_ENDOSOS": ("niif-dev-configuration-primas-endosos/lambda_function.py",
                       _PERIODO_COPIA, False),
    "PRIMAS_ARL": ("niif-dev-configuration-primas-arl/lambda_function.py", _PERIODO_COPIA, False),
    "RESERVA_MATEMATICA_RVI": ("niif-dev-configuration-reserva-matematica-rvi/lambda_function.py",
                               _PERIODO_COPIA, True),
    "RESERVA_MATEMATICA_ARP": ("niif-dev-configuration-reserva-matematica-arp/lambda_function.py",
                               _PERIODO_COPIA_ARP, False),
}

WHERE = ("p.FEC_APROBACION_PENSION >= DATE '2026-09-01' "
         "AND p.FEC_APROBACION_PENSION < DATE '2026-10-01' "
         "AND FEC_APROBACION_PENSION <= DATE '2026-09-30' "
         "AND FEC_INICIAL >= DATE '2026-09-01' AND FEC_ENVIO_TES < DATE '2026-10-01' "
         "AND PERIODO = '202609'")

pytestmark = pytest.mark.skipif(not DIRECTORIO.is_dir(),
                                reason="no está la carpeta de las lambdas actuales")


# --------------------------------------------------------------------------- datos


def _datos(codigo, con_reglas):
    spec = CATALOGO_DEV["mallas"][codigo]
    nombre_caso, ciclos = spec["nombreCaso"], spec["totalCiclos"]
    pk, ruta = f"MALLA#{codigo}", f"s3://assets/{nombre_caso}"

    def registro(sk, **campos):
        return {"PK": pk, "SK": sk, **campos}

    remediacion = {"estadoDefinicion": CON_REGLAS if con_reglas else SIN_REGLAS}
    if con_reglas:
        remediacion["reglasTransformativas"] = [{"regla": "ficticia"}]
    if codigo == "PRIMAS_RENTAS":  # sin estado con reglas: las reglas no cambian lo esperado
        remediacion["estadoDefinicion"] = SIN_REGLAS

    cnf = [
        registro("GENERAL", nombreCaso=nombre_caso),
        registro("EXTRACCION", archivoActivoUri=f"{ruta}/config_extraccion.json",
                 fuente={"conexion": "oracle"}, rutas={"bronce": "s3://bronce/niif/"},
                 controlExtraccion={"periodo_negocio": PERIODO_CNF,
                                    "ruta_estado": f"{ruta}/estado/{PERIODO_CNF}.json"},
                 salidaRuntime={"ruta": f"{ruta}/parametros_extraccion.json"}),
        registro("VALIDACION", archivoActivoUri=f"{ruta}/config_val_tec.json",
                 parametrosExtraccion={"ruta": f"{ruta}/parametros_extraccion.json"},
                 salida={"ruta": "s3://silver/validacion/"},
                 datasets=[{"tabla": "alfaprd/pagos", "reglas": []}]),
        registro("REMEDIACION", archivoActivoUri=f"{ruta}/config_remediacion.json",
                 plantillaUri=f"{ruta}/plantilla_remediacion.json", **remediacion),
        registro("SCD", archivoActivoUri=f"{ruta}/config_scd.json",
                 estructuraGoldRef="GLD#SCD", salida={"ruta": "s3://gold/scd/"}, llaves=["ID"],
                 columnasCambio=["VALOR"], columnasFijas=["ID"],
                 columnasAdministrativas=["FEC_CARGA"], metadatos={"version": 1}),
    ]
    for i in range(1, ciclos + 1):
        cnf.append(registro(f"CICLO#{i:03d}", orden=i, nombre=f"ciclo{i}", segmentos=[{
            "orden": 1, "nombre": "segmento", "forma": "COMPLETA",
            "estructuras": [
                {"orden": 2, "estructuraRef": "SRC#B", "tipoTabla": "DIMENSION",
                 "where": None, "segmentacionHorizontal": {}},
                {"orden": 1, "estructuraRef": "SRC#A", "tipoTabla": "HECHO", "where": WHERE,
                 "dependencia": "SRC#B", "reutilizacionCompartida": True,
                 "segmentacionHorizontal": {
                     "periodo_negocio": PERIODO_CNF,
                     "identificador": f"niif17_{PERIODO_CNF}_lote",
                     "ruta": f"s3://bronce/niif17_{PERIODO_CNF}_x/"}},
            ]}]))
    prs = {("SRC#A", "SRC"): {"schema": "ALFAPRD", "tabla": "A"},
           ("SRC#B", "SRC"): {"schema": "ALFAPRD", "tabla": "B"},
           ("GLD#SCD", "GLD"): {"schema": "GOLD", "tabla": "SCD"}}
    prg = {(spec["programacion"]["pk"], spec["programacion"]["sk"]):
           {"periodoNegocio": PERIODO_PRG}}
    plantillas = {f"{ruta}/plantilla_remediacion.json": {"nombre_caso": nombre_caso,
                                                          "reglas": []}}
    return cnf, prs, prg, plantillas


# ----------------------------------------------------------- AWS simulado (antes)


class _Tabla:
    def __init__(self, aws, nombre):
        self._aws, self._nombre = aws, nombre

    def query(self, **_):  # la lambda actual solo consulta la PK de su malla
        return {"Items": copy.deepcopy(self._aws.cnf)}

    def get_item(self, Key, **_):
        item = self._aws.lecturas[self._nombre].get((Key["PK"], Key["SK"]))
        return {"Item": copy.deepcopy(item)} if item else {}


class AwsSimulado:
    """Lo mínimo de boto3 que usan las lambdas actuales, registrando cada efecto."""

    def __init__(self, cnf, prs, prg, plantillas):
        self.cnf = cnf
        self.lecturas = {TABLAS["PRS_TABLE"]: prs, TABLAS["PRG_TABLE"]: prg}
        self.archivos = {k: json.dumps(v).encode() for k, v in plantillas.items()}
        self.escrituras, self.transacciones, self.ejecuciones, self.items_ejc = [], [], [], []

    def resource(self, *_, **__):
        return self

    def client(self, *_, **__):
        return self

    def Table(self, nombre):
        return _Tabla(self, nombre)

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self.archivos[f"s3://{Bucket}/{Key}"])}

    def put_object(self, Bucket, Key, Body, Metadata, **_):
        self.escrituras.append((f"s3://{Bucket}/{Key}", json.loads(Body), Metadata))
        return {"ETag": '"etag"'}

    def transact_write_items(self, TransactItems):
        self.transacciones.append(TransactItems)

    def start_execution(self, stateMachineArn, name, input):
        self.ejecuciones.append((stateMachineArn, json.loads(input)))
        return {"executionArn": f"{stateMachineArn}:{name}"}

    def put_item(self, TableName, Item):
        self.items_ejc.append({k: v["S"] for k, v in Item.items()})


class _Contexto:
    aws_request_id = "00000000-0000-0000-0000-000000000001"


def _ejecutar_actual(monkeypatch, codigo, archivo, datos):
    aws = AwsSimulado(*datos)
    spec = CATALOGO_DEV["mallas"][codigo]
    entorno = {**TABLAS, "AWS_REGION": "us-east-1", "MALLA_PK": f"MALLA#{codigo}",
               "STATE_MACHINE_ARN": ("arn:aws:states:us-east-1:111111111111:stateMachine:"
                                     + spec["stateMachineName"]),
               "PRG_PK": spec["programacion"]["pk"], "PRG_SK": spec["programacion"]["sk"]}
    for nombre, valor in entorno.items():
        monkeypatch.setenv(nombre, valor)
    monkeypatch.setattr(boto3, "resource", aws.resource)
    monkeypatch.setattr(boto3, "client", aws.client)

    modulo_spec = importlib.util.spec_from_file_location(f"actual_{codigo.lower()}",
                                                         DIRECTORIO / archivo)
    modulo = importlib.util.module_from_spec(modulo_spec)
    modulo_spec.loader.exec_module(modulo)
    evento = {"publicar": True, "ejecutar": True, "periodoNegocio": PERIODO_PRG}
    return aws, modulo.lambda_handler(evento, _Contexto())


# ------------------------------------------------------------- lambda nueva (después)


class _ClienteDynamo:
    def __init__(self):
        self.transacciones = []

    def transact_write_items(self, TransactItems):
        self.transacciones.append(TransactItems)


class _CnfConAdaptadorReal:
    """Lee de memoria y escribe con el adaptador real, para comparar la transacción."""

    def __init__(self, registros, cliente):
        self._registros = {r["SK"]: r for r in registros}
        self._adaptador = DynamoCnf(cliente, TABLAS["CNF_TABLE"])

    def consultar(self, pk):
        return copy.deepcopy(self._registros)

    def persistir_periodo(self, cnf, antes):
        return self._adaptador.persistir_periodo(cnf, antes)


def _ejecutar_nueva(codigo, periodo_copia, datos):
    cnf, prs, prg, plantillas = datos
    documento = copy.deepcopy(CATALOGO_DEV)
    if periodo_copia is not None:
        documento["mallas"][codigo]["periodo"] = copy.deepcopy(periodo_copia)
    cliente = _ClienteDynamo()
    almacen, orquestador, ejecuciones = FakeAlmacen(plantillas), FakeOrquestador(), FakeEjecuciones()
    servicio = ConfiguradorMallas(
        catalogo=catalogo.parsear(documento), cnf=_CnfConAdaptadorReal(cnf, cliente),
        prs=FakeLectura(prs), programaciones=FakeLectura(prg), almacen=almacen,
        orquestador=orquestador, ejecuciones=ejecuciones,
        idempotencia=ControlIdempotencia(FakeIdempotencia(), 3600, lambda: AHORA),
        reloj=lambda: AHORA,
    )

    class Observador:
        def __getattr__(self, _):
            return lambda *a, **k: None

    evento = Evento.desde_dict({"malla": codigo, "publicar": True, "ejecutar": True,
                                "periodoNegocio": PERIODO_PRG})
    resultado = servicio.ejecutar(evento, Invocacion(_Contexto.aws_request_id, 60_000),
                                  Observador())
    return resultado.to_dict(), almacen, orquestador, ejecuciones, cliente


# ------------------------------------------------------------------------ pruebas


@pytest.fixture(params=sorted(LAMBDAS))
def comparacion(request, monkeypatch):
    codigo = request.param
    archivo, periodo_copia, con_reglas = LAMBDAS[codigo]
    if not (DIRECTORIO / archivo).is_file():
        pytest.skip(f"no está {archivo}")
    datos = _datos(codigo, con_reglas)
    antes, respuesta_antes = _ejecutar_actual(monkeypatch, codigo, archivo, copy.deepcopy(datos))
    monkeypatch.undo()
    despues = _ejecutar_nueva(codigo, periodo_copia, copy.deepcopy(datos))
    return codigo, antes, respuesta_antes, despues


def test_publica_los_mismos_cuatro_json(comparacion):
    _, antes, _, (_, almacen, *_) = comparacion
    assert len(antes.escrituras) == 4
    assert [uri for uri, _ in almacen.escrituras] == [uri for uri, _, _ in antes.escrituras]
    for uri, contenido, _ in antes.escrituras:
        assert almacen.archivos[uri] == contenido, uri


def test_publica_con_los_mismos_metadatos_mas_el_ejecucion_id(comparacion):
    _, antes, _, (respuesta, almacen, *_) = comparacion
    for (_, metadatos), (_, _, metadatos_antes) in zip(almacen.escrituras, antes.escrituras):
        assert metadatos == {**metadatos_antes, "ejecucion-id": respuesta["ejecucionId"]}


def test_actualiza_el_periodo_en_cnf_con_la_misma_transaccion(comparacion):
    _, antes, _, (*_, cliente) = comparacion
    assert cliente.transacciones == antes.transacciones
    assert len(antes.transacciones) == 1


def test_inicia_la_misma_maquina_con_la_misma_entrada(comparacion):
    _, antes, _, (respuesta, _, orquestador, *_) = comparacion
    [(arn, entrada_antes)] = antes.ejecuciones
    [(maquina, entrada)] = orquestador.ejecuciones.values()
    assert arn.endswith(f":stateMachine:{maquina}")
    # Lo único distinto: el nombre de la ejecución es determinista y se agrega ejecucionId.
    assert entrada.pop("ejecucionId") == respuesta["ejecucionId"]
    assert entrada.pop("ejcSK") == respuesta["ejcSK"]
    entrada_antes.pop("ejcSK")
    assert entrada == entrada_antes


def test_registra_la_misma_ejecucion(comparacion):
    _, antes, _, (respuesta, _, _, ejecuciones, _) = comparacion
    [item_antes] = antes.items_ejc
    [item] = ejecuciones.items.values()
    distintos = {"SK", "executionName", "executionArn", "fechaInicio"}
    assert {k: v for k, v in item.items() if k not in distintos | {"ejecucionId"}} == {
        k: v for k, v in item_antes.items() if k not in distintos}


def test_responde_lo_mismo(comparacion):
    _, antes, respuesta_antes, (respuesta, *_) = comparacion
    for campo in ("estado", "malla", "publicar", "ejecutar", "periodoNegocio", "registrosCNF",
                  "ejcPK"):
        assert respuesta[campo] == respuesta_antes[campo], campo
    assert [p["uri"] for p in respuesta["publicados"]] == [
        p["uri"] for p in respuesta_antes["publicados"]]
    # "comparaciones" siempre salía vacía: no se conserva.
    assert respuesta_antes["comparaciones"] == []
    assert set(respuesta_antes) - set(respuesta) == {"comparaciones"}
