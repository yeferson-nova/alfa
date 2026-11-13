# Arnes: ejecuta el scd.py REAL (sin modificarlo) con Glue, boto3 y parquet simulados en memoria.
# Uso: venv-spark\Scripts\python.exe arnes_scd.py <ruta scd.py>
import os, sys, json, types, runpy, logging, traceback

SCD = sys.argv[1]
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession, Row
from pyspark.sql import readwriter
from pyspark.sql.types import StructType, StructField, StringType, LongType, DecimalType

spark = (SparkSession.builder.master("local[1]").appName("arnes-scd")
         .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# ---------- "disco" de parquet en memoria ----------
DISCO = {}
FALLA_LECTURA = set()
norm = lambda p: p.rstrip("/")

def fake_read_parquet(self, *paths, **kw):
    p = norm(paths[0])
    if p in FALLA_LECTURA:
        raise Exception("AccessDenied: simulado al leer " + p)
    if p not in DISCO:
        raise Exception("[PATH_NOT_FOUND] Path does not exist: " + p)
    return DISCO[p]

def fake_write_parquet(self, path, *a, **kw):
    df = self._df
    DISCO[norm(path)] = spark.createDataFrame(df.collect(), df.schema)  # materializado

readwriter.DataFrameReader.parquet = fake_read_parquet
readwriter.DataFrameWriter.parquet = fake_write_parquet

# ---------- S3 (JSON) en memoria ----------
S3 = {}
class FakeS3:
    def get_object(self, Bucket, Key):
        import io
        return {"Body": io.BytesIO(S3[f"s3://{Bucket}/{Key}"].encode("utf-8"))}
    def put_object(self, Bucket, Key, Body, **kw):
        S3[f"s3://{Bucket}/{Key}"] = Body.decode("utf-8") if isinstance(Body, bytes) else Body
boto3 = types.ModuleType("boto3"); boto3.client = lambda *a, **k: FakeS3()
sys.modules["boto3"] = boto3

# ---------- awsglue ----------
aw = types.ModuleType("awsglue"); utils = types.ModuleType("awsglue.utils")
ctx = types.ModuleType("awsglue.context"); jobm = types.ModuleType("awsglue.job")
def getResolvedOptions(argv, names):
    out = {}
    for n in names:
        i = argv.index("--" + n); out[n] = argv[i + 1]
    return out
class GlueContext:
    def __init__(self, sc): self.spark_session = spark
class Job:
    def __init__(self, g): pass
    def init(self, *a): pass
    def commit(self): pass
utils.getResolvedOptions = getResolvedOptions; ctx.GlueContext = GlueContext; jobm.Job = Job
sys.modules.update({"awsglue": aw, "awsglue.utils": utils, "awsglue.context": ctx, "awsglue.job": jobm})

# ---------- datos ----------
CFG = "s3://assets/caso/config_scd.json"; PAR = "s3://assets/caso/parametros.json"
SILVER = "s3://silver/caso/preparada"; GOLD = "s3://gold/niif/scd_caso"
S3[CFG] = json.dumps({
    "nombre_caso": "caso", "salida": {"ruta_gold": GOLD + "/"},
    "llaves": {"primaria": ["NUM"], "apertura": ["AP"]},
    "columnas_cambio": {"normales": ["VLR", "PER"], "alerta": []},
    "columnas_fijas": ["LOTE_CARGUE_ID"]})
ESQ = StructType([StructField("NUM", LongType()), StructField("AP", StringType()),
                  StructField("VLR", DecimalType(18, 2)), StructField("PER", StringType()),
                  StructField("LOTE_CARGUE_ID", LongType())])
from decimal import Decimal as D

def silver(filas):
    DISCO[norm(SILVER)] = spark.createDataFrame([(n, a, None if v is None else D(v), p, l) for n, a, v, p, l in filas], ESQ)

def correr(lote=1, argv_extra=()):
    S3[PAR] = json.dumps({"lote_cargue_id": lote, "estado_validacion": "SUCCESS", "estado_remediacion": "SUCCESS",
        "estado_disponibilizacion_silver": "SUCCESS", "estado_preparacion_silver": "SUCCESS",
        "ruta_silver_preparada": SILVER})
    sys.argv = ["scd.py", "--JOB_NAME", "scd", "--RUTA_CONFIG_SCD", CFG, "--RUTA_PARAMETROS_EXTRACCION", PAR, *argv_extra]
    logging.getLogger().handlers.clear()
    try:
        runpy.run_path(SCD, run_name="__main__")
        res = "OK"
    except Exception as e:
        res = f"FALLA: {type(e).__name__}: {str(e)[:140]}"
    m = json.loads(S3[PAR]).get("metricas_gold_scd")
    return res, m

def gold():
    df = DISCO.get(norm(GOLD))
    return [] if df is None else sorted([tuple(r) for r in df.select("NUM", "AP", "VLR", "PER", "VRS_REG", "ACT_REG").collect()],
                                        key=lambda t: (t[0], str(t[1]), -t[5], t[4]))

def mostrar(titulo, res, m):
    print(f"\n### {titulo}\n  resultado: {res}")
    if m: print("  metricas: " + ", ".join(f"{k.replace('total_','')}={v}" for k, v in m.items()))
    for f in gold(): print("   GOLD", f)

def reset():
    DISCO.clear(); FALLA_LECTURA.clear()

ESC = sys.argv[2:] or ["todos"]
def esc(n): return "todos" in ESC or n in ESC

# 1) Control: misma foto dos veces -> nada cambia
if esc("1"):
    reset(); silver([(1, "A", "10.00", "202511", 1), (2, "B", "20.00", "202511", 1)]); correr(1)
    mostrar("1. Control: misma foto 2 veces (esperado: 0 cambios)", *correr(2))

# 2) Columna de apertura nula, dato identico
if esc("2"):
    reset(); silver([(1, "A", "10.00", "202511", 1), (2, None, "20.00", "202511", 1)]); correr(1)
    mostrar("2. Apertura nula, mismo dato, 2a corrida (esperado: 0 cambios)", *correr(2))
    mostrar("2b. 3a corrida igual", *correr(3))

# 2p) Igual que 2, pero con 9 registros normales: la fraccion nula queda bajo el umbral (10 %)
if esc("2p"):
    reset(); filas = [(i, "A", "10.00", "202511", 1) for i in range(1, 10)] + [(10, None, "20.00", "202511", 1)]
    silver(filas); correr(1); correr(2)
    res, m = correr(3)
    print("\n### 2p. Apertura nula bajo el umbral, 3 corridas con el MISMO dato")
    print("  resultado:", res); print("  metricas:", m)
    for f in gold():
        if f[0] == 10: print("   GOLD", f)

# 3) Llave duplicada en la entrada
if esc("3"):
    reset(); silver([(1, "A", "10.00", "202511", 1)]); correr(1)
    silver([(1, "A", "10.00", "202511", 2), (1, "A", "99.00", "202511", 2)])
    mostrar("3. Llave duplicada en entrada (esperado: error o 1 activo)", *correr(2))

# 4) Periodo nuevo (patron Auxilios Funerarios)
if esc("4"):
    reset(); silver([(1, "A", "10.00", "202511", 1), (2, "A", "20.00", "202511", 1)]); correr(1)
    silver([(3, "A", "30.00", "202512", 2)])
    mostrar("4. Periodo nuevo 202512 (esperado: conservar 202511)", *correr(2))

# 5) Gold existe pero la lectura falla (permiso o error transitorio)
if esc("5"):
    reset(); silver([(1, "A", "10.00", "202511", 1), (2, "A", "20.00", "202511", 1)]); correr(1)
    silver([(1, "A", "11.00", "202511", 2), (2, "A", "20.00", "202511", 2)]); correr(2)
    print("\n  Gold ANTES de la corrida 5:", gold())
    FALLA_LECTURA.add(norm(GOLD)); silver([(9, "A", "90.00", "202512", 3)])
    res, m = correr(3); FALLA_LECTURA.clear()
    mostrar("5. Lectura de Gold falla (esperado: el job falla y Gold intacto)", res, m)

# 6) null -> "" en columna de cambio
if esc("6"):
    reset(); silver([(1, "A", "10.00", None, 1)]); correr(1)
    silver([(1, "A", "10.00", "", 2)])
    mostrar("6. PER pasa de null a '' (esperado: nueva version)", *correr(2))

# 7) Cuadre de una corrida normal con cambios
if esc("7"):
    reset(); silver([(1, "A", "10.00", "202511", 1), (2, "A", "20.00", "202511", 1), (3, "A", "30.00", "202511", 1)]); correr(1)
    silver([(1, "A", "10.00", "202511", 2), (2, "A", "21.00", "202511", 2), (4, "A", "40.00", "202511", 2)])
    mostrar("7. Normal: 1 igual, 1 cambia, 1 nuevo, 1 ausente (5% umbral -> permitir)", *correr(2, ("--PERMITIR_APAGADO_MASIVO", "true")))

spark.stop()
