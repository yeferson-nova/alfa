# Arnés del flujo de Primas ARL: encadena los jobs REALES (sin modificarlos)
#   extracción -> validación (simulada en SUCCESS) -> remediación -> Silver ARL -> SCD
# con Oracle, S3, Glue y parquet simulados en memoria.
# Uso (entorno con pyspark 3.5, Python 3.11):
#   python arnes_flujo_arl.py [escenario ...]     escenarios: control antes raiz ext01 ext02 ext03 ext04
# Variable opcional ALFA_REPOS (por defecto C:/Users/rdpuser/Git/alfa-repos).
import os, sys, io, json, types, runpy, logging, time, tempfile
from decimal import Decimal as D
from datetime import date

REPOS = os.environ.get("ALFA_REPOS", "C:/Users/rdpuser/Git/alfa-repos")
RMD = REPOS + "/glue/data-datalake-niif-rmd-ext-estandar-glue-job/src/jobs/"
EXT = RMD + "extraccion_estandar_optimized.py"
REM = RMD + "remediacion_estandar.py"
SCD = RMD + "scd.py"
SIL = REPOS + "/glue/data-datalake-niif-primas-arl-glue-job/src/jobs/silver_primas_arl.py"

# Variante "antes del fix": el mismo script sin el bloque que combina los lotes de respaldo.
with open(EXT, encoding="utf-8") as f:
    lineas = f.read().split("\n")
ini = next(i for i, l in enumerate(lineas) if 'if metadata_exclusion.get("exclusion_aplicada"):' in l)
fin = next(i for i in range(ini, ini + 15) if lineas[i].strip() == ")" and lineas[i - 1].strip() == ")")
EXT_ANTES = os.path.join(tempfile.gettempdir(), "extraccion_antes_del_fix.py")
with open(EXT_ANTES, "w", encoding="utf-8") as f:
    f.write("\n".join(lineas[:ini] + lineas[fin + 1:]))

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
from pyspark.sql import SparkSession, readwriter
spark = (SparkSession.builder.master("local[1]").appName("arnes-flujo-arl")
         .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# ---------------- almacenamiento simulado ----------------
DISCO, S3 = {}, {}
norm = lambda p: p.rstrip("/")

def _union(dfs):
    out = dfs[0]
    for d in dfs[1:]:
        out = out.unionByName(d, allowMissingColumns=True)
    return out

def leer(self, *paths, **kw):
    p = norm(paths[0])
    if p in DISCO:
        return DISCO[p]
    hijos = [v for k, v in sorted(DISCO.items()) if k.startswith(p + "/")]
    if hijos:
        return _union(hijos)
    raise Exception("[PATH_NOT_FOUND] Path does not exist: " + p)

def modo(self, m):
    self._modo = m
    return self

def escribir(self, path, *a, **kw):
    p, m = norm(path), getattr(self, "_modo", "errorifexists")
    df = self._df
    df = spark.createDataFrame(df.collect(), df.schema)
    if m == "overwrite":
        for k in [k for k in DISCO if k == p or k.startswith(p + "/")]:
            del DISCO[k]
        DISCO[p] = df
    elif m == "append":
        DISCO[p] = _union([DISCO[p], df]) if p in DISCO else df
        DISCO[p] = spark.createDataFrame(DISCO[p].collect(), DISCO[p].schema)
    else:
        if p in DISCO:
            raise Exception("ruta ya existe: " + p)
        DISCO[p] = df

readwriter.DataFrameReader.parquet = leer
readwriter.DataFrameWriter.mode = modo
readwriter.DataFrameWriter.parquet = escribir

class ClientError(Exception):
    def __init__(self, response, op):
        super().__init__(str(response)); self.response = response
bc = types.ModuleType("botocore"); bce = types.ModuleType("botocore.exceptions"); bce.ClientError = ClientError
sys.modules.update({"botocore": bc, "botocore.exceptions": bce})

class FakeS3:
    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(S3[f"s3://{Bucket}/{Key}"].encode("utf-8"))}
    def put_object(self, Bucket, Key, Body, **kw):
        S3[f"s3://{Bucket}/{Key}"] = Body.decode("utf-8") if isinstance(Body, bytes) else Body
    def head_object(self, Bucket, Key):
        if f"s3://{Bucket}/{Key}" not in S3:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {}
    def _contenido(self, Bucket, Prefix):
        base = f"s3://{Bucket}/"; pre = base + Prefix
        out = [{"Key": k[len(base):] + "/part-00000.snappy.parquet", "Size": 10}
               for k in DISCO if (k + "/").startswith(pre) or k.startswith(pre)]
        out += [{"Key": k[len(base):], "Size": len(v)} for k, v in S3.items() if k.startswith(pre)]
        return out
    def list_objects_v2(self, Bucket, Prefix="", **kw):
        c = self._contenido(Bucket, Prefix)
        return {"Contents": c[: kw.get("MaxKeys", 1000)], "IsTruncated": False} if c else {"IsTruncated": False}
    def get_paginator(self, nombre):
        s3 = self
        class P:
            def paginate(self, Bucket, Prefix=""):
                yield s3.list_objects_v2(Bucket=Bucket, Prefix=Prefix)
        return P()
boto3 = types.ModuleType("boto3"); boto3.client = lambda *a, **k: FakeS3(); sys.modules["boto3"] = boto3

# ---------------- Oracle y Glue simulados ----------------
ORACLE, FALLA_ORACLE = {}, set()
def publicar_oracle():
    for nombre, filas in ORACLE.items():
        spark.createDataFrame(filas).createOrReplaceTempView(nombre)

class DynFrame:
    def __init__(self, df): self.df = df
    def toDF(self): return self.df
class CreadorDyn:
    def from_options(self, connection_type, connection_options):
        tabla = connection_options["dbtable"].split(".")[-1].upper()
        if tabla in FALLA_ORACLE:
            raise Exception("ORA-12170 simulado: timeout leyendo " + tabla)
        q = connection_options.get("sampleQuery") or f"SELECT * FROM {tabla}"
        return DynFrame(spark.sql(q))
aw = types.ModuleType("awsglue"); ut = types.ModuleType("awsglue.utils")
cx = types.ModuleType("awsglue.context"); jb = types.ModuleType("awsglue.job")
def getResolvedOptions(argv, names):
    return {n: argv[argv.index("--" + n) + 1] for n in names}
class GlueContext:
    def __init__(self, sc): self.spark_session = spark; self.create_dynamic_frame = CreadorDyn()
class Job:
    def __init__(self, g): pass
    def init(self, *a): pass
    def commit(self): pass
ut.getResolvedOptions = getResolvedOptions; cx.GlueContext = GlueContext; jb.Job = Job
sys.modules.update({"awsglue": aw, "awsglue.utils": ut, "awsglue.context": cx, "awsglue.job": jb})

# ---------------- datos de origen ----------------
def endoso(n, periodo, vlr):
    poliza = 100 + n % 4
    return {"NID_POLIZA": poliza, "NID_ENDOSO": n, "VLR_PRIMA": D(vlr), "FEC_EXPEDICION": date(int(periodo[:4]), int(periodo[4:]), 10),
            "FEC_PROCESO_POINT": date(int(periodo[:4]), int(periodo[4:]), 15), "COD_TIPO_ENDOSO": 1, "COD_TIPO_ENDOSO_ARP": "A",
            "COD_SUCURSAL_ALFA": "01", "MODULO": "ARP", "PERIODO": periodo}

def oracle_base():
    ORACLE.clear(); FALLA_ORACLE.clear()
    ORACLE["PAR_ENDOSOS"] = [endoso(n, "202605", f"{n}00.00") for n in range(1, 11)] + \
                            [endoso(n, "202606", f"{n}00.00") for n in range(11, 21)]
    ORACLE["PAR_AFILIACIONES"] = [{"NID_AFILIACION": 100 + i, "COD_TIPO_IDENTIFICACION": "CC", "NID_NUMERO_IDENTIFICACION": 1000 + i} for i in range(4)]
    ORACLE["CGR_SUCURSALES_ALFA"] = [{"COD_SUCURSAL_ALFA": "01", "NOMBRE": "BOGOTA"}]
    ORACLE["ACU_PERSONAS_JURIDICAS"] = [{"COD_TIPO_IDENTIFICACION": "NIT", "NID_NUMERO_IDENTIFICACION": 9, "NID_CONSECUTIVO": 1,
                                         "NOMBRE_COMERCIAL": "X", "RAZON_SOCIAL": "X SA"}]
    ORACLE["ACU_PERSONAS_NATURALES"] = [{"COD_TIPO_IDENTIFICACION": "CC", "NID_NUMERO_IDENTIFICACION": 1000 + i, "PRIMER_APELLIDO": "A",
                                         "SEGUNDO_APELLIDO": "B", "PRIMER_NOMBRE": "N" + str(i), "SEGUNDO_NOMBRE": ""} for i in range(4)]
    publicar_oracle()

# ---------------- configuración ----------------
ASSETS = "s3://niif-assets/primas-arl/"
PAR = ASSETS + "parametros_extraccion.json"; CFG_EXT = ASSETS + "config_extraccion_primas_arl.json"; CFG_SCD = ASSETS + "config_scd_primas_arl.json"
GOLD = "s3://gold/niif/scd_primas_arl"
S3[CFG_SCD] = json.dumps({"nombre_caso": "primas_arl", "salida": {"ruta_gold": GOLD + "/"},
    "llaves": {"apertura": [], "primaria": ["NID_POLIZA", "NID_ENDOSO"]},
    "columnas_cambio": {"alerta": ["FEC_EXPEDICION", "PER_EXPEDICION", "FEC_PROCESO_POINT", "PER_PROCESO_POINT"],
                        "normales": ["VLR_PRIMA", "COD_TIPO_ENDOSO", "COD_TIPO_ENDOSO_ARP", "COD_SUCURSAL_ALFA", "NOM_SUCURSAL_ALFA", "MODULO",
                                     "NID_AFILIACION", "COD_TIPO_IDENTIFICACION_TERCERO", "NID_NRO_IDENTIFICACION_TERCERO", "NOM_TERCERO"]},
    "columnas_fijas": ["LOTE_CARGUE_ID"]})

def tabla(orden, schema, nombre, tipo, llaves=None, periodo=None, where=None):
    t = {"orden": orden, "schema": schema, "tabla": nombre, "tabla_relativa": f"{schema.lower()}/{nombre.lower()}", "tipo_tabla": tipo}
    if where: t["where"] = where
    t["segmentacion_horizontal"] = ({"habilitada": True, "segmento_id": f"niif17_{periodo}_alfaprd_{schema.lower()}_{nombre.lower()}",
        "id_negocio": "primas_arl", "periodo_negocio": periodo, "modo": "EXCLUIR_EXTRAIDOS", "llaves_segmentacion": llaves,
        "requiere_preparacion_con_lotes_segmento": True} if llaves else {"habilitada": False})
    return t

def config_extraccion(periodo, where, excluir=True):
    ciclo = lambda o, n, tablas: {"orden": o, "nombre": n, "segmentos": [{"orden": 1, "nombre": n, "forma": "CASCADA", "tablas": tablas}]}
    S3[CFG_EXT] = json.dumps({"nombre_caso": "primas_arl", "periodo_negocio": periodo,
        "fuente": {"sistema": "ALFAPRD", "tipo": "oracle", "connection_name": "conexion", "usar_schema_en_dbtable": False},
        "rutas": {"s3_bronze_base": "s3://bronce/niif/ALFAPRD/", "s3_silver_base": "s3://silver/niif/ALFAPRD/",
                  "ruta_parametros_extraccion": PAR, "ruta_segmentos_base": "s3://niif-assets/segmentos/"},
        "control_extraccion": {"modo_ejecucion": "INICIAL", "periodo_negocio": periodo, "excluir_llaves_segmento": excluir,
                               "permitir_reanudacion": True, "usar_checkpoints": True, "contar_registros": True},
        "ciclos": [
            ciclo(1, "pivote", [tabla(1, "ARPPROD", "PAR_ENDOSOS", "PIVOTE", ["NID_ENDOSO"], periodo, where)]),
            ciclo(2, "dependencias", [tabla(1, "ARPPROD", "PAR_AFILIACIONES", "COMPLEMENTARIA", ["NID_AFILIACION"], periodo),
                                      tabla(2, "GENERAL", "CGR_SUCURSALES_ALFA", "CATALOGO")]),
            ciclo(3, "personas", [tabla(1, "GENERAL", "ACU_PERSONAS_JURIDICAS", "COMPLEMENTARIA",
                                        ["COD_TIPO_IDENTIFICACION", "NID_NUMERO_IDENTIFICACION", "NID_CONSECUTIVO"], periodo),
                                  tabla(2, "GENERAL", "ACU_PERSONAS_NATURALES", "COMPLEMENTARIA",
                                        ["COD_TIPO_IDENTIFICACION", "NID_NUMERO_IDENTIFICACION"], periodo)])]})

# ---------------- ejecución de la cadena ----------------
def correr(script, args):
    sys.argv = [os.path.basename(script)] + [x for k, v in args.items() for x in ("--" + k, v)]
    logging.getLogger().handlers.clear(); logging.getLogger().setLevel(logging.CRITICAL)
    runpy.run_path(script, run_name="__main__")

def flujo(periodo, where, excluir=True, script_ext=EXT):
    """Imita a la Step Function: extracción, decisión de datos nuevos, validación, remediación, Silver y SCD."""
    config_extraccion(periodo, where, excluir)
    time.sleep(1.1)  # el lote es la hora en segundos
    pasos = [("extraccion", script_ext, {"JOB_NAME": "ext", "RUTA_CONFIG_EXTRACCION": CFG_EXT})]
    try:
        correr(*pasos[0][1:])
    except Exception as e:
        return f"FALLA extraccion: {str(e)[:150]}"
    par = json.loads(S3[PAR])
    if par.get("requiere_procesamiento_caso") is False:
        return "SIN DATOS NUEVOS (no corre Silver ni SCD)"
    par["estado_validacion"] = "SUCCESS"; S3[PAR] = json.dumps(par)  # validación técnica simulada
    for nombre, script, args in [
        ("remediacion", REM, {"JOB_NAME": "rem", "RUTA_PARAMETROS_EXTRACCION": PAR, "RUTA_ORIGEN_REMEDIACION": "s3://silver/val/",
                              "RUTA_DESTINO_REMEDIACION": "s3://silver/rem/"}),
        ("silver_arl", SIL, {"JOB_NAME": "sil", "RUTA_PARAMETROS_EXTRACCION": PAR,
                             "S3_SLV_PREP_PATH": "s3://silver/niif/t_data_domain/primas_arl/silver_preparado_primas-arl/"}),
        ("scd", SCD, {"JOB_NAME": "scd", "RUTA_CONFIG_SCD": CFG_SCD, "RUTA_PARAMETROS_EXTRACCION": PAR})]:
        try:
            correr(script, args)
        except Exception as e:
            return f"FALLA {nombre}: {str(e)[:170]}"
    return "OK"

def estado(titulo, resultado):
    par = json.loads(S3[PAR])
    seg = next((d for d in par.get("datasets_extraidos", []) if d.get("tabla_relativa") == "arpprod/par_endosos"), {})
    m = par.get("metricas_gold_scd") or {}
    print(f"  · {titulo}: {resultado}")
    print(f"      periodo={par.get('periodo_negocio')} lote={par.get('lote_cargue_id')} reanudacion={par.get('reanudacion', {}).get('es_reanudacion')}"
          f" | par_endosos: extraidos={seg.get('total_registros')} excluidos={seg.get('registros_excluidos_por_segmento')} lotes_segmento={len(seg.get('lotes_segmento') or [])}")
    if m and resultado == "OK":
        print(f"      SCD: entrada={m.get('total_entrada_silver_preparada')} nuevos={m.get('total_insertados_nuevos')}"
              f" apagados={m.get('total_registros_apagados_por_ausencia')} nuevas_versiones={m.get('total_nuevas_versiones_por_cambio')}")
    g = DISCO.get(GOLD)
    if g is not None:
        filas = g.groupBy("PER_PROCESO_POINT", "ACT_REG").count().orderBy("PER_PROCESO_POINT", "ACT_REG").collect()
        print("      Gold:", ", ".join(f"{r[0]}/act={r[1]}:{r[2]}" for r in filas))

def nuevos_en_dependientes():
    """Como en el incidente de prod: las tablas dependientes también traen filas nuevas en la incremental."""
    ORACLE["PAR_AFILIACIONES"].append({"NID_AFILIACION": 104, "COD_TIPO_IDENTIFICACION": "CC", "NID_NUMERO_IDENTIFICACION": 1004})
    ORACLE["ACU_PERSONAS_NATURALES"].append({"COD_TIPO_IDENTIFICACION": "CC", "NID_NUMERO_IDENTIFICACION": 1004, "PRIMER_APELLIDO": "C",
                                             "SEGUNDO_APELLIDO": "D", "PRIMER_NOMBRE": "N4", "SEGUNDO_NOMBRE": ""})
    ORACLE["ACU_PERSONAS_JURIDICAS"].append({"COD_TIPO_IDENTIFICACION": "NIT", "NID_NUMERO_IDENTIFICACION": 8, "NID_CONSECUTIVO": 1,
                                             "NOMBRE_COMERCIAL": "Y", "RAZON_SOCIAL": "Y SA"})
    publicar_oracle()

def reset():
    DISCO.clear(); [S3.pop(k) for k in list(S3) if k not in (CFG_SCD,)]; oracle_base()

ESC = sys.argv[1:] or ["control", "antes", "raiz", "ext01", "ext02", "ext03", "ext04"]
QA_ARGS = sys.argv
for esc in ESC:
    reset()
    if esc == "control":
        print("\n## CONTROL: mismo periodo 202606 (<=), base con tope y luego incremental. Esperado: Gold con los 20, apagados 0")
        estado("corrida base (NID_ENDOSO <= 18)", flujo("202606", "PERIODO <= '202606' AND NID_ENDOSO <= 18"))
        nuevos_en_dependientes()
        estado("corrida incremental", flujo("202606", "PERIODO <= '202606'"))
    elif esc == "antes":
        print("\n## ANTES DEL FIX: mismo escenario con la extracción sin combinar lotes de respaldo")
        estado("corrida base", flujo("202606", "PERIODO <= '202606' AND NID_ENDOSO <= 18", script_ext=EXT_ANTES))
        nuevos_en_dependientes()
        estado("corrida incremental", flujo("202606", "PERIODO <= '202606'", script_ext=EXT_ANTES))
    elif esc == "raiz":
        print("\n## PROBLEMA RAÍZ (SCD-02 / ARL-01): filtro '=' como en QA, periodo 202605 y luego 202606")
        estado("periodo 202605", flujo("202605", "PERIODO = '202605'"))
        estado("periodo 202606", flujo("202606", "PERIODO = '202606'"))
    elif esc == "ext01":
        print("\n## EXT-01: la extracción de 202605 falla a mitad; la siguiente corrida es de 202606")
        FALLA_ORACLE.add("ACU_PERSONAS_NATURALES")
        estado("202605 (Oracle falla en personas naturales)", flujo("202605", "PERIODO <= '202605'"))
        FALLA_ORACLE.clear()
        estado("202606 (debería traer los endosos 1 a 20)", flujo("202606", "PERIODO <= '202606'"))
    elif esc == "ext02":
        print("\n## EXT-02: el endoso 12 cambia de valor en Oracle entre dos corridas del mismo periodo")
        estado("corrida 1", flujo("202606", "PERIODO <= '202606'"))
        for r in ORACLE["PAR_ENDOSOS"]:
            if r["NID_ENDOSO"] == 12: r["VLR_PRIMA"] = D("99999.00")
        publicar_oracle()
        estado("corrida 2 (Oracle: endoso 12 = 99999.00)", flujo("202606", "PERIODO <= '202606'"))
        g = DISCO[GOLD].filter("NID_ENDOSO = 12 AND ACT_REG = 1").select("VLR_PRIMA").collect()
        print("      Gold activo endoso 12 VLR_PRIMA =", [str(r[0]) for r in g])
    elif esc == "ext03":
        print("\n## EXT-03: se carga 202605, luego 202606 por primera vez, y se vuelve a correr 202605")
        estado("202605", flujo("202605", "PERIODO <= '202605'"))
        estado("202606 (primera carga)", flujo("202606", "PERIODO <= '202606'"))
        estado("202605 otra vez", flujo("202605", "PERIODO <= '202605'"))
    elif esc == "ext04":
        print("\n## EXT-04: dos corridas con la exclusión apagada y una tercera con la exclusión encendida")
        estado("corrida 1 (sin exclusión)", flujo("202606", "PERIODO <= '202606'", excluir=False))
        estado("corrida 2 (sin exclusión)", flujo("202606", "PERIODO <= '202606'", excluir=False))
        estado("corrida 3 (con exclusión)", flujo("202606", "PERIODO <= '202606'", excluir=True))
spark.stop()
