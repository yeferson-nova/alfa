#!/usr/bin/env python3
# Solo lectura. Propone lectura_paralela (rangos de llave) para las tablas lentas o grandes de Emisiones.
#   python3 analizar-rangos-llave.py [--run <JobRunId>] [--min-minutos 5] [--min-filas 1000000] [--particiones 4]
# Lee: config_extraccion_<caso>.json y parametros_extraccion.json (assets), estadisticas y una muestra de la columna
# llave de los parquet de Bronce del lote mas reciente, y (con --run) la duracion de cada tabla en esa corrida.
# No imprime valores de filas: solo conteos, nombres de columna y limites numericos de la llave.
import json
import re
import sys
from collections import defaultdict

import boto3
import pyarrow.compute as pc
import pyarrow.fs as pafs
import pyarrow.parquet as pq

REGION = "us-east-1"
ASSETS = "niif-dev-assets"
CASOS = {"pcr_vida": "pcr-vida", "generales": "generales", "satelites": "satelites"}
GRUPO_SALIDA = "/aws-glue/jobs/output"
LLAVE = re.compile(r"(^PK$|^ID$|PK$|ID$)", re.I)
MUESTRA_GRUPOS = 24
LOTE = re.compile(r"lote_cargue_id=(\d+)")

s3 = boto3.client("s3", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)
fs = pafs.S3FileSystem(region=REGION)


def opcion(nombre, defecto):
    args = sys.argv[1:]
    return args[args.index(nombre) + 1] if nombre in args else defecto


def leer_json(bucket, key):
    return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())


def partir(uri):
    sin = uri.replace("s3://", "", 1)
    return sin.split("/", 1)[0], sin.split("/", 1)[1]


def duraciones(run_id):
    vistos, token = {}, None
    while run_id:
        kw = {"logGroupName": GRUPO_SALIDA, "logStreamNames": [run_id], "filterPattern": '"PASO_FIN"'}
        if token:
            kw["nextToken"] = token
        page = logs.filter_log_events(**kw)
        for e in page.get("events", []):
            for linea in e["message"].splitlines():
                inicio = linea.find("{")
                if inicio < 0:
                    continue
                try:
                    o = json.loads(linea[inicio:])
                except ValueError:
                    continue
                if o.get("evento") == "PASO_FIN" and o.get("paso") == "extraer_tabla":
                    vistos[(o.get("caso"), str(o.get("tabla_relativa", "")).lower())] = o.get("duracion_ms") or 0
        token = page.get("nextToken")
        if not token:
            break
    return vistos


def lote_con_datos(origen):
    bucket, key = partir(origen)
    m = LOTE.search(key)
    if not m:
        return origen
    base, resto = key[:m.start()], key[m.end():]
    lotes = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=base, Delimiter="/"):
        lotes += [LOTE.search(p["Prefix"]).group(1) for p in page.get("CommonPrefixes", []) if LOTE.search(p["Prefix"])]
    for lote in sorted(lotes, reverse=True):
        prefijo = f"{base}lote_cargue_id={lote}{resto}"
        if s3.list_objects_v2(Bucket=bucket, Prefix=prefijo, MaxKeys=1).get("KeyCount"):
            return f"s3://{bucket}/{prefijo}"
    return None


def archivos(uri):
    bucket, key = partir(uri)
    salida = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=key.rstrip("/") + "/"):
        salida += [f"{bucket}/{o['Key']}" for o in page.get("Contents", [])
                   if o["Size"] > 0 and not o["Key"].split("/")[-1].startswith(("_", "."))]
    return salida


def numerica(campo):
    t = str(campo.physical_type)
    return t in ("INT32", "INT64", "DOUBLE", "FLOAT", "FIXED_LEN_BYTE_ARRAY") or "decimal" in str(campo.logical_type).lower()


def perfil(rutas):
    filas, grupos, columnas = 0, [], None
    for ruta in rutas:
        meta = pq.ParquetFile(fs.open_input_file(ruta)).metadata
        filas += meta.num_rows
        if columnas is None:
            columnas = [meta.schema.column(i) for i in range(meta.num_columns)]
        for g in range(meta.num_row_groups):
            grupos.append((ruta, g, meta.row_group(g)))
    candidatas = [c.name for c in columnas or [] if LLAVE.search(c.name) and numerica(c)]
    return filas, grupos, candidatas


def limites(grupos, columna):
    minimo = maximo = None
    for _, _, rg in grupos:
        for i in range(rg.num_columns):
            col = rg.column(i)
            if col.path_in_schema != columna or not col.is_stats_set or not col.statistics.has_min_max:
                continue
            lo, hi = float(col.statistics.min), float(col.statistics.max)
            minimo = lo if minimo is None else min(minimo, lo)
            maximo = hi if maximo is None else max(maximo, hi)
    return minimo, maximo


def muestra(grupos, columna):
    paso = max(1, len(grupos) // MUESTRA_GRUPOS)
    valores = []
    for ruta, g, _ in grupos[::paso]:
        tabla = pq.ParquetFile(fs.open_input_file(ruta)).read_row_group(g, columns=[columna])
        arr = pc.drop_null(tabla.column(columna))
        valores += [float(v) for v in pc.cast(arr, "float64").to_pylist()]
    return valores


def carga_maxima(valores, inferior, superior, particiones):
    if not valores or superior is None or inferior is None or superior <= inferior:
        return None
    paso = (superior - inferior) / particiones
    cubetas = [0] * particiones
    for v in valores:
        i = int((v - inferior) // paso) if paso else 0
        cubetas[min(max(i, 0), particiones - 1)] += 1
    return round(100.0 * max(cubetas) / len(valores), 1)


def main():
    run_id = opcion("--run", None)
    min_minutos = float(opcion("--min-minutos", 5))
    min_filas = int(opcion("--min-filas", 1000000))
    particiones = int(opcion("--particiones", 4))
    tiempos = duraciones(run_id)
    print(f"== Tablas con mas de {min_minutos} min (corrida {run_id or 'no indicada'}) o mas de {min_filas} filas")
    propuestas = defaultdict(dict)
    for caso, carpeta in CASOS.items():
        config = leer_json(ASSETS, f"{carpeta}/config_extraccion_{caso}.json")
        actual = {}
        for f in config.get("nucleo_comun", {}).get("fases", []):
            for t in f.get("tablas", []):
                actual[str(t.get("tabla_relativa", "")).strip("/").lower()] = ("nucleo", t.get("lectura_paralela"))
        for ciclo in config.get("ciclos", []):
            for seg in ciclo.get("segmentos", []):
                for t in seg.get("tablas", []):
                    actual[str(t.get("tabla_relativa", "")).strip("/").lower()] = (caso, t.get("lectura_paralela"))
        runtime = leer_json(ASSETS, f"{carpeta}/parametros_extraccion.json")
        for d in runtime.get("datasets_extraidos", []):
            relativa = str(d.get("tabla_relativa", "")).strip("/").lower()
            ambito, bloque = actual.get(relativa, (caso, None))
            if ambito == "nucleo" and caso != "pcr_vida":
                continue
            nombre_caso = "emisiones_comun" if ambito == "nucleo" else caso
            ms = tiempos.get((nombre_caso, relativa)) or tiempos.get((caso, relativa)) or 0
            if ms < min_minutos * 60000 and (d.get("total_registros") or 0) < min_filas:
                continue
            origen = lote_con_datos(d.get("ruta_dataset_origen", ""))
            if not origen:
                print(f"-- {nombre_caso}/{relativa}: sin datos en Bronce")
                continue
            filas, grupos, candidatas = perfil(archivos(origen))
            print(f"-- {nombre_caso}/{relativa}: {filas} filas | {ms / 60000:.1f} min | grupos {len(grupos)}"
                  f" | lote {LOTE.search(origen).group(1)} | llaves {candidatas}")
            if bloque and bloque.get("habilitada"):
                col = bloque.get("columna_particion") or bloque.get("partitionColumn")
                n = int(bloque.get("numero_particiones") or bloque.get("numPartitions") or particiones)
                lo = float(bloque.get("limite_inferior") or bloque.get("lowerBound") or 0)
                hi = float(bloque.get("limite_superior") or bloque.get("upperBound") or 0)
                if col in candidatas:
                    carga = carga_maxima(muestra(grupos, col), lo, hi, n)
                    print(f"   hoy: {col} [{lo:.0f}, {hi:.0f}] en {n} particiones -> la mas cargada tiene {carga}% de las filas")
            mejor = None
            for col in candidatas:
                lo, hi = limites(grupos, col)
                if lo is None:
                    continue
                carga = carga_maxima(muestra(grupos, col), lo, hi + 1, particiones)
                print(f"   {col}: min {lo:.0f} max {hi:.0f} -> con {particiones} particiones la mas cargada tiene {carga}%")
                if carga is not None and (mejor is None or carga < mejor[0]):
                    mejor = (carga, col, lo, hi)
            if mejor:
                carga, col, lo, hi = mejor
                propuestas[nombre_caso][relativa] = {
                    "habilitada": True, "columna_particion": col, "limite_inferior": str(int(lo)),
                    "limite_superior": str(int(hi) + 1), "numero_particiones": str(particiones),
                    "carga_maxima_pct": carga,
                }
    print("\n== Propuesta de lectura_paralela (carga ideal = %.1f%%)" % (100.0 / particiones))
    print(json.dumps(propuestas, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
