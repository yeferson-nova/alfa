#!/usr/bin/env bash
# Solo lectura. NO ejecuta el job: lee las corridas que ya existen en DEV del proceso de
# extraccion de Emisiones (Glue, Step Functions, parametros_extraccion.json y checkpoints de Bronce).
# Salida: medir-tiempos-dev-<fecha>.txt -> guardarla en ../salidas/
export AWS_PAGER=""
export AWS_REGION=us-east-1 AWS_DEFAULT_REGION=us-east-1
OUT="medir-tiempos-dev-$(date +%Y%m%d-%H%M).txt"
python3 - > "$OUT" 2>&1 <<'EOF'
import json, re, statistics
from collections import defaultdict
from datetime import datetime
import boto3

JOB = "niif-dev-extraccion-estandar-emisiones"
CASOS = ["pcr-vida", "generales", "satelites"]
LOTES_POR_TABLA = 5
glue, s3, sfn = boto3.client("glue"), boto3.client("s3"), boto3.client("stepfunctions")

def t(x):
    try: return datetime.fromisoformat(str(x).replace("Z", "+00:00"))
    except Exception: return None

def seg(a, b):
    a, b = t(a), t(b)
    return (b - a).total_seconds() if a and b else None

def resumen(nombre, valores):
    v = [x for x in valores if x]
    if v:
        print(f"{nombre}: n={len(v)} min={min(v)/60:.0f} mediana={statistics.median(v)/60:.0f} max={max(v)/60:.0f} min")

print("== 1. Corridas del job", JOB, "(ultimas 50)")
runs = []
for p in glue.get_paginator("get_job_runs").paginate(JobName=JOB):
    runs += p["JobRuns"]
    if len(runs) >= 50: break
runs = runs[:50]
print(f"{'inicio':<20} {'estado':<10} {'min':>5} {'DPU-h':>6} {'workers':>8}  caso/args")
for r in runs:
    args = r.get("Arguments", {})
    caso = " ".join(f"{k}={v}" for k, v in args.items() if re.search(r"CASO|MALLA|PERIODO|LOTE", k, re.I))
    print(f"{str(r.get('StartedOn'))[:19]:<20} {r['JobRunState']:<10} {r.get('ExecutionTime',0)/60:>5.0f} "
          f"{(r.get('DPUSeconds') or 0)/3600:>6.1f} {str(r.get('WorkerType'))+'x'+str(r.get('NumberOfWorkers')):>8}  {caso}")
resumen("SUCCEEDED", [r.get("ExecutionTime") for r in runs if r["JobRunState"] == "SUCCEEDED"])

print("\n== 2. Configuracion del job")
j = glue.get_job(JobName=JOB)["Job"]
print({k: j.get(k) for k in ("WorkerType", "NumberOfWorkers", "MaxCapacity", "Timeout", "GlueVersion")})

print("\n== 3. Step Functions de Emisiones (ultimas 20 ejecuciones)")
for p in sfn.get_paginator("list_state_machines").paginate():
    for m in p["stateMachines"]:
        if "emision" not in m["name"].lower(): continue
        print("--", m["name"])
        ex = sfn.list_executions(stateMachineArn=m["stateMachineArn"], maxResults=20)["executions"]
        for e in ex:
            d = (e["stopDate"] - e["startDate"]).total_seconds() / 60 if e.get("stopDate") else None
            print(f"   {str(e['startDate'])[:19]}  {e['status']:<10} {('%.0f' % d) if d else 'en curso':>8} min")
        resumen("   SUCCEEDED", [(e["stopDate"] - e["startDate"]).total_seconds() for e in ex if e["status"] == "SUCCEEDED"])

print("\n== 4. Ultima corrida por caso (parametros_extraccion.json)")
raices = set()
for c in CASOS:
    try:
        d = json.loads(s3.get_object(Bucket="niif-dev-assets", Key=f"{c}/parametros_extraccion.json")["Body"].read())
    except Exception as exc:
        print("--", c, "sin archivo:", type(exc).__name__); continue
    cfg = d.get("configuracion") or {}
    if isinstance(cfg, str):  # en DEV llega como texto: JSON serializado o ruta S3
        try: cfg = json.loads(cfg)
        except Exception:
            print(f"   configuracion ({c}) es texto, no JSON: {cfg[:120]}"); cfg = {}
    rutas = cfg.get("rutas", {}) if isinstance(cfg, dict) else {}
    raices.update(v for k, v in rutas.items() if k in ("s3_bronze_base", "s3_bronze_comun") and isinstance(v, str) and v)
    ds = [z for z in d.get("datasets_extraidos", []) if isinstance(z, dict)]
    tot = sum(x for x in (seg(z.get("fecha_inicio_escritura"), z.get("fecha_fin_escritura")) for z in ds) if x)
    print(f"-- {c}: lote={d.get('lote_cargue_id')} periodo={d.get('periodo_negocio')} estado={d.get('estado_extraccion')} "
          f"fin={d.get('fecha_fin')} tablas={len(ds)} suma_escritura={tot/60:.0f} min")
raices = raices or {"s3://alfa-datalake-dev-bronce/niif/ACSELE"}

print("\n== 5. Historial por tabla (checkpoints _control/_SUCCESS.json ya escritos)")
hist = defaultdict(list)
for raiz in sorted(raices):
    if "-dev-" not in raiz:
        print("se omite raiz fuera de DEV:", raiz); continue
    b, _, pref = raiz.replace("s3://", "").partition("/")
    print("raiz:", raiz)
    for p in s3.get_paginator("list_objects_v2").paginate(Bucket=b, Prefix=pref.rstrip("/") + "/"):
        for o in p.get("Contents", []):
            k = o["Key"]
            if not k.endswith("_control/_SUCCESS.json"): continue
            m = re.search(r"lote_cargue_id=([^/]+)", k)
            tabla = k[len(pref):].split("/periodo_negocio=")[0].strip("/")
            caso = (re.search(r"caso_uso=([^/]+)", k) or [None, "comun"])[1]
            hist[(caso, tabla)].append((m[1] if m else "?", b, k))
por_lote = defaultdict(float)
filas_tabla = []
for (caso, tabla), items in hist.items():
    for lote, b, k in sorted(items, reverse=True)[:LOTES_POR_TABLA]:
        try: r = json.loads(s3.get_object(Bucket=b, Key=k)["Body"].read())
        except Exception: continue
        e = seg(r.get("fecha_inicio_escritura"), r.get("fecha_fin_escritura"))
        n = seg(r.get("fecha_inicio_conteo"), r.get("fecha_fin_conteo"))
        if e: por_lote[(caso, lote)] += e + (n or 0)
        filas_tabla.append((caso, tabla, lote, e, n, r.get("total_registros"), r.get("estado")))
print(f"{'caso':<10} {'lote':<16} {'escr_s':>7} {'conteo_s':>8} {'filas':>12}  estado  tabla")
for f in sorted(filas_tabla, key=lambda x: (x[0], -(x[3] or 0))):
    print(f"{f[0]:<10} {f[2]:<16} {f[3] or -1:>7.0f} {f[4] or -1:>8.0f} {str(f[5]):>12}  {f[6]}  {f[1]}")
print("\n-- Suma por caso y lote (escritura + conteo)")
for (caso, lote), s in sorted(por_lote.items()):
    print(f"{caso:<10} {lote:<16} {s/60:>6.0f} min")
EOF
echo "Listo: $OUT"
