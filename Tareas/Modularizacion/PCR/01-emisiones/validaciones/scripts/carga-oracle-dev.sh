#!/usr/bin/env bash
# Carga sobre la Oracle de Acsel-e vista desde AWS (DEV, solo lectura; no consulta Oracle).
#   bash carga-oracle-dev.sh [horas]          -> ultimas N horas (por defecto 12)
#   bash carga-oracle-dev.sh 2026-11-27T12:00 -> desde esa hora UTC hasta ahora
# 1. Conexiones de Glue a Acsel-e: servicio y si el host es una RDS de esta cuenta (no imprime host ni usuario).
# 2. Jobs de Glue que usan esas conexiones y sus corridas en la ventana.
# 3. Corridas simultaneas por franja de 15 minutos.
# 4. Filas leidas por minuto de las corridas del job de Emisiones (metricas de Glue, si el job las publica).
# 5. Si la Oracle es RDS: Performance Insights (carga por evento de espera).
# Salida: carga-oracle-dev-<fecha>.txt -> guardarla en ../salidas/
export AWS_PAGER=""
export AWS_REGION=us-east-1 AWS_DEFAULT_REGION=us-east-1
OUT="carga-oracle-dev-$(date +%Y%m%d-%H%M%S).txt"
python3 - "$@" 2>&1 <<'EOF' | sed -E 's/(^|[^0-9])[0-9]{12}([^0-9]|$)/\1************\2/g' | tee "$OUT"
import re, sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import boto3

JOB_EMISIONES = "niif-dev-extraccion-estandar-emisiones"
MARCA_CONEXION = "acsele"
FRANJA_MIN = 15
PERIODO_METRICA_S = 300
HORAS_POR_DEFECTO = 12
ESTADOS_ACTIVOS = ("STARTING", "RUNNING", "STOPPING", "WAITING")

glue, cw, rds = boto3.client("glue"), boto3.client("cloudwatch"), boto3.client("rds")
ahora = datetime.now(timezone.utc)
arg = sys.argv[1] if len(sys.argv) > 1 else str(HORAS_POR_DEFECTO)
if re.fullmatch(r"\d+", arg):
    desde = ahora - timedelta(hours=int(arg))
else:
    desde = datetime.fromisoformat(arg).replace(tzinfo=timezone.utc)
print(f"Ventana UTC: {desde:%Y-%m-%d %H:%M} -> {ahora:%Y-%m-%d %H:%M}")


def servicio(url):
    m = re.search(r"[/:]([A-Za-z0-9_$#.]+)\s*$", url or "")
    return m.group(1) if m else "?"


def host(url):
    m = re.search(r"@(?://)?([^:/]+)", url or "") or re.search(r"//([^:/]+)", url or "")
    return (m.group(1) if m else "").lower()


print("\n== 1. Conexiones de Glue a Acsel-e")
conexiones = {}
for p in glue.get_paginator("get_connections").paginate(HidePassword=True):
    for c in p["ConnectionList"]:
        if MARCA_CONEXION in c["Name"].lower():
            url = c.get("ConnectionProperties", {}).get("JDBC_CONNECTION_URL", "")
            conexiones[c["Name"]] = {"servicio": servicio(url), "host": host(url)}
oracles_rds = []
for p in rds.get_paginator("describe_db_instances").paginate():
    for d in p["DBInstances"]:
        if d["Engine"].startswith("oracle"):
            oracles_rds.append(d)
endpoints = {d.get("Endpoint", {}).get("Address", "").lower(): d for d in oracles_rds}
for nombre, c in sorted(conexiones.items()):
    en_rds = endpoints.get(c["host"])
    tipo = "RDS de esta cuenta" if en_rds else ("dominio RDS de otra cuenta" if "rds.amazonaws.com" in c["host"] else "fuera de RDS (on-premise u otra red)")
    c["rds"] = en_rds
    print(f"  {nombre} | servicio {c['servicio']} | {tipo}")
print(f"  Oracles en RDS de esta cuenta: {len(oracles_rds)}")

print("\n== 2. Jobs de Glue que usan esas conexiones y sus corridas en la ventana")
jobs = {}
for p in glue.get_paginator("get_jobs").paginate():
    for j in p["Jobs"]:
        usadas = set(j.get("Connections", {}).get("Connections", [])) & set(conexiones)
        en_args = any(MARCA_CONEXION in str(v).lower() for v in j.get("DefaultArguments", {}).values())
        if usadas or en_args or j["Name"] == JOB_EMISIONES:
            jobs[j["Name"]] = sorted(usadas) or (["(por argumento)"] if en_args else ["(por configuracion)"])
corridas = []
for nombre in sorted(jobs):
    for p in glue.get_paginator("get_job_runs").paginate(JobName=nombre):
        fin_pagina = False
        for r in p["JobRuns"]:
            inicio = r["StartedOn"]
            fin = r.get("CompletedOn") or (ahora if r["JobRunState"] in ESTADOS_ACTIVOS else inicio)
            if fin < desde:
                fin_pagina = True
                continue
            if inicio <= ahora:
                corridas.append({"job": nombre, "id": r["Id"], "inicio": inicio, "fin": fin, "estado": r["JobRunState"],
                                 "min": (fin - inicio).total_seconds() / 60, "dpu_h": (r.get("DPUSeconds") or 0) / 3600})
        if fin_pagina:
            break
print(f"  jobs con conexion a Acsel-e: {len(jobs)} | corridas en la ventana: {len(corridas)}")
for nombre in sorted(jobs):
    print(f"   {nombre}: {', '.join(jobs[nombre])}")
print()
for c in sorted(corridas, key=lambda c: c["inicio"]):
    print(f"  {c['inicio']:%m-%d %H:%M} -> {c['fin']:%H:%M}  {c['min']:6.1f} min  {c['estado']:<10} {c['job']}  {c['id'][:14]}")

print(f"\n== 3. Corridas simultaneas por franja de {FRANJA_MIN} min (todas leen Acsel-e)")
franja = desde.replace(minute=desde.minute - desde.minute % FRANJA_MIN, second=0, microsecond=0)
while franja < ahora:
    siguiente = franja + timedelta(minutes=FRANJA_MIN)
    activas = [c for c in corridas if c["inicio"] < siguiente and c["fin"] > franja]
    if activas:
        otros = sorted({c["job"] for c in activas if c["job"] != JOB_EMISIONES})
        propias = sum(1 for c in activas if c["job"] == JOB_EMISIONES)
        print(f"  {franja:%m-%d %H:%M}  corridas {len(activas):2d} | Emisiones {propias} | otros: {', '.join(otros) or '-'}")
    franja = siguiente

print("\n== 4. Filas leidas por minuto en las corridas de Emisiones (metrica glue.driver.aggregate.recordsRead)")
propias = [c for c in corridas if c["job"] == JOB_EMISIONES]
for c in sorted(propias, key=lambda c: c["inicio"]):
    datos = cw.get_metric_statistics(
        Namespace="Glue", MetricName="glue.driver.aggregate.recordsRead",
        Dimensions=[{"Name": "JobName", "Value": JOB_EMISIONES}, {"Name": "JobRunId", "Value": c["id"]},
                    {"Name": "Type", "Value": "count"}],
        StartTime=c["inicio"], EndTime=c["fin"] + timedelta(minutes=5), Period=PERIODO_METRICA_S, Statistics=["Sum"],
    )["Datapoints"]
    print(f"  {c['id'][:14]} ({c['inicio']:%m-%d %H:%M}, {c['estado']}):", "sin datos (el job no publica metricas de Glue)" if not datos else "")
    for d in sorted(datos, key=lambda d: d["Timestamp"]):
        por_min = d["Sum"] / (PERIODO_METRICA_S / 60)
        print(f"     {d['Timestamp']:%H:%M}  {por_min:12,.0f} filas/min")

print("\n== 5. Performance Insights (solo si la Oracle es RDS de esta cuenta)")
con_rds = [c for c in conexiones.values() if c.get("rds")]
if not con_rds:
    print("  no aplica: la Oracle de Acsel-e no es una RDS de esta cuenta; la carga interna solo la ve el DBA")
else:
    pi = boto3.client("pi")
    for c in con_rds:
        d = c["rds"]
        if not d.get("PerformanceInsightsEnabled"):
            print(f"  servicio {c['servicio']}: RDS sin Performance Insights")
            continue
        r = pi.get_resource_metrics(
            ServiceType="RDS", Identifier=d["DbiResourceId"], StartTime=desde, EndTime=ahora, PeriodInSeconds=FRANJA_MIN * 60,
            MetricQueries=[{"Metric": "db.load.avg", "GroupBy": {"Group": "db.wait_event", "Limit": 5}}],
        )
        print(f"  servicio {c['servicio']}: sesiones activas promedio por evento de espera")
        for serie in r["MetricList"]:
            evento = (serie["Key"].get("Dimensions") or {}).get("db.wait_event.name", "TOTAL")
            puntos = [f"{p['Timestamp']:%H:%M}={p.get('Value', 0):.1f}" for p in serie["DataPoints"] if p.get("Value")]
            print(f"   {evento}: {' '.join(puntos) or '-'}")
EOF
echo "Salida: $OUT"
