#!/usr/bin/env bash
# Solo lectura. NO ejecuta el job: mide cuanto del cluster de Glue usan las corridas
# que ya existen del proceso de extraccion de Emisiones (CloudWatch y Spark event logs).
# Salida: medir-uso-glue-dev-<fecha>.txt -> guardarla en ../salidas/
# Uso: bash medir-uso-glue-dev.sh [numero_de_corridas]   (por defecto 3 corridas exitosas)
export AWS_PAGER=""
export AWS_REGION=us-east-1 AWS_DEFAULT_REGION=us-east-1
OUT="medir-uso-glue-dev-$(date +%Y%m%d-%H%M).txt"
N="${1:-3}" python3 - > "$OUT" 2>&1 <<'EOF'
import json, os, re, statistics
from datetime import timedelta
import boto3

JOB = "niif-dev-extraccion-estandar-emisiones"
N = int(os.environ.get("N", "3"))
glue, cw, s3 = boto3.client("glue"), boto3.client("cloudwatch"), boto3.client("s3")

job = glue.get_job(JobName=JOB)["Job"]
argsd = job.get("DefaultArguments", {})
print("== 1. Job desplegado")
print({k: job.get(k) for k in ("WorkerType", "NumberOfWorkers", "Timeout", "MaxRetries", "GlueVersion")},
      "max_concurrent_runs:", job.get("ExecutionProperty", {}).get("MaxConcurrentRuns"))
print("argumentos:", {k: v for k, v in argsd.items() if k.startswith("--enable") or k in ("--conf", "--spark-event-logs-path")})

runs = []
for p in glue.get_paginator("get_job_runs").paginate(JobName=JOB):
    runs += [r for r in p["JobRuns"] if r["JobRunState"] == "SUCCEEDED" and r.get("ExecutionTime", 0) > 3600]
    if len(runs) >= N: break
runs = runs[:N]

METRICAS = [
    ("glue.driver.ExecutorAllocationManager.executors.numberAllExecutors", "ejecutores_activos"),
    ("glue.ALL.system.cpuSystemLoad", "cpu_ejecutores"),
    ("glue.driver.system.cpuSystemLoad", "cpu_driver"),
    ("glue.ALL.jvm.heap.usage", "heap_ejecutores"),
]

def serie(name, run, start, end):
    period = 300
    r = cw.get_metric_statistics(
        Namespace="Glue", MetricName=name, StartTime=start, EndTime=end, Period=period,
        Statistics=["Average", "Maximum"],
        Dimensions=[{"Name": "JobName", "Value": JOB}, {"Name": "JobRunId", "Value": run}, {"Name": "Type", "Value": "gauge"}])
    return sorted(r["Datapoints"], key=lambda d: d["Timestamp"])

print("\n== 2. Uso del cluster por corrida (CloudWatch, cada 5 min)")
for r in runs:
    start = r["StartedOn"]; end = r.get("CompletedOn") or start + timedelta(seconds=r["ExecutionTime"])
    print(f"-- {r['Id'][:12]}... inicio={str(start)[:19]} min={r['ExecutionTime']/60:.0f} "
          f"DPU-h={(r.get('DPUSeconds') or 0)/3600:.1f} {r.get('WorkerType')}x{r.get('NumberOfWorkers')}")
    for name, alias in METRICAS:
        d = serie(name, r["Id"], start, end)
        if not d:
            print(f"   {alias:<20} sin datos"); continue
        av = [x["Average"] for x in d]
        print(f"   {alias:<20} promedio={statistics.mean(av):.2f} mediana={statistics.median(av):.2f} "
              f"max={max(x['Maximum'] for x in d):.2f} puntos={len(d)}")

print("\n== 3. Etapas de Spark (event logs de las mismas corridas)")
path = argsd.get("--spark-event-logs-path", "")
m = re.match(r"s3://([^/]+)/(.*)", path)
if not m:
    print("sin --spark-event-logs-path")
else:
    bucket, prefix = m[1], m[2]
    objs = []
    for p in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        objs += p.get("Contents", [])
    from collections import defaultdict
    def lines(key):
        for line in s3.get_object(Bucket=bucket, Key=key)["Body"].iter_lines():
            yield line
    for r in runs:
        start = r["StartedOn"]; end = r.get("CompletedOn") or start + timedelta(seconds=r["ExecutionTime"])
        cand = [o for o in objs if start - timedelta(minutes=5) <= o["LastModified"] <= end + timedelta(minutes=30)]
        apps = defaultdict(list)  # con rolling, cada aplicacion es una carpeta eventlog_v2_*
        for o in cand:
            apps[o["Key"].rsplit("/", 1)[0] if "eventlog_v2_" in o["Key"] else o["Key"]].append(o["Key"])
        stages, app_ok = [], False
        for app, keys in apps.items():
            # con rolling, la carpeta trae appstatus_* (vacio) y events_<n>_*: solo se leen los events, en orden
            keys = [k for k in keys if "eventlog_v2_" not in k or "/events_" in k]
            if not keys:
                continue
            keys.sort(key=lambda k: int((re.search(r"/events_(\d+)_", k) or [0, 0])[1]))
            if not any(r["Id"].encode() in l for l in lines(keys[0]) if b"SparkListenerEnvironmentUpdate" in l):
                continue
            app_ok = True
            for k in keys:
                for line in lines(k):
                    if b'"SparkListenerStageCompleted"' not in line: continue
                    si = json.loads(line)["Stage Info"]
                    dur = ((si.get("Completion Time") or 0) - (si.get("Submission Time") or 0)) / 1000
                    stages.append((dur, si["Number of Tasks"], si.get("Stage Name", "")[:60]))
            break
        comprimidos = sum(1 for o in cand if o["Key"].endswith((".lz4", ".zstd", ".snappy", ".lzf")))
        print(f"-- {r['Id'][:12]}... archivos_candidatos={len(cand)} comprimidos={comprimidos} corrida_identificada={app_ok} etapas={len(stages)}")
        if not stages: continue
        tot = sum(s[0] for s in stages)
        una = [s for s in stages if s[1] == 1]
        print(f"   tiempo en etapas={tot/60:.0f} min; etapas con 1 sola tarea={len(una)} ({sum(s[0] for s in una)/60:.0f} min, "
              f"{100*sum(s[0] for s in una)/tot if tot else 0:.0f} %)")
        print(f"   tareas por etapa: mediana={statistics.median(s[1] for s in stages)} max={max(s[1] for s in stages)}")
        print("   20 etapas mas largas (seg, tareas, nombre):")
        for s in sorted(stages, reverse=True)[:20]:
            print(f"   {s[0]:>8.0f} {s[1]:>5}  {s[2]}")
EOF
echo "Listo: $OUT"
