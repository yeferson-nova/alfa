#!/usr/bin/env bash
set -uo pipefail
export AWS_PAGER=""

AMB="${AMB:-qa}"
MALLA="${MALLA:-rentas}"
N="${N:-6}"
WG="${WG:-primary}"
SF_NOMBRE="niif-${AMB}-primas-${MALLA}"
DB="niif-${AMB}-gold-catalog-db"
TABLA="scd_primas_${MALLA}"
RUTA_PARAMETROS="s3://niif-${AMB}-assets/primas-${MALLA}/parametros_extraccion.json"
RUTA_SCD="s3://niif-${AMB}-assets/glue-resources/estandar/scd.py"

consultar() {
  local id e
  id=$(aws athena start-query-execution --work-group "$WG" --query-string "$1" \
    --query-execution-context "Database=$DB" --query QueryExecutionId --output text) || { echo "ERROR_INICIO"; return; }
  for _ in $(seq 90); do
    e=$(aws athena get-query-execution --query-execution-id "$id" --query QueryExecution.Status.State --output text)
    case "$e" in
      SUCCEEDED) aws athena get-query-results --query-execution-id "$id" --output text \
                   --query 'ResultSet.Rows[].Data[].VarCharValue' | tr '\t' ' '; return ;;
      FAILED|CANCELLED) aws athena get-query-execution --query-execution-id "$id" \
                   --query QueryExecution.Status.StateChangeReason --output text; return ;;
    esac
    sleep 2
  done
  echo "ERROR_TIEMPO"
}

pasos_por_ejecucion() {
  local sf
  sf=$(aws stepfunctions list-state-machines --query "stateMachines[?name=='${SF_NOMBRE}'].stateMachineArn" --output text)
  echo "== Duración por paso de las últimas ${N} ejecuciones de ${SF_NOMBRE}"
  for e in $(aws stepfunctions list-executions --state-machine-arn "$sf" --max-items "$N" \
               --query 'executions[].executionArn' --output text); do
    aws stepfunctions get-execution-history --execution-arn "$e" --max-items 1000 --output json | python3 -c '
import json, sys
from datetime import datetime
nombre = sys.argv[1].split(":")[-1]
eventos = json.load(sys.stdin)["events"]
hora = lambda ev: datetime.fromisoformat(ev["timestamp"])
inicio, pasos = {}, {}
for ev in eventos:
    if ev["type"] == "TaskStateEntered":
        inicio.setdefault(ev["stateEnteredEventDetails"]["name"], hora(ev))
    if ev["type"] == "TaskStateExited":
        paso = ev["stateExitedEventDetails"]["name"]
        if paso in inicio and not paso.startswith("Consultar"):
            pasos[paso] = hora(ev) - inicio[paso]
total = hora(eventos[-1]) - hora(eventos[0])
print(f"-- {nombre} | inicio {hora(eventos[0]):%Y-%m-%d %H:%M} | total {total}")
for paso in ("ExtraccionEstandar", "CrawlerBronze", "ValidacionTecnica", "Remediacion",
             "SilverJob", "CrawlerSilver", "GoldSCD", "CrawlerGold"):
    if paso in pasos:
        print(f"   {paso:20} {pasos[paso]}")' "$e"
  done
}

comparar() {
  local sf a b
  sf=$(aws stepfunctions list-state-machines --query "stateMachines[?name=='${SF_NOMBRE}'].stateMachineArn" --output text)
  a="${1:-}"; b="${2:-}"
  if [ -z "$a" ] || [ -z "$b" ]; then
    read -r b a < <(aws stepfunctions list-executions --state-machine-arn "$sf" --max-items 2 \
                      --query 'executions[].name' --output text | head -1)
  fi
  for e in "$a" "$b"; do
    aws stepfunctions get-execution-history --execution-arn "${sf/:stateMachine:/:execution:}:${e}" \
      --max-items 1000 --output json > "/tmp/historial_${e}.json" || return 1
  done
  python3 - "$a" "$b" <<'PY'
import json, sys
from datetime import datetime, timedelta

def pasos(nombre):
    eventos = json.load(open(f"/tmp/historial_{nombre}.json"))["events"]
    hora = lambda ev: datetime.fromisoformat(ev["timestamp"])
    inicio, salida = {}, {}
    for ev in eventos:
        if ev["type"] == "TaskStateEntered":
            inicio.setdefault(ev["stateEnteredEventDetails"]["name"], hora(ev))
        if ev["type"] == "TaskStateExited":
            paso = ev["stateExitedEventDetails"]["name"]
            if paso in inicio:
                salida[paso] = hora(ev) - inicio[paso]
    salida["TOTAL"] = hora(eventos[-1]) - hora(eventos[0])
    return salida, hora(eventos[0])

def texto(t):
    if t is None:
        return "-"
    s = int(t.total_seconds())
    return f"{'-' if s < 0 else ''}{abs(s) // 60:d} min {abs(s) % 60:02d} s"

a, b = sys.argv[1], sys.argv[2]
pa, ia = pasos(a)
pb, ib = pasos(b)
print(f"== Comparación\n   A (anterior): {a} | inicio {ia:%Y-%m-%d %H:%M}\n   B (nueva)   : {b} | inicio {ib:%Y-%m-%d %H:%M}")
print(f"   {'Paso':20} {'A':>12} {'B':>12} {'B - A':>12}")
for paso in ("ExtraccionEstandar", "CrawlerBronze", "ValidacionTecnica", "Remediacion",
             "SilverJob", "CrawlerSilver", "GoldSCD", "CrawlerGold", "TOTAL"):
    ta, tb = pa.get(paso), pb.get(paso)
    dif = tb - ta if ta is not None and tb is not None else None
    print(f"   {paso:20} {texto(ta):>12} {texto(tb):>12} {texto(dif):>12}")
PY
}

cruce() {
  echo "== Ejecuciones de Rentas y Endosos en orden de inicio (para ver si se cruzaron)"
  for m in rentas endosos; do
    local sf
    sf=$(aws stepfunctions list-state-machines --query "stateMachines[?name=='niif-${AMB}-primas-${m}'].stateMachineArn" --output text)
    aws stepfunctions list-executions --state-machine-arn "$sf" --max-items "$N" --output json | python3 -c '
import json, sys
for e in json.load(sys.stdin)["executions"]:
    print("{}|{}|{}|{}|{}".format(e["startDate"][:19], e.get("stopDate", "en curso")[:19], sys.argv[1], e["status"], e["name"]))' "$m"
  done | sort | python3 -c '
import sys
filas = [l.rstrip("\n").split("|") for l in sys.stdin]
print("   {:19}  {:19}  {:8} {:10} {}".format("inicio", "fin", "malla", "estado", "ejecucion"))
for i, (ini, fin, malla, estado, nombre) in enumerate(filas):
    cruza = [f[2] for f in filas[:i] if f[1] == "en curso" or f[1] > ini]
    marca = "  <- cruza con " + ", ".join(sorted(set(cruza))) if cruza else ""
    print(f"   {ini:19}  {fin:19}  {malla:8} {estado:10} {nombre}{marca}")'
}

corridas_glue() {
  local job="$1" filtro="$2"
  echo "== ${job} (solo corridas de ${MALLA}): inicio | estado | segundos | DPU-segundos"
  aws glue get-job-runs --job-name "$job" --max-items 50 --output json | python3 -c '
import json, sys
filtro, n = sys.argv[1], int(sys.argv[2])
corridas = [c for c in json.load(sys.stdin)["JobRuns"] if filtro in json.dumps(c.get("Arguments", {}))]
for c in corridas[:n]:
    print("   {} | {:9} | {:6} | {}".format(c["StartedOn"][:16], c["JobRunState"],
          c.get("ExecutionTime", 0), c.get("DPUSeconds", "-")))' "$filtro" "$N"
}

estado_gold() {
  echo "== Métricas del último SCD (parametros_extraccion.json)"
  aws s3 cp "$RUTA_PARAMETROS" - | python3 -c '
import json, sys
p = json.load(sys.stdin)
for k in ("periodo_negocio", "total_registros_silver_preparada", "estado_gold_scd", "motivo_gold_scd",
          "metricas_gold_scd", "control_periodo_scd"):
    print(f"   {k}: {json.dumps(p.get(k), ensure_ascii=False)}")'
  echo "== Versión de scd.py desplegada (1 o más = SCD con control de periodo)"
  aws s3 cp "$RUTA_SCD" - | grep -c "periodo_activo"
  echo "== Tamaño de Gold: act_reg | registros"
  consultar "SELECT act_reg, COUNT(*) FROM \"$DB\".\"$TABLA\" GROUP BY act_reg ORDER BY act_reg"
  echo "== Archivos y tamaño de Gold en S3"
  aws s3 ls "s3://alfa-datalake-${AMB}-gold/niif/${TABLA}/" --recursive --summarize | tail -2
}

case "${1:-todo}" in
  comparar) shift; comparar "$@" ;;
  cruce) cruce ;;
  todo)
    pasos_por_ejecucion
    corridas_glue "niif-${AMB}-extraccion-estandar-optimized" "primas-${MALLA}"
    corridas_glue "niif-${AMB}-silver-primas-${MALLA}" "primas_${MALLA}"
    corridas_glue "niif-${AMB}-scd" "primas-${MALLA}"
    estado_gold ;;
  *) echo "Uso: [AMB=qa] [MALLA=rentas|endosos] [N=6] bash $0 {todo|comparar [anterior nueva]|cruce}" ;;
esac
