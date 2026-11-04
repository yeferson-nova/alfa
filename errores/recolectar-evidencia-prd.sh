#!/usr/bin/env bash
# ============================================================================
# Recolecta la evidencia para validar el incidente de Primas ARL periodo 202606
# en PROD (correo con "No hay resumen configurado" / "Totales: No hay datos").
#
# Solo LEE: describe, get, list, filter-log-events, query, scan y (opcional)
# consultas SELECT de Athena. No modifica ni escribe nada en AWS.
#
# Uso en AWS CloudShell (cuenta de prod, region us-east-1):
#   1. Subir este archivo (Actions > Upload file).
#   2. sed -i 's/\r$//' recolectar-evidencia-prd.sh     # por si vino con CRLF
#   3. bash recolectar-evidencia-prd.sh
#   4. Descargar el .zip que imprime al final (Actions > Download file).
#
# Variables opcionales (todas tienen valor por defecto):
#   ENV=prd  PERIODO=202606  REGION=us-east-1
#   EXECUTION_NAME=arl-202606-auto-20261102T030032Z-976ae7fc   (nombre completo)
#   EJECUTAR_ATHENA=1    ejecuta las consultas summary/total/dataPresence del JSON
#                        del caso (SELECT sobre Gold; genera costo en Athena)
#   LOGS_COMPLETOS=1     trae los logs sin filtrar (pueden ser grandes)
# ============================================================================
set -u

ENV="${ENV:-prd}"
PERIODO="${PERIODO:-202606}"
REGION="${REGION:-us-east-1}"
EXECUTION_NAME="${EXECUTION_NAME:-}"
EJECUTAR_ATHENA="${EJECUTAR_ATHENA:-0}"
LOGS_COMPLETOS="${LOGS_COMPLETOS:-0}"

ASSETS_BUCKET="${ASSETS_BUCKET:-niif-${ENV}-assets}"
GLOBAL_CONFIG_KEY="${GLOBAL_CONFIG_KEY:-configs/certification/json_global_notif_cert.json}"
COMPOSITE_GROUPS_KEY="${COMPOSITE_GROUPS_KEY:-configs/certification/composite_groups.json}"
STATE_MACHINE="${STATE_MACHINE:-niif-${ENV}-primas-arl}"
LAMBDA_EVALUADOR="${LAMBDA_EVALUADOR:-niif-${ENV}-composite-notification-evaluator}"
COMPOSITE_TABLE="${COMPOSITE_TABLE:-niif-${ENV}-composite-notification-status}"
PARAMS_KEY="${PARAMS_KEY:-primas-arl/parametros_extraccion.json}"
JOB_CERT="niif-${ENV}-certification"
JOB_NOTIF="niif-${ENV}-notification-certification"
JOBS_FLUJO="niif-${ENV}-extraccion-estandar-optimized niif-${ENV}-val-tecnica niif-${ENV}-remediacion-estandar niif-${ENV}-silver-primas-arl niif-${ENV}-scd ${JOB_CERT} ${JOB_NOTIF}"

export AWS_DEFAULT_REGION="$REGION" AWS_PAGER=""

SALIDA="evidencia-primas-arl-${PERIODO}-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$SALIDA"/{stepfunction,glue,logs,s3,dynamodb,athena}
RESUMEN="$SALIDA/00-resumen.txt"
FALLOS="$SALIDA/00-fallos.txt"
: > "$RESUMEN"; : > "$FALLOS"

log()  { echo "[$(date +%H:%M:%S)] $*" | tee -a "$RESUMEN"; }
fallo() { echo "[FALLO] $*" | tee -a "$FALLOS" "$RESUMEN"; }

for cmd in aws jq python3; do
  command -v "$cmd" >/dev/null 2>&1 || { echo "Falta el comando: $cmd"; exit 1; }
done

# ----------------------------------------------------------------------------
# 0. Identidad y cuenta
# ----------------------------------------------------------------------------
log "== 0. Identidad =="
aws sts get-caller-identity --output json > "$SALIDA/00-identidad.json" 2>>"$FALLOS" \
  || { fallo "sts get-caller-identity (revisar credenciales/cuenta)"; exit 1; }
ACCOUNT="$(jq -r .Account "$SALIDA/00-identidad.json")"
log "Cuenta=$ACCOUNT  Region=$REGION  ENV=$ENV  Periodo=$PERIODO"
[ "$ACCOUNT" = "992382496548" ] || log "AVISO: la cuenta no es 992382496548 (prod segun el correo)."

SM_ARN="arn:aws:states:${REGION}:${ACCOUNT}:stateMachine:${STATE_MACHINE}"

# ----------------------------------------------------------------------------
# 1. Step Function: ubicar la ejecucion y extraer pasos
# ----------------------------------------------------------------------------
log "== 1. Step Function ${STATE_MACHINE} =="
if [ -z "$EXECUTION_NAME" ]; then
  aws stepfunctions list-executions --state-machine-arn "$SM_ARN" --max-items 300 --output json \
    > "$SALIDA/stepfunction/ejecuciones-recientes.json" 2>>"$FALLOS" \
    || fallo "list-executions"
  EXECUTION_NAME="$(jq -r --arg p "arl-${PERIODO}" \
    '[.executions[] | select(.name|startswith($p))] | sort_by(.startDate) | last | .name // empty' \
    "$SALIDA/stepfunction/ejecuciones-recientes.json" 2>/dev/null)"
fi
[ -n "$EXECUTION_NAME" ] || { fallo "No se encontro ejecucion arl-${PERIODO}*. Pasar EXECUTION_NAME=..."; }
log "Ejecucion: ${EXECUTION_NAME:-<no encontrada>}"

EXEC_ARN="arn:aws:states:${REGION}:${ACCOUNT}:execution:${STATE_MACHINE}:${EXECUTION_NAME}"
WIN_INI=0; WIN_FIN=0
if [ -n "$EXECUTION_NAME" ]; then
  aws stepfunctions describe-execution --execution-arn "$EXEC_ARN" --output json \
    > "$SALIDA/stepfunction/describe-execution.json" 2>>"$FALLOS" || fallo "describe-execution"
  aws stepfunctions get-execution-history --execution-arn "$EXEC_ARN" --include-execution-data \
    --output json > "$SALIDA/stepfunction/history-completo.json" 2>>"$FALLOS" || fallo "get-execution-history"

  jq '{status, startDate, stopDate, name, input}' "$SALIDA/stepfunction/describe-execution.json" \
    > "$SALIDA/stepfunction/01-estado-y-entrada.json" 2>/dev/null
  log "Estado: $(jq -r .status "$SALIDA/stepfunction/describe-execution.json" 2>/dev/null)"

  # Todos los estados con su entrada/salida
  jq '[.events[]
       | select(.stateEnteredEventDetails or .stateExitedEventDetails)
       | {id, timestamp, type,
          estado: (.stateEnteredEventDetails.name // .stateExitedEventDetails.name),
          entrada: .stateEnteredEventDetails.input,
          salida: .stateExitedEventDetails.output}]' \
    "$SALIDA/stepfunction/history-completo.json" > "$SALIDA/stepfunction/02-estados-entrada-salida.json" 2>/dev/null

  # Fallos, timeouts y reintentos
  jq '[.events[] | select(.type|test("Failed|TimedOut|Aborted|Retry"))]' \
    "$SALIDA/stepfunction/history-completo.json" > "$SALIDA/stepfunction/03-fallos-y-reintentos.json" 2>/dev/null

  # Salida del paso resolver_notificacion (notificationMode, configKey, etc.)
  jq '[.[] | select(.estado=="resolver_notificacion")]' \
    "$SALIDA/stepfunction/02-estados-entrada-salida.json" > "$SALIDA/stepfunction/04-resolver_notificacion.json" 2>/dev/null
  jq '[.[] | select(.salida) | .salida | fromjson? | .. | objects
        | select(has("configKey")) | {notificationMode, shouldStartCertification, useCaseId, period,
                                      loteCargueId, configBucket, configKey, compositeId, compositeGroupId}]
      | unique' \
    "$SALIDA/stepfunction/02-estados-entrada-salida.json" > "$SALIDA/stepfunction/05-configKey-resueltos.json" 2>/dev/null
  log "configKey resueltos en la ejecucion:"; jq -c '.[]' "$SALIDA/stepfunction/05-configKey-resueltos.json" 2>/dev/null | tee -a "$RESUMEN"

  # Ventana de tiempo para logs y job runs
  INI="$(jq -r .startDate "$SALIDA/stepfunction/describe-execution.json")"
  FIN="$(jq -r '.stopDate // empty' "$SALIDA/stepfunction/describe-execution.json")"
  WIN_INI=$(( $(date -d "$INI" +%s) - 600 ))
  if [ -n "$FIN" ]; then WIN_FIN=$(( $(date -d "$FIN" +%s) + 21600 )); else WIN_FIN=$(date +%s); fi
  log "Ventana: $(date -u -d @$WIN_INI +%FT%TZ) a $(date -u -d @$WIN_FIN +%FT%TZ)"
fi

# ----------------------------------------------------------------------------
# 2. Glue: corridas de los jobs del flujo en la ventana
# ----------------------------------------------------------------------------
log "== 2. Glue job runs =="
JQ_TS='def ts: sub("\\.[0-9]+";"") | sub("\\+00:00$";"Z");'
: > "$SALIDA/glue/job-runs-relevantes.jsonl"
for JOB in $JOBS_FLUJO; do
  aws glue get-job-runs --job-name "$JOB" --max-results 50 --output json \
    > "$SALIDA/glue/${JOB}.runs.json" 2>>"$FALLOS" || { fallo "get-job-runs $JOB"; continue; }
  if [ "$WIN_INI" -gt 0 ]; then
    jq -c --argjson a "$WIN_INI" --argjson b "$WIN_FIN" --arg job "$JOB" "$JQ_TS"'
      .JobRuns[] | select((.StartedOn|ts|fromdateiso8601) >= $a and (.StartedOn|ts|fromdateiso8601) <= $b)
      | select(($job|test("extraccion|val-tecnica|remediacion|silver|scd")) or ((.Arguments|tostring)|test("arl|Arl|ARL")))
      | {job: $job, id: .Id, estado: .JobRunState, inicio: .StartedOn, fin: .CompletedOn,
         error: .ErrorMessage, argumentos: .Arguments}' \
      "$SALIDA/glue/${JOB}.runs.json" >> "$SALIDA/glue/job-runs-relevantes.jsonl" 2>/dev/null
  fi
done
log "Corridas relevantes: $(wc -l < "$SALIDA/glue/job-runs-relevantes.jsonl")"
jq -rs '.[] | "\(.job)  \(.estado)  \(.inicio)  \(.id)  \(.error // "")"' \
  "$SALIDA/glue/job-runs-relevantes.jsonl" 2>/dev/null | tee -a "$RESUMEN"

# configKey y certCode usados realmente por certificacion/notificacion
CONFIG_KEYS="$( { jq -r '.[].configKey' "$SALIDA/stepfunction/05-configKey-resueltos.json" 2>/dev/null
                  jq -rs '.[] | .argumentos["--CONFIG_KEY"] // empty' "$SALIDA/glue/job-runs-relevantes.jsonl" 2>/dev/null; } | sort -u | grep -v '^$')"
CERT_CODES="$(jq -rs '.[] | .argumentos["--certCode"] // empty' "$SALIDA/glue/job-runs-relevantes.jsonl" 2>/dev/null | sort -u | grep -v '^$')"
log "CONFIG_KEY usados: $(echo $CONFIG_KEYS)"
log "certCode: $(echo $CERT_CODES)"

# ----------------------------------------------------------------------------
# 3. S3: configuraciones
# ----------------------------------------------------------------------------
log "== 3. S3 (${ASSETS_BUCKET}) =="
bajar_s3() { # bucket key destino
  aws s3 cp "s3://$1/$2" "$3" --only-show-errors 2>>"$FALLOS" && log "OK s3://$1/$2" || fallo "s3 cp s3://$1/$2"
}
bajar_s3 "$ASSETS_BUCKET" "$GLOBAL_CONFIG_KEY"   "$SALIDA/s3/global_notif_cert.json"
bajar_s3 "$ASSETS_BUCKET" "$COMPOSITE_GROUPS_KEY" "$SALIDA/s3/composite_groups.json"
bajar_s3 "$ASSETS_BUCKET" "$PARAMS_KEY"          "$SALIDA/s3/parametros_extraccion_primas_arl.json"
bajar_s3 "$ASSETS_BUCKET" "primas-arl/config_extraccion_primas_arl.json" "$SALIDA/s3/config_extraccion_primas_arl.json"
N=0
for CK in $CONFIG_KEYS; do
  N=$((N+1)); bajar_s3 "$ASSETS_BUCKET" "$CK" "$SALIDA/s3/config_caso_${N}_$(basename "$CK")"
done
# Resumen rapido: cuantas consultas summary/total/detail tiene cada JSON de caso
for F in "$SALIDA"/s3/config_caso_*; do
  [ -f "$F" ] || continue
  echo "--- $(basename "$F")" | tee -a "$RESUMEN"
  jq -c '{useCaseId, summary: ((.queries.summary // [])|length), total: ((.queries.total // [])|length),
          detail: ((.queries.detail // [])|length), dataPresence: ((.email.dataPresence // [])|length)}' "$F" 2>/dev/null | tee -a "$RESUMEN"
done
if [ -f "$SALIDA/s3/composite_groups.json" ]; then
  echo "--- grupos que mencionan primasArl:" | tee -a "$RESUMEN"
  jq -c '.. | objects | select((tostring|test("primasArl";"i")) and has("grupoId"))
         | {grupoId, members, notificationUseCaseId, notificationConfigKey, notificationConfigBucket}' \
    "$SALIDA/s3/composite_groups.json" 2>/dev/null | tee -a "$RESUMEN"
fi

NOTIF_TABLE="$(jq -r '.dynamodb.notification_table // empty' "$SALIDA/s3/global_notif_cert.json" 2>/dev/null)"
NOTIF_TABLE="${NOTIF_TABLE:-niif-${ENV}-notification-certification}"
log "Tabla de notificaciones: $NOTIF_TABLE"

# ----------------------------------------------------------------------------
# 4. CloudWatch: lambda evaluadora y jobs de certificacion/notificacion
# ----------------------------------------------------------------------------
log "== 4. CloudWatch Logs =="
PATRON='?TABLES ?ATHENA ?ARGS ?DYNAMO ?"[S3]" ?ERROR ?Error ?Traceback ?AccessDenied ?COMPOSITE ?WARN'
if [ "$WIN_INI" -gt 0 ]; then
  aws logs filter-log-events --log-group-name "/aws/lambda/${LAMBDA_EVALUADOR}" \
    --start-time $((WIN_INI*1000)) --end-time $((WIN_FIN*1000)) --output json \
    > "$SALIDA/logs/lambda-${LAMBDA_EVALUADOR}.json" 2>>"$FALLOS" || fallo "logs lambda evaluadora"
  for ROW in $(jq -rs '.[] | select(.job|test("certification")) | .job + "|" + .id' "$SALIDA/glue/job-runs-relevantes.jsonl" 2>/dev/null); do
    JOB="${ROW%%|*}"; RUNID="${ROW##*|}"
    for GRP in /aws-glue/jobs/output /aws-glue/jobs/error /aws-glue/jobs/logs-v2; do
      DEST="$SALIDA/logs/${JOB}_${RUNID}_$(basename "$GRP").json"
      if [ "$LOGS_COMPLETOS" = "1" ]; then
        aws logs filter-log-events --log-group-name "$GRP" --log-stream-name-prefix "$RUNID" --output json > "$DEST" 2>>"$FALLOS"
      else
        aws logs filter-log-events --log-group-name "$GRP" --log-stream-name-prefix "$RUNID" \
          --filter-pattern "$PATRON" --output json > "$DEST" 2>>"$FALLOS"
      fi
      [ -s "$DEST" ] && jq -r '.events[]?.message' "$DEST" > "${DEST%.json}.txt" 2>/dev/null
    done
  done
  log "Lineas clave de notificacion (TABLES/ATHENA):"
  grep -h -E "TABLES|ATHENA\] (Query SUCCEEDED|Query FAILED|Query CANCELLED)|AccessDenied" "$SALIDA"/logs/*notification*txt 2>/dev/null | head -40 | tee -a "$RESUMEN"
else
  fallo "Sin ventana de tiempo: se omiten los logs (no se encontro la ejecucion)"
fi

# ----------------------------------------------------------------------------
# 5. DynamoDB
# ----------------------------------------------------------------------------
log "== 5. DynamoDB =="
for CC in $CERT_CODES; do
  aws dynamodb query --table-name "$NOTIF_TABLE" --index-name certCode-index \
    --key-condition-expression "certCode = :c" \
    --expression-attribute-values "{\":c\":{\"S\":\"$CC\"}}" --output json \
    > "$SALIDA/dynamodb/notificacion_certCode_${CC}.json" 2>>"$FALLOS" || fallo "dynamodb query certCode=$CC"
done
aws dynamodb scan --table-name "$COMPOSITE_TABLE" \
  --filter-expression "contains(compositeId, :p)" \
  --expression-attribute-values "{\":p\":{\"S\":\"#${PERIODO}\"}}" --output json \
  > "$SALIDA/dynamodb/composite_status_${PERIODO}.json" 2>>"$FALLOS" || fallo "dynamodb scan $COMPOSITE_TABLE"
jq -c '.Items[]? | {queryName: .queryName.S, status: .status.S, sinDatosPeriodo: .sinDatosPeriodo.BOOL,
                    useCaseId: .useCaseId.S, nameFile: .nameFile.S, configKey: .configKey.S}' \
  "$SALIDA"/dynamodb/notificacion_certCode_*.json 2>/dev/null | tee -a "$RESUMEN"

# ----------------------------------------------------------------------------
# 6. Athena (opcional): extraer las consultas del caso y, si se pide, ejecutarlas
# ----------------------------------------------------------------------------
log "== 6. Consultas del caso (Athena) =="
CASO_JSON="$(ls "$SALIDA"/s3/config_caso_* 2>/dev/null | head -1)"
if [ -n "$CASO_JSON" ] && [ -f "$SALIDA/s3/global_notif_cert.json" ]; then
  CERT_UNO="$(echo $CERT_CODES | awk '{print $1}')"
  python3 - "$CASO_JSON" "$SALIDA/s3/global_notif_cert.json" "$SALIDA/athena" "$PERIODO" "$ENV" "${CERT_UNO:-}" <<'PY'
import json, sys, calendar
from datetime import datetime
from string import Template
caso, glob, out, periodo, env, cert = sys.argv[1:7]
c = json.load(open(caso)); g = json.load(open(glob))
y, m = int(periodo[:4]), int(periodo[4:6])
rango = f"{datetime(y,m,1):%d/%m/%Y} - {datetime(y,m,calendar.monthrange(y,m)[1]):%d/%m/%Y}"
base = {"env": g.get("environment", env), "period": periodo, "periodRange": rango,
        "useCaseId": c.get("useCaseId", ""), "certCode": cert}
items = []
for tipo in ("summary", "total"):
    for q in c.get("queries", {}).get(tipo, []):
        items.append((tipo, q.get("queryName", "sin_nombre"), q["database"], q["query"]))
for q in c.get("email", {}).get("dataPresence", []) or []:
    items.append(("dataPresence", q.get("name", "sin_nombre"), q["database"], q["query"]))
for tipo, nombre, db, sql in items:
    nombre = nombre.replace("#", "_").replace(" ", "_")
    open(f"{out}/{tipo}_{nombre}.sql", "w").write(Template(sql).safe_substitute(base) + "\n")
    open(f"{out}/{tipo}_{nombre}.db", "w").write(Template(db).safe_substitute(base))
print(f"{len(items)} consultas extraidas (summary/total/dataPresence)")
PY
  ls "$SALIDA"/athena/*.sql >/dev/null 2>&1 || log "AVISO: el JSON del caso no trae consultas summary/total (consistente con 'No hay resumen configurado')."

  if [ "$EJECUTAR_ATHENA" = "1" ]; then
    CATALOGO="$(jq -r '.catalog // "AwsDataCatalog"' "$SALIDA/s3/global_notif_cert.json")"
    SALIDA_ATHENA="s3://$(jq -r '.buckets.assets' "$SALIDA/s3/global_notif_cert.json")/$(jq -r '.athena.output_path' "$SALIDA/s3/global_notif_cert.json")/"
    for SQL in "$SALIDA"/athena/*.sql; do
      [ -f "$SQL" ] || continue
      BASE="${SQL%.sql}"; DB="$(cat "$BASE.db")"
      QID="$(aws athena start-query-execution --query-string "file://$SQL" \
              --query-execution-context "Database=$DB,Catalog=$CATALOGO" \
              --result-configuration "OutputLocation=$SALIDA_ATHENA" \
              --query QueryExecutionId --output text 2>>"$FALLOS")"
      [ -n "$QID" ] || { fallo "athena start $(basename "$SQL")"; continue; }
      for _ in $(seq 1 120); do
        ESTADO="$(aws athena get-query-execution --query-execution-id "$QID" --query 'QueryExecution.Status.State' --output text)"
        case "$ESTADO" in SUCCEEDED|FAILED|CANCELLED) break;; esac; sleep 2
      done
      aws athena get-query-execution --query-execution-id "$QID" --output json > "$BASE.estado.json" 2>/dev/null
      [ "$ESTADO" = "SUCCEEDED" ] && aws athena get-query-results --query-execution-id "$QID" --output json > "$BASE.resultado.json" 2>>"$FALLOS"
      FILAS="$(jq '(.ResultSet.Rows|length) - 1' "$BASE.resultado.json" 2>/dev/null)"
      log "Athena $(basename "$SQL"): $ESTADO  filas=${FILAS:-n/a}"
    done
  else
    log "Athena NO ejecutado (usar EJECUTAR_ATHENA=1). Las consultas quedaron en athena/*.sql"
  fi
else
  fallo "No hay JSON de caso o global: se omite Athena"
fi

# ----------------------------------------------------------------------------
# 7. Empaquetar
# ----------------------------------------------------------------------------
log "== 7. Empaquetado =="
if command -v zip >/dev/null 2>&1; then zip -qr "${SALIDA}.zip" "$SALIDA"; else tar -czf "${SALIDA}.tar.gz" "$SALIDA"; fi
echo
echo "Listo. Revisar $RESUMEN y $FALLOS"
ls -1 "${SALIDA}".zip "${SALIDA}".tar.gz 2>/dev/null
echo "Descargar con CloudShell: Actions > Download file > $(ls -1 "${SALIDA}".zip "${SALIDA}".tar.gz 2>/dev/null | head -1)"
echo "Antes de compartir, revisar que no contenga correos o datos sensibles (userEmail, ARNs)."
