#!/usr/bin/env bash
set -uo pipefail
export AWS_PAGER=""
export AWS_REGION=us-east-1 AWS_DEFAULT_REGION=us-east-1

AMB="${1:?Indique el ambiente: dev, qa o prd}"
DIAS="${2:-7}"
P="niif-${AMB}"
DESDE=$(date -u -d "${DIAS} days ago" +%Y-%m-%dT%H:%M:%SZ)
HASTA=$(date -u +%Y-%m-%dT%H:%M:%SZ)
DESDE_MS=$(date -d "$DESDE" +%s000)
SALIDA="diag-despliegue-emisiones-${AMB}-$(date -u +%Y%m%dT%H%M%SZ).txt"
REMEDIACION_MD5_MAIN="93f3e343fb04148138136b87ffb247bb"

limpiar() { sed -E 's/[0-9]{12}/<cuenta>/g; s/[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[a-z]{2,}/<correo>/g'; }
titulo() { echo; echo "=================== $* ==================="; }

{
titulo "0. Contexto"
echo "ambiente=${AMB} desde=${DESDE} hasta=${HASTA} region=${AWS_REGION} version=2026-11-27.1"

titulo "1. Lambdas: version, handler y mapa de configuradores"
for f in "${P}-manager-configuration" "${P}-programador-configuraciones" "${P}-configuration-general" "${P}-configuration-emisiones"; do
  echo "-- $f"
  aws lambda get-function-configuration --function-name "$f" \
    --query '{mod:LastModified,sha:CodeSha256,handler:Handler,runtime:Runtime,vars:Environment.Variables}' \
    --output json 2>&1 | grep -vE 'ACCOUNT_ID|ROLE_ARN' | head -30
done

titulo "2. Lambdas que leen el stream de prg-programaciones"
STREAM=$(aws dynamodb describe-table --table-name "${P}-prg-programaciones" --query Table.LatestStreamArn --output text 2>&1)
echo "stream=${STREAM##*/}"
aws lambda list-event-source-mappings --event-source-arn "$STREAM" \
  --query 'EventSourceMappings[].[FunctionArn,State,LastModified,LastProcessingResult]' --output text 2>&1

titulo "3. Programacion PROGRAMACION#EMISIONES (solo campos de control)"
aws dynamodb get-item --table-name "${P}-prg-programaciones" \
  --key '{"PK":{"S":"PROGRAMACION#EMISIONES"},"SK":{"S":"METADATA"}}' \
  --projection-expression 'activo,scheduleEstado,scheduleName,scheduleExpression,scheduleTimezone,periodoNegocio,modoEjecucion,loteCargueId' \
  --output json 2>&1 | head -40

titulo "4. Schedules del grupo y su destino"
for s in $(aws scheduler list-schedules --group-name "${P}-programacion-configuradores" --query 'Schedules[].Name' --output text 2>/dev/null); do
  aws scheduler get-schedule --group-name "${P}-programacion-configuradores" --name "$s" \
    --query '[Name,ScheduleExpression,State,Target.Arn]' --output text 2>&1
done
echo "(si no aparece nada: no hay schedules vivos; los at() se borran al ejecutarse)"

titulo "5. CloudTrail: quien programo, quien arranco y que se desplego"
for ev in CreateSchedule UpdateSchedule StartExecution UpdateStateMachine UpdateFunctionCode20150331v2 UpdateFunctionConfiguration20150331v2 UpdateJob; do
  aws cloudtrail lookup-events --lookup-attributes AttributeKey=EventName,AttributeValue=$ev \
    --start-time "$DESDE" --end-time "$HASTA" --query 'Events[].CloudTrailEvent' --output text 2>/dev/null \
  | python3 -c "
import sys, json
for l in sys.stdin.read().split('\t'):
    l = l.strip()
    if not l:
        continue
    e = json.loads(l)
    r = e.get('requestParameters') or {}
    i = e.get('userIdentity') or {}
    q = ((i.get('sessionContext') or {}).get('sessionIssuer') or {}).get('userName') or i.get('type')
    recurso = r.get('name') or r.get('functionName') or r.get('stateMachineArn', '').split(':')[-1] or r.get('jobName') or ''
    destino = (r.get('target') or {}).get('arn', '').split(':')[-1]
    print(e['eventTime'], e['eventName'], q, recurso, destino, e.get('errorCode') or '', (e.get('errorMessage') or '')[:160])
"
done

titulo "6. Errores de la configuradora general"
aws logs filter-log-events --log-group-name "/aws/lambda/${P}-configuration-general" --start-time "$DESDE_MS" \
  --filter-pattern '?ERROR ?Error ?Traceback ?ValueError ?RuntimeError ?"Ambiente no soportado"' \
  --query 'events[].[timestamp,message]' --output text 2>&1 | cut -c1-300 | head -40
echo "-- invocaciones (REPORT) en el periodo"
aws logs filter-log-events --log-group-name "/aws/lambda/${P}-configuration-general" --start-time "$DESDE_MS" \
  --filter-pattern 'REPORT' --query 'length(events)' --output text 2>&1

titulo "7. Step Function ${P}-emisiones: definicion, ejecuciones y estado que fallo"
SM=$(aws stepfunctions list-state-machines --query "stateMachines[?name=='${P}-emisiones'].stateMachineArn" --output text)
if [ -z "$SM" ]; then
  echo "NO EXISTE la maquina ${P}-emisiones"
else
  aws stepfunctions describe-state-machine --state-machine-arn "$SM" --query 'definition' --output text > /tmp/sf-emisiones.json
  echo "definicion md5=$(md5sum < /tmp/sf-emisiones.json | cut -c1-32) actualizada=$(aws stepfunctions describe-state-machine --state-machine-arn "$SM" --query 'creationDate' --output text)"
  echo "-- jobs y lambdas que llama"
  grep -oE '"(JobName|FunctionName)": *"[^"]+"' /tmp/sf-emisiones.json | sort | uniq -c
  echo "-- argumentos de Silver Generales"
  python3 -c "
import json
d = json.load(open('/tmp/sf-emisiones.json'))
for b in d['States']['ProcesamientoPorCaso']['Branches']:
    for n, s in b['States'].items():
        if n.startswith('PreparacionSilver'):
            print(n, sorted(s['Parameters'].get('Arguments', {}).keys()))
" 2>&1
  echo "-- ultimas ejecuciones"
  aws stepfunctions list-executions --state-machine-arn "$SM" --max-items 8 \
    --query 'executions[].[name,status,startDate,stopDate]' --output text
  for EX in $(aws stepfunctions list-executions --state-machine-arn "$SM" --status-filter FAILED --max-items 2 --query 'executions[].executionArn' --output text); do
    echo "-- fallo: ${EX##*:}"
    aws stepfunctions get-execution-history --execution-arn "$EX" --max-items 400 --output json \
    | python3 -c "
import sys, json
h = json.load(sys.stdin)['events']
entradas = {}
for e in h:
    if e['type'].endswith('StateEntered'):
        entradas[e['id']] = e['stateEnteredEventDetails']['name']
for e in h:
    t = e['type']
    if 'Failed' in t or 'TimedOut' in t or 'Aborted' in t:
        det = next((v for k, v in e.items() if k.endswith('EventDetails')), {}) or {}
        estado = entradas.get(e.get('previousEventId'), '')
        prev = e.get('previousEventId')
        while not estado and prev:
            pe = next((x for x in h if x['id'] == prev), None)
            if not pe:
                break
            estado = entradas.get(pe['id'], '')
            prev = pe.get('previousEventId')
        print(e['timestamp'], t, estado, det.get('error', ''), str(det.get('cause', ''))[:400])
" 2>&1
  done
fi

titulo "8. Jobs de Glue de la malla: existencia, script y ultimas corridas"
for j in extraccion-estandar-emisiones val-tecnica val-tecnica-emisiones remediacion-estandar silver-vida silver-generales silver-satelites scd certification; do
  J="${P}-${j}"
  SCRIPT=$(aws glue get-job --job-name "$J" --query 'Job.Command.ScriptLocation' --output text 2>&1)
  if echo "$SCRIPT" | grep -q EntityNotFound; then
    echo "-- $J : NO EXISTE"
    continue
  fi
  echo "-- $J : script=${SCRIPT#s3://*/}"
  aws glue get-job --job-name "$J" --query 'Job.{mod:LastModifiedOn,glue:GlueVersion,extra:DefaultArguments."--extra-py-files"}' --output json 2>&1 | tr -d '\n '; echo
  B=$(echo "$SCRIPT" | sed -E 's#s3://([^/]+)/.*#\1#'); K=$(echo "$SCRIPT" | sed -E 's#s3://[^/]+/##')
  aws s3api head-object --bucket "$B" --key "$K" --query '{etag:ETag,mod:LastModified,bytes:ContentLength}' --output json 2>&1 | tr -d '\n '; echo
  aws glue get-job-runs --job-name "$J" --max-items 3 \
    --query 'JobRuns[].[StartedOn,JobRunState,ExecutionTime,ErrorMessage]' --output text 2>&1 | cut -c1-300
done
echo "md5 esperado de remediacion_estandar.py en main (cambio de ruta Silver por caso_uso): ${REMEDIACION_MD5_MAIN}"

titulo "9. EventBridge Scheduler: invocaciones y errores por hora"
for m in InvocationAttemptCount TargetErrorCount InvocationDroppedCount InvocationThrottleCount; do
  echo "-- $m"
  aws cloudwatch get-metric-statistics --namespace AWS/Scheduler --metric-name $m \
    --dimensions Name=ScheduleGroup,Value="${P}-programacion-configuradores" \
    --start-time "$DESDE" --end-time "$HASTA" --period 3600 --statistics Sum \
    --query 'sort_by(Datapoints,&Timestamp)[].[Timestamp,Sum]' --output text 2>&1
done
} 2>&1 | limpiar | tee "$SALIDA"

echo
echo "Salida: $SALIDA  sha256=$(sha256sum "$SALIDA" | cut -c1-12)"
