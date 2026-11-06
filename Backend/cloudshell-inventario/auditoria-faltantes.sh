#!/usr/bin/env bash
# Recolecta la evidencia que falta para cerrar la auditoria (2026-11-05).
# SOLO LECTURA: usa get/list/describe. No crea, modifica ni borra nada en AWS.
# Ejecutar en CloudShell de la cuenta DEV (us-east-1):  bash auditoria-faltantes.sh
# Resultado: ~/aud-datos.zip  (correos enmascarados; revisar antes de compartir).

set -u
export AWS_DEFAULT_REGION=us-east-1
OUT=~/aud-datos
rm -rf "$OUT"; mkdir -p "$OUT"/{config,iam,s3,glue,lambda,eventos,api,athena,dynamo}
LOG="$OUT/_errores.log"; : > "$LOG"
run() { "$@" 2>>"$LOG" || echo "FALLO: $*" >> "$LOG"; }   # un fallo no corta el script

ASSETS=niif-dev-assets
MALLAS="auxilios-funerarios primas-arl primas-endosos primas-rentas reserva-matematica-arp reserva-matematica-rvi"
BUCKETS="$ASSETS alfa-datalake-dev-bronce alfa-datalake-dev-silver alfa-datalake-dev-gold alfa-datalake-dev-niif-certification alfa-datalake-dev-niif-transport niif-dev-transport"

echo "== 1. Configuracion por malla (SCD, validacion, extraccion, remediacion, estado)"
for m in $MALLAS; do
  mkdir -p "$OUT/config/$m"
  run aws s3 cp "s3://$ASSETS/$m/" "$OUT/config/$m/" --recursive --exclude "*" \
      --include "config_*.json" --include "parametros_extraccion.json" --only-show-errors
done
echo "== 2. Configuracion de certificacion (consultas de Athena, aprobadores, grupos)"
mkdir -p "$OUT/config/certification"
run aws s3 cp "s3://$ASSETS/configs/certification/" "$OUT/config/certification/" --recursive --exclude "*" --include "*.json" --only-show-errors

echo "== 3. Buckets: versionado, cifrado, ciclo de vida, bloqueo publico"
for b in $BUCKETS; do
  d="$OUT/s3/$b"; mkdir -p "$d"
  run aws s3api get-bucket-versioning --bucket "$b" > "$d/versioning.json"
  run aws s3api get-bucket-encryption --bucket "$b" > "$d/encryption.json"
  run aws s3api get-bucket-lifecycle-configuration --bucket "$b" > "$d/lifecycle.json"
  run aws s3api get-public-access-block --bucket "$b" > "$d/publicblock.json"
  run aws s3api get-bucket-policy --bucket "$b" --query Policy --output text > "$d/policy.json"
done

echo "== 4. Glue: script desplegado (hash para comparar con el repo) y definicion de jobs"
JOBS="niif-dev-extraccion-estandar-optimized niif-dev-val-tecnica niif-dev-remediacion-estandar niif-dev-scd niif-dev-certification niif-dev-notification-certification niif-dev-silver-auxilios-funerarios niif-dev-silver-primas-arl niif-dev-silver-primas-endosos niif-dev-silver-primas-rentas niif-dev-silver-reserva-matematica-arp niif-dev-silver-reserva-matematica-rvi"
for j in $JOBS; do
  run aws glue get-job --job-name "$j" > "$OUT/glue/$j.job.json"
  loc=$(python3 -c "import json;print(json.load(open('$OUT/glue/$j.job.json'))['Job']['Command']['ScriptLocation'])" 2>>"$LOG")
  [ -n "$loc" ] && run aws s3 cp "$loc" "$OUT/glue/$j.py" --only-show-errors
  # ultimas 10 corridas: duracion y DPU-segundos reales (costo)
  run aws glue get-job-runs --job-name "$j" --max-results 10 \
    --query 'JobRuns[].{inicio:StartedOn,estado:JobRunState,seg:ExecutionTime,dpuSeg:DPUSeconds,workers:NumberOfWorkers,error:ErrorMessage}' > "$OUT/glue/$j.runs.json"
done
( cd "$OUT/glue" && sha256sum *.py > SHA256.txt 2>>"$LOG" )

echo "== 5. IAM: politicas de los roles de los jobs y de las lambdas de interes"
ROLES=$(for j in $JOBS; do python3 -c "import json;print(json.load(open('$OUT/glue/$j.job.json'))['Job']['Role'].split('/')[-1])" 2>/dev/null; done | sort -u)
for r in $ROLES niif-dev-certification-role niif-dev-short-link-redirect-role niif-dev-composite-notification-evaluator-role niif-dev-finalizador-ejecucion-role niif-dev-conf-auxilios-role; do
  d="$OUT/iam/$r"; mkdir -p "$d"
  run aws iam list-attached-role-policies --role-name "$r" > "$d/attached.json"
  run aws iam list-role-policies --role-name "$r" > "$d/inline-names.json"
  for p in $(aws iam list-role-policies --role-name "$r" --query 'PolicyNames[]' --output text 2>/dev/null); do
    run aws iam get-role-policy --role-name "$r" --policy-name "$p" > "$d/inline-$p.json"
  done
  for arn in $(aws iam list-attached-role-policies --role-name "$r" --query 'AttachedPolicies[].PolicyArn' --output text 2>/dev/null); do
    v=$(aws iam get-policy --policy-arn "$arn" --query Policy.DefaultVersionId --output text 2>/dev/null)
    run aws iam get-policy-version --policy-arn "$arn" --version-id "$v" > "$d/managed-$(basename "$arn").json"
  done
done

echo "== 6. Lambdas: codigo desplegado de las que faltan (solo .py de primer nivel)"
for f in niif-dev-manager-configuration niif-dev-short-link-redirect datalake-dev-niif-send-via-sftp niif-dev-configuration-primas-arl niif-dev-configuration-primas-rentas niif-dev-configuration-primas-endosos niif-dev-configuration-reserva-matematica-arp niif-dev-configuration-reserva-matematica-rvi; do
  d="$OUT/lambda/$f"; mkdir -p "$d"
  run aws lambda get-function-configuration --function-name "$f" > "$d/config.json"
  url=$(aws lambda get-function --function-name "$f" --query Code.Location --output text 2>>"$LOG")
  if [ -n "$url" ]; then
    curl -s -o /tmp/$$.zip "$url" && unzip -o -q /tmp/$$.zip '*.py' -x '*/*' -d "$d" 2>>"$LOG"; rm -f /tmp/$$.zip
  fi
done
# disparadores activos (quien dispara la notificacion, el copiado y el SFTP)
run aws lambda list-event-source-mappings --query 'EventSourceMappings[?contains(FunctionArn,`niif`)].{fn:FunctionArn,src:EventSourceArn,estado:State,reint:MaximumRetryAttempts,dlq:DestinationConfig,filtro:FilterCriteria}' > "$OUT/lambda/event-source-mappings.json"
for b in alfa-datalake-dev-niif-transport niif-dev-transport alfa-datalake-dev-landing-prophet alfa-datalake-dev-niif-certification; do
  run aws s3api get-bucket-notification-configuration --bucket "$b" > "$OUT/s3/notif-$b.json"
done

echo "== 7. EventBridge: reglas de cierre de ejecucion y de objetivos"
run aws events list-rules --name-prefix niif-dev --query 'Rules[].{n:Name,estado:State,patron:EventPattern,prog:ScheduleExpression}' > "$OUT/eventos/reglas-niif-dev.json"
for r in $(aws events list-rules --name-prefix niif-dev --query 'Rules[].Name' --output text 2>/dev/null); do
  run aws events list-targets-by-rule --rule "$r" --query 'Targets[].{id:Id,arn:Arn,dlq:DeadLetterConfig,retry:RetryPolicy}' > "$OUT/eventos/targets-$r.json"
done

echo "== 8. API Gateway (redirect y certificar): autorizador, throttling, logs"
for api in $(aws apigateway get-rest-apis --query 'items[].id' --output text 2>/dev/null); do
  n=$(aws apigateway get-rest-api --rest-api-id "$api" --query name --output text 2>/dev/null)
  case "$n" in *niif*|*NIIF*|*certif*|*redirect*|*short*)
    d="$OUT/api/$n"; mkdir -p "$d"
    run aws apigateway get-authorizers --rest-api-id "$api" > "$d/authorizers.json"
    run aws apigateway get-stages --rest-api-id "$api" --query 'item[].{stage:stageName,logs:accessLogSettings,metodos:methodSettings,trace:tracingEnabled,waf:webAclArn,cache:cacheClusterEnabled}' > "$d/stages.json"
    run aws apigateway get-usage-plans > "$d/usage-plans.json"
    run aws apigateway get-resources --rest-api-id "$api" --embed methods --query 'items[].{path:path,metodos:resourceMethods}' > "$d/resources.json" ;;
  esac
done

echo "== 9. Athena: workgroups y consultas recientes (bytes escaneados)"
run aws athena list-work-groups > "$OUT/athena/workgroups.json"
run aws athena get-work-group --work-group primary > "$OUT/athena/workgroup-primary.json"
ids=$(aws athena list-query-executions --work-group primary --max-results 50 --query 'QueryExecutionIds' --output text 2>>"$LOG")
[ -n "$ids" ] && run aws athena batch-get-query-execution --query-execution-ids $ids \
  --query 'QueryExecutions[].{estado:Status.State,bytes:Statistics.DataScannedInBytes,ms:Statistics.TotalExecutionTimeInMillis,bd:QueryExecutionContext.Database}' > "$OUT/athena/ultimas-consultas.json"

echo "== 10. DynamoDB: tablas del flujo (stream, PITR, cifrado)"
for t in niif-dev-notification-certification niif-dev-composite-notification-status niif-dev-short-links niif-dev-ejc-ejecuciones niif-dev-pipeline-estado niif-dev-cnf-mallas niif-dev-prg-programaciones niif-dev-approval-audit-log; do
  run aws dynamodb describe-table --table-name "$t" --query 'Table.{n:TableName,items:ItemCount,stream:StreamSpecification,sse:SSEDescription,billing:BillingModeSummary,ttl:TimeToLiveDescription}' > "$OUT/dynamo/$t.json"
  run aws dynamodb describe-continuous-backups --table-name "$t" --query 'ContinuousBackupsDescription.PointInTimeRecoveryDescription' > "$OUT/dynamo/$t.pitr.json"
  run aws dynamodb describe-time-to-live --table-name "$t" > "$OUT/dynamo/$t.ttl.json"
done

echo "== 11. Enmascarar correos y empaquetar"
find "$OUT" -type f \( -name '*.json' -o -name '*.py' -o -name '*.txt' \) -print0 | xargs -0 sed -i -E 's/[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}/<correo-enmascarado>/g'
cd ~ && zip -qr aud-datos.zip aud-datos
echo "Listo: ~/aud-datos.zip  (errores no criticos en $LOG)"
ls -la ~/aud-datos.zip; echo "Cantidad de archivos:"; find "$OUT" -type f | wc -l
