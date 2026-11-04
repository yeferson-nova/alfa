#!/usr/bin/env bash
# Inventario de solo lectura del flujo AWS (Lambdas, orquestacion, datos, seguridad).
# Uso:  bash inventario.sh [PREFIJO]
#   PREFIJO (opcional): filtra Lambdas por nombre (grep -i). Sin prefijo = todas.
#   REGION  (opcional): export REGION=us-east-1  (por defecto la configurada en CloudShell)
# Resultado: ~/inventario.zip  (descargar con Actions > Download file)

export AWS_PAGER=""
PREFIJO="${1:-}"
REGION="${REGION:-$(aws configure get region 2>/dev/null)}"
REGION="${REGION:-us-east-1}"
OUT="$HOME/inventario"
LOG="$OUT/_errores.log"

rm -rf "$OUT"
mkdir -p "$OUT"/{lambdas/codigo,lambdas/config,stepfunctions,glue,eventos,s3,dynamodb,sns,iam,athena,logs}
: > "$LOG"
cd "$OUT" || exit 1

# Ejecuta un comando, guarda stdout en archivo y deja el error en el log (no aborta).
run() { # run <archivo_salida> <comando...>
  local out="$1"; shift
  "$@" > "$out" 2>>"$LOG" || { echo "FALLO: $*" >> "$LOG"; rm -f "$out"; }
}

echo "== Fase 0: identidad y region =="
aws sts get-caller-identity | tee identidad.json
echo "Region: $REGION"
echo "Prefijo: ${PREFIJO:-<todas>}"

echo "== Fase 1: lista de Lambdas =="
aws lambda list-functions --region "$REGION" --output json > lambdas/list-functions.json 2>>"$LOG"
jq -r '.Functions[].FunctionName' lambdas/list-functions.json | sort > lambdas/nombres_todas.txt
if [ -n "$PREFIJO" ]; then
  grep -i -- "$PREFIJO" lambdas/nombres_todas.txt > lambdas/nombres.txt
else
  cp lambdas/nombres_todas.txt lambdas/nombres.txt
fi
jq -r '.Functions[] | [.FunctionName,.Runtime,.Handler,.MemorySize,.Timeout,.PackageType,.Role,.LastModified] | @tsv' \
  lambdas/list-functions.json | sort > lambdas/resumen.tsv
echo "Lambdas a procesar: $(wc -l < lambdas/nombres.txt) de $(wc -l < lambdas/nombres_todas.txt)"

echo "== Fase 2: codigo y configuracion de cada Lambda =="
while read -r f; do
  [ -z "$f" ] && continue
  echo ">> $f"
  aws lambda get-function --function-name "$f" --region "$REGION" --output json > "lambdas/config/$f.json" 2>>"$LOG"
  url=$(jq -r '.Code.Location // empty' "lambdas/config/$f.json")
  tipo=$(jq -r '.Configuration.PackageType // "Zip"' "lambdas/config/$f.json")
  if [ "$tipo" = "Zip" ] && [ -n "$url" ]; then
    curl -s -o "lambdas/codigo/$f.zip" "$url"
    mkdir -p "lambdas/codigo/$f"
    unzip -q -o "lambdas/codigo/$f.zip" -d "lambdas/codigo/$f" 2>>"$LOG"
    rm -f "lambdas/codigo/$f.zip"
  else
    echo "$f: PackageType=$tipo (sin codigo descargable, ver ImageUri en config)" >> lambdas/sin_codigo.txt
  fi
  run "lambdas/config/$f.triggers.json" aws lambda list-event-source-mappings --function-name "$f" --region "$REGION"
  run "lambdas/config/$f.policy.json"   aws lambda get-policy --function-name "$f" --region "$REGION"
  run "lambdas/config/$f.aliases.json"  aws lambda list-aliases --function-name "$f" --region "$REGION"
  run "lambdas/config/$f.url.json"      aws lambda get-function-url-config --function-name "$f" --region "$REGION"
done < lambdas/nombres.txt
run lambdas/layers.json aws lambda list-layers --region "$REGION"

echo "== Fase 3: orquestacion (Step Functions, EventBridge, SNS, SQS) =="
for arn in $(aws stepfunctions list-state-machines --region "$REGION" --query 'stateMachines[].stateMachineArn' --output text 2>>"$LOG"); do
  run "stepfunctions/$(basename "$arn").json" aws stepfunctions describe-state-machine --state-machine-arn "$arn" --region "$REGION"
done
for bus in $(aws events list-event-buses --region "$REGION" --query 'EventBuses[].Name' --output text 2>>"$LOG"); do
  for r in $(aws events list-rules --event-bus-name "$bus" --region "$REGION" --query 'Rules[].Name' --output text 2>>"$LOG"); do
    run "eventos/$bus.$r.regla.json"   aws events describe-rule --name "$r" --event-bus-name "$bus" --region "$REGION"
    run "eventos/$bus.$r.targets.json" aws events list-targets-by-rule --rule "$r" --event-bus-name "$bus" --region "$REGION"
  done
done
run sns/topics.json aws sns list-topics --region "$REGION"
for t in $(aws sns list-topics --region "$REGION" --query 'Topics[].TopicArn' --output text 2>>"$LOG"); do
  run "sns/$(basename "$t").subs.json" aws sns list-subscriptions-by-topic --topic-arn "$t" --region "$REGION"
  run "sns/$(basename "$t").attrs.json" aws sns get-topic-attributes --topic-arn "$t" --region "$REGION"
done
run sns/sqs-colas.json aws sqs list-queues --region "$REGION"

echo "== Fase 4: Glue, DynamoDB, S3 =="
run glue/jobs.json      aws glue get-jobs --region "$REGION"
run glue/databases.json aws glue get-databases --region "$REGION"
run glue/triggers.json  aws glue get-triggers --region "$REGION"
run glue/workflows.json aws glue list-workflows --region "$REGION"
run glue/crawlers.json  aws glue get-crawlers --region "$REGION"
if [ -f glue/jobs.json ]; then
  for s in $(jq -r '.Jobs[].Command.ScriptLocation // empty' glue/jobs.json | sort -u); do
    aws s3 cp "$s" glue/scripts/ --only-show-errors 2>>"$LOG" || true
  done
fi
for db in $(jq -r '.DatabaseList[].Name' glue/databases.json 2>/dev/null); do
  run "glue/tablas-$db.json" aws glue get-tables --database-name "$db" --region "$REGION"
done
for t in $(aws dynamodb list-tables --region "$REGION" --query 'TableNames[]' --output text 2>>"$LOG"); do
  run "dynamodb/$t.json" aws dynamodb describe-table --table-name "$t" --region "$REGION"
done
for b in $(aws s3api list-buckets --query 'Buckets[].Name' --output text 2>>"$LOG"); do
  run "s3/$b.notificaciones.json" aws s3api get-bucket-notification-configuration --bucket "$b"
  run "s3/$b.cifrado.json"        aws s3api get-bucket-encryption --bucket "$b"
  run "s3/$b.lifecycle.json"      aws s3api get-bucket-lifecycle-configuration --bucket "$b"
  run "s3/$b.versionado.json"     aws s3api get-bucket-versioning --bucket "$b"
  run "s3/$b.acceso-publico.json" aws s3api get-public-access-block --bucket "$b"
done

echo "== Fase 5: seguridad y costos (IAM, Athena, KMS, Lake Formation) =="
for arn in $(jq -r '.Configuration.Role // empty' lambdas/config/*.json 2>/dev/null | grep -v '^$' | sort -u); do
  r=$(basename "$arn")
  run "iam/$r.adjuntas.json" aws iam list-attached-role-policies --role-name "$r"
  run "iam/$r.inline.json"   aws iam list-role-policies --role-name "$r"
  for p in $(jq -r '.PolicyNames[]?' "iam/$r.inline.json" 2>/dev/null); do
    run "iam/$r.inline.$p.json" aws iam get-role-policy --role-name "$r" --policy-name "$p"
  done
  for pa in $(jq -r '.AttachedPolicies[]?.PolicyArn' "iam/$r.adjuntas.json" 2>/dev/null); do
    v=$(aws iam get-policy --policy-arn "$pa" --query 'Policy.DefaultVersionId' --output text 2>>"$LOG")
    run "iam/$r.adj.$(basename "$pa").json" aws iam get-policy-version --policy-arn "$pa" --version-id "$v"
  done
done
run athena/workgroups.json aws athena list-work-groups --region "$REGION"
for w in $(jq -r '.WorkGroups[].Name' athena/workgroups.json 2>/dev/null); do
  run "athena/wg-$w.json" aws athena get-work-group --work-group "$w" --region "$REGION"
done
run iam/kms-alias.json aws kms list-aliases --region "$REGION"
run iam/lakeformation-settings.json aws lakeformation get-data-lake-settings --region "$REGION"
run iam/lakeformation-permisos.json aws lakeformation list-permissions --region "$REGION"

echo "== Fase 6: actividad de los ultimos 7 dias =="
INI=$(date -u -d '7 days ago' +%FT%TZ); FIN=$(date -u +%FT%TZ)
printf "lambda\tinvocaciones_7d\terrores_7d\n" > logs/actividad-7d.tsv
while read -r f; do
  [ -z "$f" ] && continue
  inv=$(aws cloudwatch get-metric-statistics --namespace AWS/Lambda --metric-name Invocations \
    --dimensions Name=FunctionName,Value="$f" --start-time "$INI" --end-time "$FIN" --period 604800 \
    --statistics Sum --region "$REGION" --query 'Datapoints[0].Sum' --output text 2>>"$LOG")
  err=$(aws cloudwatch get-metric-statistics --namespace AWS/Lambda --metric-name Errors \
    --dimensions Name=FunctionName,Value="$f" --start-time "$INI" --end-time "$FIN" --period 604800 \
    --statistics Sum --region "$REGION" --query 'Datapoints[0].Sum' --output text 2>>"$LOG")
  printf "%s\t%s\t%s\n" "$f" "${inv:-0}" "${err:-0}" >> logs/actividad-7d.tsv
done < lambdas/nombres.txt
run logs/alarmas.json aws cloudwatch describe-alarms --region "$REGION"

echo "== Fase 7: saneamiento de secretos y empaquetado =="
# Las variables de entorno pueden traer secretos: se dejan solo los NOMBRES de las variables.
mkdir -p lambdas/env-nombres
for c in lambdas/config/*.json; do
  case "$c" in *.triggers.json|*.policy.json|*.aliases.json|*.url.json) continue;; esac
  n=$(basename "$c" .json)
  jq -r '.Configuration.Environment.Variables // {} | keys[]' "$c" > "lambdas/env-nombres/$n.txt" 2>/dev/null
  # Version sin valores de variables
  jq '.Configuration.Environment.Variables |= (if . then with_entries(.value="***OCULTO***") else . end)' "$c" > "$c.tmp" 2>/dev/null && mv "$c.tmp" "$c"
done

cd "$HOME" && rm -f inventario.zip && zip -r -q inventario.zip inventario
echo
echo "LISTO. Archivo: $HOME/inventario.zip ($(du -h inventario.zip | cut -f1))"
echo "Errores/omisiones (permisos, recursos inexistentes): $OUT/_errores.log"
echo "Descargar: menu Actions > Download file > escribir: inventario.zip"
