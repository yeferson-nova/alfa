#!/usr/bin/env bash
# Evidencia de uso de recursos en DEV (SOLO LECTURA: no crea, no cambia, no borra nada).
# Objetivo: saber, con datos y no por el nombre, que lambdas, jobs de Glue y Step Functions
# usa algo de verdad, antes de proponer que se limpia.
# Uso (CloudShell de DEV):  bash evidencia-uso-dev.sh
# Resultado: ~/evidencia-uso.zip  (Actions > Download file). No subirlo a ningun repo publico.

export AWS_PAGER=""
REGION="${REGION:-us-east-1}"
DIAS="${DIAS:-90}"
OUT="$HOME/evidencia-uso"
LOG="$OUT/_errores.log"
rm -rf "$OUT"; mkdir -p "$OUT/raw"; : > "$LOG"
cd "$OUT" || exit 1
A="--region $REGION --output json"

DESDE=$(date -u -d "-$DIAS days" +%Y-%m-%dT00:00:00Z)
HASTA=$(date -u +%Y-%m-%dT00:00:00Z)
PERIODO=$(( DIAS * 86400 ))

echo "== 0. Cuenta =="
aws sts get-caller-identity --query Account --output text > cuenta.txt 2>>"$LOG"
cat cuenta.txt

echo "== 1. Fuentes que pueden invocar lambdas =="
aws lambda list-functions $A --query 'Functions[].[FunctionName,Runtime,PackageType,LastModified]' > raw/lambdas.json 2>>"$LOG"
aws lambda list-event-source-mappings $A > raw/mappings.json 2>>"$LOG"
aws events list-rules $A > raw/reglas.json 2>>"$LOG"
: > raw/reglas-targets.tsv
for r in $(jq -r '.Rules[].Name' raw/reglas.json); do
  aws events list-targets-by-rule --rule "$r" $A 2>>"$LOG" \
    | jq -r --arg r "$r" '.Targets[] | [$r, .Arn] | @tsv' >> raw/reglas-targets.tsv
done
: > raw/schedules.tsv
for g in $(aws scheduler list-schedule-groups $A --query 'ScheduleGroups[].Name' 2>>"$LOG" | jq -r '.[]'); do
  for s in $(aws scheduler list-schedules --group-name "$g" $A --query 'Schedules[].Name' 2>>"$LOG" | jq -r '.[]'); do
    aws scheduler get-schedule --group-name "$g" --name "$s" $A 2>>"$LOG" \
      | jq -r '[.GroupName, .Name, .State, .ScheduleExpression, .Target.Arn] | @tsv' >> raw/schedules.tsv
  done
done
# API Gateway REST (v1) y HTTP (v2): a que lambda apunta cada ruta
: > raw/api-integraciones.tsv
for api in $(aws apigateway get-rest-apis $A --query 'items[].id' 2>>"$LOG" | jq -r '.[]'); do
  nombre=$(aws apigateway get-rest-api --rest-api-id "$api" $A --query name 2>>"$LOG" | jq -r .)
  aws apigateway get-resources --rest-api-id "$api" --embed methods $A 2>>"$LOG" \
    | jq -r --arg api "$api" --arg n "$nombre" '.items[] | .path as $p | (.resourceMethods // {}) | to_entries[]
        | [$api, $n, $p, .key, (.value.methodIntegration.uri // "-")] | @tsv' >> raw/api-integraciones.tsv
  aws apigateway get-stages --rest-api-id "$api" $A 2>>"$LOG" \
    | jq -r --arg api "$api" '.item[] | [$api, .stageName, (.webAclArn // "SIN_WAF")] | @tsv' >> raw/api-stages-waf.tsv
done
for api in $(aws apigatewayv2 get-apis $A --query 'Items[].ApiId' 2>>"$LOG" | jq -r '.[]'); do
  aws apigatewayv2 get-integrations --api-id "$api" $A 2>>"$LOG" \
    | jq -r --arg api "$api" '.Items[] | [$api, "httpapi", "-", "-", (.IntegrationUri // "-")] | @tsv' >> raw/api-integraciones.tsv
done
# Notificaciones de S3 hacia lambdas (solo buckets niif/datalake)
: > raw/s3-notificaciones.tsv
for b in $(aws s3api list-buckets --query 'Buckets[].Name' --output json 2>>"$LOG" | jq -r '.[]' | grep -Ei 'niif|datalake'); do
  aws s3api get-bucket-notification-configuration --bucket "$b" --output json 2>>"$LOG" \
    | jq -r --arg b "$b" '(.LambdaFunctionConfigurations // [])[] | [$b, .LambdaFunctionArn] | @tsv' >> raw/s3-notificaciones.tsv
done
# Definiciones de Step Functions (para saber que lambdas y jobs invocan)
aws stepfunctions list-state-machines $A > raw/sf.json 2>>"$LOG"
mkdir -p raw/sf
: > raw/sf-ultima.tsv
for arn in $(jq -r '.stateMachines[].stateMachineArn' raw/sf.json); do
  n="${arn##*:}"   # solo el nombre: Windows no acepta ":" en archivos
  aws stepfunctions describe-state-machine --state-machine-arn "$arn" $A --query definition 2>>"$LOG" \
    | jq -r . > "raw/sf/$n.asl.json"
  ult=$(aws stepfunctions list-executions --state-machine-arn "$arn" --max-items 1 $A \
        --query 'executions[0].[startDate,status]' 2>>"$LOG" | jq -r 'if . == null then "nunca\t-" else @tsv end')
  printf '%s\t%s\n' "$n" "$ult" >> raw/sf-ultima.tsv
done

echo "== 2. Ficha por lambda =="
printf 'lambda\truntime\tultimo_cambio\tinvocaciones_%sd\terrores_%sd\tmappings\treglas\tschedules\tapi\ts3\tstep_functions\torigenes_politica\ttags\n' "$DIAS" "$DIAS" > lambdas-uso.tsv
jq -r '.[] | @tsv' raw/lambdas.json | while IFS=$'\t' read -r fn rt pt lm; do
  met() { aws cloudwatch get-metric-statistics --namespace AWS/Lambda --metric-name "$1" \
          --dimensions Name=FunctionName,Value="$fn" --start-time "$DESDE" --end-time "$HASTA" \
          --period "$PERIODO" --statistics Sum $A 2>>"$LOG" | jq -r '[.Datapoints[].Sum] | add // 0'; }
  inv=$(met Invocations); err=$(met Errors)
  map=$(jq -r --arg f "$fn" '[.EventSourceMappings[] | select(.FunctionArn|endswith(":function:"+$f))
        | ((.EventSourceArn|split(":")|.[5]|split("/")|.[1] // .) + "=" + .State)] | join(",")' raw/mappings.json)
  reg=$(awk -F'\t' -v f=":function:$fn" 'index($2,f) && (substr($2, index($2,f)+length(f), 1)=="" || substr($2, index($2,f)+length(f), 1)==":") {print $1}' raw/reglas-targets.tsv | paste -sd, -)
  sch=$(awk -F'\t' -v f=":function:$fn" 'index($5,f){print $2"("$3")"}' raw/schedules.tsv | paste -sd, -)
  api=$(awk -F'\t' -v f=":function:$fn/" 'index($5,f){print $2" "$4" "$3}' raw/api-integraciones.tsv | paste -sd, -)
  api2=$(awk -F'\t' -v f=":function:$fn" '$2=="httpapi" && index($5,f){print $1}' raw/api-integraciones.tsv | paste -sd, -)
  s3n=$(awk -F'\t' -v f=":function:$fn" 'index($2,f){print $1}' raw/s3-notificaciones.tsv | sort -u | paste -sd, -)
  sfs=$(grep -l -E "(:function:|\"FunctionName\": *\")$fn(\"|:)" raw/sf/*.asl.json 2>/dev/null | xargs -r -n1 basename | sed 's/.asl.json//' | paste -sd, -)
  pol=$(aws lambda get-policy --function-name "$fn" $A 2>/dev/null | jq -r '.Policy | fromjson | [.Statement[].Principal.Service // "cuenta"] | unique | join(",")' 2>/dev/null)
  tags=$(aws lambda list-tags --resource "arn:aws:lambda:$REGION:$(cat cuenta.txt):function:$fn" $A 2>>"$LOG" | jq -r '.Tags | keys | join(",")')
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$fn" "${rt:-$pt}" "${lm:0:10}" "$inv" "$err" \
    "${map:--}" "${reg:--}" "${sch:--}" "${api:-${api2:--}}" "${s3n:--}" "${sfs:--}" "${pol:--}" "${tags:--}" >> lambdas-uso.tsv
done

echo "== 3. Jobs de Glue: ultima corrida y quien los invoca =="
printf 'job\tultima_corrida\testado\tcorridas_%sd\tstep_functions\n' "$DIAS" > glue-uso.tsv
for j in $(aws glue list-jobs $A --query 'JobNames' 2>>"$LOG" | jq -r '.[]'); do
  runs=$(aws glue get-job-runs --job-name "$j" --max-results 200 $A 2>>"$LOG")
  ult=$(echo "$runs" | jq -r '.JobRuns[0] | [(.StartedOn // "nunca"), (.JobRunState // "-")] | @tsv')
  n=$(echo "$runs" | jq -r --arg d "$DESDE" '[.JobRuns[] | select(.StartedOn >= $d)] | length')
  sfs=$(grep -l "\"$j\"" raw/sf/*.asl.json 2>/dev/null | xargs -r -n1 basename | sed 's/.asl.json//' | paste -sd, -)
  printf '%s\t%s\t%s\t%s\n' "$j" "$ult" "$n" "${sfs:--}" >> glue-uso.tsv
done

echo "== 4. Tablas DynamoDB con stream y quien lo lee =="
printf 'tabla\tstream\tlambdas_que_leen\n' > dynamodb-streams.tsv
for t in $(aws dynamodb list-tables $A --query TableNames 2>>"$LOG" | jq -r '.[]' | grep -Ei 'niif|datalake'); do
  s=$(aws dynamodb describe-table --table-name "$t" $A --query 'Table.LatestStreamArn' 2>>"$LOG" | jq -r '. // "-"')
  l=$(jq -r --arg s "$s" '[.EventSourceMappings[] | select(.EventSourceArn==$s) | (.FunctionArn|split(":")|.[6]) + "=" + .State] | join(",")' raw/mappings.json)
  printf '%s\t%s\t%s\n' "$t" "$(basename "$s")" "${l:--}" >> dynamodb-streams.tsv
done

cp raw/sf-ultima.tsv step-functions-uso.tsv
cd "$HOME" && rm -f evidencia-uso.zip && zip -qr evidencia-uso.zip evidencia-uso
echo "Listo: ~/evidencia-uso.zip  (errores de permisos en evidencia-uso/_errores.log)"
echo "Resumen rapido:"; column -t -s$'\t' "$OUT/lambdas-uso.tsv" | cut -c1-160
