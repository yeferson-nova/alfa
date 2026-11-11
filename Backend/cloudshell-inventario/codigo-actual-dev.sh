#!/usr/bin/env bash
# Baja el codigo ACTUAL (SOLO LECTURA) de los jobs de Glue, Step Functions y lambdas del alcance NIIF 17
# (6 mallas + cadena de notificacion/certificacion), con sus argumentos y etiquetas (para saber si son de Terraform).
# Uso (CloudShell de DEV):  bash codigo-actual-dev.sh
# Resultado: ~/codigo-actual.zip  -> descargar con /home/cloudshell-user/codigo-actual.zip

export AWS_PAGER=""
REGION="${REGION:-us-east-1}"
A="--region $REGION --output json"
OUT="$HOME/codigo-actual"
LOG="$OUT/_errores.log"
rm -rf "$OUT"; mkdir -p "$OUT"/{glue,stepfunctions,lambdas}; : > "$LOG"
CUENTA=$(aws sts get-caller-identity --query Account --output text)

MALLAS="primas-rentas primas-endosos primas-arl auxilios-funerarios reserva-matematica-arp reserva-matematica-rvi"
JOBS="niif-dev-extraccion-estandar-optimized niif-dev-val-tecnica niif-dev-remediacion-estandar niif-dev-scd niif-dev-certification niif-dev-notification-certification"
for m in $MALLAS; do JOBS="$JOBS niif-dev-silver-$m"; done
LAMBDAS="niif-dev-manager-configuration niif-dev-composite-notification-evaluator niif-dev-notification-certification-trigger niif-dev-copy-s3-cert niif-dev-move-s3-certification niif-dev-certification niif-dev-short-link-redirect niif-post-login"
for m in $MALLAS; do LAMBDAS="$LAMBDAS niif-dev-configuration-$m"; done

echo "== Glue: script, librerias extra, argumentos y etiquetas =="
for j in $JOBS; do
  d="$OUT/glue/$j"; mkdir -p "$d"
  aws glue get-job --job-name "$j" $A > "$d/job.json" 2>>"$LOG" || { echo "FALLO get-job $j" >> "$LOG"; continue; }
  loc=$(jq -r '.Job.Command.ScriptLocation' "$d/job.json")
  aws s3 cp "$loc" "$d/" --only-show-errors 2>>"$LOG"
  for lib in $(jq -r '.Job.DefaultArguments["--extra-py-files"] // empty' "$d/job.json" | tr ',' ' '); do
    aws s3 cp "$lib" "$d/libs/" --only-show-errors 2>>"$LOG"
  done
  aws glue get-tags --resource-arn "arn:aws:glue:$REGION:$CUENTA:job/$j" $A > "$d/tags.json" 2>>"$LOG"
  echo "$j <- $loc"
done

echo "== Step Functions: definicion y etiquetas =="
for m in $MALLAS; do
  n="niif-dev-$m"; arn="arn:aws:states:$REGION:$CUENTA:stateMachine:$n"
  aws stepfunctions describe-state-machine --state-machine-arn "$arn" $A --query definition 2>>"$LOG" | jq -r . > "$OUT/stepfunctions/$n.asl.json"
  aws stepfunctions list-tags-for-resource --resource-arn "$arn" $A > "$OUT/stepfunctions/$n.tags.json" 2>>"$LOG"
  echo "$n"
done

echo "== Lambdas: codigo (zip), configuracion sin variables y etiquetas =="
for f in $LAMBDAS; do
  d="$OUT/lambdas/$f"; mkdir -p "$d"
  url=$(aws lambda get-function --function-name "$f" $A --query 'Code.Location' 2>>"$LOG" | jq -r . )
  [ -n "$url" ] && [ "$url" != "null" ] && curl -s -o "$d/codigo.zip" "$url"
  # Solo los NOMBRES de las variables de entorno (los valores pueden ser sensibles)
  aws lambda get-function-configuration --function-name "$f" $A 2>>"$LOG" \
    | jq '{FunctionName,Runtime,Handler,Timeout,MemorySize,LastModified,Layers:[.Layers[]?.Arn],VariablesEntorno:(.Environment.Variables // {} | keys)}' > "$d/config.json"
  aws lambda list-tags --resource "arn:aws:lambda:$REGION:$CUENTA:function:$f" $A > "$d/tags.json" 2>>"$LOG"
  echo "$f"
done

cd "$HOME" && rm -f codigo-actual.zip && zip -qr codigo-actual.zip codigo-actual
echo "Listo: /home/cloudshell-user/codigo-actual.zip  (errores en codigo-actual/_errores.log)"
echo "Quien los administra (etiquetas):"
for t in "$OUT"/glue/*/tags.json "$OUT"/stepfunctions/*.tags.json "$OUT"/lambdas/*/tags.json; do
  printf '%s\t%s\n' "$(basename "$(dirname "$t")")/$(basename "$t")" "$(jq -c '(.Tags // .tags // {}) | if type=="array" then map({(.key):.value})|add else . end | {ManagedBy,Owner,Project} ' "$t" 2>/dev/null)"
done | column -t -s$'\t'
