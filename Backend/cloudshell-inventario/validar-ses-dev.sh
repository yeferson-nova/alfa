#!/usr/bin/env bash
# Valida en DEV lo necesario para T1 (1a SNS -> SES y 1b correo HTML). SOLO LECTURA.
# Uso en CloudShell de la cuenta DEV:  bash validar-ses-dev.sh
# Salida: ./ses-dev-validacion/ y ses-dev-validacion.zip (contiene correos: NO subir al repo publico).
set -u
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_DEV="533267035849"
ROL="niif-dev-jobs-notification-certification"
JOB="niif-dev-notification-certification"
JOB_TEST="niif-dev-notification-certification-ses-test"
BUCKET="niif-dev-assets"
TABLA="niif-dev-notification-certification"
TOPIC_NAME="niif-dev-certification-email"
REMITENTE="notificaciones@segurosalfa.com.co"
DOMINIO="${REMITENTE#*@}"
CONJUNTO="niif-dev-notificaciones"
OUT="ses-dev-validacion"
export AWS_DEFAULT_REGION="$REGION" AWS_PAGER=""

command -v jq >/dev/null || { echo "Falta jq"; exit 1; }
CUENTA="$(aws sts get-caller-identity --query Account --output text 2>/dev/null)"
if [ "$CUENTA" != "$ACCOUNT_DEV" ]; then
  echo "ABORTA: la cuenta es '$CUENTA' y no la de DEV ($ACCOUNT_DEV). Este script es solo para DEV."
  exit 1
fi
rm -rf "$OUT"; mkdir -p "$OUT"/{ses,iam,glue,s3,lambdas,dynamo,sns,dns}
RESUMEN="$OUT/RESUMEN.txt"; : > "$RESUMEN"
ok()   { echo "OK      $*" | tee -a "$RESUMEN"; }
falta(){ echo "FALTA   $*" | tee -a "$RESUMEN"; }
revisar(){ echo "REVISAR $*" | tee -a "$RESUMEN"; }
run()  { local f="$1"; shift; "$@" >"$OUT/$f" 2>"$OUT/$f.err" || echo "(error: ver $f.err)"; [ -s "$OUT/$f.err" ] || rm -f "$OUT/$f.err"; }

echo "== 1. SES: cuenta, sandbox y cuota"
run ses/cuenta.json aws sesv2 get-account
if [ -s "$OUT/ses/cuenta.json" ]; then
  P="$(jq -r '.ProductionAccessEnabled' "$OUT/ses/cuenta.json")"
  S="$(jq -r '.SendingEnabled' "$OUT/ses/cuenta.json")"
  M="$(jq -r '.SendQuota.Max24HourSend' "$OUT/ses/cuenta.json")"
  [ "$P" = "true" ] && ok "SES fuera del sandbox (cuota 24h: $M)" || falta "SES sigue en SANDBOX: solo envia a destinatarios verificados (cuota 24h: $M)"
  [ "$S" = "true" ] || falta "El envio de SES esta deshabilitado en la cuenta"
else
  falta "No se pudo leer sesv2 get-account (permisos o region)"
fi

echo "== 2. SES: identidades, DKIM y remitente"
run ses/identidades.json aws sesv2 list-email-identities
for i in $(jq -r '.EmailIdentities[]?.IdentityName' "$OUT/ses/identidades.json" 2>/dev/null); do
  run "ses/identidad-${i//[^A-Za-z0-9._-]/_}.json" aws sesv2 get-email-identity --email-identity "$i"
done
for cand in "$REMITENTE" "$DOMINIO"; do
  f="$OUT/ses/identidad-${cand//[^A-Za-z0-9._-]/_}.json"
  if [ -s "$f" ]; then
    ok "Identidad '$cand' existe: verificada=$(jq -r '.VerifiedForSendingStatus' "$f"), DKIM=$(jq -r '.DkimAttributes.Status' "$f"), MAIL FROM=$(jq -r '.MailFromAttributes.MailFromDomainStatus // "no configurado"' "$f")"
  fi
done
[ -s "$OUT/ses/identidad-${REMITENTE//[^A-Za-z0-9._-]/_}.json" ] || [ -s "$OUT/ses/identidad-${DOMINIO//[^A-Za-z0-9._-]/_}.json" ] \
  || falta "Ni '$REMITENTE' ni el dominio '$DOMINIO' estan como identidad en SES"

echo "== 3. SES: conjuntos de configuracion (rebotes y quejas)"
run ses/conjuntos.json aws sesv2 list-configuration-sets
for c in $(jq -r '.ConfigurationSets[]?' "$OUT/ses/conjuntos.json" 2>/dev/null); do
  run "ses/conjunto-$c.json" aws sesv2 get-configuration-set --configuration-set-name "$c"
  run "ses/conjunto-$c.destinos.json" aws sesv2 get-configuration-set-event-destinations --configuration-set-name "$c"
done
if jq -e --arg c "$CONJUNTO" '.ConfigurationSets|index($c)' "$OUT/ses/conjuntos.json" >/dev/null 2>&1; then
  n="$(jq '.EventDestinations|length' "$OUT/ses/conjunto-$CONJUNTO.destinos.json" 2>/dev/null)"
  [ "${n:-0}" -gt 0 ] && ok "Conjunto '$CONJUNTO' existe con $n destino(s) de eventos" || revisar "Conjunto '$CONJUNTO' existe pero sin destino de eventos (rebotes/quejas no se registran)"
else
  falta "No existe el conjunto de configuracion '$CONJUNTO' (job.dev.json lo usa)"
fi
run ses/supresion.json aws sesv2 get-account --query SuppressionAttributes
run ses/plantillas.json aws sesv2 list-email-templates

echo "== 4. DNS del dominio (SPF, DMARC)"
if command -v dig >/dev/null; then
  dig +short TXT "$DOMINIO" >"$OUT/dns/spf.txt" 2>&1
  dig +short TXT "_dmarc.$DOMINIO" >"$OUT/dns/dmarc.txt" 2>&1
  grep -qi "v=spf1" "$OUT/dns/spf.txt" && ok "SPF publicado" || falta "No se ve registro SPF en $DOMINIO"
  grep -qi "v=DMARC1" "$OUT/dns/dmarc.txt" && ok "DMARC publicado" || falta "No se ve registro DMARC en _dmarc.$DOMINIO"
else
  revisar "dig no esta instalado: pedir a quien administra el DNS que confirme SPF, DKIM y DMARC"
fi

echo "== 5. IAM del rol del job"
run iam/rol.json aws iam get-role --role-name "$ROL"
run iam/adjuntas.json aws iam list-attached-role-policies --role-name "$ROL"
run iam/inline-nombres.json aws iam list-role-policies --role-name "$ROL"
for p in $(jq -r '.PolicyNames[]?' "$OUT/iam/inline-nombres.json" 2>/dev/null); do
  run "iam/inline-$p.json" aws iam get-role-policy --role-name "$ROL" --policy-name "$p"
done
for arn in $(jq -r '.AttachedPolicies[]?.PolicyArn' "$OUT/iam/adjuntas.json" 2>/dev/null); do
  v="$(aws iam get-policy --policy-arn "$arn" --query Policy.DefaultVersionId --output text 2>/dev/null)"
  run "iam/adjunta-$(basename "$arn").json" aws iam get-policy-version --policy-arn "$arn" --version-id "$v"
done
ROL_ARN="arn:aws:iam::$ACCOUNT_DEV:role/$ROL"
ID_ARN="arn:aws:ses:$REGION:$ACCOUNT_DEV:identity/$REMITENTE"
CS_ARN="arn:aws:ses:$REGION:$ACCOUNT_DEV:configuration-set/$CONJUNTO"
run iam/simulacion-ses.json aws iam simulate-principal-policy --policy-source-arn "$ROL_ARN" \
  --action-names ses:SendEmail --resource-arns "$ID_ARN" "$CS_ARN"
if [ -s "$OUT/iam/simulacion-ses.json" ]; then
  jq -r '.EvaluationResults[]? | "\(.EvalResourceName) -> \(.EvalDecision)"' "$OUT/iam/simulacion-ses.json" | while read -r l; do
    case "$l" in *allowed) ok "ses:SendEmail: $l";; *) falta "ses:SendEmail: $l";; esac
  done
else
  revisar "No se pudo simular el permiso ses:SendEmail (falta iam:SimulatePrincipalPolicy): revisar a mano las politicas guardadas en iam/"
fi

echo "== 6. Glue: job actual, job de prueba SES y ejecuciones"
run glue/job-actual.json aws glue get-job --job-name "$JOB"
run glue/job-prueba-ses.json aws glue get-job --job-name "$JOB_TEST"
run glue/ejecuciones-actual.json aws glue get-job-runs --job-name "$JOB" --max-results 10
run glue/ejecuciones-prueba-ses.json aws glue get-job-runs --job-name "$JOB_TEST" --max-results 10
if [ -s "$OUT/glue/ejecuciones-prueba-ses.json" ]; then
  jq -r '.JobRuns[]? | "\(.StartedOn) \(.JobRunState) \(.ErrorMessage // "")"' "$OUT/glue/ejecuciones-prueba-ses.json" >"$OUT/glue/resumen-prueba-ses.txt"
  if grep -q SUCCEEDED "$OUT/glue/resumen-prueba-ses.txt"; then ok "El job de prueba SES tiene al menos una ejecucion SUCCEEDED"; else revisar "El job de prueba SES no tiene ejecuciones exitosas (ver glue/resumen-prueba-ses.txt)"; fi
fi
jq -r '.JobRuns[0].Arguments // {} | keys[]' "$OUT/glue/ejecuciones-actual.json" 2>/dev/null >"$OUT/glue/argumentos-ultima-ejecucion.txt"

echo "== 7. S3: artefactos y plantillas"
run s3/glue-resources-notification.txt aws s3 ls "s3://$BUCKET/glue-resources/notification/" --recursive
run s3/glue-resources-libraries.txt aws s3 ls "s3://$BUCKET/glue-resources/libraries/"
grep -q "niif-dev-notification-certification-1.0.0.zip" "$OUT/s3/glue-resources-libraries.txt" \
  && ok "La libreria nueva ya esta en S3" || falta "Falta subir niif-dev-notification-certification-1.0.0.zip a glue-resources/libraries/"
grep -q "job_notificacion.py" "$OUT/s3/glue-resources-notification.txt" \
  && ok "job_notificacion.py ya esta en S3" || falta "Falta subir job_notificacion.py a glue-resources/notification/"
aws s3 cp "s3://$BUCKET/glue-resources/notification/test/notification_certification_ses.py" "$OUT/s3/notification_certification_ses.py" >/dev/null 2>&1 \
  && ok "Descargado el script de la prueba SES existente (s3/notification_certification_ses.py)" || revisar "No se pudo descargar el script de prueba SES"
run s3/configs-certification.txt aws s3 ls "s3://$BUCKET/configs/certification/" --recursive
aws s3 cp "s3://$BUCKET/configs/certification/json_global_notif_cert.json" "$OUT/s3/json_global_notif_cert.json" >/dev/null 2>&1 \
  || revisar "No se pudo descargar json_global_notif_cert.json"
for k in $(awk '{print $4}' "$OUT/s3/configs-certification.txt" 2>/dev/null | grep -E '\.json$' | head -20); do
  aws s3 cp "s3://$BUCKET/$k" "$OUT/s3/$(echo "$k" | tr '/' '_')" >/dev/null 2>&1
done
run s3/html-en-bucket.txt bash -c "aws s3 ls s3://$BUCKET/ --recursive | grep -Ei '\.(html|htm)$' | head -50"
[ -s "$OUT/s3/html-en-bucket.txt" ] && ok "Hay plantillas HTML en $BUCKET (s3/html-en-bucket.txt)" \
  || revisar "No hay .html en $BUCKET: las plantillas de 1b hay que subirlas (origen: Backend/htmlNew/newUx)"

echo "== 8. Quien lee snsMessageId o SNS_EMAIL (lambdas de la cadena final)"
aws lambda list-functions --query "Functions[?contains(FunctionName,'niif-dev') || contains(FunctionName,'datalake-dev')].FunctionName" --output text 2>/dev/null | tr '\t' '\n' >"$OUT/lambdas/lista.txt"
while read -r fn; do
  [ -z "$fn" ] && continue
  url="$(aws lambda get-function --function-name "$fn" --query Code.Location --output text 2>/dev/null)"
  [ -z "$url" ] && continue
  d="$OUT/lambdas/$fn"; mkdir -p "$d"
  curl -s "$url" -o "$d.zip" && unzip -q -o "$d.zip" -d "$d" 2>/dev/null
  rm -rf "$d/"{boto3,botocore,pandas,numpy,pytz,dateutil,s3transfer,urllib3,six*,numpy*,pandas*} "$d.zip" 2>/dev/null
  grep -rIn "snsMessageId\|SNS_EMAIL\|notificationChannel" "$d" 2>/dev/null | head -5 >"$d.hallazgos.txt"
  [ -s "$d.hallazgos.txt" ] && falta "La lambda $fn referencia snsMessageId/SNS_EMAIL/notificationChannel: revisar $fn.hallazgos.txt" || rm -f "$d.hallazgos.txt"
done <"$OUT/lambdas/lista.txt"
ok "Revisadas $(wc -l <"$OUT/lambdas/lista.txt") lambdas niif/datalake-dev en busca de snsMessageId (si no hay FALTA arriba, nadie lo lee)"

echo "== 9. DynamoDB de notificaciones"
run dynamo/tabla.json aws dynamodb describe-table --table-name "$TABLA"
jq -e '.Table.GlobalSecondaryIndexes[]?|select(.IndexName=="certCode-index")' "$OUT/dynamo/tabla.json" >/dev/null 2>&1 \
  && ok "Existe el indice certCode-index" || falta "No existe el indice certCode-index en $TABLA"
run dynamo/muestra.json aws dynamodb scan --table-name "$TABLA" --max-items 5 \
  --projection-expression "certCode, notificationChannel, snsMessageId, sesMessageId, #s" --expression-attribute-names '{"#s":"status"}'

echo "== 10. SNS actual (para saber que se retira)"
TOPIC="arn:aws:sns:$REGION:$ACCOUNT_DEV:$TOPIC_NAME"
run sns/topico.json aws sns get-topic-attributes --topic-arn "$TOPIC"
run sns/suscripciones.json aws sns list-subscriptions-by-topic --topic-arn "$TOPIC" --query "Subscriptions[].{Protocol:Protocol,Arn:SubscriptionArn}"
revisar "Cuando SES quede validado: retirar sns:Publish del rol y decidir que pasa con el topico $TOPIC_NAME"

( cd "$OUT/.." && zip -qr ses-dev-validacion.zip "$OUT" )
echo
echo "=========== RESUMEN ==========="
cat "$RESUMEN"
echo
echo "Listo: ses-dev-validacion.zip (descargar con Acciones > Descargar archivo). Contiene correos: no subirlo al repo publico."
