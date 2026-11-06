#!/usr/bin/env bash
# Compara lo DESPLEGADO en AWS contra la arquitectura entregada
# (Docs/05-arquitectura/diagramas/NIIF17_ModeloSolucion_V1.0.drawio) y contra los criterios de
# resiliencia, escalabilidad, seguridad y costo del proyecto.
#
# SOLO LECTURA: usa describe/get/list. No crea, modifica ni borra nada. No descarga código,
# variables de entorno, valores de secretos ni datos de tablas.
#
# Uso (AWS CloudShell, cuenta del ambiente a revisar):
#   bash comparar-arquitectura.sh                 # DEV
#   AMB=prd bash comparar-arquitectura.sh         # PRD (en la cuenta de producción)
#   VPC_CIDR=<cidr-de-la-vpc> bash comparar-arquitectura.sh   # opcional: verifica la VPC del diagrama
# Resultado: ~/niif-arquitectura-<amb>/resumen.tsv  y  ~/niif-arquitectura-<amb>.zip
#   Estados: OK | FALTA (el diagrama lo muestra y no existe) | REVISAR (existe con riesgo) | SIN-ACCESO
#
# OJO: el resumen incluye nombres de recursos de la cuenta. No subirlo al repositorio (es público).

export AWS_PAGER=""
REGION="${REGION:-us-east-1}"
AMB="${AMB:-dev}"
export AWS_DEFAULT_REGION="$REGION"
OUT="$HOME/niif-arquitectura-$AMB"
RES="$OUT/resumen.tsv"
ERR="$OUT/errores.log"
rm -rf "$OUT" "$OUT.zip"
mkdir -p "$OUT"
: > "$ERR"
printf 'id\testado\tdetalle\n' > "$RES"

r() { printf '%s\t%s\t%s\n' "$1" "$2" "$3" | tee -a "$RES"; }
limpio() { tr '\t\n' '  ' | sed 's/  */ /g' | cut -c1-220; }
seccion() { echo; echo "== $* =="; }

# existe <id> <texto-ok> <texto-falta> <comando aws...>
existe() {
  local id="$1" ok="$2" no="$3"; shift 3
  local v rc
  v=$(aws "$@" 2>>"$ERR"); rc=$?
  if [ $rc -ne 0 ]; then r "$id" SIN-ACCESO "aws $1 $2"
  elif [ -z "$v" ] || [ "$v" = "None" ]; then r "$id" FALTA "$no"
  else r "$id" OK "$ok: $(printf '%s' "$v" | limpio)"; fi
}

ACC=$(aws sts get-caller-identity --query Account --output text 2>>"$ERR")
[ -z "$ACC" ] && { echo "No hay credenciales de AWS."; exit 1; }

# ------------------------------------------------------------------ 1. Programación (hoja Página-3)
seccion "1. Programación: DynamoDB Stream -> Lambda -> EventBridge Pipe -> EventBridge"
existe ARQ-PIPE "EventBridge Pipe presente" \
  "El diagrama muestra un EventBridge Pipe (enrutador); no hay ninguno (hoy: stream -> lambda -> Scheduler)" \
  pipes list-pipes --query 'Pipes[].Name' --output text
existe ARQ-SCHED "Grupos de Scheduler" "No hay grupos de EventBridge Scheduler" \
  scheduler list-schedule-groups --query 'ScheduleGroups[].Name' --output text

# ------------------------------------------------------------------ 2. Entrada de aprobación (hoja Página-2)
seccion "2. API Gateway + WAF + Cognito + identidad externa"
existe SEG-WAF "Web ACL regional" \
  "El diagrama pone WAF perimetral delante de API Gateway; no hay Web ACL regional" \
  wafv2 list-web-acls --scope REGIONAL --query 'WebACLs[].Name' --output text
for api in $(aws apigateway get-rest-apis --query 'items[].id' --output text 2>>"$ERR"); do
  nom=$(aws apigateway get-rest-apis --query "items[?id=='$api'].name" --output text 2>>"$ERR")
  auth=$(aws apigateway get-authorizers --rest-api-id "$api" --query 'items[].type' --output text 2>>"$ERR")
  for st in $(aws apigateway get-stages --rest-api-id "$api" --query 'item[].stageName' --output text 2>>"$ERR"); do
    d=$(aws apigateway get-stage --rest-api-id "$api" --stage-name "$st" \
        --query '[webAclArn,tracingEnabled,accessLogSettings.destinationArn,methodSettings."*/*".throttlingRateLimit]' \
        --output text 2>>"$ERR")
    set -- $d
    waf="$1"; trz="$2"; lg="$3"; thr="$4"
    est=OK; falt=""
    [ "$waf" = "None" ] && { est=REVISAR; falt="$falt sin-WAF;"; }
    [ -z "$auth" ] || [ "$auth" = "None" ] && { est=REVISAR; falt="$falt sin-authorizer;"; }
    [ "$lg" = "None" ] && { est=REVISAR; falt="$falt sin-access-log;"; }
    [ "$thr" = "None" ] && { est=REVISAR; falt="$falt sin-throttling;"; }
    r "SEG-APIGW-$nom/$st" "$est" "authorizer=${auth:-None} waf=$([ "$waf" = None ] && echo no || echo si) xray=$trz throttling=$thr.$falt"
  done
done
for p in $(aws cognito-idp list-user-pools --max-results 60 --query 'UserPools[].Id' --output text 2>>"$ERR"); do
  nom=$(aws cognito-idp describe-user-pool --user-pool-id "$p" --query 'UserPool.[Name,DeletionProtection]' --output text 2>>"$ERR")
  idp=$(aws cognito-idp list-identity-providers --user-pool-id "$p" --query 'Providers[].ProviderType' --output text 2>>"$ERR")
  mfa=$(aws cognito-idp get-user-pool-mfa-config --user-pool-id "$p" --query MfaConfiguration --output text 2>>"$ERR")
  est=OK; [ -z "$idp" ] && est=REVISAR; [ "$mfa" = "OFF" ] && est=REVISAR
  r "SEG-COGNITO-$(echo "$nom" | awk '{print $1}')" "$est" "idp-federado=${idp:-ninguno} (el diagrama pide Entra ID) mfa=$mfa proteccion-borrado=$(echo "$nom" | awk '{print $2}')"
done

# ------------------------------------------------------------------ 3. Red (hoja Página-1)
seccion "3. Red: VPC, endpoints, NAT, Transit Gateway"
if [ -n "$VPC_CIDR" ]; then
  existe ARQ-VPC "VPC del diagrama" "No existe la VPC con el CIDR indicado" \
    ec2 describe-vpcs --filters "Name=cidr-block-association.cidr-block,Values=$VPC_CIDR" --query 'Vpcs[].VpcId' --output text
else
  existe ARQ-VPC "VPCs" "No hay VPCs" ec2 describe-vpcs --query 'Vpcs[].VpcId' --output text
fi
EP=$(aws ec2 describe-vpc-endpoints --query 'VpcEndpoints[].ServiceName' --output text 2>>"$ERR" | tr '\t' '\n')
for s in s3 dynamodb glue athena secretsmanager kms sts logs states sns; do
  if printf '%s\n' "$EP" | grep -qx "com.amazonaws.$REGION.$s"; then r "COS-ENDPOINT-$s" OK "VPC endpoint presente"
  else r "COS-ENDPOINT-$s" REVISAR "Sin VPC endpoint de $s: el tráfico de jobs y lambdas en subred privada sale por NAT (costo y exposición)"; fi
done
NAT=$(aws ec2 describe-nat-gateways --filter Name=state,Values=available --query 'length(NatGateways)' --output text 2>>"$ERR")
r COS-NAT INFO "NAT Gateways activos: $NAT"
existe ARQ-TGW "Adjuntos a Transit Gateway" \
  "El diagrama conecta con FIS por Transit Gateway; no hay adjuntos en esta cuenta" \
  ec2 describe-transit-gateway-attachments --query 'TransitGatewayAttachments[].[ResourceType,State]' --output text

# ------------------------------------------------------------------ 4. Lambdas
seccion "4. Lambdas: resiliencia (reintentos, destino de fallo, concurrencia, retención, runtime)"
LAMBDAS=$(aws lambda list-functions --query 'Functions[].FunctionName' --output text 2>>"$ERR" | tr '\t' '\n' | grep -Ei 'niif|datalake|sftp')
LT="$OUT/lambdas.tsv"
printf 'funcion\truntime\tmemoria\ttimeout\ttracing\tvpc\tdlq\tdestino_fallo\treintentos\tconc_reservada\tretencion_logs\tterraform\n' > "$LT"
for f in $LAMBDAS; do
  c=$(aws lambda get-function-configuration --function-name "$f" \
      --query '[Runtime,MemorySize,Timeout,TracingConfig.Mode,VpcConfig.VpcId,DeadLetterConfig.TargetArn,FunctionArn]' --output text 2>>"$ERR")
  set -- $c; rt="$1"; mem="$2"; to="$3"; tr="$4"; vpc="$5"; dlq="$6"; arn="$7"
  ev=$(aws lambda get-function-event-invoke-config --function-name "$f" \
       --query '[DestinationConfig.OnFailure.Destination,MaximumRetryAttempts]' --output text 2>/dev/null); set -- $ev
  dest="${1:-None}"; ret="${2:-None}"
  cc=$(aws lambda get-function-concurrency --function-name "$f" --query ReservedConcurrentExecutions --output text 2>>"$ERR")
  rl=$(aws logs describe-log-groups --log-group-name-prefix "/aws/lambda/$f" --query 'logGroups[0].retentionInDays' --output text 2>>"$ERR")
  tf=$(aws lambda list-tags --resource "$arn" --query 'Tags.ManagedBy' --output text 2>>"$ERR")
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$f" "$rt" "$mem" "$to" "$tr" "$vpc" "$dlq" "$dest" "$ret" "${cc:-None}" "$rl" "$tf" >> "$LT"
  falt=""
  [ "$dest" = "None" ] && [ "$dlq" = "None" ] && falt="$falt sin-destino-de-fallo;"
  [ "$rl" = "None" ] && falt="$falt logs-sin-retencion;"
  [ "$tf" = "None" ] && falt="$falt sin-etiqueta-IaC;"
  echo "$rt" | grep -Eq '^python3\.([0-9]|10)$' && falt="$falt runtime-antiguo($rt);"
  [ "$to" -ge 600 ] 2>/dev/null && falt="$falt timeout-alto($to);"
  [ -n "$falt" ] && r "ARQ-LAMBDA-$f" REVISAR "$falt" || r "ARQ-LAMBDA-$f" OK "runtime=$rt timeout=$to"
done
seccion "4b. Disparadores (streams): reintentos y destino de fallo"
aws lambda list-event-source-mappings \
  --query 'EventSourceMappings[].[FunctionArn,EventSourceArn,State,MaximumRetryAttempts,BisectBatchOnFunctionError,DestinationConfig.OnFailure.Destination,MaximumRecordAgeInSeconds]' \
  --output text 2>>"$ERR" | tr '\t' '|' > "$OUT/event-source-mappings.txt"
while IFS='|' read -r fa src st mr bis dest age; do
  [ -z "$fa" ] && continue
  fn=${fa##*:}; falt=""
  [ "$st" != "Enabled" ] && continue
  [ "$mr" = "-1" ] && falt="$falt reintentos-infinitos;"
  [ "$dest" = "None" ] && falt="$falt sin-destino-de-fallo;"
  [ "$bis" = "False" ] && falt="$falt sin-division-de-lote;"
  [ -n "$falt" ] && r "ARQ-STREAM-$fn" REVISAR "$falt un registro defectuoso puede bloquear el shard" || r "ARQ-STREAM-$fn" OK "retry=$mr"
done < "$OUT/event-source-mappings.txt"

# ------------------------------------------------------------------ 5. Step Functions
seccion "5. Step Functions: logging, tiempo límite, captura de errores, IaC"
for arn in $(aws stepfunctions list-state-machines --query 'stateMachines[].stateMachineArn' --output text 2>>"$ERR"); do
  n=${arn##*:}; echo "$n" | grep -qi niif || continue
  d=$(aws stepfunctions describe-state-machine --state-machine-arn "$arn" \
      --query '[loggingConfiguration.level,tracingConfiguration.enabled]' --output text 2>>"$ERR"); set -- $d
  def=$(aws stepfunctions describe-state-machine --state-machine-arn "$arn" --query definition --output text 2>>"$ERR")
  nt=$(printf '%s' "$def" | grep -o 'TimeoutSeconds' | wc -l); nc=$(printf '%s' "$def" | grep -o '"Catch"' | wc -l)
  tf=$(aws stepfunctions list-tags-for-resource --resource-arn "$arn" --query "tags[?key=='ManagedBy'].value" --output text 2>>"$ERR")
  falt=""
  [ "$1" = "OFF" ] && falt="$falt logs-apagados;"
  [ "$nt" = "0" ] && falt="$falt sin-TimeoutSeconds;"
  [ -z "$tf" ] && falt="$falt sin-etiqueta-IaC;"
  [ -n "$falt" ] && r "ARQ-SFN-$n" REVISAR "$falt (Catch=$nc, X-Ray=$2)" || r "ARQ-SFN-$n" OK "Catch=$nc timeouts=$nt"
done

# ------------------------------------------------------------------ 6. Glue
seccion "6. Glue: jobs, crawlers, catálogo, cifrado"
GT="$OUT/glue-jobs.tsv"
printf 'job\tversion\tworker\tn_workers\ttimeout_min\treintentos\tconcurrencia\tsecurity_config\tauto_scaling\n' > "$GT"
for j in $(aws glue list-jobs --query 'JobNames[]' --output text 2>>"$ERR" | tr '\t' '\n' | grep -E "^niif-$AMB-|^data-datalake-niif"); do
  d=$(aws glue get-job --job-name "$j" --query 'Job.[GlueVersion,WorkerType,NumberOfWorkers,Timeout,MaxRetries,ExecutionProperty.MaxConcurrentRuns,SecurityConfiguration,DefaultArguments."--enable-auto-scaling"]' --output text 2>>"$ERR"); set -- $d
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$j" "$1" "$2" "$3" "$4" "$5" "$6" "$7" "$8" >> "$GT"
  falt=""
  [ "$4" -gt 480 ] 2>/dev/null && falt="$falt timeout=$4min;"
  [ "$7" = "None" ] && falt="$falt sin-security-configuration;"
  [ "$8" != "true" ] && falt="$falt sin-auto-scaling;"
  [ "$6" = "1" ] && falt="$falt concurrencia-1(cuello-de-botella-entre-mallas);"
  tf=$(aws glue get-tags --resource-arn "arn:aws:glue:$REGION:$ACC:job/$j" --query 'Tags.ManagedBy' --output text 2>>"$ERR")
  [ "$tf" = "None" ] || [ -z "$tf" ] && falt="$falt sin-etiqueta-IaC;"
  [ -n "$falt" ] && r "ARQ-GLUE-$j" REVISAR "$falt" || r "ARQ-GLUE-$j" OK "workers=$3 timeout=$4"
done
for c in $(aws glue list-crawlers --query 'CrawlerNames[]' --output text 2>>"$ERR" | tr '\t' '\n' | grep -E "^niif-$AMB-"); do
  rc=$(aws glue get-crawler --name "$c" --query 'Crawler.[RecrawlPolicy.RecrawlBehavior,SchemaChangePolicy.DeleteBehavior,Schedule.ScheduleExpression]' --output text 2>>"$ERR"); set -- $rc
  est=OK; [ "$1" = "CRAWL_EVERYTHING" ] && est=REVISAR
  [ "$2" = "DEPRECATE_IN_DATABASE" ] && est=REVISAR
  r "COS-CRAWLER-$c" "$est" "recrawl=$1 borrado-de-tablas=$2 programado=$3"
done
for dbn in bronze silver gold; do
  n=$(aws glue get-tables --database-name "niif-$AMB-$dbn-catalog-db" --query 'length(TableList)' --output text 2>>"$ERR")
  [ -z "$n" ] || [ "$n" = "None" ] && continue
  [ "$n" -gt 50 ] 2>/dev/null && r "ARQ-CATALOGO-$dbn" REVISAR "$n tablas (posible una tabla por lote)" || r "ARQ-CATALOGO-$dbn" OK "$n tablas"
done
cx=$(aws glue get-data-catalog-encryption-settings --query 'DataCatalogEncryptionSettings.EncryptionAtRest.CatalogEncryptionMode' --output text 2>>"$ERR")
r SEG-CATALOGO-CIFRADO "$([ "$cx" = "SSE-KMS" ] && echo OK || echo REVISAR)" "cifrado del catálogo=$cx"

# ------------------------------------------------------------------ 7. Athena y Lake Formation
seccion "7. Athena (costo) y Lake Formation (permisos)"
for w in $(aws athena list-work-groups --query 'WorkGroups[].Name' --output text 2>>"$ERR"); do
  d=$(aws athena get-work-group --work-group "$w" \
      --query 'WorkGroup.Configuration.[BytesScannedCutoffPerQuery,EnforceWorkGroupConfiguration,ResultConfiguration.EncryptionConfiguration.EncryptionOption,EngineVersion.EffectiveEngineVersion]' \
      --output text 2>>"$ERR"); set -- $d
  falt=""
  [ "$1" = "None" ] && falt="$falt sin-tope-de-bytes-por-consulta;"
  [ "$2" != "True" ] && falt="$falt configuracion-no-obligatoria;"
  [ "$3" = "None" ] && falt="$falt resultados-sin-cifrar;"
  [ -n "$falt" ] && r "COS-ATHENA-$w" REVISAR "$falt" || r "COS-ATHENA-$w" OK "tope=$1 cifrado=$3"
done
lf=$(aws lakeformation get-data-lake-settings --query 'length(DataLakeSettings.CreateTableDefaultPermissions)' --output text 2>>"$ERR")
[ "$lf" != "0" ] && [ -n "$lf" ] && r SEG-LF-DEFECTO REVISAR "Los permisos por defecto de tablas nuevas siguen abiertos (IAMAllowedPrincipals): Lake Formation se puede saltar" \
  || r SEG-LF-DEFECTO OK "sin permisos por defecto abiertos"
n=$(aws lakeformation list-permissions --principal DataLakePrincipalIdentifier=IAM_ALLOWED_PRINCIPALS --query 'length(PrincipalResourcePermissions)' --output text 2>>"$ERR")
r SEG-LF-IAMALLOWED "$([ "$n" = "0" ] && echo OK || echo REVISAR)" "concesiones a IAM_ALLOWED_PRINCIPALS: $n"

# ------------------------------------------------------------------ 8. S3
seccion "8. S3: zonas, cifrado, versionado, ciclo de vida, acceso público, TLS"
BT="$OUT/buckets.tsv"
printf 'bucket\tcifrado\tkms\tversionado\tciclo_vida\tbloqueo_publico\ttls_obligatorio\n' > "$BT"
BK=$(aws s3api list-buckets --query 'Buckets[].Name' --output text 2>>"$ERR" | tr '\t' '\n' | grep -E 'niif|alfa-datalake')
printf '%s\n' "$BK" | grep -Eqi 'aterrizaje|landing' && r ARQ-LANDING OK "Hay bucket de aterrizaje" \
  || r ARQ-LANDING FALTA "El diagrama muestra Zona de Aterrizaje (Landing, evidencia 7 años); no hay bucket con ese nombre"
for b in $BK; do
  e=$(aws s3api get-bucket-encryption --bucket "$b" --query 'ServerSideEncryptionConfiguration.Rules[0].ApplyServerSideEncryptionByDefault.[SSEAlgorithm,KMSMasterKeyID]' --output text 2>/dev/null); set -- $e
  alg="${1:-NINGUNO}"; kms="${2:-None}"
  ver=$(aws s3api get-bucket-versioning --bucket "$b" --query Status --output text 2>>"$ERR")
  lc=$(aws s3api get-bucket-lifecycle-configuration --bucket "$b" --query 'length(Rules)' --output text 2>/dev/null)
  pb=$(aws s3api get-public-access-block --bucket "$b" --query 'PublicAccessBlockConfiguration.[BlockPublicAcls,BlockPublicPolicy,IgnorePublicAcls,RestrictPublicBuckets]' --output text 2>/dev/null)
  tls=$(aws s3api get-bucket-policy --bucket "$b" --query Policy --output text 2>/dev/null | grep -c 'aws:SecureTransport')
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$b" "$alg" "$kms" "$ver" "${lc:-0}" "$pb" "$tls" >> "$BT"
  falt=""
  [ "$alg" != "aws:kms" ] && falt="$falt sin-KMS-propio($alg);"
  [ "$ver" != "Enabled" ] && falt="$falt sin-versionado;"
  { [ -z "$lc" ] || [ "$lc" = "0" ] || [ "$lc" = "None" ]; } && falt="$falt sin-ciclo-de-vida;"
  echo "$pb" | grep -q 'False' && falt="$falt bloqueo-publico-incompleto;"
  [ -z "$pb" ] && falt="$falt sin-bloqueo-publico;"
  [ "$tls" = "0" ] && falt="$falt sin-politica-TLS;"
  [ -n "$falt" ] && r "SEG-S3-$b" REVISAR "$falt" || r "SEG-S3-$b" OK "cifrado=$alg versionado=$ver"
done

# ------------------------------------------------------------------ 9. DynamoDB
seccion "9. DynamoDB: respaldo, cifrado, protección, capacidad"
for t in $(aws dynamodb list-tables --query 'TableNames[]' --output text 2>>"$ERR" | tr '\t' '\n' | grep -Ei 'niif|datalake'); do
  d=$(aws dynamodb describe-table --table-name "$t" \
      --query 'Table.[BillingModeSummary.BillingMode,SSEDescription.Status,DeletionProtectionEnabled,StreamSpecification.StreamEnabled]' --output text 2>>"$ERR"); set -- $d
  pitr=$(aws dynamodb describe-continuous-backups --table-name "$t" --query 'ContinuousBackupsDescription.PointInTimeRecoveryDescription.PointInTimeRecoveryStatus' --output text 2>>"$ERR")
  falt=""
  [ "$pitr" != "ENABLED" ] && falt="$falt sin-PITR;"
  [ "$3" != "True" ] && falt="$falt sin-proteccion-de-borrado;"
  [ "$2" = "None" ] && falt="$falt cifrado-por-defecto-AWS(sin-KMS-propio);"
  [ -n "$falt" ] && r "ARQ-DDB-$t" REVISAR "$falt facturacion=${1:-PROVISIONED} stream=$4" || r "ARQ-DDB-$t" OK "PITR activo, facturacion=$1"
done

# ------------------------------------------------------------------ 10. KMS, secretos, auditoría, alertas, costos
seccion "10. KMS, Secrets Manager, CloudTrail, alarmas, logs, presupuesto"
KN=0; for k in $(aws kms list-aliases --query 'Aliases[?!starts_with(AliasName,`alias/aws/`)].TargetKeyId' --output text 2>>"$ERR"); do
  KN=$((KN+1)); rot=$(aws kms get-key-rotation-status --key-id "$k" --query KeyRotationEnabled --output text 2>>"$ERR")
  [ "$rot" != "True" ] && r "SEG-KMS-$k" REVISAR "llave sin rotación automática" || r "SEG-KMS-$k" OK "rotación activa"
done
[ "$KN" = "0" ] && r SEG-KMS FALTA "No hay llaves KMS propias (el estándar pide llaves por dominio con rotación)"
for s in $(aws secretsmanager list-secrets --query 'SecretList[].[Name,RotationEnabled]' --output text 2>>"$ERR" | tr '\t' '|'); do
  r "SEG-SECRETO-${s%%|*}" "$([ "${s##*|}" = "True" ] && echo OK || echo REVISAR)" "rotación=${s##*|} (solo nombre, sin valor)"
done
existe SEG-CLOUDTRAIL "Trails" "No hay CloudTrail (el estándar usa CloudTrail como fuente de acceso)" \
  cloudtrail describe-trails --query 'trailList[].[Name,IsMultiRegionTrail,LogFileValidationEnabled]' --output text
na=$(aws cloudwatch describe-alarms --query 'length(MetricAlarms)' --output text 2>>"$ERR")
r ARQ-ALARMAS "$([ "${na:-0}" = "0" ] && echo FALTA || echo OK)" "alarmas de CloudWatch en la cuenta: ${na:-0} (alarma de fallo de jobs, DLQ, cuadre entrada-salida, bytes de Athena)"
sr=$(aws logs describe-log-groups --query 'logGroups[?retentionInDays==`null`].logGroupName' --output text 2>>"$ERR" | tr '\t' '\n' | grep -Eci 'niif|glue|states|lambda')
r COS-LOGS "$([ "${sr:-0}" = "0" ] && echo OK || echo REVISAR)" "grupos de logs sin retención (crecen sin límite): ${sr:-0}"
nb=$(aws budgets describe-budgets --account-id "$ACC" --query 'length(Budgets)' --output text 2>>"$ERR")
r COS-PRESUPUESTO "$([ "${nb:-0}" = "0" ] && echo FALTA || echo OK)" "presupuestos de AWS Budgets: ${nb:-0}"
existe ARQ-SES "Identidades de SES" "No hay identidades verificadas en SES (T1a)" \
  sesv2 list-email-identities --query 'EmailIdentities[].IdentityName' --output text

# ------------------------------------------------------------------ cierre
seccion "Resumen"
for e in OK REVISAR FALTA SIN-ACCESO; do printf '%-10s %s\n' "$e" "$(awk -F'\t' -v e="$e" '$2==e' "$RES" | wc -l)"; done
echo "Detalle en $RES (y tablas lambdas.tsv, glue-jobs.tsv, buckets.tsv, event-source-mappings.txt)."
echo "SIN-ACCESO = al rol de CloudShell le falta permiso de lectura; anotar cuál y pedirlo."
( cd "$HOME" && zip -qr "niif-arquitectura-$AMB.zip" "niif-arquitectura-$AMB" ) 2>/dev/null \
  && echo "Descargar: Actions > Download file > niif-arquitectura-$AMB.zip"
