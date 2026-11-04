#!/usr/bin/env bash
# Despliega el spike de Primas Rentas en la cuenta DEV como una lambda NUEVA
# (no toca las lambdas existentes ni se conecta a ningun stream).
#
# Uso (en AWS CloudShell, dentro de la carpeta del spike):
#   bash desplegar-cloudshell.sh            # empaqueta, crea rol y crea/actualiza la lambda
#   bash desplegar-cloudshell.sh probar     # invoca en modo validacion (NO escribe nada)
#   bash desplegar-cloudshell.sh eliminar   # borra lambda, rol y log group del spike
#
# Variables que se pueden sobreescribir: FUNCION, ROL, REGION, BUCKET_PARAMS,
# BUCKETS_EXTRA, CNF_TABLE, PRS_TABLE, EJC_TABLE, STATE_MACHINE, MALLA_PK, PERIODO.
set -euo pipefail

FUNCION="${FUNCION:-niif-dev-configuration-general-spike}"
ROL="${ROL:-niif-dev-configuration-general-spike-role}"
REGION="${REGION:-us-east-1}"
CNF_TABLE="${CNF_TABLE:-niif-dev-cnf-mallas}"
PRS_TABLE="${PRS_TABLE:-niif-dev-prs-estructuras}"
EJC_TABLE="${EJC_TABLE:-niif-dev-ejc-ejecuciones}"
STATE_MACHINE="${STATE_MACHINE:-niif-dev-primas-rentas}"
MALLA_PK="${MALLA_PK:-MALLA#PRIMAS_RENTAS}"
# Bucket donde viven los JSON de parametros (inferido de las Step Functions; confirmar).
BUCKET_PARAMS="${BUCKET_PARAMS:-niif-dev-assets}"
BUCKETS_EXTRA="${BUCKETS_EXTRA:-}"   # otros buckets separados por coma, si CNF apunta a ellos
PERIODO="${PERIODO:-}"               # AAAAMM, solo para 'probar'
# Claves KMS (separadas por coma) del bucket de parametros y de las tablas. Verificadas
# para niif-dev-assets y niif-dev-cnf-mallas; agregar las de PRS y EJC si son distintas.
KMS_KEYS="${KMS_KEYS:-arn:aws:kms:us-east-1:533267035849:key/c417247b-8e2c-4d21-9b5b-6b6b5c548bea,arn:aws:kms:us-east-1:533267035849:key/a8eabea1-9561-471d-9cc2-2b96f301d7cb}"

AQUI="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ZIP="/tmp/${FUNCION}.zip"
export AWS_DEFAULT_REGION="$REGION" AWS_PAGER=""

cuenta="$(aws sts get-caller-identity --query Account --output text)"
sm_arn="arn:aws:states:${REGION}:${cuenta}:stateMachine:${STATE_MACHINE}"
rol_arn="arn:aws:iam::${cuenta}:role/${ROL}"

paso() { printf '\n== %s\n' "$*"; }

empaquetar() {
  paso "1. Empaquetar src/ -> ${ZIP}"
  [ -d "${AQUI}/src/configurador" ] || { echo "No encuentro ${AQUI}/src/configurador"; exit 1; }
  rm -f "$ZIP"
  python3 - "$AQUI/src" "$ZIP" <<'PY'
import os, sys, zipfile
src, destino = sys.argv[1:3]
with zipfile.ZipFile(destino, "w", zipfile.ZIP_DEFLATED) as z:
    for raiz, dirs, archivos in os.walk(src):
        dirs[:] = [d for d in dirs if d != "__pycache__"]
        for a in archivos:
            if not a.endswith(".pyc"):
                ruta = os.path.join(raiz, a)
                z.write(ruta, os.path.relpath(ruta, src))
PY
  unzip -l "$ZIP" | tail -n 1
}

crear_rol() {
  paso "2. Rol IAM ${ROL} (minimo privilegio)"
  if aws iam get-role --role-name "$ROL" >/dev/null 2>&1; then
    echo "El rol ya existe: se actualiza su politica."
  else
    aws iam create-role --role-name "$ROL" \
      --description "Spike configurador NIIF 17 (lambda general)" \
      --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}' \
      --output text --query Role.Arn
    aws iam attach-role-policy --role-name "$ROL" \
      --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
  fi

  local recursos_s3="\"arn:aws:s3:::${BUCKET_PARAMS}/*\""
  if [ -n "$BUCKETS_EXTRA" ]; then
    IFS=',' read -ra extra <<< "$BUCKETS_EXTRA"
    for b in "${extra[@]}"; do recursos_s3+=",\"arn:aws:s3:::${b}/*\""; done
  fi
  local recursos_kms=""
  IFS=',' read -ra claves <<< "$KMS_KEYS"
  for k in "${claves[@]}"; do recursos_kms+="${recursos_kms:+,}\"${k}\""; done
  local t="arn:aws:dynamodb:${REGION}:${cuenta}:table"
  aws iam put-role-policy --role-name "$ROL" --policy-name configurador-spike --policy-document "{
    \"Version\":\"2012-10-17\",
    \"Statement\":[
      {\"Sid\":\"CnfLeerYActualizarPeriodo\",\"Effect\":\"Allow\",\"Action\":[\"dynamodb:Query\",\"dynamodb:GetItem\",\"dynamodb:UpdateItem\"],\"Resource\":\"${t}/${CNF_TABLE}\"},
      {\"Sid\":\"PrsSoloLectura\",\"Effect\":\"Allow\",\"Action\":[\"dynamodb:GetItem\",\"dynamodb:Query\"],\"Resource\":\"${t}/${PRS_TABLE}\"},
      {\"Sid\":\"EjcRegistrarEjecucion\",\"Effect\":\"Allow\",\"Action\":\"dynamodb:PutItem\",\"Resource\":\"${t}/${EJC_TABLE}\"},
      {\"Sid\":\"ParametrosS3\",\"Effect\":\"Allow\",\"Action\":[\"s3:GetObject\",\"s3:PutObject\"],\"Resource\":[${recursos_s3}]},
      {\"Sid\":\"ClavesKmsDeS3YDynamo\",\"Effect\":\"Allow\",\"Action\":[\"kms:Decrypt\",\"kms:GenerateDataKey\",\"kms:DescribeKey\"],\"Resource\":[${recursos_kms}]},
      {\"Sid\":\"IniciarSoloEstaStepFunction\",\"Effect\":\"Allow\",\"Action\":\"states:StartExecution\",\"Resource\":\"${sm_arn}\"}
    ]}"
  echo "Esperando propagacion del rol (10 s)..."; sleep 10
}

desplegar() {
  paso "3. Crear o actualizar la lambda ${FUNCION}"
  local entorno="Variables={CNF_TABLE=${CNF_TABLE},PRS_TABLE=${PRS_TABLE},EJC_TABLE=${EJC_TABLE},STATE_MACHINE_ARN=${sm_arn},MALLA_PK=${MALLA_PK}}"
  if aws lambda get-function --function-name "$FUNCION" >/dev/null 2>&1; then
    aws lambda update-function-code --function-name "$FUNCION" --zip-file "fileb://${ZIP}" \
      --query '[FunctionName,LastModified,CodeSize]' --output text
    aws lambda wait function-updated --function-name "$FUNCION"
    aws lambda update-function-configuration --function-name "$FUNCION" \
      --handler configurador.handler.lambda_handler --timeout 300 --memory-size 256 \
      --environment "$entorno" --query '[FunctionName,State]' --output text
  else
    aws lambda create-function --function-name "$FUNCION" --runtime python3.12 \
      --architectures x86_64 --handler configurador.handler.lambda_handler \
      --role "$rol_arn" --zip-file "fileb://${ZIP}" --timeout 300 --memory-size 256 \
      --environment "$entorno" \
      --tags "Proyecto=NIIF17,Ambiente=dev,Proposito=spike-configurador-general,ManagedBy=manual" \
      --query '[FunctionName,State]' --output text
  fi
  aws lambda wait function-updated --function-name "$FUNCION"
  aws logs create-log-group --log-group-name "/aws/lambda/${FUNCION}" 2>/dev/null || true
  aws logs put-retention-policy --log-group-name "/aws/lambda/${FUNCION}" --retention-in-days 14
}

probar() {
  paso "Invocacion en modo validacion (publicar=false: NO escribe en S3/DynamoDB ni inicia la Step Function)"
  local evento='{}'
  [ -n "$PERIODO" ] && evento="{\"periodoNegocio\":\"${PERIODO}\"}"
  aws lambda invoke --function-name "$FUNCION" --cli-binary-format raw-in-base64-out \
    --payload "$evento" --log-type Tail --query 'LogResult' --output text /tmp/salida-spike.json \
    | base64 -d || true
  echo; echo "--- respuesta:"; cat /tmp/salida-spike.json; echo
}

eliminar() {
  paso "Eliminar recursos del spike"
  read -r -p "Se borran ${FUNCION}, ${ROL} y su log group. Escribe SI para continuar: " r
  [ "$r" = "SI" ] || { echo "Cancelado."; exit 0; }
  aws lambda delete-function --function-name "$FUNCION" || true
  aws iam detach-role-policy --role-name "$ROL" \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole || true
  aws iam delete-role-policy --role-name "$ROL" --policy-name configurador-spike || true
  aws iam delete-role --role-name "$ROL" || true
  aws logs delete-log-group --log-group-name "/aws/lambda/${FUNCION}" || true
}

case "${1:-desplegar}" in
  desplegar)
    echo "Cuenta ${cuenta}, region ${REGION}"
    empaquetar; crear_rol; desplegar
    paso "Listo. Siguiente paso: bash desplegar-cloudshell.sh probar"
    ;;
  probar)   probar ;;
  eliminar) eliminar ;;
  *) echo "Uso: bash $0 [desplegar|probar|eliminar]"; exit 1 ;;
esac
