#!/usr/bin/env bash
# ============================================================================
# Prueba CONTROLADA en DEV para replicar el fallo de Primas ARL (SCD de Gold
# apaga todo lo que no viene en una corrida incremental) y para validar a quien
# le llegan los correos de certificacion.
#
# SOLO DEV: aborta si la cuenta no es la de DEV. Se ejecuta en AWS CloudShell
# de la cuenta DEV (us-east-1). Requiere: aws, jq.
#
#   bash replicar-fallo-dev.sh prechequeo
#   bash replicar-fallo-dev.sh destinatarios
#   bash replicar-fallo-dev.sh agregar-email  tu@correo.com [--aprobador]
#   bash replicar-fallo-dev.sh filtro         tu@correo.com
#   bash replicar-fallo-dev.sh aislar         tu@correo.com
#   bash replicar-fallo-dev.sh restaurar
#   bash replicar-fallo-dev.sh respaldar-gold
#   bash replicar-fallo-dev.sh restaurar-gold
#   bash replicar-fallo-dev.sh ejecutar-prueba          (flujo completo, 2 corridas)
#   bash replicar-fallo-dev.sh validar
#
# Variables: MI_EMAIL (obligatoria en ejecutar-prueba), PERIODO (default 202606),
# CORTE_NID_ENDOSO (si no se da, se calcula con Athena sobre Bronce DEV),
# YES=1 (no pedir confirmaciones), FORZAR=1 (permitir periodo con segmento previo).
# ============================================================================
set -u

CUENTA_DEV="${CUENTA_DEV:-533267035849}"
CUENTA_PRD="992382496548"
REGION="${REGION:-us-east-1}"
ENV_N="dev"
PERIODO="${PERIODO:-202606}"
BUCKET="niif-${ENV_N}-assets"
CASO_KEY="primas-arl/primasArl.json"
EXTR_KEY="primas-arl/config_extraccion_primas_arl.json"
PARAMS_KEY="primas-arl/parametros_extraccion.json"
SEG_PREFIX="segmentos/niif17_${PERIODO}_alfaprd_arpprod_par_endosos/"
TOPIC_ARN="${TOPIC_ARN:-arn:aws:sns:${REGION}:${CUENTA_DEV}:niif-${ENV_N}-certification-email}"
NOTIF_TABLE="niif-${ENV_N}-notification-certification"
EJC_TABLE="niif-${ENV_N}-ejc-ejecuciones"
SM_ARN="arn:aws:states:${REGION}:${CUENTA_DEV}:stateMachine:niif-${ENV_N}-primas-arl"
GOLD_DB="niif-${ENV_N}-gold-catalog-db"
BRONZE_DB="niif-${ENV_N}-bronze-catalog-db"
ATHENA_OUT="s3://${BUCKET}/athena/notifications/"
GOLD_BUCKET="alfa-datalake-${ENV_N}-gold"
GOLD_PREFIX="niif/scd_primas_arl/"
W="replica-fallo-dev"
export AWS_DEFAULT_REGION="$REGION" AWS_PAGER=""
mkdir -p "$W/respaldo"

log()  { echo "[$(date +%H:%M:%S)] $*"; }
die()  { echo "ERROR: $*" >&2; exit 1; }
confirmar() { [ "${YES:-0}" = "1" ] && return 0; read -r -p "$1 [s/N] " r; [ "$r" = "s" ] || [ "$r" = "S" ]; }

for c in aws jq; do command -v "$c" >/dev/null 2>&1 || die "Falta el comando $c"; done

verificar_cuenta() {
  local cta; cta="$(aws sts get-caller-identity --query Account --output text 2>/dev/null)" || die "Sin credenciales AWS"
  [ "$cta" = "$CUENTA_PRD" ] && die "Esta sesion es PROD ($cta). Este script es solo para DEV."
  [ "$cta" = "$CUENTA_DEV" ] || die "Cuenta $cta distinta de DEV ($CUENTA_DEV). Abortado."
  log "Cuenta DEV verificada: $cta"
}

s3_get() { aws s3 cp "s3://${BUCKET}/$1" "$2" --only-show-errors && sed -i '1s/^\xEF\xBB\xBF//' "$2"; }
s3_put() { aws s3 cp "$1" "s3://${BUCKET}/$2" --only-show-errors; }

# Respalda una sola vez (local y copia en S3) el archivo original
respaldar_una_vez() { # key nombre_local
  local dest="$W/respaldo/$2"
  if [ ! -f "$dest" ]; then
    s3_get "$1" "$dest" || die "No se pudo leer s3://${BUCKET}/$1"
    s3_put "$dest" "primas-arl/respaldo-prueba/$2.$(date -u +%Y%m%dT%H%M%SZ)"
    log "Respaldo creado: $dest"
  fi
}

athena_escalar() { # base sql -> primer valor
  local qid st
  qid="$(aws athena start-query-execution --query-string "$2" \
         --query-execution-context "Database=$1" --result-configuration "OutputLocation=$ATHENA_OUT" \
         --query QueryExecutionId --output text 2>/dev/null)" || return 1
  for _ in $(seq 1 120); do
    st="$(aws athena get-query-execution --query-execution-id "$qid" --query 'QueryExecution.Status.State' --output text)"
    case "$st" in SUCCEEDED) break;; FAILED|CANCELLED) return 1;; esac; sleep 2
  done
  [ "$st" = "SUCCEEDED" ] || return 1
  aws athena get-query-results --query-execution-id "$qid" --query 'ResultSet.Rows[1].Data[0].VarCharValue' --output text
}

gold_activos()  { athena_escalar "$GOLD_DB" 'SELECT count(*) FROM "scd_primas_arl" WHERE act_reg = 1'; }
gold_activos_p(){ athena_escalar "$GOLD_DB" "SELECT count(*) FROM \"scd_primas_arl\" WHERE act_reg = 1 AND CAST(per_proceso_point AS VARCHAR) = '${PERIODO}'"; }

# ---------------------------------------------------------------- prechequeo
cmd_prechequeo() {
  verificar_cuenta
  log "Estado Gold DEV (activos): $(gold_activos 2>/dev/null || echo 'no consultable')"
  aws s3 ls "s3://${BUCKET}/${SEG_PREFIX}" >/dev/null 2>&1 \
    && log "AVISO: ya existe segmento previo del periodo $PERIODO (la prueba exige un periodo sin segmento, o FORZAR=1)" \
    || log "OK: no hay segmento previo del periodo $PERIODO"
  for k in "$CASO_KEY" "$EXTR_KEY" "$PARAMS_KEY"; do
    aws s3 ls "s3://${BUCKET}/$k" >/dev/null 2>&1 && log "OK s3://${BUCKET}/$k" || log "FALTA s3://${BUCKET}/$k"
  done
  aws stepfunctions describe-state-machine --state-machine-arn "$SM_ARN" --query name --output text >/dev/null 2>&1 \
    && log "OK Step Function niif-${ENV_N}-primas-arl" || log "FALTA/sin acceso a la Step Function"
}

# ------------------------------------------------------------- destinatarios
cmd_destinatarios() {
  verificar_cuenta
  s3_get "$CASO_KEY" "$W/caso.json" || die "No se pudo leer el JSON del caso"
  jq -r '.email.destinations[]? | ascii_downcase | gsub("^\\s+|\\s+$";"")' "$W/caso.json" | sort -u > "$W/dest.txt"
  jq -r '.approvers.users[]?  | ascii_downcase' "$W/caso.json" | sort -u > "$W/aprob.txt"
  aws sns list-subscriptions-by-topic --topic-arn "$TOPIC_ARN" --output json > "$W/subs.json" || die "No se pudo leer el topic SNS"
  jq -r '.Subscriptions[]|select(.Protocol=="email")|select(.SubscriptionArn!="PendingConfirmation")|.Endpoint|ascii_downcase' "$W/subs.json" | sort -u > "$W/conf.txt"
  jq -r '.Subscriptions[]|select(.Protocol=="email")|select(.SubscriptionArn=="PendingConfirmation")|.Endpoint|ascii_downcase' "$W/subs.json" | sort -u > "$W/pend.txt"

  echo "== Destinatarios del caso (email.destinations) ==";           cat "$W/dest.txt"
  echo "== Aprobadores (approvers.users) ==";                         cat "$W/aprob.txt"
  echo "== Llegan HOY (destinatario + suscripcion SNS confirmada) =="; comm -12 "$W/dest.txt" "$W/conf.txt"
  echo "== NO llegan: estan en destinations pero sin suscripcion confirmada =="; comm -23 "$W/dest.txt" "$W/conf.txt"
  echo "== Suscripcion PENDIENTE de confirmar (el dueno debe dar clic en el correo de AWS) =="; cat "$W/pend.txt"
  echo "== Suscritos pero NO reciben (no estan en destinations) ==";  comm -13 "$W/dest.txt" "$W/conf.txt"

  echo "== FilterPolicy de cada suscripcion confirmada (debe ser {\"recipient\":[correo]}) =="
  jq -r '.Subscriptions[]|select(.Protocol=="email")|select(.SubscriptionArn!="PendingConfirmation")|.SubscriptionArn+"|"+.Endpoint' "$W/subs.json" | while IFS='|' read -r arn ep; do
    pol="$(aws sns get-subscription-attributes --subscription-arn "$arn" --query 'Attributes.FilterPolicy' --output text 2>/dev/null)"
    echo "  $ep -> ${pol:-SIN FILTRO (recibiria los mensajes de todos)}"
  done

  hoy="$(TZ=America/Bogota date +%F)"
  echo "== Correos ENVIADOS hoy ($hoy) segun DynamoDB $NOTIF_TABLE =="
  aws dynamodb scan --table-name "$NOTIF_TABLE" \
    --filter-expression "#s = :s AND begins_with(sentDate, :d)" --expression-attribute-names '{"#s":"status"}' \
    --expression-attribute-values "{\":s\":{\"S\":\"SENT\"},\":d\":{\"S\":\"$hoy\"}}" --output json > "$W/enviados-hoy.json" 2>/dev/null \
    && jq -r '.Items[]|[.useCaseId.S,.certCode.S,.sentDate.S,((.emailDestination.L // [])|map(.S)|join(";"))]|@tsv' "$W/enviados-hoy.json" \
    || echo "  (sin acceso o sin registros)"
}

# ------------------------------------------------------------- agregar email
validar_email() { [[ "$1" =~ ^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$ ]] || die "Correo invalido: $1"; }

suscribir_si_falta() {
  if jq -e --arg e "$EMAIL" '.Subscriptions[]?|select(.Protocol=="email" and (.Endpoint|ascii_downcase)==$e)' \
        <(aws sns list-subscriptions-by-topic --topic-arn "$TOPIC_ARN" --output json) >/dev/null 2>&1; then
    log "Ya existe suscripcion SNS para $EMAIL (revisa que este CONFIRMADA con: destinatarios)"
  else
    aws sns subscribe --topic-arn "$TOPIC_ARN" --protocol email --notification-endpoint "$EMAIL" >/dev/null \
      && log "Suscripcion creada. AWS envia un correo a $EMAIL: DAR CLIC EN 'Confirm subscription'."
  fi
}

cmd_agregar_email() {
  verificar_cuenta; EMAIL="$(echo "${1:-}" | tr 'A-Z' 'a-z')"; validar_email "$EMAIL"
  respaldar_una_vez "$CASO_KEY" "primasArl.json.orig"
  s3_get "$CASO_KEY" "$W/caso.json" || die "No se pudo leer el JSON del caso"
  local filtro='.email.destinations = ((.email.destinations // []) | if index($e) then . else . + [$e] end)'
  [ "${2:-}" = "--aprobador" ] && filtro="$filtro | .approvers.users = ((.approvers.users // []) | if index(\$e) then . else . + [\$e] end)"
  jq --arg e "$EMAIL" "$filtro" "$W/caso.json" > "$W/caso.nuevo.json" || die "jq fallo"
  jq -e . "$W/caso.nuevo.json" >/dev/null || die "JSON invalido"
  diff <(jq -S . "$W/caso.json") <(jq -S . "$W/caso.nuevo.json") || true
  confirmar "Subir el JSON modificado a s3://${BUCKET}/${CASO_KEY}?" || die "Cancelado"
  s3_put "$W/caso.nuevo.json" "$CASO_KEY"; log "JSON del caso actualizado (el original queda en $W/respaldo/)"
  suscribir_si_falta
  echo "Siguiente: confirma la suscripcion desde tu bandeja y luego ejecuta:  bash $0 filtro $EMAIL"
}

cmd_filtro() {
  verificar_cuenta; EMAIL="$(echo "${1:-}" | tr 'A-Z' 'a-z')"; validar_email "$EMAIL"
  arn="$(aws sns list-subscriptions-by-topic --topic-arn "$TOPIC_ARN" --output json \
        | jq -r --arg e "$EMAIL" '.Subscriptions[]|select(.Protocol=="email" and (.Endpoint|ascii_downcase)==$e)|.SubscriptionArn' | head -1)"
  [ -n "$arn" ] || die "No hay suscripcion para $EMAIL (usa agregar-email)"
  [ "$arn" != "PendingConfirmation" ] || die "La suscripcion de $EMAIL sigue PENDIENTE de confirmar"
  aws sns set-subscription-attributes --subscription-arn "$arn" --attribute-name FilterPolicy \
      --attribute-value "{\"recipient\":[\"$EMAIL\"]}" && log "FilterPolicy configurado para $EMAIL"
}

# --------------------------------------------------------- aislar / restaurar
cmd_aislar() {  # deja el caso enviando SOLO al correo indicado (para no escribir a otras personas)
  verificar_cuenta; EMAIL="$(echo "${1:-}" | tr 'A-Z' 'a-z')"; validar_email "$EMAIL"
  respaldar_una_vez "$CASO_KEY" "primasArl.json.orig"
  s3_get "$CASO_KEY" "$W/caso.json" || die "No se pudo leer el JSON del caso"
  jq --arg e "$EMAIL" '.email.destinations = [$e] | .approvers.users = [$e]' "$W/caso.json" > "$W/caso.aislado.json" || die "jq fallo"
  s3_put "$W/caso.aislado.json" "$CASO_KEY"; log "Caso AISLADO: destinations y approvers = [$EMAIL] (restaurar con: bash $0 restaurar)"
}

cmd_restaurar() {
  verificar_cuenta
  [ -f "$W/respaldo/primasArl.json.orig" ] && s3_put "$W/respaldo/primasArl.json.orig" "$CASO_KEY" && log "Restaurado $CASO_KEY"
  [ -f "$W/respaldo/config_extraccion.json.orig" ] && s3_put "$W/respaldo/config_extraccion.json.orig" "$EXTR_KEY" && log "Restaurado $EXTR_KEY"
  [ -f "$W/respaldo/primasArl.json.orig" ] || [ -f "$W/respaldo/config_extraccion.json.orig" ] || log "No hay respaldos locales en $W/respaldo/"
}

cmd_respaldar_gold() {
  verificar_cuenta; local dst="s3://${GOLD_BUCKET}/niif/_respaldo_prueba/scd_primas_arl/$(date -u +%Y%m%dT%H%M%SZ)/"
  confirmar "Copiar Gold DEV a $dst ?" || die "Cancelado"
  aws s3 sync "s3://${GOLD_BUCKET}/${GOLD_PREFIX}" "$dst" --only-show-errors && echo "$dst" > "$W/respaldo/gold.destino" && log "Respaldo de Gold: $dst"
}
cmd_restaurar_gold() {
  verificar_cuenta; local src; src="$(cat "$W/respaldo/gold.destino" 2>/dev/null)" || die "No hay respaldo de Gold registrado"
  confirmar "Restaurar Gold DEV desde $src (borra lo que no este en el respaldo)?" || die "Cancelado"
  aws s3 sync "$src" "s3://${GOLD_BUCKET}/${GOLD_PREFIX}" --delete --only-show-errors && log "Gold restaurado desde $src"
}

# ------------------------------------------------------------- Step Function
iniciar_ejecucion() { # etiqueta -> imprime executionArn
  local nombre="arl-${PERIODO}-prueba-$1-$(date -u +%Y%m%dT%H%M%SZ)"
  local pk="EJC#PRIMAS_ARL#${PERIODO}" sk="EJECUCION#${nombre}"
  aws dynamodb put-item --table-name "$EJC_TABLE" --item "$(jq -nc --arg pk "$pk" --arg sk "$sk" --arg n "$nombre" --arg p "$PERIODO" \
    '{PK:{S:$pk},SK:{S:$sk},malla:{S:"PRIMAS_ARL"},periodoNegocio:{S:$p},executionName:{S:$n},estado:{S:"INICIADA"},origen:{S:"PRUEBA_DEV"}}')" >/dev/null || die "No se pudo crear el registro EJC"
  aws stepfunctions start-execution --state-machine-arn "$SM_ARN" --name "$nombre" \
    --input "$(jq -nc --arg pk "$pk" --arg sk "$sk" --arg p "$PERIODO" --arg r "s3://${BUCKET}/${EXTR_KEY}" \
      '{ejcPK:$pk,ejcSK:$sk,malla:"PRIMAS_ARL",periodoNegocio:$p,modoEjecucion:"NORMAL",rutaConfigExtraccion:$r,origen:"PRUEBA_DEV"}')" \
    --query executionArn --output text
}

esperar_ejecucion() { # arn
  local st
  for _ in $(seq 1 180); do
    st="$(aws stepfunctions describe-execution --execution-arn "$1" --query status --output text)"
    [ "$st" != "RUNNING" ] && { echo "$st"; return; }; sleep 30
  done; echo "TIMEOUT"
}

correr() { # etiqueta
  log "Iniciando ejecucion '$1' del periodo $PERIODO"
  local arn st; arn="$(iniciar_ejecucion "$1")" || die "No se pudo iniciar la Step Function"
  log "ARN: $arn  (esperando, puede tardar ~25 min)"
  st="$(esperar_ejecucion "$arn")"; log "Resultado '$1': $st"
  [ "$st" = "SUCCEEDED" ] || die "La ejecucion '$1' termino en $st; revisar en la consola de Step Functions"
  s3_get "$PARAMS_KEY" "$W/params.$1.json" || die "No se pudo leer parametros_extraccion.json"
}

# ----------------------------------------------------------- prueba completa
cmd_ejecutar_prueba() {
  verificar_cuenta
  [ -n "${MI_EMAIL:-}" ] || die "Define MI_EMAIL=tu@correo.com (los correos de la prueba llegan solo a esa direccion)"
  EMAIL="$(echo "$MI_EMAIL" | tr 'A-Z' 'a-z')"; validar_email "$EMAIL"
  if aws s3 ls "s3://${BUCKET}/${SEG_PREFIX}" >/dev/null 2>&1 && [ "${FORZAR:-0}" != "1" ]; then
    die "El periodo $PERIODO ya tiene segmento previo en DEV: la primera corrida ya excluiria llaves. Usa otro PERIODO o FORZAR=1."
  fi
  respaldar_una_vez "$CASO_KEY" "primasArl.json.orig"
  respaldar_una_vez "$EXTR_KEY" "config_extraccion.json.orig"
  trap 'log "Restaurando archivos originales..."; cmd_restaurar' EXIT

  local corte="${CORTE_NID_ENDOSO:-}"
  if [ -z "$corte" ]; then
    corte="$(athena_escalar "$BRONZE_DB" "SELECT CAST(approx_percentile(nid_endoso, 0.98) AS bigint) FROM \"alfaprd_arpprod_par_endosos\" WHERE periodo <= '${PERIODO}'" 2>/dev/null)"
  fi
  [[ "$corte" =~ ^[0-9]+$ ]] || die "No se pudo calcular el corte de NID_ENDOSO. Define CORTE_NID_ENDOSO=<numero>"
  log "Corte NID_ENDOSO = $corte  (corrida 1: endosos <= corte; corrida 2: todos; los 'nuevos' son los > corte)"

  log "Resumen de lo que se hara:"
  echo " - Caso AISLADO: los correos de la prueba llegan solo a $EMAIL"
  echo " - Se modifica TEMPORALMENTE s3://${BUCKET}/${EXTR_KEY} (WHERE de par_endosos) y se restaura al terminar"
  echo " - Se ejecuta la Step Function niif-${ENV_N}-primas-arl dos veces para el periodo $PERIODO"
  echo " - Se MODIFICA Gold DEV (scd_primas_arl). Haz antes: bash $0 respaldar-gold"
  confirmar "Continuar?" || die "Cancelado"

  cmd_aislar "$EMAIL"; suscribir_si_falta

  # Corrida 1 (base): config con WHERE limitado por el corte
  jq --argjson c "$corte" 'walk(if type=="object" and .tabla_relativa=="arpprod/par_endosos" and has("where")
        then .where = (.where + " AND NID_ENDOSO <= " + ($c|tostring)) else . end)' \
     "$W/respaldo/config_extraccion.json.orig" > "$W/config_extraccion.base.json" || die "jq fallo al armar config base"
  [ "$(jq -r '[..|objects|select(.tabla_relativa=="arpprod/par_endosos")|.where]|first' "$W/config_extraccion.base.json")" != \
    "$(jq -r '[..|objects|select(.tabla_relativa=="arpprod/par_endosos")|.where]|first' "$W/respaldo/config_extraccion.json.orig")" ] \
    || die "No se pudo modificar el WHERE de par_endosos en la config"
  s3_put "$W/config_extraccion.base.json" "$EXTR_KEY"
  correr base
  ACT_BASE="$(gold_activos)"; log "Gold activos tras la corrida base: $ACT_BASE"

  # Corrida 2 (incremental): config original, aparecen como nuevos los endosos > corte
  s3_put "$W/respaldo/config_extraccion.json.orig" "$EXTR_KEY"
  correr incremental
  ACT_FIN="$(gold_activos)"; ACT_FIN_P="$(gold_activos_p)"
  log "Gold activos tras la corrida incremental: $ACT_FIN (del periodo $PERIODO: $ACT_FIN_P)"
  jq -n --arg a "$ACT_BASE" --arg f "$ACT_FIN" --arg p "$ACT_FIN_P" '{activos_base:($a|tonumber),activos_final:($f|tonumber),activos_periodo_final:($p|tonumber)}' > "$W/gold.json"

  cmd_validar
  echo
  echo "== Correo =="
  echo "Revisa la bandeja de $EMAIL: asunto 'NIIF 17 - Certificacion Primas ARL Periodo $PERIODO'."
  echo "Si el fallo se replico, 'Totales' solo contara los registros nuevos (o dira 'No hay datos') y 'Resumen' dira lo que tenga configurado."
  echo "Quien lo recibio: bash $0 destinatarios"
}

# ------------------------------------------------------------------ validar
cmd_validar() {
  local P="$W/params.incremental.json"
  [ -f "$P" ] || { s3_get "$PARAMS_KEY" "$P" || die "Sin parametros"; }
  local ent ins sc cer apag exc
  ent="$(jq -r '.metricas_gold_scd.total_entrada_silver_preparada' "$P")"
  ins="$(jq -r '.metricas_gold_scd.total_insertados_nuevos' "$P")"
  sc="$(jq -r '.metricas_gold_scd.total_registros_sin_cambio' "$P")"
  cer="$(jq -r '.metricas_gold_scd.total_versiones_cerradas_por_cambio' "$P")"
  apag="$(jq -r '.metricas_gold_scd.total_registros_apagados_por_ausencia' "$P")"
  exc="$(jq -r '[.datasets_extraidos[]|select(.tabla_relativa=="arpprod/par_endosos")|.segmentacion_horizontal.registros_excluidos_por_segmento]|first' "$P")"
  local base act_base act_fin
  act_base="$(jq -r '.activos_base // empty' "$W/gold.json" 2>/dev/null)"; act_fin="$(jq -r '.activos_final // empty' "$W/gold.json" 2>/dev/null)"
  local fallos=0
  chk() { if [ "$2" = "1" ]; then echo "PASS $1"; else echo "FAIL $1"; fallos=$((fallos+1)); fi; echo "     $3"; }
  chk "Gold SCD termino en SUCCESS" "$([ "$(jq -r .estado_gold_scd "$P")" = "SUCCESS" ] && echo 1 || echo 0)" "estado_gold_scd=$(jq -r .estado_gold_scd "$P")"
  chk "Extraccion incremental: se excluyeron llaves ya extraidas" "$([ "${exc:-0}" -gt 0 ] 2>/dev/null && echo 1 || echo 0)" "registros_excluidos_por_segmento=$exc"
  chk "Apagados por ausencia == excluidos por segmento" "$([ "$apag" = "$exc" ] && echo 1 || echo 0)" "apagados=$apag excluidos=$exc"
  chk "Ninguna fila previa coincide con la entrada (sin cambio=0, cerradas=0)" "$([ "$sc" = "0" ] && [ "$cer" = "0" ] && echo 1 || echo 0)" "sin_cambio=$sc cerradas=$cer"
  chk "Entrada del SCD == insertados" "$([ "$ent" = "$ins" ] && echo 1 || echo 0)" "entrada=$ent insertados=$ins"
  if [ -n "$act_base" ] && [ -n "$act_fin" ]; then
    chk "Todo lo activo tras la corrida base fue apagado (apagados == activos_base)" "$([ "$apag" = "$act_base" ] && echo 1 || echo 0)" "apagados=$apag activos_base=$act_base"
    chk "Tras la corrida incremental solo quedan activos los registros nuevos" "$([ "$act_fin" = "$ent" ] && echo 1 || echo 0)" "activos_final=$act_fin entrada=$ent"
  fi
  echo "-----"
  [ "$fallos" -eq 0 ] && echo "FALLO REPLICADO: el SCD apaga todo lo que no viene en una corrida incremental." \
                      || echo "No se replico por completo ($fallos verificaciones FAIL): revisar los valores."
  return "$fallos"
}

uso() { sed -n '3,22p' "$0"; }

case "${1:-}" in
  prechequeo)       cmd_prechequeo ;;
  destinatarios)    cmd_destinatarios ;;
  agregar-email)    shift; cmd_agregar_email "$@" ;;
  filtro)           shift; cmd_filtro "$@" ;;
  aislar)           shift; cmd_aislar "$@" ;;
  restaurar)        cmd_restaurar ;;
  respaldar-gold)   cmd_respaldar_gold ;;
  restaurar-gold)   cmd_restaurar_gold ;;
  ejecutar-prueba)  cmd_ejecutar_prueba ;;
  validar)          cmd_validar ;;
  *)                uso ;;
esac
