#!/usr/bin/env bash
set -uo pipefail
export AWS_PAGER=""

AMB="${AMB:-dev}"
WG="${WG:-primary}"
OUT="${OUT:-}"
SCRIPT_LOCAL="${SCRIPT_LOCAL:-./silver-primas-rentas.py}"
JOB_SILVER="niif-${AMB}-silver-primas-rentas"
JOB_SCD="${JOB_SCD:-niif-${AMB}-scd}"
DB="niif-${AMB}-gold-catalog-db"
TABLA="scd_primas_rentas"
BUCKET_ASSETS="niif-${AMB}-assets"
BUCKET_GOLD="alfa-datalake-${AMB}-gold"
RUTA_PARAMETROS="s3://${BUCKET_ASSETS}/primas-rentas/parametros_extraccion.json"
RUTA_CONFIG_SCD="s3://${BUCKET_ASSETS}/primas-rentas/config_scd_primas_rentas.json"
RUTA_SILVER="s3://alfa-datalake-${AMB}-silver/niif/t_data_domain/primas_rentas/silver_preparado_primas_rentas/"
RUTA_GOLD="s3://${BUCKET_GOLD}/niif/${TABLA}/"
RUTA_CONTROL="s3://${BUCKET_GOLD}/niif/_control_scd/${TABLA}/periodo_activo.json"
RUTA_RESPALDOS="s3://${BUCKET_ASSETS}/primas-rentas/respaldos_fix_rentas"
MARCA_FIX='col("r.FEC_EXPEDICION").between('

if [ "$AMB" != "dev" ]; then
  echo "Este script es solo para DEV. QA y PRD los despliega DevOps."
  exit 1
fi

consultar() {
  local sql="$1" id estado intento=0
  local args=(--work-group "$WG" --query-string "$sql" --query-execution-context "Database=$DB")
  [ -n "$OUT" ] && args+=(--result-configuration "OutputLocation=$OUT")
  id=$(aws athena start-query-execution "${args[@]}" --query QueryExecutionId --output text 2>/dev/null) || { echo "ERROR_INICIO"; return; }
  while [ $intento -lt 90 ]; do
    estado=$(aws athena get-query-execution --query-execution-id "$id" --query QueryExecution.Status.State --output text)
    case "$estado" in
      SUCCEEDED) aws athena get-query-results --query-execution-id "$id" --output text \
                   --query 'ResultSet.Rows[].Data[].VarCharValue' | tr '\t' ' '; return ;;
      FAILED|CANCELLED) echo "ERROR_${estado}"; return ;;
    esac
    intento=$((intento + 1)); sleep 2
  done
  echo "ERROR_TIEMPO"
}

ruta_script() {
  aws glue get-job --job-name "$JOB_SILVER" --query Job.Command.ScriptLocation --output text
}

esperar_job() {
  local job="$1" id="$2" estado
  while true; do
    estado=$(aws glue get-job-run --job-name "$job" --run-id "$id" --query JobRun.JobRunState --output text)
    case "$estado" in
      SUCCEEDED|FAILED|STOPPED|TIMEOUT|ERROR) break ;;
    esac
    sleep 20
  done
  echo "Estado: $estado"
  [ "$estado" != "SUCCEEDED" ] && aws glue get-job-run --job-name "$job" --run-id "$id" --query JobRun.ErrorMessage --output text
}

script() {
  local ruta
  ruta=$(ruta_script)
  echo "== Cuenta $(aws sts get-caller-identity --query Account --output text) | job $JOB_SILVER"
  echo "ScriptLocation: $ruta"
  if aws s3 cp "$ruta" - 2>/dev/null | grep -qF "$MARCA_FIX"; then
    echo "El script desplegado TRAE el fix."
  else
    echo "El script desplegado NO trae el fix."
  fi
}

foto() {
  echo "== parametros_extraccion.json"
  aws s3 cp "$RUTA_PARAMETROS" - 2>/dev/null | python3 -c '
import json, sys
p = json.load(sys.stdin)
for k in ("periodo_negocio", "lote_cargue_id", "estado_validacion", "estado_remediacion",
          "estado_disponibilizacion_silver", "estado_preparacion_silver",
          "total_registros_silver_preparada", "fecha_preparacion_silver",
          "lotes_usados_preparacion_silver", "estado_gold_scd", "motivo_gold_scd",
          "metricas_gold_scd", "control_periodo_scd"):
    print(f"{k}: {json.dumps(p.get(k), ensure_ascii=False)}")' || echo "no existe"
  echo "== Archivo de control del SCD"
  aws s3 cp "$RUTA_CONTROL" - 2>/dev/null || echo "no existe"
  echo
  echo "== Gold activos por periodo de pago (per_pago_renta activos expedidas_en_el_periodo)"
  consultar "SELECT per_pago_renta, COUNT(*), SUM(CASE WHEN per_expedicion_renta = per_pago_renta THEN 1 ELSE 0 END) FROM \"$DB\".\"$TABLA\" WHERE act_reg = 1 GROUP BY per_pago_renta ORDER BY per_pago_renta"
  echo "== Gold llaves duplicadas activas (debe ser 0)"
  consultar "SELECT COUNT(*) FROM (SELECT nid_renta FROM \"$DB\".\"$TABLA\" WHERE act_reg = 1 GROUP BY nid_renta HAVING COUNT(*) > 1)"
}

respaldo() {
  local destino="${RUTA_RESPALDOS}/$(date -u +%Y%m%dT%H%M%SZ)"
  aws s3 cp "$(ruta_script)" "${destino}/silver-primas-rentas.py" --only-show-errors || return 1
  aws s3 cp "$RUTA_PARAMETROS" "${destino}/parametros_extraccion.json" --only-show-errors || return 1
  aws s3 sync "$RUTA_SILVER" "${destino}/silver_preparado/" --only-show-errors || return 1
  aws s3 sync "$RUTA_GOLD" "${destino}/gold/" --only-show-errors || return 1
  aws s3 cp "$RUTA_CONTROL" "${destino}/periodo_activo.json" --only-show-errors 2>/dev/null || echo "Sin archivo de control que respaldar"
  echo "Respaldo en ${destino}/"
}

subir() {
  local ruta
  [ -f "$SCRIPT_LOCAL" ] || { echo "No existe $SCRIPT_LOCAL. Suba el archivo a CloudShell (Actions > Upload file)."; return 1; }
  grep -qF "$MARCA_FIX" "$SCRIPT_LOCAL" || { echo "$SCRIPT_LOCAL no trae el fix."; return 1; }
  ruta=$(ruta_script)
  aws s3 cp "$SCRIPT_LOCAL" "$ruta" --only-show-errors && echo "Script subido a $ruta"
  script
}

silver() {
  local id
  id=$(aws glue start-job-run --job-name "$JOB_SILVER" \
    --arguments "{\"--RUTA_PARAMETROS_EXTRACCION\":\"$RUTA_PARAMETROS\",\"--S3_SLV_PREP_PATH\":\"$RUTA_SILVER\"}" \
    --query JobRunId --output text) || return 1
  echo "Corrida $id de $JOB_SILVER"
  esperar_job "$JOB_SILVER" "$id"
  aws s3 cp "$RUTA_PARAMETROS" - | python3 -c '
import json, sys
p = json.load(sys.stdin)
print("periodo_negocio:", p.get("periodo_negocio"))
print("estado_preparacion_silver:", p.get("estado_preparacion_silver"))
print("total_registros_silver_preparada:", p.get("total_registros_silver_preparada"))'
}

scd() {
  local id args
  args="{\"--RUTA_CONFIG_SCD\":\"$RUTA_CONFIG_SCD\",\"--RUTA_PARAMETROS_EXTRACCION\":\"$RUTA_PARAMETROS\""
  [ "${PERMITIR:-false}" = "true" ] && args="${args},\"--PERMITIR_APAGADO_MASIVO\":\"true\""
  args="${args}}"
  id=$(aws glue start-job-run --job-name "$JOB_SCD" --arguments "$args" --query JobRunId --output text) || return 1
  echo "Corrida $id de $JOB_SCD (PERMITIR_APAGADO_MASIVO=${PERMITIR:-false})"
  esperar_job "$JOB_SCD" "$id"
  aws s3 cp "$RUTA_PARAMETROS" - | python3 -c '
import json, sys
p = json.load(sys.stdin)
for k in ("estado_gold_scd", "motivo_gold_scd", "metricas_gold_scd", "control_periodo_scd"):
    print(f"{k}: {json.dumps(p.get(k), ensure_ascii=False)}")'
}

crawler() {
  local nombre="niif-${AMB}-crawler-gold"
  while [ "$(aws glue get-crawler --name "$nombre" --query Crawler.State --output text)" != "READY" ]; do sleep 15; done
  aws glue start-crawler --name "$nombre" && echo "Crawler $nombre iniciado"
  sleep 15
  while [ "$(aws glue get-crawler --name "$nombre" --query Crawler.State --output text)" != "READY" ]; do sleep 15; done
  echo "Crawler $nombre terminado"
}

restaurar() {
  local origen="${1:?Uso: restaurar ${RUTA_RESPALDOS}/<fecha>}"
  origen="${origen%/}"
  aws s3 cp "${origen}/silver-primas-rentas.py" "$(ruta_script)" --only-show-errors && echo "Script restaurado"
  aws s3 cp "${origen}/parametros_extraccion.json" "$RUTA_PARAMETROS" --only-show-errors && echo "parametros_extraccion.json restaurado"
  aws s3 sync "${origen}/silver_preparado/" "$RUTA_SILVER" --delete --only-show-errors && echo "Silver preparada restaurada"
  aws s3 sync "${origen}/gold/" "$RUTA_GOLD" --delete --only-show-errors && echo "Gold restaurado"
  if aws s3 ls "${origen}/periodo_activo.json" >/dev/null 2>&1; then
    aws s3 cp "${origen}/periodo_activo.json" "$RUTA_CONTROL" --only-show-errors && echo "Control restaurado"
  else
    aws s3 rm "$RUTA_CONTROL" --only-show-errors 2>/dev/null; echo "Control sin respaldo: borrado"
  fi
}

todo() {
  script
  foto
  respaldo || { echo "Falló el respaldo; no se sigue."; return 1; }
  subir || return 1
  silver
  echo
  echo "Revise el total de Silver. Para llevarlo a Gold:"
  echo "  bash $0 scd                (si frena por el apagado de las rentas que sobran:)"
  echo "  PERMITIR=true bash $0 scd"
  echo "  bash $0 crawler && bash $0 foto"
}

case "${1:-}" in
  script) script ;;
  foto) foto ;;
  respaldo) respaldo ;;
  subir) subir ;;
  silver) silver ;;
  scd) scd ;;
  crawler) crawler ;;
  restaurar) shift; restaurar "$@" ;;
  todo) todo ;;
  *) echo "Uso: [SCRIPT_LOCAL=./silver-primas-rentas.py] [PERMITIR=true] bash $0 {todo|script|foto|respaldo|subir|silver|scd|crawler|restaurar <ruta>}" ;;
esac
