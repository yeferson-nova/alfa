#!/usr/bin/env bash
set -uo pipefail

AMB="${AMB:-dev}"
WG="${WG:-primary}"
OUT="${OUT:-}"
MALLA="${MALLA:-primas-arl}"
TABLA="${TABLA:-scd_primas_arl}"
COLS_PERIODO="${COLS_PERIODO:-per_proceso_point,per_expedicion}"
JOB_SCD="${JOB_SCD:-}"
DB="niif-${AMB}-gold-catalog-db"
BUCKET_GOLD="alfa-datalake-${AMB}-gold"
BUCKET_ASSETS="niif-${AMB}-assets"
RUTA_GOLD="s3://${BUCKET_GOLD}/niif/${TABLA}/"
RUTA_CONTROL="s3://${BUCKET_GOLD}/niif/_control_scd/${TABLA}/periodo_activo.json"
RUTA_CONFIG="${RUTA_CONFIG:-s3://${BUCKET_ASSETS}/${MALLA}/config_scd_${TABLA#scd_}.json}"
RUTA_PARAMETROS="${RUTA_PARAMETROS:-s3://${BUCKET_ASSETS}/${MALLA}/parametros_extraccion.json}"
RUTA_RESPALDO_BASE="s3://${BUCKET_GOLD}/respaldos_scd/${TABLA}"

consultar() {
  local sql="$1" id estado intento=0
  local args=(--work-group "$WG" --query-string "$sql" --query-execution-context "Database=$DB")
  [ -n "$OUT" ] && args+=(--result-configuration "OutputLocation=$OUT")
  id=$(aws athena start-query-execution "${args[@]}" --query QueryExecutionId --output text 2>/dev/null) || { echo "ERROR_INICIO"; return; }
  while [ $intento -lt 90 ]; do
    estado=$(aws athena get-query-execution --query-execution-id "$id" --query QueryExecution.Status.State --output text)
    case "$estado" in
      SUCCEEDED) aws athena get-query-results --query-execution-id "$id" --output text \
                   --query 'ResultSet.Rows[].Data[].VarCharValue' | tr '\t' ' ' ; return ;;
      FAILED|CANCELLED) echo "ERROR_${estado}"; return ;;
    esac
    intento=$((intento + 1)); sleep 2
  done
  echo "ERROR_TIEMPO"
}

foto() {
  echo "== Cuenta $(aws sts get-caller-identity --query Account --output text) | $DB.$TABLA"
  echo "== Archivo de control ($RUTA_CONTROL)"
  aws s3 cp "$RUTA_CONTROL" - 2>/dev/null || echo "no existe"
  echo
  echo "== parametros_extraccion.json (campos del SCD)"
  aws s3 cp "$RUTA_PARAMETROS" - 2>/dev/null | python3 -c '
import json, sys
p = json.load(sys.stdin)
for k in ("periodo_negocio", "lote_cargue_id", "estado_extraccion", "estado_gold_scd", "motivo_gold_scd", "metricas_gold_scd", "control_periodo_scd", "ruta_silver_preparada"):
    print(f"{k}: {json.dumps(p.get(k), ensure_ascii=False)}")
def buscar(nodo, ruta=""):
    if isinstance(nodo, dict):
        for k, v in nodo.items():
            if k == "lotes_segmento":
                print(f"lotes_segmento {ruta}: {v}")
            else:
                buscar(v, f"{ruta}/{k}")
    elif isinstance(nodo, list):
        for i, v in enumerate(nodo):
            buscar(v, f"{ruta}[{i}]")
buscar(p)'
  echo "== Total Gold activos"
  consultar "SELECT COUNT(*) FROM \"$DB\".\"$TABLA\" WHERE act_reg = 1"
  for c in ${COLS_PERIODO//,/ }; do
    echo "== Gold por $c y act_reg (columnas: periodo act_reg vrs_max filas)"
    consultar "SELECT $c, act_reg, MAX(vrs_reg), COUNT(*) FROM \"$DB\".\"$TABLA\" GROUP BY $c, act_reg ORDER BY $c, act_reg"
  done
  echo "== Gold por vrs_reg y act_reg"
  consultar "SELECT vrs_reg, act_reg, COUNT(*) FROM \"$DB\".\"$TABLA\" GROUP BY vrs_reg, act_reg ORDER BY vrs_reg, act_reg"
}

respaldo() {
  local destino="${RUTA_RESPALDO_BASE}/$(date -u +%Y%m%dT%H%M%SZ)/"
  aws s3 sync "$RUTA_GOLD" "$destino" --only-show-errors && echo "Respaldo en $destino"
  aws s3 cp "$RUTA_PARAMETROS" "${destino%/}_parametros_extraccion.json" --only-show-errors
}

restaurar() {
  local origen="${1:?Uso: restaurar s3://.../respaldos_scd/<tabla>/<fecha>/}"
  aws s3 sync "$origen" "$RUTA_GOLD" --delete --only-show-errors && echo "Gold restaurado desde $origen"
  aws s3 rm "$RUTA_CONTROL" --only-show-errors && echo "Control borrado"
}

scd() {
  if [ -z "$JOB_SCD" ]; then
    echo "Defina JOB_SCD. Jobs con 'scd' en el nombre:"
    aws glue list-jobs --query 'JobNames' --output text | tr '\t' '\n' | grep -i scd
    return 1
  fi
  local extra=()
  [ "${PERMITIR:-false}" = "true" ] && extra=(--arguments "{\"--RUTA_CONFIG_SCD\":\"$RUTA_CONFIG\",\"--RUTA_PARAMETROS_EXTRACCION\":\"$RUTA_PARAMETROS\",\"--PERMITIR_APAGADO_MASIVO\":\"true\"}")
  [ ${#extra[@]} -eq 0 ] && extra=(--arguments "{\"--RUTA_CONFIG_SCD\":\"$RUTA_CONFIG\",\"--RUTA_PARAMETROS_EXTRACCION\":\"$RUTA_PARAMETROS\"}")
  local id estado
  id=$(aws glue start-job-run --job-name "$JOB_SCD" "${extra[@]}" --query JobRunId --output text) || return 1
  echo "Corrida $id de $JOB_SCD (PERMITIR_APAGADO_MASIVO=${PERMITIR:-false})"
  while true; do
    estado=$(aws glue get-job-run --job-name "$JOB_SCD" --run-id "$id" --query JobRun.JobRunState --output text)
    case "$estado" in
      SUCCEEDED|FAILED|STOPPED|TIMEOUT|ERROR) break ;;
    esac
    sleep 20
  done
  echo "Estado: $estado"
  aws glue get-job-run --job-name "$JOB_SCD" --run-id "$id" --query JobRun.ErrorMessage --output text
}

gold_ilegible() {
  local archivo="${RUTA_GOLD}part-99999-prueba-scd01.snappy.parquet"
  case "${1:-}" in
    poner) echo "no es parquet" | aws s3 cp - "$archivo" --only-show-errors && echo "Archivo corrupto en $archivo" ;;
    quitar) aws s3 rm "$archivo" --only-show-errors && echo "Archivo corrupto borrado" ;;
    *) echo "Uso: gold-ilegible {poner|quitar}"; return 1 ;;
  esac
}

freno_mismo_periodo() {
  local silver original copia partes total mitad
  original=$(mktemp)
  aws s3 cp "$RUTA_PARAMETROS" "$original" --only-show-errors || return 1
  silver=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["ruta_silver_preparada"].rstrip("/") + "/")' "$original")
  copia="${silver}_prueba_freno/"
  mapfile -t partes < <(aws s3 ls "$silver" | awk '{print $4}' | grep '\.parquet$')
  total=${#partes[@]}
  if [ "$total" -lt 2 ]; then
    echo "Silver preparada tiene $total archivo parquet; no se puede armar una entrada parcial copiando archivos."
    return 1
  fi
  mitad=$(( total / 2 ))
  for p in "${partes[@]:0:$mitad}"; do
    aws s3 cp "${silver}${p}" "${copia}${p}" --only-show-errors
  done
  echo "Entrada parcial: $mitad de $total archivos en $copia"
  python3 -c 'import json,sys; p=json.load(open(sys.argv[1])); p["ruta_silver_preparada"]=sys.argv[2]; print(json.dumps(p, ensure_ascii=False, indent=2))' "$original" "$copia" \
    | aws s3 cp - "$RUTA_PARAMETROS" --only-show-errors
  PERMITIR=false scd
  aws s3 cp "$RUTA_PARAMETROS" - | python3 -c 'import json,sys; p=json.load(sys.stdin); print("estado_gold_scd:", p.get("estado_gold_scd")); print("motivo:", p.get("motivo_gold_scd"))'
  aws s3 cp "$original" "$RUTA_PARAMETROS" --only-show-errors && echo "parametros_extraccion.json restaurado"
  aws s3 rm "$copia" --recursive --only-show-errors && echo "Entrada parcial borrada"
  rm -f "$original"
}

script() {
  local job="${JOB_SCD:?Defina JOB_SCD}"
  local ruta
  ruta=$(aws glue get-job --job-name "$job" --query Job.Command.ScriptLocation --output text)
  echo "ScriptLocation: $ruta"
  if aws s3 cp "$ruta" - 2>/dev/null | grep -q "_control_scd"; then
    echo "El script desplegado trae la regla de periodo."
  else
    echo "El script desplegado NO trae la regla de periodo."
  fi
}

case "${1:-}" in
  foto) foto ;;
  respaldo) respaldo ;;
  restaurar) shift; restaurar "$@" ;;
  scd) scd ;;
  script) script ;;
  gold-ilegible) shift; gold_ilegible "$@" ;;
  freno-mismo-periodo) freno_mismo_periodo ;;
  *) echo "Uso: [AMB=dev] [JOB_SCD=...] [PERMITIR=true] bash $0 {script|foto|respaldo|scd|gold-ilegible poner|quitar|freno-mismo-periodo|restaurar <ruta>}" ;;
esac
