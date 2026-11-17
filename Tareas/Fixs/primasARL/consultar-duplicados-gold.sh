#!/usr/bin/env bash
set -uo pipefail

AMB="${AMB:-dev}"
WG="${WG:-primary}"
OUT="${OUT:-}"
DB="niif-${AMB}-gold-catalog-db"
SALIDA="duplicados-gold-${AMB}.tsv"

MALLAS=(
  "auxilios-funerarios|scd_auxilios_funerarios|num_solicitud_tesoreria,num_poliza,tipo_identificacion_afiliado,num_identificacion_afiliado,cod_concepto,cod_origen"
  "pcr_vida|scd_pcr_vida|nro_operacion,nro_poliza"
  "primas-arl|scd_primas_arl|nid_poliza,nid_endoso"
  "primas-endosos|scd_primas_endosos|nid_renta"
  "primas-rentas|scd_primas_rentas|nid_renta"
  "reserva-matematica-arp|scd_reserva_matematica_arp|per_corte,fec_ini_per_corte,fec_fin_per_corte,ramo,nro_poliza,nro_siniestro"
  "reserva-matematica-rvi|scd_reserva_matematica_rvi|per_corte,fec_ini_per_corte,fec_fin_per_corte,per_pago,fec_ini_per_pago,fec_fin_per_pago,ani_per_pago,nro_poliza"
)

consultar() {
  local sql="$1" id estado intento=0
  local args=(--work-group "$WG" --query-string "$sql" --query-execution-context "Database=$DB")
  [ -n "$OUT" ] && args+=(--result-configuration "OutputLocation=$OUT")
  id=$(aws athena start-query-execution "${args[@]}" --query QueryExecutionId --output text 2>/dev/null) || { echo "ERROR_INICIO"; return; }
  while [ $intento -lt 90 ]; do
    estado=$(aws athena get-query-execution --query-execution-id "$id" --query QueryExecution.Status.State --output text)
    case "$estado" in
      SUCCEEDED) aws athena get-query-results --query-execution-id "$id" --query 'ResultSet.Rows[1].Data[0].VarCharValue' --output text; return ;;
      FAILED|CANCELLED) echo "ERROR_${estado}"; return ;;
    esac
    intento=$((intento + 1)); sleep 2
  done
  echo "ERROR_TIEMPO"
}

echo "Cuenta: $(aws sts get-caller-identity --query Account --output text) | Base: $DB | Workgroup: $WG"
printf "malla\ttabla\texiste\tllaves_duplicadas\tvigentes_con_llave_vacia\n" | tee "$SALIDA"
for entrada in "${MALLAS[@]}"; do
  IFS='|' read -r malla tabla llaves <<< "$entrada"
  if ! aws glue get-table --database-name "$DB" --name "$tabla" >/dev/null 2>&1; then
    printf "%s\t%s\tno\t-\t-\n" "$malla" "$tabla" | tee -a "$SALIDA"; continue
  fi
  nulos=$(echo "$llaves" | sed 's/,/ IS NULL OR /g; s/$/ IS NULL/')
  duplicadas=$(consultar "SELECT COUNT(*) FROM (SELECT $llaves FROM \"$DB\".\"$tabla\" WHERE act_reg = 1 GROUP BY $llaves HAVING COUNT(*) > 1)")
  vacias=$(consultar "SELECT COUNT(*) FROM \"$DB\".\"$tabla\" WHERE act_reg = 1 AND ($nulos)")
  printf "%s\t%s\tsi\t%s\t%s\n" "$malla" "$tabla" "$duplicadas" "$vacias" | tee -a "$SALIDA"
done
echo "Resultado en $SALIDA"
