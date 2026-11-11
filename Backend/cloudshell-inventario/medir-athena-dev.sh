#!/usr/bin/env bash
# Mide las consultas de Athena de la cadena de certificacion y notificacion NIIF 17 (roadmap, paso 2 / 7.1).
# SOLO LECTURA por defecto: lee el historial de Athena, las corridas de Glue, el catalogo y el tamano de Gold.
#
# Uso (CloudShell de DEV):
#   bash medir-athena-dev.sh                    # historial de los ultimos 45 dias (maximo que guarda Athena)
#   bash medir-athena-dev.sh 15                 # historial de los ultimos 15 dias
#   bash medir-athena-dev.sh 45 --ejecutar 202606
#       ademas CORRE AHORA las consultas summary, total y de disponibilidad de los 4 casos para ese periodo.
#       Escanea datos de Gold y tiene costo. No corre las detail (son UNLOAD y escriben archivos).
#
# Resultado: /home/cloudshell-user/medicion-athena.zip   (descargar con Actions > Download file)

export AWS_PAGER=""
REGION="${REGION:-us-east-1}"
AMB="${AMB:-dev}"
DIAS="${1:-45}"
EJECUTAR="no"; PERIODO=""
if [ "${2:-}" = "--ejecutar" ]; then EJECUTAR="si"; PERIODO="${3:-}"; fi
if [ "$EJECUTAR" = "si" ] && ! [[ "$PERIODO" =~ ^[0-9]{6}$ ]]; then
  echo "Con --ejecutar hay que indicar el periodo AAAAMM. Ejemplo: bash medir-athena-dev.sh 45 --ejecutar 202606" >&2; exit 1
fi

BUCKET="niif-${AMB}-assets"
DB="niif-${AMB}-gold-catalog-db"
JOBS="niif-${AMB}-certification niif-${AMB}-notification-certification"
CASOS="primas-arl/primasArl.json primas-rentas/primasRentasEndosos.json auxilios-funerarios/auxilioFunerario.json reserva-matematica/reservaMatematicaRviArp.json"
GLOBAL="configs/certification/json_global_notif_cert.json"

# Se trabaja en /tmp: el $HOME de CloudShell tiene 1 GB.
OUT="/tmp/medicion-athena"
rm -rf "$OUT"; mkdir -p "$OUT"/{raw,config}; LOG="$OUT/_errores.log"; : > "$LOG"
A="--region $REGION --output json"
echo "dias=$DIAS ejecutar=$EJECUTAR periodo=${PERIODO:--} ambiente=$AMB fecha=$(date -u +%FT%TZ)" > "$OUT/parametros.txt"

echo "== 1. Configuracion: JSON de caso y global (solo para clasificar las consultas) =="
for k in $CASOS $GLOBAL; do
  aws s3 cp "s3://$BUCKET/$k" "$OUT/config/$(echo "$k" | tr / _)" --only-show-errors 2>>"$LOG" || echo "NO EXISTE: $k" | tee -a "$LOG"
done
# Se quitan correos y aprobadores antes de guardar
for f in "$OUT"/config/*.json; do
  [ -f "$f" ] && jq 'del(.email.destinations, .email.sender, .approvers)' "$f" > "$f.tmp" 2>>"$LOG" && mv "$f.tmp" "$f"
done

echo "== 2. Workgroups (tope de escaneo) =="
aws athena list-work-groups $A > "$OUT/raw/workgroups.json" 2>>"$LOG"
: > "$OUT/raw/workgroups-config.jsonl"
for wg in $(jq -r '.WorkGroups[].Name' "$OUT/raw/workgroups.json"); do
  aws athena get-work-group --work-group "$wg" $A 2>>"$LOG" | jq -c '.WorkGroup | {Name, State,
    corte_bytes: .Configuration.BytesScannedCutoffPerQuery, forzado: .Configuration.EnforceWorkGroupConfiguration,
    salida: .Configuration.ResultConfiguration.OutputLocation}' >> "$OUT/raw/workgroups-config.jsonl"
done

echo "== 3. Historial de consultas de Athena (puede tardar unos minutos) =="
: > "$OUT/raw/ids.txt"
for wg in $(jq -r '.WorkGroups[].Name' "$OUT/raw/workgroups.json"); do
  aws athena list-query-executions --work-group "$wg" $A --query 'QueryExecutionIds' 2>>"$LOG" | jq -r '.[]' >> "$OUT/raw/ids.txt"
done
sort -u "$OUT/raw/ids.txt" -o "$OUT/raw/ids.txt"
echo "consultas en el historial: $(wc -l < "$OUT/raw/ids.txt")"
: > "$OUT/raw/ejecuciones.jsonl"
split -l 50 "$OUT/raw/ids.txt" "$OUT/raw/lote_"
for lote in "$OUT"/raw/lote_*; do
  aws athena batch-get-query-execution --query-execution-ids $(cat "$lote") $A 2>>"$LOG" \
    | jq -c '.QueryExecutions[]' >> "$OUT/raw/ejecuciones.jsonl"
  rm -f "$lote"
done

echo "== 4. Corridas de los jobs de Glue de la cadena =="
: > "$OUT/raw/glue-runs.jsonl"
for j in $JOBS; do
  aws glue get-job-runs --job-name "$j" --max-results 200 $A 2>>"$LOG" \
    | jq -c --arg j "$j" '.JobRuns[] | {job: $j, id: .Id, estado: .JobRunState, inicio: .StartedOn, fin: .CompletedOn,
        segundos: .ExecutionTime, dpu_segundos: (.DPUSeconds // null), workers: .NumberOfWorkers, tipo: .WorkerType,
        error: ((.ErrorMessage // "")[0:200])}' >> "$OUT/raw/glue-runs.jsonl"
done

echo "== 5. Tablas de Gold: particiones y tamano en S3 =="
: > "$OUT/raw/gold-tablas.jsonl"
for t in $(aws glue get-tables --database-name "$DB" $A --query 'TableList[].Name' 2>>"$LOG" | jq -r '.[]' | grep -E '^scd_|reserva_matematica_cargue_unico'); do
  info=$(aws glue get-table --database-name "$DB" --name "$t" $A 2>>"$LOG" \
    | jq -c '.Table | {tabla: .Name, ubicacion: .StorageDescriptor.Location, particiones: [.PartitionKeys[]?.Name],
        columnas: (.StorageDescriptor.Columns | length), formato: (.StorageDescriptor.InputFormat | split(".") | last)}')
  ubic=$(echo "$info" | jq -r '.ubicacion')
  tam=$(aws s3 ls "$ubic" --recursive --summarize 2>>"$LOG" | awk '/Total Objects/{o=$3} /Total Size/{s=$3} END{printf "%s %s", o+0, s+0}')
  echo "$info" | jq -c --arg o "${tam% *}" --arg b "${tam#* }" '. + {archivos: ($o|tonumber), bytes: ($b|tonumber)}' >> "$OUT/raw/gold-tablas.jsonl"
  echo "  $t -> ${tam#* } bytes en ${tam% *} archivos"
done

if [ "$EJECUTAR" = "si" ]; then
  echo "== 6. Ejecucion controlada de summary, total y disponibilidad para $PERIODO (tiene costo) =="
  SALIDA="s3://$BUCKET/$(jq -r '.athena.output_path // "athena/notifications"' "$OUT/config/$(echo $GLOBAL | tr / _)" 2>/dev/null)/medicion/"
  : > "$OUT/raw/ejecucion-controlada.jsonl"
  for f in "$OUT"/config/*.json; do
    caso=$(jq -r '.useCaseId // empty' "$f"); [ -z "$caso" ] && continue
    jq -c '[(.queries.summary // [])[] | . + {tipo:"summary"}] + [(.queries.total // [])[] | . + {tipo:"total"}]
           + [(.email.dataPresence // [])[] | . + {tipo:"dataPresence", queryName:.name}] | .[]' "$f" | while read -r q; do
      sql=$(echo "$q" | jq -r '.query' | sed "s/\${period}/$PERIODO/g; s/\$period/$PERIODO/g")
      dbq=$(echo "$q" | jq -r '.database')
      qid=$(aws athena start-query-execution --query-string "$sql" --query-execution-context "Database=$dbq,Catalog=AwsDataCatalog" \
            --result-configuration "OutputLocation=$SALIDA" $A --query QueryExecutionId 2>>"$LOG" | jq -r .)
      [ -z "$qid" ] || [ "$qid" = "null" ] && { echo "  fallo al iniciar: $caso $(echo "$q" | jq -r .queryName)" | tee -a "$LOG"; continue; }
      for _ in $(seq 1 900); do
        st=$(aws athena get-query-execution --query-execution-id "$qid" $A --query 'QueryExecution.Status.State' 2>>"$LOG" | jq -r .)
        case "$st" in SUCCEEDED|FAILED|CANCELLED) break;; esac; sleep 1
      done
      aws athena get-query-execution --query-execution-id "$qid" $A 2>>"$LOG" \
        | jq -c --arg caso "$caso" --arg tipo "$(echo "$q" | jq -r .tipo)" --arg nombre "$(echo "$q" | jq -r .queryName)" \
          '{caso:$caso, tipo:$tipo, consulta:$nombre, id:.QueryExecution.QueryExecutionId, estado:.QueryExecution.Status.State,
            motivo:(.QueryExecution.Status.StateChangeReason // ""), ms:.QueryExecution.Statistics.TotalExecutionTimeInMillis,
            bytes:.QueryExecution.Statistics.DataScannedInBytes}' >> "$OUT/raw/ejecucion-controlada.jsonl"
      echo "  $caso $(echo "$q" | jq -r .tipo) $(tail -1 "$OUT/raw/ejecucion-controlada.jsonl" | jq -r '"\(.estado) \(.ms) ms \(.bytes) bytes"')"
    done
  done
fi

echo "== 7. Analisis =="
python3 - "$OUT" "$DB" <<'PY'
import glob, json, os, re, statistics, sys
from collections import defaultdict
out, db = sys.argv[1], sys.argv[2]
PRECIO_TB = 5.0  # USD por TB escaneado en Athena (us-east-1); minimo 10 MB por consulta

def norm(t):
    return re.sub(r"\s+", " ", (t or "")).strip().lower()

patrones = []
for f in glob.glob(os.path.join(out, "config", "*.json")):
    try:
        d = json.load(open(f, encoding="utf-8"))
    except Exception:
        continue
    caso = d.get("useCaseId")
    if not caso:
        continue
    items = [("summary", q) for q in d.get("queries", {}).get("summary", [])]
    items += [("total", q) for q in d.get("queries", {}).get("total", [])]
    items += [("detail", q) for q in d.get("queries", {}).get("detail", [])]
    items += [("dataPresence", dict(q, queryName=q.get("name"))) for q in d.get("email", {}).get("dataPresence", [])]
    for tipo, q in items:
        plantilla = re.escape(norm(q.get("query")))
        plantilla = re.sub(r"\\\$(?:\\\{)?period(?:\\\})?", r"\\d{6}", plantilla)
        plantilla = re.sub(r"\\\$(?:\\\{)?[a-z_]+(?:\\\})?", r".*?", plantilla)
        patrones.append((re.compile("^" + plantilla + "$"), caso, tipo, q.get("queryName", "")))

TABLAS = re.compile(r'(scd_[a-z_]+|reserva_matematica_cargue_unico)')
filas = []
for linea in open(os.path.join(out, "raw", "ejecuciones.jsonl"), encoding="utf-8"):
    e = json.loads(linea)
    texto = e.get("Query", "")
    contexto = e.get("QueryExecutionContext", {}).get("Database", "")
    tablas = sorted(set(TABLAS.findall(texto)))
    if not tablas and contexto != db:
        continue
    caso, tipo, nombre = "-", "otra", "-"
    n = norm(texto)
    for patron, c, t, qn in patrones:
        if patron.match(n):
            caso, tipo, nombre = c, t, qn
            break
    if tipo == "otra" and n.startswith("unload"):
        tipo = "detail?"
    st, s = e.get("Status", {}), e.get("Statistics", {})
    filas.append({
        "fecha": str(st.get("SubmissionDateTime", ""))[:19], "estado": st.get("State"), "caso": caso, "tipo": tipo,
        "consulta": nombre, "tablas": ",".join(tablas), "workgroup": e.get("WorkGroup"),
        "total_ms": s.get("TotalExecutionTimeInMillis") or 0, "cola_ms": s.get("QueryQueueTimeInMillis") or 0,
        "motor_ms": s.get("EngineExecutionTimeInMillis") or 0, "bytes": s.get("DataScannedInBytes") or 0,
        "id": e.get("QueryExecutionId"), "motivo": (st.get("StateChangeReason") or "")[:150],
    })

cols = ["fecha", "estado", "caso", "tipo", "consulta", "tablas", "workgroup", "total_ms", "cola_ms", "motor_ms", "bytes", "id", "motivo"]
with open(os.path.join(out, "consultas-gold.tsv"), "w", encoding="utf-8") as f:
    f.write("\t".join(cols) + "\n")
    for r in sorted(filas, key=lambda x: x["fecha"]):
        f.write("\t".join(str(r[c]) for c in cols) + "\n")

def pct(v, p):
    v = sorted(v)
    return v[min(len(v) - 1, int(round(p * (len(v) - 1))))] if v else 0

def costo(b):
    return max(b, 10 * 1024 ** 2) / 1024 ** 4 * PRECIO_TB

res = ["RESUMEN DE CONSULTAS SOBRE GOLD NIIF (historial de Athena)", ""]
grupos = defaultdict(list)
for r in filas:
    grupos[(r["caso"], r["tipo"])].append(r)
res.append(f"{'caso':28} {'tipo':13} {'n':>4} {'fallas':>6} {'mediana_s':>9} {'p90_s':>7} {'max_s':>7} {'MB_prom':>9} {'MB_max':>9} {'USD_total':>9}")
for (caso, tipo), rs in sorted(grupos.items()):
    ok = [r for r in rs if r["estado"] == "SUCCEEDED"]
    t = [r["total_ms"] / 1000 for r in ok]
    b = [r["bytes"] for r in ok]
    res.append(f"{caso:28} {tipo:13} {len(rs):>4} {len(rs) - len(ok):>6} {pct(t, .5):>9.1f} {pct(t, .9):>7.1f} {max(t or [0]):>7.1f} "
               f"{(statistics.mean(b) if b else 0) / 1048576:>9.1f} {max(b or [0]) / 1048576:>9.1f} {sum(costo(x) for x in b):>9.4f}")
total_b = sum(r["bytes"] for r in filas)
res += ["", f"Consultas sobre Gold: {len(filas)} | fallidas: {sum(1 for r in filas if r['estado'] != 'SUCCEEDED')} | "
        f"escaneado total: {total_b / 1024 ** 3:.2f} GB | costo estimado: USD {sum(costo(r['bytes']) for r in filas):.4f}"]
lentas = sorted(filas, key=lambda r: -r["total_ms"])[:10]
res += ["", "LAS 10 MAS LENTAS"] + [f"  {r['total_ms']/1000:7.1f}s {r['bytes']/1048576:9.1f} MB {r['estado']:9} {r['caso']} {r['tipo']} {r['fecha']}" for r in lentas]

ruta_g = os.path.join(out, "raw", "gold-tablas.jsonl")
if os.path.exists(ruta_g):
    res += ["", "TABLAS DE GOLD (COS-07)"]
    for linea in open(ruta_g, encoding="utf-8"):
        g = json.loads(linea)
        res.append(f"  {g['tabla']:36} {g['bytes'] / 1048576:10.1f} MB {g['archivos']:>6} archivos  particiones={g['particiones'] or 'NINGUNA'}  columnas={g['columnas']}")

ruta_w = os.path.join(out, "raw", "workgroups-config.jsonl")
if os.path.exists(ruta_w):
    res += ["", "WORKGROUPS (COS-08)"]
    for linea in open(ruta_w, encoding="utf-8"):
        w = json.loads(linea)
        res.append(f"  {w['Name']:20} tope={w.get('corte_bytes') or 'SIN TOPE'} forzado={w.get('forzado')}")

ruta_j = os.path.join(out, "raw", "glue-runs.jsonl")
if os.path.exists(ruta_j):
    res += ["", "JOBS DE GLUE DE LA CADENA"]
    runs = defaultdict(list)
    for linea in open(ruta_j, encoding="utf-8"):
        j = json.loads(linea)
        runs[j["job"]].append(j)
    for job, rs in runs.items():
        seg = [r["segundos"] or 0 for r in rs if r["estado"] == "SUCCEEDED"]
        res.append(f"  {job:45} corridas={len(rs)} ok={len(seg)} mediana={pct(seg, .5)}s p90={pct(seg, .9)}s max={max(seg or [0])}s")

ruta_c = os.path.join(out, "raw", "ejecucion-controlada.jsonl")
if os.path.exists(ruta_c):
    res += ["", "EJECUCION CONTROLADA"]
    for linea in open(ruta_c, encoding="utf-8"):
        c = json.loads(linea)
        res.append(f"  {c['caso']:28} {c['tipo']:13} {c['estado']:9} {(c['ms'] or 0)/1000:7.1f}s {(c['bytes'] or 0)/1048576:9.1f} MB  {c['motivo'][:80]}")

open(os.path.join(out, "resumen.txt"), "w", encoding="utf-8").write("\n".join(res) + "\n")
print("\n".join(res))
PY

cd /tmp && rm -f "$HOME/medicion-athena.zip" && zip -qr "$HOME/medicion-athena.zip" medicion-athena
echo
echo "Listo: /home/cloudshell-user/medicion-athena.zip ($(du -h "$HOME/medicion-athena.zip" | cut -f1)). Errores en $LOG"
