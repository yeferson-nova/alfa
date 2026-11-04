#!/usr/bin/env bash
# Recolecta, en SOLO LECTURA, lo que falta para el roadmap (T2 lambda general y T1 SNS -> SES).
# No crea, modifica ni borra nada en AWS. Solo hace describe/get/list y descarga código.
#
# Uso (en AWS CloudShell, region us-east-1):
#   bash recolectar-faltantes.sh            # todas las secciones
#   bash recolectar-faltantes.sh lambdas s3 # solo algunas
# Secciones: lambdas s3 ses dynamo scheduler iam glue sfn athena logs buckets metricas
# Resultado: ~/niif-faltantes.zip  (descargar con Actions > Download file)
#
# OJO: el zip trae correos corporativos (destinatarios y suscriptores) y variables de
# entorno. No subirlo a un repositorio ni compartirlo fuera del equipo.

export AWS_PAGER=""
REGION="${REGION:-us-east-1}"
export AWS_DEFAULT_REGION="$REGION"
OUT="$HOME/niif-faltantes"
LOG="$OUT/_errores.log"
TMP="/tmp/niif-faltantes"   # los zips de las lambdas van a /tmp, no al home (1 GB)

ASSETS_BUCKET="niif-dev-assets"
MALLAS="primas-rentas primas-endosos primas-arl auxilios-funerarios reserva-matematica-rvi reserva-matematica-arp"
MALLAS_PK="PRIMAS_RENTAS PRIMAS_ENDOSOS PRIMAS_ARL AUXILIOS_FUNERARIOS RESERVA_MATEMATICA_RVI RESERVA_MATEMATICA_ARP"

LAMBDAS_TAREA="niif-dev-manager-configuration $(for m in $MALLAS; do printf 'niif-dev-configuration-%s ' "$m"; done)"
LAMBDAS_CADENA="niif-dev-notification-certification-trigger niif-dev-copy-s3-cert niif-dev-move-s3-certification \
niif-dev-short-link-redirect niif-dev-composite-notification-evaluator niif-dev-certification \
EnvioSftpFis datalake-dev-niif-send-via-sftp ALFA-DEV-DATALAKE-PipelineObserver testNotifications"
LAMBDAS_REFERENCIA="niif-dev-programador-configuraciones"

GLUE_JOBS="niif-dev-extraccion-estandar-optimized niif-dev-val-tecnica niif-dev-remediacion-estandar niif-dev-scd \
niif-dev-certification niif-dev-notification-certification $(for m in $MALLAS; do printf 'niif-dev-silver-%s ' "$m"; done)"
CRAWLERS="niif-dev-crawler-bronze niif-dev-crawler-silver niif-dev-crawler-gold"
STATE_MACHINES="$(for m in $MALLAS; do printf 'niif-dev-%s ' "$m"; done)"

TABLAS_DYNAMO="niif-dev-prg-programaciones niif-dev-cnf-mallas niif-dev-prs-estructuras niif-dev-ejc-ejecuciones \
niif-dev-pipeline-estado niif-dev-notification-certification niif-dev-composite-notification-status niif-dev-short-links \
niif-dev-approval-audit-log datalake-dev-use-case-queries datalake-dev-use-case-emails datalake-dev-use-case-approvers"

BUCKETS="niif-dev-assets niif-dev-certification niif-dev-transport alfa-datalake-dev-niif-transport \
alfa-datalake-dev-niif-certification alfa-datalake-dev-bronce alfa-datalake-dev-silver alfa-datalake-dev-gold niif-dev-lambda-artifacts"

# Carpetas de niif-dev-assets donde viven los JSON de configuración.
PREFIJOS_S3="configs/ remediacion-estandar/ reserva-matematica/ $(for m in $MALLAS; do printf '%s/ ' "$m"; done)"

run() { # run <archivo_salida> <comando...>: guarda stdout; si falla, lo anota y sigue.
  local out="$1"; shift
  "$@" > "$out" 2>>"$LOG" || { echo "FALLO: $*" >> "$LOG"; rm -f "$out"; }
}
seccion() { echo; echo "== $* =="; }
quiere() { [ "$SECCIONES" = "todo" ] || [[ " $SECCIONES " == *" $1 "* ]]; }

SECCIONES="${*:-todo}"
rm -rf "$OUT" "$TMP"
mkdir -p "$OUT"/{lambdas/codigo,lambdas/config,s3,ses,dynamo,scheduler,iam,glue,sfn,athena,logs,buckets,metricas} "$TMP"
: > "$LOG"
cd "$OUT" || exit 1

# Extrae del zip de una lambda solo el código propio: deja fuera las librerías empaquetadas
# (pandas, numpy, boto3...) que fueron las que llenaron el disco en la extracción anterior.
cat > "$TMP/extraer.py" <<'PY'
import sys, zipfile, os
zip_path, destino = sys.argv[1], sys.argv[2]
TERCEROS = {"boto3","botocore","s3transfer","jmespath","urllib3","dateutil","pytz","tzdata","six","numpy",
            "numpy.libs","pandas","pandas.libs","bin","requests","certifi","charset_normalizer","idna",
            "pyarrow","pyarrow.libs","__pycache__","_distutils_hack","setuptools","pip","typing_extensions"}
EXT = (".py", ".json", ".html", ".txt", ".yaml", ".yml", ".cfg", ".md", ".sql", ".j2", ".tpl")
with zipfile.ZipFile(zip_path) as z:
    nombres = z.namelist()
    for n in nombres:  # paquetes declarados por los .dist-info también son de terceros
        partes = n.split("/")
        if partes[0].endswith((".dist-info", ".egg-info")):
            base = partes[0].split("-")[0]
            TERCEROS.update({base, base.lower(), base.replace("_", "-")})
            if partes[-1] == "top_level.txt":
                TERCEROS.update(l.strip() for l in z.read(n).decode("utf-8", "ignore").splitlines() if l.strip())
    copiados = 0
    for info in z.infolist():
        n = info.filename
        top = n.split("/")[0]
        if info.is_dir() or top in TERCEROS or top.endswith((".dist-info", ".egg-info")) or top == "six.py":
            continue
        if not n.lower().endswith(EXT) or info.file_size > 2_000_000:
            continue
        z.extract(info, destino); copiados += 1
    print(f"{copiados} archivos propios de {len(nombres)} en el paquete")
PY

echo "Cuenta: $(aws sts get-caller-identity --query Account --output text 2>>"$LOG")  Region: $REGION"
aws sts get-caller-identity > identidad.json 2>>"$LOG"

# ---------------------------------------------------------------- lambdas
if quiere lambdas; then
  seccion "Lambdas: configuración, triggers, permisos y código propio"
  for f in $LAMBDAS_TAREA $LAMBDAS_CADENA $LAMBDAS_REFERENCIA; do
    echo "- $f"
    # get-function-configuration NO trae la URL prefirmada del código (no quedan tokens en los archivos).
    run "lambdas/config/$f.json"          aws lambda get-function-configuration --function-name "$f"
    [ -s "lambdas/config/$f.json" ] || { echo "  no existe o sin permiso"; continue; }
    run "lambdas/config/$f.triggers.json" aws lambda list-event-source-mappings --function-name "$f"
    run "lambdas/config/$f.policy.json"   aws lambda get-policy --function-name "$f"
    run "lambdas/config/$f.tags.json"     aws lambda list-tags --resource "$(jq -r .FunctionArn "lambdas/config/$f.json")"
    run "lambdas/config/$f.alias.json"    aws lambda list-aliases --function-name "$f"
    run "lambdas/config/$f.concurrency.json" aws lambda get-function-concurrency --function-name "$f"
    run "lambdas/config/$f.destinos.json" aws lambda get-function-event-invoke-config --function-name "$f"
    if [ "$(jq -r '.PackageType // "Zip"' "lambdas/config/$f.json")" = "Image" ]; then
      uri="$(jq -r '.Code.ImageUri // empty' "lambdas/config/$f.json")"
      [ -z "$uri" ] && uri="$(aws lambda get-function --function-name "$f" --query Code.ImageUri --output text 2>>"$LOG")"
      echo "$uri" > "lambdas/codigo/$f.IMAGEN.txt"
      repo="$(echo "$uri" | sed -E 's#^[^/]+/([^:@]+).*#\1#')"
      run "lambdas/codigo/$f.ecr-imagenes.json" aws ecr describe-images --repository-name "$repo"
      echo "  imagen de contenedor: el código no se puede bajar desde CloudShell (sin Docker). Ver IMAGEN.txt"
      continue
    fi
    url="$(aws lambda get-function --function-name "$f" --query Code.Location --output text 2>>"$LOG")"
    if [ -n "$url" ] && [ "$url" != "None" ]; then
      curl -s -o "$TMP/$f.zip" "$url" && python3 -I "$TMP/extraer.py" "$TMP/$f.zip" "lambdas/codigo/$f" 2>>"$LOG" \
        || echo "FALLO codigo: $f" >> "$LOG"
      rm -f "$TMP/$f.zip"
    fi
  done
fi

# ---------------------------------------------------------------- s3 (JSON de configuración)
if quiere s3; then
  seccion "S3 $ASSETS_BUCKET: JSON y plantillas de configuración (menores de 1 MB)"
  run s3/raiz-assets.txt aws s3 ls "s3://$ASSETS_BUCKET/"
  for p in $PREFIJOS_S3; do
    lista="s3/listado-${p%/}.json"
    run "$lista" aws s3api list-objects-v2 --bucket "$ASSETS_BUCKET" --prefix "$p" \
      --query 'Contents[].{k:Key,s:Size,m:LastModified}' --output json
    [ -s "$lista" ] || continue
    jq -r '.[]? | select(.s < 1048576) | select(.k | test("\\.(json|html|htm|txt)$"; "i"))
                | select(.k | test("/(historico|diagnosticos|athena|unload|resultados)/"; "i") | not) | .k' "$lista" |
    while read -r k; do
      mkdir -p "s3/$ASSETS_BUCKET/$(dirname "$k")"
      aws s3 cp "s3://$ASSETS_BUCKET/$k" "s3/$ASSETS_BUCKET/$k" --only-show-errors 2>>"$LOG"
    done
    echo "- $p: $(find "s3/$ASSETS_BUCKET/${p%/}" -type f 2>/dev/null | wc -l) archivos"
  done
fi

# ---------------------------------------------------------------- ses (preparación para T1a)
if quiere ses; then
  seccion "SES: cuenta, sandbox, identidades y conjuntos de configuración"
  run ses/cuenta-v2.json          aws sesv2 get-account
  run ses/cuota-v1.json           aws ses get-send-quota
  run ses/identidades-v2.json     aws sesv2 list-email-identities
  run ses/identidades-v1.json     aws ses list-identities
  if [ -s ses/identidades-v2.json ]; then
    for i in $(jq -r '.EmailIdentities[]?.IdentityName' ses/identidades-v2.json); do
      run "ses/identidad-${i//[^A-Za-z0-9._-]/_}.json" aws sesv2 get-email-identity --email-identity "$i"
    done
  fi
  run ses/config-sets.json        aws sesv2 list-configuration-sets
  if [ -s ses/config-sets.json ]; then
    for c in $(jq -r '.ConfigurationSets[]?' ses/config-sets.json); do
      run "ses/config-set-$c.json"          aws sesv2 get-configuration-set --configuration-set-name "$c"
      run "ses/config-set-$c.destinos.json" aws sesv2 get-configuration-set-event-destinations --configuration-set-name "$c"
    done
  fi
  run ses/plantillas.json         aws sesv2 list-email-templates
  run ses/supresion.json          aws sesv2 get-account --query SuppressionAttributes
  run ses/regla-delivery-test.json aws events describe-rule --name niif-dev-ses-delivery-test
  topic="arn:aws:sns:$REGION:$(jq -r .Account identidad.json):niif-dev-certification-email"
  run ses/sns-topic-certificacion.json aws sns get-topic-attributes --topic-arn "$topic"
fi

# ---------------------------------------------------------------- dynamo
if quiere dynamo; then
  seccion "DynamoDB: definición de tablas y registros de configuración (sin datos personales)"
  for t in $TABLAS_DYNAMO; do
    run "dynamo/$t.describe.json" aws dynamodb describe-table --table-name "$t"
    run "dynamo/$t.backups.json"  aws dynamodb describe-continuous-backups --table-name "$t"
    run "dynamo/$t.ttl.json"      aws dynamodb describe-time-to-live --table-name "$t"
  done
  for m in $MALLAS_PK; do
    run "dynamo/cnf-mallas.$m.json" aws dynamodb query --table-name niif-dev-cnf-mallas \
      --key-condition-expression "PK = :p" --expression-attribute-values "{\":p\":{\"S\":\"MALLA#$m\"}}"
  done
  run dynamo/prg-programaciones.items.json        aws dynamodb scan --table-name niif-dev-prg-programaciones --max-items 200
  run dynamo/composite-notification-status.items.json aws dynamodb scan --table-name niif-dev-composite-notification-status --max-items 100
  run dynamo/pipeline-estado.items.json           aws dynamodb scan --table-name niif-dev-pipeline-estado --max-items 100
  run dynamo/ejc-ejecuciones.ultimos.json         aws dynamodb scan --table-name niif-dev-ejc-ejecuciones --max-items 100
  # notification-certification tiene correos de usuarios: solo se cuentan los registros por estado.
  run dynamo/notification-certification.conteo-por-estado.json bash -c \
    "aws dynamodb scan --table-name niif-dev-notification-certification --projection-expression '#s' \
       --expression-attribute-names '{\"#s\":\"status\"}' --output json | jq '[.Items[].status.S] | group_by(.) | map({estado: .[0], n: length})'"
fi

# ---------------------------------------------------------------- scheduler
if quiere scheduler; then
  seccion "EventBridge Scheduler: schedules de los configuradores"
  run scheduler/grupos.json aws scheduler list-schedule-groups
  for g in niif-dev-programacion-configuradores niif-dev-programacion-cargues; do
    run "scheduler/$g.lista.json" aws scheduler list-schedules --group-name "$g"
    [ -s "scheduler/$g.lista.json" ] || continue
    for s in $(jq -r '.Schedules[]?.Name' "scheduler/$g.lista.json"); do
      run "scheduler/$g.$s.json" aws scheduler get-schedule --group-name "$g" --name "$s"
    done
  done
fi

# ---------------------------------------------------------------- iam
if quiere iam; then
  seccion "IAM: políticas de los roles del flujo"
  roles="niif-dev-scheduler-configuradores-role niif-dev-crawler $(for m in $MALLAS; do printf 'niif-dev-%s ' "$m"; done)"
  for f in lambdas/config/*.json; do
    case "$f" in *.triggers.json|*.policy.json|*.tags.json|*.alias.json|*.concurrency.json|*.destinos.json) continue;; esac
    r="$(jq -r '.Role // empty' "$f" 2>/dev/null | awk -F/ '{print $NF}')"; [ -n "$r" ] && roles="$roles $r"
  done
  for j in $GLUE_JOBS; do
    r="$(aws glue get-job --job-name "$j" --query Job.Role --output text 2>>"$LOG" | awk -F/ '{print $NF}')"
    [ -n "$r" ] && [ "$r" != "None" ] && roles="$roles $r"
  done
  for r in $(echo "$roles" | tr ' ' '\n' | sort -u); do
    mkdir -p "iam/$r"
    run "iam/$r/rol.json"       aws iam get-role --role-name "$r"
    run "iam/$r/adjuntas.json"  aws iam list-attached-role-policies --role-name "$r"
    run "iam/$r/en-linea.json"  aws iam list-role-policies --role-name "$r"
    [ -s "iam/$r/en-linea.json" ] && for p in $(jq -r '.PolicyNames[]?' "iam/$r/en-linea.json"); do
      run "iam/$r/en-linea.$p.json" aws iam get-role-policy --role-name "$r" --policy-name "$p"
    done
    [ -s "iam/$r/adjuntas.json" ] && for a in $(jq -r '.AttachedPolicies[]?.PolicyArn' "iam/$r/adjuntas.json"); do
      case "$a" in arn:aws:iam::aws:policy/*) continue;; esac   # las administradas por AWS se conocen
      v="$(aws iam get-policy --policy-arn "$a" --query Policy.DefaultVersionId --output text 2>>"$LOG")"
      run "iam/$r/adjunta.$(basename "$a").json" aws iam get-policy-version --policy-arn "$a" --version-id "$v"
    done
  done
  echo "- $(ls iam | wc -l) roles"
fi

# ---------------------------------------------------------------- glue
if quiere glue; then
  seccion "Glue: definición, últimas ejecuciones, crawlers y etiquetas"
  for j in $GLUE_JOBS; do
    run "glue/$j.job.json"  aws glue get-job --job-name "$j"
    run "glue/$j.runs.json" aws glue get-job-runs --job-name "$j" --max-items 20 \
      --query 'JobRuns[].{id:Id,estado:JobRunState,inicio:StartedOn,seg:ExecutionTime,dpuSeg:DPUSeconds,workers:NumberOfWorkers,tipo:WorkerType,intento:Attempt,error:ErrorMessage}'
    run "glue/$j.tags.json" aws glue get-tags --resource-arn "arn:aws:glue:$REGION:$(jq -r .Account identidad.json):job/$j"
  done
  for c in $CRAWLERS; do run "glue/$c.crawler.json" aws glue get-crawler --name "$c"; done
  run glue/crawlers.metricas.json aws glue get-crawler-metrics --crawler-name-list $CRAWLERS
  run glue/conexion-alfaprd.json aws glue get-connection --name datalake-dev-alfaprd-dev-connection --hide-password
fi

# ---------------------------------------------------------------- step functions
if quiere sfn; then
  seccion "Step Functions: definición vigente, últimas ejecuciones y una historia de cada tipo"
  cuenta="$(jq -r .Account identidad.json)"
  for m in $STATE_MACHINES; do
    arn="arn:aws:states:$REGION:$cuenta:stateMachine:$m"
    run "sfn/$m.describe.json"   aws stepfunctions describe-state-machine --state-machine-arn "$arn"
    run "sfn/$m.tags.json"       aws stepfunctions list-tags-for-resource --resource-arn "$arn"
    run "sfn/$m.ejecuciones.json" aws stepfunctions list-executions --state-machine-arn "$arn" --max-items 30
    [ -s "sfn/$m.ejecuciones.json" ] || continue
    for est in SUCCEEDED FAILED; do
      e="$(jq -r --arg e "$est" '[.executions[]? | select(.status==$e)][0].executionArn // empty' "sfn/$m.ejecuciones.json")"
      [ -n "$e" ] && run "sfn/$m.historia-$est.json" aws stepfunctions get-execution-history --execution-arn "$e" --max-items 1000
      [ -n "$e" ] && run "sfn/$m.entrada-$est.json"  aws stepfunctions describe-execution --execution-arn "$e"
    done
  done
fi

# ---------------------------------------------------------------- athena
if quiere athena; then
  seccion "Athena: grupos de trabajo y bytes escaneados de las últimas consultas"
  run athena/grupos.json aws athena list-work-groups
  run athena/primary.json aws athena get-work-group --work-group primary
  ids="$(aws athena list-query-executions --work-group primary --max-items 300 --query 'QueryExecutionIds' --output text 2>>"$LOG" | tr '\t' '\n' | grep -v '^None$')"
  : > athena/consultas.jsonl
  echo "$ids" | xargs -r -n 50 | while read -r lote; do
    aws athena batch-get-query-execution --query-execution-ids $lote --output json 2>>"$LOG" |
      jq -c '.QueryExecutions[] | {id: .QueryExecutionId, estado: .Status.State, enviada: .Status.SubmissionDateTime,
             base: .QueryExecutionContext.Database, bytes: .Statistics.DataScannedInBytes,
             ms: .Statistics.EngineExecutionTimeInMillis, consulta: (.Query[0:300])}' >> athena/consultas.jsonl
  done
  echo "- $(wc -l < athena/consultas.jsonl) consultas"
fi

# ---------------------------------------------------------------- logs
if quiere logs; then
  seccion "CloudWatch Logs: retención de los grupos del flujo"
  for p in /aws/lambda/niif-dev /aws-glue/jobs /aws/vendedlogs/states /aws/lambda/ALFA-DEV-DATALAKE /aws/events/niif-dev; do
    run "logs/$(echo "$p" | tr '/' '_').json" aws logs describe-log-groups --log-group-name-prefix "$p" \
      --query 'logGroups[].{grupo:logGroupName,retencionDias:retentionInDays,bytes:storedBytes}'
  done
fi

# ---------------------------------------------------------------- buckets
if quiere buckets; then
  seccion "S3: existencia, cifrado, versionado, ciclo de vida y notificaciones"
  for b in $BUCKETS; do
    run "buckets/$b.existe.json"         aws s3api head-bucket --bucket "$b"
    [ -f "buckets/$b.existe.json" ] || { echo "- $b: NO existe o sin acceso"; continue; }
    run "buckets/$b.cifrado.json"        aws s3api get-bucket-encryption --bucket "$b"
    run "buckets/$b.versionado.json"     aws s3api get-bucket-versioning --bucket "$b"
    run "buckets/$b.ciclo-vida.json"     aws s3api get-bucket-lifecycle-configuration --bucket "$b"
    run "buckets/$b.notificaciones.json" aws s3api get-bucket-notification-configuration --bucket "$b"
    run "buckets/$b.eventbridge.json"    aws s3api get-bucket-notification-configuration --bucket "$b" --query EventBridgeConfiguration
    run "buckets/$b.bloqueo-publico.json" aws s3api get-public-access-block --bucket "$b"
    run "buckets/$b.raiz.txt"            aws s3 ls "s3://$b/"
    echo "- $b: ok"
  done
fi

# ---------------------------------------------------------------- métricas del observador (costo real)
if quiere metricas; then
  seccion "Costo real de los últimos 30 días (registros de PipelineObserver, Logs Insights)"
  grupo="/aws/lambda/ALFA-DEV-DATALAKE-PipelineObserver"
  desde=$(( $(date +%s) - 30*24*3600 )); hasta=$(date +%s)
  consulta() { # consulta <archivo> <query>
    local qid; qid="$(aws logs start-query --log-group-name "$grupo" --start-time "$desde" --end-time "$hasta" \
      --query-string "$2" --query queryId --output text 2>>"$LOG")" || return
    for _ in $(seq 1 30); do
      sleep 3
      aws logs get-query-results --query-id "$qid" > "$1" 2>>"$LOG"
      [ "$(jq -r .status "$1")" = "Complete" ] && return
    done
    echo "TIMEOUT consulta Logs Insights: $1" >> "$LOG"
  }
  consulta metricas/jobs.json 'filter recordType="GLUE_JOB" and job like /niif-dev/
    | stats count() as ejecuciones, avg(durationSec) as seg_prom, max(durationSec) as seg_max, sum(dpuHours) as dpu_horas by job, state'
  consulta metricas/step-functions.json 'filter recordType="SFN_EXECUTION" and stateMachine like /niif-dev/
    | stats count() as ejecuciones, avg(durationSec) as seg_prom, max(durationSec) as seg_max by stateMachine, executionStatus'
  consulta metricas/pasos.json 'filter recordType="SFN_STEP" and stateMachine like /niif-dev/
    | stats count() as veces, avg(durationSec) as seg_prom, max(durationSec) as seg_max by stateMachine, step'
  consulta metricas/crawlers.json 'filter recordType="GLUE_CRAWLER" and crawler like /niif-dev/
    | stats count() as ejecuciones, avg(durationSec) as seg_prom, max(durationSec) as seg_max by crawler, state'
fi

# ---------------------------------------------------------------- resumen
seccion "Resumen rápido"
{
  echo "Generado: $(date -u +%FT%TZ)  Cuenta: $(jq -r .Account identidad.json 2>/dev/null)  Secciones: $SECCIONES"
  echo
  echo "## Código propio recuperado por lambda"
  for d in lambdas/codigo/*; do [ -d "$d" ] && echo "$(basename "$d"): $(find "$d" -name '*.py' | wc -l) .py"; done
  ls lambdas/codigo/*.IMAGEN.txt 2>/dev/null | sed 's#.*/##; s#\.IMAGEN.txt# (imagen de contenedor)#'
  echo
  echo "## Triggers sobre el stream de prg-programaciones (PP-03: debe quedar uno solo)"
  for f in lambdas/config/*.triggers.json; do
    jq -r --arg f "$(basename "$f" .triggers.json)" '.EventSourceMappings[]? | select(.EventSourceArn|test("prg-programaciones"))
      | "\($f): \(.State) filtro=\(.FilterCriteria.Filters // [] | length)"' "$f" 2>/dev/null
  done
  echo
  echo "## SES"
  jq -r '"Producción habilitada: \(.ProductionAccessEnabled)  Envío habilitado: \(.SendingEnabled)  Cuota 24h: \(.SendQuota.Max24HourSend)"' ses/cuenta-v2.json 2>/dev/null
  jq -r '.EmailIdentities[]? | "Identidad: \(.IdentityName) (\(.IdentityType)) verificada=\(.VerificationStatus // .SendingEnabled)"' ses/identidades-v2.json 2>/dev/null
  echo
  echo "## Schedules de configuradores"
  jq -r '.Schedules[]? | "\(.Name) \(.State) -> \(.Target.Arn | split(":") | last)"' scheduler/niif-dev-programacion-configuradores.lista.json 2>/dev/null
  echo
  echo "## Buckets que no existen o sin acceso"
  for b in $BUCKETS; do [ -f "buckets/$b.existe.json" ] || echo "$b"; done
  echo
  echo "## Errores registrados: $(grep -c '^FALLO' "$LOG")  (detalle en _errores.log)"
} | tee _resumen.txt

cd "$HOME" && rm -f niif-faltantes.zip && zip -qr niif-faltantes.zip niif-faltantes && rm -rf "$TMP"
echo
echo "Listo: ~/niif-faltantes.zip ($(du -h niif-faltantes.zip | cut -f1)). Descargar con Actions > Download file: /home/cloudshell-user/niif-faltantes.zip"
