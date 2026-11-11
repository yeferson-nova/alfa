#!/usr/bin/env bash
# ============================================================================
# Caso 9411325 (inprod): el enlace de certificacion abre
#   https://datalake-%20prd-niif.auth.us-east-1.amazoncognito.com/...
# y el navegador dice "no se puede obtener acceso a esta pagina" (DNS).
#
# Solo LEE: describe, get, list, lookup-events y scan con filtro (sin escribir).
# No imprime tokens ni correos: de los enlaces solo guarda host, client_id,
# redirect_uri, scope y el metodo del state. No guarda los JSON completos.
#
# Uso en AWS CloudShell (us-east-1):
#   1. Actions > Upload file: este archivo.
#   2. sed -i 's/\r$//' evidencia-enlaces-cognito.sh
#   3. ENV=prd bash evidencia-enlaces-cognito.sh      (en DEV: ENV=dev)
#   4. Descargar el .tar.gz que imprime al final.
#
# Variables opcionales: ENV=prd REGION=us-east-1 DIAS_AUDITORIA=90 DIAS_LOGS=14
#   ASSETS_BUCKET (defecto niif-$ENV-assets)  PREFIJO_MALLAS (defecto vacio = todo)
# ============================================================================
set -u

ENV="${ENV:-prd}"
REGION="${REGION:-us-east-1}"
DIAS_AUDITORIA="${DIAS_AUDITORIA:-90}"
DIAS_LOGS="${DIAS_LOGS:-14}"
ASSETS_BUCKET="${ASSETS_BUCKET:-niif-${ENV}-assets}"
BUCKET_ESCRITO_EN_CORREO="niif-${ENV}-assests"
PREFIJO_MALLAS="${PREFIJO_MALLAS:-}"

export AWS_DEFAULT_REGION="$REGION" AWS_PAGER=""

SALIDA="evidencia-enlaces-cognito-${ENV}"
rm -rf "$SALIDA" "${SALIDA}.tar.gz"
mkdir -p "$SALIDA"/{s3,cognito,apigw,lambdas,dynamodb,auditoria,glue,dns}
RESUMEN="$SALIDA/00-resumen.txt"
FALLOS="$SALIDA/00-fallos.txt"
: > "$RESUMEN"; : > "$FALLOS"

log() { echo "$*" | tee -a "$RESUMEN"; }
fallo() { echo "[FALLO] $*" | tee -a "$FALLOS" >&2; }

# Ejecuta un comando AWS y guarda su salida; si falla lo anota y sigue.
guardar() {
  local destino="$1"; shift
  if ! "$@" > "$destino" 2> "${destino}.err"; then
    fallo "$* -> $(head -c 300 "${destino}.err" | tr '\n' ' ')"
  fi
  [ -s "${destino}.err" ] || rm -f "${destino}.err"
}

log "== Evidencia enlaces Cognito | ENV=$ENV | $(date -u +%Y-%m-%dT%H:%M:%SZ) =="
guardar "$SALIDA/00-identidad.json" aws sts get-caller-identity
log "Cuenta/rol: ver 00-identidad.json (no se imprime aqui)"
ENV_MAYUS="$(echo "$ENV" | tr '[:lower:]' '[:upper:]')"
if ! grep -q "ALFA-${ENV_MAYUS}-" "$SALIDA/00-identidad.json" 2>/dev/null; then
  log "!! ATENCION: el rol de esta sesion no parece de ${ENV_MAYUS}; ENV no coincide con la cuenta. Se detiene."
  exit 1
fi

# ----------------------------------------------------------------------------
# 1. S3: bucket correcto vs el escrito en el correo, y versiones de los JSON
# ----------------------------------------------------------------------------
log ""
log "## 1. Buckets"
for b in "$ASSETS_BUCKET" "$BUCKET_ESCRITO_EN_CORREO"; do
  if aws s3api head-bucket --bucket "$b" 2>/dev/null; then
    log "  $b : EXISTE y es accesible"
  else
    log "  $b : NO existe o sin acceso"
  fi
done

guardar "$SALIDA/s3/listado-json.json" aws s3api list-objects-v2 \
  --bucket "$ASSETS_BUCKET" --prefix "$PREFIJO_MALLAS" \
  --query "Contents[?ends_with(Key,'.json')].[Key,LastModified,Size]" --output json

# JSON de las mallas con enlaces: se descargan a un temporal fuera de la salida
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
python3 - "$SALIDA/s3/listado-json.json" > "$TMP/claves.txt" <<'PY'
import json, sys
try:
    filas = json.load(open(sys.argv[1]))
except Exception:
    filas = []
for fila in filas or []:
    clave = fila[0]
    if "/" in clave and not clave.endswith("/"):
        print(clave)
PY

log ""
log "## 2. Versiones de cada JSON (quien lo toco y cuando se ve en CloudTrail; aqui solo fechas)"
: > "$SALIDA/s3/versiones.tsv"
printf "clave\tversion_id\tes_ultima\tfecha\ttamano\n" >> "$SALIDA/s3/versiones.tsv"
while read -r clave; do
  [ -z "$clave" ] && continue
  aws s3api list-object-versions --bucket "$ASSETS_BUCKET" --prefix "$clave" --max-items 10 \
    --query "Versions[?Key=='$clave'].[Key,VersionId,IsLatest,LastModified,Size]" --output text 2>/dev/null \
    >> "$SALIDA/s3/versiones.tsv" || fallo "list-object-versions $clave"
done < "$TMP/claves.txt"
log "  ver s3/versiones.tsv (si el bucket no tiene versionado, solo sale la version actual)"

# ----------------------------------------------------------------------------
# 3. Enlaces Certificar / No certificar de cada JSON (analisis local)
# ----------------------------------------------------------------------------
log ""
log "## 3. Enlaces de certificacion por JSON (host, client_id, redirect_uri)"
mkdir -p "$TMP/json"
while read -r clave; do
  [ -z "$clave" ] && continue
  dest="$TMP/json/$(echo "$clave" | tr '/' '__')"
  aws s3 cp "s3://${ASSETS_BUCKET}/${clave}" "$dest" --only-show-errors 2>>"$FALLOS" || fallo "s3 cp $clave"
done < "$TMP/claves.txt"

python3 - "$TMP/json" "$SALIDA/s3/enlaces.tsv" "$SALIDA/s3/hosts-unicos.txt" <<'PY' | tee -a "$RESUMEN"
import json, os, re, sys
from urllib.parse import urlparse, parse_qs, unquote

carpeta, salida_tsv, salida_hosts = sys.argv[1:4]
CLAVES = ("certificarLink", "nocertificarLink")
filas, hosts = [], set()

def buscar(obj, ruta=""):
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in CLAVES and isinstance(v, str):
                yield ruta + "." + k if ruta else k, v
            else:
                yield from buscar(v, f"{ruta}.{k}" if ruta else k)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            yield from buscar(v, f"{ruta}[{i}]")

for nombre in sorted(os.listdir(carpeta)):
    try:
        datos = json.load(open(os.path.join(carpeta, nombre), encoding="utf-8"))
    except Exception:
        continue
    for ruta, url in buscar(datos):
        crudo = url
        u = urlparse(url.strip())
        host = u.netloc
        q = parse_qs(u.query)
        try:
            estado = json.loads(unquote(q.get("state", [""])[0]))
            metodo = estado.get("method", "?")
        except Exception:
            metodo = "?"
        problemas = []
        if url != url.strip():
            problemas.append("ESPACIO_EXTREMOS")
        if re.search(r"\s|%20", host):
            problemas.append("ESPACIO_EN_HOST")
        if not re.fullmatch(r"[a-z0-9.-]+", host):
            problemas.append("HOST_CARACTER_INVALIDO")
        if not host.endswith(".amazoncognito.com") and "amazoncognito" not in host:
            problemas.append("HOST_NO_COGNITO")
        if not u.scheme == "https":
            problemas.append("NO_HTTPS")
        filas.append((nombre, ruta, host, q.get("client_id", ["?"])[0],
                      unquote(q.get("redirect_uri", ["?"])[0]), metodo,
                      ",".join(problemas) or "OK"))
        hosts.add(host)

with open(salida_tsv, "w", encoding="utf-8") as f:
    f.write("json\truta\thost\tclient_id\tredirect_uri\tmetodo_state\testado\n")
    for fila in filas:
        f.write("\t".join(fila) + "\n")
with open(salida_hosts, "w", encoding="utf-8") as f:
    for h in sorted(hosts):
        f.write(h + "\n")

print(f"  JSON con enlaces: {len({fila[0] for fila in filas})} | enlaces: {len(filas)}")
malos = [fila for fila in filas if fila[6] != "OK"]
print(f"  Enlaces con problema de formato: {len(malos)}")
for fila in malos:
    print(f"    !! {fila[0]} :: {fila[1]} :: host=[{fila[2]!r}] :: {fila[6]}")
print("  Hosts distintos encontrados (repr muestra espacios ocultos):")
for h in sorted(hosts):
    print(f"    {h!r}")
print("  Combinaciones host/client_id/redirect_uri distintas:")
for combo in sorted({(f[2], f[3], f[4]) for f in filas}):
    print(f"    {combo[0]!r} | {combo[1]} | {combo[2]}")
PY

# ----------------------------------------------------------------------------
# 4. DNS: el host del enlace resuelve? y el corregido (sin espacio)?
# ----------------------------------------------------------------------------
log ""
log "## 4. DNS de los hosts de los enlaces"
python3 - "$SALIDA/s3/hosts-unicos.txt" "$SALIDA/dns/resolucion.txt" <<'PY' | tee -a "$RESUMEN"
import re, socket, sys
hosts = [l.rstrip("\n") for l in open(sys.argv[1], encoding="utf-8") if l.strip()]
candidatos = []
for h in hosts:
    candidatos.append(h)
    limpio = re.sub(r"\s|%20", "", h)
    if limpio != h:
        candidatos.append(limpio)
vistos, salida = set(), []
for h in candidatos:
    if h in vistos:
        continue
    vistos.add(h)
    try:
        ips = sorted({r[4][0] for r in socket.getaddrinfo(h, 443)})
        linea = f"  {h!r}: RESUELVE ({len(ips)} IP)"
    except Exception as e:
        linea = f"  {h!r}: NO RESUELVE ({type(e).__name__})"
    print(linea)
    salida.append(linea)
open(sys.argv[2], "w", encoding="utf-8").write("\n".join(salida) + "\n")
PY

# ----------------------------------------------------------------------------
# 5. Cognito: user pools, dominios, clientes, callbacks, proveedores
# ----------------------------------------------------------------------------
log ""
log "## 5. Cognito"
guardar "$SALIDA/cognito/user-pools.json" aws cognito-idp list-user-pools --max-results 60
POOLS="$(python3 - "$SALIDA/cognito/user-pools.json" <<'PY'
import json, sys
try:
    for p in json.load(open(sys.argv[1])).get("UserPools", []):
        print(p["Id"], p["Name"])
except Exception:
    pass
PY
)"
echo "$POOLS" | while read -r pid pnombre; do
  [ -z "$pid" ] && continue
  log "  pool: $pnombre ($pid)"
  guardar "$SALIDA/cognito/pool-${pid}.json" aws cognito-idp describe-user-pool --user-pool-id "$pid" \
    --query "UserPool.{Id:Id,Name:Name,Domain:Domain,CustomDomain:CustomDomain,Status:Status,LastModifiedDate:LastModifiedDate,CreationDate:CreationDate}"
  guardar "$SALIDA/cognito/idp-${pid}.json" aws cognito-idp list-identity-providers --user-pool-id "$pid" \
    --query "Providers[].{Name:ProviderName,Type:ProviderType,LastModified:LastModifiedDate}"
  guardar "$SALIDA/cognito/clientes-${pid}.json" aws cognito-idp list-user-pool-clients --user-pool-id "$pid" --max-results 60
  for cid in $(python3 - "$SALIDA/cognito/clientes-${pid}.json" <<'PY'
import json, sys
try:
    for c in json.load(open(sys.argv[1])).get("UserPoolClients", []):
        print(c["ClientId"])
except Exception:
    pass
PY
); do
    guardar "$SALIDA/cognito/cliente-${pid}-${cid}.json" aws cognito-idp describe-user-pool-client \
      --user-pool-id "$pid" --client-id "$cid" \
      --query "UserPoolClient.{ClientName:ClientName,CallbackURLs:CallbackURLs,LogoutURLs:LogoutURLs,AllowedOAuthFlows:AllowedOAuthFlows,AllowedOAuthScopes:AllowedOAuthScopes,AllowedOAuthFlowsUserPoolClient:AllowedOAuthFlowsUserPoolClient,SupportedIdentityProviders:SupportedIdentityProviders,LastModifiedDate:LastModifiedDate}"
  done
done

# Dominios: el declarado por cada pool y los de los enlaces (con y sin espacio)
log ""
log "  Dominios de Cognito (prefijo) a comprobar:"
python3 - "$SALIDA/s3/hosts-unicos.txt" "$SALIDA/cognito" <<'PY' > "$TMP/dominios.txt"
import glob, json, re, sys
doms = set()
for h in open(sys.argv[1], encoding="utf-8"):
    h = h.strip()
    if not h:
        continue
    for variante in (h, re.sub(r"\s|%20", "", h)):
        pref = variante.split(".auth.")[0]
        if pref:
            doms.add(pref)
for ruta in glob.glob(sys.argv[2] + "/pool-*.json"):
    try:
        d = json.load(open(ruta)).get("Domain")
        if d:
            doms.add(d)
    except Exception:
        pass
for d in sorted(doms):
    print(d)
PY
while read -r dom; do
  [ -z "$dom" ] && continue
  seguro="$(echo "$dom" | tr -c 'A-Za-z0-9._-' '_')"
  salida_dom="$SALIDA/cognito/dominio-${seguro}.json"
  if aws cognito-idp describe-user-pool-domain --domain "$dom" \
      --query "DomainDescription.{Domain:Domain,Status:Status,UserPoolId:UserPoolId,Version:Version}" \
      > "$salida_dom" 2>"${salida_dom}.err"; then
    log "    [$dom] -> $(tr -d '\n ' < "$salida_dom")"
  else
    log "    [$dom] -> ERROR/NO EXISTE: $(head -c 160 "${salida_dom}.err" | tr '\n' ' ')"
  fi
done < "$TMP/dominios.txt"

# ----------------------------------------------------------------------------
# 6. API Gateway (post_login, certify, not_certify, autorizador, WAF)
# ----------------------------------------------------------------------------
log ""
log "## 6. API Gateway"
guardar "$SALIDA/apigw/apis.json" aws apigateway get-rest-apis --limit 100 \
  --query "items[].{id:id,name:name,createdDate:createdDate,endpoint:endpointConfiguration.types}"
for api in $(python3 - "$SALIDA/apigw/apis.json" <<'PY'
import json, sys
try:
    for a in json.load(open(sys.argv[1])):
        n = (a.get("name") or "").lower()
        if "niif" in n or "certific" in n:
            print(a["id"])
except Exception:
    pass
PY
); do
  log "  api: $api"
  guardar "$SALIDA/apigw/${api}-stages.json" aws apigateway get-stages --rest-api-id "$api" \
    --query "item[].{stage:stageName,deployment:deploymentId,webAcl:webAclArn,lastUpdated:lastUpdatedDate}"
  guardar "$SALIDA/apigw/${api}-authorizers.json" aws apigateway get-authorizers --rest-api-id "$api" \
    --query "items[].{name:name,type:type,providerARNs:providerARNs,identitySource:identitySource}"
  guardar "$SALIDA/apigw/${api}-resources.json" aws apigateway get-resources --rest-api-id "$api" --embed methods \
    --query "items[].{path:path,methods:keys(resourceMethods || `{}`)}"
done

# ----------------------------------------------------------------------------
# 6b. Logs de acceso del API y reglas del WAF (llegaron usuarios a /post_login?)
#     Solo cuenta por ruta, estado HTTP y dia; no guarda IP, usuario ni agente.
# ----------------------------------------------------------------------------
log ""
log "## 6b. Logs de acceso del API (ultimos ${DIAS_LOGS} dias) y WAF"
for stagefile in "$SALIDA"/apigw/*-stages.json; do
  [ -f "$stagefile" ] || continue
  api="$(basename "$stagefile" -stages.json)"
  python3 - "$stagefile" <<'PY' > "$TMP/stages-${api}.txt"
import json, sys
try:
    etapas = json.load(open(sys.argv[1])) or []
except Exception:
    etapas = []
for e in etapas:
    print(e.get("stage") or "", e.get("webAcl") or "-", sep="\t")
PY
  while IFS=$'\t' read -r stage webacl; do
    [ -z "$stage" ] && continue
    grupo="$(aws apigateway get-stage --rest-api-id "$api" --stage-name "$stage" \
      --query "accessLogSettings.destinationArn" --output text 2>/dev/null | sed 's/.*log-group://;s/:\*$//')"
    log "  api=$api stage=$stage log_group=${grupo:-NINGUNO} waf=$( [ "$webacl" = "-" ] && echo NO || echo SI )"
    if [ -n "$grupo" ] && [ "$grupo" != "None" ]; then
      DESDE_MS="$(( ($(date -u +%s) - DIAS_LOGS*86400) * 1000 ))"
      aws logs filter-log-events --log-group-name "$grupo" --start-time "$DESDE_MS" \
        --filter-pattern '"post_login"' --max-items 2000 --output json 2>>"$FALLOS" \
        > "$TMP/logs-${api}-${stage}.json" || fallo "filter-log-events $grupo"
      python3 - "$TMP/logs-${api}-${stage}.json" "$SALIDA/apigw/${api}-${stage}-post_login-conteo.tsv" <<'PY' | tee -a "$RESUMEN"
import collections, json, re, sys
cuenta = collections.Counter()
try:
    eventos = json.load(open(sys.argv[1])).get("events", [])
except Exception:
    eventos = []
for ev in eventos:
    try:
        d = json.loads(ev["message"])
    except Exception:
        continue
    dia = str(d.get("requestTime", ""))[:11]
    cuenta[(dia, d.get("httpMethod"), d.get("resourcePath"), d.get("status"))] += 1
with open(sys.argv[2], "w", encoding="utf-8") as f:
    f.write("dia\tmetodo\truta\testado\tcantidad\n")
    for (dia, m, r, st), n in sorted(cuenta.items()):
        f.write(f"{dia}\t{m}\t{r}\t{st}\t{n}\n")
print(f"    eventos /post_login leidos: {len(eventos)} | combinaciones: {len(cuenta)} (ver apigw/*-conteo.tsv)")
PY
    fi
    if [ "$webacl" != "-" ]; then
      nombre="$(echo "$webacl" | awk -F/ '{print $(NF-1)}')"; ident="$(echo "$webacl" | awk -F/ '{print $NF}')"
      guardar "$SALIDA/apigw/${api}-${stage}-waf.json" aws wafv2 get-web-acl --scope REGIONAL --name "$nombre" --id "$ident" \
        --query "WebACL.{Name:Name,DefaultAction:DefaultAction,Rules:Rules[].{Name:Name,Priority:Priority,Action:Action,Override:OverrideAction}}"
      guardar "$SALIDA/apigw/${api}-${stage}-waf-logging.json" aws wafv2 get-logging-configuration --resource-arn "$webacl" \
        --query "LoggingConfiguration.{Destinos:LogDestinationConfigs}"
    fi
  done < "$TMP/stages-${api}.txt"
done

# ----------------------------------------------------------------------------
# 7. Lambdas de la cadena (solo configuracion; variables relevantes)
# ----------------------------------------------------------------------------
log ""
log "## 7. Lambdas relacionadas"
guardar "$SALIDA/lambdas/lista.json" aws lambda list-functions --max-items 200 \
  --query "Functions[?contains(FunctionName,'niif')].{n:FunctionName,r:Runtime,m:LastModified}"
for fn in $(python3 - "$SALIDA/lambdas/lista.json" <<'PY'
import json, sys
try:
    for f in json.load(open(sys.argv[1])):
        n = f["n"]
        if any(x in n for x in ("post-login", "post_login", "postLogin", "certification", "short-link", "api")):
            print(n)
except Exception:
    pass
PY
); do
  seguro="$(echo "$fn" | tr -c 'A-Za-z0-9._-' '_')"
  aws lambda get-function-configuration --function-name "$fn" --output json 2>>"$FALLOS" | python3 -c '
import json, re, sys
try:
    c = json.load(sys.stdin)
except Exception:
    sys.exit(0)
env = (c.get("Environment") or {}).get("Variables") or {}
seguras = {}
for k, v in env.items():
    if re.search(r"COGNITO|DOMAIN|REDIRECT|CLIENT_ID|URL|API|STAGE|TABLE|BUCKET|ENV", k, re.I) \
            and not re.search(r"SECRET|TOKEN|PASSWORD|KEY", k, re.I):
        seguras[k] = v
    else:
        seguras[k] = "<oculto>"
print(json.dumps({"FunctionName": c.get("FunctionName"), "Runtime": c.get("Runtime"),
                  "LastModified": c.get("LastModified"), "Handler": c.get("Handler"),
                  "Variables": seguras}, indent=2))
' > "$SALIDA/lambdas/${seguro}.json"
done
log "  ver lambdas/*.json (valores solo de variables de dominio, URL, tabla, bucket)"

# ----------------------------------------------------------------------------
# 8. DynamoDB de enlaces cortos: hay URL guardadas con espacio?
#    (solo cuenta y muestra caso/accion/fecha; nunca el token ni la URL)
# ----------------------------------------------------------------------------
log ""
log "## 8. Enlaces cortos con espacio en el host (patron -%20 o "- "; el %20 del scope es normal)"
for tabla in $(aws dynamodb list-tables --query "TableNames[?contains(@,'short')]" --output text 2>/dev/null); do
  guardar "$SALIDA/dynamodb/${tabla}-cuenta.json" aws dynamodb scan --table-name "$tabla" \
    --filter-expression "contains(#u, :a) OR contains(#u, :b)" \
    --expression-attribute-names '{"#u":"url"}' \
    --expression-attribute-values '{":a":{"S":"-%20"},":b":{"S":"- "}}' --select COUNT
  guardar "$SALIDA/dynamodb/${tabla}-muestra.json" aws dynamodb scan --table-name "$tabla" \
    --filter-expression "contains(#u, :a) OR contains(#u, :b)" \
    --expression-attribute-names '{"#u":"url","#a":"action","#c":"creationDate"}' \
    --expression-attribute-values '{":a":{"S":"-%20"},":b":{"S":"- "}}' \
    --projection-expression "useCaseId,#a,#c" --max-items 50
  log "  $tabla: $(tr -d '\n ' < "$SALIDA/dynamodb/${tabla}-cuenta.json" 2>/dev/null)"
done

# ----------------------------------------------------------------------------
# 9. Glue: ultimas corridas del job de notificacion (que JSON de config usaron)
# ----------------------------------------------------------------------------
log ""
log "## 9. Corridas recientes del job de notificacion"
JOB_NOTIF="niif-${ENV}-notification-certification"
aws glue get-job-runs --job-name "$JOB_NOTIF" --max-results 20 --output json 2>>"$FALLOS" | python3 -c '
import json, re, sys
try:
    runs = json.load(sys.stdin).get("JobRuns", [])
except Exception:
    runs = []
for r in runs:
    args = {k: v for k, v in (r.get("Arguments") or {}).items()
            if re.search(r"CONFIG|RUTA|MALLA|EJECUCION|CASO|PERIODO", k, re.I)}
    print(json.dumps({"inicio": str(r.get("StartedOn")), "estado": r.get("JobRunState"),
                      "args": args}))
' > "$SALIDA/glue/notificacion-corridas.jsonl"
log "  ver glue/notificacion-corridas.jsonl"

# ----------------------------------------------------------------------------
# 10. CloudTrail (eventos de gestion): cambios a Cognito, API GW y lambdas
# ----------------------------------------------------------------------------
log ""
log "## 10. Cambios recientes de gestion (CloudTrail, ${DIAS_AUDITORIA} dias; sin identidad)"
INICIO="$(date -u -d "-${DIAS_AUDITORIA} days" +%Y-%m-%dT%H:%M:%SZ)"
for fuente in cognito-idp.amazonaws.com apigateway.amazonaws.com; do
  aws cloudtrail lookup-events --start-time "$INICIO" \
    --lookup-attributes AttributeKey=EventSource,AttributeValue="$fuente" --max-results 50 --output json \
    2>>"$FALLOS" | python3 -c '
import json, sys
try:
    ev = json.load(sys.stdin).get("Events", [])
except Exception:
    ev = []
lectura = ("Get", "List", "Describe")
for e in ev:
    if e["EventName"].startswith(lectura):
        continue
    print(str(e["EventTime"]) + "	" + e["EventName"])
' > "$SALIDA/auditoria/${fuente%%.*}.tsv"
  log "  $fuente: $(wc -l < "$SALIDA/auditoria/${fuente%%.*}.tsv") eventos de escritura (ver auditoria/)"
done
aws cloudtrail lookup-events --start-time "$INICIO" \
  --lookup-attributes AttributeKey=EventName,AttributeValue=UpdateFunctionConfiguration20150331v2 --max-results 50 \
  --output json 2>>"$FALLOS" | python3 -c '
import json, sys
try:
    ev = json.load(sys.stdin).get("Events", [])
except Exception:
    ev = []
for e in ev:
    print(str(e["EventTime"]) + "	" + e["EventName"])
' > "$SALIDA/auditoria/lambda-config.tsv"

# ----------------------------------------------------------------------------
# Cierre
# ----------------------------------------------------------------------------
log ""
log "## Fallos de comandos: $(wc -l < "$FALLOS") (ver 00-fallos.txt; AccessDenied esta bien, solo se anota)"
tar -czf "${SALIDA}.tar.gz" "$SALIDA"
echo
echo "Listo. Descargar: ${SALIDA}.tar.gz  (Actions > Download file)"
echo "Lo primero a mirar: 00-resumen.txt (secciones 3, 4 y 5)."
