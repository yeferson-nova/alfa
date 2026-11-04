"""Muestra, con los registros que YA tenemos en la evidencia (sin acceder a AWS), la linea de
tiempo del incidente Primas ARL 202606: Step Function, corridas de Glue, log de la lambda y
las metricas que escribieron los jobs.

Uso: python -I ver-logs-evidencia.py <carpeta evidencia-primas-arl-202606>
"""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

if len(sys.argv) != 2:
    sys.exit("Uso: python -I ver-logs-evidencia.py <carpeta-evidencia>")
base = Path(sys.argv[1])


def cargar(ruta):
    with open(base / ruta, encoding="utf-8") as f:
        return json.load(f)


def utc(texto):
    return datetime.fromisoformat(texto).astimezone(timezone.utc)


def hora(dt):
    return dt.strftime("%H:%M:%S")


ejec = cargar("stepfunction/01-estado-y-entrada.json")
ini, fin = utc(ejec["startDate"]), utc(ejec["stopDate"])
print(f"Ejecucion Step Function: estado={ejec['status']}  {ini:%Y-%m-%d} {hora(ini)} -> {hora(fin)} UTC\n")

# A. Step Function: estados que entran (de la evidencia 02-estados-entrada-salida.json)
print("A. STEP FUNCTION - estados en orden (hora UTC)")
estados = cargar("stepfunction/02-estados-entrada-salida.json")
for e in estados:
    if e["type"].endswith("StateEntered") and e["estado"]:
        print(f"   {hora(utc(e['timestamp']))}  {e['estado']}")

# B. Corridas de Glue dentro de la ventana de la ejecucion
print("\nB. GLUE - corridas dentro de la ejecucion (de glue/job-runs-relevantes.jsonl)")
print(f"   {'job':40s} {'estado':10s} {'inicio':9s} {'fin':9s} {'dur(s)':>6s}  id de corrida")
with open(base / "glue/job-runs-relevantes.jsonl", encoding="utf-8") as f:
    corridas = [json.loads(linea) for linea in f if linea.strip()]
corridas = [c for c in corridas if ini <= utc(c["inicio"]) <= fin]
for c in sorted(corridas, key=lambda c: c["inicio"]):
    dur = int((utc(c["fin"]) - utc(c["inicio"])).total_seconds()) if c.get("fin") else -1
    print(f"   {c['job']:40s} {c['estado']:10s} {hora(utc(c['inicio'])):9s} {hora(utc(c['fin'])) if c.get('fin') else '-':9s} {dur:>6d}  {c['id'][:20]}...")

# C. Log de la lambda evaluadora
print("\nC. LAMBDA composite-notification-evaluator (de logs/lambda-...json)")
lambda_log = cargar("logs/lambda-niif-prd-composite-notification-evaluator.json")
for ev in lambda_log.get("events", []):
    t = datetime.fromtimestamp(ev["timestamp"] / 1000, tz=timezone.utc)
    msg = ev["message"].strip().split("\n")[0]
    if msg.startswith(("INIT_START", "END ", "REPORT")):
        continue
    print(f"   {hora(t)}  {msg[:230]}")

# D. Metricas que escribieron los jobs en parametros_extraccion.json
print("\nD. METRICAS ESCRITAS POR LOS JOBS (de s3/parametros_extraccion_primas_arl.json)")
p = cargar("s3/parametros_extraccion_primas_arl.json")
me, mg = p["metricas_extraccion"], p["metricas_gold_scd"]
endosos = next(d for d in p["datasets_extraidos"] if d["tabla_relativa"] == "arpprod/par_endosos")
seg = endosos["segmentacion_horizontal"]
print(f"   Extraccion : registros extraidos={me['total_registros_extraidos']:,}  excluidos par_endosos={seg['registros_excluidos_por_segmento']:,}  llaves nuevas={endosos['total_llaves_segmento']}")
print(f"   Validacion : estado={p['estado_validacion']}  errores={p['total_errores_validacion']}  (el flujo continua por diseno)")
print(f"   Silver     : registros preparados={p['total_registros_silver_preparada']}  modo={p['modo_escritura_silver_preparada']}")
print(f"   Gold SCD   : entrada={mg['total_entrada_silver_preparada']}  insertados={mg['total_insertados_nuevos']}  "
      f"apagados por ausencia={mg['total_registros_apagados_por_ausencia']:,}  gold anterior={mg['total_gold_anterior']:,}  gold final={mg['total_gold_final']:,}")

# E. Lo que NO esta en los logs capturados
print("\nE. LIMITES DE LO CAPTURADO")
vacios = [x.name for x in sorted((base / "logs").glob("*certification*")) if x.stat().st_size <= 2]
print(f"   - Logs de salida de Glue vacios (no capturados): {len(vacios)} archivos en logs/ (error de credenciales en CloudShell).")
print("   - Por eso NO hay lineas [TABLES]/[ATHENA] del job de notificacion ni los 'Registros apagados por ausencia' en texto de log.")
print("   - Esas cifras si estan confirmadas por las metricas de D y por el CSV de Athena.")
