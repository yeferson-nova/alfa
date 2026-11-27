#!/usr/bin/env bash
# Prueba en DEV de la version modular del proceso de extraccion de Emisiones.
#   bash probar-despliegue-dev.sh                  -> verificar (solo lectura)
#   bash probar-despliegue-dev.sh subir-paquete modulos.zip -> sube el paquete de prueba (no cambia el job)
#   bash probar-despliegue-dev.sh ejecutar [ID]    -> verificar, respaldar, configurar y lanzar el job
#                                                     (si el job no tiene --extra-py-files, usa el paquete de prueba solo en esa corrida)
#   bash probar-despliegue-dev.sh ejecutar-sombra [ID] -> igual que ejecutar, y antes de lanzar agrega a cada JSON los bloques
#                                                     filtro_spark en modo SOMBRA (necesita filtro_spark_bloques.py y
#                                                     tamanos-oracle-acsele.json en la carpeta actual)
#   bash probar-despliegue-dev.sh ejecutar-concurrencia [ID] -> igual que ejecutar, y antes de lanzar agrega a control_extraccion
#                                                     de cada JSON los valores de concurrencia de prueba (concurrencia-prueba.json
#                                                     en la carpeta actual, o los valores por defecto de abajo)
#   bash probar-despliegue-dev.sh ejecutar-diagnostico [ID] -> sombra + concurrencia + medir_lectura en la misma corrida
#                                                     (equivalencia y lectura/escritura por tabla; el tiempo total no sirve de medida)
#   bash probar-despliegue-dev.sh seguir <JobRunId> -> estado, eventos y comparacion con el respaldo
# Salida: probar-despliegue-dev-<modo>-<fecha>.txt -> guardarla en ../salidas/
export AWS_PAGER=""
export AWS_REGION=us-east-1 AWS_DEFAULT_REGION=us-east-1
MODO="${1:-verificar}"
OUT="probar-despliegue-dev-${MODO}-$(date +%Y%m%d-%H%M).txt"
python3 - "$@" 2>&1 <<'EOF' | sed -E 's/(^|[^0-9])[0-9]{12}([^0-9]|$)/\1************\2/g' | tee "$OUT"
import hashlib, io, json, os, sys, zipfile
from datetime import datetime, timezone
import boto3

JOB = "niif-dev-extraccion-estandar-emisiones"
CONFIGURADORA = "niif-dev-configuration-general"
MAQUINA = "niif-dev-emisiones"
MARCA_MODULAR = "emissions_extraction.config.composition"
MODULO_CLAVE = "emissions_extraction/config/composition.py"
GRUPO_SALIDA = "/aws-glue/jobs/output"
GRUPO_ERROR = "/aws-glue/jobs/error"
CARPETA_PRUEBAS = "pruebas-modular"
CARPETA_PAQUETE_MANUAL = "pruebas-manuales"
NOMBRE_PAQUETE = "modulos.zip"
EVENTOS_CLAVE = ("EJECUCION_ID_RESUELTO", "EMISIONES_INICIO", "CASO_EXTRACCION_OK",
                 "EMISIONES_EXTRACCION_OK", "ADVERTENCIA_NUCLEO_COMUN_SQL_DIFERENTE", "CONCURRENCIA_APLICADA",
                 "ADVERTENCIA_METRICAS_NO_PUBLICADAS")
ARCHIVO_CONCURRENCIA = "concurrencia-prueba.json"
# Casos en paralelo y 3 tablas a la vez tambien en las fases CASCADA_POR_FASE (las tablas de rama heredan la forma
# del segmento). El nucleo y la fase CASCADA de PCR Vida siguen de a una. nucleo_en_paralelo solo tiene efecto con un
# paquete que lo incluya; las versiones anteriores lo ignoran.
# Tope de 9 sesiones: el pico de hoy (3 tablas de PCR Vida, 2 de ellas con 4 particiones).
CONCURRENCIA_PRUEBA = {
    "casos_en_paralelo": True,
    "nucleo_en_paralelo": True,
    "modos_paralelos": ["PARALELO", "PARALLEL", "CASCADA_POR_FASE"],
    "max_concurrencia_jdbc": 3,
    "max_sesiones_oracle": 9,
}

args = sys.argv[1:]
modo = args[0] if args else "verificar"
glue, s3 = boto3.client("glue"), boto3.client("s3")
sfn, lam, logs = boto3.client("stepfunctions"), boto3.client("lambda"), boto3.client("logs")


def partir(uri):
    sin = uri.replace("s3://", "", 1)
    return sin.split("/", 1)[0], sin.split("/", 1)[1]


def leer_s3(uri):
    b, k = partir(uri)
    return s3.get_object(Bucket=b, Key=k)["Body"].read()


def sha(data):
    return hashlib.sha256(data).hexdigest()[:12]


def veredicto(ok, texto):
    print(("PASS " if ok else "FAIL ") + texto)
    return ok


def uri_paquete_manual(script):
    return script.rsplit("/", 1)[0] + f"/{CARPETA_PAQUETE_MANUAL}/{NOMBRE_PAQUETE}"


def revisar_zip(uri, data):
    z = zipfile.ZipFile(io.BytesIO(data))
    tiene = MODULO_CLAVE in z.namelist()
    print("  ", uri, "->", "contiene el paquete" if tiene else "no contiene el paquete", "| sha256:", sha(data))
    if tiene:
        for n in sorted(n for n in z.namelist() if n.endswith(".py")):
            print("     ", n, sha(z.read(n)))
    return tiene


def subir_paquete():
    if len(args) < 2 or not os.path.isfile(args[1]):
        print("Uso: bash probar-despliegue-dev.sh subir-paquete modulos.zip (el zip en la carpeta actual)")
        return
    data = open(args[1], "rb").read()
    print("== Paquete local", args[1])
    if not revisar_zip(args[1], data):
        print("FAIL el zip no trae", MODULO_CLAVE, "en la raiz; no se sube.")
        return
    script = glue.get_job(JobName=JOB)["Job"]["Command"]["ScriptLocation"]
    destino = uri_paquete_manual(script)
    b, k = partir(destino)
    try:
        s3.put_object(Bucket=b, Key=k, Body=data)
    except Exception as exc:
        print("FAIL no se pudo subir:", type(exc).__name__, str(exc)[:200])
        return
    veredicto(sha(leer_s3(destino)) == sha(data), f"paquete de prueba en {destino}")
    print("La definicion del job no se toca. Siguiente: bash probar-despliegue-dev.sh ejecutar")


def verificar():
    print("== 1. Definicion del job", JOB)
    job = glue.get_job(JobName=JOB)["Job"]
    da = job.get("DefaultArguments", {})
    script = job["Command"]["ScriptLocation"]
    print("glue:", job.get("GlueVersion"), "| worker:", job.get("WorkerType"), "x", job.get("NumberOfWorkers"),
          "| timeout:", job.get("Timeout"), "| concurrencia:", job.get("ExecutionProperty", {}).get("MaxConcurrentRuns"))
    print("script:", script)
    print("--extra-py-files:", da.get("--extra-py-files", "(no tiene)"))
    print("--additional-python-modules:", da.get("--additional-python-modules", "(no tiene)"))

    print("\n== 2. Script desplegado")
    b, k = partir(script)
    meta = s3.head_object(Bucket=b, Key=k)
    cuerpo = leer_s3(script)
    modular = MARCA_MODULAR.encode() in cuerpo
    print("modificado:", meta["LastModified"].isoformat(), "| bytes:", len(cuerpo), "| sha256:", sha(cuerpo))
    print("version:", "MODULAR (script delgado)" if modular else "ANTERIOR (script unico)")

    print("\n== 3. Paquete emissions_extraction en S3")
    prefijo = k.rsplit("/", 1)[0] + "/"
    claves = []
    for p in s3.get_paginator("list_objects_v2").paginate(Bucket=b, Prefix=prefijo):
        claves += [o["Key"] for o in p.get("Contents", [])]
    sueltos = sorted(x for x in claves if "/emissions_extraction/" in x and x.endswith(".py"))
    zips = [x for x in claves if x.endswith((".zip", ".whl"))]
    print("objetos bajo el prefijo:", len(claves), "| .py del paquete sueltos:", len(sueltos), "| zip/whl:", zips or "(ninguno)")
    for x in sueltos:
        print("  ", x[len(prefijo):], sha(s3.get_object(Bucket=b, Key=x)["Body"].read()))

    print("\n== 4. El job puede importar el paquete (--extra-py-files)")
    importable = False
    for uri in [u.strip() for u in da.get("--extra-py-files", "").split(",") if u.strip()]:
        try:
            importable = revisar_zip(uri, leer_s3(uri)) or importable
        except Exception as exc:
            print("  ", uri, "-> no se pudo leer:", type(exc).__name__)
    paquete_corrida = None
    if not importable:
        manual = uri_paquete_manual(script)
        print("  paquete de prueba (solo para la corrida):")
        try:
            if revisar_zip(manual, leer_s3(manual)):
                paquete_corrida = manual
        except Exception as exc:
            print("  ", manual, "-> no existe o no se pudo leer:", type(exc).__name__)

    print("\n== 5. Corridas del job desde el despliegue")
    runs = glue.get_job_runs(JobName=JOB, MaxResults=10)["JobRuns"]
    en_curso = [r for r in runs if r["JobRunState"] in ("STARTING", "RUNNING", "STOPPING", "WAITING")]
    for r in runs:
        if r["StartedOn"] >= meta["LastModified"]:
            print(r["Id"], r["JobRunState"], r["StartedOn"].isoformat(), f"{r.get('ExecutionTime', 0)/60:.0f} min",
                  (r.get("ErrorMessage") or "")[:300])
    print("en curso:", len(en_curso))

    print("\n== 6. Step Function", MAQUINA)
    sm = next((m for p in sfn.get_paginator("list_state_machines").paginate() for m in p["stateMachines"]
               if m["name"] == MAQUINA), None)
    corriendo = sfn.list_executions(stateMachineArn=sm["stateMachineArn"], statusFilter="RUNNING")["executions"] if sm else []
    print("existe:", bool(sm), "| ejecuciones en curso:", len(corriendo))

    print("\n== Veredicto")
    a = veredicto(modular, "el script desplegado es la version modular")
    c = veredicto(importable or bool(paquete_corrida),
                  "el paquete llega al job por --extra-py-files" + (" (paquete de prueba en la corrida)" if paquete_corrida else ""))
    d = veredicto(not en_curso and not corriendo, "no hay corridas en curso del job ni de la Step Function")
    if modular and not importable:
        print("ALERTA: la definicion del job no tiene el paquete; la malla fallara al arrancar "
              "(ModuleNotFoundError: emissions_extraction) hasta que DevOps lo agregue a la IaC.")
    return a and c and d, runs, paquete_corrida


def rutas_actuales(runs):
    for r in runs:
        valor = r.get("Arguments", {}).get("--RUTAS_CONFIG_EXTRACCION")
        if valor:
            return json.loads(valor)
    return []


def respaldar(rutas, carpeta):
    os.makedirs(carpeta, exist_ok=True)
    respaldo = {}
    for uri in rutas:
        cfg = json.loads(leer_s3(uri))
        caso = cfg.get("caso_uso") or cfg.get("caso") or uri.rstrip("/").split("/")[-2]
        ruta_param = cfg.get("rutas", {}).get("ruta_parametros_extraccion")
        param = json.loads(leer_s3(ruta_param)) if ruta_param else {}
        respaldo[caso] = {"configuracion": cfg, "ruta_parametros": ruta_param, "parametros": param}
        print("respaldo", caso, "| lote anterior:", param.get("lote_cargue_id"), "| datasets:", len(param.get("datasets_extraidos", [])))
    with open(os.path.join(carpeta, "antes.json"), "w", encoding="utf-8") as f:
        json.dump(respaldo, f, ensure_ascii=False, indent=1)
    return respaldo


def ejecutar():
    listo, runs, paquete_corrida = verificar()
    if not listo:
        print("\nNo se lanza nada: el veredicto tiene FAIL.")
        return
    marca = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    carpeta = os.path.join(CARPETA_PRUEBAS, marca)
    print("\n== 7. Respaldo de la configuracion y del runtime anteriores ->", carpeta)
    respaldar(rutas_actuales(runs), carpeta)

    print("\n== 8. Configuradora (como el paso 1 de la Step Function, sin ejecutar la malla)")
    resp = lam.invoke(FunctionName=CONFIGURADORA, Payload=json.dumps({"codigoMalla": "EMISIONES"}).encode())
    cuerpo = json.loads(resp["Payload"].read())
    if resp.get("FunctionError"):
        print("FAIL la configuradora respondio con error:", json.dumps(cuerpo)[:500])
        return
    res = cuerpo["resultado"]
    rutas = res["rutasConfigExtraccion"]
    print("periodo:", res.get("periodoNegocio"), "| lote:", res.get("loteCargueId"),
          "| id configuradora:", res.get("idEjecucion"), "| casos:", len(rutas))

    if modo in ("ejecutar-sombra", "ejecutar-diagnostico"):
        print("\n== 8b. Bloques filtro_spark en modo SOMBRA (solo en el JSON de esta corrida; cnf-mallas no se toca)")
        sys.path.insert(0, os.getcwd())
        import filtro_spark_bloques as fsb
        tamanos = fsb.cargar_tamanos(os.path.join(os.getcwd(), fsb.TAMANOS))
        for uri in rutas:
            cfg = json.loads(leer_s3(uri))
            caso = cfg.get("nombre_caso", "caso")
            with open(os.path.join(carpeta, f"config-original-{caso}.json"), "w", encoding="utf-8") as f:
                json.dump(cfg, f, ensure_ascii=False, indent=1)
            bloques, reporte = fsb.generar(cfg, tamanos)
            nuevo, aplicadas = fsb.aplicar(cfg, bloques)
            with open(os.path.join(carpeta, f"filtro-spark-{caso}.json"), "w", encoding="utf-8") as f:
                json.dump({"bloques": bloques, "reporte": reporte}, f, ensure_ascii=False, indent=1)
            b, k = partir(uri)
            s3.put_object(Bucket=b, Key=k, Body=json.dumps(nuevo, ensure_ascii=False).encode("utf-8"))
            print(f"{caso}: {aplicadas} tablas en SOMBRA | contextos: {list(bloques['contextos_spark'])}")

    if modo in ("ejecutar-concurrencia", "ejecutar-diagnostico"):
        valores = dict(CONCURRENCIA_PRUEBA)
        if os.path.isfile(ARCHIVO_CONCURRENCIA):
            with open(ARCHIVO_CONCURRENCIA, encoding="utf-8") as f:
                valores = json.load(f)
        if modo == "ejecutar-diagnostico":
            valores["medir_lectura"] = True
        print("\n== 8c. Concurrencia de prueba en control_extraccion (solo en el JSON de esta corrida; cnf-mallas no se toca)")
        print("valores:", json.dumps(valores, ensure_ascii=False), "| origen:",
              ARCHIVO_CONCURRENCIA if os.path.isfile(ARCHIVO_CONCURRENCIA) else "valores por defecto del script")
        for uri in rutas:
            cfg = json.loads(leer_s3(uri))
            caso = cfg.get("nombre_caso", "caso")
            with open(os.path.join(carpeta, f"config-original-{caso}.json"), "w", encoding="utf-8") as f:
                json.dump(cfg, f, ensure_ascii=False, indent=1)
            cfg.setdefault("control_extraccion", {}).update(valores)
            b, k = partir(uri)
            s3.put_object(Bucket=b, Key=k, Body=json.dumps(cfg, ensure_ascii=False).encode("utf-8"))
            print(f"{caso}: control_extraccion con {sorted(valores)}")

    print("\n== 9. Lanzar el job")
    argumentos = {"--RUTAS_CONFIG_EXTRACCION": json.dumps(rutas)}
    if len(args) > 1:
        argumentos["--EJECUCION_ID"] = args[1]
    if paquete_corrida:
        argumentos["--extra-py-files"] = paquete_corrida
        print("paquete solo para esta corrida:", paquete_corrida)
    run_id = glue.start_job_run(JobName=JOB, Arguments=argumentos)["JobRunId"]
    estado = {"job_run_id": run_id, "paquete_corrida": paquete_corrida,
              "lote": res.get("loteCargueId"), "periodo": res.get("periodoNegocio"),
              "id_configuradora": res.get("idEjecucion"), "id_enviado": argumentos.get("--EJECUCION_ID"),
              "rutas": rutas, "carpeta": carpeta}
    with open(os.path.join(carpeta, "estado.json"), "w", encoding="utf-8") as f:
        json.dump(estado, f, ensure_ascii=False, indent=1)
    print("JobRunId:", run_id)
    print("Seguimiento: bash probar-despliegue-dev.sh seguir", run_id)


def eventos(run_id):
    vistos = []
    token = None
    while True:
        kw = {"logGroupName": GRUPO_SALIDA, "logStreamNames": [run_id], "filterPattern": '"evento"'}
        if token:
            kw["nextToken"] = token
        try:
            page = logs.filter_log_events(**kw)
        except logs.exceptions.ResourceNotFoundException:
            return vistos
        for e in page.get("events", []):
            for linea in e["message"].splitlines():
                inicio = linea.find("{")
                if inicio < 0:
                    continue
                try:
                    vistos.append(json.loads(linea[inicio:]))
                except ValueError:
                    pass
        token = page.get("nextToken")
        if not token:
            return vistos


def seguir():
    run_id = args[1]
    r = glue.get_job_run(JobName=JOB, RunId=run_id)["JobRun"]
    print("== 1. Corrida", run_id)
    print("estado:", r["JobRunState"], "| inicio:", r["StartedOn"].isoformat(),
          "| minutos:", f"{r.get('ExecutionTime', 0)/60:.1f}", "| DPU-seg:", r.get("DPUSeconds"))
    print("argumentos:", {k: v for k, v in r.get("Arguments", {}).items() if k in ("--EJECUCION_ID",)} or "(sin --EJECUCION_ID)")
    if r.get("ErrorMessage"):
        print("error:", r["ErrorMessage"][:800])

    print("\n== 2. Eventos del log")
    ev = eventos(run_id)
    for e in ev:
        if e.get("evento") in EVENTOS_CLAVE:
            resumen = {k: v for k, v in e.items() if k not in ("datasets_extraidos", "runtimes")}
            print(json.dumps(resumen, ensure_ascii=False)[:600])
    tablas = [e for e in ev if e.get("evento") == "TABLA_FIN"]
    por_estado = {}
    for t in tablas:
        clave = (t.get("caso"), t.get("estado"))
        por_estado[clave] = por_estado.get(clave, 0) + 1
    print("TABLA_FIN por caso y estado:", {f"{a}/{b}": n for (a, b), n in sorted(por_estado.items(), key=str)})
    fin_tabla = [e for e in ev if e.get("evento") == "PASO_FIN" and e.get("paso") == "extraer_tabla"]
    por_resultado = {}
    for e in fin_tabla:
        clave = (e.get("caso"), e.get("resultado"))
        por_resultado[clave] = por_resultado.get(clave, 0) + 1
    print("Tablas por caso y resultado (PASO_FIN):", {f"{a}/{b}": n for (a, b), n in sorted(por_resultado.items(), key=str)})
    minutos = {f"{e.get('paso')}/{e.get('caso')}": round(e["duracion_ms"] / 60000, 1) for e in ev
               if e.get("evento") in ("PASO_FIN", "COMPONENTE_FIN") and e.get("paso") in ("extraer_nucleo", "extraer_caso", "extraccion_emisiones")
               and isinstance(e.get("duracion_ms"), (int, float))}
    if minutos:
        print("Minutos por paso:", minutos)
    lentas = sorted(fin_tabla, key=lambda e: e.get("duracion_ms") or 0, reverse=True)[:10]
    if lentas:
        print("Tablas mas lentas:", [f"{e.get('caso')}/{e.get('tabla_relativa')} {round((e.get('duracion_ms') or 0) / 60000, 1)} min" for e in lentas])
    medidas = [e for e in fin_tabla if isinstance(e.get("duracion_lectura_ms"), (int, float))]
    if medidas:
        lectura = sum(e["duracion_lectura_ms"] for e in medidas) / 60000
        escritura = sum(e.get("duracion_escritura_ms") or 0 for e in medidas) / 60000
        print(f"
== 2c. Lectura y escritura (medir_lectura): {len(medidas)} tablas | lectura {lectura:.1f} min | escritura {escritura:.1f} min")
        for e in sorted(medidas, key=lambda e: e["duracion_lectura_ms"], reverse=True)[:15]:
            print(f"   {e.get('caso')}/{e.get('tabla_relativa')}: lectura {e['duracion_lectura_ms'] / 60000:.1f} min"
                  f" | escritura {(e.get('duracion_escritura_ms') or 0) / 60000:.1f} min | filas {e.get('filas_salida')}")
    contextos = [e for e in ev if e.get("evento") in ("CONTEXTO_SPARK_CARGADO", "ADVERTENCIA_CONTEXTO_SPARK_EXCEDIDO")]
    pasos_ctx = {(e.get("caso"), e.get("contexto")): e.get("duracion_ms") for e in ev
                 if e.get("evento") == "PASO_FIN" and e.get("paso") == "cargar_contexto"}
    iguales = [e for e in ev if e.get("evento") == "EQUIVALENCIA_FILTRO_SPARK"]
    distintas = [e for e in ev if e.get("evento") == "ADVERTENCIA_EQUIVALENCIA_FILTRO_SPARK"]
    respaldos = [e for e in ev if e.get("evento") == "ADVERTENCIA_FILTRO_SPARK_RESPALDO"]
    if contextos or iguales or distintas or respaldos:
        print("\n== 2b. Filtro en Spark (modo SOMBRA)")
        for e in contextos:
            ms = pasos_ctx.get((e.get("caso"), e.get("contexto")))
            print(f"   contexto {e.get('caso')}/{e.get('contexto')}: {e.get('filas')} filas"
                  f"{'' if ms is None else f' en {ms/60000:.1f} min'}{' EXCEDIDO' if 'EXCEDIDO' in e['evento'] else ''}")
        print(f"   tablas equivalentes: {len(iguales)} | distintas: {len(distintas)} | respaldo a la SQL: {len(respaldos)}")
        for e in distintas:
            print(f"   DISTINTA {e.get('caso')}/{e.get('tabla_relativa')}: sql={e.get('filas_sql')} spark={e.get('filas_spark')}"
                  f" solo_sql={e.get('solo_sql')} solo_spark={e.get('solo_spark')}")
        for e in respaldos:
            print(f"   RESPALDO {e.get('caso')}/{e.get('tabla_relativa')}: {e.get('motivo')}")
        veredicto(not distintas and not respaldos, "todas las tablas en SOMBRA dan las mismas filas que la SQL")

    if r["JobRunState"] in ("FAILED", "ERROR", "TIMEOUT", "STOPPED"):
        print("\n== 3. Errores del driver")
        try:
            page = logs.filter_log_events(logGroupName=GRUPO_ERROR, logStreamNames=[run_id],
                                          filterPattern='?Error ?Exception ?Traceback', limit=30)
            for e in page.get("events", []):
                print(" ", e["message"].strip()[:300])
        except logs.exceptions.ResourceNotFoundException:
            print("sin log de errores")
        return

    estado_path = next((os.path.join(CARPETA_PRUEBAS, d, "estado.json") for d in sorted(os.listdir(CARPETA_PRUEBAS))
                        if os.path.exists(os.path.join(CARPETA_PRUEBAS, d, "estado.json"))
                        and json.load(open(os.path.join(CARPETA_PRUEBAS, d, "estado.json")))["job_run_id"] == run_id),
                       None) if os.path.isdir(CARPETA_PRUEBAS) else None
    if r["JobRunState"] != "SUCCEEDED" or not estado_path:
        print("\nLa comparacion con el respaldo se hace cuando la corrida termina en SUCCEEDED y existe su respaldo.")
        return

    print("\n== 3. Comparacion con la corrida anterior (respaldo)")
    antes = json.load(open(os.path.join(os.path.dirname(estado_path), "antes.json"), encoding="utf-8"))
    for caso, previo in antes.items():
        nuevo = json.loads(leer_s3(previo["ruta_parametros"]))
        a = {d.get("tabla_relativa"): d for d in previo["parametros"].get("datasets_extraidos", [])}
        n = {d.get("tabla_relativa"): d for d in nuevo.get("datasets_extraidos", [])}
        filas = [t for t in sorted(set(a) | set(n)) if (a.get(t) or {}).get("total_registros") != (n.get(t) or {}).get("total_registros")]
        destinos = [t for t in sorted(set(a) & set(n)) if a[t].get("ruta_dataset_destino") != n[t].get("ruta_dataset_destino")]
        print(f"-- {caso}: estado={nuevo.get('estado_extraccion')} lote={nuevo.get('lote_cargue_id')} id={nuevo.get('id_ejecucion')}")
        print(f"   tablas antes={len(a)} ahora={len(n)} | filas distintas={len(filas)} | ruta Silver distinta={len(destinos)}")
        for t in filas[:20]:
            print(f"   filas  {t}: {(a.get(t) or {}).get('total_registros')} -> {(n.get(t) or {}).get('total_registros')}")
        for t in destinos[:5]:
            print(f"   silver {t}: {a[t].get('ruta_dataset_destino')} -> {n[t].get('ruta_dataset_destino')}")
        veredicto(not filas, f"{caso}: mismas filas por tabla que la corrida anterior")


{"verificar": lambda: verificar(), "subir-paquete": subir_paquete, "ejecutar": ejecutar,
 "ejecutar-sombra": ejecutar, "ejecutar-concurrencia": ejecutar,
 "ejecutar-diagnostico": ejecutar, "seguir": seguir}.get(modo, lambda: print("modo no valido:", modo))()
EOF
echo "Salida: $OUT"
