#!/usr/bin/env python3
"""Diagnóstico de solo lectura de la malla Emisiones (Generales, Satélites y PCR Vida).

Se corre igual en cada ambiente (QA y PRD por DevOps, DEV por Desarrollo) y deja
una carpeta de evidencia para comparar los ambientes capa por capa con
``comparar_ambientes.py``. No escribe ni borra nada en AWS: solo usa llamadas
Get/List/Describe/Query, lee objetos de S3 y lanza consultas SELECT en Athena.

Uso en CloudShell (región us-east-1):
    python3 diag_emisiones_v2.py --amb qa --periodo 202606
    python3 diag_emisiones_v2.py --amb prd --periodo 202606 --workgroup primary

Secciones (cada una es independiente; si una falla por permisos, se anota y sigue):
  codigo         scripts de Glue y código de la lambda configuradora desplegados (sha256)
  orquestacion   Step Function, últimas ejecuciones y corridas de los jobs
  configuracion  PRG y CNF en DynamoDB, JSON publicados en S3 y runtime de la extracción
  fuente         conexión JDBC que usa la extracción (sin usuario ni clave)
  almacenamiento lotes en Bronce y salidas del Silver en S3
  datos          totales por producto en Silver y por producto y concepto en Bronce (Athena)
  oracle         genera las SQL para correr en la Oracle de ese ambiente con su propia configuración

Datos personales: no se consultan ni se guardan columnas de personas (solo agregados por
producto y concepto). La evidencia tiene nombres de recursos y rutas: va a la carpeta de
evidencias fuera de Git, no al repositorio.
"""

import argparse
import hashlib
import io
import json
import os
import re
import sys
import tarfile
import time
import traceback
import urllib.request
import zipfile
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key
from botocore.config import Config

VERSION = "2026-11-23.3 (datos por ruta exacta + tomadores y agentes)"
REGION = "us-east-1"
CONFIG_BOTO = Config(region_name=REGION, retries={"max_attempts": 5, "mode": "standard"})

CASOS = {
    # nombre_caso: (carpeta en niif-<amb>-assets, PK en cnf-mallas, job Silver)
    "generales": ("generales", "MALLA#GENERALES", "silver-generales"),
    "satelites": ("satelites", "MALLA#SATELITES", "silver-satelites"),
    "pcr_vida": ("pcr-vida", "MALLA#PCR_VIDA", "silver-vida"),
}
JOBS = [
    "extraccion-estandar-emisiones",
    "val-tecnica",
    "remediacion-estandar",
    "silver-generales",
    "silver-satelites",
    "silver-vida",
    "scd",
]
VARIABLES_LAMBDA_VISIBLES = {"ENV", "PRG_TABLE", "CNF_TABLE", "PRG_SK", "LOG_LEVEL"}
PATRON_SECRETO = re.compile(r"(secret|password|passwd|pwd|token|key|credential)", re.I)
PATRON_AMBIENTE = re.compile(r"\b(?:niif|alfa-datalake|datalake)-(dev|qa|prd|prod)-[a-z0-9-]*", re.I)
COLUMNAS_PRODUCTO = ("idproducto", "id_producto", "productid", "codigo_producto")
TIPOS_MONTO = ("decimal", "double", "float", "real")


# --------------------------------------------------------------------------- utilidades

class Evidencia:
    """Carpeta de salida, registro de errores y alertas."""

    def __init__(self, raiz):
        self.raiz = raiz
        self.errores = []
        self.alertas = []
        os.makedirs(raiz, exist_ok=True)

    def ruta(self, *partes):
        ruta = os.path.join(self.raiz, *partes)
        os.makedirs(os.path.dirname(ruta), exist_ok=True)
        return ruta

    def json(self, nombre, contenido):
        with open(self.ruta(nombre), "w", encoding="utf-8") as archivo:
            json.dump(contenido, archivo, ensure_ascii=False, indent=2, default=a_json)

    def texto(self, nombre, contenido, modo="w"):
        with open(self.ruta(nombre), modo, encoding="utf-8") as archivo:
            archivo.write(contenido)

    def binario(self, nombre, contenido):
        with open(self.ruta(nombre), "wb") as archivo:
            archivo.write(contenido)

    def error(self, seccion, detalle):
        texto = f"{type(detalle).__name__}: {detalle}" if isinstance(detalle, Exception) else str(detalle)
        self.errores.append({"seccion": seccion, "error": texto[:800]})
        log(f"ERROR en {seccion}: {texto[:300]}")

    def alerta(self, codigo, detalle, **datos):
        self.alertas.append({"codigo": codigo, "detalle": detalle, **datos})
        log(f"ALERTA {codigo}: {detalle}")


def log(mensaje):
    print(f"[{datetime.now(timezone.utc):%H:%M:%S}] {mensaje}", flush=True)


def a_json(valor):
    if isinstance(valor, Decimal):
        return int(valor) if valor == valor.to_integral_value() else str(valor)
    if isinstance(valor, (datetime,)):
        return valor.isoformat()
    if isinstance(valor, (set, frozenset)):
        return sorted(valor, key=str)
    if isinstance(valor, bytes):
        return f"<{len(valor)} bytes>"
    return str(valor)


def sha256(contenido):
    if isinstance(contenido, str):
        contenido = contenido.encode("utf-8")
    return hashlib.sha256(contenido).hexdigest()


def partes_s3(uri):
    coincidencia = re.match(r"^s3a?://([^/]+)/?(.*)$", str(uri or ""))
    if not coincidencia:
        raise ValueError(f"URI S3 inválida: {uri}")
    return coincidencia.group(1), coincidencia.group(2)


def seccion(nombre):
    """Decorador: la sección registra su error y no corta el diagnóstico."""

    def envoltura(funcion):
        def ejecutar(ctx, *args, **kwargs):
            log(f"== Sección {nombre}")
            try:
                return funcion(ctx, *args, **kwargs)
            except Exception as exc:  # noqa: BLE001 - se registra y se sigue con la siguiente sección
                ctx.ev.error(nombre, exc)
                ctx.ev.texto("errores_detalle.txt", f"\n--- {nombre}\n{traceback.format_exc()}", modo="a")
                return None

        return ejecutar

    return envoltura


def ambientes_ajenos(texto, amb):
    """Devuelve las menciones a recursos de otro ambiente dentro de un texto."""
    propio = {"prd", "prod"} if amb in {"prd", "prod"} else {amb}
    hallados = set()
    for coincidencia in PATRON_AMBIENTE.finditer(texto or ""):
        if coincidencia.group(1).lower() not in propio:
            hallados.add(coincidencia.group(0))
    return sorted(hallados)


def normalizar_sql(sql):
    return re.sub(r"\s+", " ", str(sql or "")).strip()


def tablas_de_config(config):
    """Recorre las tablas del núcleo y de los ciclos (JSON publicado o ítem CNF)."""
    nucleo = config.get("nucleo_comun") or config.get("nucleoComun") or {}
    for fase in nucleo.get("fases", []):
        for tabla in fase.get("tablas", []):
            yield "nucleo", tabla
    for ciclo in config.get("ciclos", []):
        ciclo = ciclo.get("ciclo", ciclo)
        for segmento in ciclo.get("segmentos", []):
            for tabla in segmento.get("tablas", []):
                yield "rama", tabla


def resumen_tabla(origen, tabla):
    return {
        "origen": origen,
        "tabla": str(tabla.get("tabla") or "").upper(),
        "tabla_relativa": tabla.get("tabla_relativa"),
        "fase": tabla.get("fase"),
        "estrategia_lectura": tabla.get("estrategia_lectura"),
        "obligatoria": (tabla.get("disponibilidad") or {}).get("obligatoria", True),
        "sha256_consulta_sql": sha256(normalizar_sql(tabla.get("consulta_sql"))) if tabla.get("consulta_sql") else None,
        "sha256_where": sha256(normalizar_sql(tabla.get("where"))) if tabla.get("where") else None,
        "columnas": len(tabla.get("columnas") or []),
    }


class Contexto:
    def __init__(self, args):
        self.args = args
        self.amb = args.amb
        self.periodo = args.periodo
        self.assets = f"niif-{self.amb}-assets"
        sello = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self.ev = Evidencia(os.path.abspath(args.salida or f"evidencia-pcr-emisiones-{self.amb}-{sello}"))
        self.s3 = boto3.client("s3", config=CONFIG_BOTO)
        self.glue = boto3.client("glue", config=CONFIG_BOTO)
        self.lmb = boto3.client("lambda", config=CONFIG_BOTO)
        self.sfn = boto3.client("stepfunctions", config=CONFIG_BOTO)
        self.ddb = boto3.resource("dynamodb", config=CONFIG_BOTO)
        self.athena = boto3.client("athena", config=CONFIG_BOTO)
        self.prg_table = f"niif-{self.amb}-prg-programaciones"
        self.cnf_table = f"niif-{self.amb}-cnf-mallas"
        self.configs_publicadas = {}
        self.runtimes = {}
        self.jobs = {}

    def leer_s3(self, uri):
        bucket, clave = partes_s3(uri)
        respuesta = self.s3.get_object(Bucket=bucket, Key=clave)
        return respuesta["Body"].read(), {
            "uri": uri,
            "etag": respuesta.get("ETag", "").strip('"'),
            "version_id": respuesta.get("VersionId"),
            "ultima_modificacion": respuesta.get("LastModified"),
            "bytes": respuesta.get("ContentLength"),
        }

    def versiones_s3(self, uri, maximo=15):
        bucket, clave = partes_s3(uri)
        try:
            respuesta = self.s3.list_object_versions(Bucket=bucket, Prefix=clave, MaxKeys=maximo)
        except Exception as exc:  # noqa: BLE001 - el bucket puede no tener versionado o permiso
            return {"error": str(exc)[:300]}
        return [
            {
                "version_id": v.get("VersionId"),
                "ultima_modificacion": v.get("LastModified"),
                "etag": v.get("ETag", "").strip('"'),
                "es_la_actual": v.get("IsLatest"),
            }
            for v in respuesta.get("Versions", [])
            if v.get("Key") == clave
        ]

    def listar_prefijos(self, uri):
        bucket, prefijo = partes_s3(uri)
        prefijo = prefijo.rstrip("/") + "/"
        salida = []
        paginador = self.s3.get_paginator("list_objects_v2")
        for pagina in paginador.paginate(Bucket=bucket, Prefix=prefijo, Delimiter="/"):
            salida.extend(p["Prefix"] for p in pagina.get("CommonPrefixes", []))
        return salida

    def resumen_objetos(self, uri):
        """Cuenta objetos, bytes y la última fecha bajo un prefijo."""
        bucket, prefijo = partes_s3(uri)
        prefijo = prefijo.rstrip("/") + "/"
        total, tamano, ultima = 0, 0, None
        paginador = self.s3.get_paginator("list_objects_v2")
        for pagina in paginador.paginate(Bucket=bucket, Prefix=prefijo):
            for objeto in pagina.get("Contents", []):
                total += 1
                tamano += objeto.get("Size", 0)
                if ultima is None or objeto["LastModified"] > ultima:
                    ultima = objeto["LastModified"]
        return {"prefijo": f"s3://{bucket}/{prefijo}", "objetos": total, "bytes": tamano, "ultima_modificacion": ultima}


# --------------------------------------------------------------------------- secciones

@seccion("codigo")
def capturar_codigo(ctx):
    resumen = {"glue": {}, "lambda": {}}
    for sufijo in JOBS:
        nombre = f"niif-{ctx.amb}-{sufijo}"
        try:
            job = ctx.glue.get_job(JobName=nombre)["Job"]
        except ctx.glue.exceptions.EntityNotFoundException:
            resumen["glue"][nombre] = {"existe": False}
            continue
        argumentos = {
            k: ("<oculto>" if PATRON_SECRETO.search(k) else v)
            for k, v in (job.get("DefaultArguments") or {}).items()
        }
        dato = {
            "existe": True,
            "script": job["Command"].get("ScriptLocation"),
            "glue_version": job.get("GlueVersion"),
            "worker_type": job.get("WorkerType"),
            "workers": job.get("NumberOfWorkers"),
            "timeout_min": job.get("Timeout"),
            "conexiones": (job.get("Connections") or {}).get("Connections", []),
            "argumentos_por_defecto": argumentos,
            "ultima_modificacion_job": job.get("LastModifiedOn"),
        }
        try:
            contenido, meta = ctx.leer_s3(dato["script"])
            dato.update({"sha256_script": sha256(contenido), "objeto": meta})
            ctx.ev.binario(os.path.join("codigo", "glue", nombre, os.path.basename(dato["script"])), contenido)
            dato["versiones_script"] = ctx.versiones_s3(dato["script"])
            for ajeno in ambientes_ajenos(contenido.decode("utf-8", "replace"), ctx.amb):
                ctx.ev.alerta("COD-AMBIENTE-AJENO", f"El script de {nombre} menciona un recurso de otro ambiente", valor=ajeno)
        except Exception as exc:  # noqa: BLE001
            ctx.ev.error(f"codigo:{nombre}", exc)
        for clave in ("--extra-py-files", "--extra-files"):
            for uri in str(argumentos.get(clave) or "").split(","):
                if uri.startswith("s3://"):
                    try:
                        contenido, meta = ctx.leer_s3(uri)
                        dato.setdefault("dependencias", []).append({"uri": uri, "sha256": sha256(contenido), **meta})
                    except Exception as exc:  # noqa: BLE001
                        ctx.ev.error(f"codigo:{nombre}:{uri}", exc)
        resumen["glue"][nombre] = dato
    ctx.jobs = resumen["glue"]

    funcion = f"niif-{ctx.amb}-configuration-general"
    try:
        respuesta = ctx.lmb.get_function(FunctionName=funcion)
        conf = respuesta["Configuration"]
        variables = (conf.get("Environment") or {}).get("Variables") or {}
        ctx.prg_table = variables.get("PRG_TABLE", ctx.prg_table)
        ctx.cnf_table = variables.get("CNF_TABLE", ctx.cnf_table)
        dato = {
            "existe": True,
            "code_sha256": conf.get("CodeSha256"),
            "ultima_modificacion": conf.get("LastModified"),
            "runtime": conf.get("Runtime"),
            "handler": conf.get("Handler"),
            "version": conf.get("Version"),
            "variables": {k: (variables[k] if k in VARIABLES_LAMBDA_VISIBLES else "<no se muestra>") for k in sorted(variables)},
            "archivos": {},
        }
        # La URL firmada se usa solo para descargar; nunca se imprime ni se guarda.
        with urllib.request.urlopen(respuesta["Code"]["Location"], timeout=60) as descarga:  # noqa: S310
            paquete = zipfile.ZipFile(io.BytesIO(descarga.read()))
        for nombre in paquete.namelist():
            if nombre.endswith(".py") and nombre.count("/") <= 1:
                contenido = paquete.read(nombre)
                dato["archivos"][nombre] = sha256(contenido)
                ctx.ev.binario(os.path.join("codigo", "lambda", funcion, nombre), contenido)
                texto = contenido.decode("utf-8", "replace")
                for ajeno in ambientes_ajenos(texto, ctx.amb):
                    ctx.ev.alerta("COD-AMBIENTE-AJENO", f"{funcion}/{nombre} menciona un recurso de otro ambiente", valor=ajeno)
                validos = re.search(r"\{\s*\"dev\"\s*,\s*\"qa\"\s*,\s*\"prod\"\s*\}", texto)
                if validos and ctx.amb not in {"dev", "qa", "prod"}:
                    ctx.ev.alerta(
                        "COD-AMBIENTE-NO-ACEPTADO",
                        f"{funcion}/{nombre} solo acepta dev, qa y prod; el ambiente es '{ctx.amb}'",
                    )
        resumen["lambda"][funcion] = dato
    except ctx.lmb.exceptions.ResourceNotFoundException:
        resumen["lambda"][funcion] = {"existe": False}
    ctx.ev.json("codigo/resumen_codigo.json", resumen)


def _nombre_estado(eventos_por_id, evento):
    actual = evento
    for _ in range(50):
        if actual is None:
            return None
        if actual["type"].endswith("StateEntered"):
            return actual["stateEnteredEventDetails"]["name"]
        actual = eventos_por_id.get(actual.get("previousEventId"))
    return None


def _parametros_glue(nodo):
    """Devuelve los bloques Parameters/Arguments que tengan JobName, en cualquier nivel."""
    if isinstance(nodo, dict):
        for clave in ("Parameters", "Arguments"):
            bloque = nodo.get(clave)
            if isinstance(bloque, dict) and "JobName" in bloque:
                yield bloque
        for valor in nodo.values():
            yield from _parametros_glue(valor)
    elif isinstance(nodo, list):
        for valor in nodo:
            yield from _parametros_glue(valor)


def _historial(ctx, arn):
    eventos = []
    paginador = ctx.sfn.get_paginator("get_execution_history")
    for pagina in paginador.paginate(executionArn=arn, PaginationConfig={"MaxItems": 3000}):
        eventos.extend(pagina["events"])
    return eventos


@seccion("orquestacion")
def capturar_orquestacion(ctx):
    nombre = f"niif-{ctx.amb}-emisiones"
    arn = None
    for pagina in ctx.sfn.get_paginator("list_state_machines").paginate():
        for maquina in pagina["stateMachines"]:
            if maquina["name"] == nombre:
                arn = maquina["stateMachineArn"]
    if not arn:
        ctx.ev.alerta("ORQ-SIN-STEP-FUNCTION", f"No existe la Step Function {nombre}")
        return
    descripcion = ctx.sfn.describe_state_machine(stateMachineArn=arn)
    definicion = descripcion["definition"]
    ctx.ev.texto("orquestacion/definicion_step_function.json", definicion)
    for ajeno in ambientes_ajenos(definicion, ctx.amb):
        ctx.ev.alerta("ORQ-AMBIENTE-AJENO", "La Step Function menciona un recurso de otro ambiente", valor=ajeno)
    # silver_generales.py exige --RUTA_CONFIG_SCD; se mira si llega por la Step Function o por el job.
    job_generales = f"niif-{ctx.amb}-silver-generales"
    en_definicion = any(
        "--RUTA_CONFIG_SCD" in (p.get("Arguments") or {})
        for p in _parametros_glue(json.loads(definicion))
        if p.get("JobName") == job_generales
    )
    por_defecto = "--RUTA_CONFIG_SCD" in ((ctx.jobs.get(job_generales) or {}).get("argumentos_por_defecto") or {})
    if not en_definicion and not por_defecto:
        ctx.ev.alerta("ORQ-FALTA-ARGUMENTO", f"{job_generales} exige --RUTA_CONFIG_SCD y no llega ni por la Step Function ni por los argumentos del job")

    ejecuciones = []
    respuesta = ctx.sfn.list_executions(stateMachineArn=arn, maxResults=ctx.args.ejecuciones)
    for ejecucion in respuesta["executions"]:
        dato = {
            "nombre": ejecucion["name"],
            "estado": ejecucion["status"],
            "inicio": ejecucion["startDate"],
            "fin": ejecucion.get("stopDate"),
            "pasos": [],
        }
        try:
            eventos = _historial(ctx, ejecucion["executionArn"])
            por_id = {e["id"]: e for e in eventos}
            for evento in eventos:
                tipo = evento["type"]
                if tipo == "TaskSucceeded":
                    estado = _nombre_estado(por_id, evento)
                    detalle = evento["taskSucceededEventDetails"]
                    paso = {"paso": estado, "resultado": "OK", "recurso": detalle.get("resourceType")}
                    try:
                        salida = json.loads(detalle.get("output") or "{}")
                    except json.JSONDecodeError:
                        salida = {}
                    if detalle.get("resourceType") == "glue":
                        paso.update({
                            "job": salida.get("JobName"),
                            "job_run_id": salida.get("Id"),
                            "inicio": salida.get("StartedOn"),
                            "segundos": salida.get("ExecutionTime"),
                            "argumentos": {
                                k: v for k, v in (salida.get("Arguments") or {}).items() if not PATRON_SECRETO.search(k)
                            },
                        })
                    if detalle.get("resourceType") == "lambda":
                        carga = salida.get("Payload") or {}
                        resultado = carga.get("resultado") if isinstance(carga.get("resultado"), dict) else carga
                        paso["respuesta"] = {
                            k: resultado.get(k)
                            for k in (
                                "periodoNegocio", "idEjecucion", "loteCargueId", "modoEjecucion",
                                "fechaInicio", "fechaFin", "status", "casos", "rutasConfigExtraccion",
                            )
                            if isinstance(resultado, dict) and k in resultado
                        }
                    dato["pasos"].append(paso)
                elif tipo in {"TaskFailed", "ExecutionFailed", "TaskTimedOut"}:
                    llave = {"TaskFailed": "taskFailedEventDetails", "ExecutionFailed": "executionFailedEventDetails",
                             "TaskTimedOut": "taskTimedOutEventDetails"}[tipo]
                    detalle = evento.get(llave, {})
                    dato["pasos"].append({
                        "paso": _nombre_estado(por_id, evento),
                        "resultado": tipo,
                        "error": detalle.get("error"),
                        "causa": str(detalle.get("cause") or "")[:600],
                    })
        except Exception as exc:  # noqa: BLE001
            dato["error_historial"] = str(exc)[:300]
        ejecuciones.append(dato)
    ctx.ev.json("orquestacion/ejecuciones.json", ejecuciones)

    corridas = {}
    for sufijo in JOBS:
        job = f"niif-{ctx.amb}-{sufijo}"
        try:
            runs = ctx.glue.get_job_runs(JobName=job, MaxResults=ctx.args.ejecuciones)["JobRuns"]
        except Exception as exc:  # noqa: BLE001
            corridas[job] = {"error": str(exc)[:300]}
            continue
        corridas[job] = [
            {
                "id": r.get("Id"),
                "estado": r.get("JobRunState"),
                "inicio": r.get("StartedOn"),
                "fin": r.get("CompletedOn"),
                "segundos": r.get("ExecutionTime"),
                "dpu_segundos": r.get("DPUSeconds"),
                "workers": r.get("NumberOfWorkers"),
                "worker_type": r.get("WorkerType"),
                "error": str(r.get("ErrorMessage") or "")[:400] or None,
                "argumentos": {k: v for k, v in (r.get("Arguments") or {}).items() if not PATRON_SECRETO.search(k)},
            }
            for r in runs
        ]
    ctx.ev.json("orquestacion/corridas_glue.json", corridas)


@seccion("configuracion")
def capturar_configuracion(ctx):
    prg = ctx.ddb.Table(ctx.prg_table).get_item(
        Key={"PK": "PROGRAMACION#EMISIONES", "SK": "METADATA"}, ConsistentRead=True
    ).get("Item")
    ctx.ev.json("configuracion/prg_programacion_emisiones.json", prg or {"existe": False})
    if not prg:
        ctx.ev.alerta("CFG-SIN-PRG", f"No existe PROGRAMACION#EMISIONES en {ctx.prg_table}")
    else:
        if str(prg.get("periodoNegocio")) != ctx.periodo:
            ctx.ev.alerta("CFG-PERIODO-PRG", "El PRG hoy apunta a otro periodo (la última corrida puede no ser la del periodo pedido)",
                          prg=str(prg.get("periodoNegocio")), pedido=ctx.periodo)
        if prg.get("loteCargueId") is not None:
            ctx.ev.alerta("CFG-LOTE-FIJO", "El PRG fija loteCargueId: una reejecución reutiliza los checkpoints de ese lote y no vuelve a leer Oracle",
                          lote=str(prg.get("loteCargueId")))
        if prg.get("idEjecucion"):
            ctx.ev.alerta("CFG-ID-FIJO", "El PRG fija idEjecucion", id=str(prg.get("idEjecucion")))

    tabla_cnf = ctx.ddb.Table(ctx.cnf_table)
    cnf = {}
    for pk in [c[1] for c in CASOS.values()] + ["MALLA#EMISIONES"]:
        items = []
        consulta = {"KeyConditionExpression": Key("PK").eq(pk), "ConsistentRead": True}
        while True:
            respuesta = tabla_cnf.query(**consulta)
            items.extend(respuesta.get("Items", []))
            if "LastEvaluatedKey" not in respuesta:
                break
            consulta["ExclusiveStartKey"] = respuesta["LastEvaluatedKey"]
        cnf[pk] = items
        ctx.ev.json(f"configuracion/cnf_{pk.replace('#', '_').lower()}.json", items)
        resumen = []
        for item in items:
            fila = {"SK": item.get("SK"), "versionModelo": item.get("versionModelo"),
                    "archivoActivoUri": item.get("archivoActivoUri"), "filtrosEfectivos": item.get("filtrosEfectivos")}
            fila["tablas"] = [resumen_tabla(o, t) for o, t in tablas_de_config(item)]
            resumen.append(fila)
        ctx.ev.json(f"configuracion/cnf_{pk.replace('#', '_').lower()}_resumen.json", resumen)

    for caso, (carpeta, pk, _job) in CASOS.items():
        uris = {
            "extraccion": str(next((i.get("archivoActivoUri") for i in cnf.get(pk, []) if i.get("archivoActivoUri")), None)
                              or f"s3://{ctx.assets}/{carpeta}/config_extraccion_{caso}.json").replace("{env}", ctx.amb),
            "validacion": f"s3://{ctx.assets}/{carpeta}/config_val_tec_{caso}.json",
            "remediacion": f"s3://{ctx.assets}/{carpeta}/config_remediacion_{caso}.json",
            "scd": f"s3://{ctx.assets}/{carpeta}/config_scd_{caso}.json",
            "runtime": f"s3://{ctx.assets}/{carpeta}/parametros_extraccion.json",
        }
        resumen_caso = {}
        for tipo, uri in uris.items():
            try:
                contenido, meta = ctx.leer_s3(uri)
            except Exception as exc:  # noqa: BLE001
                resumen_caso[tipo] = {"uri": uri, "error": str(exc)[:200]}
                continue
            ctx.ev.binario(os.path.join("configuracion", "s3", caso, os.path.basename(partes_s3(uri)[1])), contenido)
            texto = contenido.decode("utf-8", "replace")
            meta["sha256"] = sha256(contenido)
            meta["versiones"] = ctx.versiones_s3(uri)
            for ajeno in ambientes_ajenos(texto, ctx.amb):
                ctx.ev.alerta("CFG-AMBIENTE-AJENO", f"{uri} menciona un recurso de otro ambiente", valor=ajeno)
            resumen_caso[tipo] = meta
            datos = json.loads(texto)
            if tipo == "extraccion":
                ctx.configs_publicadas[caso] = datos
                resumen_caso[tipo]["contenido"] = {
                    k: datos.get(k)
                    for k in ("nombre_caso", "id_ejecucion", "periodo_negocio", "lote_cargue_id", "modo_ejecucion",
                              "ventana_extraccion", "filtros_efectivos", "rutas", "fuente", "control_extraccion")
                }
                resumen_caso[tipo]["tablas"] = [resumen_tabla(o, t) for o, t in tablas_de_config(datos)]
                if not (datos.get("rutas") or {}).get("s3_silver_base"):
                    ctx.ev.alerta("CFG-SIN-RUTA-SILVER", f"{caso}: la configuración no trae rutas.s3_silver_base; la extracción deriva la ruta Silver con un reemplazo de texto",
                                  rutas=datos.get("rutas"))
            if tipo == "runtime":
                ctx.runtimes[caso] = datos
                resumen_caso[tipo]["contenido"] = resumir_runtime(ctx, caso, datos)
        ctx.ev.json(f"configuracion/resumen_{caso}.json", resumen_caso)


def resumir_runtime(ctx, caso, runtime):
    claves = [
        "id_ejecucion", "periodo_negocio", "lote_cargue_id", "estado_extraccion", "calidad_fuentes", "fecha_fin",
        "estado_preparacion_silver", "ruta_silver_preparada", "total_registros_silver_preparada",
        "fecha_preparacion_silver", "estado_validacion", "estado_remediacion",
    ]
    salida = {k: runtime.get(k) for k in claves if k in runtime}
    salida["otras_claves"] = sorted(k for k in runtime if k not in claves and k != "datasets_extraidos")
    datasets = []
    for d in runtime.get("datasets_extraidos") or runtime.get("datasets") or []:
        fila = {k: d.get(k) for k in (
            "tabla_relativa", "estado", "total_registros", "lote_cargue_id_dataset", "datos_nuevos",
            "origen_compartido", "ruta_dataset_origen", "ruta_dataset_destino", "fecha_fin", "motivo",
            "lotes_preparacion_silver", "lotes_segmento",
        ) if k in d}
        datasets.append(fila)
        estado = str(d.get("estado") or "").upper()
        if estado in {"SKIPPED_NOT_AVAILABLE", "REUSED"}:
            ctx.ev.alerta("RUN-DATASET-" + estado, f"{caso}: {d.get('tabla_relativa')} quedó {estado}")
        destino = str(d.get("ruta_dataset_destino") or "")
        if destino.startswith("s3") and "-bronce" in partes_s3(destino)[0]:
            ctx.ev.alerta("RUN-SILVER-EN-BRONCE", f"{caso}: la ruta Silver de {d.get('tabla_relativa')} apunta a un bucket de Bronce", ruta=destino)
    salida["datasets"] = datasets
    if str(runtime.get("periodo_negocio")) != ctx.periodo:
        ctx.ev.alerta("RUN-OTRO-PERIODO", f"{caso}: el runtime actual es de otro periodo; los datos del periodo pedido pudieron sobrescribirse",
                      runtime=str(runtime.get("periodo_negocio")), pedido=ctx.periodo)
    if runtime.get("calidad_fuentes") == "INCOMPLETA":
        ctx.ev.alerta("RUN-FUENTES-INCOMPLETAS", f"{caso}: la extracción terminó SUCCESS con calidad_fuentes=INCOMPLETA")
    return salida


@seccion("fuente")
def capturar_fuente(ctx):
    salida = {"conexiones_job": {}, "fuente_config": {}}
    try:
        job = ctx.glue.get_job(JobName=f"niif-{ctx.amb}-extraccion-estandar-emisiones")["Job"]
        nombres = (job.get("Connections") or {}).get("Connections", [])
    except Exception as exc:  # noqa: BLE001
        nombres = []
        ctx.ev.error("fuente:job", exc)
    for caso, config in ctx.configs_publicadas.items():
        fuente = config.get("fuente") or {}
        salida["fuente_config"][caso] = {k: v for k, v in fuente.items() if not PATRON_SECRETO.search(k)}
        if fuente.get("connection_name"):
            nombres.append(fuente["connection_name"].replace("{env}", ctx.amb))
    for nombre in sorted(set(nombres)):
        try:
            conexion = ctx.glue.get_connection(Name=nombre, HidePassword=True)["Connection"]
            propiedades = conexion.get("ConnectionProperties") or {}
            salida["conexiones_job"][nombre] = {
                "tipo": conexion.get("ConnectionType"),
                "jdbc_url": propiedades.get("JDBC_CONNECTION_URL"),
                "usa_secreto": bool(propiedades.get("SECRET_ID")),
                "ultima_modificacion": conexion.get("LastUpdatedTime"),
            }
        except Exception as exc:  # noqa: BLE001
            salida["conexiones_job"][nombre] = {"error": str(exc)[:200]}
    ctx.ev.json("fuente/conexiones.json", salida)


@seccion("almacenamiento")
def capturar_almacenamiento(ctx):
    salida = {}
    for caso, runtime in ctx.runtimes.items():
        datos_caso = {"lotes_bronce_del_periodo": {}, "checkpoints": {}, "silver_preparada": None}
        for d in runtime.get("datasets_extraidos") or []:
            origen = str(d.get("ruta_dataset_origen") or "")
            coincidencia = re.match(r"^(s3://.+?/periodo_negocio=[^/]+)/lote_cargue_id=", origen)
            if coincidencia:
                try:
                    datos_caso["lotes_bronce_del_periodo"][d.get("tabla_relativa")] = ctx.listar_prefijos(coincidencia.group(1))
                except Exception as exc:  # noqa: BLE001
                    datos_caso["lotes_bronce_del_periodo"][d.get("tabla_relativa")] = {"error": str(exc)[:200]}
            if origen.endswith("/data/"):
                control = origen[: -len("data/")] + "_control/_SUCCESS.json"
                try:
                    contenido, meta = ctx.leer_s3(control)
                    previo = json.loads(contenido)
                    datos_caso["checkpoints"][d.get("tabla_relativa")] = {
                        "estado": previo.get("estado"), "total_registros": previo.get("total_registros"),
                        "fecha_fin": previo.get("fecha_fin"), "ultima_modificacion": meta["ultima_modificacion"],
                    }
                except Exception as exc:  # noqa: BLE001
                    datos_caso["checkpoints"][d.get("tabla_relativa")] = {"error": str(exc)[:200]}
        varios = {t: len(v) for t, v in datos_caso["lotes_bronce_del_periodo"].items() if isinstance(v, list) and len(v) > 1}
        if varios:
            ctx.ev.alerta("ALM-VARIOS-LOTES", f"{caso}: hay más de un lote en Bronce para el periodo", tablas=varios)
        salida[caso] = datos_caso

    base_silver = f"s3://alfa-datalake-{ctx.amb}-silver/niif/preparacion_emisiones"
    for caso in CASOS:
        try:
            prefijos = ctx.listar_prefijos(f"{base_silver}/{caso}")
            salida.setdefault(caso, {})["silver_preparada"] = [ctx.resumen_objetos(f"s3://{partes_s3(base_silver)[0]}/{p}") for p in prefijos]
        except Exception as exc:  # noqa: BLE001
            salida.setdefault(caso, {})["silver_preparada"] = {"error": str(exc)[:200]}
    ctx.ev.json("almacenamiento/s3.json", salida)


# ------------------------------------------------------------------------ Athena

def _tablas_catalogo(ctx):
    """Tablas del catálogo de Glue de este ambiente con su ubicación en S3."""
    # No se supone el nombre de la base: se recorren todas y se filtra por la ubicación en S3.
    tablas = []
    for pagina in ctx.glue.get_paginator("get_databases").paginate():
        for base in pagina["DatabaseList"]:
            nombre_base = base["Name"]
            for pag_tablas in ctx.glue.get_paginator("get_tables").paginate(DatabaseName=nombre_base):
                for tabla in pag_tablas["TableList"]:
                    sd = tabla.get("StorageDescriptor") or {}
                    ubicacion = (sd.get("Location") or "").lower()
                    if f"-{ctx.amb}-" not in ubicacion or not any(p in ubicacion for p in ("preparacion_emisiones", "acsele")):
                        continue
                    tablas.append({
                        "base": nombre_base,
                        "tabla": tabla["Name"],
                        "ubicacion": (sd.get("Location") or "").replace("s3a://", "s3://").rstrip("/"),
                        "columnas": [(c["Name"].lower(), c["Type"].lower()) for c in sd.get("Columns", [])],
                        "particiones": [(c["Name"].lower(), c["Type"].lower()) for c in tabla.get("PartitionKeys", [])],
                    })
    return tablas


def _tabla_para_ruta(tablas, ruta):
    """Tabla del catálogo para una ruta exacta del runtime.

    Devuelve (tabla, filtro_path). Si hay una tabla con esa misma ubicación, no hace falta
    filtro. Si solo hay una tabla "padre", se filtra por la pseudo-columna "$path" para leer
    únicamente esa carpeta. Si no hay ninguna, devuelve (None, None) y NO se consulta: nunca
    se suma una tabla completa sin filtro (en la versión anterior eso tomó cargas viejas).
    """
    if not ruta:
        return None, None
    objetivo = str(ruta).replace("s3a://", "s3://").rstrip("/")
    exactas = [t for t in tablas if t["ubicacion"].lower() == objetivo.lower()]
    if exactas:
        return exactas[0], None
    padres = [t for t in tablas if objetivo.lower().startswith(t["ubicacion"].lower() + "/")]
    padres.sort(key=lambda t: -len(t["ubicacion"]))
    if padres:
        prefijo = objetivo.replace("'", "") + "/%"
        return padres[0], f"\"$path\" LIKE '{prefijo}'"
    return None, None


def _consultar(ctx, base, sql, nombre):
    ctx.ev.texto(f"datos/{nombre}.sql", sql + "\n")
    respuesta = ctx.athena.start_query_execution(
        QueryString=sql, QueryExecutionContext={"Database": base}, WorkGroup=ctx.args.workgroup
    )
    ejecucion = respuesta["QueryExecutionId"]
    for _ in range(300):
        estado = ctx.athena.get_query_execution(QueryExecutionId=ejecucion)["QueryExecution"]
        actual = estado["Status"]["State"]
        if actual in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            break
        time.sleep(2)
    else:
        raise TimeoutError(f"Athena no terminó la consulta {nombre}")
    if actual != "SUCCEEDED":
        raise RuntimeError(f"{nombre}: {actual} {estado['Status'].get('StateChangeReason', '')[:400]}")
    filas = []
    for pagina in ctx.athena.get_paginator("get_query_results").paginate(QueryExecutionId=ejecucion):
        for fila in pagina["ResultSet"]["Rows"]:
            filas.append([c.get("VarCharValue") for c in fila["Data"]])
    escaneado = (estado.get("Statistics") or {}).get("DataScannedInBytes")
    with open(ctx.ev.ruta(f"datos/{nombre}.csv"), "w", encoding="utf-8") as archivo:
        for fila in filas:
            archivo.write(",".join("" if v is None else '"' + str(v).replace('"', '""') + '"' for v in fila) + "\n")
    log(f"   {nombre}: {max(len(filas) - 1, 0)} filas, {escaneado} bytes escaneados")
    return filas, escaneado


def _filtro_texto(columna, valor):
    return f"CAST({columna} AS varchar) = '{str(valor).replace(chr(39), '')}'"


def _es_llave(columna):
    return columna.startswith(("id", "nro_", "codigo_", "cod_", "numero_")) or columna in {
        "lote_cargue_id", "unidad_riesgo", "periodo_negocio", "periodo_fechaemision", "periodo_emision",
    }


def _dataset(runtime, sufijo):
    for d in runtime.get("datasets_extraidos") or []:
        relativa = str(d.get("tabla_relativa") or "").lower().strip("/")
        if relativa == sufijo or relativa.endswith("/" + sufijo):
            return d
    return None


@seccion("datos")
def capturar_datos(ctx):
    if ctx.args.sin_athena:
        log("   Athena desactivado con --sin-athena")
        return
    if not ctx.runtimes:
        ctx.ev.error("datos", "No hay runtimes cargados: correr también la sección configuracion")
        return
    tablas = _tablas_catalogo(ctx)
    ctx.ev.json("datos/catalogo_tablas.json", [{k: t[k] for k in ("base", "tabla", "ubicacion", "particiones")} for t in tablas])
    resumen = {"silver": {}, "bronce": {}}

    # ---- Silver preparada: la carpeta exacta que publicó el runtime de cada caso.
    for caso, runtime in ctx.runtimes.items():
        ruta = runtime.get("ruta_silver_preparada")
        tabla, filtro_path = _tabla_para_ruta(tablas, ruta)
        if not tabla:
            resumen["silver"][caso] = {"ruta": ruta, "resultado": "SIN_TABLA_EN_CATALOGO_PARA_LA_RUTA"}
            ctx.ev.alerta("DAT-SIN-CATALOGO", f"{caso}: ninguna tabla del catálogo cubre {ruta}")
            continue
        columnas = dict(tabla["columnas"] + tabla["particiones"])
        producto = next((c for c in COLUMNAS_PRODUCTO if c in columnas), None)
        grupos = [c for c in (producto, "periodo_fechaemision", "periodo_emision") if c and c in columnas]
        montos = [c for c, tipo in tabla["columnas"] if tipo.startswith(TIPOS_MONTO) and c not in grupos and not _es_llave(c)]
        filtros = [f for f in (filtro_path,) if f]
        if "periodo_negocio" in columnas:
            filtros.append(_filtro_texto("periodo_negocio", ctx.periodo))
        donde = " AND ".join(filtros) or "1=1"
        llaves = [c for c in ("idpoliza", "nro_operacion", "unidad_riesgo") if c in columnas]
        extras = [f"array_join(array_agg(DISTINCT CAST({c} AS varchar)), '|') AS {c}s" for c in ("lote_cargue_id", "id_ejecucion") if c in columnas]
        if llaves:
            extras.append("COUNT(DISTINCT " + " || '#' || ".join(f"COALESCE(CAST({k} AS varchar), '')" for k in llaves) + ") AS filas_distintas_llave")
        sql = (
            "SELECT " + ", ".join(grupos + ["COUNT(*) AS filas"] + extras + [f"SUM({m}) AS {m}" for m in montos])
            + f' FROM "{tabla["base"]}"."{tabla["tabla"]}" WHERE {donde}'
            + (" GROUP BY " + ", ".join(grupos) + " ORDER BY " + ", ".join(grupos) if grupos else "")
        )
        resumen["silver"][caso] = {"ruta": ruta, "tabla": f'{tabla["base"]}.{tabla["tabla"]}', "filtro": donde,
                                   "columna_producto": producto, "montos": montos}
        try:
            _filas, escaneado = _consultar(ctx, tabla["base"], sql, f"silver_{caso}_totales")
            resumen["silver"][caso]["bytes_escaneados"] = escaneado
        except Exception as exc:  # noqa: BLE001
            ctx.ev.error(f"datos:silver:{caso}", exc)

        # Montos agrupados por (operación, póliza) en el job: si hay varias filas por llave,
        # sumar filas repite montos. Se toma un valor por (producto, operación, póliza).
        if producto and {"nro_operacion", "idpoliza"}.issubset(columnas) and montos:
            llave = [producto, "nro_operacion", "idpoliza"]
            sql_llave = (
                "SELECT " + ", ".join([producto, "COUNT(*) AS llaves", "SUM(filas) AS filas"] + [f"SUM({m}) AS {m}" for m in montos])
                + " FROM (SELECT " + ", ".join(llave + ["COUNT(*) AS filas"] + [f"MAX({m}) AS {m}" for m in montos])
                + f' FROM "{tabla["base"]}"."{tabla["tabla"]}" WHERE {donde} GROUP BY ' + ", ".join(llave) + ") x"
                + f" GROUP BY {producto} ORDER BY {producto}"
            )
            try:
                _consultar(ctx, tabla["base"], sql_llave, f"silver_{caso}_totales_por_operacion")
            except Exception as exc:  # noqa: BLE001
                ctx.ev.error(f"datos:silver_llave:{caso}", exc)

    # ---- Movimientos del lote de esta corrida (copia en Silver o Bronce, ruta exacta del runtime).
    for caso, runtime in ctx.runtimes.items():
        tpm, agp = _dataset(runtime, "thirdpartymovementpolicy"), _dataset(runtime, "agregatedpolicy")
        if not tpm or not agp:
            resumen["bronce"][caso] = "EL_RUNTIME_NO_TRAE_MOVIMIENTOS_O_POLIZAS"
            continue
        elegido = None
        for clave in ("ruta_dataset_origen", "ruta_dataset_destino"):  # primero el Bronce exacto del lote
            t, ft = _tabla_para_ruta(tablas, tpm.get(clave))
            a, fa = _tabla_para_ruta(tablas, agp.get(clave))
            if t and a:
                elegido = (clave, t, ft, a, fa)
                break
        if not elegido:
            resumen["bronce"][caso] = {"resultado": "SIN_TABLA_PARA_LA_RUTA_DEL_LOTE (no se consulta para no sumar otras cargas)",
                                       "rutas": {k: tpm.get(k) for k in ("ruta_dataset_destino", "ruta_dataset_origen")}}
            continue
        clave, t, ft, a, fa = elegido
        ct = dict(t["columnas"] + t["particiones"])
        if not {"concept", "ammount", "policyid"}.issubset(ct):
            resumen["bronce"][caso] = {"error": "la tabla elegida no tiene concept, ammount y policyid", "tabla": t["tabla"]}
            continue
        filtros_t = [f.replace('"$path"', 't."$path"') for f in (ft,) if f]
        filtros_a = [f for f in (fa,) if f]
        sql = (
            "SELECT CAST(ap.productid AS varchar) AS productid, upper(t.concept) AS concepto, "
            "COUNT(*) AS filas, SUM(CAST(t.ammount AS decimal(38,6))) AS total "
            f'FROM "{t["base"]}"."{t["tabla"]}" t '
            f'JOIN (SELECT DISTINCT agregatedpolicyid, productid FROM "{a["base"]}"."{a["tabla"]}" '
            f"WHERE {' AND '.join(filtros_a) or '1=1'}) ap "
            "ON CAST(ap.agregatedpolicyid AS varchar) = CAST(t.policyid AS varchar) "
            f"WHERE {' AND '.join(filtros_t) or '1=1'} "
            "GROUP BY 1, 2 ORDER BY 1, 2"
        )
        resumen["bronce"][caso] = {"origen": clave, "lote": runtime.get("lote_cargue_id"),
                                   "tabla_movimientos": f'{t["base"]}.{t["tabla"]}', "tabla_polizas": f'{a["base"]}.{a["tabla"]}'}
        try:
            _filas, escaneado = _consultar(ctx, t["base"], sql, f"bronce_{caso}_movimientos")
            resumen["bronce"][caso]["bytes_escaneados"] = escaneado
        except Exception as exc:  # noqa: BLE001
            ctx.ev.error(f"datos:bronce:{caso}", exc)
    ctx.ev.json("datos/resumen_datos.json", resumen)


# ------------------------------------------------------------------------ Terceros

# Montos que se acompañan por tomador y por agente (solo los que existan en la tabla).
MONTOS_TERCEROS = (
    "prima", "prima_vida", "comision_total_interincter", "comision_agente", "comision",
    "costo_recaudacion", "costo_recaudacion_vida", "costo_recaudo",
    "iva_costo_recaudacion", "iva_costo_recaudacion_vida", "iva_costo_recaudo",
)


def _normalizado_sql(columna):
    """Mismo normalizado que validar_terceros.py: espacios colapsados, sin bordes, mayúsculas."""
    return f"upper(trim(regexp_replace(coalesce(CAST({columna} AS varchar), ''), '\\s+', ' ')))"


def _huella_sql(columna):
    # Huella SHA-256 del nombre normalizado: permite cruzar con el RRC sin sacar nombres de AWS.
    return f"lower(to_hex(sha256(to_utf8({_normalizado_sql(columna)}))))"


@seccion("terceros")
def capturar_terceros(ctx):
    """Tomadores y agentes del Silver preparado, por producto (solo huellas y conteos)."""
    if not ctx.runtimes:
        ctx.ev.error("terceros", "No hay runtimes cargados: correr también la sección configuracion")
        return
    tablas = _tablas_catalogo(ctx)
    resumen = {}
    for caso, runtime in ctx.runtimes.items():
        tabla, filtro_path = _tabla_para_ruta(tablas, runtime.get("ruta_silver_preparada"))
        if not tabla:
            resumen[caso] = "SIN_TABLA_EN_CATALOGO_PARA_LA_RUTA"
            continue
        columnas = dict(tabla["columnas"] + tabla["particiones"])
        producto = next((c for c in COLUMNAS_PRODUCTO if c in columnas), None)
        if not producto:
            resumen[caso] = "SIN_COLUMNA_DE_PRODUCTO"
            continue
        filtros = [f for f in (filtro_path,) if f]
        if "periodo_negocio" in columnas:
            filtros.append(_filtro_texto("periodo_negocio", ctx.periodo))
        donde = " AND ".join(filtros) or "1=1"
        origen = f'"{tabla["base"]}"."{tabla["tabla"]}"'
        montos = [m for m in MONTOS_TERCEROS if m in columnas]
        operacion = "nro_operacion" if "nro_operacion" in columnas else None
        resumen[caso] = {"tabla": f'{tabla["base"]}.{tabla["tabla"]}', "filtro": donde,
                         "tomador": "tomador" in columnas, "agente": "agente" in columnas, "montos": montos}

        # 1) Completitud por producto.
        partes = [producto, "COUNT(*) AS filas"]
        for rol in ("tomador", "agente"):
            if rol in columnas:
                vacio = f"({rol} IS NULL OR trim(CAST({rol} AS varchar)) = '')"
                partes += [f"COUNT_IF({vacio}) AS sin_{rol}",
                           f"COUNT(DISTINCT {_normalizado_sql(rol)}) AS {rol}{'es' if rol == 'tomador' else 's'}_distintos",
                           f"COUNT_IF(strpos(CAST({rol} AS varchar), ',') > 0) AS {rol}_con_varios_nombres"]
        for ident in ("identificacion_tomador",):
            if ident in columnas:
                partes.append(f"COUNT_IF({ident} IS NULL OR trim(CAST({ident} AS varchar)) = '') AS sin_{ident}")
        sql = f"SELECT {', '.join(partes)} FROM {origen} WHERE {donde} GROUP BY {producto} ORDER BY {producto}"
        try:
            _consultar(ctx, tabla["base"], sql, f"terceros_{caso}_completitud")
        except Exception as exc:  # noqa: BLE001
            ctx.ev.error(f"terceros:completitud:{caso}", exc)

        # 2) y 3) Totales por producto y tomador / agente (huella del nombre, nunca el nombre).
        for rol in ("tomador", "agente"):
            if rol not in columnas:
                continue
            seleccion = [producto, f"{_huella_sql(rol)} AS {rol}_huella",
                         f"({rol} IS NULL OR trim(CAST({rol} AS varchar)) = '') AS {rol}_vacio",
                         "COUNT(*) AS filas"]
            if operacion:
                seleccion.append(f"COUNT(DISTINCT {operacion}) AS operaciones")
            seleccion += [f"SUM({m}) AS {m}" for m in montos]
            sql = (f"SELECT {', '.join(seleccion)} FROM {origen} WHERE {donde} "
                   f"GROUP BY 1, 2, 3 ORDER BY 1, 2")
            try:
                _consultar(ctx, tabla["base"], sql, f"terceros_{caso}_por_{rol}")
            except Exception as exc:  # noqa: BLE001
                ctx.ev.error(f"terceros:{rol}:{caso}", exc)
    ctx.ev.json("terceros/resumen_terceros.json", resumen)


# ------------------------------------------------------------------------ Oracle

def _renderizar_sql(tabla, config):
    """Replica render_sql/compile_sql de la extracción para las estrategias SQL_CONFIGURADA*."""
    estrategia = tabla.get("estrategia_lectura", "SQL_CONFIGURADA")
    esquema = tabla.get("schema") or (config.get("fuente") or {}).get("schema", "ACSELE_ALFA")
    ventana = config.get("ventana_extraccion") or {}
    if estrategia in {"SQL_CONFIGURADA", "SQL_CONFIGURADA_UNA_TABLA"}:
        sql = normalizar_sql(tabla.get("consulta_sql"))
    elif estrategia == "DIRECTA":
        columnas = tabla.get("columnas") or []
        proyeccion = ", ".join(f"T0.{str(c).upper()}" for c in columnas) if columnas else "T0.*"
        where = normalizar_sql(tabla.get("where")).replace("{ALIAS}", "T0") or "1=1"
        sql = f"SELECT {proyeccion} FROM {esquema}.{str(tabla.get('tabla')).upper()} T0 WHERE {where}"
    else:
        return None
    reemplazos = {
        "{SCHEMA}": (config.get("fuente") or {}).get("schema", "ACSELE_ALFA"),
        "{START}": str(ventana.get("fecha_inicio")),
        "{END}": str(ventana.get("fecha_fin")),
        "{START_DATE}": str(ventana.get("fecha_inicio"))[:10],
        "{END_DATE}": str(ventana.get("fecha_fin"))[:10],
        "{PERIODO_NEGOCIO}": str(config.get("periodo_negocio")),
        "{LOTE_CARGUE_ID}": str(config.get("lote_cargue_id")),
        "{ID_EJECUCION}": str(config.get("id_ejecucion")),
    }
    for marcador, valor in reemplazos.items():
        sql = sql.replace(marcador, valor)
    return re.sub(r"/\*\+.*?\*/", "", sql, flags=re.S)


@seccion("oracle")
def generar_sql_oracle(ctx):
    for caso, config in ctx.configs_publicadas.items():
        esquema = (config.get("fuente") or {}).get("schema", "ACSELE_ALFA")
        encabezado = (
            f"-- Generado por diag_emisiones_v2.py ({ctx.amb}, {caso}, periodo {config.get('periodo_negocio')}, "
            f"lote {config.get('lote_cargue_id')}).\n"
            "-- Son las MISMAS consultas que usó la extracción en este ambiente (sin hints).\n"
            "-- Correr en la Oracle ACSELE de ESTE ambiente, solo lectura. Exportar el resultado a CSV.\n\n"
        )
        conteos = [encabezado, "-- Conteo por tabla: comparar con total_registros del runtime (parametros_extraccion.json).\n"]
        for _origen, tabla in tablas_de_config(config):
            nombre = str(tabla.get("tabla_relativa") or tabla.get("tabla"))
            sql = _renderizar_sql(tabla, config)
            if sql is None:
                conteos.append(f"-- {nombre}: estrategia {tabla.get('estrategia_lectura')} no se replica aquí; usar el conteo del runtime.\n")
                continue
            conteos.append(f"SELECT '{nombre}' AS TABLA, COUNT(*) AS FILAS FROM ({sql}) Q;\n")
            if str(tabla.get("tabla")).upper() == "THIRDPARTYMOVEMENTPOLICY":
                ctx.ev.texto(f"oracle/{caso}_movimientos_por_producto_concepto.sql", encabezado + (
                    "-- Totales por producto y concepto sobre la consulta de movimientos de la extracción.\n"
                    "-- Se compara con datos/bronce_<caso>_movimientos.csv (misma forma).\n"
                    "SELECT TO_CHAR(AP.PRODUCTID) AS PRODUCTID, UPPER(Q.CONCEPT) AS CONCEPTO, COUNT(*) AS FILAS, SUM(Q.AMMOUNT) AS TOTAL\n"
                    f"FROM ({sql}) Q\n"
                    f"JOIN (SELECT DISTINCT AGREGATEDPOLICYID, PRODUCTID FROM {esquema}.AGREGATEDPOLICY) AP\n"
                    "  ON AP.AGREGATEDPOLICYID = Q.POLICYID\n"
                    "GROUP BY AP.PRODUCTID, UPPER(Q.CONCEPT)\nORDER BY 1, 2;\n"
                ))
        ctx.ev.texto(f"oracle/{caso}_conteos_por_tabla.sql", "".join(conteos))


# ------------------------------------------------------------------------ principal

def main():
    parser = argparse.ArgumentParser(description="Diagnóstico de solo lectura de la malla Emisiones")
    parser.add_argument("--amb", required=True, help="dev, qa o prd (como aparece en los nombres de recursos)")
    parser.add_argument("--periodo", default="202606")
    parser.add_argument("--workgroup", default="primary", help="Workgroup de Athena con ubicación de resultados")
    parser.add_argument("--ejecuciones", type=int, default=10)
    parser.add_argument("--salida", default=None)
    parser.add_argument("--sin-athena", action="store_true")
    parser.add_argument("--secciones", default="codigo,orquestacion,configuracion,fuente,almacenamiento,datos,terceros,oracle")
    args = parser.parse_args()
    if not re.fullmatch(r"[a-z]{2,5}", args.amb):
        parser.error("--amb inválido")
    if not re.fullmatch(r"\d{6}", args.periodo):
        parser.error("--periodo debe ser AAAAMM")

    ctx = Contexto(args)
    log(f"Diagnóstico Emisiones v{VERSION} | ambiente={ctx.amb} periodo={ctx.periodo} región={REGION} salida={ctx.ev.raiz}")
    pedidas = {s.strip() for s in args.secciones.split(",")}
    pasos = [
        ("codigo", capturar_codigo), ("orquestacion", capturar_orquestacion),
        ("configuracion", capturar_configuracion), ("fuente", capturar_fuente),
        ("almacenamiento", capturar_almacenamiento), ("datos", capturar_datos), ("terceros", capturar_terceros),
        ("oracle", generar_sql_oracle),
    ]
    for nombre, funcion in pasos:
        if nombre in pedidas:
            funcion(ctx)

    ctx.ev.json("resumen.json", {
        "version_script": VERSION,
        "ambiente": ctx.amb,
        "periodo": ctx.periodo,
        "fecha_utc": datetime.now(timezone.utc).isoformat(),
        "tablas_dynamodb": {"prg": ctx.prg_table, "cnf": ctx.cnf_table},
        "alertas": ctx.ev.alertas,
        "errores": ctx.ev.errores,
    })
    archivo_tar = ctx.ev.raiz.rstrip("/") + ".tar.gz"
    with tarfile.open(archivo_tar, "w:gz") as tar:
        tar.add(ctx.ev.raiz, arcname=os.path.basename(ctx.ev.raiz))
    log(f"Listo. Alertas: {len(ctx.ev.alertas)} | errores: {len(ctx.ev.errores)}")
    log(f"Descargar: {archivo_tar}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
