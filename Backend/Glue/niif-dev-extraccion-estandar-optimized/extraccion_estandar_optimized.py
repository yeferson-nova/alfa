import sys
import re
import json
import logging
import boto3
from urllib.parse import urlparse
from datetime import datetime

from botocore.exceptions import ClientError

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import lit, monotonically_increasing_id
from pyspark import StorageLevel


# =================================================
# LOGGING
# =================================================
logger = logging.getLogger(__name__)

# =================================================
# HELPERS GENERALES
# =================================================
def _leer_lotes_disponibles(spark, ruta, lotes):
    import boto3
    from urllib.parse import urlparse
    from pyspark.sql.functions import col

    uri = urlparse(ruta)
    if uri.scheme != "s3":
        raise ValueError("Ruta no S3: " + ruta)

    s3 = boto3.client("s3")
    hay_parquet = False
    for pagina in s3.get_paginator("list_objects_v2").paginate(
        Bucket=uri.netloc,
        Prefix=uri.path.lstrip("/").rstrip("/") + "/",
    ):
        if any(
            obj["Key"].lower().endswith(".parquet")
            and obj.get("Size", 0) > 0
            for obj in pagina.get("Contents", [])
        ):
            hay_parquet = True
            break

    if not hay_parquet:
        return None, set()

    # Los errores de permisos, lectura o esquema no se ocultan.
    df = spark.read.parquet(ruta)
    columnas = [c for c in df.columns if c.lower() == "lote_cargue_id"]
    if len(columnas) != 1:
        raise ValueError("Columna de lote ausente o ambigua: " + ruta)

    columna = columnas[0]
    presentes = {
        int(fila[0])
        for fila in (
            df.select(col(columna).cast("bigint").alias("_lote"))
            .filter(col("_lote").isin(lotes))
            .distinct()
            .collect()
        )
    }
    return df, presentes

def _lotes_requeridos_dataset(parametros, item, lote_actual):
    tabla = str(item.get("tabla_relativa", "")).strip().lower()
    referencias = parametros.get("lotes_usados_preparacion_silver") or {}

    candidatos = []
    for nombre, lotes in referencias.items():
        if str(nombre).strip().lower() == tabla:
            candidatos.append(lotes)

    candidatos.append(item.get("lotes_preparacion_silver"))

    for segmento in parametros.get("segmentos_preparacion", []):
        if str(segmento.get("tabla_relativa", "")).strip().lower() == tabla:
            candidatos.append(segmento.get("lotes_segmento"))

    candidatos.extend([
        item.get("lotes_segmento"),
        (item.get("segmentacion_horizontal") or {}).get("lotes_segmento"),
        item.get("lote_cargue_id_dataset"),
        lote_actual,
    ])

    for valor in candidatos:
        if valor is None:
            continue
        if not isinstance(valor, (list, tuple, set)):
            valor = [valor]
        lotes = sorted(set(int(x) for x in valor if x is not None))
        if lotes:
            return lotes

    raise ValueError("Sin lotes requeridos para " + tabla)


def _reconciliar_disponibilidad_final(spark, parametros):
    for item in parametros.get("datasets_extraidos", []):
        if item.get("estado") != "SUCCESS":
            continue
        if not item.get("requiere_preparacion_silver", False):
            continue

        tabla = item.get("tabla_relativa")
        lotes = _lotes_requeridos_dataset(
            parametros, item, parametros["lote_cargue_id"]
        )
        ruta_bronze = item.get("ruta_dataset_origen")
        ruta_silver = item.get("ruta_dataset_destino")
        if not ruta_bronze or not ruta_silver:
            raise ValueError(
                f"{tabla}: faltan rutas para comprobar disponibilidad"
            )

        _, presentes_silver = _leer_lotes_disponibles(
            spark, ruta_silver, lotes
        )
        pendientes = sorted(set(lotes) - presentes_silver)
        presentes_bronze = set()

        if pendientes:
            _, presentes_bronze = _leer_lotes_disponibles(
                spark, ruta_bronze, pendientes
            )

        ausentes = sorted(set(pendientes) - presentes_bronze)
        sh = item.get("segmentacion_horizontal") or {}

        espera_filas = (
            int(item.get("total_registros") or 0) > 0
            or int(
                item.get("registros_excluidos_por_segmento")
                or sh.get("registros_excluidos_por_segmento")
                or 0
            ) > 0
            or any(
                int(lote) != int(parametros["lote_cargue_id"])
                for lote in pendientes
            )
        )

        if ausentes and espera_filas:
            raise ValueError(
                f"{tabla}: lotes requeridos ausentes en Bronze "
                f"y Silver: {ausentes}. No se declara disponibilidad."
            )

        recuperar = sorted(presentes_bronze)
        item["lotes_disponibles_silver"] = sorted(presentes_silver)
        item["lotes_pendientes_disponibilizacion_silver"] = recuperar
        item["disponibilidad_silver_verificada_por_lote"] = True

        # No cambiar el lote de ejecución ni las referencias originales.
        if recuperar:
            item["requiere_validacion_tecnica"] = True
            item["requiere_disponibilizacion_silver"] = True
            item["modo_disponibilidad"] = "PENDIENTE_DESDE_BRONZE"
        else:
            item["requiere_disponibilizacion_silver"] = False

        logger.info(
            f"{tabla}: requeridos={lotes}, "
            f"disponibles_silver={sorted(presentes_silver)}, "
            f"pendientes_desde_bronze={recuperar}"
        )

def ahora_dt():
    return datetime.now()


def ahora_str():
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def segundos_entre(inicio, fin):
    return int((fin - inicio).total_seconds())


def minutos(segundos):
    return None if segundos is None else round(float(segundos) / 60.0, 2)


def normalizar_texto(valor):
    return str(valor).lower().strip().replace(" ", "_")


def ensure_trailing_slash(path):
    return path if path.endswith("/") else path + "/"


def parse_s3_uri(ruta_s3):
    parsed = urlparse(ruta_s3)
    return parsed.netloc, parsed.path.lstrip("/")


def existe_s3(ruta_s3):
    bucket, key = parse_s3_uri(ruta_s3)
    s3 = boto3.client("s3")

    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in ["404", "NoSuchKey", "NotFound"]:
            return False
        raise


def existe_prefijo_s3(ruta_s3):
    bucket, prefix = parse_s3_uri(ruta_s3)
    s3 = boto3.client("s3")

    resp = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix,
        MaxKeys=1
    )

    return "Contents" in resp


def obtener_lotes_desde_ruta_llaves(ruta_llaves_base):
    if not ruta_llaves_base:
        return []

    bucket, prefix = parse_s3_uri(ruta_llaves_base)
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    lotes = set()

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            match = re.search(r"lote_cargue_id=(\d+)", key)
            if match:
                lotes.add(int(match.group(1)))

    return sorted(list(lotes))


def cargar_json_s3(ruta_s3):
    bucket, key = parse_s3_uri(ruta_s3)
    s3 = boto3.client("s3")

    obj = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))


def guardar_json_s3(ruta_s3, contenido):
    bucket, key = parse_s3_uri(ruta_s3)
    s3 = boto3.client("s3")

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(contenido, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json"
    )


# =================================================
# HELPERS EXTRACCION
# =================================================
def construir_dbtable(schema, tabla, usar_schema_en_dbtable=True):
    return f"{schema}.{tabla}" if usar_schema_en_dbtable else tabla


def construir_query_where(tabla_cfg, usar_schema_en_dbtable=True):
    dbtable = construir_dbtable(
        schema=tabla_cfg["schema"],
        tabla=tabla_cfg["tabla"],
        usar_schema_en_dbtable=usar_schema_en_dbtable
    )

    where = tabla_cfg.get("where")
    query = f"SELECT * FROM {dbtable}"

    return f"{query} WHERE {where}" if where else query


def normalizar_tabla_relativa(tabla_cfg):
    schema = tabla_cfg["schema"]
    tabla = tabla_cfg["tabla"]

    return tabla_cfg.get(
        "tabla_relativa",
        f"{schema.lower()}/{tabla.lower()}"
    ).lower().strip("/")


def construir_ruta_catalogable(tabla_cfg, sistema_origen):
    sistema = normalizar_texto(sistema_origen)
    schema = normalizar_texto(tabla_cfg["schema"])
    tabla = normalizar_texto(tabla_cfg["tabla"])

    return f"{schema}/{sistema}_{schema}_{tabla}"


def construir_nombre_tabla_catalogo(tabla_cfg, sistema_origen):
    sistema = normalizar_texto(sistema_origen)
    schema = normalizar_texto(tabla_cfg["schema"])
    tabla = normalizar_texto(tabla_cfg["tabla"])

    return f"{sistema}_{schema}_{tabla}"


def puede_reutilizar_snapshot(tabla_cfg):
    reutilizacion = tabla_cfg.get("reutilizacion_compartida", {}) or {}

    return (
        bool(reutilizacion.get("habilitada", False))
        and not bool(
            tabla_cfg.get(
                "segmentacion_horizontal",
                {}
            ).get("habilitada", False)
        )
    )


def obtener_lote_snapshot_vigente(spark, ruta):
    if not existe_prefijo_s3(ruta):
        return None, None

    df = spark.read.parquet(ensure_trailing_slash(ruta))
    columnas = {c.lower(): c for c in df.columns}

    if "lote_cargue_id" not in columnas:
        return None, None

    lote_col = columnas["lote_cargue_id"]

    lote = (
        df
        .selectExpr(
            f"max(cast(`{lote_col}` as bigint)) "
            "as lote_vigente"
        )
        .first()["lote_vigente"]
    )

    if lote is None:
        return None, None

    total = (
        df
        .filter(
            df[lote_col].cast("bigint") == int(lote)
        )
        .count()
    )

    return int(lote), int(total)


def upsert_dataset_extraido(parametros_extraccion, registro_dataset):
    tabla_relativa = registro_dataset["tabla_relativa"].lower()

    datasets = parametros_extraccion.get("datasets_extraidos", [])
    datasets = [
        d for d in datasets
        if d.get("tabla_relativa", "").lower() != tabla_relativa
    ]

    datasets.append(registro_dataset)
    parametros_extraccion["datasets_extraidos"] = datasets


# =================================================
# CHECKPOINTS
# =================================================
def crear_checkpoint_base(guardar_detalle_eventos=False):
    checkpoint = {
        "estado": "IN_PROGRESS",
        "ciclo_actual": None,
        "segmento_actual": None,
        "tabla_actual": None,
        "ultimo_ciclo_ok": None,
        "ultimo_segmento_ok": None,
        "ultima_tabla_ok": None,
        "tablas_exitosas": []
    }

    if guardar_detalle_eventos:
        checkpoint["detalle"] = []

    return checkpoint


def actualizar_checkpoint(
    parametros_extraccion,
    estado,
    guardar_detalle_eventos=False,
    ciclo=None,
    segmento=None,
    tabla=None,
    mensaje=None
):
    checkpoint = parametros_extraccion.get(
        "checkpoint_extraccion",
        crear_checkpoint_base(guardar_detalle_eventos)
    )

    checkpoint["estado"] = estado

    if ciclo:
        checkpoint["ciclo_actual"] = {
            "orden": ciclo.get("orden"),
            "nombre": ciclo.get("nombre")
        }

    if segmento:
        checkpoint["segmento_actual"] = {
            "orden": segmento.get("orden"),
            "nombre": segmento.get("nombre"),
            "forma": segmento.get("forma")
        }

    if tabla:
        checkpoint["tabla_actual"] = {
            "orden": tabla.get("orden"),
            "schema": tabla.get("schema"),
            "tabla": tabla.get("tabla"),
            "tabla_relativa": normalizar_tabla_relativa(tabla)
        }

    if guardar_detalle_eventos and mensaje:
        checkpoint.setdefault("detalle", []).append({
            "fecha": ahora_str(),
            "estado": estado,
            "mensaje": mensaje
        })

    parametros_extraccion["checkpoint_extraccion"] = checkpoint


def registrar_ok_tabla(parametros_extraccion, tabla_cfg, guardar_detalle_eventos):
    checkpoint = parametros_extraccion.get(
        "checkpoint_extraccion",
        crear_checkpoint_base(guardar_detalle_eventos)
    )

    tabla_relativa = normalizar_tabla_relativa(tabla_cfg)

    checkpoint["ultima_tabla_ok"] = {
        "orden": tabla_cfg.get("orden"),
        "schema": tabla_cfg.get("schema"),
        "tabla": tabla_cfg.get("tabla"),
        "tabla_relativa": tabla_relativa
    }

    tablas_exitosas = set(checkpoint.get("tablas_exitosas", []))
    tablas_exitosas.add(tabla_relativa)

    checkpoint["tablas_exitosas"] = sorted(list(tablas_exitosas))
    parametros_extraccion["checkpoint_extraccion"] = checkpoint


def registrar_ok_segmento(parametros_extraccion, segmento, guardar_detalle_eventos):
    checkpoint = parametros_extraccion.get(
        "checkpoint_extraccion",
        crear_checkpoint_base(guardar_detalle_eventos)
    )

    checkpoint["ultimo_segmento_ok"] = {
        "orden": segmento.get("orden"),
        "nombre": segmento.get("nombre"),
        "forma": segmento.get("forma")
    }

    parametros_extraccion["checkpoint_extraccion"] = checkpoint


def registrar_ok_ciclo(parametros_extraccion, ciclo, guardar_detalle_eventos):
    checkpoint = parametros_extraccion.get(
        "checkpoint_extraccion",
        crear_checkpoint_base(guardar_detalle_eventos)
    )

    checkpoint["ultimo_ciclo_ok"] = {
        "orden": ciclo.get("orden"),
        "nombre": ciclo.get("nombre")
    }

    parametros_extraccion["checkpoint_extraccion"] = checkpoint


def obtener_tablas_exitosas(parametros_extraccion):
    tablas = set()

    for item in parametros_extraccion.get("datasets_extraidos", []):
        if item.get("estado") == "SUCCESS" and item.get("tabla_relativa"):
            tablas.add(item["tabla_relativa"].lower())

    checkpoint = parametros_extraccion.get("checkpoint_extraccion", {})

    for tabla in checkpoint.get("tablas_exitosas", []):
        tablas.add(tabla.lower())

    return tablas


# =================================================
# SEGMENTACION
# =================================================
def construir_segmento_id(sistema_origen, periodo_negocio, tabla_cfg, segmentacion):
    segmento_id_cfg = segmentacion.get("segmento_id")

    if segmento_id_cfg:
        return normalizar_texto(segmento_id_cfg)

    periodo = str(periodo_negocio) if periodo_negocio is not None else "sin_periodo"
    sistema = normalizar_texto(sistema_origen)
    schema = normalizar_texto(tabla_cfg["schema"])
    tabla = normalizar_texto(tabla_cfg["tabla"])

    return f"niif17_{periodo}_{sistema}_{schema}_{tabla}"


def construir_metadata_segmentacion(
    nombre_caso,
    sistema_origen,
    periodo_negocio,
    tabla_cfg,
    lote_actual
):
    tabla_relativa = normalizar_tabla_relativa(tabla_cfg)
    segmentacion = tabla_cfg.get("segmentacion_horizontal", {}) or {}

    habilitada = bool(segmentacion.get("habilitada", False))

    segmento_id = construir_segmento_id(
        sistema_origen=sistema_origen,
        periodo_negocio=periodo_negocio,
        tabla_cfg=tabla_cfg,
        segmentacion=segmentacion
    )

    lotes_segmento = segmentacion.get("lotes_segmento") or [lote_actual]

    if lote_actual not in lotes_segmento:
        lotes_segmento.append(lote_actual)

    return {
        "habilitada": habilitada,
        "segmento_id": segmento_id,
        "id_negocio": segmentacion.get("id_negocio", nombre_caso),
        "periodo_negocio": segmentacion.get("periodo_negocio", periodo_negocio),
        "lote_actual": lote_actual,
        "lotes_segmento": sorted(list(set(lotes_segmento))),
        "llaves_segmentacion": segmentacion.get("llaves_segmentacion", []),
        "modo_segmentacion": segmentacion.get("modo", "NINGUNO"),
        "requiere_preparacion_con_lotes_segmento": bool(
            segmentacion.get("requiere_preparacion_con_lotes_segmento", habilitada)
        ),
        "ruta_llaves_segmento_base": None,
        "exclusion_aplicada": False,
        "registros_excluidos_por_segmento": 0,
        "observacion": "Metadata de segmentación registrada." if habilitada else None
    }


def construir_ruta_llaves_segmento_base(ruta_segmentos_base, segmento_id):
    segmento_id_norm = normalizar_texto(segmento_id)

    return (
        f"{ensure_trailing_slash(ruta_segmentos_base)}"
        f"{segmento_id_norm}/llaves/"
    )


def construir_ruta_llaves_segmento(ruta_segmentos_base, segmento_id, lote_cargue_id):
    return (
        f"{construir_ruta_llaves_segmento_base(ruta_segmentos_base, segmento_id)}"
        f"lote_cargue_id={lote_cargue_id}/"
    )


def resolver_llaves_reales(df, llaves_segmentacion, tabla_relativa):
    columnas_df = {c.upper(): c for c in df.columns}
    llaves_reales = []

    for llave in llaves_segmentacion:
        llave_upper = llave.upper()

        if llave_upper not in columnas_df:
            raise ValueError(
                f"La llave de segmentación '{llave}' no existe en {tabla_relativa}. "
                f"Columnas disponibles: {df.columns}"
            )

        llaves_reales.append(columnas_df[llave_upper])

    return llaves_reales

def combinar_lotes_respaldados(previos, actual, hay_nuevos, tabla):
    """Une los lotes de respaldo de las llaves excluidas con el actual."""
    if not previos:
        raise ValueError(
            f"{tabla}: exclusion sin lotes de respaldo validados"
        )
    lotes = {int(x) for x in previos}
    if hay_nuevos:
        lotes.add(int(actual))
    return sorted(lotes)


def aplicar_exclusion_llaves_segmento(
    spark,
    df,
    metadata_segmentacion,
    tabla_relativa,
    ruta_segmentos_base,
    excluir_llaves_segmento,
    ruta_bronze,
    ruta_silver
):
    import boto3
    from urllib.parse import urlparse
    from pyspark.sql import functions as F

    metadata = {
        "exclusion_aplicada": False,
        "ruta_llaves_segmento_base": None,
        "registros_excluidos_por_segmento": 0
    }

    if (
        not excluir_llaves_segmento
        or not metadata_segmentacion.get("habilitada", False)
        or metadata_segmentacion.get(
            "modo_segmentacion", "NINGUNO"
        ).upper() != "EXCLUIR_EXTRAIDOS"
    ):
        return df, metadata

    llaves = metadata_segmentacion.get("llaves_segmentacion", [])
    if not llaves:
        raise ValueError("Segmentación sin llaves: " + tabla_relativa)

    reales = resolver_llaves_reales(
        df=df,
        llaves_segmentacion=llaves,
        tabla_relativa=tabla_relativa
    )

    ruta_llaves = construir_ruta_llaves_segmento_base(
        ruta_segmentos_base,
        metadata_segmentacion["segmento_id"]
    )
    metadata["ruta_llaves_segmento_base"] = ruta_llaves

    if not existe_prefijo_s3(ruta_llaves):
        return df, metadata

    def seleccionar_claves(frame, incluir_lote=True):
        columnas = {c.lower(): c for c in frame.columns}
        necesarias = list(reales)
        if incluir_lote:
            necesarias.append("lote_cargue_id")

        faltantes = [
            c for c in necesarias if c.lower() not in columnas
        ]
        if faltantes:
            raise ValueError(
                f"{tabla_relativa}: faltan columnas {faltantes}"
            )

        expresiones = [
            F.col(columnas[c.lower()]).alias(c)
            for c in reales
        ]
        if incluir_lote:
            expresiones.append(
                F.col(columnas["lote_cargue_id"])
                .cast("bigint").alias("_lote_respaldo")
            )
        return frame.select(*expresiones).dropDuplicates()

    previas = seleccionar_claves(spark.read.parquet(ruta_llaves))

    # Comprobar solo las claves presentes en la entrada Oracle actual.
    entrada = df.select(*reales).dropDuplicates()
    requeridas = previas.join(entrada, reales, "inner").cache()

    if not requeridas.take(1):
        requeridas.unpersist()
        return df, metadata

    if requeridas.filter(F.col("_lote_respaldo").isNull()).take(1):
        raise ValueError(
            f"{tabla_relativa}: llaves previas sin lote válido"
        )

    s3 = boto3.client("s3")
    disponibles = None

    for ruta in (ruta_bronze, ruta_silver):
        uri = urlparse(ruta)
        if uri.scheme != "s3":
            raise ValueError("Ruta inesperada: " + ruta)

        hay_parquet = False
        for pagina in s3.get_paginator("list_objects_v2").paginate(
            Bucket=uri.netloc,
            Prefix=uri.path.lstrip("/").rstrip("/") + "/"
        ):
            if any(
                o["Key"].lower().endswith(".parquet")
                and o.get("Size", 0) > 0
                for o in pagina.get("Contents", [])
            ):
                hay_parquet = True
                break

        if not hay_parquet:
            continue

        respaldo = seleccionar_claves(spark.read.parquet(ruta))
        respaldo = respaldo.join(
            requeridas,
            reales + ["_lote_respaldo"],
            "left_semi"
        )
        disponibles = (
            respaldo if disponibles is None
            else disponibles.unionByName(respaldo)
        )

    if disponibles is None:
        raise ValueError(
            f"{tabla_relativa}: hay llaves previas pero no hay "
            "datos disponibles en Bronze ni Silver. No se excluye."
        )

    faltantes = requeridas.join(
        disponibles.dropDuplicates(),
        reales + ["_lote_respaldo"],
        "left_anti"
    )
    muestra = faltantes.limit(3).collect()
    if muestra:
        raise ValueError(
            f"{tabla_relativa}: claves sin respaldo en su lote "
            f"Bronze/Silver. No se excluye. Muestra: {muestra}"
        )

    claves_excluir = requeridas.select(*reales).dropDuplicates()
    total = df.join(claves_excluir, reales, "inner").count()
    resultado = df.join(claves_excluir, reales, "left_anti")

    # LOTES_RESPALDO_EXCLUSION_V2
    metadata["lotes_respaldo_exclusion"] = sorted({
        int(fila["_lote_respaldo"])
        for fila in requeridas.select("_lote_respaldo").distinct().collect()
    })
    metadata["exclusion_aplicada"] = True
    metadata["registros_excluidos_por_segmento"] = total

    logger.info(
        f"{tabla_relativa}: exclusión respaldada por clave y lote. "
        f"Filas excluidas={total}"
    )
    # No liberar requeridas aquí: resultado conserva una lectura perezosa.
    return resultado, metadata


def guardar_llaves_segmentacion_s3(
    df_brz,
    metadata_segmentacion,
    nombre_caso,
    sistema_origen,
    tabla_relativa,
    periodo_negocio,
    lote_cargue_id,
    ruta_segmentos_base
):
    if not metadata_segmentacion.get("habilitada", False):
        return None

    llaves_segmentacion = metadata_segmentacion.get("llaves_segmentacion", [])

    if not llaves_segmentacion:
        return None

    llaves_reales = resolver_llaves_reales(
        df=df_brz,
        llaves_segmentacion=llaves_segmentacion,
        tabla_relativa=tabla_relativa
    )

    segmento_id = metadata_segmentacion["segmento_id"]

    ruta_llaves_base = construir_ruta_llaves_segmento_base(
        ruta_segmentos_base,
        segmento_id
    )

    ruta_llaves = construir_ruta_llaves_segmento(
        ruta_segmentos_base,
        segmento_id,
        lote_cargue_id
    )

    metadata_segmentacion["ruta_llaves_segmento_base"] = ruta_llaves_base

    df_llaves = (
        df_brz
        .select(*llaves_reales)
        .dropDuplicates(llaves_reales)
        .withColumn("segmento_id", lit(segmento_id))
        .withColumn("nombre_caso", lit(nombre_caso))
        .withColumn("sistema_origen", lit(sistema_origen))
        .withColumn("tabla_relativa", lit(tabla_relativa))
        .withColumn("periodo_negocio", lit(str(periodo_negocio)))
        .withColumn("lote_cargue_id", lit(lote_cargue_id).cast("bigint"))
        .withColumn("fecha_extraccion", lit(ahora_str()))
    )

    columnas_salida = [
        "segmento_id",
        "nombre_caso",
        "sistema_origen",
        "tabla_relativa",
        "periodo_negocio",
        "lote_cargue_id",
        "fecha_extraccion"
    ] + llaves_reales

    df_llaves = df_llaves.select(*columnas_salida)

    (
        df_llaves
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(ruta_llaves)
    )

    total_llaves = df_llaves.count()

    return {
        "ruta_llaves_segmento_base": ruta_llaves_base,
        "ruta_llaves_segmento": ruta_llaves,
        "total_llaves_segmento": total_llaves
    }


def upsert_segmento_preparacion(
    parametros_extraccion,
    metadata_segmentacion,
    tabla_relativa,
    metadata_llaves_segmento=None
):
    segmentos = parametros_extraccion.get("segmentos_preparacion", [])
    segmento_id = metadata_segmentacion["segmento_id"]
    id_negocio = metadata_segmentacion.get("id_negocio")
    periodo_negocio = metadata_segmentacion.get("periodo_negocio")

    segmentos = [
        s for s in segmentos
        if not (
            s.get("segmento_id") == segmento_id
            and s.get("tabla_relativa") == tabla_relativa
            and s.get("id_negocio") == id_negocio
            and str(s.get("periodo_negocio")) == str(periodo_negocio)
        )
    ]

    ruta_llaves_segmento_base = (
        metadata_llaves_segmento.get("ruta_llaves_segmento_base")
        if metadata_llaves_segmento
        else metadata_segmentacion.get("ruta_llaves_segmento_base")
    )

    segmentos.append({
        "segmento_id": segmento_id,
        "tabla_relativa": tabla_relativa,
        "id_negocio": id_negocio,
        "periodo_negocio": periodo_negocio,
        "lote_actual": metadata_segmentacion.get("lote_actual"),
        "lotes_segmento": metadata_segmentacion.get("lotes_segmento", []),
        "llaves_segmentacion": metadata_segmentacion.get("llaves_segmentacion", []),
        "requiere_preparacion_con_lotes_segmento": metadata_segmentacion.get(
            "requiere_preparacion_con_lotes_segmento",
            False
        ),
        "ruta_llaves_segmento_base": ruta_llaves_segmento_base,
        "ruta_llaves_segmento": (
            metadata_llaves_segmento.get("ruta_llaves_segmento")
            if metadata_llaves_segmento
            else None
        ),
        "total_llaves_segmento": (
            metadata_llaves_segmento.get("total_llaves_segmento")
            if metadata_llaves_segmento
            else None
        ),
        "exclusion_aplicada": metadata_segmentacion.get("exclusion_aplicada", False),
        "registros_excluidos_por_segmento": metadata_segmentacion.get(
            "registros_excluidos_por_segmento",
            0
        )
    })

    parametros_extraccion["segmentos_preparacion"] = segmentos


# =================================================
# PARAMETROS GLUE
# =================================================
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "RUTA_CONFIG_EXTRACCION"
    ]
)

ruta_config_extraccion = args["RUTA_CONFIG_EXTRACCION"]


# =================================================
# CONTEXTO GLUE
# =================================================
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.caseSensitive", "false")

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# =================================================
# CARGA CONFIG
# =================================================
config = cargar_json_s3(ruta_config_extraccion)

nombre_caso = config["nombre_caso"]

fuente = config["fuente"]
sistema_origen = fuente.get("sistema")

if not sistema_origen:
    raise ValueError("El JSON debe contener fuente.sistema.")

connection_name = fuente["connection_name"]
usar_schema_en_dbtable = bool(fuente.get("usar_schema_en_dbtable", True))
fetchsize = str(fuente.get("fetchsize", "10000"))

rutas = config["rutas"]
s3_bronze_base = ensure_trailing_slash(rutas["s3_bronze_base"])
s3_silver_base = ensure_trailing_slash(rutas["s3_silver_base"])
ruta_parametros_extraccion = rutas["ruta_parametros_extraccion"]

ruta_segmentos_base = rutas.get("ruta_segmentos_base")

if not ruta_segmentos_base:
    raise ValueError(
        "El JSON debe contener rutas.ruta_segmentos_base."
    )

ruta_segmentos_base = ensure_trailing_slash(
    ruta_segmentos_base
)

control = config.get("control_extraccion", {})

limite_unidades = control.get("limite_unidades_extraccion")
calcular_unidades = control.get("calcular_unidades_extraccion", True)
accion_si_supera_limite = control.get("accion_si_supera_limite", "ADVERTIR").upper()
permitir_reanudacion = control.get("permitir_reanudacion", True)
usar_checkpoints = control.get("usar_checkpoints", True)
contar_registros = control.get("contar_registros", True)
persistir_dataframe = control.get("persistir_dataframe", True)
excluir_llaves_segmento = bool(control.get("excluir_llaves_segmento", False))

modo_ejecucion = control.get("modo_ejecucion", "INICIAL").upper()
periodo_negocio = control.get("periodo_negocio") or config.get("periodo_negocio")

if not periodo_negocio:
    raise ValueError("El JSON debe contener periodo_negocio.")

if modo_ejecucion not in ["INICIAL", "REPROCESO", "RECARGUE_HISTORICO"]:
    raise ValueError(
        f"modo_ejecucion no soportado: {modo_ejecucion}. "
        "Valores válidos: INICIAL, REPROCESO, RECARGUE_HISTORICO"
    )

if accion_si_supera_limite not in ["ADVERTIR", "FALLAR"]:
    raise ValueError(
        f"accion_si_supera_limite no soportada: {accion_si_supera_limite}."
    )

salida_runtime = config.get("salida_runtime", {})
estructura_dataset = salida_runtime.get("estructura_dataset", {})
checkpoint_cfg = salida_runtime.get("checkpoint_extraccion", {})

agregar_nro_registro = estructura_dataset.get("agregar_nro_registro", True)
agregar_lote_cargue_id = estructura_dataset.get("agregar_lote_cargue_id", True)
modo_escritura = estructura_dataset.get("modo_escritura", "overwrite").lower()
compresion = estructura_dataset.get("compresion", "snappy")

guardar_detalle_eventos = checkpoint_cfg.get("guardar_detalle_eventos", False)
guardar_checkpoint_inicio_tabla = checkpoint_cfg.get("guardar_checkpoint_inicio_tabla", False)
saltar_tablas_exitosas = checkpoint_cfg.get(
    "saltar_tablas_exitosas_en_reanudacion",
    True
)

if modo_escritura not in ["overwrite", "append"]:
    raise ValueError("modo_escritura debe ser overwrite o append.")

ciclos = sorted(config.get("ciclos", []), key=lambda x: x.get("orden", 0))

if not ciclos:
    raise ValueError("El JSON de extracción no contiene ciclos.")


# =================================================
# PARAMETROS DE EJECUCION
# =================================================
fecha_inicio_job_dt = ahora_dt()
timestamp_ingesta = int(fecha_inicio_job_dt.timestamp())
fecha_ejecucion = fecha_inicio_job_dt.strftime("%Y-%m-%dT%H:%M:%S")

reanudar_ejecucion = False
parametros_extraccion = None

if permitir_reanudacion and existe_s3(ruta_parametros_extraccion):
    parametros_existentes = cargar_json_s3(ruta_parametros_extraccion)
    estado_anterior = parametros_existentes.get("estado_extraccion")

    if estado_anterior in ["FAILED", "IN_PROGRESS"]:
        parametros_extraccion = parametros_existentes
        timestamp_ingesta = parametros_extraccion["lote_cargue_id"]
        reanudar_ejecucion = True

        parametros_extraccion["estado_extraccion"] = "IN_PROGRESS"
        parametros_extraccion["fecha_reanudacion"] = fecha_ejecucion

if parametros_extraccion is None:
    parametros_extraccion = {
        "execution_id": f"bronze_{nombre_caso}_{timestamp_ingesta}",
        "nombre_caso": nombre_caso,
        "sistema_origen": sistema_origen,
        "periodo_negocio": periodo_negocio,
        "modo_ejecucion": modo_ejecucion,
        "lote_cargue_id": timestamp_ingesta,
        "fecha_ejecucion": fecha_ejecucion,
        "estado_extraccion": "IN_PROGRESS",
        "ruta_origen_base": s3_bronze_base,
        "ruta_destino_base": s3_silver_base,
        "ruta_config_extraccion": ruta_config_extraccion,
        "datasets_extraidos": [],
        "segmentos_preparacion": [],
        "checkpoint_extraccion": crear_checkpoint_base(guardar_detalle_eventos),
        "reanudacion": {
            "habilitada": permitir_reanudacion,
            "es_reanudacion": False
        }
    }
else:
    parametros_extraccion.setdefault("datasets_extraidos", [])
    parametros_extraccion.setdefault("segmentos_preparacion", [])
    parametros_extraccion.setdefault(
        "checkpoint_extraccion",
        crear_checkpoint_base(guardar_detalle_eventos)
    )

    parametros_extraccion["sistema_origen"] = sistema_origen
    parametros_extraccion["periodo_negocio"] = periodo_negocio
    parametros_extraccion["modo_ejecucion"] = modo_ejecucion
    parametros_extraccion["reanudacion"] = {
        "habilitada": permitir_reanudacion,
        "es_reanudacion": reanudar_ejecucion,
        "fecha_reanudacion": fecha_ejecucion if reanudar_ejecucion else None
    }

logger.info("=== INICIO JOB EXTRACCION ESTANDAR NIIF ===")
logger.info(f"Nombre caso            : {nombre_caso}")
logger.info(f"Sistema origen         : {sistema_origen}")
logger.info(f"Periodo negocio        : {periodo_negocio}")
logger.info(f"Modo ejecución         : {modo_ejecucion}")
logger.info(f"Excluir llaves         : {excluir_llaves_segmento}")
logger.info(f"Lote cargue id         : {timestamp_ingesta}")
logger.info(f"Ruta config            : {ruta_config_extraccion}")
logger.info(f"Ruta Bronze base       : {s3_bronze_base}")
logger.info(f"Ruta segmentos base    : {ruta_segmentos_base}")
logger.info(f"Es reanudación         : {reanudar_ejecucion}")

guardar_json_s3(ruta_parametros_extraccion, parametros_extraccion)


# =================================================
# EXTRACCION
# =================================================
try:
    tablas_exitosas = obtener_tablas_exitosas(parametros_extraccion)

    for ciclo in ciclos:
        if usar_checkpoints:
            actualizar_checkpoint(
                parametros_extraccion,
                estado="CICLO_IN_PROGRESS",
                guardar_detalle_eventos=guardar_detalle_eventos,
                ciclo=ciclo,
                mensaje=f"Iniciando ciclo {ciclo.get('nombre')}"
            )

        segmentos = sorted(ciclo.get("segmentos", []), key=lambda x: x.get("orden", 0))

        if not segmentos:
            raise ValueError(f"El ciclo {ciclo.get('nombre')} no contiene segmentos.")

        for segmento in segmentos:
            forma = segmento.get("forma", "CASCADA").upper()

            if forma != "CASCADA":
                logger.warning(
                    f"Forma {forma} configurada, pero esta versión ejecuta todo en CASCADA."
                )

            if usar_checkpoints:
                actualizar_checkpoint(
                    parametros_extraccion,
                    estado="SEGMENTO_IN_PROGRESS",
                    guardar_detalle_eventos=guardar_detalle_eventos,
                    ciclo=ciclo,
                    segmento=segmento,
                    mensaje=f"Iniciando segmento {segmento.get('nombre')}"
                )

            tablas = sorted(segmento.get("tablas", []), key=lambda x: x.get("orden", 0))

            if not tablas:
                raise ValueError(f"El segmento {segmento.get('nombre')} no contiene tablas.")

            for tabla_cfg in tablas:
                schema = tabla_cfg["schema"]
                tabla = tabla_cfg["tabla"]
                tabla_relativa = normalizar_tabla_relativa(tabla_cfg)
                dependencia = tabla_cfg.get("dependencia")

                if (
                    reanudar_ejecucion
                    and saltar_tablas_exitosas
                    and tabla_relativa in tablas_exitosas
                ):
                    logger.info(f"Saltando tabla ya extraída: {tabla_relativa}")
                    continue

                logger.info(f"===== INICIANDO EXTRACCION {schema}.{tabla} =====")

                fecha_inicio_tabla_dt = ahora_dt()
                fecha_inicio_tabla = fecha_inicio_tabla_dt.strftime("%Y-%m-%dT%H:%M:%S")

                metadata_segmentacion = construir_metadata_segmentacion(
                    nombre_caso=nombre_caso,
                    sistema_origen=sistema_origen,
                    periodo_negocio=periodo_negocio,
                    tabla_cfg=tabla_cfg,
                    lote_actual=timestamp_ingesta
                )

                if usar_checkpoints:
                    actualizar_checkpoint(
                        parametros_extraccion,
                        estado="TABLA_IN_PROGRESS",
                        guardar_detalle_eventos=guardar_detalle_eventos,
                        ciclo=ciclo,
                        segmento=segmento,
                        tabla=tabla_cfg,
                        mensaje=f"Extrayendo {schema}.{tabla}"
                    )

                    if guardar_checkpoint_inicio_tabla:
                        guardar_json_s3(ruta_parametros_extraccion, parametros_extraccion)

                ruta_catalogable = construir_ruta_catalogable(
                    tabla_cfg,
                    sistema_origen
                )
                tabla_catalogo = construir_nombre_tabla_catalogo(
                    tabla_cfg,
                    sistema_origen
                )

                target_path = (
                    f"{s3_bronze_base}{ruta_catalogable}/"
                )
                silver_target_path = (
                    f"{s3_silver_base}{ruta_catalogable}/"
                )

                if puede_reutilizar_snapshot(tabla_cfg):
                    lote_reutilizado, total_reutilizado = (
                        obtener_lote_snapshot_vigente(
                            spark,
                            target_path
                        )
                    )

                    if lote_reutilizado is not None:
                        silver_snapshot_disponible = False
                        silver_uri = urlparse(silver_target_path)
                        silver_prefix = silver_uri.path.lstrip("/").rstrip("/") + "/"
                        silver_s3 = boto3.client("s3")
                        silver_token = None
                        while True:
                            silver_kwargs = {
                                "Bucket": silver_uri.netloc,
                                "Prefix": silver_prefix,
                                "MaxKeys": 1000
                            }
                            if silver_token:
                                silver_kwargs["ContinuationToken"] = silver_token
                            silver_response = silver_s3.list_objects_v2(**silver_kwargs)
                            if any(
                                obj["Key"].lower().endswith(".parquet")
                                and obj.get("Size", 0) > 0
                                for obj in silver_response.get("Contents", [])
                            ):
                                silver_snapshot_disponible = True
                                break
                            if not silver_response.get("IsTruncated"):
                                break
                            silver_token = silver_response.get("NextContinuationToken")

                        requiere_copia_reutilizada = not silver_snapshot_disponible
                        modo_snapshot = (
                            "REUTILIZADA_COMPARTIDA"
                            if silver_snapshot_disponible
                            else "PENDIENTE_DESDE_BRONZE"
                        )

                        logger.info(
                            f"REUTILIZANDO SNAPSHOT {tabla_relativa}. "
                            f"lote={lote_reutilizado}, "
                            f"registros={total_reutilizado}"
                        )

                        registro_dataset = {
                            "estado": "SUCCESS",
                            "sistema_origen":
                                sistema_origen.lower(),
                            "esquema": schema.lower(),
                            "tabla": tabla.lower(),
                            "tabla_relativa": tabla_relativa,
                            "tabla_catalogo": tabla_catalogo,
                            "ruta_catalogable":
                                ruta_catalogable,
                            "ruta_dataset_origen":
                                target_path,
                            "ruta_dataset_destino":
                                silver_target_path,
                            "tipo_tabla":
                                tabla_cfg.get("tipo_tabla"),
                            "segmentacion_horizontal":
                                metadata_segmentacion,
                            "segmentacion_horizontal_habilitada":
                                False,
                            "escritura_bronze":
                                "REUTILIZADA",
                            "modo_disponibilidad":
                                modo_snapshot,
                            "lote_cargue_id_dataset":
                                lote_reutilizado,
                            "lotes_preparacion_silver": [
                                lote_reutilizado
                            ],
                            "datos_nuevos": False,
                            "datos_nuevos_para_flujo": False,
                            "requiere_validacion_tecnica":
                                False,
                            "requiere_disponibilizacion_silver":
                                requiere_copia_reutilizada,
                            "requiere_preparacion_silver":
                                True,
                            "total_registros":
                                total_reutilizado,
                            "fecha_inicio_extraccion":
                                ahora_str(),
                            "fecha_fin_extraccion":
                                ahora_str(),
                            "observacion":
                                "Snapshot compartido reutilizado."
                        }

                        upsert_dataset_extraido(
                            parametros_extraccion,
                            registro_dataset
                        )

                        parametros_extraccion.setdefault(
                            "lotes_usados_preparacion_silver",
                            {}
                        )[tabla_relativa] = [
                            lote_reutilizado
                        ]

                        if usar_checkpoints:
                            registrar_ok_tabla(
                                parametros_extraccion,
                                tabla_cfg,
                                guardar_detalle_eventos
                            )

                        guardar_json_s3(
                            ruta_parametros_extraccion,
                            parametros_extraccion
                        )

                        tablas_exitosas.add(
                            tabla_relativa
                        )
                        continue

                dbtable_name = construir_dbtable(
                    schema=schema,
                    tabla=tabla,
                    usar_schema_en_dbtable=usar_schema_en_dbtable
                )

                where = tabla_cfg.get("where")

                connection_options = {
                    "connectionName": connection_name,
                    "useConnectionProperties": "true",
                    "dbtable": dbtable_name,
                    "oracle.net.CONNECT_TIMEOUT": "30000",
                    "oracle.jdbc.ReadTimeout": "30000",
                    "fetchsize": fetchsize
                }

                if where:
                    query = construir_query_where(
                        tabla_cfg=tabla_cfg,
                        usar_schema_en_dbtable=usar_schema_en_dbtable
                    )

                    connection_options["sampleQuery"] = query
                    logger.info(f"Query: {query}")

                fecha_inicio_lectura_dt = ahora_dt()

                dyf = glueContext.create_dynamic_frame.from_options(
                    connection_type="oracle",
                    connection_options=connection_options
                )

                df = dyf.toDF()

                metadata_exclusion = {
                    "exclusion_aplicada": False,
                    "ruta_llaves_segmento_base": None,
                    "registros_excluidos_por_segmento": 0
                }

                df, metadata_exclusion = aplicar_exclusion_llaves_segmento(
                    spark=spark,
                    df=df,
                    metadata_segmentacion=metadata_segmentacion,
                    tabla_relativa=tabla_relativa,
                    ruta_segmentos_base=ruta_segmentos_base,
                    excluir_llaves_segmento=excluir_llaves_segmento,
                    ruta_bronze=target_path,
                    ruta_silver=silver_target_path
                )

                metadata_segmentacion["exclusion_aplicada"] = metadata_exclusion["exclusion_aplicada"]
                metadata_segmentacion["ruta_llaves_segmento_base"] = metadata_exclusion["ruta_llaves_segmento_base"]
                metadata_segmentacion["registros_excluidos_por_segmento"] = metadata_exclusion["registros_excluidos_por_segmento"]

                fecha_fin_lectura_dt = ahora_dt()
                duracion_lectura_segundos = segundos_entre(
                    fecha_inicio_lectura_dt,
                    fecha_fin_lectura_dt
                )

                if persistir_dataframe and contar_registros:
                    df = df.persist(StorageLevel.MEMORY_AND_DISK)

                total_columnas = len(df.columns)

                fecha_inicio_count_dt = ahora_dt()
                total_registros = df.count() if contar_registros else None
                fecha_fin_count_dt = ahora_dt()

                # =================================================
                # DATOS NUEVOS / FASE 3
                # =================================================
                datos_nuevos = total_registros is None or total_registros > 0
                if metadata_exclusion.get("exclusion_aplicada"):
                    metadata_segmentacion["lotes_segmento"] = (
                        combinar_lotes_respaldados(
                            metadata_exclusion.get(
                                "lotes_respaldo_exclusion"
                            ),
                            timestamp_ingesta,
                            datos_nuevos,
                            tabla_relativa,
                        )
                    )
                datos_nuevos_para_flujo = datos_nuevos
                motivo_sin_datos_nuevos = None

                if (
                    metadata_segmentacion.get("exclusion_aplicada")
                    and total_registros == 0
                ):
                    # Conservar solo los respaldos validados de las llaves excluidas.
                    if not metadata_segmentacion.get("lotes_segmento"):
                        raise ValueError(
                            f"{tabla_relativa}: universo reutilizado "
                            "sin lotes"
                        )

                    datos_nuevos = False
                    datos_nuevos_para_flujo = False
                    motivo_sin_datos_nuevos = "EXCLUSION_DEJO_DATASET_SIN_REGISTROS_NUEVOS"

                    logger.info(
                        f"{tabla_relativa} sin datos nuevos por exclusión. "
                        f"Lotes segmento vigentes: {metadata_segmentacion.get('lotes_segmento')}"
                    )

                # En modo exclusión, las tablas no segmentadas/catalogos no deben forzar continuidad
                if excluir_llaves_segmento and not metadata_segmentacion.get("habilitada", False):
                    datos_nuevos_para_flujo = False

                duracion_count_segundos = segundos_entre(
                    fecha_inicio_count_dt,
                    fecha_fin_count_dt
                )

                unidades_extraccion = (
                    total_registros * total_columnas
                    if calcular_unidades and total_registros is not None
                    else None
                )

                supera_limite_unidades = False

                if (
                    limite_unidades is not None
                    and unidades_extraccion is not None
                    and unidades_extraccion > int(limite_unidades)
                ):
                    supera_limite_unidades = True

                    mensaje_limite = (
                        f"{schema}.{tabla} supera límite de unidades "
                        f"({unidades_extraccion} > {limite_unidades})."
                    )

                    if accion_si_supera_limite == "FALLAR":
                        raise ValueError(
                            f"{mensaje_limite} accion_si_supera_limite=FALLAR"
                        )

                    logger.warning(mensaje_limite)

                df_brz = df

                if agregar_nro_registro:
                    df_brz = df_brz.withColumn(
                        "NRO_REGISTRO",
                        monotonically_increasing_id().cast("bigint")
                    )

                if agregar_lote_cargue_id:
                    df_brz = df_brz.withColumn(
                        "LOTE_CARGUE_ID",
                        lit(timestamp_ingesta).cast("bigint")
                    )


                fecha_inicio_write_dt = ahora_dt()
                metadata_llaves_segmento = None
                escritura_bronze = "SKIPPED"

                if total_registros is None or total_registros > 0:
                    modo_escritura_real = (
                        "append"
                        if metadata_segmentacion.get("exclusion_aplicada")
                        else modo_escritura
                    )

                    logger.info(
                        f"Escribiendo Bronze: {target_path} "
                        f"modo={modo_escritura_real}"
                    )

                    (
                        df_brz.write
                        .mode(modo_escritura_real)
                        .option("compression", compresion)
                        .parquet(target_path)
                    )

                    escritura_bronze = "SUCCESS"

                    if metadata_segmentacion["habilitada"]:
                        metadata_llaves_segmento = guardar_llaves_segmentacion_s3(
                            df_brz=df_brz,
                            metadata_segmentacion=metadata_segmentacion,
                            nombre_caso=nombre_caso,
                            sistema_origen=sistema_origen,
                            tabla_relativa=tabla_relativa,
                            periodo_negocio=periodo_negocio,
                            lote_cargue_id=timestamp_ingesta,
                            ruta_segmentos_base=ruta_segmentos_base
                        )
                else:
                    logger.info(
                        f"{tabla_relativa} quedó con 0 registros después de exclusión. "
                        "No se sobrescribe Bronze."
                    )

                fecha_fin_write_dt = ahora_dt()
                duracion_write_segundos = segundos_entre(
                    fecha_inicio_write_dt,
                    fecha_fin_write_dt
                )

                if persistir_dataframe and contar_registros:
                    df.unpersist()

                fecha_fin_tabla_dt = ahora_dt()
                fecha_fin_tabla = fecha_fin_tabla_dt.strftime("%Y-%m-%dT%H:%M:%S")
                duracion_tabla_segundos = segundos_entre(
                    fecha_inicio_tabla_dt,
                    fecha_fin_tabla_dt
                )

                dependencia_resumen = None

                if dependencia:
                    dependencia_resumen = {
                        "tabla_origen": dependencia.get("tabla_origen"),
                        "llaves": dependencia.get("llaves", [])
                    }

                registro_dataset = {
                    "estado": "SUCCESS",
                    "sistema_origen": sistema_origen.lower(),
                    "esquema": schema.lower(),
                    "tabla": tabla.lower(),
                    "tabla_relativa": tabla_relativa,
                    "tabla_catalogo": tabla_catalogo,
                    "ruta_catalogable": ruta_catalogable,
                    "ruta_dataset_origen": target_path,
                    "ruta_dataset_destino": silver_target_path,
                    "tipo_tabla": tabla_cfg.get("tipo_tabla"),
                    "ciclo_orden": ciclo.get("orden"),
                    "ciclo_nombre": ciclo.get("nombre"),
                    "segmento_orden": segmento.get("orden"),
                    "segmento_nombre": segmento.get("nombre"),
                    "forma": forma,
                    "tabla_orden": tabla_cfg.get("orden"),
                    "dependencia": dependencia_resumen,
                    "segmentacion_horizontal": metadata_segmentacion,
                    "segmentacion_horizontal_habilitada": metadata_segmentacion["habilitada"],
                    "segmento_id": metadata_segmentacion["segmento_id"],
                    "id_negocio": metadata_segmentacion.get("id_negocio"),
                    "periodo_negocio_segmento": metadata_segmentacion.get("periodo_negocio"),
                    "lotes_segmento": metadata_segmentacion["lotes_segmento"],
                    "llaves_segmentacion": metadata_segmentacion["llaves_segmentacion"],
                    "ruta_llaves_segmento": metadata_llaves_segmento.get("ruta_llaves_segmento") if metadata_llaves_segmento else None,
                    "total_llaves_segmento": metadata_llaves_segmento.get("total_llaves_segmento") if metadata_llaves_segmento else None,
                    "ruta_llaves_segmento_base": (
                        metadata_llaves_segmento.get("ruta_llaves_segmento_base")
                        if metadata_llaves_segmento
                        else metadata_segmentacion.get("ruta_llaves_segmento_base")
                    ),
                    "exclusion_aplicada": metadata_segmentacion.get("exclusion_aplicada", False),
                    "registros_excluidos_por_segmento": metadata_segmentacion.get(
                        "registros_excluidos_por_segmento",
                        0
                    ),
                    "escritura_bronze": escritura_bronze,
                    "modo_disponibilidad": "EXTRAIDA",
                    "lote_cargue_id_dataset":
                        timestamp_ingesta,
                    "lotes_preparacion_silver": (
                        metadata_segmentacion.get(
                            "lotes_segmento",
                            []
                        )
                        if metadata_segmentacion.get(
                            "habilitada",
                            False
                        )
                        else [timestamp_ingesta]
                    ),
                    "datos_nuevos": datos_nuevos,
                    "datos_nuevos_para_flujo": datos_nuevos_para_flujo,
                    "motivo_sin_datos_nuevos": motivo_sin_datos_nuevos,
                    "requiere_validacion_tecnica": datos_nuevos,
                    "requiere_disponibilizacion_silver": datos_nuevos,
                    "requiere_preparacion_silver": True,
                    "modo_ejecucion": modo_ejecucion,
                    "modo_extraccion_tabla": modo_ejecucion,
                    "total_registros": total_registros,
                    "total_columnas": total_columnas,
                    "unidades_extraccion": unidades_extraccion,
                    "limite_unidades_extraccion": limite_unidades,
                    "supera_limite_unidades": supera_limite_unidades,
                    "accion_si_supera_limite": accion_si_supera_limite,
                    "fecha_inicio_extraccion": fecha_inicio_tabla,
                    "fecha_fin_extraccion": fecha_fin_tabla,
                    "duracion_segundos": duracion_tabla_segundos,
                    "duracion_lectura_segundos": duracion_lectura_segundos,
                    "duracion_count_segundos": duracion_count_segundos if contar_registros else None,
                    "duracion_write_segundos": duracion_write_segundos,
                    "dbtable_oracle": dbtable_name,
                    "usar_schema_en_dbtable": usar_schema_en_dbtable
                }

                upsert_dataset_extraido(
                    parametros_extraccion,
                    registro_dataset
                )

                parametros_extraccion.setdefault(
                    "lotes_usados_preparacion_silver",
                    {}
                )[tabla_relativa] = (
                    registro_dataset.get(
                        "lotes_preparacion_silver",
                        [timestamp_ingesta]
                    )
                )

                if metadata_segmentacion["habilitada"]:
                    upsert_segmento_preparacion(
                        parametros_extraccion=parametros_extraccion,
                        metadata_segmentacion=metadata_segmentacion,
                        tabla_relativa=tabla_relativa,
                        metadata_llaves_segmento=metadata_llaves_segmento
                    )

                if usar_checkpoints:
                    registrar_ok_tabla(
                        parametros_extraccion,
                        tabla_cfg,
                        guardar_detalle_eventos
                    )

                    actualizar_checkpoint(
                        parametros_extraccion,
                        estado="TABLA_SUCCESS",
                        guardar_detalle_eventos=guardar_detalle_eventos,
                        ciclo=ciclo,
                        segmento=segmento,
                        tabla=tabla_cfg,
                        mensaje=f"OK {schema}.{tabla}"
                    )

                guardar_json_s3(ruta_parametros_extraccion, parametros_extraccion)

                tablas_exitosas.add(tabla_relativa)
                logger.info(f"===== OK {schema}.{tabla} =====")

            registrar_ok_segmento(parametros_extraccion, segmento, guardar_detalle_eventos)

            if usar_checkpoints:
                actualizar_checkpoint(
                    parametros_extraccion,
                    estado="SEGMENTO_SUCCESS",
                    guardar_detalle_eventos=guardar_detalle_eventos,
                    ciclo=ciclo,
                    segmento=segmento,
                    mensaje=f"OK segmento {segmento.get('nombre')}"
                )

        registrar_ok_ciclo(parametros_extraccion, ciclo, guardar_detalle_eventos)

        if usar_checkpoints:
            actualizar_checkpoint(
                parametros_extraccion,
                estado="CICLO_SUCCESS",
                guardar_detalle_eventos=guardar_detalle_eventos,
                ciclo=ciclo,
                mensaje=f"OK ciclo {ciclo.get('nombre')}"
            )

except Exception as e:
    logger.error("ERROR EN EXTRACCION ESTANDAR", exc_info=True)

    parametros_extraccion["estado_extraccion"] = "FAILED"
    parametros_extraccion["error_extraccion"] = str(e)
    parametros_extraccion["fecha_error_extraccion"] = ahora_str()

    if usar_checkpoints:
        actualizar_checkpoint(
            parametros_extraccion,
            estado="FAILED",
            guardar_detalle_eventos=guardar_detalle_eventos,
            mensaje=str(e)
        )

    guardar_json_s3(ruta_parametros_extraccion, parametros_extraccion)

    job.commit()
    raise


# =================================================
# CIERRE EXITOSO
# =================================================
fecha_fin_job_dt = ahora_dt()
fecha_fin_extraccion = fecha_fin_job_dt.strftime("%Y-%m-%dT%H:%M:%S")
duracion_total_job = segundos_entre(fecha_inicio_job_dt, fecha_fin_job_dt)

datasets_extraidos = parametros_extraccion.get("datasets_extraidos", [])

total_registros_extraidos = 0
total_unidades_extraccion = 0
total_datasets_con_conteo = 0

datasets_con_datos_nuevos = []
datasets_sin_datos_nuevos = []
datasets_con_datos_nuevos_para_flujo = []

for dataset in datasets_extraidos:
    total_registros_dataset = dataset.get("total_registros")
    unidades_dataset = dataset.get("unidades_extraccion")

    if total_registros_dataset is not None:
        total_registros_extraidos += int(total_registros_dataset)
        total_datasets_con_conteo += 1

    if unidades_dataset is not None:
        total_unidades_extraccion += int(unidades_dataset)

    if dataset.get("datos_nuevos") is True:
        datasets_con_datos_nuevos.append(dataset.get("tabla_relativa"))
    else:
        datasets_sin_datos_nuevos.append(dataset.get("tabla_relativa"))

    if dataset.get("datos_nuevos_para_flujo") is True:
        datasets_con_datos_nuevos_para_flujo.append(dataset.get("tabla_relativa"))

hay_datos_nuevos = len(datasets_con_datos_nuevos_para_flujo) > 0

_reconciliar_disponibilidad_final(spark, parametros_extraccion)

datasets_reutilizables_para_procesamiento = []

for dataset in datasets_extraidos:
    requiere_preparacion = bool(
        dataset.get("requiere_preparacion_silver", False)
    )
    modo_disponibilidad = str(
        dataset.get("modo_disponibilidad") or ""
    ).upper()
    exclusion_aplicada = bool(
        dataset.get("exclusion_aplicada", False)
    )
    lotes_reutilizados = (
        dataset.get("lotes_segmento")
        or dataset.get("lotes_preparacion_silver")
        or []
    )

    reutilizable = (
        requiere_preparacion
        and (
            modo_disponibilidad in ("REUTILIZADA_COMPARTIDA", "PENDIENTE_DESDE_BRONZE")
            or (
                exclusion_aplicada
                and bool(lotes_reutilizados)
            )
        )
    )

    if reutilizable:
        datasets_reutilizables_para_procesamiento.append(
            dataset.get("tabla_relativa")
        )

hay_datos_reutilizables = bool(
    datasets_reutilizables_para_procesamiento
)

entrada_funcional_vacia = any(
    str(dataset.get("tipo_tabla", "")).upper() == "PIVOTE"
    and dataset.get("total_registros") == 0
    and not bool(dataset.get("exclusion_aplicada", False))
    for dataset in datasets_extraidos
)

requiere_procesamiento_caso = bool(
    hay_datos_nuevos
    or hay_datos_reutilizables
    or entrada_funcional_vacia
)

parametros_extraccion["hay_datos_reutilizables"] = (
    hay_datos_reutilizables
)
parametros_extraccion["datasets_reutilizables_para_procesamiento"] = (
    datasets_reutilizables_para_procesamiento
)
parametros_extraccion["entrada_funcional_vacia"] = (
    entrada_funcional_vacia
)
parametros_extraccion["requiere_procesamiento_caso"] = (
    requiere_procesamiento_caso
)

parametros_extraccion["estado_extraccion"] = "SUCCESS"
parametros_extraccion["fecha_fin_extraccion"] = fecha_fin_extraccion

parametros_extraccion["hay_datos_nuevos"] = hay_datos_nuevos
parametros_extraccion["datasets_con_datos_nuevos"] = datasets_con_datos_nuevos
parametros_extraccion["datasets_sin_datos_nuevos"] = datasets_sin_datos_nuevos
parametros_extraccion["datasets_con_datos_nuevos_para_flujo"] = datasets_con_datos_nuevos_para_flujo

parametros_extraccion["estado_flujo_extraccion"] = (
    "SUCCESS_CON_DATOS_NUEVOS"
    if hay_datos_nuevos
    else "SUCCESS_SIN_DATOS_NUEVOS"
)

parametros_extraccion["metricas_extraccion"] = {
    "fecha_inicio": parametros_extraccion.get("fecha_ejecucion"),
    "fecha_fin": fecha_fin_extraccion,
    "duracion_total_segundos": duracion_total_job,
    "duracion_total_minutos": minutos(duracion_total_job),
    "total_datasets": len(datasets_extraidos),
    "total_datasets_con_conteo": total_datasets_con_conteo,
    "total_registros_extraidos": total_registros_extraidos if contar_registros else None,
    "total_unidades_extraccion": total_unidades_extraccion if calcular_unidades and contar_registros else None,
    "contar_registros": contar_registros,
    "persistir_dataframe": persistir_dataframe,
    "modo_escritura": modo_escritura,
    "compresion": compresion,
    "modo_ejecucion": modo_ejecucion,
    "sistema_origen": sistema_origen,
    "periodo_negocio": periodo_negocio,
    "excluir_llaves_segmento": excluir_llaves_segmento,
    "hay_datos_nuevos": hay_datos_nuevos,
    "hay_datos_reutilizables": hay_datos_reutilizables,
    "entrada_funcional_vacia": entrada_funcional_vacia,
    "requiere_procesamiento_caso": requiere_procesamiento_caso,
    "total_datasets_reutilizables_para_procesamiento": len(
        datasets_reutilizables_para_procesamiento
    ),
    "total_datasets_con_datos_nuevos": len(datasets_con_datos_nuevos),
    "total_datasets_sin_datos_nuevos": len(datasets_sin_datos_nuevos),
    "total_datasets_con_datos_nuevos_para_flujo": len(datasets_con_datos_nuevos_para_flujo)
}

if usar_checkpoints:
    actualizar_checkpoint(
        parametros_extraccion,
        estado="SUCCESS",
        guardar_detalle_eventos=guardar_detalle_eventos,
        mensaje="Extracción finalizada correctamente"
    )

guardar_json_s3(ruta_parametros_extraccion, parametros_extraccion)

logger.info("=== FIN JOB EXTRACCION ESTANDAR NIIF ===")
logger.info(f"PARAMETROS_EXTRACCION guardado en: {ruta_parametros_extraccion}")
logger.info(f"Hay datos nuevos para flujo: {hay_datos_nuevos}")
logger.info(f"Estado flujo extracción: {parametros_extraccion['estado_flujo_extraccion']}")
logger.info(f"Duración total extracción segundos: {duracion_total_job}")
logger.info(f"Duración total extracción minutos : {minutos(duracion_total_job)}")

job.commit()