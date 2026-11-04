"""Silver profiling -> fct_perfilamiento.

Reads the silver table for the given domain via Glue Catalog, computes
per-attribute stats and inserts into alfa-datalake-calidad-gold.fct_perfilamiento.

Bronze profiling per source is left as a follow-up - bronze has no unified
table in Glue Catalog (CLIENTS/ACSELE, CLIENTS/ALFAPROD, ...).

Usage:
    python jobs/02_perfilamiento_silver.py --dominio clientes --mes 2026-06
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import functions as F

from lib import config
from lib.catalogo import leer_atributos_criticos

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


# fct_perfilamiento has 19 cols + load_dt partition at the end. insertInto matches by position.
_COLS_FCT_PERFILAMIENTO = [
    "id",
    "fecha_ejecucion",
    "activo_de_informacion",
    "fuente_de_informacion",
    "tabla_o_conjunto_de_datos",
    "columna_o_atributo",
    "registros_totales",
    "cantidad_de_registros_nulos",
    "pct_nulos",
    "pct_completitud",
    "valor_moda",
    "frecuencia",
    "id_capa",
    "id_tipo_de_dato",
    "cantidad_de_registros_unicos",
    "pct_porcentaje",
    "longitud",
    "maximo",
    "minimo",
    "load_dt",  # partition key — string YYYY-MM-DD
]


def perfilar_atributo(df_silver, atributo: dict, tabla_silver: str):
    # returns 1-row df with the 19 fct_perfilamiento cols + load_dt partition
    columna = atributo["columna_fisica"]
    if columna not in df_silver.columns:
        log.warning(f"col '{columna}' not in silver, skipping")
        return None

    # single agg pass for the base metrics
    metricas = df_silver.agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(F.col(columna).isNull(), 1).otherwise(0)).alias("nulos"),
        F.approx_count_distinct(F.col(columna)).alias("unicos"),
        F.avg(F.length(F.col(columna).cast("string"))).alias("long_prom"),
    ).collect()[0]

    total = metricas["total"]
    nulos = metricas["nulos"]
    unicos = metricas["unicos"]
    long_prom = metricas["long_prom"]
    pct_nulos = (nulos / total * 100.0) if total > 0 else 0.0
    pct_completitud = 100.0 - pct_nulos
    pct_unicos = (unicos / total * 100.0) if total > 0 else 0.0

    # min/max only on numeric-castable cols
    try:
        minmax = df_silver.agg(
            F.min(F.col(columna).cast("double")).alias("mn"),
            F.max(F.col(columna).cast("double")).alias("mx"),
        ).collect()[0]
        v_min, v_max = minmax["mn"], minmax["mx"]
    except Exception:
        v_min, v_max = None, None

    # mode + frequency (ignore nulls)
    moda_df = (
        df_silver
        .filter(F.col(columna).isNotNull())
        .groupBy(columna)
        .count()
        .orderBy(F.desc("count"))
        .limit(1)
        .collect()
    )
    if moda_df:
        v_moda = str(moda_df[0][columna])
        v_frec = int(moda_df[0]["count"])
    else:
        v_moda, v_frec = None, 0

    spark = df_silver.sparkSession
    df_out = spark.createDataFrame([(
        atributo["id_atributo"],
        atributo.get("nombre_activo") or "",
        atributo.get("fuente") or "",
        tabla_silver,
        columna,
        int(total),
        int(nulos),
        float(pct_nulos),
        float(pct_completitud),
        v_moda,
        int(v_frec),
        int(atributo.get("id_capa") or 0),
        int(atributo.get("id_tipo_dato") or 0),
        int(unicos),
        float(pct_unicos),
        float(long_prom) if long_prom is not None else None,
        float(v_max) if v_max is not None else None,
        float(v_min) if v_min is not None else None,
    )], schema=(
        "id_atributo LONG, nombre_activo STRING, fuente STRING, tabla STRING, "
        "columna STRING, total LONG, nulos LONG, pct_n DOUBLE, pct_c DOUBLE, "
        "moda STRING, frec LONG, id_capa LONG, id_tipo LONG, unicos LONG, "
        "pct_u DOUBLE, longitud DOUBLE, maximo DOUBLE, minimo DOUBLE"
    ))

    return (df_out
        .withColumn("id", F.xxhash64(
            F.col("id_atributo").cast("string"),
            F.current_date().cast("string"),
        ))
        .withColumn("fecha_ejecucion", F.current_timestamp())
        .withColumn("activo_de_informacion", F.col("nombre_activo"))
        .withColumn("fuente_de_informacion", F.col("fuente"))
        .withColumn("tabla_o_conjunto_de_datos", F.col("tabla"))
        .withColumn("columna_o_atributo", F.col("columna"))
        .withColumn("registros_totales", F.col("total"))
        .withColumn("cantidad_de_registros_nulos", F.col("nulos"))
        .withColumn("pct_nulos", F.col("pct_n"))
        .withColumn("pct_completitud", F.col("pct_c"))
        .withColumn("valor_moda", F.col("moda"))
        .withColumn("frecuencia", F.col("frec"))
        .withColumn("id_tipo_de_dato", F.col("id_tipo"))
        .withColumn("cantidad_de_registros_unicos", F.col("unicos"))
        .withColumn("pct_porcentaje", F.col("pct_u"))
        .withColumn("load_dt", F.date_format(F.current_date(), "yyyy-MM-dd"))
        .select(*_COLS_FCT_PERFILAMIENTO)
    )


def cargar_silver(spark, dominio: str):
    if config.is_local():
        return spark.read.csv(
            str(config.FIXTURES_DIR / "synthetic_clientes.csv"),
            header=True, inferSchema=True,
        )
    tabla = config.SILVER_TABLA_POR_DOMINIO.get(dominio)
    if not tabla:
        raise NotImplementedError(f"no silver table mapped for '{dominio}'")
    fq = f"`{config.AWSPaths.SILVER_DB}`.{tabla}"
    log.info(f"reading silver: {fq}")
    return spark.table(fq)


def main(args):
    if config.is_local():
        from lib.spark_session import build_local_spark
        spark = build_local_spark(app_name="perfilamiento-silver-local")
    else:
        from lib.spark_session import get_glue_spark
        spark = get_glue_spark()
        # insertInto on partitioned table needs nonstrict for dynamic partitions
        spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

    tabla_silver = config.SILVER_TABLA_POR_DOMINIO.get(args.dominio, "synthetic")
    df_silver = cargar_silver(spark, args.dominio)
    log.info(f"silver rows: {df_silver.count()}")

    atributos = leer_atributos_criticos(spark, args.dominio)
    log.info(f"{len(atributos)} critical attributes")

    resultados = []
    for atr in atributos:
        log.info(f"  profiling {atr['nombre']}")
        df_p = perfilar_atributo(df_silver, atr, tabla_silver)
        if df_p is not None:
            resultados.append(df_p)

    if not resultados:
        log.warning("nothing to write")
        return

    from functools import reduce
    df_final = reduce(lambda a, b: a.unionByName(b), resultados)

    if config.is_local():
        log.info("[local] results:")
        df_final.show(50, truncate=False)
        return

    fq_table = "`alfa-datalake-calidad-gold`.fct_perfilamiento"
    n = df_final.count()
    log.info(f"writing {n} rows to {fq_table}")
    df_final.write.mode("append").insertInto(fq_table)
    log.info("done")


def _parse_args():
    if config.is_local():
        parser = argparse.ArgumentParser(description="Perfilamiento Silver")
        parser.add_argument("--dominio", required=True, choices=config.DOMINIOS)
        parser.add_argument("--mes", required=True, help="YYYY-MM")
        return parser.parse_args()
    from argparse import Namespace
    from awsglue.utils import getResolvedOptions  # type: ignore[import-not-found]
    opts = getResolvedOptions(sys.argv, ["dominio", "mes"])
    return Namespace(dominio=opts["dominio"], mes=opts["mes"])


if __name__ == "__main__":
    main(_parse_args())
