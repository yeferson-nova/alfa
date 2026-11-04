"""Perfilamiento de bronce -> fct_perfilamiento.

Itera los activos bronce (id_capa=1) del dominio en tbl_activo, lee cada tabla
fisica via Glue Catalog (`fuente_origen`.`nombre_fisico`) y perfila los atributos
declarados como criticos en tbl_atributo + tbl_regla_atributo. Resultado:
una fila por (activo, atributo) en alfa-datalake-calidad-gold.fct_perfilamiento.

100% parametrico: se agrega un activo bronce a tbl_activo y este job lo
levanta en la siguiente corrida, sin tocar codigo.

Uso:
    python jobs/02_perfilamiento_bronze.py --dominio clientes --mes 2026-06
    # filtrar a un solo activo (debug / smoke test):
    python jobs/02_perfilamiento_bronze.py --dominio clientes --mes 2026-06 --activos 118
"""
from __future__ import annotations

import argparse
import logging
import sys
from collections import defaultdict
from functools import reduce
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import functions as F

from lib import config
from lib.catalogo import leer_atributos_criticos
from lib.fact_calidad import asignar_ids_consecutivos
from lib.writers import ensure_external_table

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# columnas reales de fct_perfilamiento (nombres con espacios/"%", verificados contra
# el catalogo Glue - ver docs/T1_Calidad/SegurosAlfa_HallazgoFctTables_14Jul2026.md).
# insertInto matchea por posicion, por eso el orden aca debe calzar con
# _COLS_FCT_PERFILAMIENTO. Si el ambiente esta nuevo o se reseteo, ensure_external_table
# la recrea sola antes de escribir.
FCT_PERFILAMIENTO_DDL = (
    "id bigint, `fecha ejecucion` timestamp, `activo de informacion` string, "
    "`fuente de informacion` string, `tabla o conjunto de datos` string, "
    "`columna o atributo` string, `registros totales` bigint, "
    "`cantidad de registros nulos` bigint, `% nulos` double, `%completitud` double, "
    "`valor que mas ser repite (moda)` string, frecuencia bigint, id_capa bigint, "
    "`id_tipo de dato` bigint, `cantidad de registros unicos` bigint, "
    "`% porcentaje` double, longitud double, maximo double, minimo double"
)
FCT_PERFILAMIENTO_SUBPATH = "GOLD/CALIDAD/fct_perfilamiento/"
FCT_PERFILAMIENTO_PARTITION_DDL = "load_dt string"


# fct_perfilamiento: 19 cols + particion load_dt. insertInto empareja por posicion.
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
    "load_dt",
]


def perfilar_atributo(df, atributo: dict, tabla_fq: str):
    """Una fila por atributo (19 cols + load_dt). Devuelve None si la col no existe."""
    columna = atributo["columna_fisica"]
    if columna not in df.columns:
        log.warning(f"    col '{columna}' no esta en {tabla_fq}, se omite")
        return None

    # metricas base - si fallan no se puede seguir con el atributo
    try:
        metricas = df.agg(
            F.count(F.lit(1)).alias("total"),
            F.sum(F.when(F.col(columna).isNull(), 1).otherwise(0)).alias("nulos"),
            F.approx_count_distinct(F.col(columna)).alias("unicos"),
            F.avg(F.length(F.col(columna).cast("string"))).alias("long_prom"),
        ).collect()[0]
    except Exception as e:
        log.error(f"    col '{columna}' fallaron las metricas base, se omite: {str(e)[:120]}")
        return None

    total = metricas["total"]
    nulos = metricas["nulos"]
    unicos = metricas["unicos"]
    long_prom = metricas["long_prom"]
    pct_nulos = (nulos / total * 100.0) if total > 0 else 0.0
    pct_completitud = 100.0 - pct_nulos
    pct_unicos = (unicos / total * 100.0) if total > 0 else 0.0

    # maximo/minimo SOLO para columnas numericas reales. Para campos string/texto/fecha
    # van NULL de forma consistente (no castear a double, sino quedan llenos en unos y
    # null en otros segun si el string tiene valores numericos).
    _NUMERICOS = ("tinyint", "smallint", "int", "bigint", "long",
                  "float", "double", "decimal")
    col_type = dict(df.dtypes).get(columna, "string").lower()
    if any(col_type.startswith(t) for t in _NUMERICOS):
        try:
            minmax = df.agg(
                F.min(F.col(columna).cast("double")).alias("mn"),
                F.max(F.col(columna).cast("double")).alias("mx"),
            ).collect()[0]
            v_min, v_max = minmax["mn"], minmax["mx"]
        except Exception:
            v_min, v_max = None, None
    else:
        v_min, v_max = None, None

    # moda + frecuencia (ignorando nulos)
    try:
        moda_df = (
            df
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
    except Exception:
        v_moda, v_frec = None, 0

    spark = df.sparkSession
    df_out = spark.createDataFrame([(
        int(atributo["id_atributo"]) if atributo.get("id_atributo") is not None else None,
        atributo.get("nombre_activo") or "",
        atributo.get("fuente") or "",
        atributo.get("tabla_fisica") or "",
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
        # id es provisional; el job lo reasigna consecutivo despues de la union
        .withColumn("id", F.lit(0).cast("long"))
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


_NUMERICOS = ("tinyint", "smallint", "int", "bigint", "long", "float", "double", "decimal")


def perfilar_tabla(df, meta: dict, activo: dict, columnas: list | None = None):
    """Perfila las columnas indicadas (o todas si columnas=None) en pocas pasadas: 1 agg
    para metricas base + min/max; moda por columna. Devuelve un DataFrame (una fila por
    columna) en el shape de fct_perfilamiento. Mucho mas rapido que 3 pasadas/columna."""
    disponibles = set(df.columns)
    cols = [c for c in columnas if c in disponibles] if columnas else list(df.columns)
    if not cols:
        return None
    # seleccionar SOLO las columnas a perfilar: evita que el cache/lectura toque columnas
    # exoticas (ej. 'id' bigint con schema-convert) que no estamos perfilando.
    df = df.select(*[F.col(c) for c in cols])
    dtypes = dict(df.dtypes)

    def es_num(c):
        return any(dtypes.get(c, "string").lower().startswith(t) for t in _NUMERICOS)

    # ---- 1 sola pasada: total + (nulos, unicos, longitud, min/max) de TODAS las columnas
    exprs = [F.count(F.lit(1)).alias("total")]
    for i, c in enumerate(cols):
        col = F.col(c)
        exprs.append(F.sum(F.when(col.isNull(), 1).otherwise(0)).alias(f"nul_{i}"))
        exprs.append(F.approx_count_distinct(col).alias(f"unq_{i}"))
        exprs.append(F.avg(F.length(col.cast("string"))).alias(f"len_{i}"))
        if es_num(c):  # max/min SOLO numericos; string/fecha -> NULL
            exprs.append(F.min(col.cast("double")).alias(f"min_{i}"))
            exprs.append(F.max(col.cast("double")).alias(f"max_{i}"))
    base = df.agg(*exprs).collect()[0].asDict()
    total = int(base.get("total") or 0)
    if total == 0:
        return None

    # moda (N groupBy = la parte cara): tabla chica -> exacta sobre cache; tabla grande
    # -> sobre una muestra (~2M filas) y la frecuencia se escala. Hace factible perfilar
    # tablas de cientos de millones (ej. acsele_cobertura 209M).
    _UMBRAL = 20_000_000
    if total <= _UMBRAL:
        df_moda = df.cache()
        df_moda.count()
        factor_frec = 1.0
    else:
        frac = min(1.0, 2_000_000 / total)
        df_moda = df.sample(False, frac).cache()
        df_moda.count()
        factor_frec = 1.0 / frac

    id_activo = int(activo["id_activo"])
    nombre_activo = activo.get("nombre_activo") or ""
    fuente = activo.get("fuente") or ""
    tabla = activo.get("nombre_fisico") or ""
    id_capa = int(activo.get("id_capa") or 0)

    filas = []
    for i, c in enumerate(cols):
        nulos = int(base.get(f"nul_{i}") or 0)
        unicos = int(base.get(f"unq_{i}") or 0)
        long_prom = base.get(f"len_{i}")
        pct_nulos = (nulos / total * 100.0) if total > 0 else 0.0
        v_min = base.get(f"min_{i}") if es_num(c) else None
        v_max = base.get(f"max_{i}") if es_num(c) else None
        # moda por columna (ignora nulos); sobre df_moda (cache exacto o muestra)
        try:
            md = (df_moda.filter(F.col(c).isNotNull()).groupBy(c).count()
                    .orderBy(F.desc("count")).limit(1).collect())
            v_moda = str(md[0][c]) if md else None
            v_frec = round(md[0]["count"] * factor_frec) if md else 0
        except Exception:
            v_moda, v_frec = None, 0
        id_atr, id_tipo = meta.get((id_activo, c.lower()), (None, None))
        filas.append((
            int(id_atr) if id_atr is not None else None,
            nombre_activo, fuente, tabla, c,
            total, nulos, float(pct_nulos), float(100.0 - pct_nulos),
            v_moda, v_frec, id_capa, int(id_tipo) if id_tipo is not None else 0,
            unicos, float((unicos / total * 100.0) if total > 0 else 0.0),
            float(long_prom) if long_prom is not None else None,
            float(v_max) if v_max is not None else None,
            float(v_min) if v_min is not None else None,
        ))

    spark = df.sparkSession
    df_out = spark.createDataFrame(filas, schema=(
        "id_atributo LONG, nombre_activo STRING, fuente STRING, tabla STRING, "
        "columna STRING, total LONG, nulos LONG, pct_n DOUBLE, pct_c DOUBLE, "
        "moda STRING, frec LONG, id_capa LONG, id_tipo LONG, unicos LONG, "
        "pct_u DOUBLE, longitud DOUBLE, maximo DOUBLE, minimo DOUBLE"
    ))
    return (df_out
        .withColumn("id", F.lit(0).cast("long"))
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


def _fqn(fuente_origen: str, nombre_fisico: str) -> str:
    """Glue catalog fqn con backticks por los guiones de la DB."""
    return f"`{fuente_origen}`.`{nombre_fisico}`"


def _agrupar_por_activo(atributos: list[dict]) -> dict[int, list[dict]]:
    grupos: dict[int, list[dict]] = defaultdict(list)
    for a in atributos:
        grupos[int(a["id_activo"])].append(a)
    return dict(grupos)


def main(args: argparse.Namespace) -> None:
    if config.is_local():
        from lib.spark_session import build_local_spark
        spark = build_local_spark(app_name="perfilamiento-bronze-local")
    else:
        from lib.spark_session import get_glue_spark
        spark = get_glue_spark()
        # insertInto sobre tabla particionada necesita nonstrict para particiones dinamicas
        spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

    if config.is_local():
        # local: fixture sintetico, sin iteracion por activo
        log.info("[local] perfilando fixture sintetico")
        df = spark.read.csv(
            str(config.FIXTURES_DIR / "synthetic_clientes.csv"),
            header=True, inferSchema=True,
        )
        atributos = leer_atributos_criticos(spark, args.dominio)
        resultados = [perfilar_atributo(df, atr, "synthetic_clientes") for atr in atributos]
        resultados = [r for r in resultados if r is not None]
        if resultados:
            df_final = reduce(lambda a, b: a.unionByName(b), resultados)
            log.info("[local] results:")
            df_final.show(50, truncate=False)
        return

    # AWS: solo DATOS CRITICOS (dato_critico='Yes' en tbl_atributo), INDEPENDIENTE de reglas
    # Mapeo atributo/activo por tbl_atributo + tbl_activo.
    # default bronce (id_capa=1); --activos ignora el filtro de capa.
    capa = None if args.activos else 1
    atributos = leer_atributos_criticos(spark, args.dominio, id_capa=capa)
    log.info(f"{len(atributos)} atributos criticos (dato_critico=Yes) en '{args.dominio}' (capa={capa})")
    grupos = _agrupar_por_activo(atributos)

    if args.activos:
        ids = {int(x.strip()) for x in args.activos.split(",") if x.strip()}
        grupos = {k: v for k, v in grupos.items() if k in ids}
        log.info(f"filtrado a {len(grupos)} activos: {sorted(grupos.keys())}")

    if not grupos:
        log.warning("sin atributos criticos a perfilar - nada que escribir "
                    "(revisar dato_critico='Yes' en tbl_atributo para esta capa/dominio)")
        return

    # mapa (id_activo, nombre_campo_lower) -> (id_atributo, id_tipo_dato) para perfilar_tabla
    meta = {}
    for at in atributos:
        meta[(int(at["id_activo"]), str(at["columna_fisica"]).lower())] = (
            at.get("id_atributo"), at.get("id_tipo_dato")
        )

    resultados = []
    for id_activo, atrs in grupos.items():
        primer = atrs[0]
        fuente = primer.get("fuente") or ""
        tabla = primer.get("tabla_fisica") or ""
        if not fuente or not tabla:
            log.warning(f"  activo {id_activo}: sin fuente/nombre_fisico, se omite")
            continue
        cols_criticas = [a["columna_fisica"] for a in atrs]
        activo_info = {
            "id_activo": id_activo, "nombre_fisico": tabla, "fuente": fuente,
            "nombre_activo": primer.get("nombre_activo") or "", "id_capa": primer.get("id_capa"),
        }
        fq = _fqn(fuente, tabla)
        try:
            df = spark.table(fq)
            log.info(f"activo {id_activo} -> {fq} ({len(cols_criticas)} cols criticas)")
            df_p = perfilar_tabla(df, meta, activo_info, columnas=cols_criticas)
            if df_p is not None:
                df_p = df_p.cache()
                df_p.count()
                resultados.append(df_p)
            else:
                log.warning(f"  activo {id_activo}: tabla vacia o sin cols, se omite")
        except Exception as e:
            log.error(f"  activo {id_activo} no se pudo leer/perfilar ({fq}): {str(e)[:150]}")
            continue

    if not resultados:
        log.warning("nada que escribir")
        return

    df_final = reduce(lambda a, b: a.unionByName(b), resultados)

    fq_table = f"`{config.AWSPaths.CALIDAD_GOLD_DB}`.fct_perfilamiento"

    ensure_external_table(
        spark,
        fq_table,
        columns_ddl=FCT_PERFILAMIENTO_DDL,
        location=f"s3://{config.AWSPaths.GOLD_BUCKET}/{FCT_PERFILAMIENTO_SUBPATH}",
        partition_ddl=FCT_PERFILAMIENTO_PARTITION_DDL,
    )

    # ids consecutivos arrancando desde MAX(id)+1 de lo que ya hay.
    # idempotencia manual: para re-correr el mismo load_dt, limpiar S3 primero:
    #   aws s3 rm s3://alfa-datalake-dev-gold/fct_perfilamiento/load_dt=<fecha>/ --recursive
    df_final = asignar_ids_consecutivos(df_final, spark, fq_table)

    n = df_final.count()
    log.info(f"escribiendo {n} filas en {fq_table}")
    df_final.write.mode("append").insertInto(fq_table)
    log.info("listo")


def _parse_args():
    if config.is_local():
        parser = argparse.ArgumentParser(description="Perfilamiento Bronze")
        parser.add_argument("--dominio", required=True, choices=config.DOMINIOS)
        parser.add_argument("--mes", required=True, help="YYYY-MM")
        parser.add_argument("--activos", default="", help="csv de id_activo a perfilar")
        return parser.parse_args()
    from argparse import Namespace

    from awsglue.utils import getResolvedOptions  # type: ignore[import-not-found]
    requeridos = ["dominio", "mes"]
    opcionales = [a for a in ("activos",) if f"--{a}" in sys.argv]
    # nombres de recursos por ambiente (qa/prd); si no se pasan, quedan los defaults DEV
    recursos = [a for a in config.RESOURCE_ARGS if f"--{a}" in sys.argv]
    opts = getResolvedOptions(sys.argv, requeridos + opcionales + recursos)
    config.aplicar_recursos_glue(opts)
    return Namespace(
        dominio=opts["dominio"],
        mes=opts["mes"],
        activos=opts.get("activos", ""),
    )


if __name__ == "__main__":
    main(_parse_args())
