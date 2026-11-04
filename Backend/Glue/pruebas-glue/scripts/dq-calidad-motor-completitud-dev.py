"""DQ engine -> fct_calidad.

Lee las reglas activas del catalogo (por dominio + dimension + capa), itera los
activos involucrados, lee la tabla fisica real de cada activo via su fqn
(`fuente_origen`.`nombre_fisico`) y aplica los patrones. Escribe metricas a fct_calidad.

Data-driven: agregar reglas = poblar tbl_regla_atributo. El motor lee la tabla
fisica que declara cada activo, no una tabla hardcodeada.

Usage:
    python jobs/04_motor_calidad.py --dominio clientes --mes 2026-06
    # por defecto solo capa gold (--capa 3) y dimension COMPLETITUD
    python jobs/04_motor_calidad.py --dominio clientes --mes 2026-06 --capa 1 --dimension VALIDEZ
"""
from __future__ import annotations

import argparse
import logging
import sys
from collections import defaultdict
from functools import reduce
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lib import config
from lib.cascada import ejecutar_con_cascada, ordenar_cascada
from lib.catalogo import leer_reglas_activas, resolver_listas_referencia
from lib.fact_calidad import a_fact_calidad, asignar_ids_consecutivos
from lib.reglas import aplicar_regla
from lib.writers import ensure_external_table, merge_delta

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# columnas + ubicacion reales de fct_calidad (verificadas contra el catalogo Glue,
# ver docs/T1_Calidad/SegurosAlfa_HallazgoFctTables_14Jul2026.md). Si el ambiente
# esta nuevo o se reseteo (tabla eliminada), ensure_external_table la recrea sola.
FCT_CALIDAD_DDL = (
    "id int, id_periodo int, fecha date, id_activo bigint, id_atributo bigint, "
    "id_dominio bigint, id_capa bigint, id_regla int, id_umbral bigint, "
    "valor_referencia int, valor_resultado bigint"
)
FCT_CALIDAD_SUBPATH = "GOLD/CALIDAD_CARGA_GOLD/fct_calidad/"


def cargar_silver(spark, dominio: str) -> DataFrame:
    # solo modo local: fixture sintetico. AWS itera tablas reales por activo.
    return spark.read.csv(
        str(config.FIXTURES_DIR / "synthetic_clientes.csv"),
        header=True,
        inferSchema=True,
    )


def _fqn(fuente_origen: str, nombre_fisico: str) -> str:
    # backticks por los guiones de la db
    return f"`{fuente_origen}`.`{nombre_fisico}`"


def _schema_vacio(spark) -> DataFrame:
    return spark.createDataFrame(
        [],
        schema=(
            "id INT, id_periodo INT, fecha DATE, "
            "id_activo BIGINT, id_atributo BIGINT, id_dominio BIGINT, "
            "id_capa BIGINT, id_regla INT, id_umbral BIGINT, "
            "valor_referencia INT, valor_resultado BIGINT"
        ),
    )


def ejecutar_motor(
    spark, dominio: str, dimension: str, mes: str,
    cascada: bool = False, id_capa: int | None = None,
) -> DataFrame:
    if config.is_local():
        # local conserva el flujo simple sobre el fixture
        df_silver = cargar_silver(spark, dominio)
        if cascada:
            return ejecutar_con_cascada(spark, df_silver, dominio=dominio, mes=mes)
        reglas = leer_reglas_activas(spark, dominio=dominio, dimension=dimension)
        reglas = resolver_listas_referencia(spark, reglas)
        resultados = []
        for regla in reglas:
            try:
                resultados.append(a_fact_calidad(aplicar_regla(df_silver, regla), regla, mes))
            except Exception as e:
                log.error(f"  rule {regla.codigo} failed: {e}")
        if not resultados:
            log.warning("ninguna regla produjo resultado; nada que escribir")
            return _schema_vacio(spark)
        return reduce(lambda a, b: a.unionByName(b), resultados)

    # AWS: data-driven por activo
    if cascada:
        log.info(f"loading ALL dimensions (cascada) for '{dominio}' (capa={id_capa})")
        reglas = []
        for dim in ("COMPLETITUD", "VALIDEZ", "UNICIDAD", "EXACTITUD"):
            reglas += leer_reglas_activas(spark, dominio=dominio, dimension=dim, id_capa=id_capa)
    else:
        log.info(f"loading {dimension} rules for '{dominio}' (capa={id_capa})")
        reglas = leer_reglas_activas(spark, dominio=dominio, dimension=dimension, id_capa=id_capa)
    # listas dinámicas: resolver parametros['id_lista'] -> 'valores' desde tbl_lista_referencia
    reglas = resolver_listas_referencia(spark, reglas)
    log.info(f"  active rules: {len(reglas)}")
    if not reglas:
        log.warning("no rules - nothing to run")
        return _schema_vacio(spark)

    # agrupar reglas por activo para leer cada tabla fisica una sola vez
    por_activo: dict[int, list] = defaultdict(list)
    for r in reglas:
        por_activo[int(r.id_activo)].append(r)

    resultados = []
    for id_activo, reglas_activo in por_activo.items():
        primera = reglas_activo[0]
        fuente = primera.fuente_origen or ""
        tabla = primera.tabla_fisica or ""
        if not fuente or not tabla:
            log.warning(f"  activo {id_activo}: sin fuente/nombre_fisico, skip")
            continue
        fq = _fqn(fuente, tabla)
        log.info(f"activo {id_activo} -> {fq} ({len(reglas_activo)} reglas)")
        try:
            # leer solo las columnas criticas evita schema-convert errors en cols exoticas.
            # incluir tambien las columnas referenciadas por parametros (columna_condicion
            # de RequeridoSiCondicion, columna_apellido de NombreApellido) aunque no tengan
            # regla propia, sino el patron no las encuentra.
            import json as _json
            cols = {r.columna_fisica for r in reglas_activo}
            for r in reglas_activo:
                try:
                    p = _json.loads(r.parametros) if r.parametros else {}
                    for k in ("columna_condicion", "columna_apellido"):
                        if p.get(k):
                            cols.add(p[k])
                except Exception:
                    log.debug(f"  parametros no parseables para {r.codigo}: {r.parametros}")
            df_full = spark.table(fq)
            cols = [c for c in cols if c in df_full.columns]
            df = df_full.select(*[F.col(c) for c in cols])
        except Exception as e:
            log.error(f"  activo {id_activo} no se pudo leer ({fq}): {str(e)[:150]}")
            continue

        # con --cascada: IsComplete primero, luego resto de completitud y V/U/E
        # sobre el subset que paso IsComplete (cascada.ordenar_cascada).
        pares = ordenar_cascada(df, reglas_activo) if cascada else [(df, r) for r in reglas_activo]
        for df_sub, regla in pares:
            log.info(f"  apply {regla.codigo} on {regla.nombre_atributo} ({regla.patron_dqdl})")
            try:
                df_fact = a_fact_calidad(aplicar_regla(df_sub, regla), regla, mes)
                # materializar AHORA (cache+count) para que un error de lectura de la
                # columna fuente (ej. schema-convert en cols exoticas) caiga dentro del
                # try y se saltee la regla, no en el insertInto final (lazy eval).
                df_fact = df_fact.cache()
                df_fact.count()
                resultados.append(df_fact)
            except Exception as e:
                log.error(f"  rule {regla.codigo} on {regla.nombre_atributo} failed: {str(e)[:150]}")
                continue

    if not resultados:
        # resiliencia: el motor corre con las tablas que existan; si ninguna regla
        # produjo salida (p.ej. tablas físicas aún no materializadas), no se aborta
        # el job, se devuelve vacío y main() registra "nothing to write".
        log.warning("ninguna regla produjo resultado (tablas faltantes o no aplicables); nada que escribir")
        return _schema_vacio(spark)

    return reduce(lambda a, b: a.unionByName(b), resultados)


def main(args: argparse.Namespace) -> None:
    if config.is_local():
        from lib.spark_session import build_local_spark

        spark = build_local_spark(app_name="motor-calidad-local")
    else:
        from lib.spark_session import get_glue_spark

        spark = get_glue_spark()

    log.info(f"mode={config.get_mode().value} dominio={args.dominio} mes={args.mes} "
             f"dimension={'ALL (cascada)' if args.cascada else args.dimension} capa={args.capa}")

    df_resultado = ejecutar_motor(
        spark,
        dominio=args.dominio,
        dimension=args.dimension,
        mes=args.mes,
        cascada=args.cascada,
        id_capa=args.capa,
    )

    n_filas = df_resultado.count()
    if n_filas == 0:
        log.warning("nothing to write")
        return

    if config.is_local():
        target = config.gold_fact_calidad_path()
        log.info(f"[local] {n_filas} rows -> {target}")
        merge_delta(
            df_nuevo=df_resultado,
            path_target=target,
            merge_keys=["id_atributo", "id_regla", "id_periodo"],
            partition_by=["id_periodo"],
            coalesce_n=1,
        )
    else:
        fq_table = f"`{config.AWSPaths.CALIDAD_GOLD_DB}`.fct_calidad"

        ensure_external_table(
            spark,
            fq_table,
            columns_ddl=FCT_CALIDAD_DDL,
            location=f"s3://{config.AWSPaths.GOLD_BUCKET}/{FCT_CALIDAD_SUBPATH}",
        )

        # ids consecutivos: MAX(id)+1, +2, ... sobre lo que ya hay en la fact.
        # idempotencia: fct_calidad es Hive non-transactional, no soporta DELETE.
        # antes de re-correr el mismo periodo, limpiar S3 manual:
        #   aws s3 rm s3://alfa-datalake-dev-gold/fct_calidad/ --recursive
        # (CR pendiente: migrar a Iceberg para soportar DELETE/MERGE atomico).
        df_resultado = asignar_ids_consecutivos(df_resultado, spark, fq_table)

        log.info(f"writing {n_filas} rows to {fq_table}")
        df_resultado.write.mode("append").insertInto(fq_table)

    log.info("--- summary ---")
    df_resultado.select(
        "id_atributo", "id_regla", "valor_referencia", "valor_resultado"
    ).show(50, truncate=False)
    log.info("done")


def _parse_args():
    # local uses argparse; in Glue use getResolvedOptions to skip --JOB_NAME etc.
    if config.is_local():
        parser = argparse.ArgumentParser(description="DQ engine")
        parser.add_argument("--dominio", required=True, choices=config.DOMINIOS)
        parser.add_argument(
            "--dimension",
            default="COMPLETITUD",
            choices=config.DIMENSIONES,
        )
        parser.add_argument("--mes", required=True, help="YYYY-MM")
        parser.add_argument("--cascada", action="store_true", default=False)
        parser.add_argument("--capa", type=int, default=None,
                            help="1=bronce, 2=silver, 3=gold. None=todas")
        return parser.parse_args()

    from argparse import Namespace

    from awsglue.utils import getResolvedOptions  # type: ignore[import-not-found]
    # only ask for the optional flags that were actually passed - getResolvedOptions errors if missing
    requeridos = ["dominio", "mes"]
    opcionales = [a for a in ("dimension", "cascada", "capa") if f"--{a}" in sys.argv]
    # nombres de recursos por ambiente (qa/prd); si no se pasan, quedan los defaults DEV
    recursos = [a for a in config.RESOURCE_ARGS if f"--{a}" in sys.argv]
    opts = getResolvedOptions(sys.argv, requeridos + opcionales + recursos)
    config.aplicar_recursos_glue(opts)
    # default gold (3): fct_calidad solo registra activos gold salvo override explicito
    capa_raw = opts.get("capa")
    return Namespace(
        dominio=opts["dominio"],
        mes=opts["mes"],
        dimension=opts.get("dimension", "COMPLETITUD"),
        cascada=str(opts.get("cascada", "false")).lower() == "true",
        capa=int(capa_raw) if capa_raw not in (None, "") else 3,
    )


if __name__ == "__main__":
    main(_parse_args())
