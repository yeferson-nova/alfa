"""
================================================================================
Gold Views Recreation Job 
================================================================================
Tables:
    1. gold_caract_clientes_poliza_emision   (43M)
    2. gold_caract_clientes_rol_cliente_estado (129M)
    3. gold_siniestros_ultimo_movimiento
    4. gold_clientes_detalle_completo         (16M)
    5. gold_caract_clientes_clientes_unicos   (9M)
    6. gold_comportamiento_valores            (~18M - valores proporcionales)
    7. gold_comportamiento_clientes           (~50M - id_cliente+periodo+negocio+demo)
    8. gold_puente_cliente_negocio            (~6.7M - lista clientes+demo)
================================================================================
"""
import sys
import boto3
from urllib.parse import urlparse
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from logger_config import logger
from utils import Utils
import traceback

CLASS_NAME = "GoldViewsRecreationJob"

def _delete_s3_prefix(s3_path: str):
    """Borra todos los archivos en un prefix S3 antes de recrear la tabla."""
    parsed = urlparse(s3_path)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")
    s3 = boto3.resource("s3")
    bucket_obj = s3.Bucket(bucket)
    objects = list(bucket_obj.objects.filter(Prefix=prefix))
    if objects:
        bucket_obj.delete_objects(Delete={"Objects": [{"Key": o.key} for o in objects]})
        print(f"S3 cleanup: deleted {len(objects)} objects from s3://{bucket}/{prefix}")
    else:
        print(f"S3 cleanup: nothing to delete at s3://{bucket}/{prefix}")
TABLES = [
    "gold_caract_clientes_poliza_emision",
    "gold_caract_clientes_rol_cliente_estado",
    "gold_siniestros_ultimo_movimiento",
    "gold_clientes_detalle_completo",
    "gold_caract_clientes_clientes_unicos",
    "gold_comportamiento_valores",
    "gold_comportamiento_clientes",
    "gold_puente_cliente_negocio",
]

def _query_poliza_emision(database, tables_bucket):
    db = database
    return f"""
        CREATE TABLE gold_caract_clientes_poliza_emision
        WITH (format = 'PARQUET', write_compression = 'SNAPPY', external_location = '{tables_bucket}/gold_caract_clientes_poliza_emision/')
        AS
        SELECT
            pol.id,
            pol.id_poliza,
            pol.id_fuente,
            pol.num_poliza numero_poliza,
            CAST(pol.fecha_efectiva_cancelacion AS DATE) fecha_efectiva_cancelacion,
            CAST(pol.fecha_cancelacion AS DATE) fecha_cancelacion,
            pol.motivo_cancelacion, pol.numero_poliza_matriz,
            prod.codigo_producto, 
            prod.descripcion_producto, 
            prod.ramo,
            prod.unidad_negocio,
            prod.codigo_compania, 
            prod.compania,
            tp_po.descripcion tipo_poliza, es_po.descripcion estado_poliza,
            li.descripcion id_linea_credito, canl.descripcion canal_venta,
            tp_ne.descripcion tipo_negocio,
            CAST(pol.fecha_emision_poliza AS DATE) fecha_emision_poliza,
            CAST(pol.fecha_inicio_vigencia AS DATE) fecha_inicio_vigencia,
            CAST(pol.fecha_fin_vigencia AS DATE) fecha_fin_vigencia,
            CAST(pol.valor_asegurado AS DECIMAL(18,4)) valor_asegurado_poliza,
            CAST(pol.valor_prima AS DECIMAL(18,4)) valor_prima_poliza,
            pol.id_fecha_emision, pol.id_fecha_inicio_vigencia,
            pol.id_fecha_fin_vigencia, pol.id_periodo_calculo,
            CAST(date_parse(concat(peri.periodo,'01'),'%Y%m%d') AS timestamp) periodo_calculo,
            CAST(date_parse(NULLIF(peri.fe_inicial,''),'%Y-%m-%d %H:%i:%s') AS DATE) fe_inicial_periodo_calculo,
            CAST(date_parse(NULLIF(peri.fe_final,''),'%Y-%m-%d %H:%i:%s') AS DATE) fe_final_periodo_calculo,
            peri.version version_periodo_calculo, peri.estado estado_periodo_calculo,
            pol.id_fecha_cargue, rp.nombre_tomador
        FROM "{db}".gold_caract_clientes_fact_poliza_emision pol
        LEFT JOIN (
            SELECT pro.id, pro.codigo codigo_producto, pro.descripcion descripcion_producto,
                   ra.descripcion ramo, un.descripcion unidad_negocio, pro.codigo_compania, pro.compania
            FROM "{db}".gold_caract_clientes_dim_producto pro
            LEFT JOIN "{db}".gold_caract_clientes_dim_dim_ramos_gold ra ON ra.id = pro.id_ramo
            LEFT JOIN "{db}".gold_caract_clientes_dim_dim_unidad_negocio_gold un ON un.id = pro.id_unidad_negocio
        ) prod ON prod.id = pol.id_producto
        LEFT JOIN "{db}".gold_caract_clientes_dim_dim_tipo_poliza_gold tp_po ON tp_po.id = pol.id_tipo
        LEFT JOIN "{db}".gold_caract_clientes_dim_dim_estado_poliza_gold es_po ON es_po.id = pol.id_estado
        LEFT JOIN "{db}".gold_caract_clientes_dim_dim_linea_credito_gold li ON li.id = pol.id_linea_credito
        LEFT JOIN "{db}".gold_caract_clientes_dim_dim_canal_venta_gold canl ON canl.id = pol.id_canal_venta
        LEFT JOIN "{db}".gold_caract_clientes_dim_dim_tipo_negocio_gold tp_ne ON tp_ne.id = pol.id_tipo_negocio
        LEFT JOIN "{db}".gold_caract_clientes_dim_dim_periodo_gold peri ON peri.id = pol.id_periodo_calculo
        LEFT JOIN (
            SELECT cp.id_poliza,
                   MAX(CASE WHEN cp.id_rol=1 THEN fc.nombre_completo END) AS nombre_tomador,
                   MAX(CASE WHEN cp.id_rol=4 THEN fc.nombre_completo END) AS nombre_pagador
            FROM "{db}".gold_caract_clientes_fact_clientes_polizas cp
            LEFT JOIN "{db}".gold_caract_clientes_fact_cliente fc ON fc.id = cp.id_cliente
            WHERE cp.id_rol IN (1,4) GROUP BY cp.id_poliza
        ) rp ON rp.id_poliza = pol.id
    """

def _query_rol_cliente_estado(database, tables_bucket):
    db = database
    return f"""
        CREATE TABLE gold_caract_clientes_rol_cliente_estado
        WITH (format = 'PARQUET', write_compression = 'SNAPPY', external_location = '{tables_bucket}/gold_caract_clientes_rol_cliente_estado/')
        AS
        WITH base_tomador AS (
            SELECT cli_po.id_cliente, cli_po.id_poliza, fact_cli.nombre_completo
            FROM "{db}".gold_caract_clientes_fact_clientes_polizas cli_po
            LEFT JOIN "{db}".gold_caract_clientes_fact_cliente fact_cli ON fact_cli.id = cli_po.id_cliente
            WHERE cli_po.id_rol = 1
        ),
        base_pagador AS (
            SELECT cli_po.id_cliente, cli_po.id_poliza, fact_cli.nombre_completo
            FROM "{db}".gold_caract_clientes_fact_clientes_polizas cli_po
            LEFT JOIN "{db}".gold_caract_clientes_fact_cliente fact_cli ON fact_cli.id = cli_po.id_cliente
            WHERE cli_po.id_rol = 4
        ),
        contacto_unico AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY id_cliente ORDER BY id DESC) AS rn_contacto
            FROM "{db}".gold_caract_clientes_fact_ubicacion_contactabilidad
            WHERE best_record = true
        )
        SELECT
            cli_po.id_cliente, cli_po.id_poliza,
            rol.descripcion AS rol, es_cli.descripcion AS estado_cliente,
            fact_cli.fecha_nacimiento, cli_po.id_fuente, cli_po.id_fecha_cargue,
            tom.nombre_completo AS nombre_tomador, pag.nombre_completo AS nombre_pagador,
            dpaisg_ubi.descripcion AS pais_ubicacion, dciug_ubi.descripcion AS ciudad_ubicacion,
            ddepg_ubi.descripcion AS departamento_ubicacion
        FROM "{db}".gold_caract_clientes_fact_clientes_polizas cli_po
        LEFT JOIN "{db}".gold_caract_clientes_dim_dim_rol_gold rol ON rol.id = cli_po.id_rol
        LEFT JOIN "{db}".gold_caract_clientes_dim_dim_estado_cliente_gold es_cli ON es_cli.id = cli_po.id_estado
        LEFT JOIN (
            SELECT *, CASE WHEN COALESCE(fecha_nacimiento, DATE '1900-01-01') <= DATE '1901-01-01' THEN -1
                ELSE date_diff('year', fecha_nacimiento, current_date) END AS edad
            FROM "{db}".gold_caract_clientes_fact_cliente
        ) fact_cli ON fact_cli.id = cli_po.id_cliente
        LEFT JOIN "{db}".gold_caract_clientes_dim_dim_genero_gold dgeng ON dgeng.id = fact_cli.id_genero
        LEFT JOIN contacto_unico fuc ON fuc.id_cliente = cli_po.id_cliente AND fuc.rn_contacto = 1
        LEFT JOIN "{db}".gold_caract_clientes_dim_dim_paises_gold dpaisg_ubi ON CAST(fuc.id_pais AS integer) = dpaisg_ubi.id
        LEFT JOIN "{db}".gold_caract_clientes_dim_dim_ciudad_gold dciug_ubi ON fuc.id_ciudad = dciug_ubi.id
        LEFT JOIN "{db}".gold_caract_clientes_dim_dim_departamento_gold ddepg_ubi ON dciug_ubi.departamento = cast(ddepg_ubi.codigo as varchar) and cast(dpaisg_ubi.codigo as varchar) = ddepg_ubi.pais
        LEFT JOIN base_tomador tom ON tom.id_poliza = cli_po.id_poliza
        LEFT JOIN base_pagador pag ON pag.id_poliza = cli_po.id_poliza
    """

def _query_siniestros_ultimo_movimiento(database, tables_bucket):
    db = database
    return f"""
        CREATE TABLE gold_siniestros_ultimo_movimiento
        WITH (format = 'PARQUET', write_compression = 'SNAPPY', external_location = '{tables_bucket}/gold_siniestros_ultimo_movimiento/')
        AS
        SELECT id_siniestro_fk, SPLIT_PART(id_poliza_pk,'-',1) id_poliza_clean,
            "Fecha de aviso" fecha_aviso, fecha_ocurrencia,
            CEDULA numero_documento_asegurado, valor_transaccion, id_fuente_fk
        FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY id_fuente_fk, SPLIT_PART(id_poliza_pk,'-',1), CEDULA
                ORDER BY "Fecha de Transaccion" DESC, id_siniestro_fk DESC
            ) rn FROM "{db}".gold_caract_siniestros_fact_movimiento_view
        ) s WHERE s.rn = 1
    """
def _query_clientes_detalle_completo(database: str, tables_bucket: str) -> str:
    db = database
    return f"""
        CREATE TABLE gold_clientes_detalle_completo
        WITH (format = 'PARQUET', write_compression = 'SNAPPY', external_location = '{tables_bucket}/gold_clientes_detalle_completo/')
        AS
        WITH rol_unico AS (
            SELECT
                r.id_poliza,
                r.id_cliente,
                r.rol,
                r.nombre_tomador,
                r.estado_cliente
            FROM "{db}".gold_caract_clientes_rol_cliente_estado r
            GROUP BY r.id_poliza, r.id_cliente, r.rol, r.nombre_tomador, r.estado_cliente
        ),
        detalle AS (
            SELECT
                pol.id_fecha_cargue,
                fc.nombre_completo,
                fc.numero_documento,
                dtdg.descripcion                      tipo_documento,
                dtpg.descripcion                      tipo_persona,
                rol.rol,
                rol.nombre_tomador,
                rol.estado_cliente,
                pol.estado_poliza,
                pol.numero_poliza,
                pol.numero_poliza_matriz,
                pol.codigo_producto,
                pol.descripcion_producto,
                pol.tipo_poliza,
                pol.ramo,
                pol.unidad_negocio,
                pol.fecha_emision_poliza,
                pol.fecha_inicio_vigencia,
                pol.fecha_fin_vigencia,
                pol.fecha_cancelacion,
                pol.motivo_cancelacion,
                pol.canal_venta,
                DATE_PARSE(peri.periodo, '%Y%m') periodo_calculo,
                pol.valor_asegurado_poliza,
                pol.valor_prima_poliza,
                pol.id                                id_poliza_fct,
                rol.id_cliente,
                fc.id                                 id_datos_cliente,
                pol.id_poliza,
                pol.id_fuente,
                sin.id_fuente_fk                      id_fuente_siniestro,
                sin.id_siniestro_fk                   nid_siniestro,
                sin.fecha_aviso                       siniestro_fecha_aviso,
                sin.fecha_ocurrencia                  siniestro_fecha_ocurrencia,
                sin.valor_transaccion                 siniestro_valor_ult_movimiento,
                ROW_NUMBER() OVER (
                    PARTITION BY pol.numero_poliza, pol.id_fuente, fc.numero_documento, rol.rol, pol.unidad_negocio
                    ORDER BY pol.id_periodo_calculo DESC
                ) rn
            FROM "{db}".gold_caract_clientes_poliza_emision pol
            LEFT JOIN "{db}".gold_caract_clientes_dim_dim_periodo_gold peri
                ON peri.id = pol.id_periodo_calculo
            LEFT JOIN rol_unico rol
                ON pol.id = rol.id_poliza
            LEFT JOIN "{db}".gold_caract_clientes_fact_cliente fc
                ON rol.id_cliente = fc.id
            LEFT JOIN "{db}".gold_caract_clientes_dim_dim_tipo_doc_gold dtdg
                ON fc.id_tipo_documento = dtdg.id
            LEFT JOIN "{db}".gold_caract_clientes_dim_dim_tipo_persona_gold dtpg
                ON fc.id_tipo_persona = dtpg.id
            LEFT JOIN "{db}".gold_siniestros_ultimo_movimiento sin
                ON  sin.id_poliza_clean = CAST(pol.id_poliza AS VARCHAR)
                AND sin.numero_documento_asegurado = fc.numero_documento
                AND rol.rol = 'Asegurado'
                AND (
                        (pol.id_fuente = '2'  AND sin.id_fuente_fk IN ('2', '3'))
                     OR (pol.id_fuente = '1'  AND sin.id_fuente_fk = '1')
                    )
        )
        SELECT
            nombre_completo,
            numero_documento,
            tipo_documento,
            tipo_persona,
            rol,
            nombre_tomador,
            estado_cliente,
            CASE
                WHEN unidad_negocio = 'Renta Vitalicia'
                     AND fecha_cancelacion IS NOT NULL
                THEN 'Inactiva'
                ELSE estado_poliza
            END                                       estado_poliza,
            numero_poliza,
            numero_poliza_matriz,
            codigo_producto,
            descripcion_producto,
            tipo_poliza,
            ramo,
            unidad_negocio,
            fecha_emision_poliza,
            fecha_inicio_vigencia,
            fecha_fin_vigencia,
            fecha_cancelacion,
            motivo_cancelacion,
            canal_venta,
            periodo_calculo,
            valor_asegurado_poliza,
            valor_prima_poliza,
            id_poliza_fct,
            id_cliente,
            id_datos_cliente,
            id_poliza,
            id_fuente,
            id_fuente_siniestro,
            nid_siniestro,
            siniestro_fecha_aviso,
            siniestro_fecha_ocurrencia,
            siniestro_valor_ult_movimiento,
            id_fecha_cargue
        FROM detalle
        WHERE rn = 1
    """
def _query_clientes_unicos(database, tables_bucket):
    db = database
    return f"""
        CREATE TABLE gold_caract_clientes_clientes_unicos
        WITH (format = 'PARQUET', write_compression = 'SNAPPY', external_location = '{tables_bucket}/gold_caract_clientes_clientes_unicos/')
        AS
        SELECT DISTINCT id id_cliente FROM "{db}".gold_caract_clientes_fact_cliente
    """

def _query_comportamiento_valores(database, tables_bucket):
    db = database
    return f"""
        CREATE TABLE gold_comportamiento_valores
        WITH (format = 'PARQUET', write_compression = 'SNAPPY', external_location = '{tables_bucket}/gold_comportamiento_valores/')
        AS
        -- CTE: distribuye el valor de la poliza proporcionalmente entre estados del cliente
        -- Una poliza colectiva puede tener clientes Activos e Inactivos simultaneamente
        -- El valor se reparte segun cuantos clientes hay en cada estado
        -- Ejemplo: poliza con valor 1000, 3 Activos y 1 Inactivo → Activo=750, Inactivo=250
        -- NOTA: No se usan window functions (COUNT DISTINCT OVER) porque Athena no las soporta
        WITH cnt_por_estado AS (
            SELECT id_poliza, rol, estado_cliente,
                COUNT(DISTINCT id_cliente) AS cnt_estado,
                MIN(id_cliente)            AS id_cliente_ref
            FROM (
                SELECT DISTINCT id_poliza, rol, estado_cliente, id_cliente
                FROM "{db}".gold_caract_clientes_rol_cliente_estado
            )
            GROUP BY id_poliza, rol, estado_cliente
        ),
        cnt_total AS (
            SELECT id_poliza, rol,
                COUNT(DISTINCT id_cliente) AS cnt_total
            FROM (
                SELECT DISTINCT id_poliza, rol, id_cliente
                FROM "{db}".gold_caract_clientes_rol_cliente_estado
            )
            GROUP BY id_poliza, rol
        ),
        rol_proporcional AS (
            SELECT
                e.id_poliza,
                e.rol,
                e.estado_cliente,
                e.cnt_estado,
                t.cnt_total,
                e.id_cliente_ref
            FROM cnt_por_estado e
            INNER JOIN cnt_total t ON e.id_poliza = t.id_poliza AND e.rol = t.rol
        )
        SELECT
            pol.periodo_calculo,
            rp.rol,
            rp.estado_cliente,
            cd.genero,
            cd.rango_edad,
            cd.edad,
            cd.pais_ubicacion,
            cd.departamento_ubicacion,
            cd.ciudad_ubicacion,
            cd.actividad_economica,
            cd.ocupacion,
            cd.tipo_persona,
            cd.estado_civil,
            cd.nivel_estudios,
            pol.nombre_tomador,
            pol.descripcion_producto,
            pol.ramo,
            pol.unidad_negocio,
            pol.compania,
            pol.tipo_poliza,
            pol.estado_poliza,
            pol.canal_venta,
            SUM(pol.valor_asegurado_poliza * CAST(rp.cnt_estado AS DOUBLE) / NULLIF(CAST(rp.cnt_total AS DOUBLE), 0)) AS valor_asegurado,
            SUM(pol.valor_prima_poliza     * CAST(rp.cnt_estado AS DOUBLE) / NULLIF(CAST(rp.cnt_total AS DOUBLE), 0)) AS valor_prima,
            COUNT(DISTINCT pol.id) AS cantidad_polizas
        FROM "{db}".gold_caract_clientes_poliza_emision pol
        INNER JOIN rol_proporcional rp ON pol.id = rp.id_poliza
        LEFT JOIN (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY id DESC) AS rn_cd
            FROM "{db}".gold_caract_clientes_datos_completos_view
            WHERE best_record = true
        ) cd ON rp.id_cliente_ref = cd.id AND cd.rn_cd = 1
        GROUP BY
            pol.periodo_calculo, rp.rol, rp.estado_cliente,
            cd.genero, cd.rango_edad, cd.edad,
            cd.pais_ubicacion, cd.departamento_ubicacion, cd.ciudad_ubicacion,
            cd.actividad_economica, cd.ocupacion, cd.tipo_persona,
            cd.estado_civil, cd.nivel_estudios,
            pol.nombre_tomador, pol.descripcion_producto, pol.ramo,
            pol.unidad_negocio, pol.compania, pol.tipo_poliza,
            pol.estado_poliza, pol.canal_venta
    """

def _query_comportamiento_clientes(database, tables_bucket):
    db = database
    return f"""
        CREATE TABLE gold_comportamiento_clientes
        WITH (format = 'PARQUET', write_compression = 'SNAPPY', external_location = '{tables_bucket}/gold_comportamiento_clientes/')
        AS
        SELECT DISTINCT
            r.id_cliente, 
            r.rol,
            r.estado_cliente,
            pol.periodo_calculo,
            pol.id_fecha_cargue,
            pol.nombre_tomador,
            pol.descripcion_producto, 
            pol.ramo,
            pol.unidad_negocio,
            pol.compania,
            pol.tipo_poliza,
            pol.estado_poliza, 
            pol.canal_venta,
            cd.genero, 
            cd.rango_edad, 
            cd.edad,
            cd.pais_ubicacion, 
            cd.departamento_ubicacion,
            cd.ciudad_ubicacion,
            cd.actividad_economica, 
            cd.ocupacion,
            cd.tipo_persona,
            cd.estado_civil,
            cd.nivel_estudios
        FROM (
            SELECT DISTINCT id_poliza, rol, estado_cliente, id_cliente
            FROM "{db}".gold_caract_clientes_rol_cliente_estado
        ) r
        INNER JOIN "{db}".gold_caract_clientes_poliza_emision pol
            ON r.id_poliza = pol.id
        LEFT JOIN (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY id DESC) AS rn_cd
            FROM "{db}".gold_caract_clientes_datos_completos_view
            WHERE best_record = true
        ) cd ON r.id_cliente = cd.id AND cd.rn_cd = 1
    """

def _query_puente_cliente_negocio(database, tables_bucket):
    db = database
    return f"""
        CREATE TABLE gold_puente_cliente_negocio
        WITH (format = 'PARQUET', write_compression = 'SNAPPY', external_location = '{tables_bucket}/gold_puente_cliente_negocio/')
        AS
        SELECT DISTINCT
            r.id_cliente,
            r.rol,
            r.estado_cliente,
            pol.periodo_calculo,
            pol.id_fecha_cargue,
            pol.nombre_tomador,
            pol.descripcion_producto,
            pol.ramo,
            pol.unidad_negocio,
            pol.compania, 
            pol.tipo_poliza,
            pol.estado_poliza,
            pol.canal_venta,
            cd.genero,
            cd.rango_edad,
            cd.edad,
            cd.pais_ubicacion, 
            cd.departamento_ubicacion,
            cd.ciudad_ubicacion,
            cd.actividad_economica,
            cd.ocupacion, 
            cd.tipo_persona,
            cd.estado_civil,
            cd.nivel_estudios
        FROM (
            SELECT DISTINCT id_poliza, rol, estado_cliente, id_cliente
            FROM "{db}".gold_caract_clientes_rol_cliente_estado
        ) r
        INNER JOIN "{db}".gold_caract_clientes_poliza_emision pol
            ON r.id_poliza = pol.id
        LEFT JOIN (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY id DESC) AS rn_cd
            FROM "{db}".gold_caract_clientes_datos_completos_view
            WHERE best_record = true
        ) cd ON r.id_cliente = cd.id AND cd.rn_cd = 1
    """

_RECREATION_STEPS = [
    ("gold_caract_clientes_poliza_emision", "Create poliza_emision", _query_poliza_emision),
    ("gold_caract_clientes_rol_cliente_estado", "Create rol_cliente_estado", _query_rol_cliente_estado),
    ("gold_siniestros_ultimo_movimiento", "Create siniestros", _query_siniestros_ultimo_movimiento),
    ("gold_clientes_detalle_completo", "Create detalle_completo", _query_clientes_detalle_completo),
    ("gold_caract_clientes_clientes_unicos", "Create clientes_unicos", _query_clientes_unicos),
    ("gold_comportamiento_valores", "Create comportamiento_valores", _query_comportamiento_valores),
    ("gold_comportamiento_clientes", "Create comportamiento_clientes", _query_comportamiento_clientes),
    ("gold_puente_cliente_negocio", "Create puente_cliente_negocio", _query_puente_cliente_negocio),
]

def main():
    try:
        args = getResolvedOptions(sys.argv, ["JOB_NAME", "DATABASE", "OUTPUT_LOCATION"])
        job_name = args["JOB_NAME"]
        database = args["DATABASE"]
        output_location = args["OUTPUT_LOCATION"]
        tables_bucket = output_location.rstrip("/") + "/tables"
        sc = SparkContext()
        glue_context = GlueContext(sc)
        spark = glue_context.spark_session
        job = Job(glue_context)
        job.init(job_name, args)
        utils = Utils()
        logger.info(f"Gold views recreation starting | database: {database} | output: {output_location} | tables_bucket: {tables_bucket}", CLASS_NAME)
        tables_done = 0
        tables_failed = 0
        try:
            for table_name, description, query_fn in _RECREATION_STEPS:
                table_s3_path = f"{tables_bucket}/{table_name}/"
                logger.info(f"Dropping: {table_name}", CLASS_NAME)
                utils.execute_athena_ddl(query=f"DROP TABLE IF EXISTS {table_name}",
                    database=database, output_location=output_location, description=f"Drop {table_name}")
                logger.info(f"Cleaning S3: {table_s3_path}", CLASS_NAME)
                _delete_s3_prefix(table_s3_path)
                logger.info(f"Creating: {table_name}", CLASS_NAME)
                utils.execute_athena_ddl(query=query_fn(database, tables_bucket),
                    database=database, output_location=output_location, description=description)
                logger.success(f"Completed: {table_name}", CLASS_NAME)
                tables_done += 1
            logger.success(f"All tables recreated | done: {tables_done} | failed: {tables_failed}", CLASS_NAME)
        except Exception as e:
            tables_failed += 1
            logger.error(f"Job failed: {str(e)}", CLASS_NAME)
            raise
        finally:
            job.commit()
    except Exception as e:
        print(f"Job failed: {str(e)}")


if __name__ == "__main__":
    main()
