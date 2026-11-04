import sys, traceback
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from commons.LogsHelper import LogsHelper

################################
### Configuracion de Entorno ###
################################
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logs = LogsHelper()
logger = logs.logger_conf()

spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")

try:
#--------------------------------------------------------------------------------
#--Consulta que trae la data desde el catalogo
#--------------------------------------------------------------------------------
    query = """WITH VW AS (
    SELECT 
          VW.cedula
        , VW.id_siniestro_fk                                  AS numero_siniestro
        , CAST(VW.valor_transaccion AS BIGINT)                AS VALOR_TRANSACCION
        , CAST(VW.valor_transaccion_ajustado AS BIGINT)       AS valor_transaccion_ajustado
        , VW.fecha_inicio_vigencia
        , VW.fecha_ocurrencia                                 AS fecha_siniestro
        , VW.`Fecha de Transaccion`                           
        , VW.id_poliza_pk
        , CAST(VW.id_tipo_transaccion_fk AS DECIMAL(19,0))          AS id_tipo_transaccion_fk
        , CAST(VW.id_fuente_fk AS DECIMAL(19,0))                    AS id_fuente_fk
        , VW.id_ramo_fk
        , VW.id_siniestro_fk
        FROM `datalake-dev-gold-catalog-db`.`gold_caract_siniestros_fact_movimiento_view` VW
        WHERE VW.`Fecha de Transaccion` >= to_date('2026-02-01')
        AND VW.`Fecha de Transaccion` <=  to_date('2026-02-28')
    )
    , PL AS (
            SELECT
              PP.id_poliza_pk
            , PP.compania
            , PP.id_producto_fk
            FROM `datalake-dev-gold-catalog-db`.`gold_caract_siniestros_fact_poliza_pf` PP)
    , DTT AS (
        SELECT /*+ BROADCAST(dtt)*/
          dtt.id_tipo_transaccion_pk
        , dtt.tipo_de_transaccion  
        FROM `datalake-dev-gold-catalog-db`.`gold_caract_siniestros_dim_tipo_transaccion` dtt
    )
    , FUE AS (
        SELECT /*+ BROADCAST(fue)*/
        fue.id_fuente
        , fue.desc_fuente
        FROM `datalake-dev-gold-catalog-db`.`gold_caract_siniestros_dim_fuente` fue
    )
    , RM AS (
        SELECT /*+ BROADCAST(RM)*/
          rm.id_ramo_pk AS codigo_ramo
        , rm.ramo
        FROM `datalake-dev-gold-catalog-db`.`gold_caract_siniestros_dim_ramo_tecnico` RM 
    )
    , PR AS (
        SELECT /*+ BROADCAST(pr)*/
          pr.id_producto_pk
        , pr.producto   
        FROM `datalake-dev-gold-catalog-db`.`gold_caract_siniestros_dim_producto` pr
    )
    , DET AS (
        SELECT 
          det.id_poliza_fk
        , det.id_siniestro_fk
        , det.id_poliza_detallada_pk as NRO_POLIZA
        FROM `datalake-dev-gold-catalog-db`.`gold_caract_siniestros_fact_poliza_detallada_pf` det
        GROUP BY det.id_poliza_fk
        , det.id_siniestro_fk
        , det.id_poliza_detallada_pk
    )
    SELECT
      VW.cedula
    , PL.compania
    , rm.codigo_ramo
    , rm.ramo
    , pr.id_producto_pk
    , pr.producto
    , dtt.tipo_de_transaccion
    , det.NRO_POLIZA
    , VW.numero_siniestro
    , SUM(VW.valor_transaccion)           AS VALOR_TRANSACCION
    , SUM(VW.valor_transaccion_ajustado)  AS valor_transaccion_ajustado
    , vw.fecha_inicio_vigencia
    , vw.fecha_siniestro
    , vw.`Fecha de Transaccion`
    , fue.desc_fuente
    FROM VW 
    INNER JOIN PL
    ON VW.id_poliza_pk = PL.id_poliza_pk
    INNER JOIN DTT
    ON VW.id_tipo_transaccion_fk = dtt.id_tipo_transaccion_pk
    INNER JOIN FUE
    ON VW.id_fuente_fk = fue.id_fuente
    INNER JOIN RM
    ON vw.id_ramo_fk = RM.codigo_ramo
    INNER JOIN PR
    ON pl.id_producto_fk = pr.id_producto_pk
    LEFT JOIN det
    ON VW.id_poliza_pk = det.id_poliza_fk
    AND VW.id_siniestro_fk = det.id_siniestro_fk
    group by VW.cedula
    , PL.compania
    , rm.codigo_ramo
    , rm.ramo
    , pr.id_producto_pk
    , pr.producto
    , dtt.tipo_de_transaccion
    , det.NRO_POLIZA
    , VW.numero_siniestro
    , VW.fecha_inicio_vigencia
    , VW.fecha_siniestro
    , VW.`Fecha de Transaccion`
    , fue.desc_fuente
    """
    df_resultado = spark.sql(query)
    
    logger.info(f"Salida consulta:\n{df_resultado.show(10)}")
    #logger.info(f"Salida consulta:\n{query}")
    
    #--validamos plan de --Consulta
    df_resultado.explain(True)
    
except Exception as e:
    logger.error(f"ERROR WITH Evaluate Data Quality: {traceback.format_exc()}")  

#--Fin ETL
job.commit()
