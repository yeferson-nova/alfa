import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import time
from io import StringIO
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
print(f"Delta Extensions: {spark.conf.get('spark.sql.extensions')}")
# Spark Libraries
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, coalesce, when, substring, 
    max as spark_max, sum as spark_sum, count,
    row_number, desc, concat, to_date, date_format, current_date,
    add_months, lpad
)
from pyspark.sql.types import DecimalType
from decimal import Decimal
from datetime import datetime
from pyspark.sql.window import Window
# Custom Libraries
from logger_config import logger
from utils import Utils
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
def load_table(table_name: str, spark:SparkSession, table_paths:dict, utils_instance:Utils) -> DataFrame:
    s3_target = table_paths[table_name]
    logger.info(f"Loading table: {table_name}", "AlfaprdStagingPolizas")
    # load df
    df = utils_instance.read_full_parquet(
                spark,
                s3_target_path=s3_target
        )
    logger.info(f"Table {table_name} loaded successfully", "AlfaprdStagingPolizas")
    return df
table_paths =  {
      "arpprod_par_polizas": "s3://alfa-datalake-qa-bronce/ALFAPROD/ARPPROD/ALFAPROD_ARPPROD_PAR_POLIZAS/",
      "arpprod_sar_pagos_coberturas": "s3://alfa-datalake-qa-bronce/ALFAPRD/ARPPROD/SAR_PAGOS_COBERTURAS/",
      "arpprod_par_trabajadores": "s3://alfa-datalake-qa-bronce/ALFAPRD/ARPPROD/PAR_TRABAJADORES/",
      "arpprod_sar_siniestros": "s3://alfa-datalake-qa-bronce/ALFAPRD/ARPPROD/SAR_SINIESTROS/",
      "general_cgr_estados": "s3://alfa-datalake-qa-bronce/ALFAPROD/GENERAL/ALFAPROD_GENERAL_CGR_ESTADOS/",
      "arpprod_sar_auditoria_medica": "s3://alfa-datalake-qa-bronce/ALFAPROD/ARPPROD/ALFAPROD_ARPPROD_SAR_AUDITORIA_MEDICA/",
      "arpprod_gar_conceptos_medicos_am" : "s3://alfa-datalake-qa-bronce/ALFAPROD/ARPPROD/ALFAPROD_ARPPROD_GAR_CONCEPTOS_MEDICOS_AM/",
      "general_cgr_sucursales_alfa": "s3://alfa-datalake-qa-bronce/ALFAPROD/GENERAL/ALFAPROD_GENERAL_CGR_SUCURSALES_ALFA/"
}
utils_instance = Utils()
df_arpprod_par_polizas = load_table("arpprod_par_polizas", spark, table_paths, utils_instance)
df_arpprod_sar_pagos_coberturas = load_table("arpprod_sar_pagos_coberturas", spark, table_paths, utils_instance)
df_arpprod_par_trabajadores = load_table("arpprod_par_trabajadores", spark, table_paths, utils_instance)
df_arpprod_sar_siniestros = load_table("arpprod_sar_siniestros", spark, table_paths, utils_instance)
df_general_cgr_estados = load_table("general_cgr_estados", spark, table_paths, utils_instance)
df_arpprod_sar_auditoria_medica = load_table("arpprod_sar_auditoria_medica", spark, table_paths, utils_instance)
df_arpprod_gar_conceptos_medicos_am = load_table("arpprod_gar_conceptos_medicos_am", spark, table_paths, utils_instance)
df_alfaprod_general_cgr_sucursales_alfa = load_table("general_cgr_sucursales_alfa", spark, table_paths, utils_instance)
df_general_cgr_estados.show(truncate=False)
literals = {
            "FUENTE": "ARPIS",
            "ESTADO_POLIZA": "",
            "FECHA_CANCELACION": "N/A",
            "FECHA_EFECTIVA_CANCELACION": "N/A",
            "MOTIVO_CANCELACION": "N/A",
            "PAGADOR_MAESTRO": "N/A",
            "TIPO_NEGOCIO": "DIRECTO",
            "TIPO_POLIZA": "OBLIGATORIA",
            "VALOR_PRIMA": Decimal("0"),
            "LINEA_CREDITO": "N/A",
            "UNIDAD_NEGOCIO": "ARL",
            "CANAL_VENTA": "N/A",
            "COD_COBERTURA_DEFAULT": "99",
            "DESC_COBERTURA_DEFAULT": "ARL Sin Prestación",
            "FECHA_CORTE_CALCULO": "N/A",
            "NID_CALCULO": "N/A"
        }
def __get_df_arl_polizas(df_arpprod_par_polizas: DataFrame,
                           df_alfaprod_general_cgr_sucursales_alfa: DataFrame,
                           df_arpprod_sar_pagos_coberturas: DataFrame,
                           df_arpprod_par_trabajadores: DataFrame,
                           df_arpprod_sar_siniestros: DataFrame,
                           df_general_cgr_estados: DataFrame,
                           df_arpprod_sar_auditoria_medica: DataFrame,
                           df_arpprod_gar_conceptos_medicos_am: DataFrame) -> DataFrame:
        """
        Extract and transform ARL/ARPIS policy data with OPTIMIZED performance.

        UPDATED: New coverage logic using sar_auditoria_medica and gar_conceptos_medicos_am
        UPDATED: New ranking logic using ROW_NUMBER with num_endoso DESC, modulo DESC

        OPTIMIZATIONS APPLIED:
        - ROW_NUMBER window function for latest policy version (num_endoso DESC, modulo DESC)
        - CTE for poliza_origenes_medicos (auditoria_medica + conceptos_medicos)
        - CTE for valor_asegurado calculation (pagos aggregation)
        - Join with cgr_estados to get estado_poliza from trabajadores
        - GROUP BY to consolidate results

        Args:
            df_arpprod_par_polizas (DataFrame): ARL policies
            df_alfaprod_general_cgr_sucursales_alfa (DataFrame): Branch catalog
            df_arpprod_sar_pagos_coberturas (DataFrame): Claims payments for valor_asegurado
            df_arpprod_par_trabajadores (DataFrame): Workers/employees data
            df_arpprod_sar_siniestros (DataFrame): Claims data
            df_general_cgr_estados (DataFrame): Status catalog
            df_arpprod_sar_auditoria_medica (DataFrame): Medical audit data
            df_arpprod_gar_conceptos_medicos_am (DataFrame): Medical concepts catalog

        Returns:
            DataFrame: Transformed ARL policy data with standardized schema

        Business Rules:
            - Filters to latest version using ROW_NUMBER (ORDER BY num_endoso DESC, modulo DESC)
            - Coverage from auditoria_medica, defaults to '99' / 'ARL Sin Prestación' if null
            - Uses OBLIGATORIA as policy type
            - NUMERO_POLIZA: nid_poliza-tipo_id-num_id (concatenated)
            - NUMERO_POLIZA_MATRIZ: nid_poliza only
            - PK_POLIZA_CLIENTE: nid_poliza||numero_documento (no separator)
            - VALOR_ASEGURADO: SUM of payments per claim (NVL to 0)
            - Gets estado_poliza from cgr_estados via trabajadores
        """
        # Define constant literals
        literals = {
            "FUENTE": "ARPIS",
            "ESTADO_POLIZA": "",
            "FECHA_CANCELACION": "N/A",
            "FECHA_EFECTIVA_CANCELACION": "N/A",
            "MOTIVO_CANCELACION": "N/A",
            "PAGADOR_MAESTRO": "N/A",
            "TIPO_NEGOCIO": "DIRECTO",
            "TIPO_POLIZA": "OBLIGATORIA",
            "VALOR_PRIMA": Decimal("0"),
            "LINEA_CREDITO": "N/A",
            "UNIDAD_NEGOCIO": "ARL",
            "CANAL_VENTA": "N/A",
            "COD_COBERTURA_DEFAULT": "99",
            "DESC_COBERTURA_DEFAULT": "ARL Sin Prestación",
            "FECHA_CORTE_CALCULO": "N/A",
            "NID_CALCULO": "N/A"
        }

        # ========================================================================
        # STEP 1: CREATE par_polizas_ranked WITH ROW_NUMBER
        # ========================================================================
        logger.info("Step 1: Creating par_polizas_ranked with ROW_NUMBER", "AlfaprdStagingPolizas")

        window_polizas_rank = Window.partitionBy("nid_poliza").orderBy(
            col("num_endoso").desc(),
            col("modulo").desc()
        )

        df_polizas_ranked = df_arpprod_par_polizas.select(
            col("nid_poliza"),
            col("fec_expedicion"),
            col("fec_inicial_vigencia"),
            col("fec_final_vigencia"),
            col("cod_sucursal_alfa"),
            col("cod_producto"),
            col("modulo"),
            col("num_endoso"),
            row_number().over(window_polizas_rank).alias("rn")
        )

        # Filter rn = 1
        df_polizas_filtered = df_polizas_ranked.filter(col("rn") == 1).drop("rn")

        # ========================================================================
        # STEP 2: CREATE poliza_origenes_medicos (NEW COVERAGE LOGIC)
        # ========================================================================
        logger.info("Step 2: Creating poliza_origenes_medicos", "AlfaprdStagingPolizas")

        df_origenes_medicos = df_arpprod_sar_auditoria_medica.alias("T1") \
            .join(
                df_arpprod_gar_conceptos_medicos_am.alias("T2"),
                col("T1.cod_concepto_medico_am") == col("T2.cod_concepto_medico_am"),
                "inner"
            ) \
            .select(
                col("T1.nid_siniestro"),
                col("T1.cod_concepto_medico_am").alias("cod_concepto"),
                col("T2.descripcion").alias("concepto")
            )

        # ========================================================================
        # STEP 3: CREATE poliza_valor_asegurado_pagados (CTE)
        # ========================================================================
        logger.info("Step 3: Creating poliza_valor_asegurado_pagados", "AlfaprdStagingPolizas")

        df_valor_asegurado = df_arpprod_sar_pagos_coberturas \
            .groupBy("nid_siniestro") \
            .agg(
                count("*").alias("total_pagos"),
                spark_sum("vlr_pago").alias("total_pagado")
            )

        # ========================================================================
        # STEP 4: BUILD MAIN QUERY WITH JOINS
        # ========================================================================
        logger.info("Step 4: Building main query with joins", "AlfaprdStagingPolizas")

        # Join 1: Sucursales
        result = df_polizas_filtered.alias("PO") \
            .join(
                df_alfaprod_general_cgr_sucursales_alfa.alias("SU"),
                col("SU.COD_SUCURSAL_ALFA") == col("PO.cod_sucursal_alfa"),
                "inner"
            )

        # Join 2: Trabajadores
        result = result.join(
            df_arpprod_par_trabajadores.alias("TRA"),
            col("TRA.nid_afiliacion") == col("PO.nid_poliza"),
            "inner"
        )

        # Join 3: Siniestros (LEFT JOIN)
        result = result.join(
            df_arpprod_sar_siniestros.alias("SIN"),
            col("SIN.nid_trabajador") == col("TRA.nid_trabajador"),
            "left"
        )

        # Join 4: Origenes Medicos (LEFT JOIN - NEW)
        result = result.join(
            df_origenes_medicos.alias("POM"),
            col("SIN.nid_siniestro") == col("POM.nid_siniestro"),
            "left"
        )

        # Join 5: Valor Asegurado (LEFT JOIN)
        result = result.join(
            df_valor_asegurado.alias("PVAP"),
            col("PVAP.nid_siniestro") == col("SIN.nid_siniestro"),
            "left"
        )

        # Join 6: Estados (INNER JOIN)
        result = result.join(
            df_general_cgr_estados.alias("GES"),
            col("GES.cod_estado") == col("TRA.cod_estado_trabajador"),
            "inner"
        )

        # ========================================================================
        # STEP 5: SELECT FINAL COLUMNS WITH BUSINESS LOGIC
        # ========================================================================
        logger.info("Step 5: Selecting final columns", "AlfaprdStagingPolizas")

        result = result.select(
            # Source and identification
            lit(literals["FUENTE"]).alias("origen"),
            coalesce(col("POM.cod_concepto"), lit(literals["COD_COBERTURA_DEFAULT"])).alias("cod_cobertura"),
            coalesce(col("POM.concepto"), lit(literals["DESC_COBERTURA_DEFAULT"])).alias("desc_cobertura"),

            # Policy status
            col("GES.nombre").alias("estado_poliza"),
            lit(literals["FECHA_CANCELACION"]).alias("fecha_cancelacion"),
            lit(literals["FECHA_EFECTIVA_CANCELACION"]).alias("fecha_efectiva_cancelacion"),

            # Policy dates
            col("PO.fec_expedicion").alias("fecha_emision_poliza"),
            col("PO.fec_inicial_vigencia").alias("fecha_inicio_vigencia"),
            col("PO.fec_final_vigencia").alias("fecha_fin_vigencia"),

            # Motivo cancelacion
            lit(literals["MOTIVO_CANCELACION"]).alias("motivo_cancelacion"),

            # Policy identification
            concat(
                col("PO.nid_poliza").cast("string"),
                lit("-"),
                col("TRA.cod_tipo_identificacion").cast("string"),
                lit("-"),
                col("TRA.nid_numero_identificacion")
            ).alias("numero_poliza"),
            col("PO.nid_poliza").alias("numero_poliza_matriz"),
            lit(literals["PAGADOR_MAESTRO"]).alias("pagador_maestro"),

            # Branch information
            col("PO.cod_sucursal_alfa").alias("cod_sucursal"),
            col("SU.nombre").alias("desc_sucursal"),

            # Policy classification
            lit(literals["TIPO_NEGOCIO"]).alias("tipo_negocio"),
            lit(literals["TIPO_POLIZA"]).alias("tipo_poliza"),

            # Financial information
            coalesce(col("PVAP.total_pagado"), lit(0)).cast(DecimalType(18, 5)).alias("valor_asegurado"),
            lit(literals["VALOR_PRIMA"]).cast(DecimalType(18, 5)).alias("valor_prima"),

            # Business classification
            lit(literals["LINEA_CREDITO"]).alias("linea_credito"),
            lit(literals["UNIDAD_NEGOCIO"]).alias("unidad_negocio"),
            lit(literals["CANAL_VENTA"]).alias("canal_venta"),

            # Product code
            col("PO.cod_producto").alias("cod_producto"),

            # Worker identification
            col("TRA.cod_tipo_identificacion").alias("tipo_documento"),
            col("TRA.nid_numero_identificacion").alias("numero_documento"),

            # Calculation metadata
            lit(literals["FECHA_CORTE_CALCULO"]).alias("fecha_corte_calculo"),
            lit(literals["NID_CALCULO"]).alias("nid_calculo"),

            # PK_POLIZA_CLIENTE: nid_poliza||numero_documento (sin separador)
            concat(
                col("PO.nid_poliza").cast("string"),
                col("TRA.nid_numero_identificacion")
            ).alias("pk_poliza_cliente")
        )

        # ========================================================================
        # STEP 6: APPLY DISTINCT (consolidate results like SQL)
        # ========================================================================
        logger.info("Step 6: Applying DISTINCT", "AlfaprdStagingPolizas")

        result = result.distinct()

        logger.success("ARL polizas processed with new coverage logic", "AlfaprdStagingPolizas")

        return result
df_arl = __get_df_arl_polizas(
                df_arpprod_par_polizas,
                df_alfaprod_general_cgr_sucursales_alfa,
                df_arpprod_sar_pagos_coberturas,
                df_arpprod_par_trabajadores,
                df_arpprod_sar_siniestros,
                df_general_cgr_estados,
                df_arpprod_sar_auditoria_medica,
                df_arpprod_gar_conceptos_medicos_am
            )
print(f'Total records: {df_arl.count()}')
df_arl.groupBy("estado_poliza").count().show()
BUCKET = 'alfa-datalake-dev-clientes-assets'
PREFIXES = ['valor_asegurado_vw_gold_siniestros',
            'valor_asegurado_vw_gold_siniestros_reserva',
            'valor_prima_vw_silver']

athena_output_locations = [
f's3://{BUCKET}/{PREFIXES[0]}',
f's3://{BUCKET}/{PREFIXES[1]}',
f's3://{BUCKET}/{PREFIXES[2]}',
]
from logger_config import logger
from utils import Utils
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import DecimalType
from pyspark.sql.functions import col, concat, lit
utils_instance = Utils()
def get_df_previsional_siniestros(spark, output_location: str) -> DataFrame:
   """
   Load Previsional siniestros data from Gold layer Athena view.
   Executes the query defined in query_previsional_polizas.sql against
   Gold layer tables that are not available in Bronze.
   Args:
       output_location (str): S3 path for Athena query results
   Returns:
       DataFrame: Previsional siniestros aggregated data
   """
   query = """
   SELECT
       T1.id_fuente_fk,
       T1.id_poliza_pk,
       T2.unidad_de_negocio,
       "Fecha de Transaccion",
       '1' AS "Tipo_Identificacion",
       CEDULA AS "Numero_Documento_Asegurado",
       T1.id_tipo_transaccion_fk,
       T3.tipo_de_transaccion,
       count(id_siniestro_fk) AS Nro_Siniestros,
       sum(valor_transaccion) AS valor_transaccion
   FROM "alfa-datalake-dev-gold"."gold_caract_siniestros_fact_movimiento_view" T1
   INNER JOIN "alfa-datalake-dev-gold"."gold_caract_siniestros_fact_poliza_pf" T2
       ON T1.id_poliza_pk = T2.id_poliza_pk
   INNER JOIN "alfa-datalake-dev-gold"."gold_caract_siniestros_dim_tipo_transaccion" T3
       ON CAST(T1.id_tipo_transaccion_fk AS INT) = T3.id_tipo_transaccion_pk
       AND T1.id_fuente_fk = '3'
   WHERE UPPER(T3.tipo_de_transaccion) = 'PAGO'
   GROUP BY "Fecha de Transaccion",
       T1.id_fuente_fk,
       T1.id_poliza_pk,
       T2.unidad_de_negocio,
       CEDULA,
       T1.id_tipo_transaccion_fk,
       T3.tipo_de_transaccion
   """
   logger.info("Loading Previsional siniestros from Gold layer", "AlfaprdStagingPolizas")
   df = utils_instance.execute_athena_query_to_dataframe(
       spark=spark,
       query=query,
       database="alfa-datalake-dev-gold",
       output_location=output_location
   )
   df = df.withColumn(
         "valor_transaccion",
         col("valor_transaccion").cast(DecimalType(18, 2))
       ) \
       .withColumn(
            "pk_client",
           concat(
                            col("id_poliza_pk"),
                            lit("-"),
                            col("Tipo_Identificacion"),
                            lit("-"),
                            col("Numero_Documento_Asegurado")
           ) )
   return df
def transform_polizas_numero_poliza(df):
    """
    Transform numero_poliza field for ALFAPRD_POLIZAS_MVP2.
    Nueva lógica: numero_poliza = CONCAT(numero_poliza_matriz, '-', tipo_identificacion, '-', numero_identificacion)
    """
    from pyspark.sql.functions import concat, col, coalesce, lit
    
    if all(c in df.columns for c in ['numero_poliza_matriz', 'cod_tipo_identificacion', 'numero_identificacion']):
        logger.info("Transforming numero_poliza field: CONCAT(numero_poliza_matriz, '-', tipo_identificacion, '-', numero_identificacion)")
        df = df.withColumn(
            "numero_poliza",
            concat(
                coalesce(col("numero_poliza_matriz"), lit("")),
                lit("-"),
                coalesce(col("cod_tipo_identificacion"), lit("")),
                lit("-"),
                coalesce(col("numero_identificacion"), lit(""))
            )
        )
    else:
        logger.warning("Fields required for numero_poliza transformation not found, skipping transformation")
    
    return df
def transform_clientes_poliza(df):
    """
    Transform poliza field for ALFAPRD_CLIENTES_MVP2.
    Nueva lógica:
    1. Separar el campo poliza por guion '-' y tomar el primer elemento
    2. poliza = CONCAT(poliza_primer_elemento, '-', tipo_identificacion, '-', numero_identificacion)
    
    Ejemplo: Si poliza = "9-2-1022990929", extraer "9" y concatenar con guiones
    Resultado: "9-1-1234567890"
    """
    from pyspark.sql.functions import concat, col, split, coalesce, lit, trim
    
    if all(c in df.columns for c in ['poliza', 'cod_tipo_identificacion', 'numero_identificacion']):
        logger.info("Transforming poliza field: Extract first element from split by '-' and CONCAT with '-' separator")
        
        # Separar poliza por guion y tomar el primer elemento
        df = df.withColumn(
            "poliza_primer_elemento",
            trim(split(col("poliza"), "-").getItem(0))
        )
        
        # Reconstruir el campo poliza CON GUIONES
        df = df.withColumn(
            "poliza",
            concat(
                coalesce(col("poliza_primer_elemento"), lit("")),
                lit("-"),
                coalesce(col("cod_tipo_identificacion"), lit("")),
                lit("-"),
                coalesce(col("numero_identificacion"), lit(""))
            )
        ).drop("poliza_primer_elemento")  # Eliminar columna temporal
        
    else:
        logger.warning("Fields required for poliza transformation not found, skipping transformation")
    
    return df
def get_df_previsional_reserva_sn(spark, output_location: str) -> DataFrame:
   """
   Load Previsional siniestros data from Gold layer Athena view.
   Executes the query defined in query_previsional_polizas.sql against
   Gold layer tables that are not available in Bronze.
   Args:
       output_location (str): S3 path for Athena query results
   Returns:
       DataFrame: Previsional siniestros aggregated data
   """
   query = """
   WITH query_reserva AS (
    SELECT
        ano,
        mes,
        estado,
        SUM(monto) AS sum_monto,
        id_siniestro_fk
    FROM "alfa-datalake-dev-gold"."gold_caract_siniestros_fact_estado_siniestro"
    WHERE id_fuente_fk = '3'
    AND estado = 'En Reserva'
    GROUP BY ano, mes, estado, id_siniestro_fk
    )
    SELECT T1.sum_monto,
        T1.id_siniestro_fk,
        '1' AS "tipo_identificacion_asegurado",
        T2.cedula AS "numero_documento_asegurado"
    FROM query_reserva T1
    INNER JOIN "alfa-datalake-dev-gold"."gold_caract_siniestros_fact_siniestros_pf" T2
    ON T1.id_siniestro_fk = T2.id_siniestro_pk
   """
   logger.info("Loading Previsional siniestros from Gold layer", "AlfaprdStagingPolizas")
   df = utils_instance.execute_athena_query_to_dataframe(
       spark=spark,
       query=query,
       database="alfa-datalake-dev-gold",
       output_location=output_location
   )
   df = df.withColumn(
         "valor_transaccion",
         col("sum_monto").cast(DecimalType(18, 2))
       ) \
       .withColumn(
            "pk_client",
           concat(
                            col("id_siniestro_fk"),
                            lit("-"),
                            col("tipo_identificacion_asegurado"),
                            lit("-"),
                            col("numero_documento_asegurado")
           ) )
   return df
def get_df_clientes_valor_prima_silver(spark, output_location: str) -> DataFrame:
   """
   Load Previsional siniestros data from Gold layer Athena view.
   Executes the query defined in query_previsional_polizas.sql against
   Gold layer tables that are not available in Bronze.
   Args:
       output_location (str): S3 path for Athena query results
   Returns:
       DataFrame: Previsional siniestros aggregated data
   """
   query = """
       SELECT
        rol,
        cod_tipo_identificacion,
        numero_identificacion,
        poliza,
        periodo_ejecucion,
        SUM(valor_prima) as valor_prima_periodo
    FROM "alfa-datalake-dev-silver"."clientes_alfaprd_silver"
    WHERE rol = '2'
    GROUP BY poliza, 
             periodo_ejecucion,
             rol,
             cod_tipo_identificacion,
             numero_identificacion
   """
   logger.info("Loading Clientes Agruped by Poliza and Valor Prima from Silver layer", "AlfaprdStagingPolizas")
   df = utils_instance.execute_athena_query_to_dataframe(
       spark=spark,
       query=query,
       database="alfa-datalake-dev-silver",
       output_location=output_location
   )
   df = transform_clientes_poliza(df)
   return df
df_previsional_siniestros = get_df_previsional_siniestros(spark, athena_output_locations[0])
df_previsional_reservas_sn = get_df_previsional_reserva_sn(spark, athena_output_locations[1])
df_silver_valor_prima_clientes = get_df_clientes_valor_prima_silver(spark, athena_output_locations[2])
full_path = 's3://alfa-datalake-dev-silver/POLIZAS_PRODUCTO/ALFAPRD/CLIENTES_ALFAPRD_SILVER/'
df_clientes_silver = utils_instance.read_full_delta(spark, full_path)
full_path = 's3://alfa-datalake-dev-silver/POLIZAS_PRODUCTO/ALFAPRD/POLIZAS_ALFAPRD_SILVER/'
df_polizas_silver = utils_instance.read_full_delta(spark, full_path)
#df_clietes_clean = transform_clientes_poliza(df_clientes_silver)
df_polizas_clean = transform_polizas_numero_poliza(df_polizas_silver)
df_silver_valor_prima_clientes.show(5, truncate=False)
df_polizas_clean.show(5, truncate=False)
filter_poliza = '9-2-38601098'
df_polizas_clean.filter(col("numero_poliza") == filter_poliza).show(truncate=False)
df_silver_valor_prima_clientes.filter(col("poliza") == filter_poliza).show(truncate=False)
df_previsional_reservas_sn.show(5,truncate=False)
df_silver_data = spark.read.csv("s3://alfa-datalake-dev-clientes-assets/test/3eab58eb-9742-4a8d-a92b-f3b47f78ef1d.csv",
                               header="true")
df_silver_data.show(5, truncate=False)
# Select Columns of | df_previsional_siniestros | to match with | df_silver_data |
df_prv_siniestros = df_previsional_siniestros.select("pk_client",
        "id_poliza_pk",
        "Tipo_Identificacion",
        "Numero_Documento_Asegurado",
        "valor_transaccion",
        "Nro_Siniestros"
)
df_prv_siniestros.show(5, truncate=False)
# joined by tipo_id + num_documento
df_joined_by_num_document = df_silver_data.alias("T1") \
            .join(
                df_prv_siniestros.alias("T2"),
                col("T1.numero_documento") == col("T2.Numero_Documento_Asegurado"),
                "inner"
            ) \
            .select(
                col("T1.numero_documento").alias("numero_doc_silver"),
                col("T2.Numero_Documento_Asegurado").alias("numero_doc_view_gold"),
                col("T1.Tipo_Identificacion").alias("tipo_id_silver"),
                col("T2.Tipo_Identificacion").alias("tipo_id_view_gold"),
                col("T2.valor_transaccion").alias("valor_transaccion_view_gold"),
                col("T1.valor_transaccion").alias("valor_transaccion_silver")
            )
print(f'Total Matched: {df_joined_by_num_document.count()}')
df_joined_by_num_document.show(5, truncate=False)
df_joined_by_num_document.groupBy("tipo_id_silver").count().show(5, truncate=False)
df_silver_data.show(5, truncate=False)
df_stg_silver_data = df_silver_data.withColumn(
    "pk_client_w_poliza",
    concat(
        col("Tipo_Identificacion"),
        lit("-"),
        col("numero_documento")
    )
)

df_prv_siniestros = df_prv_siniestros.withColumn(
    "pk_client_w_poliza",
    concat(
        lit("2"),
        lit("-"),
        col("Numero_Documento_Asegurado")
    )
)

df_joined = df_stg_silver_data.alias("T1") \
            .join(
                df_prv_siniestros.alias("T2"),
                col("T1.pk_client_w_poliza") == col("T2.pk_client_w_poliza"),
                "inner"
            )
print(f'Total Matched: {df_joined.count()}')
df_joined.show(5, truncate=False)
df_prv_siniestros.groupBy("id_poliza_pk").count().show(5, truncate=False)
job.commit()