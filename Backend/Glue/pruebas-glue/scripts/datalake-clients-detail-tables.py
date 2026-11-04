# AWS Glue - diagnostico poliza_emision: joins comentables, salida solo FACT
import sys, boto3, traceback
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import date, timedelta

from commons import Commons
from prepareData import PrepareData
from reports import reports


args=getResolvedOptions(sys.argv,[
    'JOB_NAME','PREFIX_TEMPTABLE_FILE','FILE_TEMP_TABLES_NAME',
    'BUCKET_SOURCE_TEMP_FILE','SAVE_BUCKET','SAVE_PREFIX',
    'ANIO_EXEC','MES_EXEC', 'EXEC_C_POL_E','EXEC_C_ROL_EST','EXECT_ULT_SIN','DATABASE','EXECT_DETALLE_COMP','EXECT_UNIQ_CLI','EXEC_VAL_COMP','EXEC_CLI_VIEW','EXEC_COMP_CLI',
    'STEP_ONE','STEP_TWO','STEP_THREE','STEP_FOUR'
])

PREFIX_TEMPTABLE_FILE=args['PREFIX_TEMPTABLE_FILE']
FILE_TEMP_TABLES_NAME=args['FILE_TEMP_TABLES_NAME']
BUCKET_SOURCE_TEMP_FILE=args['BUCKET_SOURCE_TEMP_FILE']
SAVE_BUCKET=args['SAVE_BUCKET']
SAVE_PREFIX=args['SAVE_PREFIX']
ANIO_EXEC=int(args['ANIO_EXEC'])
MES_EXEC=int(args['MES_EXEC'])
DATABASE = args['DATABASE']
EXEC_C_POL_E = int(args['EXEC_C_POL_E'])
EXEC_C_ROL_EST = int(args['EXEC_C_ROL_EST'])
EXECT_ULT_SIN = int(args['EXECT_ULT_SIN'])
EXECT_DETALLE_COMP = int(args['EXECT_DETALLE_COMP'])
EXECT_UNIQ_CLI = int(args['EXECT_UNIQ_CLI'])
EXEC_VAL_COMP = int(args['EXEC_VAL_COMP'])
EXEC_CLI_VIEW = int(args['EXEC_CLI_VIEW'])
EXEC_COMP_CLI = int(args['EXEC_COMP_CLI'])
STEP_ONE = int(args['STEP_ONE'])
STEP_TWO = int(args['STEP_TWO'])
STEP_THREE = int(args['STEP_THREE'])
STEP_FOUR = int(args['STEP_FOUR'])

sc=SparkContext(conf=SparkConf())
glueContext=GlueContext(sc)
spark=glueContext.spark_session
spark.conf.set('spark.sql.shuffle.partitions','800')
spark.conf.set('spark.sql.adaptive.enabled','true')
spark.conf.set('spark.sql.adaptive.coalescePartitions.enabled','true')
spark.conf.set('spark.sql.adaptive.skewJoin.enabled','true')
spark.conf.set('spark.sql.parquet.enableVectorizedReader','true')
spark.conf.set('spark.sql.parquet.aggregatePushdown','false')

spark.sparkContext.setCheckpointDir(
    f"s3://{SAVE_BUCKET}/checkpoints/refactor_tablas/"
)

job=Job(glueContext)
job.init(args['JOB_NAME'],args)
s3_client=boto3.client('s3')

"""verificamos los datos entrantes para la carga incremental."""
if ANIO_EXEC == 0:
    fecha_anterior = date.today().replace(day=1) - timedelta(days=1)
    ANIO_EXEC = fecha_anterior.year
    MES_EXEC = fecha_anterior.month

commonsHelper = Commons(glueContext, spark, ANIO_EXEC, MES_EXEC)
PrepareData = PrepareData(glueContext, spark)
reports = reports(glueContext, spark)

def main():
    try:
    
        prefix=PREFIX_TEMPTABLE_FILE.strip('/')
        archivo=FILE_TEMP_TABLES_NAME.lstrip('/')
        key_excel=f'{prefix}/{archivo}' if prefix else archivo
        
        configuracion=commonsHelper.leer_mapeo_xlsx(BUCKET_SOURCE_TEMP_FILE,key_excel)
        dfs=commonsHelper.cargar_dataframes(configuracion)
        
        df_contactabilidad = PrepareData.preparar_contactabilidad(
        dfs["ubicacion_contactabilidad"]
        )
        
        df_cliente = PrepareData.preparar_cliente(dfs['cliente'])
        df_cliente = PrepareData.preparar_roles_cliente(dfs, df_cliente)
        
        print("Iniciando checkpoint de df_cliente...")

        df_cliente = df_cliente.checkpoint(
            eager=True
        )    
        print("CHECKPOINT df_cliente COMPLETADO")
        print("=" * 80)
        
        if STEP_ONE == 1:
            if EXEC_C_POL_E == 1:
                df=reports.generar_reporte_poliza_emision(dfs)
                commonsHelper.guardar_dataframe_delta(
                    df=df,
                    bucket=SAVE_BUCKET,
                    prefix=SAVE_PREFIX,
                    nombre_df="gold_caract_clientes_poliza_emision",
                    anio_exec=ANIO_EXEC,
                    mes_exec=MES_EXEC,
                    forzar_recarga=False
                )
            if EXEC_C_ROL_EST == 1:
                df=reports.generar_reporte_rol_cliente_estado(dfs,df_cliente,df_contactabilidad)
                commonsHelper.guardar_dataframe_delta(
                    df=df,
                    bucket=SAVE_BUCKET,
                    prefix=SAVE_PREFIX,
                    nombre_df="gold_caract_clientes_rol_cliente_estado",
                    anio_exec=ANIO_EXEC,
                    mes_exec=MES_EXEC,
                    forzar_recarga=False
                )
            if EXECT_ULT_SIN == 1:
                df_movimiento = commonsHelper.leer_vista_athena(
                    database=DATABASE,
                    vista="gold_caract_siniestros_fact_movimiento_view",
                    output_s3="s3://alfa-datalake-qa-gold/GOLD/CLIENTES_GOLD/temp/athena"
                )
                df = PrepareData.preparar_movimiento_siniestros(df_movimiento)
                commonsHelper.guardar_dataframe(
                    df=df,
                    bucket=SAVE_BUCKET,
                    prefix=SAVE_PREFIX,
                    nombre_df="gold_siniestros_ultimo_movimiento"
                )
        if STEP_TWO == 1:
            if EXEC_CLI_VIEW == 1:
                df = reports.generar_datos_completos_view(dfs, df_cliente, df_contactabilidad)
                df = df.filter(
                    F.col("id_cliente").isNotNull()
                )
                commonsHelper.guardar_dataframe(
                    df=df,
                    bucket=SAVE_BUCKET,
                    prefix=SAVE_PREFIX,
                    nombre_df="gold_caract_clientes_datos_completos_view"
                )
        if STEP_THREE == 1:
            print("llego al paso tres")
            if EXECT_DETALLE_COMP == 1:            
                df = reports.generar_detalle_clientes(
                    dfs, 
                    df_cliente
                )
                commonsHelper.guardar_dataframe_delta(
                    df=df,
                    bucket=SAVE_BUCKET,
                    prefix=SAVE_PREFIX,
                    nombre_df="gold_clientes_detalle_completo",
                    anio_exec=ANIO_EXEC,
                    mes_exec=MES_EXEC,
                    forzar_recarga=False
                )   

            
            if EXECT_UNIQ_CLI == 1:
                df = reports.generar_clientes_unicos(dfs)
                df = df.filter(
                    F.col("id_cliente").isNotNull()
                )
                commonsHelper.guardar_dataframe_delta(
                    df=df,
                    bucket=SAVE_BUCKET,
                    prefix=SAVE_PREFIX,
                    nombre_df="gold_caract_clientes_clientes_unicos",
                    anio_exec=ANIO_EXEC,
                    mes_exec=MES_EXEC,
                    forzar_recarga=False
                )   
    
            if EXEC_VAL_COMP == 1:
                print("entro a comportamiento valores")
                df = reports.generar_resumen_clientes(dfs)
                commonsHelper.guardar_dataframe_delta(
                    df=df,
                    bucket=SAVE_BUCKET,
                    prefix=SAVE_PREFIX,
                    nombre_df="gold_comportamiento_valores",
                    anio_exec=ANIO_EXEC,
                    mes_exec=MES_EXEC,
                    forzar_recarga=False
                )   
               
        if STEP_FOUR == 1:
            if EXEC_COMP_CLI == 1:
                df = reports.generar_comportamiento_clientes(dfs)
                commonsHelper.guardar_dataframe_delta(
                    df=df,
                    bucket=SAVE_BUCKET,
                    prefix=SAVE_PREFIX,
                    nombre_df="gold_comportamiento_clientes",
                    anio_exec=ANIO_EXEC,
                    mes_exec=MES_EXEC,
                    forzar_recarga=False
                )  
                commonsHelper.guardar_dataframe_delta(
                    df=df,
                    bucket=SAVE_BUCKET,
                    prefix=SAVE_PREFIX,
                    nombre_df="gold_puente_cliente_negocio",
                    anio_exec=ANIO_EXEC,
                    mes_exec=MES_EXEC,
                    forzar_recarga=False
                )  
        job.commit()
    except Exception as e:
        print(traceback.format_exc())
        raise e

if __name__=='__main__':
    main()
