import sys
import traceback
import re
import boto3
import compt3.src.utils.common.df_method as df_mtd
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from datetime import datetime, timedelta
from commons.ExtractHelper import ExtractHelper
from pyspark.sql.types import DecimalType, StringType, IntegerType, BooleanType, FloatType, DoubleType, StructType, StructField
from commons.TransformationHelper import TransformationHelper
from commons.PolizaHelper import PolizaHelper
from commons.UrincterrdeudoresHelper import UrincterrdeudoresHelper
from commons.ClaimHelper import ClaimHelper
from commons.EstadoSiniestroHelperA import EstadoSiniestroHelperA
from commons.IncrementalCharge import IncrementalCharge
from commons.DocumentoHelper import DocumentoHelper
from pyspark.sql import functions as F
from step_gold.orchestrator_gold import main
from compt3.orchestrator.orchestrator_gold_dane import orchestrator_gold_dane
from commons.StepGold import StepGold
from commons.StepGoldPlus import StepGoldPlus
from commons.StepGoldBodega import StepGoldBodega
from commons.GoldTransformation import GoldTransformation
from commons.FechaObjHelper import FechaObjHelper
from commons.SiprenDocumentoHelper import SiprenDocumentoHelper
from pyspark.sql.window import Window

 
################################
### Configuracion de Entorno ###
################################
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BUCKET_NAME_SILVER', 'BUCKET_NAME_GOLD','BUCKET_NAME_ASSETS','DATE','EXECUTE_DIM','EXECUTE_FACT','HISTORIC','DATE_CUT_1','DATE_CUT_2'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead","LEGACY")
sc.setCheckpointDir("s3://alfa-datalake-qa-assets/glue_gold_tmp/")

####################################
### Iniciar variables de entorno ###
####################################
NM_BUCKET_ASSETS = args['BUCKET_NAME_ASSETS']
BUCKET_NAME_SILVER = args['BUCKET_NAME_SILVER']
BUCKET_NAME_GOLD = args['BUCKET_NAME_GOLD']
DATE = args['DATE']
EXECUTE_DIM = args['EXECUTE_DIM']
EXECUTE_FACT = args['EXECUTE_FACT']
HISTORIC = args['HISTORIC']
DATE_CUT_1 = args['DATE_CUT_1']
DATE_CUT_2 = args['DATE_CUT_2']
EXECUTE_FACT_SINIESTROS = True
EXECUTE_FACT_MOVIMIENTO = True
EXECUTE_FACT_ESTADO = True
EXECUTE_FACT_POLIZA = True
EXECUTE_FACT_POLIZA_DETALLADA = True
EXECUTE_FACT_ASEGURADO = True

########################
###Iniciar librerias ###
########################
ExtractHelper = ExtractHelper(glueContext, spark, "-", "-")
IncrementalCharge = IncrementalCharge(glueContext, spark, "-", "-")
PolizaHelper = PolizaHelper(glueContext, spark)
Urincterrdeudores_helper = UrincterrdeudoresHelper(glueContext, spark)
TransformationHelper = TransformationHelper()
ClaimHelper = ClaimHelper(glueContext, spark)
GoldTransformation = GoldTransformation(glueContext, spark, 'None')
DocumentoHelper = DocumentoHelper(glueContext, spark)
FechaObj_helper = FechaObjHelper(glueContext, spark) 
EstadoSiniestro_Helper = EstadoSiniestroHelperA(glueContext, spark)
StepGold = StepGold(glueContext, spark)
StepGoldPlus = StepGoldPlus(glueContext, spark)
StepGoldBodega = StepGoldBodega(glueContext, spark)
SiprenDocumentoHelper = SiprenDocumentoHelper(glueContext, spark)

path = f"s3://{NM_BUCKET_ASSETS}/support_phase_files/procces_ola1.xlsx"
sheet = "gold_phase"
df_tables = ExtractHelper.extract_excel_data(path, sheet)
sheet = "gold_tables"
df_tmp_tables = ExtractHelper.extract_excel_data(path, sheet)

def distinct_tables(tb_name):
    df = spark.sql(f"SELECT * FROM global_temp.{tb_name}")
    df = df.distinct()
    df.createOrReplaceGlobalTempView(tb_name)
    
    return True
def obtain_last_date(fecha = None):
    """
    This Function help us to obtein the last cut date.
    """
    if fecha is None:
        fecha = datetime.now()

    primer_dia_mes_actual = datetime(fecha.year, fecha.month, 1)

    ultimo_dia_mes_anterior = primer_dia_mes_actual - timedelta(days=1)
    if DATE_CUT_1 != '0':
        date = DATE_CUT_1.split("-")
        date_return =  f"{date[0]}-{date[1]}-{date[2]}", f"{ultimo_dia_mes_anterior.year}-{ultimo_dia_mes_anterior.month}-{ultimo_dia_mes_anterior.day}"
    if DATE_CUT_2 != '0':
        date = DATE_CUT_2.split("-")
        date_return = f"{ultimo_dia_mes_anterior.year}-{ultimo_dia_mes_anterior.month}-1", f"{date[0]}-{date[1]}-{date[2]}"
    if DATE_CUT_1 != '0' and DATE_CUT_2 != '0':
        date1 = DATE_CUT_1.split("-")
        date2 = DATE_CUT_2.split("-")
        date_return = f"{date1[0]}-{date1[1]}-{date1[2]}", f"{date2[0]}-{date2[1]}-{date2[2]}"
    if DATE_CUT_1 == '0' and DATE_CUT_2 == '0':
        date_return = f"{ultimo_dia_mes_anterior.year}-{ultimo_dia_mes_anterior.month}-1", f"{ultimo_dia_mes_anterior.year}-{ultimo_dia_mes_anterior.month}-{ultimo_dia_mes_anterior.day}"
    
    return date_return


try:
    
    for row in df_tmp_tables.collect():
        table = row["Table"].upper()
        if row["Phase"].upper() == "SILVER":
            path_source =f"s3://{BUCKET_NAME_SILVER}/{row['DB'].upper()}/{row['SCHEMA'].upper()}/{row['DB'].upper()}_SILVER_"
        else:
            path_source =f"s3://{BUCKET_NAME_GOLD}/{row['DB'].upper()}/{row['SCHEMA'].upper()}/{row['DB'].upper()}-CARACT_SINIESTROS-"
        IncrementalCharge.charge_tmp_tables(path_source, table, 'Gold')
    
    if EXECUTE_DIM == "1":

        for row in df_tables.collect():
            if row['Status'] == '1':
                print(table)
                sql = row['Logic']
                df = spark.sql(sql)
                
                schema_str = row['ESQUEMA']
                
                table = row['Table'].upper()
                schema = TransformationHelper.parse_schema(schema_str)
                
                for field in schema.fields:
                    field_name = field.name
                    field_type = field.dataType
                    
                    df = df.withColumn(field_name, df[field_name].cast(field_type))
                
                df = spark.createDataFrame(df.rdd, schema)
                df= df.distinct()
                df_incremental = None
                try:
                    df_incremental = spark.sql(f"SELECT * FROM global_temp.{table}")
                except Exception as e:
                    print(e)
                if df_incremental is not None:
                    pk_id = row['PK'].strip()
                    df = df.join(df_incremental, on=pk_id, how='left_anti')
                path = f"s3://{BUCKET_NAME_GOLD}/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-{table}/"
                ExtractHelper.save_files(df,path)
            if row['Status'] == '2':
                if row['Table'] == 'DIM_CIUDAD':
                    df = orchestrator_gold_dane(args, spark, glueContext, "ACSELE_SILVER_MUNICIPIO")
                    path = f"s3://{BUCKET_NAME_GOLD}/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-DIM_CIUDAD/"
                    ExtractHelper.save_files(df,path)
                if row['Table'] == 'DIM_DEPARTAMENTO':   
                    df = orchestrator_gold_dane(args, spark, glueContext, "DEPARTMENT")
                    path = f"s3://{BUCKET_NAME_GOLD}/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-DIM_DEPARTAMENTO/"
                    ExtractHelper.save_files(df,path)
                    
    if EXECUTE_FACT == "1":
        
        fecha_corte_1, fecha_corte_2 = obtain_last_date()
        
        TABLES = ['POLICYDCO', 'PREPOLICY', 'RISKUNITDCO', 'AGREGATEDPOLICY', 'AGREGATEDRISKUNIT', 'CONTEXTOPERATION']
        for table in TABLES:
            distinct_tables(table)
        
        df_ubicacion = GoldTransformation.chargue_ubicacion_acsele()
        print("ENTRE A ACSEL")
        df = main(
            args=args,
            spark_session=spark,
            glue_context=glueContext
        )
        print("SALI DE ACSEL")
        try:
            df = df.checkpoint(eager= True)
            print("cree el checkpoitn df")
        except Exception as e:
            print(e)
        
        df_tierra = StepGold.create_component()
        try:
            df_tierra = df_tierra.checkpoint(eager= True)
            print("cree el checkpoitn de tierra")
        except Exception as e:
            print(e)
            
        df_plus = StepGoldPlus.create_component()
        df_plus = df_plus.filter(~F.col("fecha_transaccion").isNull())
        try:
            df_plus = df_plus.checkpoint(eager= True)
            print("cree el checkpoitn de plus")
        except Exception as e:
            print(e)
        
        df_bodega = StepGoldBodega.create_component()
        df_bodega = df_bodega.withColumn("fecha_objecion", F.lit(None))
        
        df_main = df_tierra.unionByName(df_plus).unionByName(df_bodega)
   
        df_ubicacion.alias("UB")
        window_spec = Window.partitionBy("ID_SINIESTRO_CLAIM").orderBy("ID_SINIESTRO_CLAIM")
        df_ubicacion = df_ubicacion.withColumn("row_number", F.row_number().over(window_spec))
        df_ubicacion = df_ubicacion.filter(F.col("row_number")== 1).drop("row_number")
        print(f"Fin df ubicaciones - {datetime.now()}")
        
        df_date = FechaObj_helper.create_fecha_obj()
        window_spec = Window.partitionBy("nro_siniestro_claim").orderBy("nro_siniestro_claim")
        df_date = df_date.withColumn("row_number", F.row_number().over(window_spec))
        df_date = df_date.filter(F.col("row_number")== 1).drop("row_number")
        print("fin creacion de objecion")
        df = df.join(
            df_date,
            on="NRO_SINIESTRO_CLAIM",
            how="left"
        )

        df = df.join(
            df_ubicacion,
            on="ID_SINIESTRO_CLAIM",
            how="left"
        )

        df = df.withColumn(
            "fundamento_no_pago", F.lit(None)  # No importa la magnitud de la diferencia, siempre será "Pagado"
        )
        
        df2 = ClaimHelper.create_claim()
        print(df2.count())
        print("conteo del point inicial")
        df = GoldTransformation.clean_first_df(df, df2)

        df = df_mtd.step_clean_empty_values(
            df_data=df,
            column_id="departamentovalue",
            column_id_value="01",
            column_name="ciudadvalue",
            column_name_value="01001",
        )
        print(df.count())
        print("despues del primer clean")
        
        df = GoldTransformation.clean_second_df(df)
        print(df.count())
        print("despues del segundo clean")
        print(f"fin ultima parte antes de sipren - {datetime.now()}")
        decimal_cast = "decimal(19, 0)"
        df_sini =  df.select(
                F.col("nro_siniestro_claim").alias("id_siniestro_pk"),
                F.col("CAUSA_SINIESTRO").alias("id_causa_siniestro_fk"),
                F.col('source').alias("id_fuente_fk"),
                F.col("FE_AVISO").cast("date").alias("fecha_aviso")
            ).distinct() 
        
        df_sini_sipren =  df_main.select(
                F.col("numero_siniestro").alias("id_siniestro_pk"),
                F.col("cod_causa").alias("id_causa_siniestro_fk"),
                F.col('id_fuente_fk').alias("id_fuente_fk"),
                F.col("fecha_aviso").cast("date").alias("fecha_aviso"),
                F.col("cedula")
                
            ).distinct()  

        print("fin seleccion datos movimientos")
        windowSpec = Window.partitionBy("id_siniestro_pk").orderBy(F.col("fecha_aviso").desc())
        df_max_reclamo = df_sini.withColumn("row_num", F.row_number().over(windowSpec))
        df_max_reclamo = df_max_reclamo.filter(F.col("row_num") == 1).drop("row_num")
        df_max_reclamo_sipren = df_sini_sipren.withColumn("row_num", F.row_number().over(windowSpec))
        df_max_reclamo_sipren = df_max_reclamo_sipren.filter(F.col("row_num") == 1).drop("row_num")
        df_doc= DocumentoHelper.create_cedula() 
        df_cel = df_max_reclamo.join(df_doc, F.col("id_siniestro_pk") == F.col("numero_siniestro") ,how = "left").drop("numero_siniestro")
        print("despues del cruce contra la cedula")
        print(df_cel.count())
        df_cel = df_cel.union(df_max_reclamo_sipren)
        df_siniestros = df_cel.withColumn(
            "cedula",
            F.when(
                F.col("cedula").isNull() | (F.col("cedula") == ""),
                "No registra"
            ).otherwise(F.col("cedula"))
        )
        
        print(f"fin de configuracion documentos - {datetime.now()}")
        if EXECUTE_FACT_SINIESTROS:
            df_siniestros = df_siniestros.union(df_max_reclamo_sipren)
            print(f"fin carga siniestros - {datetime.now()}")
        df_siniestros = df_siniestros.dropDuplicates(["id_siniestro_pk"]) # bugfix - 020725
        df_incremental = None
        try:
            df_incremental = spark.sql("SELECT * FROM global_temp.SINIESTROS")
        except Exception as e:
            print(e)
        if df_incremental is not None:
            df_siniestros = df_siniestros.join(df_incremental, on="id_siniestro_pk", how='left_anti')
            df_siniestros = df_siniestros.dropDuplicates(["id_siniestro_pk"]) # bugfix - 020725

        ExtractHelper.save_files(df_siniestros, f"s3://{BUCKET_NAME_GOLD}/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-FACT_SINIESTROS-PF/")
        print(f"fin save sini - {datetime.now()}")
        
        df_comp4 = GoldTransformation.chargue_comp4_df(df)
        df_comp4 = df_comp4.withColumn(
            "tipo_transaccion",
            F.when(
                F.upper(F.col("tipo_transaccion")) == 'RECOBROS',
                F.lit("Recobro")
                ).otherwise(
                    F.col("tipo_transaccion")
                    )
            )
        
        df_movimientos = GoldTransformation.chargue_comp4_mov(df_comp4)
        
        df_movimientos_plus = df_main.select(
            F.col("numero_siniestro").alias("id_siniestro_fk"),
            F.col("id_tipo_transaccion_fk"),
            F.col("id_categoria_fk"),
            F.col("fecha_transaccion").cast("date"),
            F.lit(None).cast("date").alias("FECHA_OBJECION"),
            F.col("valor_transaccion_ajustado").cast("double"),
            F.col("valor_transaccion").cast("double"),
            F.col("id_fuente_fk"),
            F.col("cod_origen").alias("id_cobertura_fk"),
            F.col("fecha_ocurrencia").cast("date").alias("fecha_ocurrencia"),
            F.col("poliza").cast("string").alias("id_poliza_fk"),
            F.col("depto_lugar_ocurrencia").cast("string").alias("id_departamento_ocurrencia_fk"),
            F.col("ciudad_lugar_ocurrencia").cast("string").alias("id_ciudad_ocurrencia_fk")
        )

        if EXECUTE_FACT_MOVIMIENTO:
            df_movimientos = df_movimientos.unionAll(df_movimientos_plus)
        
        df_movimientos = df_movimientos.withColumn(
            "valor_transaccion_ajustado",
            F.when(
                ((F.col("id_fuente_fk") == "0") & (F.lower(F.col("id_tipo_transaccion_fk")) == "6")) |
         
                ((F.col("id_fuente_fk") == "1") & (F.lower(F.col("id_tipo_transaccion_fk")).isin([
                    "2", "4", "5","8"
                ]))) |
         
                ((F.col("id_fuente_fk") == "2") & (F.lower(F.col("id_tipo_transaccion_fk")).isin([
                    "5", "6"
                ]))) |
         
                ((F.col("id_fuente_fk") == "3") & (F.lower(F.col("id_tipo_transaccion_fk")).isin([
                    "5", "6"
                ]))),
                F.col("valor_transaccion_ajustado") * -1  # Multiplicar por -1
                #(mantener valor original)
            ).otherwise(F.col("valor_transaccion_ajustado"))
        )
        if HISTORIC == '1':
            df_movimientos = df_movimientos.filter(F.to_date((F.col("fecha_transaccion")),"YYYY-mm-dd") <= fecha_corte_2)
        else:
            df_movimientos = df_movimientos.filter(F.to_date((F.col("fecha_transaccion")),"YYYY-mm-dd") >= fecha_corte_1)
            df_movimientos = df_movimientos.filter(F.to_date((F.col("fecha_transaccion")),"YYYY-mm-dd") <= fecha_corte_2)
        
        path = f"s3://{BUCKET_NAME_GOLD}/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-FACT_MOVIMIENTO/"
        ExtractHelper.save_files(df_movimientos,path)

        df_fact_estado = df_comp4.select(
            F.col("id_siniestro_fk"),
            F.col("fecha_transaccion"),
            F.col("TIPO_TRANSACCION"),
            F.col("id_fuente_fk"),
            F.col("FECHA_OBJECION"),
            F.col("valor_transaccion_ajustado").alias("VALOR_TRANSACCION"),
            F.col("fundamento_no_pago")
        )
        
        df_estado_acsel = df_fact_estado.filter(F.col("id_fuente_fk") != "0")
        df_estado_point = df_fact_estado.filter(F.col("id_fuente_fk") == "0")
        df_estado_point = df_estado_point.filter(F.lower(F.col("categoria")).isin("perdida","gastos"))
        df_estado_point = df_estado_point.filter(F.lower(F.col("TIPO_TRANSACCION")) != "rechazo de pago point")#como hacer esta parte ?

        df_fact_estado = df_estado_acsel.unionAll(df_estado_point)
        df_fact_estado = df_fact_estado.drop("categoria")

        
        if EXECUTE_FACT_ESTADO:
            df_movimientos_dim = spark.sql("SELECT * FROM global_temp.DIM_TIPO_TRANSACCION")
            df_movimientos_dim = df_movimientos_dim.distinct()
            df_estado_plus = df_movimientos_plus.join( df_movimientos_dim, F.col("id_tipo_transaccion_pk") == F.col("id_tipo_transaccion_fk"), "inner")
            print("homologacion con transaccion")
            print(df_estado_plus.count())
            df_estado_plus = df_estado_plus.select(
                    F.col("id_siniestro_fk"),
                    F.col("fecha_transaccion"),
                    F.col("tipo_de_transaccion").alias("TIPO_TRANSACCION"),
                    F.col("id_fuente_fk"),
                    F.col("FECHA_OBJECION"),
                    F.col("valor_transaccion_ajustado").alias("VALOR_TRANSACCION"),
                    F.lit("").alias("fundamento_no_pago")
                )
            
            df_fact_estado = df_fact_estado.unionAll(df_estado_plus)
            df_fact_estado = df_fact_estado.checkpoint(eager= True)
        
        
        try:
            df_incremental = spark.sql("SELECT * FROM global_temp.FACT_ESTADO_SINIESTRO")
        except Exception as e:
            print(e)
            df_incremental = None
        
        if df_incremental is not None:
            print(f"fechas de corte {fecha_corte_1}-{fecha_corte_2}")
            df_mov_ant = TransformationHelper.create_reserv_mov(df_incremental)
            df_estado = df_fact_estado.filter(F.to_date((F.col("fecha_transaccion")),"YYYY-mm-dd") >= fecha_corte_1)
            df_estado = df_estado.filter(F.to_date((F.col("fecha_transaccion")),"YYYY-mm-dd") <= fecha_corte_2)
            df_estado = df_estado.unionAll(df_mov_ant)
            df_estado = EstadoSiniestro_Helper.create_estado_siniestro(df_estado)
            fecha_compt4 = datetime.strptime(fecha_corte_1, "%Y-%m-%d")
            ano = fecha_compt4.year
            mes = fecha_compt4.month
            print(f"esta es la fecha para compt 4 {ano}-{mes}")
            df_estado = df_estado.filter((F.col("ano")==ano) & (F.col("mes")==mes))
            path = f"s3://{BUCKET_NAME_GOLD}/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-FACT_ESTADO_SINIESTRO/"
            ExtractHelper.save_files(df_estado,path)
            
            print(f"fin estado sin incremental - {datetime.now()}")
            
        if df_incremental is None:    
            
            path_estado = f"s3://{BUCKET_NAME_GOLD}/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-FACT_ESTADO_SINIESTRO/"
            df_aux = GoldTransformation.create_compt_4_aux_table()
            df_fact_estado = df_fact_estado.union(df_aux)
            df_estado = df_fact_estado.filter((F.col("id_fuente_fk").isin("1","0")))
            anio_limite, mes_limite = 2022, 12 ## Modificar a la fecha que se carga es decir si el proceso se ejecuta el 21/03/2026, se debe colocar 2026, 1
            try:
                df_estado = df_estado.checkpoint(eager= True)
                print("cree el checkpoitn df_estado")
            except Exception as e:
                print(e)
            df_estado_use = EstadoSiniestro_Helper.create_estado_siniestro(df_estado, "S")
            df_estado_use = df_estado_use.filter(
                (F.col("ano") < anio_limite) |
                ((F.col("ano") == anio_limite) & (F.col("mes") <= mes_limite))
                )
            ExtractHelper.save_files(df_estado_use,path_estado)
        
        df = PolizaHelper.create_policy()
        df = df.withColumn("COD_PRODUCTO", F.col("COD_PRODUCTO").cast("int"))
        df = df.withColumn("COD_PRODUCTO", F.col("COD_PRODUCTO").cast("string"))
        path = f"s3://{BUCKET_NAME_GOLD}/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-FACT_POLIZA/"
    
        df_poliza = df.select(
            F.col("pk_poliza").alias("id_poliza_pk"),
            F.col('COD_RAMO_TEC').cast(decimal_cast).alias("id_ramo_fk"),
            F.col("cod_compania").alias("compania"),
            F.col("COD_PRODUCTO").cast(decimal_cast).alias("id_producto_fk"),
            F.col("Source").alias("id_fuente_fk"),
            F.col("DESC_UNIDAD_NEGOCIO").alias("unidad_de_negocio"),
            F.col("NRO_ACUERDO").alias("poliza_lider")
        )  

        df_poliza_plus = df_main.select(
            F.col("poliza").cast("string").alias("id_poliza_pk"),
            F.col("id_ramo_fk").cast(decimal_cast),
            F.col("compania"),
            F.col("id_producto_fk").cast(decimal_cast),
            F.col("id_fuente_fk"),
            F.col("unidad_de_negocio"),
            F.col("poliza_lider")
        )  
        if EXECUTE_FACT_POLIZA:

            df_poliza = df_poliza.union(df_poliza_plus)          

        path = f"s3://{BUCKET_NAME_GOLD}/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-FACT_POLIZA-PF/"
        df_incremental = None
        try:
            df_incremental = spark.sql("SELECT * FROM global_temp.POLIZAS")
        except Exception as e:
            print(e)
        if df_incremental is not None:
            df_poliza = df_poliza.join(df_incremental, on="id_poliza_pk", how='left_anti')
    
        windowSpec = Window.partitionBy("id_poliza_pk").orderBy(F.col("id_poliza_pk").desc())
        df_poliza = df_poliza.withColumn("row_num", F.row_number().over(windowSpec))
        df_poliza = df_poliza.filter(F.col("row_num") == 1).drop("row_num")
        ExtractHelper.save_files(df_poliza,path)
        print(f"fin poliza - {datetime.now()}")
    
        df_poliza_detallada_ola0 = Urincterrdeudores_helper.create_urincterrdeudores()        
        df_poliza_detallada_ola1 =df_main.withColumn(
                "fecha_desembolso_credito", 
                F.lit(None).cast("date")  # No aplica
            ).select(
            F.col("fecha_desembolso_credito"),
            F.col("numero_siniestro").cast("string").alias("id_siniestro_fk"),
            F.col("poliza").cast("string").alias("id_poliza_fk"),
            F.col("fecha_inicio_vigencia").cast("date"),
            F.col("id_fuente_fk").cast("string")
        ).distinct()
        window_spec = Window.partitionBy("id_siniestro_fk", "id_poliza_fk") \
                            .orderBy(F.col("fecha_inicio_vigencia").desc())

        df_poliza_detallada_ola1 = df_poliza_detallada_ola1 .withColumn("rn", F.row_number().over(window_spec)) \
                                .filter(F.col("rn") == 1) \
                                .drop("rn")
         # Create window specification for generating sequential IDs
        window_spec = Window.orderBy(F.monotonically_increasing_id())
        df_poliza_detallada_ola1 = df_poliza_detallada_ola1.withColumn(
            "id_poliza_detallada_pk",
            (F.row_number().over(window_spec) + 1000000000).cast("int")
        )
        # Union
        df_final_detallada = df_poliza_detallada_ola0
        if EXECUTE_FACT_POLIZA_DETALLADA:
            df_final_detallada = df_poliza_detallada_ola1.unionByName(df_poliza_detallada_ola0)

        path = f"s3://{BUCKET_NAME_GOLD}/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-FACT_POLIZA_DETALLADA-PF/"
        df_incremental = None
        try:
            df_incremental = spark.sql("SELECT * FROM global_temp.POLIZADETALLADA")
        except Exception as e:
            print(e)
        if df_incremental is not None:
            df_final_detallada = df_final_detallada.join(df_incremental, on="id_poliza_fk", how='left_anti')
        ExtractHelper.save_files(df_final_detallada,path)
        print(f"fin poliza Detallada - {datetime.now()}")
        sql_asegurado = """
        WITH datos_sin_duplicados AS (
            SELECT DISTINCT
                cedula AS CC_NIT_ASEGURADO,
                fecha_nacimiento AS FECHA_NACIMIENTO_ASEGURADO,
                genero_asegurado AS GENERO_ASEGURADO
            FROM global_temp.asegurado
            UNION ALL
            SELECT DISTINCT
                cc_nit_asegurado AS CC_NIT_ASEGURADO,
                fecha_nacimiento_asegurado AS FECHA_NACIMIENTO_ASEGURADO,
                genero_ AS GENERO_ASEGURADO
            FROM global_temp.cierresgenerales
            UNION ALL
            SELECT DISTINCT
                cc_nit_asegurado AS CC_NIT_ASEGURADO,
                fecha_nacimiento_asegurado AS FECHA_NACIMIENTO_ASEGURADO,
                genero_ AS GENERO_ASEGURADO
            FROM global_temp.cierrevida
        )
        SELECT 
            ROW_NUMBER() OVER (ORDER BY CC_NIT_ASEGURADO) + 1000000000 AS id_asegurado_pk,
            CC_NIT_ASEGURADO as cedula,
            MAX(FECHA_NACIMIENTO_ASEGURADO) AS fecha_nacimiento,
            MAX(GENERO_ASEGURADO) as genero
        FROM datos_sin_duplicados
        GROUP BY CC_NIT_ASEGURADO
        ORDER BY id_asegurado_pk
        """
        
        df = spark.sql(sql_asegurado)
        df_aseg_sipren = df_main.select(
            F.col("cedula"),
            F.col("fecha_nacimiento"),
            F.col("genero")
            )
            
        df_aseg_sipren = SiprenDocumentoHelper.create_asegurado(df_aseg_sipren)

        if EXECUTE_FACT_ASEGURADO:
            df = df.unionAll(df_aseg_sipren)
        
        campos_prioritarios = ['genero', 'fecha_nacimiento']  
        df = df.withColumn("completitud", sum(F.when(F.col(c).isNotNull(), 1).otherwise(0) for c in campos_prioritarios))
        window_spec = Window.partitionBy("cedula").orderBy(F.desc("completitud"))
        df_con_rank = df.withColumn("rn", F.row_number().over(window_spec))
        df = df_con_rank.filter(F.col("rn") == 1).drop("completitud", "rn")
        
        df_incremental = None
        try:
            df_incremental = spark.sql("SELECT * FROM global_temp.ASEGURADOGOLD")
        except Exception as e:
            print(e)
        if df_incremental is not None:
            df = df.join(df_incremental, on="cedula", how='left_anti')
            campos_prioritarios = ['genero', 'fecha_nacimiento']  
            df = df.withColumn("completitud", sum(F.when(F.col(c).isNotNull(), 1).otherwise(0) for c in campos_prioritarios))
            window_spec = Window.partitionBy("cedula").orderBy(F.desc("completitud"))
            df_con_rank = df.withColumn("rn", F.row_number().over(window_spec))
            df = df_con_rank.filter(F.col("rn") == 1).drop("completitud", "rn")
        path = f"s3://{BUCKET_NAME_GOLD}/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-FACT_ASEGURADO/"
        ExtractHelper.save_files(df,path)
        print(f"fin asegurado - {datetime.now()}")
        
except Exception as e:
    print(traceback.format_exc())
    raise e