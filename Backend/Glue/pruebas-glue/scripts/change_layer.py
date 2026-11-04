"""Script that load bronce phase"""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from commons.ExtractHelper import ExtractHelper
from commons.IncrementalCharge import IncrementalCharge
from commons.TransformationHelper import TransformationHelper
from commons.GoldTransformation import GoldTransformation
from commons.FechaObjHelper import FechaObjHelper
from commons.ClaimHelper import ClaimHelper
from commons.DocumentoHelper import DocumentoHelper
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import compt3.src.utils.common.df_method as df_mtd
from step_gold.orchestrator_gold import main
from pyspark.sql.window import Window
import datetime
import traceback
import boto3
import time

args = getResolvedOptions(
    sys.argv, 
        [
        'JOB_NAME',
        'BUCKET_NAME_BRONCE',
        'BUCKET_NAME_SILVER',
        'BUCKET_NAME_GOLD',
        'BUCKET_NAME_ASSETS',
        'FUNCTIONALITY',
        'STEP'
        ]
    )
    
BUCKET_NAME_BRONCE = args['BUCKET_NAME_BRONCE']
BUCKET_NAME_SILVER = args['BUCKET_NAME_SILVER']
BUCKET_NAME_GOLD = args['BUCKET_NAME_GOLD']
BUCKET_NAME_ASSETS = args['BUCKET_NAME_ASSETS']
FUNCTIONALITY = args['FUNCTIONALITY']
STEP = args['STEP']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead","LEGACY")
spark.conf.set("spark.sql.files.ignoreCorruptFiles","true")
spark.conf.set("spark.hadoop.fs.s3a.experimental.input.fadvise","sequential")

s3_client = boto3.client("s3")
TransformationHelper = TransformationHelper()
ExtractHelper = ExtractHelper(glueContext, spark, "", "")
IncrementalCharge = IncrementalCharge(glueContext, spark, "", "")
GoldTransformation = GoldTransformation(glueContext, spark, 'None')
FechaObj_helper = FechaObjHelper(glueContext, spark)
ClaimHelper = ClaimHelper(glueContext, spark)
DocumentoHelper = DocumentoHelper(glueContext, spark)

path = f"s3://{BUCKET_NAME_ASSETS}/support_phase_files/Registros_cambiados.xlsx"
sheet = "Hoja1"
df_data = ExtractHelper.extract_excel_data(path, sheet)


path = f"s3://{BUCKET_NAME_ASSETS}/support_phase_files/Normalizacion_Datos.xlsx"
sheet = "Capitalizacion_datos"
df_capitalizacion = ExtractHelper.extract_excel_data(path, sheet)
sheet = "Casteo_datos"
df_casteodata = ExtractHelper.extract_excel_data(path, sheet)

path = f"s3://{BUCKET_NAME_ASSETS}/support_phase_files/procces_ola0.xlsx"
sheet = "gold_phase"
df_tables = ExtractHelper.extract_excel_data(path, sheet)
sheet = "gold_tables"
df_tmp_tables = ExtractHelper.extract_excel_data(path, sheet)

date = current_date = datetime.datetime.now()

bk_date = f"{date.year}-{date.month}-{date.day}"

if FUNCTIONALITY == 'ORACLE':
    try:
        proccess_table = IncrementalCharge.get_proccess_table()
        for table, pk in proccess_table.items():
            ## v INICIO PROCESO BRONCE v ##
            if STEP == "UNO":
                path_source = f"s3://{BUCKET_NAME_BRONCE}/ACSELE/ACSELE_ALFA/ACSELE-ACSELE_ALFA-"#revisar path
                table_in_proccess = f"{table}_MODIFY"
                df_data_modify = IncrementalCharge.charge_tmp_tables_with_partition(path_source, table_in_proccess, 'Gold')
                df_validation_modify = None
                df_validation_modify = df_data_modify.filter(F.col("partition_date") == bk_date)
                if df_validation_modify is not None:
                    
                    df_data_modify = df_data_modify.filter(F.col("RN") == 1)
                    df_data_modify = df_data_modify.filter(F.col("partition_date") == bk_date)
                    df_data_modify.cache()
                    path_source = f"s3://{BUCKET_NAME_BRONCE}/ACSELE/ACSELE_ALFA/ACSELE-ACSELE_ALFA-"
                    df = IncrementalCharge.charge_tmp_tables_with_partition(path_source, table, 'Gold')
                    df.cache()
                    bk_path_source = f"s3://{BUCKET_NAME_BRONCE}/BACKUP/{bk_date}/ACSELE_ALFA/ACSELE-ACSELE_ALFA-"
                    date_cambiados_bk = df.select("partition_date").distinct().collect()##guard las particiones que tienen cambios
                    for i in date_cambiados_bk:
                        df_bk = df.filter(F.col("partition_date")==i["partition_date"])
                        df_bk = df_bk.drop("partition_date")
                        IncrementalCharge.save_files_modify(df_bk, bk_path_source, f"{table}_BK", i["partition_date"])
                    #df-- data actual
                    #df_data_modify -- datos que cambian
                    original_columns = df.columns
                    change_columns = df_data_modify.columns
                    df_joined = df.join(df_data_modify, pk, "LEFT")
                    df_update = df_joined.select(
                        F.coalesce(df_data_modify[pk], df[pk]).alias(pk),
                        *[
                            F.coalesce(df_data_modify[col], df[col]).alias(col) if col != "partition_date" else df[col]
                            for col in original_columns if col in change_columns
                         ]
                        )
                    date_cambiados = df_update.select("partition_date").distinct().collect()##guard las particiones que tienen cambios
                    df_update.cache()
                    for i in date_cambiados:
                        IncrementalCharge.delete_data(path_source, table, i["partition_date"], BUCKET_NAME_BRONCE)
                    for i in date_cambiados:
                        df_updated = df_update.filter(F.col("partition_date")==i["partition_date"]).drop("partition_date")
                        IncrementalCharge.save_files_modify(df_updated, path_source, f"{table}", i["partition_date"])
                else:
                    print("No hay datos para modificar")
            if STEP == "DOS":
                
                campos = IncrementalCharge.get_fields(table)
                
                path_source = f"s3://{BUCKET_NAME_BRONCE}/ACSELE/ACSELE_ALFA/ACSELE-ACSELE_ALFA-"#revisar path
                table_in_proccess = f"{table}_MODIFY"
                df_data_modify = IncrementalCharge.charge_tmp_tables_with_partition(path_source, table_in_proccess, 'Gold')
                df_data_modify.cache()
                
                df_validation_modify = None
                df_validation_modify = df_data_modify.filter(F.col("partition_date") == bk_date)
                if df_validation_modify is not None:
                    
                    df_data_modify = df_data_modify.filter(F.col("RN") == 1).select(pk).distinct()
                    df_data_modify = df_data_modify.filter(F.col("partition_date") == bk_date)
                    df_data_modify.cache()
                    
                    path_source = f"s3://{BUCKET_NAME_BRONCE}/ACSELE/ACSELE_ALFA/ACSELE-ACSELE_ALFA-"
                    df_data_bronce = IncrementalCharge.charge_tmp_tables_with_partition(path_source, table, 'Gold')
                    df_data_bronce.cache()
                    
                    campos = campos.split(",")
                    df_change = df_data_bronce.join(
                        df_data_modify,
                        pk,
                        "INNER"
                        ).select(df_data_bronce["*"])## traigo la informacion que se cambio en bronce
                    df_change = df_change.select(*campos)
                    #aplico transformaciones silver
                    for capitalizacion in df_capitalizacion.collect():
                        if capitalizacion["TABLA"].upper() == table.upper():
                            df_change = TransformationHelper.data_normalice(capitalizacion["CAMPO"],df)
                    df_change = TransformationHelper.date_normalice(df_change)
                    for capitalizacion in df_capitalizacion.collect():
                        if capitalizacion["TABLA"].upper() == table.upper():
                            df_change = TransformationHelper.data_normalice(capitalizacion["CAMPO"],df_change)
                    for rowcast in df_casteodata.collect():
                        if table.upper() == rowcast['TABLA'].upper():
                            df_change = TransformationHelper.data_cast(rowcast['CAMPO'].upper(),rowcast['NORMALIZACION'], df_change)
                    df_change = TransformationHelper.data_normalice_string(df_change, None)
                    df_change = df_change.withColumn("partition_date", F.lit(None))
                    
                    path_source_silver = f"s3://{BUCKET_NAME_SILVER}/ACSELE/TRANSFORMATION/ACSELE_SILVER_"
                    df_data_silver = IncrementalCharge.charge_tmp_tables_with_partition(path_source_silver, table, 'Gold')
                    df_data_silver.cache()
                    
                    original_columns = df_data_silver.columns
                    change_columns = df_change.columns
                    df_joined = df_data_silver.join(df_change, pk, "LEFT")
                    df_update = df_joined.select(
                        F.coalesce(df_change[pk], df_data_silver[pk]).alias(pk),
                        *[
                            F.coalesce(df_change[col], df_data_silver[col]).alias(col) if col != "partition_date" else df_data_silver[col]
                            for col in original_columns if col in change_columns
                         ]
                        )
                    
                    df_update.cache()
                    
                    bk_path_source = f"s3://{BUCKET_NAME_SILVER}/BACKUP/{bk_date}/TRANSFORMATION/ACSELE_SILVER_"
                    date_cambiados_bk = df_data_bronce.select("partition_date").distinct().collect()##guard las particiones que tienen cambios
                    for i in date_cambiados_bk:
                        df_bk = df_data_silver.filter(F.col("partition_date")==i["partition_date"])
                        df_bk = df_bk.drop("partition_date")
                        IncrementalCharge.save_files_modify(df_bk, bk_path_source, f"{table}_BK", i["partition_date"])
                    
                    date_cambiados = df_update.select("partition_date").distinct().collect()##guard las particiones que tienen cambios
                    for i in date_cambiados:
                        IncrementalCharge.delete_data(path_source_silver, table, i["partition_date"], BUCKET_NAME_SILVER)
                    for i in date_cambiados:
                        df_updated = df_update.filter(F.col("partition_date")==i["partition_date"]).drop("partition_date")
                        IncrementalCharge.save_files_modify(df_updated, path_source_silver, f"{table}", i["partition_date"])
                    
        if STEP == "TRES":

            tables = ['CLAIM', 'PAYMENTORDER']
            val_data = 0
            df_result_comb = None
            for table in tables:
                path_source = f"s3://{BUCKET_NAME_BRONCE}/ACSELE/ACSELE_ALFA/ACSELE-ACSELE_ALFA-"#revisar path
                table_in_proccess = f"{table}_MODIFY"
                df_data_modify = IncrementalCharge.charge_tmp_tables_with_partition(path_source, table_in_proccess, 'Gold')
                df_validation_modify = None
                df_validation_modify = df_data_modify.filter(F.col("partition_date") == bk_date)
                if df_validation_modify is not None:
                    
                    df_data_modify = df_data_modify.filter(F.col("partition_date") == bk_date)
                    df_data_modify = df_data_modify.filter(F.col("RN") == 1).select(pk).distinct()
                    
                    val_data = val_data + 1
                    
                    if table == 'CLAIM':
                        data_comb = df_data_modify.select('CLAIMNUMBER')
                        if df_result_comb is None:
                            df_result_comb = data_comb
                        if df_result_comb is not None:
                            df_result_comb = df_result_comb.union(data_comb)
                    if table == 'PAYMENTORDER':
                        path_source = f"s3://{BUCKET_NAME_BRONCE}/ACSELE/ACSELE_ALFA/ACSELE-ACSELE_ALFA-"#revisar path
                        table_in_proccess = "CLAIM"
                        df_claim = IncrementalCharge.charge_tmp_tables_with_partition(path_source, table_in_proccess, 'Gold')
                        path_source = f"s3://{BUCKET_NAME_BRONCE}/ACSELE/ACSELE_ALFA/ACSELE-ACSELE_ALFA-"#revisar path
                        table_in_proccess = "RI_CLAIMOPERATIONS"
                        df_ri = IncrementalCharge.charge_tmp_tables_with_partition(path_source, table_in_proccess, 'Gold')
                        data_comb = df_data_modify.join(
                                df_ri,
                                df_data_modify["PAYMENTORDERID"] == df_ri["POR_ID"],
                                "INNER"
                            ).join(
                                df_claim,
                                df_ri["CLAIMID"] == df_claim["CLAIMID"],
                                "INNER"
                            ).select(df_claim["CLAIMNUMBER"]).distinct()
                        if df_result_comb is None:
                            df_result_comb = data_comb
                        if df_result_comb is not None:
                            df_result_comb = df_result_comb.union(data_comb)
            if val_data > 0:
                data_comb = df_result_comb
                table = 'MOVIMIENTO'
                path_source_gold = f"s3://{BUCKET_NAME_GOLD}/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-FACT_"
                df_data_gold = IncrementalCharge.charge_tmp_tables_with_partition(path_source_gold, table, 'Gold')
                df_data_gold.cache()
                bk_path_source = f"s3://{BUCKET_NAME_GOLD}/BACKUP/{bk_date}/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-FACT_"
                date_cambiados_bk = df_data_gold.select("partition_date").distinct().collect()##guard las particiones que tienen cambios
                for i in date_cambiados_bk:
                    df_bk = df_data_gold.filter(F.col("partition_date")==i["partition_date"]).drop("partition_date")
                    IncrementalCharge.save_files_modify(df_bk, bk_path_source, f"{table}_BK", i["partition_date"])
                fecha_corte = GoldTransformation.obtain_last_date()
                df_data_gold_save = df_data_gold.filter(F.to_date((F.col("fecha_transaccion")),"YYYY-mm-dd") <= fecha_corte)
                df_data_gold = df_data_gold.filter(F.to_date((F.col("fecha_transaccion")),"YYYY-mm-dd") > fecha_corte)
                df_data_gold = df_data_gold.join(
                    data_comb,
                    df_data_gold.id_siniestro_fk == data_comb.CLAIMNUMBER,
                    "left_anti"
                    ).select(df_data_gold["*"])
                df_data_gold_union = df_data_gold
                df_data_gold_union = df_data_gold_union.union(df_data_gold_save)
                data_gold_change = df_data_gold_union.select("partition_date").distinct().collect()
                for i in data_gold_change:
                    df_data_gold_save = df_data_gold_union.filter(F.col("partition_date")==i["partition_date"]).drop("partition_date")
                    IncrementalCharge.save_files_modify(df_data_gold_save, path_source_gold, f"{table}_COPY", i["partition_date"])
                    IncrementalCharge.delete_data(path_source_gold, table, i["partition_date"], BUCKET_NAME_GOLD)
                    IncrementalCharge.charge_tmp_tables_with_partition(path_source_gold, f"{table}_COPY", 'Gold')
                    IncrementalCharge.save_files_modify(df_data_gold_save, path_source_gold, f"{table}", i["partition_date"])
                    IncrementalCharge.delete_data(path_source_gold, f"{table}_COPY", i["partition_date"], BUCKET_NAME_GOLD)
                
                    
                for row in df_tmp_tables.collect():
                    table = row["Table"].upper()
                    if row["Phase"].upper() == "SILVER":
                        path_source =f"s3://{BUCKET_NAME_SILVER}/{row['DB'].upper()}/{row['SCHEMA'].upper()}/{row['DB'].upper()}_SILVER_"
                    else:
                        path_source =f"s3://{BUCKET_NAME_GOLD}/{row['DB'].upper()}/{row['SCHEMA'].upper()}/{row['DB'].upper()}-CARACT_SINIESTROS-"

                    IncrementalCharge.charge_tmp_tables(path_source, table, 'Gold')
                df_ubicacion = GoldTransformation.chargue_ubicacion_acsele()
    
                df = main(
                    args=args,
                    spark_session=spark,
                    glue_context=glueContext
                )
                
                df_ubicacion.alias("UB")
                window_spec = Window.partitionBy("ID_SINIESTRO_CLAIM").orderBy("ID_SINIESTRO_CLAIM")
                df_ubicacion = df_ubicacion.withColumn("row_number", F.row_number().over(window_spec))
                df_ubicacion = df_ubicacion.filter(F.col("row_number")== 1).drop("row_number")
                
                df_date = FechaObj_helper.create_fecha_obj()
            
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
    
                df = GoldTransformation.clean_first_df(df, df2)
        
                df = df_mtd.step_clean_empty_values(
                    df_data=df,
                    column_id="departamentovalue",
                    column_id_value="01",
                    column_name="ciudadvalue",
                    column_name_value="01001",
                )
                
                df = GoldTransformation.clean_second_df(df)
        
                decimal_cast = "decimal(19, 0)"
                df_sini =  df.select(
                        F.col("nro_siniestro_claim").alias("id_siniestro_pk"),
                        F.col("CAUSA_SINIESTRO").alias("id_causa_siniestro_fk"),
                        F.col("COD_COBERTURA").cast(decimal_cast).alias("id_cobertura_fk"),
                        F.col('source').alias("id_fuente_fk"),
                        F.col("FE_OCURRENCIA").cast("date").alias("fecha_ocurrencia"),
                        F.col("FE_AVISO").cast("date").alias("fecha_aviso"),
                        F.col("PK_SINIESTRO").alias("id_poliza_fk"),
                        F.col("DEPARTAMENTOVALUE").alias("id_departamento_ocurrencia_fk"),
                        F.col("CIUDADVALUE").alias("id_ciudad_ocurrencia_fk")
                    ).distinct() 
                
                windowSpec = Window.partitionBy("id_siniestro_pk").orderBy(F.col("fecha_aviso").desc(),F.col("fecha_ocurrencia").desc())
                df_max_reclamo = df_sini.withColumn("row_num", F.row_number().over(windowSpec))
                df_max_reclamo = df_max_reclamo.filter(F.col("row_num") == 1).drop("row_num")
                df_doc= DocumentoHelper.create_cedula() 
                df_cel = df_max_reclamo.join(df_doc, F.col("id_siniestro_pk") == F.col("numero_siniestro") ,how = "left").drop("numero_siniestro")
                df_siniestros = df_cel.withColumn(
                    "cedula",
                    F.when(
                        F.col("cedula").isNull() | (F.col("cedula") == ""),
                        "No registra"
                    ).otherwise(F.col("cedula"))
                )
                
                df_incremental = None
                try:
                    df_incremental = spark.sql("SELECT * FROM global_temp.SINIESTROS")
                except Exception as e:
                    print(e)
                if df_incremental is not None:
                    df_siniestros = df_siniestros.join(df_incremental, on="id_siniestro_pk", how='left_anti')
            
                ExtractHelper.save_files(df_siniestros, f"s3://{BUCKET_NAME_GOLD}/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-FACT_SINIESTROS-PF/")
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
                df_movimientos = df_movimientos.join(
                    data_comb,
                    df_movimientos.id_siniestro_fk == data_comb.CLAIMNUMBER,
                    "INNER"
                    ).drop("CLAIMNUMBER")

                fecha_corte = GoldTransformation.obtain_last_date()
                df_movimientos = df_movimientos.filter(F.to_date((F.col("fecha_transaccion")),"YYYY-mm-dd") > fecha_corte)
                path = f"s3://{BUCKET_NAME_GOLD}/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-FACT_MOVIMIENTO/"
                ExtractHelper.save_files(df_movimientos,path)
            
        #FIN TABLA    
    except Exception as e:
        print(traceback.format_exc())
        raise e
else:
    try:
        data = []
        columns = ["TABLA", "PK_FIELD", "PK_KEY", "MODIFY_FIELD","VALUE","OLD_VALUE","DATE"]
        date = current_date = datetime.datetime.now()
        for row in df_data.collect():
            path_source = f"s3://{BUCKET_NAME_BRONCE}/ACSELE/ACSELE_ALFA/ACSELE-ACSELE_ALFA-"
            table = row["Tabla"]
            IncrementalCharge.charge_tmp_tables_with_partition(path_source, table, 'Gold')
            df = None
            df = spark.sql(f"SELECT * FROM global_temp.{table}")
            if df != None:
                partition = df.filter(F.col(row["Id_field"]) == row["Id"]).select("partition_date").first()[0]
                df_to_modify = df.filter(F.col("partition_date") == partition)
                df_search = df_to_modify.filter(F.col(row["Id_field"]) == row["Id"])
                old_value = df_search.filter(F.col(row["Id_field"]) == row["Id"]).select(row["modify_field"].upper()).first()[0]
                df_search = df_search.withColumn(
                    row["modify_field"].upper(), F.lit(row["value"])
                )
                df_final = df_to_modify.filter(~(F.col(row["Id_field"]) == row["Id"]))
                df_final = df_final.union(df_search)
                df_final = df_final.drop("partition_date")
                IncrementalCharge.delete_data(path_source, table, partition)
                IncrementalCharge.save_files_modify(df_final, path_source, table, partition)
                historical_row = (row["Tabla"], row["Id_field"], row["Id"], row["modify_field"], row["value"], str(old_value), f"{date.year}-{date.month}-{date.day}")
                data.append(historical_row)
                df = spark.createDataFrame(data, columns)

            else:
                print("No hay datos par amodificar")
        path_source = f"s3://{BUCKET_NAME_BRONCE}/MODIFY/ACSELE_ALFA/ACSELE-ACSELE_ALFA-"
        table = 'HISTORICAL_MODIFY'
        IncrementalCharge.save_files_modify(df, path_source, table, partition)
    except Exception as e:
        print(traceback.format_exc())
        raise e