"""Script that load bronce phase"""

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from awsgluedq.transforms import EvaluateDataQuality
from commons.LogsHelper import LogsHelper
from pyspark.sql import functions as F
from pyspark.sql.functions import col, coalesce, current_timestamp, current_date
from pyspark.sql.window import Window

import sys, traceback, boto3, json

logs = LogsHelper()
logger = logs.logger_conf()

args = getResolvedOptions(
    sys.argv, 
        [
        'JOB_NAME',
        'DB_NAME_CATALOG',        
        'DB_NAME_ALFA_DATALAKE', 
        'BUCKET_NAME_GOLD',
        'DB_NAME_CAL_GOLD'
        ]
    )
DB_NAME_CATALOG = args['DB_NAME_CATALOG']
DB_NAME_ALFA_DATALAKE = args['DB_NAME_ALFA_DATALAKE']
BUCKET_NAME_GOLD = args['BUCKET_NAME_GOLD']
DB_NAME_CAL_GOLD = args['DB_NAME_CAL_GOLD']

# Use getOrCreate SparkContext on AWS Glue
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session#
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
s3_client = boto3.client("s3")

spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")



def glue_table_exists_safe(database, table_name):
    try:
        spark.table(f"{database}.{table_name}")
        return True
    except Exception:
        return False

###################################################################
################# consultar catalogo  #############################
###################################################################

##tipo activo
df_activo = glueContext.create_dynamic_frame.from_catalog(
                        database=f"{DB_NAME_CATALOG}",  
                        table_name="tbl_activo"
                    ).toDF()
##tipo dato
df_tipodat = glueContext.create_dynamic_frame.from_catalog(
                        database=f"{DB_NAME_CATALOG}",  
                        table_name="tbl_tipo_dato"
                    ).toDF()                    
#atributo
df_atributo = glueContext.create_dynamic_frame.from_catalog(
                        database=f"{DB_NAME_CATALOG}",  
                        table_name="tbl_atributo"
                    ).toDF()
#dominio
df_dominio = glueContext.create_dynamic_frame.from_catalog(
                        database=f"{DB_NAME_CATALOG}",  
                        table_name="tbl_dominio"
                    ).toDF()  
#tbl_regla
df_regla = glueContext.create_dynamic_frame.from_catalog(
                        database=f"{DB_NAME_CATALOG}",  
                        table_name="tbl_regla"
                    ).toDF()  
#tbl_regla_atributo
df_regla_atributo = glueContext.create_dynamic_frame.from_catalog(
                        database=f"{DB_NAME_CATALOG}",  
                        table_name="tbl_regla_atributo"
                    ).toDF()
#tbl_lista_referencia
df_lista_referencia = glueContext.create_dynamic_frame.from_catalog(
                        database=f"{DB_NAME_CATALOG}",  
                        table_name="tbl_lista_referencia"
                    ).toDF()                       
#tbl_capa
df_capa = glueContext.create_dynamic_frame.from_catalog(
                        database=f"{DB_NAME_CATALOG}",  
                        table_name="tbl_capa"
                    ).toDF()  
#df_tbl_umbral
df_umbral = glueContext.create_dynamic_frame.from_catalog(
                        database=f"{DB_NAME_CATALOG}",  
                        table_name="tbl_umbral"
                    ).toDF()     
                    
##dim calendario
df_calendario = glueContext.create_dynamic_frame.from_catalog(
                        database=f"{DB_NAME_ALFA_DATALAKE}",  
                        table_name="gold_caract_clientes_dim_dim_calendario_gold"
                    ).toDF()                    

try:      
    
                        ###################################################################
                        ########################## PROCESO DIM_ACTIVO    ##################
                        ###################################################################
                        
    
    
    df_tbl_activo=(
    df_activo.select(
        F.col("id_activo"),            		
        F.col("nombre_fisico"),        		
        F.col("fuente_origen"),        		
        F.col("retencion_meses"),    		
        F.col("id_estado"),            		
        F.col("id_dominio"),         		
        F.col("nombre_activo_funcional"),   
        F.col("id_tipo"),        
        F.col("tipo_tabla") #provisional para dev
    ))     #tbl 1 datos bronce  
    
    df_tbl_activo.show(20, truncate=False)
    logger.info("Mostradas primeras 20 filas de df_tbl_activo")

    
    dim_exists = glue_table_exists_safe(  
        f"{DB_NAME_CAL_GOLD}",
        "dim_activo"
    )
   
   
    if not dim_exists:
        logger.info("DIM_ACTIVO no existe → carga inicial")
    
        df_merge = (
            df_tbl_activo
                .select(
                    "id_activo",
                    "nombre_fisico",        	
                    "fuente_origen" ,       	
                    "retencion_meses",      	
                    "id_estado",            	
                    "id_dominio",         	
                    "nombre_activo_funcional",
                    "id_tipo",
                    "tipo_tabla", #provisional para dev
                    F.current_date().alias("load_dt")
                )
        )
    else:
        logger.info("DIM_ACTIVO existe → merge catálogo vs dimensión")
        df_dim_activo = glueContext.create_dynamic_frame.from_catalog(
            database=f"{DB_NAME_CAL_GOLD}",
            table_name="dim_activo"
        ).toDF()
    
        df_merge = (
        df_dim_activo.alias("dimact")
        .join(df_tbl_activo.alias("tblact"),
              col("dimact.id") == col("tblact.id_activo"),
              "full_outer")
        .select(
            coalesce(col("tblact.id_activo"),     col("dimact.id")).alias("id"),
            coalesce(col("tblact.nombre_fisico"), col("dimact.nombre_fisico")).alias("nombre_fisico"),
            coalesce(col("tblact.fuente_origen"),   col("dimact.fuente_origen")).alias("fuente_origen"),
            coalesce(col("tblact.retencion_meses"), col("dimact.retencion_meses")).alias("retencion_meses"),
            coalesce(col("tblact.id_estado"),     col("dimact.id_estado")).alias("id_estado"),
            coalesce(col("tblact.id_dominio"), col("dimact.id_dominio")).alias("id_dominio"),                    #id dominio ponerla en catalogo de categoria a dominio
            coalesce(col("tblact.nombre_activo_funcional"), col("dimact.nombre_fisica")).alias("nombre_activo_funcional"),      #cambiar a nombre_activo_funcional en el catalogo y en la dim nom act fun
            coalesce(col("tblact.id_tipo"), col("dimact.tipo de tabla")).alias("tipo_de_tabla"),                   #tipo de tabla se debe poner en cat bronce y la dimension
            coalesce(col("tblact.tipo_de_tabla"), col("dimact.tipo de tabla")).alias("tipo_de_tabla"),                   #
            
            F.to_date(F.current_timestamp()).alias("load_dt")
        )
    )
    
    df_merge.printSchema()
    df_merge.show(20, truncate=False)
    
    output_path = f"s3://{BUCKET_NAME_GOLD}/GOLD/CALIDAD_CARGA_GOLD/dim_activo/"
        
    df_merge.write \
            .mode("overwrite") \
            .parquet(output_path)
        
    logger.info(f"DIM_ACTIVO cargada correctamente en {output_path}")
    

                        ###################################################################
                        ########################## PROCESO DIM_ATRIBUTO  ##################
                        ###################################################################
                        
    
    
    df_tbl_atributo=(
    df_atributo.select(
        F.col("id_atributo"),            		
        F.col("id_activo"),
        F.col("nombre_campo"),
        F.col("id_tipo_dato"),
        F.col("precision_dato"),
        F.col("escala_dato"),
        F.col("valor_ejemplo"),
        F.col("es_pk"),
        F.col("es_nulo")          		
    ))     #tbl 1 datos bronce  
    
    df_tbl_atributo.show(20, truncate=False)
    logger.info("Mostradas primeras 20 filas de df_tbl_atributo")

    
    dim_exists = glue_table_exists_safe(  
        f"{DB_NAME_CAL_GOLD}",
        "dim_atributo"
    )
   
   
    if not dim_exists:
        logger.info("DIM_ATRIBUTO no existe → carga inicial")
    
        df_merge = (
            df_tbl_atributo
                .select(
                    "id_atributo",            		
                    "id_activo",
                    "nombre_campo",
                    "id_tipo_dato",
                    "precision_dato",
                    "escala_dato",
                    "valor_ejemplo",
                    "es_pk",
                    "es_nulo",
                    F.current_date().alias("load_dt")
                )
        )
    else:
        logger.info("DIM_ATRIBUTO existe → merge catálogo vs dimensión")
        df_dim_atributo = glueContext.create_dynamic_frame.from_catalog(
            database=f"{DB_NAME_CAL_GOLD}",
            table_name="dim_atributo"
        ).toDF()
    
        df_merge = (
        df_dim_atributo.alias("dimatrib")
        .join(df_tbl_atributo.alias("tblatrib"),
              col("dimatrib.id") == col("tblatrib.id_atributo"),
              "full_outer")
        .select(
            coalesce(col("tblatrib.id_atributo"),col("dimatrib.id")).alias("id"),
            coalesce(col("tblatrib.id_activo"),col("dimatrib.id_activo")).alias("id_activo"),
            coalesce(col("tblatrib.nombre_campo"),col("dimatrib.nombre_campo")).alias("nombre_campo"),
            coalesce(col("tblatrib.id_tipo_dato"),col("dimatrib.id_tipo_dato")).alias("id_tipo_dato"),
            coalesce(col("tblatrib.precision_dato"),col("dimatrib.precision_dato")).alias("precision_dato"),
            coalesce(col("tblatrib.escala_dato"),col("dimatrib.escala_dato")).alias("escala_dato"),
            coalesce(col("tblatrib.valor_ejemplo"),col("dimatrib.valor_ejemplo")).alias("valor_ejemplo"),
            coalesce(col("tblatrib.es_pk"),col("dimatrib.es_pk")).alias("es_pk"),
            coalesce(col("tblatrib.es_nulo"),col("dimatrib.es_nulo")).alias("es_nulo"),
            F.to_date(F.current_timestamp()).alias("load_dt")
        )
    )
    
    df_merge.printSchema()
    df_merge.show(20, truncate=False)
    
    output_path = f"s3://{BUCKET_NAME_GOLD}/GOLD/CALIDAD_CARGA_GOLD/dim_atributo/"
        
    df_merge.write \
            .mode("overwrite") \
            .parquet(output_path)
        
    logger.info(f"DIM_ATRIBUTO cargada correctamente en {output_path}") 
    
                        ###################################################################
                        ########################## PROCESO DIM_TIPODAT   ##################
                        ###################################################################
                        
    
    
    df_tbl_tipodat=(
    df_tipodat.select(
        F.col("id_tipo_dato"),            		
        F.col("formato_default")         		
    ))     #tbl 1 datos bronce   
    
    df_tbl_tipodat.show(20, truncate=False)
    logger.info("Mostradas primeras 20 filas de df_tbl_tipodat")

    
    dim_exists = glue_table_exists_safe(  
        f"{DB_NAME_CAL_GOLD}",
        "dim_tipodat"
    )
   
   
    if not dim_exists:
        logger.info("DIM_TIPODAT no existe → carga inicial")
    
        df_merge = (
            df_tbl_tipodat
                .select(
                    "id_tipo_dato",           		
                    F.col("formato_default").alias("desc_tipo_dato"),
                    F.current_date().alias("load_dt")
                )
        )
    else:
        logger.info("DIM_TIPODAT existe → merge catálogo vs dimensión")
        df_dim_atributo = glueContext.create_dynamic_frame.from_catalog(
            database=f"{DB_NAME_CAL_GOLD}",
            table_name="dim_tipodat"
        ).toDF()
    
        df_merge = (
        df_dim_tipodat.alias("dimtipodat")
        .join(df_tbl_tipodat.alias("tbltipodat"),
              col("dimtipodat.id_tipo de dato") == col("tbltipodat.id_tipo_dato"),
              "full_outer")
        .select(
            coalesce(col("tbltipodat.id_tipo_dato"),col("dimtipodat.id_tipo de dato")).alias("id_tipo_dato"),
            coalesce(col("tbltipodat.formato_default"),col("dimtipodat.desc_tipo_dato")).alias("desc_tipo_dato"),
            F.to_date(F.current_timestamp()).alias("load_dt")
        )
    )
    
    df_merge.printSchema()
    df_merge.show(20, truncate=False)
    
    output_path = f"s3://{BUCKET_NAME_GOLD}/GOLD/CALIDAD_CARGA_GOLD/dim_tipodat/"
        
    df_merge.write \
            .mode("overwrite") \
            .parquet(output_path)
        
    logger.info(f"DIM_TIPODAT cargada correctamente en {output_path}") 
                       
                        ###################################################################
                        ########################## PROCESO DIM_DOMINIO   ##################
                        ###################################################################
                        
    df_tbl_dominio = (
        df_dominio.select(
            F.col("id").alias("id"),
            F.col("descripcion"),
            F.col("id_capa"),
            F.col("id_umbral"),
            F.col("id_categoria")
        )
    )
    
    df_tbl_dominio.show(20, truncate=False)
    logger.info("Mostradas primeras 20 filas de df_tbl_dominio")

    
    dim_exists = glue_table_exists_safe(  
        f"{DB_NAME_CAL_GOLD}",
        "dim_dominio"
    )
    if not dim_exists:
        logger.info("DIM_DOMINIO no existe → carga inicial")
    
        df_merge = (
            df_tbl_dominio
                .select(
                    "id",
                    "descripcion",
                    "id_capa",
                    "id_umbral",
                    "id_categoria",
                    F.current_date().alias("load_dt")  
                )
        )
    else:
        logger.info("DIM_DOMINIO existe → merge catálogo vs dimensión")
    
        df_dim_dominio = glueContext.create_dynamic_frame.from_catalog(
            database=f"{DB_NAME_CAL_GOLD}",
            table_name="dim_dominio"
        ).toDF()
    
        df_merge = (
            df_dim_dominio.alias("dimdom")
            .join(
                df_tbl_dominio.alias("tbldom"),
                col("dimdom.id") == col("tbldom.id"),
                "full_outer"
           
            .select(
                F.coalesce(F.col("tbldom.id"), F.col("dimdom.id")).alias("id"),
                F.coalesce(F.col("tbldom.descripcion"), F.col("dimdom.descripcion")).alias("descripcion"),
                F.coalesce(F.col("tbldom.id_capa"), F.col("dimdom.id_capa")).alias("id_capa"),
                F.coalesce(F.col("tbldom.id_umbral"), F.col("dimdom.id_umbral")).alias("id_umbral"),
                F.current_date().alias("load_dt")
            )

        )
        )

    df_merge.printSchema()
    df_merge.show(20, truncate=False)
    
    
    output_path = f"s3://{BUCKET_NAME_GOLD}/GOLD/CALIDAD_CARGA_GOLD/dim_dominio/"
    
    df_merge.write \
        .mode("overwrite") \
        .parquet(output_path)
    
    logger.info(f"DIM_DOMINIO cargada correctamente en {output_path}")
    
                        ###################################################################
                        ########################## PROCESO DIM_CAPA   #####################
                        ###################################################################
                        
    COL_C_DIM_CAPA = "dimcap.id"   
    COL_C_TBL_CAPA = "tblcap.id_capa" 
                        
    df_tbl_capa=(
    df_capa.select(
        F.col("id_capa"),            		
        F.col("descripcion")
    ))     #tbl 1 datos bronce 
    
    df_tbl_capa.show(20, truncate=False)
    logger.info("Mostradas primeras 20 filas de df_tbl_capa")

    
    dim_exists = glue_table_exists_safe(  
        f"{DB_NAME_CAL_GOLD}",
        "dim_capa"
    )
    if not dim_exists:
        logger.info("DIM_CAPA no existe → carga inicial")
    
        df_merge = (
            df_tbl_capa
                .select(
                    "id_capa",
                    "descripcion",
                    F.current_date().alias("load_dt")  
                )
        )
    else:
        logger.info("DIM_CAPA existe → merge catálogo vs dimensión")
    
        df_dim_dominio = glueContext.create_dynamic_frame.from_catalog(
            database=f"{DB_NAME_CAL_GOLD}",
            table_name="dim_capa"
        ).toDF()
    
        df_merge = (
        df_dim_capa.alias("dimcap")
        .join(df_tbl_capa.alias("tblcap"),
              col(COL_C_DIM_CAPA) == col(COL_C_TBL_CAPA),
              "full_outer")
        .select(
            coalesce(col(COL_C_TBL_CAPA),col(COL_C_DIM_CAPA)).alias("id"),
            coalesce(col("tblcap.descripcion"),col("dimcap.descripcion")).alias("descripcion"),
            F.to_date(F.current_timestamp()).alias("load_dt")
        )
    )
    df_merge.printSchema()
    df_merge.show(20, truncate=False)
    
    
    output_path = f"s3://{BUCKET_NAME_GOLD}/GOLD/CALIDAD_CARGA_GOLD/dim_capa/"
    
    df_merge.write \
        .mode("overwrite") \
        .parquet(output_path)
    
    logger.info(f"DIM_CAPA cargada correctamente en {output_path}")
    
 
                        ###################################################################
                        ########################## PROCESO DIM_REGLA  #####################
                        ###################################################################
                        
    df_tbl_regla=(
    df_regla.select(
        F.col("id"),            		
        F.col("codigo_regla"),
        F.col("dimension"),
        F.col("tipo_regla"),
        F.col("nombre_tecnico"),
        F.col("descripcion_estandar"),
        F.col("ejemplos"),
        F.col("dqdl")  
    ))     #tbl 1 datos bronce      
    
    df_tbl_regla.show(20, truncate=False)
    logger.info("Mostradas primeras 20 filas de df_tbl_regla")

    
    dim_exists = glue_table_exists_safe(  
        f"{DB_NAME_CAL_GOLD}",
        "dim_regla"
    )
    if not dim_exists:
        logger.info("DIM_REGLA no existe → carga inicial")
    
        df_merge = (
            df_tbl_regla
                .select(
                   "id",            		
                   "codigo_regla",
                   "dimension",
                   "tipo_regla",
                   "nombre_tecnico",
                   "descripcion_estandar",
                   "ejemplos",
                   "dqdl",
                    F.current_date().alias("load_dt")  
                )
        )
    else:
        logger.info("DIM_REGLA existe → merge catálogo vs dimensión")
    
        df_dim_regla = glueContext.create_dynamic_frame.from_catalog(
            database=f"{DB_NAME_CAL_GOLD}",
            table_name="dim_regla"
        ).toDF()
    
        df_merge = (
            df_dim_regla.alias("dimrgl")
            .join(df_tbl_regla.alias("tblrgl"),
                  col("dimrgl.id") == col("tblrgl.id"),
                  "full_outer")
            .select(
                coalesce(col("tblrgl.id"),col("dimrgl.id")).alias("id"),
                coalesce(col("tblrgl.codigo_regla"),col("dimrgl.codigo_regla")).alias("codigo_regla"),
                coalesce(col("tblrgl.dimension"),col("dimrgl.dimension")).alias("dimension"),
                coalesce(col("tblrgl.tipo_regla"),col("dimrgl.tipo_regla")).alias("tipo_regla"),
                coalesce(col("tblrgl.nombre_tecnico"),col("dimrgl.nombre_tecnico")).alias("nombre_tecnico"),
                coalesce(col("tblrgl.descripcion_estandar"),col("dimrgl.descripcion_estandar")).alias("descripcion_estandar"),
                coalesce(col("tblrgl.ejemplos"),col("dimrgl.ejemplos")).alias("ejemplos"),
                coalesce(col("tblrgl.dqdl"),col("dimrgl.dqdl")).alias("dqdl"),
                F.to_date(F.current_timestamp()).alias("load_dt")
            )
        )
    
    df_merge.printSchema()
    df_merge.show(20, truncate=False)
    
    output_path = f"s3://{BUCKET_NAME_GOLD}/GOLD/CALIDAD_CARGA_GOLD/dim_regla/"
    
    df_merge.write \
        .mode("overwrite") \
        .parquet(output_path)
    
    logger.info(f"DIM_REGLA cargada correctamente en {output_path}")

                        ###################################################################
                        ########################## PROCESO DIM_REGLA_ATRIBUTO #############
                        ###################################################################
                        
    df_tbl_regla_atributo=(
    df_regla_atributo.select(
        F.col("id_atributo"),            		
        F.col("id_regla"),
        F.col("criticidad"),
        F.col("aplica"),
        F.col("parametros"),
        F.col("fecha_vigencia_desde"),
        F.col("fecha_vigencia_hasta")
    ))     #tbl 1 datos bronce      
    
    df_tbl_regla_atributo.show(20, truncate=False)
    logger.info("Mostradas primeras 20 filas de df_tbl_regla_atributo")

    
    dim_exists = glue_table_exists_safe(  
        f"{DB_NAME_CAL_GOLD}",
        "dim_regla_atributo"
    )
    if not dim_exists:
        logger.info("DIM_REGLA no existe → carga inicial")
    
        df_merge = (
            df_tbl_regla_atributo
                .select(
                   "id_atributo",            		
                   "id_regla",
                   "criticidad",
                   "aplica",
                   "parametros",
                   "fecha_vigencia_desde",
                   "fecha_vigencia_hasta",
                    F.current_date().alias("load_dt")  
                )
        )
    else:
        logger.info("DIM_REGLA_ATRIBUTO existe → merge catálogo vs dimensión")
    
        df_dim_regla_atributo = glueContext.create_dynamic_frame.from_catalog(
            database=f"{DB_NAME_CAL_GOLD}",
            table_name="dim_regla_atributo"
        ).toDF()
    
        df_merge = (
            df_dim_regla_atributo.alias("dimrglat")
            .join(df_tbl_regla_atributo.alias("tblrglat"),
                  col("dimrglat.id_atributo") == col("tblrglat.id_atributo"),
                  "full_outer")
            .select(
                coalesce(col("tblrglat.id_atributo"),col("dimrglat.id_atributo")).alias("id_atributo"),
                coalesce(col("tblrglat.id_regla"),col("dimrglat.id_regla")).alias("id_regla"),
                coalesce(col("tblrglat.criticidad"),col("dimrglat.criticidad")).alias("criticidad"),
                coalesce(col("tblrglat.aplica"),col("dimrglat.aplica")).alias("aplica"),
                coalesce(col("tblrglat.parametros"),col("dimrglat.parametros")).alias("parametros"),
                coalesce(col("tblrglat.fecha_vigencia_desde"),col("dimrglat.fecha_vigencia_desde")).alias("fecha_vigencia_desde"),
                coalesce(col("tblrglat.fecha_vigencia_hasta"),col("dimrglat.fecha_vigencia_hasta")).alias("fecha_vigencia_hasta"),
                F.to_date(F.current_timestamp()).alias("load_dt")
            )
        )
    
    df_merge.printSchema()
    df_merge.show(20, truncate=False)
    
    output_path = f"s3://{BUCKET_NAME_GOLD}/GOLD/CALIDAD_CARGA_GOLD/dim_regla_atributo/"
    
    df_merge.write \
        .mode("overwrite") \
        .parquet(output_path)
    
    logger.info(f"DIM_REGLA_ATRIBUTO cargada correctamente en {output_path}")
    

                        ###################################################################
                        ########################## PROCESO DIM_UBRAL  #####################
                        ###################################################################
                        
    df_tbl_umbral=(
    df_umbral.select(
        F.col("id_umbral"),            		
        F.col("descripcion"),
        F.col("valor_alto"),
        F.col("valor_medio"),
        F.col("valor_bajo")  
    ))     #tbl 1 datos bronce         
    
    df_tbl_umbral.show(20, truncate=False)
    logger.info("Mostradas primeras 20 filas de df_tbl_umbral")

    
    dim_exists = glue_table_exists_safe(  
        f"{DB_NAME_CAL_GOLD}",
        "dim_umbral"
    )
    if not dim_exists:
        logger.info("DIM_UMBRAL no existe → carga inicial")
    
        df_merge = (
            df_tbl_umbral
                .select(
                   "id_umbral",
                   "descripcion",            		
                   "valor_alto",
                   "valor_medio",
                   "valor_bajo",
                    F.current_date().alias("load_dt")  
                )
        )
    else:
        logger.info("DIM_UMBRAL existe → merge catálogo vs dimensión")
    
        df_dim_umbral = glueContext.create_dynamic_frame.from_catalog(
            database=f"{DB_NAME_CAL_GOLD}",
            table_name="dim_umbral"
        ).toDF()
    
        df_merge = (
            df_dim_umbral.alias("dimumb")
            .join(df_tbl_umbral.alias("tblumb"),
                  col("dimumb.id_umbral") == col("tblumb.id_umbral"),
                  "full_outer")
            .select(
                coalesce(col("tblumb.id_umbral"),col("dimumb.id_umbral")).alias("id_umbral"),
                coalesce(col("tblumb.descripcion"),col("dimumb.descripcion")).alias("descripcion"),
                coalesce(col("tblumb.valor_alto"),col("dimumb.valor_alto")).alias("valor_alto"),
                coalesce(col("tblumb.valor_medio"),col("dimumb.valor_medio")).alias("valor_medio"),
                coalesce(col("tblumb.valor_bajo"),col("dimumb.valor_bajo")).alias("valor_bajo"),
                F.to_date(F.current_timestamp()).alias("load_dt")
            )
        )
    
    df_merge.printSchema()
    df_merge.show(20, truncate=False)
    
    output_path = f"s3://{BUCKET_NAME_GOLD}/GOLD/CALIDAD_CARGA_GOLD/dim_umbral/"
    
    df_merge.write \
        .mode("overwrite") \
        .parquet(output_path)
    
    logger.info(f"DIM_UMBRAL cargada correctamente en {output_path}")
    

                        ###################################################################
                        ########################## PROCESO DIM_LISTA_REFERENCIA ###########
                        ###################################################################
                        
    df_tbl_lista_referencia=(
    df_lista_referencia.select(
        F.col("id_lista"),            		
        F.col("nombre_lista"),
        F.col("codigo"),
        F.col("descripcion")
    ))     #tbl 1 datos bronce         
    
    df_tbl_lista_referencia.show(20, truncate=False)
    logger.info("Mostradas primeras 20 filas de df_tbl_lista_referencia")

    
    dim_exists = glue_table_exists_safe(  
        f"{DB_NAME_CAL_GOLD}",
        "dim_lista_referencia"
    )
    if not dim_exists:
        logger.info("DIM_LISTA_REFERENCIA no existe → carga inicial")
    
        df_merge = (
            df_tbl_lista_referencia
                .select(
                   "id_lista",
                   "nombre_lista",            		
                   "codigo",
                   "descripcion",
                    F.current_date().alias("load_dt")  
                )
        )
    else:
        logger.info("DIM_LISTA_REFERENCIA existe → merge catálogo vs dimensión")
    
        df_dim_lista_referencia = glueContext.create_dynamic_frame.from_catalog(
            database=f"{DB_NAME_CAL_GOLD}",
            table_name="dim_lista_referencia"
        ).toDF()
    
        df_merge = (
            df_dim_lista_referencia.alias("dimlirf")
            .join(df_tbl_lista_referencia.alias("tbllirf"),
                  col("dimlirf.id_lista") == col("tbllirf.id_lista"),
                  "full_outer")
            .select(
                coalesce(col("tbllirf.id_lista"),col("dimlirf.id_lista")).alias("id_lista"),
                coalesce(col("tbllirf.nombre_lista"),col("dimlirf.nombre_lista")).alias("nombre_lista"),
                coalesce(col("tbllirf.codigo"),col("dimlirf.codigo")).alias("codigo"),
                coalesce(col("tbllirf.descripcion"),col("dimlirf.descripcion")).alias("descripcion"),
                F.to_date(F.current_timestamp()).alias("load_dt")
            )
        )
    
    df_merge.printSchema()
    df_merge.show(20, truncate=False)
    
    output_path = f"s3://{BUCKET_NAME_GOLD}/GOLD/CALIDAD_CARGA_GOLD/dim_lista_referencia/"
    
    df_merge.write \
        .mode("overwrite") \
        .parquet(output_path)
    
    logger.info(f"DIM_LISTA_REFERENCIA cargada correctamente en {output_path}")   
    

                        ###################################################################
                        ########################## PROCESO DIM_CALENDARIO   ###############
                        ###################################################################
                        
    
    df_tbl_calendario = (
        df_calendario
            .withColumn(
                "fecha_formada",
                F.to_date(
                    F.concat_ws(
                        "-",
                        F.col("anio_t").cast("string"),
                        F.lpad(F.col("mes_t").cast("string"), 2, "0"),
                        F.lpad(F.col("dia_mes_t").cast("string"), 2, "0")
                    ),
                    "yyyy-MM-dd"
                )
            )
            .select(
                F.col("fecha_formada"),
                F.col("anio_t").alias("año"),
                F.col("mes_t").alias("mes"),
                F.col("dia_mes_t").alias("dia")
            )
    )

    
    df_tbl_calendario.show(20, truncate=False)
    logger.info("Mostradas primeras 20 filas de df_tbl_calendario")

    
    dim_exists = glue_table_exists_safe(  
        f"{DB_NAME_CAL_GOLD}",
        "dim_calendario"
    )
    
    if not dim_exists:
        df_merge = df_tbl_calendario.select(
            col("fecha_formada").alias("fecha"),
            "año",
            "mes",
            "dia"
        )
    
    else:
        df_dim_calendario = spark.table(
        f"{DB_NAME_CAL_GOLD}.dim_calendario"  
        )
    
        df_merge = (
            df_dim_calendario.alias("dimcal")
            .join(
                df_tbl_calendario.alias("tblcal"),
                col("dimcal.fecha") == col("tblcal.fecha_formada"),
                "full_outer"
            )
            .select(
                F.coalesce(col("tblcal.fecha_formada"), col("dimcal.fecha")).alias("fecha"),
                F.coalesce(col("tblcal.año"), col("dimcal.año")).alias("año"),
                F.coalesce(col("tblcal.mes"), col("dimcal.mes")).alias("mes"),
                F.coalesce(col("tblcal.dia"), col("dimcal.dia")).alias("dia")
            )
        )
        
        
    window_fecha = Window.orderBy("fecha")
    window_periodo = Window.orderBy("periodo")
    window_id = Window.orderBy("fecha")


    df_final = (
        df_merge
            .withColumn("fecha_corta", F.to_date("fecha"))
            .withColumn("periodo", F.date_format("fecha", "yyyyMM"))
            .withColumn("id", F.row_number().over(window_fecha))
            .withColumn("id_periodo", F.dense_rank().over(window_periodo))
            .withColumn("load_dt", F.current_date())
            .select(
                "id",
                "id_periodo",
                "fecha",
                "fecha_corta",
                "periodo",
                "año",
                "mes",
                "dia",
                "load_dt"
            )
    )


    df_final.printSchema()
    df_final.show(20, truncate=False)
    
    
    output_path = f"s3://{BUCKET_NAME_GOLD}/GOLD/CALIDAD_CARGA_GOLD/dim_calendario/"
    
    df_final.write \
        .mode("overwrite") \
        .parquet(output_path)
    
    logger.info(f"DIM_CALENDARIO cargada correctamente en {output_path}")
    
    
except Exception as e:
    logger.error(f"ERROR WITH Evaluate error ATRIBUTO: {traceback.format_exc()}")  
    
