SQL_PARTICIPANTES = 'SELECT DISTINCT PP.AGREGATEDPARENTID AS POLIZA, ROL.ROL_DESCRIPTION,\n PT.DOCUMENTOIDENTIDADINPUT, PT.NOMBREPREDETERMINADOINPUT, PPPOL.STATUS\n FROM STPO_POLICYPARTICIPATIONDCO PPPOL\n JOIN STPO_POLICYPARTICIPATION PP ON PP.AGREGATEDOBJECTID=PPPOL.AGREGATEDOBJECTID\n JOIN STTS_ROLE ROL ON ROL.ROL_ID=PP.ROL_ID\n JOIN PRETHIRDPARTY PT ON PT.STATIC=PP.THIRDPARTYID'
SQL_TOMADOR = "SELECT DISTINCT POLIZA, DOCUMENTOIDENTIDADINPUT AS IDENTIFICACION_TOMADOR, NOMBREPREDETERMINADOINPUT AS TOMADOR FROM VIDA_PARTICIPANTES WHERE UPPER(ROL_DESCRIPTION) LIKE '%TOMADOR%'"

import json,boto3
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
B='alfa-datalake-dev-assets'
BASE='pruebas/pcr/vida/ejecuciones/d1e3c879db73414191cb1677cc187983/'
ROOT=BASE+'diagnostico_tomador_v1/'
s=boto3.Session(region_name='us-east-1')
if s.client('sts').get_caller_identity()['Account']!='533267035849':raise ValueError('Solo DEV')
s3=s.client('s3');rt=json.loads(s3.get_object(Bucket=B,Key=BASE+'postextraccion_27_v1/runtime.json')['Body'].read())
if rt['lote_cargue_id']!=1793024990:raise ValueError('Lote incorrecto')
report={'estado':'RUNNING','alcance':'Solo lectura Silver; reporte tecnico sin nombres ni documentos en claro'}
def save():s3.put_object(Bucket=B,Key=ROOT+'resultado.json',Body=json.dumps(report,ensure_ascii=False,indent=2,default=str).encode(),ContentType='application/json')
save()
try:
 gc=GlueContext(SparkContext.getOrCreate());spark=gc.spark_session
 for key in ('spark.sql.parquet.datetimeRebaseModeInRead','spark.sql.parquet.int96RebaseModeInRead'):spark.conf.set(key,'LEGACY')
 needed={'STPO_POLICYPARTICIPATIONDCO','STPO_POLICYPARTICIPATION','STTS_ROLE','PRETHIRDPARTY','AGREGATEDPOLICY','PREPRODUCT'}
 for d in rt['datasets_extraidos']:
  name=d['tabla'].upper()
  if name not in needed:continue
  uri=d['ruta_dataset_destino']
  if not uri.startswith('s3://alfa-datalake-dev-silver/'+BASE+'postextraccion_27_v1/'):raise ValueError('Ruta no aislada')
  df=spark.read.parquet(uri).filter(F.col('LOTE_CARGUE_ID')==1793024990)
  for field in df.schema.fields:
   if isinstance(field.dataType,StringType):df=df.withColumn(field.name,F.when(F.col(field.name)=='',F.lit(None)).otherwise(F.col(field.name)))
  df.createOrReplaceTempView(name)
 spark.sql(SQL_PARTICIPANTES).createOrReplaceTempView('VIDA_PARTICIPANTES')
 candidates=spark.sql(SQL_TOMADOR)
 conflicts=candidates.groupBy('POLIZA').count().filter('count > 1')
 rows=candidates.join(conflicts.select('POLIZA'),'POLIZA')
 # Reproducir el diagnostico exacto; limitar la salida, no escoger tomadores.
 report['polizas_ambiguas_total']=conflicts.count()
 selected=spark.sql("SELECT DISTINCT AP.AGREGATEDPOLICYID AS POLIZA FROM AGREGATEDPOLICY AP JOIN PREPRODUCT P ON P.STATIC=AP.PRODUCTID WHERE P.STATIC=48897 AND P.COMPANIAVALUE='50.0'")
 report['polizas_ambiguas_producto_compania']=conflicts.join(selected,'POLIZA','left_semi').count()
 report['detalle']= [r.asDict() for r in rows.groupBy('POLIZA').agg(F.count('*').alias('candidatos_distintos'),F.countDistinct('IDENTIFICACION_TOMADOR').alias('documentos_distintos_no_nulos'),F.countDistinct('TOMADOR').alias('nombres_distintos_no_nulos'),F.sum(F.when(F.col('IDENTIFICACION_TOMADOR').isNull(),1).otherwise(0)).alias('documentos_nulos'),F.sum(F.when(F.col('TOMADOR').isNull(),1).otherwise(0)).alias('nombres_nulos')).orderBy('POLIZA').limit(50).collect()]
 # Hashes permiten distinguir pares sin exponer datos personales.
 report['pares_hash']=[r.asDict() for r in rows.select('POLIZA',F.sha2(F.to_json(F.struct('IDENTIFICACION_TOMADOR','TOMADOR'),options={'ignoreNullFields':'false'}),256).alias('par_hash')).orderBy('POLIZA','par_hash').limit(100).collect()]
 report['estado']='SUCCESS';report['advertencia']='No cambia reglas ni selecciona candidatos. Falta evaluar pertenencia a la salida final y semantica SQL.';save()
except Exception as exc:report.update(estado='FAILED',error=str(exc));save();raise
