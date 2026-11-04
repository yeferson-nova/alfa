
import sys,json,boto3
from datetime import datetime,timezone
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
args=getResolvedOptions(sys.argv,['JOB_NAME','RESULT_KEY'])
session=boto3.Session(region_name='us-east-1')
if session.client('sts').get_caller_identity()['Account']!='533267035849':raise ValueError('Cuenta distinta de DEV')
if not args['RESULT_KEY'].startswith('pruebas/pcr/diagnostico-indices/resultados/'):raise ValueError('Ruta no autorizada')
s3=session.client('s3');bucket='alfa-datalake-dev-assets'
r={'estado':'RUNNING','inicio':datetime.now(timezone.utc).isoformat(),
   'alcance':'Metadatos visibles de indices; NO plan real ni prueba de uso del indice'}
def save():
 s3.put_object(Bucket=bucket,Key=args['RESULT_KEY'],Body=json.dumps(r,ensure_ascii=False,indent=2,default=str).encode(),ContentType='application/json')
save()
try:
 gc=GlueContext(SparkContext.getOrCreate());job=Job(gc);job.init(args['JOB_NAME'],args)
 sql="""SELECT I.OWNER AS INDEX_OWNER,I.INDEX_NAME,I.INDEX_TYPE,I.STATUS,
 I.PARTITIONED,I.VISIBILITY,I.UNIQUENESS,I.LAST_ANALYZED,
 C.COLUMN_NAME,C.COLUMN_POSITION,C.DESCEND
 FROM ALL_INDEXES I JOIN ALL_IND_COLUMNS C
 ON C.INDEX_OWNER=I.OWNER AND C.INDEX_NAME=I.INDEX_NAME
 AND C.TABLE_OWNER=I.TABLE_OWNER AND C.TABLE_NAME=I.TABLE_NAME
 WHERE I.TABLE_OWNER='ACSELE_ALFA' AND I.TABLE_NAME='CONTEXTOPERATION'
 ORDER BY I.INDEX_NAME,C.COLUMN_POSITION"""
 df=gc.create_dynamic_frame.from_options(connection_type='oracle',connection_options={
 'useConnectionProperties':'true','connectionName':'datalake-dev-acsele-sop-connection',
 'dbtable':'ALL_INDEXES','sampleQuery':sql}).toDF()
 rows=df.limit(501).collect()
 if len(rows)>500:raise ValueError('Metadatos superan limite; no truncar silenciosamente')
 r.update(estado='SUCCESS',indices_columnas=[x.asDict() for x in rows],
 fin=datetime.now(timezone.utc).isoformat(),advertencia='Sin filas no demuestra ausencia de indices; revisar visibilidad. Indices particionados requieren revision de particiones. No crear indices ni forzar hints a partir de este resultado.')
 save();print(json.dumps(r,ensure_ascii=False,default=str));job.commit()
except Exception as exc:
 r.update(estado='FAILED',error_tipo=type(exc).__name__)
 try:save()
 except Exception:pass
 raise
