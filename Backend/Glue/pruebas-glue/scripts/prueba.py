
import sys,json,boto3,hashlib
from datetime import datetime,timezone
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pcr_segmentos_checkpoint import SegmentRunner,S3Store,fingerprint
args=getResolvedOptions(sys.argv,['JOB_NAME','PRUEBA_ID','FALLAR'])
if args['FALLAR'] not in ('true','false'):raise ValueError('FALLAR invalido')
import re
if not re.fullmatch('[a-f0-9]{32}',args['PRUEBA_ID']):raise ValueError('PRUEBA_ID invalido')
session=boto3.Session(region_name='us-east-1')
if session.client('sts').get_caller_identity()['Account']!='533267035849':raise ValueError('Cuenta distinta de DEV')
s3=session.client('s3');bucket='alfa-datalake-dev-assets'
root='pruebas/pcr/vida/prueba_checkpoint/'+args['PRUEBA_ID']+'/'
store=S3Store(s3,bucket)
gc=GlueContext(SparkContext.getOrCreate());spark=gc.spark_session
for k in ('spark.sql.parquet.datetimeRebaseModeInRead','spark.sql.parquet.int96RebaseModeInRead','spark.sql.parquet.datetimeRebaseModeInWrite','spark.sql.parquet.int96RebaseModeInWrite'):spark.conf.set(k,'LEGACY')
job=Job(gc);job.init(args['JOB_NAME'],args)
rtkey='pruebas/pcr/vida/ejecuciones/26c8996eb27249508e748f332fd4bf0d/parametros_extraccion.json'
rt=json.loads(s3.get_object(Bucket=bucket,Key=rtkey)['Body'].read())
if rt.get('estado_extraccion')!='SUCCESS' or rt.get('periodo_negocio')!='202606':raise ValueError('Runtime de referencia inesperado')
items=[d for d in rt['datasets_extraidos'] if d['tabla_relativa'].lower()=='acsele_alfa/contextoperation']
if len(items)!=1 or items[0]['total_registros']!=5:raise ValueError('Pivote de referencia inesperado')
source=items[0]['ruta_dataset_origen']
expected='s3://alfa-datalake-dev-bronce/pruebas/pcr/vida/ejecuciones/26c8996eb27249508e748f332fd4bf0d/'
if not source.startswith(expected):raise ValueError('Fuente fuera de ejecucion validada')
df=spark.read.parquet(source)
cols=[c.upper() for c in df.columns]
if len(cols)!=len(set(cols)):raise ValueError('Columnas ambiguas')
df=df.toDF(*cols).cache()
ids=sorted(int(r['ID']) for r in df.select('ID').collect())
if ids!=sorted([1059735080,1059734662,1059736162,1059736419,1059735505]):raise ValueError('IDs distintos de referencia')
# Inventario exacto de objetos de entrada para impedir reanudar con fuente modificada.
from urllib.parse import urlparse
p=urlparse(source);inventory=[]
for page in s3.get_paginator('list_objects_v2').paginate(Bucket=p.netloc,Prefix=p.path.lstrip('/').rstrip('/')+'/'):
    inventory.extend({'key':o['Key'],'etag':o['ETag'],'size':o['Size']} for o in page.get('Contents',[]) if o['Key'].endswith('.parquet'))
if not inventory:raise ValueError('Fuente sin Parquet')
segments=[]
for i in range(0,len(ids),2):
    keys=ids[i:i+2];sid=str(i//2+1).zfill(3);key=root+'inputs/'+sid+'.json'
    value={'ids':keys};old=store.get(key)
    if old is None:store.create(key,value)
    elif old!=value:raise ValueError('Llaves persistidas distintas')
    segments.append({'id':sid,'input_ref':key,'input_hash':fingerprint(value),'max_rows':2,'max_bytes':10485760})
plan={'execution_id':args['PRUEBA_ID'],'periodo':'202606','lote':int(rt['lote_cargue_id']),
      'config_hash':fingerprint({'fuente':source,'segmento':2,'columnas':cols}),
      'source_snapshot':fingerprint(sorted(inventory,key=lambda x:x['key'])),'segments':segments}
runner=SegmentRunner(store,root,plan)

def files(prefix):
    found=[];size=0
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket=bucket,Prefix=prefix):
        for o in page.get('Contents',[]):
            if o['Key'].endswith('.parquet'):found.append(o['Key']);size+=o['Size']
    return sorted(found),size

def extract(seg,prefix,ph):
    value=store.get(seg['input_ref'])
    if fingerprint(value)!=seg['input_hash']:raise ValueError('Llaves cambiaron')
    rows=df.filter(F.col('ID').isin(value['ids']))
    count=rows.count()
    if count!=len(value['ids']):raise ValueError('Faltan registros del segmento')
    rows.coalesce(1).write.mode('errorifexists').parquet('s3://'+bucket+'/'+prefix+'datos/')
    rows.select('ID').dropDuplicates().coalesce(1).write.mode('errorifexists').parquet('s3://'+bucket+'/'+prefix+'llaves/')
    data,nb=files(prefix+'datos/');keys,kb=files(prefix+'llaves/')
    print('SEGMENTO EXTRAIDO:',seg['id'],'FILAS:',count,flush=True)
    return {'segment_id':seg['id'],'input_hash':seg['input_hash'],'plan_hash':ph,
        'attempt_prefix':prefix,'rows':count,'bytes':nb+kb,'key_rows':count,'keys_required':True,
        'data_files':data,'key_files':keys,'writes_complete':True}

def verify(result):
    for key in result['data_files']+result['key_files']:s3.head_object(Bucket=bucket,Key=key)
    actual=spark.read.parquet(*['s3://'+bucket+'/'+k for k in result['data_files']]).count()
    keys=spark.read.parquet(*['s3://'+bucket+'/'+k for k in result['key_files']]).count()
    if actual!=result['rows'] or keys!=result['key_rows']:raise ValueError('Conteos de commit distintos')

try:
    final=runner.run(extract,verify,fail_after=1 if args['FALLAR']=='true' else None)
    committed=runner.committed(verify)
    paths=[k for r in committed for k in r['data_files']]
    result=spark.read.parquet(*['s3://'+bucket+'/'+k for k in paths])
    actual_ids=sorted(int(r['ID']) for r in result.select('ID').collect())
    if actual_ids!=ids:raise ValueError('Perdida o duplicacion de IDs')
    # Comparar multiconjuntos completos; no solo IDs o totales.
    # CHECKPOINT_COMPARE_FIVE_ROWS_V1: evitar exceptAll/count en Spark.
    from collections import Counter as _CheckpointCounter
    _expected_schema = [(f.name, f.dataType.json()) for f in df.select(*cols).schema.fields]
    _actual_schema = [(f.name, f.dataType.json()) for f in result.select(*cols).schema.fields]
    if _actual_schema != _expected_schema:
        raise ValueError("Esquema de salida distinto de fuente")
    # Solo esta prueba de cinco filas. Nunca colectar un periodo completo.
    _expected_rows = df.select(*cols).limit(6).collect()
    _actual_rows = result.select(*cols).limit(6).collect()
    if len(_expected_rows) != 5 or len(_actual_rows) != 5:
        raise ValueError("La comparacion exige exactamente cinco filas por conjunto")
    if _CheckpointCounter(tuple(r) for r in _expected_rows) != _CheckpointCounter(tuple(r) for r in _actual_rows):
        raise ValueError("Filas diferentes de fuente, incluyendo duplicados y NULL")
    report={'estado':'SUCCESS','prueba_id':args['PRUEBA_ID'],'segmentos':3,'filas':5,
        'filas_completas_coinciden':True,'archivos_llaves_confirmados':len(runner.key_paths(verify)),
        'alcance':'Solo pivote Parquet; NO extractor Oracle ni dependencias 27 tablas',
        'fecha':datetime.now(timezone.utc).isoformat()}
    key=root+'resultado.json';old=store.get(key)
    if old is None:store.create(key,report)
    print(json.dumps(report,ensure_ascii=False),flush=True);job.commit()
finally:df.unpersist()
