
import sys,json,boto3
from decimal import Decimal,getcontext
from datetime import datetime,date,timezone
from collections import Counter
from numbers import Number
from urllib.parse import urlparse
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark import StorageLevel
getcontext().prec=60
args=getResolvedOptions(sys.argv,['JOB_NAME','MANIFEST_KEY'])
session=boto3.Session(region_name='us-east-1')
if session.client('sts').get_caller_identity()['Account']!='533267035849':raise ValueError('Cuenta distinta de DEV')
s3=session.client('s3');bucket='alfa-datalake-dev-assets'
if not args['MANIFEST_KEY'].startswith('pruebas/pcr/vida/referencia_athena_50/'):raise ValueError('Manifest fuera de ruta')
p=json.loads(s3.get_object(Bucket=bucket,Key=args['MANIFEST_KEY'])['Body'].read())
def leer(uri):
 u=urlparse(uri);return json.loads(s3.get_object(Bucket=u.netloc,Key=u.path.lstrip('/'))['Body'].read())
report={'estado':'RUNNING','alcance':'Referencia ADAPTADA del SQL original sobre Parquet; NO ejecucion Oracle','inicio':datetime.now(timezone.utc).isoformat()}
def save():s3.put_object(Bucket=bucket,Key=p['resultado_key'],Body=json.dumps(report,ensure_ascii=False,indent=2).encode(),ContentType='application/json')
save();cache=[]
try:
 gc=GlueContext(SparkContext.getOrCreate());spark=gc.spark_session;job=Job(gc);job.init(args['JOB_NAME'],args)
 for k in ('spark.sql.parquet.datetimeRebaseModeInRead','spark.sql.parquet.int96RebaseModeInRead','spark.sql.parquet.datetimeRebaseModeInWrite','spark.sql.parquet.int96RebaseModeInWrite'):spark.conf.set(k,'LEGACY')
 spark.conf.set('spark.sql.caseSensitive','false');spark.conf.set('spark.sql.ansi.enabled','true')
 spark.conf.set('spark.sql.decimalOperations.allowPrecisionLoss','false')
 rt=leer(p['runtime'])
 if rt.get('estado_preparacion_silver')!='SUCCESS' or rt.get('periodo_negocio')!='202606' or rt.get('ruta_silver_preparada')!=p['desarrollo']:raise ValueError('Runtime no corresponde a muestra conocida')
 if any(rt.get(k)!='SUCCESS' for k in ('estado_extraccion','estado_validacion','estado_remediacion','estado_disponibilizacion_silver')) or rt.get('lote_cargue_id')!=1792559315:raise ValueError('Etapas o lote inesperados')
 ds=rt['datasets_extraidos']
 if len(ds)!=27:raise ValueError('Se esperaban 27 fuentes')
 for d in ds:
  ruta=d['ruta_dataset_destino']
  if not ruta.startswith('s3://alfa-datalake-dev-silver/pruebas/pcr/vida/ejecuciones/a28bcba92b8d41509898a41ff0d266da/base/'):raise ValueError('Fuente fuera de prueba')
  df=spark.read.parquet(ruta);cols=[c.upper() for c in df.columns]
  if len(cols)!=len(set(cols)):raise ValueError('Columnas ambiguas')
  df=df.toDF(*cols).filter(F.col('LOTE_CARGUE_ID').isin([int(x) for x in (d.get('lotes_preparacion_silver') or [rt['lote_cargue_id']])]))
  for field in df.schema.fields:
   if isinstance(field.dataType,StringType):df=df.withColumn(field.name,F.when(F.col(field.name)=='',F.lit(None)).otherwise(F.col(field.name)))
  df=df.persist(StorageLevel.MEMORY_AND_DISK);cache.append(df)
  if df.count()!=int(d['total_registros']):raise ValueError('Conteo fuente distinto: '+d['tabla_relativa'])
  df.createOrReplaceTempView(d['tabla_relativa'].split('/')[-1].upper())
 spark.sql('SELECT AGREGATEDPARENTID, OPERATIONPK, MIN(AGREGATEDOBJECTID) AS MIN_ID FROM INSURANCEOBJECTDCO GROUP BY AGREGATEDPARENTID,OPERATIONPK').createOrReplaceTempView('REF_MIN_OBJETO')
 for name,sql in p['etapas']:
  df=spark.sql(sql).persist(StorageLevel.MEMORY_AND_DISK);cache.append(df)
  n=df.count()
  if n>2000:raise ValueError('Referencia excede limite controlado')
  print('REFERENCIA',name,n,flush=True);df.createOrReplaceTempView(name)
 ref=spark.table('VIDA_FINAL');dev=spark.read.parquet(p['desarrollo'])
 def filas(df):
  cols=[c.upper() for c in df.columns]
  if len(cols)!=len(set(cols)):raise ValueError('Columnas ambiguas')
  rows=df.toDF(*cols).limit(2001).collect()
  if len(rows)>2000:raise ValueError('Salida supera limite; no truncar para comparar')
  return [c for c in cols if c!='LOTE_CARGUE_ID'],[r.asDict() for r in rows]
 cr,rr=filas(ref);cd,rd=filas(dev)
 if len(cr)!=63 or set(cr)!=set(cd):raise ValueError('Esquemas de negocio distintos')
 pivot=spark.table('CONTEXTOPERATION').select('ID').collect()
 ids={int(r['ID']) for r in pivot}
 if len(pivot)!=50 or len(ids)!=50:raise ValueError('Pivote no tiene 50 IDs distintos')
 ops_ref={int(r['NRO_OPERACION']) for r in rr};ops_dev={int(r['NRO_OPERACION']) for r in rd}
 if not ops_ref.issubset(ids) or not ops_dev.issubset(ids):raise ValueError('Salida fuera del pivote')
 def norm(v):
  if v is None or v=='':return None
  if isinstance(v,(Decimal,Number)):
   d=Decimal(str(v))
   if not d.is_finite():raise ValueError('Numero no finito')
   return ('n',d)
  if isinstance(v,datetime):return ('d',v.isoformat(sep=' ',timespec='microseconds'))
  if isinstance(v,date):return ('d',datetime.combine(v,datetime.min.time()).isoformat(sep=' ',timespec='microseconds'))
  return ('s',str(v))
 def bag(rows):return Counter(tuple(norm(r.get(c)) for c in cr) for r in rows)
 coincide=bag(rr)==bag(rd)
 def total(rows,c):
  vals=[Decimal(str(r[c])) for r in rows if r.get(c) is not None]
  return str(sum(vals,Decimal(0))) if vals else None
 report['importes']=[{'concepto':c,'referencia':total(rr,c),'desarrollo':total(rd,c),'nulos_referencia':sum(r.get(c) is None for r in rr),'nulos_desarrollo':sum(r.get(c) is None for r in rd)} for c in ['PRIMA','COSTO_RECAUDO','IVA_COSTO_RECAUDO','COMISION','VALOR_ASEGURADO']]
 campos=['NRO_OPERACION','PRIMA','COSTO_RECAUDO','IVA_COSTO_RECAUDO','COMISION','VALOR_ASEGURADO']
 out=ref.toDF(*[c.upper() for c in ref.columns]).select(*[F.col(c).cast('decimal(19,0)' if c=='NRO_OPERACION' else 'decimal(38,10)').alias(c.lower()) for c in campos])
 out.coalesce(1).write.mode('errorifexists').parquet(p['referencia'])
 ref.write.mode('errorifexists').parquet(p['referencia'].rstrip('/')+'_63_columnas/')
 report.update(estado='SUCCESS',filas_referencia=len(rr),filas_desarrollo=len(rd),columnas_comparadas=63,
  operaciones_pivote=len(ids),operaciones_referencia=len(ops_ref),operaciones_desarrollo=len(ops_dev),
  operaciones_solo_referencia=sorted(ops_ref-ops_dev),operaciones_solo_desarrollo=sorted(ops_dev-ops_ref),
  filas_solo_referencia=sum((bag(rr)-bag(rd)).values()),filas_solo_desarrollo=sum((bag(rd)-bag(rr)).values()),
  columnas_con_diferencias=[c for c in cr if Counter(norm(r.get(c)) for r in rr)!=Counter(norm(r.get(c)) for r in rd)],
  coincide_63_columnas=coincide,referencia=p['referencia'],fin=datetime.now(timezone.utc).isoformat(),
  advertencia='No es resultado Oracle ni certificacion del periodo. NULL no se convierte a cero.')
 save();print(json.dumps(report,ensure_ascii=False));job.commit()
except Exception as exc:
 report.update(estado='FAILED',error_tipo=type(exc).__name__)
 try:save()
 except Exception:pass
 raise
finally:
 for df in cache:df.unpersist()
