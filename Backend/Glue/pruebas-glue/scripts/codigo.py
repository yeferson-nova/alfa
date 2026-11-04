import sys,json,re,time,uuid,hashlib
from decimal import Decimal
from functools import reduce
from concurrent.futures import ThreadPoolExecutor,as_completed
from datetime import datetime,timezone
import boto3
from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark import StorageLevel
B='alfa-datalake-dev-assets'
BASE='pruebas/pcr/vida/ejecuciones/d1e3c879db73414191cb1677cc187983/'
LOT=1793024990
s=boto3.Session(region_name='us-east-1')
if s.client('sts').get_caller_identity()['Account']!='533267035849':raise ValueError('Solo DEV')
a=getResolvedOptions(sys.argv,['JOB_NAME','VIDA_RAMA'])
mode=a['VIDA_RAMA']
if mode not in ('grande','cuatro'):raise ValueError('Rama invalida')
if a['JOB_NAME']!='niif-dev-pcr-vida-final-'+mode+'-10w':raise ValueError('Job invalido')
ROOT=BASE+'final_10w_20q_20k_v1/'+mode+'/'
s3=s.client('s3')
def get(key):return json.loads(s3.get_object(Bucket=B,Key=key)['Body'].read())
def maybe(key):
 try:return get(key)
 except ClientError as exc:
  if exc.response['Error']['Code'] in ('NoSuchKey','404','NotFound'):return None
  raise
def put(key,value):s3.put_object(Bucket=B,Key=key,Body=json.dumps(value,ensure_ascii=False,indent=2).encode(),ContentType='application/json')
rt=get(ROOT+'runtime.json');cfg=get(BASE+'config_extraccion_periodo.json')
code=hashlib.sha256(open(__file__,'rb').read()).hexdigest()
if rt.get('lote_cargue_id')!=LOT or rt.get('periodo_negocio')!='202606':raise ValueError('Contrato diferente')
if rt.get('codigo_final') and rt['codigo_final']!=code:raise ValueError('Codigo cambio; no reusar checkpoints')
rt['codigo_final']=code
idx={d['tabla_relativa']:d for d in rt['datasets_extraidos']}
if len(idx)!=len(rt['datasets_extraidos']):raise ValueError('Datasets duplicados')
tables=[t for c in cfg['ciclos'] for seg in c['segmentos'] for t in seg['tablas']]
if len(tables)!=27:raise ValueError('Inventario diferente')
gc=GlueContext(SparkContext.getOrCreate());spark=gc.spark_session;job=Job(gc);job.init(a['JOB_NAME'],a)
for key in ('spark.sql.parquet.datetimeRebaseModeInWrite','spark.sql.parquet.int96RebaseModeInWrite','spark.sql.parquet.datetimeRebaseModeInRead','spark.sql.parquet.int96RebaseModeInRead'):spark.conf.set(key,'LEGACY')
credentials=gc.extract_jdbc_conf(cfg['fuente']['connection_name']);url=credentials.get('fullUrl') or credentials.get('url')
if not url or not url.startswith('jdbc:oracle:'):raise ValueError('Conexion no Oracle')
props={'user':credentials['user'],'password':credentials['password'],'driver':'oracle.jdbc.OracleDriver','fetchsize':str(cfg['fuente']['fetchsize']),'queryTimeout':'600','oracle.net.CONNECT_TIMEOUT':'30000','oracle.jdbc.ReadTimeout':'150000'}

_scope_cache = {}
_table_index = {t['tabla_relativa']: t for t in tables}
def _raw_parent(name):
    d = idx[name]
    if d['estado'] != 'SUCCESS' or d['lote_cargue_id_dataset'] != LOT:
        raise ValueError('Padre no confirmado')
    return spark.read.parquet(d['ruta_dataset_origen']).filter(F.col('LOTE_CARGUE_ID') == LOT)
def _semi(child, parent, pairs):
    names = ['_vida_key_' + str(i) for i in range(len(pairs))]
    right = parent.select(*[F.col(p).alias(n) for (p,c),n in zip(pairs,names)]).distinct()
    condition = reduce(lambda a,b:a & b, [child[c] == right[n] for (p,c),n in zip(pairs,names)])
    return child.join(right,condition,'left_semi')
_prep = _raw_parent('acsele_alfa/preproduct').filter((F.col('STATIC') == F.lit(48897)) & (F.col('COMPANIAVALUE') == F.lit('50.0')))
_ap = _semi(_raw_parent('acsele_alfa/agregatedpolicy'), _prep, [('STATIC','PRODUCTID')])
_po = _semi(_raw_parent('acsele_alfa/policydco'), _ap, [('AGREGATEDPOLICYID','AGREGATEDOBJECTID')])
_co = _semi(_raw_parent('acsele_alfa/contextoperation'), _po, [('OPERATIONPK','ID')]).persist(StorageLevel.DISK_ONLY)
_scope_count = _co.count()
if _scope_count == 0:
    raise ValueError('Universo Vida vacio; no se consulta Oracle')
_scope_cache['acsele_alfa/contextoperation'] = _co
_scope_cache['acsele_alfa/policydco'] = _semi(_po,_co,[('ID','OPERATIONPK')])
_scope_cache['acsele_alfa/agregatedpolicy'] = _semi(_ap,_scope_cache['acsele_alfa/policydco'],[('AGREGATEDOBJECTID','AGREGATEDPOLICYID')])
_scope_cache['acsele_alfa/preproduct'] = _prep
print('UNIVERSO_VIDA_OPERACIONES',_scope_count,flush=True)
rt['seleccion_vida'] = {'producto_static':48897,'companiavalue':'50.0','operaciones_pivote':_scope_count,'origen':'Bronze lote 1793024990','alcance':'Solo seleccion de universo; calculos y filtros de roles permanecen en preparacion','advertencia':'19 datasets previos son supersets; pendientes restringidas. No certificacion ni snapshot Oracle.'}
def scoped_parent(name):
    if name in _scope_cache:
        return _scope_cache[name]
    child = _raw_parent(name)
    deps = _table_index[name].get('dependencias_lectura',[])
    if not deps:
        raise ValueError('Padre sin relacion ni seleccion explicita')
    destinations = sorted(m['destino'] for m in deps[0]['mapeo'])
    keyframes = []
    for dep in deps:
        if sorted(m['destino'] for m in dep['mapeo']) != destinations:
            raise ValueError('Destinos diferentes; no simplificar')
        mapping = {m['destino']:m['origen'] for m in dep['mapeo']}
        parent = scoped_parent(dep['tabla_origen'])
        keyframes.append(parent.select(*[F.col(mapping[d]).alias(d) for d in destinations]))
    keyframe = reduce(lambda a,b:a.unionByName(b),keyframes).distinct()
    selected = child.join(keyframe,destinations,'left_semi')
    _scope_cache[name] = selected
    return selected

allowed={'acsele_alfa/thirdpartymovementpolicy'} if mode=='grande' else {'acsele_alfa/prethirdparty','acsele_alfa/thirdpartyrole','acsele_alfa/aseguradovida','acsele_alfa/stts_role'}
def save():put(ROOT+'runtime.json',rt)
def number(v):
 if isinstance(v,bool) or not isinstance(v,(int,Decimal)):raise ValueError('Llave no numerica exacta')
 d=Decimal(v)
 if not d.is_finite() or d!=d.to_integral_value():raise ValueError('Llave no entera')
 return str(int(d))
def predicate(cols,rows):
 groups=[]
 for i in range(0,len(rows),900):
  part=rows[i:i+900]
  if len(cols)==1:groups.append(cols[0]+' IN ('+','.join(r[0] for r in part)+')')
  else:groups.append('('+','.join(cols)+') IN ('+','.join('('+','.join(r)+')' for r in part)+')')
 return '('+' OR '.join(groups)+')' if groups else '1=0'
rt['estado_extraccion']='IN_PROGRESS';rt.pop('error_extraccion',None);save()
try:
 for t in tables:
  name=t['tabla_relativa']
  if name not in allowed or name in idx:continue
  for x in [t['schema'],t['tabla']]+t['columnas']:
   if not re.fullmatch('[A-Za-z][A-Za-z0-9_]*',x):raise ValueError('Identificador invalido')
  if t.get('where') is not None:raise ValueError('Filtro no soportado; no se ignora')
  deps=t['dependencias_lectura'];cols=sorted(m['destino'] for m in deps[0]['mapeo'])
  frames=[]
  for dep in deps:
   if sorted(m['destino'] for m in dep['mapeo'])!=cols:raise ValueError('Destinos incompatibles')
   parent=scoped_parent(dep['tabla_origen']);mapping={m['destino']:m['origen'] for m in dep['mapeo']}
   frames.append(parent.select(*[F.col(mapping[c]).alias(c) for c in cols]))
  folder=ROOT+'tablas/'+t['tabla'].lower()+'/'
  plan=maybe(folder+'plan.json')
  if plan is None:
   keys=reduce(lambda x,y:x.unionByName(y),frames).filter(reduce(lambda x,y:x&y,[F.col(c).isNotNull() for c in cols])).distinct().orderBy(*cols)
   batch=[];groups=0;total=0
   for row in keys.toLocalIterator(prefetchPartitions=False):
    batch.append([number(v) for v in row]);total+=1
    if len(batch)==20000:
     put(folder+'llaves/'+str(groups)+'.json',{'columnas':cols,'llaves':batch});groups+=1;batch=[]
   if batch or groups==0:put(folder+'llaves/'+str(groups)+'.json',{'columnas':cols,'llaves':batch});groups+=1
   plan={'grupos':groups,'llaves':total,'columnas':cols,'codigo':code,'lote':LOT,'tabla':name}
   put(folder+'plan.json',plan)
  if plan['codigo']!=code or plan['lote']!=LOT or plan['columnas']!=cols:raise ValueError('Plan incompatible')
  rt['checkpoint_extraccion']={'tabla_actual':name,'total_grupos':plan['grupos'],'total_llaves':plan['llaves'],'grupos_confirmados':0};save()
  def verify(commit):
   if commit.get('codigo')!=code or commit.get('lote')!=LOT:raise ValueError('Commit incompatible')
   check=spark.read.parquet(commit['ruta']).agg(F.count('*').alias('n'),F.count('LOTE_CARGUE_ID').alias('nl'),F.min('LOTE_CARGUE_ID').alias('lo'),F.max('LOTE_CARGUE_ID').alias('hi')).first()
   if check['n']!=commit['filas'] or check['nl']!=check['n'] or (check['n'] and (check['lo']!=LOT or check['hi']!=LOT)):raise ValueError('Salida confirmada no coincide')
  def extract(g):
   commit_key=folder+'commits/'+str(g)+'.json'
   commit=maybe(commit_key)
   if commit:
    manifest=get(folder+'llaves/'+str(g)+'.json')
    if commit.get('grupo')!=g or commit.get('llaves_hash')!=hashlib.sha256(json.dumps(manifest,sort_keys=True).encode()).hexdigest():raise ValueError('Commit no corresponde a las llaves')
    verify(commit);return g,commit
   manifest=get(folder+'llaves/'+str(g)+'.json')
   pred=predicate(cols,manifest['llaves'])
   query='(SELECT '+', '.join(t['columnas'])+' FROM '+t['schema']+'.'+t['tabla']+') VIDA_SRC'
   path='s3://alfa-datalake-dev-bronce/'+folder+'grupos/g='+str(g)+'/intento='+uuid.uuid4().hex+'/'
   start=time.monotonic()
   df=spark.read.jdbc(url=url,table=query,predicates=[pred],properties=props)
   df=df.toDF(*[c.upper() for c in df.columns]).select(*t['columnas'])
   df.withColumn('LOTE_CARGUE_ID',F.lit(LOT).cast('bigint')).write.mode('errorifexists').option('compression','snappy').parquet(path)
   n=spark.read.parquet(path).count()
   commit={'grupo':g,'filas':n,'ruta':path,'codigo':code,'lote':LOT,'llaves_hash':hashlib.sha256(json.dumps(manifest,sort_keys=True).encode()).hexdigest(),'segundos':round(time.monotonic()-start,3)}
   put(commit_key,commit)
   return g,commit
  commits={}
  # Ventana acotada: 20 consultas, no miles de futures ni millones de llaves en memoria.
  for first in range(0,plan['grupos'],20):
   with ThreadPoolExecutor(max_workers=20) as pool:
    futures=[pool.submit(extract,g) for g in range(first,min(first+20,plan['grupos']))]
    for f in as_completed(futures):
     g,c=f.result();commits[g]=c
     rt['checkpoint_extraccion']['grupos_confirmados']=len(commits);save()
     print('GRUPO_CONFIRMADO',name,g,c['filas'],'total',len(commits),'de',plan['grupos'],flush=True)
  paths=[commits[g]['ruta'] for g in range(plan['grupos'])]
  # Consolidacion S3, sin nueva consulta Oracle. Se rehace solo esta fase si se corta.
  output='s3://alfa-datalake-dev-bronce/'+folder+'dataset/intento='+uuid.uuid4().hex+'/'
  df=spark.read.parquet(*paths).select(*t['columnas'],'LOTE_CARGUE_ID').withColumn('NRO_REGISTRO',F.monotonically_increasing_id().cast('bigint'))
  df.write.mode('errorifexists').option('compression','snappy').parquet(output)
  n=spark.read.parquet(output).count()
  if n!=sum(c['filas'] for c in commits.values()):raise ValueError('Conteo consolidado diferente')
  route=t['schema'].lower()+'/acsele_'+t['schema'].lower()+'_'+t['tabla'].lower()
  record={'estado':'SUCCESS','sistema_origen':cfg['fuente']['sistema'].lower(),'esquema':t['schema'].lower(),'tabla':t['tabla'].lower(),'tabla_relativa':name,'tabla_catalogo':route.split('/')[-1],'ruta_catalogable':route,'ruta_dataset_origen':output,'ruta_dataset_destino':'s3://alfa-datalake-dev-silver/'+folder+'silver/','tipo_tabla':t['tipo_tabla'],'lote_cargue_id_dataset':LOT,'lotes_preparacion_silver':[LOT],'datos_nuevos':n>0,'datos_nuevos_para_flujo':n>0,'requiere_validacion_tecnica':n>0,'requiere_disponibilizacion_silver':n>0,'requiere_preparacion_silver':True,'total_registros':n,'total_columnas':len(t['columnas']),'unidades_extraccion':n*len(t['columnas']),'segmentacion_horizontal_habilitada':False,'segmentacion_horizontal':{'habilitada':False},'modo_disponibilidad':'EXTRAIDA','escritura_bronze':'SUCCESS','modo_ejecucion':'INICIAL','plan_grupos':'s3://'+B+'/'+folder+'plan.json'}
  rt['datasets_extraidos'].append(record);idx[name]=record;rt.setdefault('lotes_usados_preparacion_silver',{})[name]=[LOT];save()
 if not allowed.issubset(idx):raise ValueError('Rama incompleta')
 rt['estado_extraccion']='SUCCESS_RAMA_'+mode.upper();save();job.commit()
except Exception as exc:
 rt['estado_extraccion']='FAILED';rt['error_extraccion']=str(exc);save();raise
