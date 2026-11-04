
import sys, json, hashlib, re
from datetime import datetime, timezone
import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
A=getResolvedOptions(sys.argv,['ROOT_KEY','SQL_SHA'])
s=boto3.Session(region_name='us-east-1')
if s.client('sts').get_caller_identity()['Account']!='533267035849': raise ValueError('Solo DEV')
s3=s.client('s3'); B='alfa-datalake-dev-assets'; root=A['ROOT_KEY']
if not re.fullmatch(r'pruebas/pcr/vida/periodo=202606/e1_mov_conteo/ejecucion=[a-f0-9]{32}/',root): raise ValueError('Ruta invalida')
r={'estado':'IN_PROGRESS','fase':'INICIANDO','periodo':'202606','conexion':'datalake-dev-acsele-sop-connection','inicio_utc':datetime.now(timezone.utc).isoformat(),'alcance':'Conteo mensual de E1_MOV original para junio 2026. Solo lectura; no guarda detalle.'}
def save(): s3.put_object(Bucket=B,Key=root+'resultado.json',Body=json.dumps(r,ensure_ascii=False,indent=2).encode(),ContentType='application/json')
conn=st=rs=None
try:
 save(); body=s3.get_object(Bucket=B,Key=root+'consulta.sql')['Body']
 try: raw=body.read()
 finally: body.close()
 if hashlib.sha256(raw).hexdigest()!=A['SQL_SHA']: raise ValueError('SQL modificado')
 q=raw.decode('utf-8')
 clean=re.sub(r'/\*.*?\*/|--[^\n]*',' ',q,flags=re.S)
 if ';' in q or re.search(r'\b(CREATE|DROP|COMMIT|INSERT|UPDATE|DELETE|MERGE|ALTER|TRUNCATE)\b',clean,re.I): raise ValueError('Consulta no autorizada')
 gc=GlueContext(SparkContext.getOrCreate()); jvm=gc.spark_session.sparkContext._gateway.jvm
 c=gc.extract_jdbc_conf('datalake-dev-acsele-sop-connection'); url=c.get('fullUrl') or c.get('url')
 if not url or not url.startswith('jdbc:oracle:'): raise ValueError('Conexion no Oracle')
 props=jvm.java.util.Properties(); props.setProperty('user',c['user']); props.setProperty('password',c['password']); props.setProperty('oracle.net.CONNECT_TIMEOUT','30000'); props.setProperty('oracle.jdbc.ReadTimeout','3720000')
 conn=jvm.oracle.jdbc.OracleDriver().connect(url,props); conn.setReadOnly(True)
 r['fase']='CONSULTANDO_ORACLE'; save(); print('CONSULTANDO E1_MOV JUNIO 2026',flush=True)
 st=conn.createStatement(); st.setQueryTimeout(3600); rs=st.executeQuery(q)
 if not rs.next(): raise ValueError('Consulta sin fila de respuesta')
 filas=rs.getLong('FILAS'); operaciones=rs.getLong('OPERACIONES')
 if rs.next(): raise ValueError('Consulta devolvio mas de una fila')
 r.update({'estado':'SUCCESS','fase':'COMPLETO','filas':filas,'operaciones':operaciones,'fin_utc':datetime.now(timezone.utc).isoformat()}); save(); print('RESULTADO',filas,operaciones,flush=True)
except Exception as exc:
 r.update({'estado':'FAILED','error_tipo':type(exc).__name__,'error':str(exc)[:1800]}); save(); raise
finally:
 if rs is not None: rs.close()
 if st is not None: st.close()
 if conn is not None: conn.close()
