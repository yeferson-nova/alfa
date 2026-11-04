
import sys,json,boto3
from datetime import datetime,timezone
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
args=getResolvedOptions(sys.argv,['JOB_NAME','PERIODO','TOPE','RESULT_BUCKET','RESULT_KEY'])
session=boto3.Session(region_name='us-east-1')
if session.client('sts').get_caller_identity()['Account']!='533267035849':
    raise ValueError('Cuenta distinta de DEV')
if args['RESULT_BUCKET']!='alfa-datalake-dev-assets' or not args['RESULT_KEY'].startswith('pruebas/pcr/diagnostico-volumen/resultados/'):
    raise ValueError('Ruta de resultado no autorizada por el script')
fecha=datetime.strptime(args['PERIODO'],'%Y%m')
if fecha.strftime('%Y%m')!=args['PERIODO']:raise ValueError('Periodo invalido')
tope=int(args['TOPE'])
if not 1<=tope<=100000:raise ValueError('Tope fuera de 1..100000')
desde=fecha.strftime('%Y-%m-01')
hasta=datetime(fecha.year+int(fecha.month==12),fecha.month%12+1,1).strftime('%Y-%m-%d')
s3=session.client('s3')
result={'estado':'RUNNING','periodo':args['PERIODO'],'tope':tope,
    'alcance':'CONTEXTOPERATION STATUS=2; sin producto/compania; no es conteo exacto de Vida ni dependencias',
    'inicio_utc':datetime.now(timezone.utc).isoformat()}
def guardar():
    s3.put_object(Bucket=args['RESULT_BUCKET'],Key=args['RESULT_KEY'],
        Body=json.dumps(result,ensure_ascii=False,indent=2).encode(),ContentType='application/json')
# Si falla esta escritura no se envia ninguna consulta Oracle.
guardar()
try:
    gc=GlueContext(SparkContext.getOrCreate())
    job=Job(gc);job.init(args['JOB_NAME'],args)
    sql=("SELECT COUNT(*) AS N FROM (SELECT 1 FROM ACSELE_ALFA.CONTEXTOPERATION "
        "WHERE STATUS=2 AND TIME_STAMP >= DATE '"+desde+"' "
        "AND TIME_STAMP < DATE '"+hasta+"' AND ROWNUM <= "+str(tope+1)+")")
    print('INICIO CONTEO ACOTADO',args['PERIODO'],flush=True)
    df=gc.create_dynamic_frame.from_options(connection_type='oracle',connection_options={
        'useConnectionProperties':'true','connectionName':'datalake-dev-acsele-sop-connection',
        'dbtable':'ACSELE_ALFA.CONTEXTOPERATION','sampleQuery':sql}).toDF()
    rows=df.collect()
    if len(rows)!=1 or rows[0]['N'] is None:raise ValueError('Resultado no interpretable')
    n=int(rows[0]['N'])
    result.update(estado='SUCCESS',filas_observadas_hasta_tope_mas_uno=n,
        conteo_exacto=n<=tope,total_operaciones=n if n<=tope else None,
        minimo_operaciones=n,supera_tope=n>tope,
        fin_utc=datetime.now(timezone.utc).isoformat())
    guardar();print(json.dumps(result,ensure_ascii=False),flush=True);job.commit()
except Exception as exc:
    result.update(estado='FAILED',error_tipo=type(exc).__name__,
        fin_utc=datetime.now(timezone.utc).isoformat())
    try:guardar()
    except Exception as persist_error:print('NO SE PUDO GUARDAR ERROR:',type(persist_error).__name__)
    raise
