import sys, json, re, time, uuid, hashlib
from decimal import Decimal
from functools import reduce
from datetime import datetime, timezone
import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark import StorageLevel
B = 'alfa-datalake-dev-assets'
BASE = 'pruebas/pcr/vida/ejecuciones/d1e3c879db73414191cb1677cc187983/'
LOT = 1793024990
VERSION = 'DIRECTAS11_AGRUPADAS_V1'

def number(v):
    if isinstance(v, bool) or not isinstance(v, (int, Decimal)):
        raise ValueError('Llave no numerica exacta')
    d = Decimal(v)
    if not d.is_finite() or d != d.to_integral_value():
        raise ValueError('Llave no entera')
    return str(int(d))

def predicate(cols, rows):
    groups = []
    for start in range(0, len(rows), 900):
        part = rows[start:start + 900]
        if len(cols) == 1:
            groups.append(cols[0] + ' IN (' + ','.join((number(r[0]) for r in part)) + ')')
        else:
            groups.append('(' + ','.join(cols) + ') IN (' + ','.join(('(' + ','.join((number(v) for v in r)) + ')' for r in part)) + ')')
    return '(' + ' OR '.join(groups) + ')' if groups else '1=0'

def main():
    session = boto3.Session(region_name='us-east-1')
    if session.client('sts').get_caller_identity()['Account'] != '533267035849':
        raise ValueError('Solo DEV')
    a = getResolvedOptions(sys.argv, ['JOB_NAME'])
    if a['JOB_NAME'] != 'niif-dev-pcr-vida-rama7-aislada':
        raise ValueError('Job incorrecto')
    s3 = session.client('s3')

    def read(k):
        return json.loads(s3.get_object(Bucket=B, Key=BASE + k)['Body'].read())
    rt = read('rama7_vida_v1/parametros_extraccion.json')
    cfg = read('config_extraccion_periodo.json')
    digest = lambda x: hashlib.sha256(json.dumps(x, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()).hexdigest()
    if rt.get('lote_cargue_id') != LOT or rt.get('periodo_negocio') != '202606' or rt.get('config_checkpoint_hash') != digest(cfg):
        raise ValueError('Contrato diferente')
    if rt.get('estado_extraccion') not in ('FAILED', 'IN_PROGRESS', 'PAUSADO_CONTROLADO'):
        raise ValueError('Runtime no recuperable')
    code = hashlib.sha256(open(__file__, 'rb').read()).hexdigest()
    if rt.get('codigo_rama7') and rt['codigo_rama7'] != code:
        raise ValueError('Codigo rama diferente')
    rt['codigo_rama7'] = code
    tables = [t for c in sorted(cfg['ciclos'], key=lambda x: x['orden']) for seg in sorted(c['segmentos'], key=lambda x: x['orden']) for t in sorted(seg['tablas'], key=lambda x: x['orden'])]
    if len(tables) != 27 or len({t['tabla_relativa'] for t in tables}) != 27:
        raise ValueError('Inventario diferente')
    idx = {d['tabla_relativa']: d for d in rt['datasets_extraidos']}
    if len(idx) != len(rt['datasets_extraidos']) or not {t['tabla_relativa'] for t in tables[:10]}.issubset(idx):
        raise ValueError('Faltan tablas previas')
    for d in idx.values():
        if d['estado'] != 'SUCCESS' or d['lote_cargue_id_dataset'] != LOT or (not d['ruta_dataset_origen'].startswith('s3://alfa-datalake-dev-bronce/' + BASE)):
            raise ValueError('Dataset previo invalido')
    for t in tables:
        for x in [t['schema'], t['tabla']] + t['columnas']:
            if not re.fullmatch('[A-Za-z][A-Za-z0-9_]*', x):
                raise ValueError('Identificador invalido')
    gc = GlueContext(SparkContext.getOrCreate())
    spark = gc.spark_session
    job = Job(gc)
    job.init(a['JOB_NAME'], a)
    for key in ('spark.sql.parquet.datetimeRebaseModeInWrite', 'spark.sql.parquet.int96RebaseModeInWrite', 'spark.sql.parquet.datetimeRebaseModeInRead', 'spark.sql.parquet.int96RebaseModeInRead'):
        spark.conf.set(key, 'LEGACY')
    credentials = gc.extract_jdbc_conf(cfg['fuente']['connection_name'])
    url = credentials.get('fullUrl') or credentials.get('url')
    if not url or not url.startswith('jdbc:oracle:'):
        raise ValueError('Conexion no Oracle')
    props = {'user': credentials['user'], 'password': credentials['password'], 'driver': 'oracle.jdbc.OracleDriver', 'fetchsize': str(cfg['fuente']['fetchsize']), 'queryTimeout': '600', 'oracle.net.CONNECT_TIMEOUT': '30000', 'oracle.jdbc.ReadTimeout': '150000'}

    def save():
        s3.put_object(Bucket=B, Key=BASE + 'rama7_vida_v1/parametros_extraccion.json', Body=json.dumps(rt, ensure_ascii=False, indent=2).encode(), ContentType='application/json')
    _scope_cache = {}
    _table_index = {t['tabla_relativa']: t for t in tables}

    def _raw_parent(name):
        d = idx[name]
        if d['estado'] != 'SUCCESS' or d['lote_cargue_id_dataset'] != LOT:
            raise ValueError('Padre no confirmado')
        return spark.read.parquet(d['ruta_dataset_origen']).filter(F.col('LOTE_CARGUE_ID') == LOT)

    def _semi(child, parent, pairs):
        names = ['_vida_key_' + str(i) for i in range(len(pairs))]
        right = parent.select(*[F.col(p).alias(n) for (p, c), n in zip(pairs, names)]).distinct()
        condition = reduce(lambda a, b: a & b, [child[c] == right[n] for (p, c), n in zip(pairs, names)])
        return child.join(right, condition, 'left_semi')
    _prep = _raw_parent('acsele_alfa/preproduct').filter((F.col('STATIC') == F.lit(48897)) & (F.col('COMPANIAVALUE') == F.lit('50.0')))
    _ap = _semi(_raw_parent('acsele_alfa/agregatedpolicy'), _prep, [('STATIC', 'PRODUCTID')])
    _po = _semi(_raw_parent('acsele_alfa/policydco'), _ap, [('AGREGATEDPOLICYID', 'AGREGATEDOBJECTID')])
    _co = _semi(_raw_parent('acsele_alfa/contextoperation'), _po, [('OPERATIONPK', 'ID')]).persist(StorageLevel.DISK_ONLY)
    _scope_count = _co.count()
    if _scope_count == 0:
        raise ValueError('Universo Vida vacio; no se consulta Oracle')
    _scope_cache['acsele_alfa/contextoperation'] = _co
    _scope_cache['acsele_alfa/policydco'] = _semi(_po, _co, [('ID', 'OPERATIONPK')])
    _scope_cache['acsele_alfa/agregatedpolicy'] = _semi(_ap, _scope_cache['acsele_alfa/policydco'], [('AGREGATEDOBJECTID', 'AGREGATEDPOLICYID')])
    _scope_cache['acsele_alfa/preproduct'] = _prep
    print('UNIVERSO_VIDA_OPERACIONES', _scope_count, flush=True)
    rt['seleccion_vida'] = {'producto_static': 48897, 'companiavalue': '50.0', 'operaciones_pivote': _scope_count, 'origen': 'Bronze lote 1793024990', 'alcance': 'Solo seleccion de universo; calculos y filtros de roles permanecen en preparacion', 'advertencia': '19 datasets previos son supersets; pendientes restringidas. No certificacion ni snapshot Oracle.'}

    def scoped_parent(name):
        if name in _scope_cache:
            return _scope_cache[name]
        child = _raw_parent(name)
        deps = _table_index[name].get('dependencias_lectura', [])
        if not deps:
            raise ValueError('Padre sin relacion ni seleccion explicita')
        destinations = sorted((m['destino'] for m in deps[0]['mapeo']))
        keyframes = []
        for dep in deps:
            if sorted((m['destino'] for m in dep['mapeo'])) != destinations:
                raise ValueError('Destinos diferentes; no simplificar')
            mapping = {m['destino']: m['origen'] for m in dep['mapeo']}
            parent = scoped_parent(dep['tabla_origen'])
            keyframes.append(parent.select(*[F.col(mapping[d]).alias(d) for d in destinations]))
        keyframe = reduce(lambda a, b: a.unionByName(b), keyframes).distinct()
        selected = child.join(keyframe, destinations, 'left_semi')
        _scope_cache[name] = selected
        return selected
    rt.update(estado_extraccion='IN_PROGRESS', codigo_directas17=code, version_directas17=VERSION)
    rt.pop('error_extraccion', None)
    rt.pop('advertencia', None)
    save()
    try:
        for t in tables:
            if t['tabla_relativa'] not in ('acsele_alfa/stpo_insobjparticipationdco', 'acsele_alfa/stpo_insobjparticipation', 'acsele_alfa/thirdpartymovementpolicy', 'acsele_alfa/prethirdparty', 'acsele_alfa/thirdpartyrole', 'acsele_alfa/aseguradovida', 'acsele_alfa/stts_role'):
                continue
            name = t['tabla_relativa']
            if name in idx:
                print('REUTILIZA', name, flush=True)
                continue
            if t.get('where') is not None:
                raise ValueError('Filtro no soportado; no se ignora')
            deps = t['dependencias_lectura']
            cols = sorted((m['destino'] for m in deps[0]['mapeo']))
            frames = []
            if len(set(cols)) != len(cols):
                raise ValueError('Mapeo ambiguo')
            for dep in deps:
                if sorted((m['destino'] for m in dep['mapeo'])) != cols:
                    raise ValueError('Destinos incompatibles')
                parent = idx[dep['tabla_origen']]
                mapping = {m['destino']: m['origen'] for m in dep['mapeo']}
                df = scoped_parent(dep['tabla_origen'])
                frames.append(df.select(*[F.col(mapping[c]).alias(c) for c in cols]))
            keys = reduce(lambda x, y: x.unionByName(y), frames).filter(reduce(lambda x, y: x & y, [F.col(c).isNotNull() for c in cols])).distinct().persist(StorageLevel.DISK_ONLY)
            route = t['schema'].lower() + '/acsele_' + t['schema'].lower() + '_' + t['tabla'].lower()
            target = 's3://alfa-datalake-dev-bronce/' + 'pruebas/pcr/vida/ejecuciones/d1e3c879db73414191cb1677cc187983/rama7_vida_v1/' + route + '/directo_intento=' + uuid.uuid4().hex + '/'
            start = time.monotonic()
            writes = 0
            key_count = 0
            rt['checkpoint_extraccion'] = {'estado': 'IN_PROGRESS', 'tabla_actual': {'schema': t['schema'], 'tabla': t['tabla'], 'tabla_relativa': name}, 'estrategia': VERSION, 'grupos_escritos': 0}
            save()
            table = '(SELECT ' + ', '.join(t['columnas']) + ' FROM ' + t['schema'] + '.' + t['tabla'] + ') VIDA_SRC'

            def write(predicates):
                nonlocal writes
                df = spark.read.jdbc(url=url, table=table, predicates=predicates, properties=props)
                df = df.toDF(*[c.upper() for c in df.columns]).select(*t['columnas'])
                if writes >= 1 << 23:
                    raise ValueError('Numeracion tecnica excedida')
                df = df.withColumn('NRO_REGISTRO', (F.monotonically_increasing_id() + F.lit(writes * (1 << 40))).cast('bigint')).withColumn('LOTE_CARGUE_ID', F.lit(LOT).cast('bigint'))
                df.write.mode('errorifexists' if writes == 0 else 'append').option('compression', 'snappy').parquet(target)
                writes += 1
                rt['checkpoint_extraccion']['grupos_escritos'] = writes
                if writes == 1 or writes % 10 == 0:
                    save()
                print('AVANCE', name, writes, flush=True)
            try:
                batch = []
                pending = []
                capacity = 9000
                for row in keys.toLocalIterator(prefetchPartitions=False):
                    batch.append(tuple(row))
                    key_count += 1
                    if len(batch) >= capacity:
                        pending.append(predicate(cols, batch))
                        batch = []
                        if len(pending) == 2:
                            write(pending)
                            pending = []
                if batch:
                    pending.append(predicate(cols, batch))
                if pending:
                    write(pending)
                if not writes:
                    write(['1=0'])
            finally:
                keys.unpersist()
            check = spark.read.parquet(target).agg(F.count('*').alias('n'), F.count('LOTE_CARGUE_ID').alias('nl'), F.min('LOTE_CARGUE_ID').alias('lo'), F.max('LOTE_CARGUE_ID').alias('hi')).first()
            n = check['n']
            if check['nl'] != n or (n and (check['lo'] != LOT or check['hi'] != LOT)):
                raise ValueError('Lote incorrecto')
            record = {'estado': 'SUCCESS', 'sistema_origen': cfg['fuente']['sistema'].lower(), 'esquema': t['schema'].lower(), 'tabla': t['tabla'].lower(), 'tabla_relativa': name, 'tabla_catalogo': route.split('/')[-1], 'ruta_catalogable': route, 'ruta_dataset_origen': target, 'ruta_dataset_destino': 's3://alfa-datalake-dev-silver/' + 'pruebas/pcr/vida/ejecuciones/d1e3c879db73414191cb1677cc187983/rama7_vida_v1/' + 'base/' + route + '/', 'tipo_tabla': t['tipo_tabla'], 'lote_cargue_id_dataset': LOT, 'lotes_preparacion_silver': [LOT], 'datos_nuevos': n > 0, 'datos_nuevos_para_flujo': n > 0, 'requiere_validacion_tecnica': n > 0, 'requiere_disponibilizacion_silver': n > 0, 'requiere_preparacion_silver': True, 'total_registros': n, 'total_columnas': len(t['columnas']), 'unidades_extraccion': n * len(t['columnas']), 'segmentacion_horizontal_habilitada': False, 'segmentacion_horizontal': {'habilitada': False}, 'modo_disponibilidad': 'EXTRAIDA', 'escritura_bronze': 'SUCCESS', 'modo_ejecucion': 'INICIAL', 'duracion_segundos': round(time.monotonic() - start, 3), 'llaves_padre_distintas': key_count, 'grupos_escritos': writes, 'estrategia_lectura': VERSION}
            rt['datasets_extraidos'].append(record)
            idx[name] = record
            rt.setdefault('lotes_usados_preparacion_silver', {})[name] = [LOT]
            save()
            print('TABLA_SUCCESS', name, n, flush=True)
        if len(idx) != 26:
            raise ValueError('Inventario incompleto')
        rt.update(estado_extraccion='SUCCESS_RAMA7', hay_datos_nuevos=any((d['datos_nuevos'] for d in idx.values())), fecha_fin_extraccion=datetime.now(timezone.utc).isoformat())
        rt['estado_flujo_extraccion'] = 'SUCCESS_CON_DATOS_NUEVOS' if rt['hay_datos_nuevos'] else 'SUCCESS_SIN_DATOS_NUEVOS'
        rt['checkpoint_extraccion']['estado'] = 'SUCCESS'
        save()
        job.commit()
    except Exception as exc:
        rt.update(estado_extraccion='FAILED', error_extraccion=str(exc))
        save()
        raise
if __name__ == '__main__':
    main()
