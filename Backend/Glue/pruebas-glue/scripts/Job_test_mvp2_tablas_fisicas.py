import sys
import boto3
import time
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'DATABASE', 'OUTPUT_LOCATION'])

# ============================================================
# CONFIGURACIÓN - Viene de los parámetros del job
# ============================================================
DATABASE = args['DATABASE']
OUTPUT_LOCATION = args['OUTPUT_LOCATION']

# ============================================================
# CLIENTE ATHENA
# ============================================================
athena = boto3.client('athena')

def ejecutar_query(query, descripcion):
    """Ejecuta una query en Athena y espera a que termine."""
    print(f"▶ Ejecutando: {descripcion}")
    print(f"  Query: {query[:200]}...")
   
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': DATABASE},
        ResultConfiguration={'OutputLocation': OUTPUT_LOCATION}
    )
    query_id = response['QueryExecutionId']
    print(f"  Query ID: {query_id}")
   
    while True:
        result = athena.get_query_execution(QueryExecutionId=query_id)
        estado = result['QueryExecution']['Status']['State']
       
        if estado == 'SUCCEEDED':
            stats = result['QueryExecution'].get('Statistics', {})
            tiempo = stats.get('TotalExecutionTimeInMillis', 0) / 1000
            datos = stats.get('DataScannedInBytes', 0) / (1024 * 1024)
            print(f"  ✅ OK - {tiempo:.1f}s - {datos:.1f} MB escaneados")
            return True
           
        elif estado in ('FAILED', 'CANCELLED'):
            razon = result['QueryExecution']['Status'].get('StateChangeReason', 'Sin detalle')
            print(f"  ❌ FALLÓ: {razon}")
            raise Exception(f"Query falló [{descripcion}]: {razon}")
       
        time.sleep(5)


# ============================================================
# PASO 1: RECREAR SINIESTROS ULTIMO MOVIMIENTO
# ============================================================
ejecutar_query(
    "DROP TABLE IF EXISTS gold_siniestros_ultimo_movimiento",
    "Drop tabla siniestros_ultimo_movimiento"
)

ejecutar_query(f"""
    CREATE TABLE gold_siniestros_ultimo_movimiento
    WITH (format = 'PARQUET', write_compression = 'SNAPPY')
    AS
    SELECT
        id_siniestro_fk,
        SPLIT_PART(id_poliza_pk, '-', 1) id_poliza_clean,
        "Fecha de aviso"                  fecha_aviso,
        fecha_ocurrencia,
        CEDULA                            numero_documento_asegurado,
        valor_transaccion,
        id_fuente_fk
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY id_fuente_fk, SPLIT_PART(id_poliza_pk, '-', 1), CEDULA
                ORDER BY "Fecha de Transaccion" DESC, id_siniestro_fk DESC
            ) rn
        FROM "{DATABASE}".gold_caract_siniestros_fact_movimiento_view
    ) s
    WHERE s.rn = 1
""",
    "Crear tabla siniestros_ultimo_movimiento"
)


# ============================================================
# PASO 2: RECREAR DETALLE COMPLETO CLIENTES
# ============================================================
ejecutar_query(
    "DROP TABLE IF EXISTS gold_clientes_detalle_completo",
    "Drop tabla clientes_detalle_completo"
)

ejecutar_query(f"""
    CREATE TABLE gold_clientes_detalle_completo
    WITH (format = 'PARQUET', write_compression = 'SNAPPY')
    AS
    WITH ultimos_3_periodos AS (
        SELECT DISTINCT id_periodo_calculo
        FROM "{DATABASE}".gold_caract_clientes_poliza_emision_view
        WHERE id_periodo_calculo IS NOT NULL
        ORDER BY id_periodo_calculo DESC
        LIMIT 1
    ),
    polizas_filtradas AS (
        SELECT pol.*
        FROM "{DATABASE}".gold_caract_clientes_poliza_emision_view pol
        INNER JOIN ultimos_3_periodos u3
            ON pol.id_periodo_calculo = u3.id_periodo_calculo
    ),
    rol_unico AS (
        SELECT
            r.id_poliza,
            r.id_cliente,
            r.rol,
            r.nombre_tomador,
            r.estado_cliente
        FROM "{DATABASE}".gold_caract_clientes_rol_cliente_estado_view r
        GROUP BY r.id_poliza, r.id_cliente, r.rol, r.nombre_tomador, r.estado_cliente
    )
    SELECT
        fc.nombre_completo,
        fc.numero_documento,
        dtdg.descripcion                     tipo_documento,
        dtpg.descripcion                     tipo_persona,
        rol.rol,
        rol.nombre_tomador,
        rol.estado_cliente,
        pol.estado_poliza,
        pol.numero_poliza,
        pol.numero_poliza_matriz,
        pol.codigo_producto,
        pol.descripcion_producto,
        pol.tipo_poliza,
        pol.ramo,
        pol.unidad_negocio,
        pol.fecha_emision_poliza,
        pol.fecha_inicio_vigencia,
        pol.fecha_fin_vigencia,
        pol.fecha_cancelacion,
        pol.motivo_cancelacion,
        pol.canal_venta,
        peri.periodo                          periodo_calculo,
        pol.valor_asegurado_poliza,
        pol.valor_prima_poliza,
        pol.id                                id_poliza_fct,
        rol.id_cliente,
        fc.id                                 id_datos_cliente,
        pol.id_poliza,
        pol.id_fuente,
        sin.id_fuente_fk                      id_fuente_siniestro,
        sin.id_siniestro_fk                   nid_siniestro,
        sin.fecha_aviso                       siniestro_fecha_aviso,
        sin.fecha_ocurrencia                  siniestro_fecha_ocurrencia,
        sin.valor_transaccion                 siniestro_valor_ult_movimiento
    FROM polizas_filtradas pol
    LEFT JOIN "{DATABASE}".gold_caract_clientes_dim_dim_periodo_gold peri
        ON peri.id = pol.id_periodo_calculo
    LEFT JOIN rol_unico rol
        ON pol.id = rol.id_poliza
    LEFT JOIN "{DATABASE}".gold_caract_clientes_fact_cliente fc
        ON rol.id_cliente = fc.id
    LEFT JOIN "{DATABASE}".gold_caract_clientes_dim_dim_tipo_doc_gold dtdg
        ON fc.id_tipo_documento = dtdg.id
    LEFT JOIN "{DATABASE}".gold_caract_clientes_dim_dim_tipo_persona_gold dtpg
        ON fc.id_tipo_persona = dtpg.id
    LEFT JOIN "{DATABASE}".gold_siniestros_ultimo_movimiento sin
        ON  sin.id_poliza_clean = CAST(pol.id_poliza AS VARCHAR)
        AND sin.numero_documento_asegurado = fc.numero_documento
        AND rol.rol = 'Asegurado'
        AND (
                (pol.id_fuente IN ('2','3') AND sin.id_fuente_fk = '2')
             OR (pol.id_fuente = '1'        AND sin.id_fuente_fk = '1')
            )
""",
    "Crear tabla clientes_detalle_completo"
)


# ============================================================
# RESUMEN FINAL
# ============================================================
print("")
print("=" * 60)
print("✅ JOB COMPLETADO EXITOSAMENTE")
print("=" * 60)
print(f"Database: {DATABASE}")
print("Tablas recreadas:")
print("  - gold_siniestros_ultimo_movimiento")
print("  - gold_clientes_detalle_completo")
print("")
print("Consultá con:")
print("  SELECT * FROM gold_clientes_detalle_completo LIMIT 100;")
print("=" * 60)
