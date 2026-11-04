import sys
import boto3
import time
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'DATABASE', 'OUTPUT_LOCATION'])

DATABASE = args['DATABASE']
OUTPUT_LOCATION = args['OUTPUT_LOCATION']

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
# PASO 1: RECREAR gold_caract_clientes_poliza_emision
# ============================================================
ejecutar_query(
    "DROP TABLE IF EXISTS gold_caract_clientes_poliza_emision",
    "Drop tabla gold_caract_clientes_poliza_emision"
)

ejecutar_query(f"""
    CREATE TABLE gold_caract_clientes_poliza_emision
    WITH (format = 'PARQUET', write_compression = 'SNAPPY')
    AS
    SELECT
        pol.id
      , pol.id_poliza
      , pol.id_fuente
      , pol.num_poliza                                                          numero_poliza
      , CAST(pol.fecha_efectiva_cancelacion AS DATE)                            fecha_efectiva_cancelacion
      , CAST(pol.fecha_cancelacion AS DATE)                                     fecha_cancelacion
      , pol.motivo_cancelacion
      , pol.numero_poliza_matriz
      , prod.codigo_producto
      , prod.descripcion_producto
      , prod.ramo
      , prod.unidad_negocio
      , prod.codigo_compania
      , prod.compania
      , tp_po.descripcion                                                        tipo_poliza
      , es_po.descripcion                                                        estado_poliza
      , li.descripcion                                                           id_linea_credito
      , canl.descripcion                                                         canal_venta
      , tp_ne.descripcion                                                        tipo_negocio
      , CAST(pol.fecha_emision_poliza AS DATE)                                   fecha_emision_poliza
      , CAST(pol.fecha_inicio_vigencia AS DATE)                                  fecha_inicio_vigencia
      , CAST(pol.fecha_fin_vigencia AS DATE)                                     fecha_fin_vigencia
      , CAST(pol.valor_asegurado AS DECIMAL(18, 4))                              valor_asegurado_poliza
      , CAST(pol.valor_prima AS DECIMAL(18, 4))                                  valor_prima_poliza
      , pol.id_fecha_emision
      , pol.id_fecha_inicio_vigencia
      , pol.id_fecha_fin_vigencia
      , pol.id_periodo_calculo
      , CAST(date_parse(concat(peri.periodo, '01'), '%Y%m%d') AS timestamp)     periodo_calculo
      , CAST(date_parse(NULLIF(peri.fe_inicial, ''), '%Y-%m-%d %H:%i:%s') AS DATE) fe_inicial_periodo_calculo
      , CAST(date_parse(NULLIF(peri.fe_final,   ''), '%Y-%m-%d %H:%i:%s') AS DATE) fe_final_periodo_calculo
      , peri.version                                                             version_periodo_calculo
      , peri.estado                                                              estado_periodo_calculo
      , rp.nombre_tomador
    FROM
        "{DATABASE}".gold_caract_clientes_fact_poliza_emision pol
    LEFT JOIN (
        SELECT
            pro.id
          , pro.codigo       codigo_producto
          , pro.descripcion  descripcion_producto
          , ra.descripcion   ramo
          , un.descripcion   unidad_negocio
          , pro.codigo_compania
          , pro.compania
        FROM "{DATABASE}".gold_caract_clientes_dim_producto pro
        LEFT JOIN "{DATABASE}".gold_caract_clientes_dim_dim_ramos_gold          ra ON ra.id = pro.id_ramo
        LEFT JOIN "{DATABASE}".gold_caract_clientes_dim_dim_unidad_negocio_gold un ON un.id = pro.id_unidad_negocio
    ) prod ON prod.id = pol.id_producto
    LEFT JOIN "{DATABASE}".gold_caract_clientes_dim_dim_tipo_poliza_gold   tp_po ON tp_po.id = pol.id_tipo
    LEFT JOIN "{DATABASE}".gold_caract_clientes_dim_dim_estado_poliza_gold es_po ON es_po.id = pol.id_estado
    LEFT JOIN "{DATABASE}".gold_caract_clientes_dim_dim_linea_credito_gold li    ON li.id    = pol.id_linea_credito
    LEFT JOIN "{DATABASE}".gold_caract_clientes_dim_dim_canal_venta_gold   canl  ON canl.id  = pol.id_canal_venta
    LEFT JOIN "{DATABASE}".gold_caract_clientes_dim_dim_tipo_negocio_gold  tp_ne ON tp_ne.id = pol.id_tipo_negocio
    LEFT JOIN "{DATABASE}".gold_caract_clientes_dim_dim_periodo_gold       peri  ON peri.id  = pol.id_periodo_calculo
    LEFT JOIN (
    SELECT
        cp.id_poliza,
        MAX(CASE WHEN cp.id_rol = 1 THEN fc.nombre_completo END) AS nombre_tomador,
        MAX(CASE WHEN cp.id_rol = 4 THEN fc.nombre_completo END) AS nombre_pagador
    FROM "{DATABASE}".gold_caract_clientes_fact_clientes_polizas cp
    LEFT JOIN "{DATABASE}".gold_caract_clientes_fact_cliente fc ON fc.id = cp.id_cliente
    WHERE cp.id_rol IN (1, 4)
    GROUP BY cp.id_poliza
) rp ON rp.id_poliza = pol.id

""",
    "Crear tabla gold_caract_clientes_poliza_emision"
)


# ============================================================
# PASO 2: RECREAR TABLA  gold_caract_clientes_rol_cliente_estado
# ============================================================
ejecutar_query(
    "DROP TABLE IF EXISTS gold_caract_clientes_rol_cliente_estado",
    "Drop  gold_caract_clientes_rol_cliente_estado"
)

ejecutar_query(f"""
    CREATE TABLE gold_caract_clientes_rol_cliente_estado
    WITH (format = 'PARQUET', write_compression = 'SNAPPY')
    AS
    WITH base_tomador AS (
        SELECT
            cli_po.id_cliente
          , cli_po.id_poliza
          , fact_cli.nombre_completo
        FROM "{DATABASE}".gold_caract_clientes_fact_clientes_polizas cli_po
        LEFT JOIN "{DATABASE}".gold_caract_clientes_fact_cliente fact_cli ON fact_cli.id = cli_po.id_cliente
        WHERE cli_po.id_rol = 1
    ),
    base_pagador AS (
        SELECT
            cli_po.id_cliente
          , cli_po.id_poliza
          , fact_cli.nombre_completo
        FROM "{DATABASE}".gold_caract_clientes_fact_clientes_polizas cli_po
        LEFT JOIN "{DATABASE}".gold_caract_clientes_fact_cliente fact_cli ON fact_cli.id = cli_po.id_cliente
        WHERE cli_po.id_rol = 4
    ),
    contacto_unico AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY id_cliente ORDER BY id DESC) AS rn_contacto
        FROM "{DATABASE}".gold_caract_clientes_fact_ubicacion_contactabilidad
        WHERE best_record = true
    )
    SELECT
        cli_po.id_cliente
      , cli_po.id_poliza
      , rol.descripcion                                                          AS rol
      , es_cli.descripcion                                                       AS estado_cliente
      , fact_cli.fecha_nacimiento
      , cli_po.id_fuente
      , tom.nombre_completo                                                      AS nombre_tomador
      , pag.nombre_completo                                                      AS nombre_pagador
      , dpaisg_ubi.descripcion                                                   AS pais_ubicacion
      , dciug_ubi.descripcion                                                    AS ciudad_ubicacion
      , ddepg_ubi.descripcion                                                    AS departamento_ubicacion
    FROM "{DATABASE}".gold_caract_clientes_fact_clientes_polizas cli_po
    LEFT JOIN "{DATABASE}".gold_caract_clientes_dim_dim_rol_gold            rol      ON rol.id    = cli_po.id_rol
    LEFT JOIN "{DATABASE}".gold_caract_clientes_dim_dim_estado_cliente_gold es_cli   ON es_cli.id = cli_po.id_estado
    LEFT JOIN (
        SELECT *,
            CASE
                WHEN COALESCE(fecha_nacimiento, DATE '1900-01-01') <= DATE '1901-01-01' THEN -1
                ELSE date_diff('year', fecha_nacimiento, current_date)
            END AS edad
        FROM "{DATABASE}".gold_caract_clientes_fact_cliente
    ) fact_cli ON fact_cli.id = cli_po.id_cliente
    LEFT JOIN "{DATABASE}".gold_caract_clientes_dim_dim_genero_gold         dgeng     ON dgeng.id    = fact_cli.id_genero
    LEFT JOIN contacto_unico                                                fuc       ON fuc.id_cliente = cli_po.id_cliente AND fuc.rn_contacto = 1
    LEFT JOIN "{DATABASE}".gold_caract_clientes_dim_dim_ciudad_gold         dciug_ubi ON dciug_ubi.id = fuc.id_ciudad
    LEFT JOIN "{DATABASE}".gold_caract_clientes_dim_dim_departamento_gold   ddepg_ubi ON dciug_ubi.departamento = ddepg_ubi.codigo
    LEFT JOIN "{DATABASE}".gold_caract_clientes_dim_dim_paises_gold         dpaisg_ubi ON CAST(fuc.id_pais AS integer) = dpaisg_ubi.id
    LEFT JOIN base_tomador tom ON tom.id_poliza = cli_po.id_poliza
    LEFT JOIN base_pagador pag ON pag.id_poliza = cli_po.id_poliza
""",
    "Crear tabla gold_caract_clientes_rol_cliente_estado"
)
 

# ============================================================
# PASO 3: RECREAR SINIESTROS ULTIMO MOVIMIENTO
# ============================================================
ejecutar_query(
    "DROP TABLE IF EXISTS gold_siniestros_ultimo_movimiento",
    "Drop tabla gold_siniestros_ultimo_movimiento"
)

ejecutar_query(f"""
    CREATE TABLE gold_siniestros_ultimo_movimiento
    WITH (format = 'PARQUET', write_compression = 'SNAPPY')
    AS
    SELECT
        id_siniestro_fk,
        SPLIT_PART(id_poliza_pk, '-', 1)  id_poliza_clean,
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
    "Crear tabla gold_siniestros_ultimo_movimiento"
)


# ============================================================
# PASO 4: RECREAR DETALLE COMPLETO CLIENTES
# ============================================================
ejecutar_query(
    "DROP TABLE IF EXISTS gold_clientes_detalle_completo",
    "Drop tabla gold_clientes_detalle_completo"
)

ejecutar_query(f"""
    CREATE TABLE gold_clientes_detalle_completo
    WITH (format = 'PARQUET', write_compression = 'SNAPPY')
    AS
    WITH rol_unico AS (
        SELECT
            r.id_poliza,
            r.id_cliente,
            r.rol,
            r.nombre_tomador,
            r.estado_cliente
        FROM "{DATABASE}".gold_caract_clientes_rol_cliente_estado r
        GROUP BY r.id_poliza, r.id_cliente, r.rol, r.nombre_tomador, r.estado_cliente
    ),
    detalle AS (
        SELECT
            fc.nombre_completo,
            fc.numero_documento,
            dtdg.descripcion                      tipo_documento,
            dtpg.descripcion                      tipo_persona,
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
            sin.valor_transaccion                 siniestro_valor_ult_movimiento,
            ROW_NUMBER() OVER (
                PARTITION BY pol.numero_poliza, pol.id_fuente, fc.numero_documento, rol.rol, pol.unidad_negocio
                ORDER BY pol.id_periodo_calculo DESC
            ) rn
        FROM "{DATABASE}".gold_caract_clientes_poliza_emision pol
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
    )
    SELECT
        nombre_completo,
        numero_documento,
        tipo_documento,
        tipo_persona,
        rol,
        nombre_tomador,
        estado_cliente,
        estado_poliza,
        numero_poliza,
        numero_poliza_matriz,
        codigo_producto,
        descripcion_producto,
        tipo_poliza,
        ramo,
        unidad_negocio,
        fecha_emision_poliza,
        fecha_inicio_vigencia,
        fecha_fin_vigencia,
        fecha_cancelacion,
        motivo_cancelacion,
        canal_venta,
        periodo_calculo,
        valor_asegurado_poliza,
        valor_prima_poliza,
        id_poliza_fct,
        id_cliente,
        id_datos_cliente,
        id_poliza,
        id_fuente,
        id_fuente_siniestro,
        nid_siniestro,
        siniestro_fecha_aviso,
        siniestro_fecha_ocurrencia,
        siniestro_valor_ult_movimiento
    FROM detalle
    WHERE rn = 1
""",
    "Crear tabla gold_clientes_detalle_completo"
)


# ============================================================
# PASO 5: RECREAR TABLA CLIENTES ÚNICOS
# ============================================================
ejecutar_query(
    "DROP TABLE IF EXISTS gold_caract_clientes_clientes_unicos",
    "Drop tabla clientes_unicos"
)

ejecutar_query(f"""
    CREATE TABLE gold_caract_clientes_clientes_unicos
    WITH (format = 'PARQUET', write_compression = 'SNAPPY')
    AS
    SELECT DISTINCT id id_cliente
    FROM
     "{DATABASE}"."gold_caract_clientes_fact_cliente"

""",
    "Crear tabla clientes_unicos"
)

# ============================================================
# RESUMEN FINAL
# ============================================================
print("")
print("=" * 60)
print("✅ JOB COMPLETADO EXITOSAMENTE")
print("=" * 60)
print(f"Database: {DATABASE}")
print("Tablas/Vistas recreadas en orden:")
print("  1. gold_caract_clientes_poliza_emision        (TABLA)")
print("  2. gold_caract_clientes_rol_cliente_estado (TABLA)")
print("  3. gold_siniestros_ultimo_movimiento           (TABLA)")
print("  4. gold_clientes_detalle_completo              (TABLA)")
print(" 5. gold_caract_clientes_clientes_unicos (TABLA)")
