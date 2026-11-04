"""
================================================================================
        JOB DE LIMPIEZA - ALFAPRD MVP2 SILVER - PERÍODO 202507
================================================================================
MODO: ELIMINACIÓN REAL
PERÍODO: yyyy=2025, mm=07
ROLLBACK: Disponible (VACUUM deshabilitado)
================================================================================
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col
from datetime import datetime

# Parámetros
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'BUCKET_SILVER',
    'YYYY',
    'MM'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

BUCKET_SILVER = args['BUCKET_SILVER']
YYYY = args['YYYY']
MM = args['MM']

PATH_CLIENTES = f"s3://{BUCKET_SILVER}/POLIZAS_PRODUCTO/ALFAPRD/ALFAPRD_CLIENTES_STG/"
PATH_POLIZAS = f"s3://{BUCKET_SILVER}/POLIZAS_PRODUCTO/ALFAPRD/ALFAPRD_POLIZAS_STG/"

print("=" * 80)
print("LIMPIEZA ALFAPRD SILVER - ELIMINACIÓN REAL")
print("=" * 80)
print(f"Bucket: {BUCKET_SILVER}")
print(f"Período a eliminar: yyyy={YYYY}, mm={MM}")
print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 80)

try:
    from delta.tables import DeltaTable

    # ========================================================================
    # PASO 1: ANÁLISIS PRE-ELIMINACIÓN
    # ========================================================================
    print(f"\nPASO 1: ANÁLISIS PRE-ELIMINACIÓN")
    print("=" * 80)

    # CLIENTES
    df_clientes = spark.read.format("delta").load(PATH_CLIENTES)
    total_clientes = df_clientes.count()
    clientes_periodo = df_clientes.filter(f"yyyy = '{YYYY}' AND mm = '{MM}'").count()

    print(f"\nCLIENTES_ALFAPRD_SILVER:")
    print(f"  Total registros: {total_clientes:,}")
    print(f"  Registros yyyy={YYYY}, mm={MM}: {clientes_periodo:,}")
    print(f"  Otros períodos: {total_clientes - clientes_periodo:,}")

    # POLIZAS
    df_polizas = spark.read.format("delta").load(PATH_POLIZAS)
    total_polizas = df_polizas.count()
    polizas_periodo = df_polizas.filter(f"yyyy = '{YYYY}' AND mm = '{MM}'").count()

    print(f"\nPOLIZAS_ALFAPRD_SILVER:")
    print(f"  Total registros: {total_polizas:,}")
    print(f"  Registros yyyy={YYYY}, mm={MM}: {polizas_periodo:,}")
    print(f"  Otros períodos: {total_polizas - polizas_periodo:,}")

    # Distribución por período
    print(f"\nDistribución CLIENTES por período:")
    df_clientes.groupBy("yyyy", "mm").count().orderBy("yyyy", "mm").show(20, False)

    print(f"\nDistribución POLIZAS por período:")
    df_polizas.groupBy("yyyy", "mm").count().orderBy("yyyy", "mm").show(20, False)

    # ========================================================================
    # PASO 2: HISTORIAL DELTA ANTES DE ELIMINAR
    # ========================================================================
    print(f"\nPASO 2: HISTORIAL DELTA (ANTES DE ELIMINAR)")
    print("=" * 80)

    delta_clientes = DeltaTable.forPath(spark, PATH_CLIENTES)
    delta_polizas = DeltaTable.forPath(spark, PATH_POLIZAS)

    print(f"\nHistorial CLIENTES:")
    delta_clientes.history(5).select("version", "timestamp", "operation").show(5, False)

    print(f"\nHistorial POLIZAS:")
    delta_polizas.history(5).select("version", "timestamp", "operation").show(5, False)

    version_clientes_before = delta_clientes.history().first()['version']
    version_polizas_before = delta_polizas.history().first()['version']

    print(f"\nVersión actual CLIENTES: {version_clientes_before}")
    print(f"Versión actual POLIZAS: {version_polizas_before}")

    # ========================================================================
    # PASO 3: ELIMINACIÓN
    # ========================================================================
    print(f"\nPASO 3: EJECUTANDO ELIMINACIÓN")
    print("=" * 80)
    print(f"\n🔴 ELIMINANDO REGISTROS...")

    # ELIMINAR CLIENTES
    print(f"\nEliminando CLIENTES donde yyyy='{YYYY}' AND mm='{MM}'...")
    delta_clientes.delete(f"yyyy = '{YYYY}' AND mm = '{MM}'")
    print(f"✅ CLIENTES eliminados")

    # ELIMINAR POLIZAS
    print(f"\nEliminando POLIZAS donde yyyy='{YYYY}' AND mm='{MM}'...")
    delta_polizas.delete(f"yyyy = '{YYYY}' AND mm = '{MM}'")
    print(f"✅ POLIZAS eliminadas")

    # ========================================================================
    # PASO 4: VERIFICACIÓN POST-ELIMINACIÓN
    # ========================================================================
    print(f"\nPASO 4: VERIFICACIÓN POST-ELIMINACIÓN")
    print("=" * 80)

    # CLIENTES después
    df_clientes_after = spark.read.format("delta").load(PATH_CLIENTES)
    total_clientes_after = df_clientes_after.count()
    clientes_periodo_after = df_clientes_after.filter(f"yyyy = '{YYYY}' AND mm = '{MM}'").count()

    print(f"\nCLIENTES_ALFAPRD_SILVER (DESPUÉS):")
    print(f"  Total registros: {total_clientes_after:,}")
    print(f"  Registros yyyy={YYYY}, mm={MM}: {clientes_periodo_after:,}")
    print(f"  Eliminados: {clientes_periodo:,}")

    # POLIZAS después
    df_polizas_after = spark.read.format("delta").load(PATH_POLIZAS)
    total_polizas_after = df_polizas_after.count()
    polizas_periodo_after = df_polizas_after.filter(f"yyyy = '{YYYY}' AND mm = '{MM}'").count()

    print(f"\nPOLIZAS_ALFAPRD_SILVER (DESPUÉS):")
    print(f"  Total registros: {total_polizas_after:,}")
    print(f"  Registros yyyy={YYYY}, mm={MM}: {polizas_periodo_after:,}")
    print(f"  Eliminados: {polizas_periodo:,}")

    # Validación
    if clientes_periodo_after > 0:
        print(f"\n⚠️  WARNING: Aún quedan {clientes_periodo_after:,} registros de CLIENTES del período")
    else:
        print(f"\n✅ CLIENTES del período eliminados completamente")

    if polizas_periodo_after > 0:
        print(f"⚠️  WARNING: Aún quedan {polizas_periodo_after:,} registros de POLIZAS del período")
    else:
        print(f"✅ POLIZAS del período eliminadas completamente")

    # ========================================================================
    # PASO 5: OPTIMIZE (sin VACUUM para permitir rollback)
    # ========================================================================
    print(f"\nPASO 5: OPTIMIZACIÓN (COMPACTACIÓN DE ARCHIVOS)")
    print("=" * 80)

    print(f"\nOptimizando CLIENTES...")
    delta_clientes.optimize().executeCompaction()
    print(f"✅ CLIENTES optimizado")

    print(f"\nOptimizando POLIZAS...")
    delta_polizas.optimize().executeCompaction()
    print(f"✅ POLIZAS optimizado")

    # ========================================================================
    # PASO 6: HISTORIAL DELTA DESPUÉS DE ELIMINAR
    # ========================================================================
    print(f"\nPASO 6: HISTORIAL DELTA (DESPUÉS DE ELIMINAR)")
    print("=" * 80)

    print(f"\nHistorial CLIENTES:")
    delta_clientes.history(5).select("version", "timestamp", "operation", "operationMetrics").show(5, False)

    print(f"\nHistorial POLIZAS:")
    delta_polizas.history(5).select("version", "timestamp", "operation", "operationMetrics").show(5, False)

    version_clientes_after = delta_clientes.history().first()['version']
    version_polizas_after = delta_polizas.history().first()['version']

    print(f"\nVersión nueva CLIENTES: {version_clientes_after}")
    print(f"Versión nueva POLIZAS: {version_polizas_after}")

    # ========================================================================
    # RESUMEN FINAL
    # ========================================================================
    print(f"\n{'=' * 80}")
    print(f"RESUMEN FINAL")
    print(f"{'=' * 80}")

    print(f"\nPERÍODO ELIMINADO: yyyy={YYYY}, mm={MM}")

    print(f"\nCLIENTES:")
    print(f"  Antes:     {total_clientes:,} registros")
    print(f"  Después:   {total_clientes_after:,} registros")
    print(f"  Eliminados: {clientes_periodo:,} registros")

    print(f"\nPOLIZAS:")
    print(f"  Antes:     {total_polizas:,} registros")
    print(f"  Después:   {total_polizas_after:,} registros")
    print(f"  Eliminados: {polizas_periodo:,} registros")

    print(f"\n{'=' * 80}")
    print(f"ROLLBACK DISPONIBLE")
    print(f"{'=' * 80}")
    print(f"\nSi necesitas revertir la eliminación, ejecuta:")
    print(f"\n-- Restaurar CLIENTES")
    print(f"RESTORE TABLE delta.`{PATH_CLIENTES}` TO VERSION AS OF {version_clientes_before};")
    print(f"\n-- Restaurar POLIZAS")
    print(f"RESTORE TABLE delta.`{PATH_POLIZAS}` TO VERSION AS OF {version_polizas_before};")

    print(f"\n{'=' * 80}")
    print(f"✅ JOB COMPLETADO EXITOSAMENTE")
    print(f"{'=' * 80}")

    job.commit()

except Exception as e:
    print(f"\n{'=' * 80}")
    print(f"❌ ERROR EN EL JOB")
    print(f"{'=' * 80}")
    print(f"\nError: {str(e)}")
    import traceback
    print(f"\nTraceback completo:")
    print(traceback.format_exc())
    raise