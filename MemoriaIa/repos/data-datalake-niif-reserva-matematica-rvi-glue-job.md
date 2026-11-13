---
tipo: repo
estado: vigente
actualizado: 2026-11-12
repo: data-datalake-niif-reserva-matematica-rvi-glue-job
categoria: glue
paso: "2 · Silver (preparación)"
malla: reserva-matematica-rvi
---
# data-datalake-niif-reserva-matematica-rvi-glue-job
> Job Silver de Reserva Matemática RVI (rentas vitalicias): prepara pensiones, movimientos de pago y beneficiarios por periodo de corte.

## Rol en el flujo
- Capa Silver (preparación) del paso 2. Lo lanza la Step Function `niif-{env}-reserva-matematica-rvi` (job `niif-{env}-silver-reserva-matematica-rvi`, nombre por confirmar).
- Entrega a Gold SCD (job genérico). Se certifica junto con ARP (grupo compuesto).

## Entradas y salidas
- Entra: runtime `RUTA_PARAMETROS_EXTRACCION`, `S3_SLV_PREP_PATH`, `S3_TMP_PREP_PATH` y `PER_CORTE` (AAAAMM). Lee pensiones, distribución de vigencia, movimientos de pago, conceptos de pago, novedades, personas naturales, IPC y catálogos.
- Sale: Parquet de Silver preparada por `PER_CORTE` (overwrite) y runtime con `silver_preparaciones` por corte.
- Lógica propia: doble cruce de cálculos (`CRV_CALCULOS_2`, con `NID_CALCULO_2` trazable), IPC con left join y regla histórica común (versión interna "v2.3").

## Recursos AWS que toca
- S3 `niif-{env}-assets` (config y runtime), buckets Silver base, temporal y preparada. Sin DynamoDB ni Athena.
- Se despliega a `glue-resources/reserva-matematicas-rvi` (Jenkins).

## Código
- `src/jobs/silver_reserva_matematica_rvi.py`: script único de ~1250 líneas, ejecutado al importarse.
- Sin `tests/`; `tox.ini` presente (no hay `tox.init` como en otros repos). README solo describe el job.

## Estado
- En Alfa: `origin/quality`, commit a3661a2 (2026-10-09). Desplegado: por confirmar.
- `quality` frente a `main` (medido en GitHub el 2026-11-12): 0 commits solo en `quality`, 3 solo en `main`, 0 archivos distintos; mismo contenido que `main` (los commits de diferencia son merges); la base `quality` sirve. Ver ARQ-50 en [[auditoria-repos-2026-11]].
- Código nuevo nuestro: ninguno.

## Brechas y relacionadas
- Joins sin cuadre y `dropDuplicates` sin orden en Silver: ARQ-05 en [[auditoria-pipeline-2026-11]].
- Relacionado: [[familia-mallas-alfaprd]], [[arquitectura-niif17]], [[repo-alfa-repos]]
