---
tipo: repo
estado: vigente
actualizado: 2026-11-12
repo: data-datalake-niif-reserva-matematica-arp-glue-job
categoria: glue
paso: "2 · Silver (preparación)"
malla: reserva-matematica-arp
---
# data-datalake-niif-reserva-matematica-arp-glue-job
> Job Silver de Reserva Matemática ARP: prepara pensionados y beneficiarios con la lógica actuarial de la reserva, por periodo de corte.

## Rol en el flujo
- Capa Silver (preparación) del paso 2, sobre la Silver base ya disponibilizada. Lo lanza la Step Function `niif-{env}-reserva-matematica-arp` (job `niif-{env}-silver-reserva-matematica-arp`, nombre por confirmar).
- Entrega a Gold SCD (job genérico). ARP y RVI se certifican juntas (grupo compuesto Reserva Matemática RVI + ARP).

## Entradas y salidas
- Entra: runtime `RUTA_PARAMETROS_EXTRACCION`, `S3_SLV_PREP_PATH`, `S3_TMP_PREP_PATH` y `PER_CORTE` (AAAAMM, validado como numérico). Lee afiliados, pensiones, beneficiarios, personas naturales, parentescos, orígenes, IPC y estados.
- Sale: Parquet de Silver preparada por `PER_CORTE` (overwrite) y runtime con `silver_preparaciones` por corte; si la entrada funcional queda vacía lo confirma explícitamente.
- Calcula edades, fechas de corte, clasificación de beneficiarios (pivot, hijos, cónyuge) y rangos con ventanas de Spark.

## Recursos AWS que toca
- S3 `niif-{env}-assets` (config y runtime), buckets Silver base, temporal y preparada. Sin DynamoDB ni Athena.
- Se despliega a `glue-resources/reserva-matematicas-arp` (Jenkins).

## Código
- `src/jobs/silver_reserva_matematica_arp.py`: script único de ~1160 líneas, versión interna "v2.4" según su observación de runtime.
- Sin `tests/`; `tox.ini` presente, el README solo describe el job (sin estructura de repo).

## Estado
- En Alfa: `origin/quality`, commit e05da03 (2026-10-22). Desplegado: por confirmar.
- Código nuevo nuestro: ninguno. La tarea de validaciones de la reserva (T3) no vive aquí.

## Brechas y relacionadas
- Joins sin cuadre y `dropDuplicates` sin orden en Silver: ARQ-05 en [[auditoria-pipeline-2026-11]].
- Relacionado: [[familia-mallas-alfaprd]], [[arquitectura-niif17]], [[repo-alfa-repos]]
