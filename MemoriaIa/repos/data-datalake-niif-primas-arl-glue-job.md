---
tipo: repo
estado: vigente
actualizado: 2026-11-12
repo: data-datalake-niif-primas-arl-glue-job
categoria: glue
paso: "2 · Bronce (extracción) y Silver (preparación)"
malla: primas-arl
---
# data-datalake-niif-primas-arl-glue-job
> Dos jobs propios de Primas ARL: un Bronce específico (extracción simple de 8 tablas Oracle) y el Silver preparado.

## Rol en el flujo
- Paso 2. `bronze_primas_arl.py` extrae de Oracle ALFAPRD; `silver_primas_arl.py` prepara la Silver. Los lanza la Step Function `niif-{env}-primas-arl`.
- Ojo: la extracción real de la malla es el job genérico `extraccion_estandar_optimized.py` (repo `data-datalake-niif-rmd-ext-estandar-glue-job`); este Bronce parece una versión previa. Por confirmar si algún job desplegado lo usa.

## Entradas y salidas
- Bronce: entra la conexión Glue a Oracle (`CONNECT_NAME`) y 8 tablas fijas en el código (esquemas ARPPROD y GENERAL: pólizas, endosos, afiliaciones, tipos de endoso, sucursales, personas). Sale Parquet Bronce y un runtime con estado IN_PROGRESS, SUCCESS o FAILED.
- Silver: entra el runtime con `par_endosos` y `par_afiliaciones`, más terceros y sucursal; sale Parquet preparado (prima, periodos de expedición y de proceso Point) y runtime con `silver_preparacion`.

## Recursos AWS que toca
- Conexión Glue a Oracle, S3 `niif-{env}-assets` y buckets Bronce/Silver. Se despliega a `glue-resources/primas-arl` (Jenkins).

## Código
- `src/jobs/bronze_primas_arl.py` (~390 líneas, con líneas en blanco intercaladas) y `src/jobs/silver_primas_arl.py` (~550). Scripts únicos ejecutados al importarse; sin `tests/`, cobertura mínima 0 en `tox.ini`. README heredado de Endosos, no fiable.

## Estado
- En Alfa: `origin/quality`, commit bc912f2 (2026-10-22). Desplegado: por confirmar.
- `quality` frente a `main` (medido en GitHub el 2026-11-12): 3 commits solo en `quality`, 1 solo en `main`; ramas divergentes. Ver ARQ-50 en [[auditoria-repos-2026-11]].
- Código nuevo nuestro: ninguno.

## Brechas y relacionadas
- Incidente en PRD: certificación Primas ARL 202606 vacía por extracción incremental con exclusión de llaves y SCD que apaga por ausencia, en [[inprod-primas-arl-202606]] (la causa está en los jobs genéricos, no en este Silver).
- ARQ-04 y ARQ-05 en [[auditoria-pipeline-2026-11]].
- Relacionado: [[familia-mallas-alfaprd]], [[repo-alfa-repos]]
