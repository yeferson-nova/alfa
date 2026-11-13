---
tipo: repo
estado: vigente
actualizado: 2026-11-12
repo: data-datalake-niif-primas-endosos-glue-job
categoria: glue
paso: "2 · Silver (preparación)"
malla: primas-endosos
---
# data-datalake-niif-primas-endosos-glue-job
> Job Silver de la malla Primas Endosos: arma la Silver preparada de endosos de rentas. Es la única capa propia de la malla en este repo.

## Rol en el flujo
- Capa Silver (preparación) del paso 2. Lo lanza la Step Function `niif-{env}-primas-endosos` tras la remediación (job `niif-{env}-silver-primas-endosos`, nombre por confirmar en Terraform).
- Entrega a Gold SCD (job genérico en `data-datalake-niif-auxilios-funerarios-glue-job`). Rentas y Endosos se certifican juntas (grupo compuesto con ese par de casos en `cfg-datalake-niif-config-s3`).

## Entradas y salidas
- Entra: `parametros_extraccion.json` (`RUTA_PARAMETROS_EXTRACCION`), Silver base de endosos (tipo de endoso, renta, endoso reversado o reversión, devolución, ramo POINT) y `S3_SLV_PREP_PATH`.
- Sale: Parquet de Silver preparada y runtime actualizado (`silver_preparacion`, lotes usados). Usa `collect_set` y `struct` para agrupar reversiones.
- Mismas guardas de estado que las otras Silver: remediación y disponibilización en SUCCESS.

## Recursos AWS que toca
- S3 `niif-{env}-assets` (config y runtime) y buckets Silver base y preparada. Sin DynamoDB ni Athena.
- Se despliega a `s3://niif-{env}-assets/glue-resources/primas-endosos` (Jenkins).

## Código
- `src/jobs/silver_primas_endosos.py`: script único de ~555 líneas ejecutado al importarse, sin funciones de entrada.
- Sin `tests/`; `tox.ini` con cobertura mínima 0. El README es la plantilla heredada de Endosos y lista un job Bronce y pruebas que no existen en el repo.

## Estado
- En Alfa: `origin/quality`, commit da48cfc (2026-10-22). Desplegado: por confirmar.
- `quality` frente a `main` (medido en GitHub el 2026-11-12): 0 commits solo en `quality`, 4 solo en `main`; `main` tiene commits que `quality` no tiene (por revisar si son merges o cambios de PRD). Ver ARQ-50 en [[auditoria-repos-2026-11]].
- Código nuevo nuestro: ninguno.

## Brechas y relacionadas
- ARQ-04 y ARQ-05 (Silver sin guarda de 0 filas, joins sin cuadre) en [[auditoria-pipeline-2026-11]].
- Relacionado: [[familia-mallas-alfaprd]], [[arquitectura-niif17]], [[repo-alfa-repos]]
