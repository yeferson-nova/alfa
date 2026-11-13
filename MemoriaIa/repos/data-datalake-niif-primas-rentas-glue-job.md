---
tipo: repo
estado: vigente
actualizado: 2026-11-12
repo: data-datalake-niif-primas-rentas-glue-job
categoria: glue
paso: "2 · Silver (preparación)"
malla: primas-rentas
---
# data-datalake-niif-primas-rentas-glue-job
> Job Silver de la malla Primas Rentas: arma la Silver preparada a partir de la Silver base. Es la única capa propia de la malla en este repo.

## Rol en el flujo
- Capa Silver (preparación) del paso 2. Lo lanza la Step Function `niif-{env}-primas-rentas` después de extracción, validación técnica y remediación (job `niif-{env}-silver-primas-rentas`, nombre exacto por confirmar en Terraform).
- Entrega a Gold SCD (job `scd` genérico, que vive en `data-datalake-niif-auxilios-funerarios-glue-job`).

## Entradas y salidas
- Entra: el JSON de runtime `parametros_extraccion.json` (argumento `RUTA_PARAMETROS_EXTRACCION`), la Silver base de rentas con cálculo, tipo y concepto de pago, y la ruta `S3_SLV_PREP_PATH`.
- Sale: Parquet de Silver preparada y el mismo runtime actualizado con `silver_preparacion`, lotes usados y estado de preparación.
- Exige `estado_remediacion` y `estado_disponibilizacion_silver` en SUCCESS; acepta validación en SUCCESS o FAILED (la validación no bloquea).

## Recursos AWS que toca
- S3 `niif-{env}-assets` (config y runtime), buckets Silver base y preparada; sin DynamoDB ni Athena en el código.
- Se despliega a `s3://niif-{env}-assets/glue-resources/primas-rentas` (Jenkins).

## Código
- `src/jobs/silver-primas-rentas.py`: script único de ~660 líneas ejecutado al importarse; el guion en el nombre impide importarlo como módulo.
- Lee solo los lotes de preparación de cada tabla (`lotes_segmento`), calcula periodos (`PER_*`) y escribe con overwrite.
- Sin pruebas: no hay carpeta `tests/`; `tox.ini` tiene umbral de cobertura 0. El README es copia de otro repo (habla de Endosos y de un job Bronce inexistente): no tomarlo como fuente.

## Estado
- En Alfa: `origin/quality`, commit 853c725 (2026-10-22). Desplegado: por confirmar.
- `quality` frente a `main` (medido en GitHub el 2026-11-12): 0 commits solo en `quality`, 3 solo en `main`, 0 archivos distintos; mismo contenido que `main` (los commits de diferencia son merges); la base `quality` sirve. Ver ARQ-50 en [[auditoria-repos-2026-11]].
- Código nuevo nuestro: ninguno.

## Brechas y relacionadas
- **ARQ-46** el README del repo es copia de otro y no lo describe, en [[auditoria-repos-2026-11]].
- **ARQ-47** sin pruebas en el repo, en [[auditoria-repos-2026-11]].
- Silver con joins sin cuadre y guarda de 0 filas parcial: ARQ-04 y ARQ-05 en [[auditoria-pipeline-2026-11]].
- Relacionado: [[familia-mallas-alfaprd]], [[arquitectura-niif17]], [[repo-alfa-repos]], [[est-desarrollo-hexagonal]]
