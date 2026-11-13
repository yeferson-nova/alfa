---
tipo: repo
estado: vigente
actualizado: 2026-11-12
repo: data-datalake-niif-primas-previsional-glue-job
categoria: glue
paso: "2 · Bronce (gestor de archivos) y Gold SCD"
malla: transversal
---
# data-datalake-niif-primas-previsional-glue-job
> Caso Primas Previsional: carga un Excel de cesión desde landing a Bronce con validaciones y luego versiona en Gold con SCD. No viene de Oracle.

## Rol en el flujo
- Paso 2 con fuente no estructurada (archivo `NIIF17_PRIMAS_PREVISIONAL`, fila "fuente no estructurada" del diagrama). Fuera de las 6 mallas ALFAPRD. Lo orquesta su propia Step Function (por confirmar).
- Entrega a la certificación genérica (usa otro JSON global y su propio JSON de caso en `previsional/`).

## Entradas y salidas
- Gestor: entra `USE_JSON_CONFIG`, `CONFIG_BUCKET`, `CONFIG_KEY`, `PERIOD` (y `FECHA_CARGA` opcional). Lee el Excel de la carpeta IN del bucket landing, valida existencia, nombre, extensión, hoja, estructura, orden de columnas, nulidad y tipos, acumulando todos los errores.
- Archivos inválidos se mueven a ERROR; los válidos pasan a OUT; escribe metadata y logs de validación; sale Parquet Bronce con columna de lote.
- SCD: entra `RUTA_CONFIG_SCD`, `FECHA_CARGA`, `LOTE_CARGUE_ID`; lee la tabla Bronce por el catálogo de Glue y versiona en Gold (cierra ausentes, vigencias, `ACT_REG`).

## Recursos AWS que toca
- Buckets landing `niif-{env}-landing`, Bronce, Silver (logs) y assets; catálogo Glue (tabla Bronce). Se despliega a `glue-resources/primas-previsional`.
- Depende de `pandas` y `openpyxl` (wheelhouse en `cfg-datalake-niif-config-s3`).

## Código
- `src/jobs/gestor_archivos_previsional.py` (~1640 líneas, con `main()`) y `src/jobs/scd_gestor_archivos_previsional.py` (~910, script al importarse). Se declara "job piloto". Sin `tests/`; README heredado de Endosos (no fiable).

## Estado
- En Alfa: `origin/quality`, commit 5871549 (2026-09-01). Desplegado: por confirmar.
- Código nuevo nuestro: ninguno.

## Brechas y relacionadas
- Comparte el job de certificación con otras mallas con otro JSON global (riesgo CER-H5): [[job-certificacion]].
- Plantilla de correo `previsional.html` en `tpl-datalake-niif-email-template-s3`.
- Relacionado: [[familia-mallas-alfaprd]], [[repo-alfa-repos]]
