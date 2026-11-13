---
tipo: repo
estado: vigente
actualizado: 2026-11-12
repo: data-datalake-niif-pcr-emisiones-glue-job
categoria: glue
paso: "2 · Bronce (extracción)"
malla: transversal
---
# data-datalake-niif-pcr-emisiones-glue-job
> Extracción Oracle a Bronce de la familia PCR (Vida y emisiones, origen ACSELE): variante ampliada del extractor estándar con lectura relacional por dependencias.

## Rol en el flujo
- Primer job del flujo PCR Vida (caso `pcr_vida`). Paso 2 del diagrama. No pertenece a las 6 mallas ALFAPRD.
- Entrega al resto de la cadena genérica y a `silver_vida_deudores.py` (repo `data-datalake-emisiones-vida-glue-job`).

## Entradas y salidas
- Entra: `RUTA_CONFIG_EXTRACCION` (el JSON `pcr_vida/config_extraccion_pcr_vida.json` de `cfg-datalake-niif-config-s3`, con fuente Oracle ACSELE y un ciclo de dependencias).
- Sale: Parquet Bronce por lote y `parametros_extraccion.json` con checkpoint y estado.

## Recursos AWS que toca
- Conexión Glue a Oracle, S3 `niif-{env}-assets` y buckets Bronce/Silver. Se despliega a `glue-resources/pcr-emisiones`.

## Código
- `src/jobs/extraccion_estandar_emisiones.py` (~3400 líneas): mismas funciones que `extraccion_estandar_optimized.py` más lectura JDBC particionada por `sampleQuery`, consultas hijas por llaves materializadas, validación de identificadores SQL y validación del motor de dependencias. Timeout de conexión mayor (120 s frente a 30 s).
- Parece una copia más antigua en un punto (la función `combinar_lotes_respaldados` está arriba y sin parámetro de tabla), por confirmar.
- Es el único glue de estos repos con carpeta `tests/`, pero solo trae `conftest.py` y paquetes vacíos. Incluye `Dockerfile`, `Makefile`, `run_local.sh`. README es plantilla inicial.

## Estado
- En Alfa: `origin/quality`, commit 900fe44 (2026-11-06). Desplegado: por confirmar.
- `quality` frente a `main` (medido en GitHub el 2026-11-12): 8 commits solo en `quality`, 0 solo en `main`; `main` no tiene lo de `quality`. Ver ARQ-50 en [[auditoria-repos-2026-11]].
- Código nuevo nuestro: ninguno.

## Brechas y relacionadas
- Misma exposición que el extractor estándar a exclusión de llaves y Silver parcial: [[inprod-primas-arl-202606]]. PCR sin documento funcional: [[reunion-diseno-pcr]].
- Relacionado: [[familia-mallas-alfaprd]], [[arquitectura-niif17]], [[repo-alfa-repos]]
