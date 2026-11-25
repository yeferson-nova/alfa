---
tipo: repo
estado: vigente
actualizado: 2026-11-25
repo: data-datalake-niif-pcr-emisiones-glue-job
categoria: glue
paso: "2 · Bronce (extracción)"
malla: transversal
---
# data-datalake-niif-pcr-emisiones-glue-job
> Extracción Oracle a Bronce de la malla 1 (Emisiones) del proceso PCR, origen Acsel-e: un solo job para los casos PCR Vida, Generales y Satélites (núcleo común una vez y luego cada caso).

## Rol en el flujo
- Primer job de `niif-snf-emisiones` (malla 1 de 4 del proceso PCR: Emisiones → Primas Cedidas → Reaseguros → Primas PCR; ver [[reunion-extraccion-victor]]). Paso 2 del diagrama. No pertenece a las 6 mallas ALFAPRD.
- Entrega al resto de la cadena genérica y a `silver_vida_deudores.py` (repo `data-datalake-emisiones-vida-glue-job`).

## Entradas y salidas
- Entra: `RUTA_CONFIG_EXTRACCION` (el JSON `pcr_vida/config_extraccion_pcr_vida.json` de `cfg-datalake-niif-config-s3`, con fuente Oracle ACSELE y un ciclo de dependencias).
- Sale: Parquet Bronce por lote y `parametros_extraccion.json` con checkpoint y estado.

## Recursos AWS que toca
- Conexión Glue a Oracle, S3 `niif-{env}-assets` y buckets Bronce/Silver. Se despliega a `glue-resources/pcr-emisiones`.

## Código
- `src/jobs/extraccion_estandar_emisiones.py`: en `quality` (sync `3d5f623`, 2026-11-20) tiene **819 líneas**, reescrito (commit `e1a13ac`): núcleo común + ramas por caso, fases CASCADA/PARALELO, dependencias con `EXISTS`, checkpoint `_control/_SUCCESS.json`. La versión anterior (~3400 líneas, con `sampleQuery` y llaves materializadas) solo queda en ramas viejas. Análisis de tiempos en [[modularizacion-extraccion]].
- Parece una copia más antigua en un punto (la función `combinar_lotes_respaldados` está arriba y sin parámetro de tabla), por confirmar.
- Es el único glue de estos repos con carpeta `tests/`, pero solo trae `conftest.py` y paquetes vacíos. Incluye `Dockerfile`, `Makefile`, `run_local.sh`. README es plantilla inicial.

## Estado
- En Alfa: `origin/quality`, commit 900fe44 (2026-11-06). Desplegado: por confirmar.
- `quality` frente a `main` (medido en GitHub el 2026-11-12): 8 commits solo en `quality`, 0 solo en `main`, 14 archivos distintos; `quality` va 14 archivos por delante de `main`: lo de QA no se ha promovido a PRD. Ver ARQ-50 en [[auditoria-repos-2026-11]].
- Código nuevo nuestro: ninguno.
- **Modularización (2026-11-20):** está en nuestro alcance (job de Glue, pasos 2 y 3). Entra en [[modularizacion-extraccion]]; sus copias en DEV son ARQ-60 ([[auditoria-extraccion-2026-11]]).

## Brechas y relacionadas
- **ARQ-61** rutas quemadas por ambiente (`:52-53`, `:581`) y **ARQ-63** checkpoint reutilizado / `SUCCESS` con fuentes `INCOMPLETA` (`:607-611`, `:768-776`), versión `quality` `e1a13ac2`. Caso: [[inprod-pcr-emisiones-202606]].
- Misma exposición que el proceso de extracción estándar a exclusión de llaves y Silver parcial: [[inprod-primas-arl-202606]]. PCR sin documento funcional: [[reunion-diseno-pcr]].
- Relacionado: [[familia-mallas-alfaprd]], [[arquitectura-niif17]], [[repo-alfa-repos]]
