---
tipo: repo
estado: vigente
actualizado: 2026-11-12
repo: data-datalake-niif-rmd-ext-estandar-glue-job
categoria: glue
paso: "2 · Bronce (extracción) y remediación"
malla: transversal
---
# data-datalake-niif-rmd-ext-estandar-glue-job
> Jobs genéricos de extracción Oracle a Bronce y de remediación (disponibilización Bronce a Silver). Los usan las 6 mallas ALFAPRD con distinto JSON.

## Rol en el flujo
- Primer job de cada Step Function de malla (extracción) y tercero (remediación, tras la validación técnica). Paso 2 del diagrama.
- Entrega al Silver de cada malla (por tabla `lotes_segmento` y estados en el runtime) y a la validación técnica.

## Entradas y salidas
- Extracción: entra `RUTA_CONFIG_EXTRACCION` (JSON con fuente Oracle por conexión Glue, tablas, ciclos de dependencias, segmentación por llaves y exclusión de ya extraídos). Sale Parquet Bronce por lote, llaves de segmentación y `parametros_extraccion.json` con checkpoint, reanudación y `estado_extraccion`.
- Remediación: entra `RUTA_PARAMETROS_EXTRACCION`, `RUTA_ORIGEN_REMEDIACION` y `RUTA_DESTINO_REMEDIACION`; copia datasets Bronce a Silver base y deja `estado_remediacion` y `estado_disponibilizacion_silver` en SUCCESS. No corrige datos: marca `PENDIENTE_ANALITICA` y nadie lo lee.

## Recursos AWS que toca
- Conexión Glue a Oracle (ALFAPRD), S3 `niif-{env}-assets` (configs y runtime), buckets Bronce y Silver. Se despliega a `glue-resources/estandar`.

## Código
- `src/jobs/extraccion_estandar_optimized.py` (~1900 líneas): lectura JDBC con reanudación por checkpoint, reutilización de snapshots y exclusión de llaves (`combinar_lotes_respaldados`, `aplicar_exclusion_llaves_segmento`).
- `src/jobs/remediacion_estandar.py` (~680 líneas). Scripts únicos; sin `tests/`; el README es heredado de Endosos (no fiable).
- Nombre engañoso: "rmd" y "estandar" pero es la base de toda la familia. La variante PCR vive en `data-datalake-niif-pcr-emisiones-glue-job`.

## Estado
- En Alfa: `origin/quality`, commit 4247229 (2026-11-06). Desplegado: por confirmar.
- `quality` frente a `main` (medido en GitHub el 2026-11-12): 3 commits solo en `quality`, 3 solo en `main`, 1 archivos distintos; las dos ramas divergen en 1 archivo por identificar. Ver ARQ-50 en [[auditoria-repos-2026-11]].
- Código nuevo nuestro: ninguno. El fix de exclusión de llaves planeado en [[familia-mallas-alfaprd]] parece ya estar en esta versión (`combinar_lotes_respaldados` debajo de `resolver_llaves_reales` y con parámetro de tabla); por confirmar contra el código desplegado.

## Brechas y relacionadas
- **ARQ-48** aloja la extracción y la remediación genéricas de todas las mallas, en [[auditoria-repos-2026-11]].
- INC-1 del incidente PRD (extracción incremental excluyó llaves y dejó Silver parcial): [[inprod-primas-arl-202606]]. Vacío o parcial llega a certificar: ARQ-04; validación no bloquea y remediación no corrige: ARQ-03, en [[auditoria-pipeline-2026-11]].
- Relacionado: [[familia-mallas-alfaprd]], [[arquitectura-niif17]], [[repo-alfa-repos]]
