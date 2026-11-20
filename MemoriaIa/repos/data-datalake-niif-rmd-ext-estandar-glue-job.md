---
tipo: repo
estado: vigente
actualizado: 2026-11-20
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
- **Diseño (Víctor, 2026-11-20): las reglas de remediación son activables.** La copia Bronce → Silver base no es un movimiento muerto: es el punto donde se aplican las reglas activas; sin reglas activas pasa el dato igual. Verificado en código (2026-11-20): el script solo recibe esos 3 argumentos, no lee ninguna configuración de reglas ni tiene mecanismo de activación (líneas 213-221, 474-480). Lo que falta es la activación por configuración, no quitar la copia.
- **Reglas activadas que no se aplican (verificado 2026-11-20):** `config_remediacion_reserva_matematica_{arp,rvi}.json` traen `remediacion_habilitada: true` con reglas (`TRIM_ESPACIOS_INICIO/FIN` en nombres y apellidos) y trazabilidad; PCR Vida trae `false` y `reglas: []`. Las Step Functions de ARP, RVI y PCR Vida pasan `--RUTA_CONFIG_REMEDIACION`, pero el script no lo lee y ningún job de `glue\` implementa esas reglas. Las demás mallas no tienen config de remediación.

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
- **Modularización (2026-11-20):** [[modularizacion]] y [[modularizacion-extraccion]]. Rama `feature/modular-rmd-ext-estandar`: red de caracterización y carpetas vacías; sin los 6 commits de `bugfix/scd-freno-periodo`. Script de extracción desplegado en DEV = repo (sha `792c1d26`). Brechas de extracción ARQ-57, COS-11, ARQ-58 y ARQ-59 en [[auditoria-extraccion-2026-11]].
- **ARQ-48** aloja la extracción y la remediación genéricas de todas las mallas, en [[auditoria-repos-2026-11]].
- INC-1 del incidente PRD (extracción incremental excluyó llaves y dejó Silver parcial): [[inprod-primas-arl-202606]]. Vacío o parcial llega a certificar: ARQ-04; validación no bloquea y remediación no corrige: ARQ-03, en [[auditoria-pipeline-2026-11]].
- Relacionado: [[familia-mallas-alfaprd]], [[arquitectura-niif17]], [[repo-alfa-repos]]
