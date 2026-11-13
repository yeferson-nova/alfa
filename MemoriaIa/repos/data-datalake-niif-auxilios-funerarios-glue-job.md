---
tipo: repo
estado: vigente
actualizado: 2026-11-12
repo: data-datalake-niif-auxilios-funerarios-glue-job
categoria: glue
paso: "2 · Validación técnica, Silver y Gold SCD"
malla: auxilios-funerarios
---
# data-datalake-niif-auxilios-funerarios-glue-job
> Silver de Auxilios Funerarios y, además, los dos jobs genéricos de toda la familia: validación técnica y Gold SCD.

## Rol en el flujo
- Paso 2. `silver_aux_funerarios.py` es solo de esta malla. `validacion_tecnica.py` y `scd.py` son los jobs genéricos de las 6 mallas ALFAPRD (mismo script, distinto JSON de configuración).
- Orden en la Step Function: extracción, validación técnica, remediación, Silver, SCD, certificación.

## Entradas y salidas
- Validación técnica: entra `RUTA_CONFIG_VALIDACION`, compara los datasets contra la configuración, escribe hallazgos y deja `estado_validacion` en SUCCESS o FAILED; `validacion_tecnica_detiene_flujo` no bloquea.
- Silver: entra el runtime, `S3_SLV_PREP_PATH`, `FEC_DESDE` y `FEC_HASTA`; une pensiones, solicitudes de giro, distribución, novedades, conceptos y orígenes; sale Parquet preparado.
- SCD: entra `RUTA_CONFIG_SCD` y el runtime; compara por hash las columnas de cambio entre Silver preparada y Gold activo, cierra versiones y agrega nuevas (`ACT_REG`, vigencias); sale Gold con overwrite controlado.

## Recursos AWS que toca
- S3 `niif-{env}-assets` (configs `auxilios-funerarios/` y runtime), buckets Silver y Gold. Se despliega a `glue-resources/auxilios-funerarios`.

## Código
- `src/jobs/validacion_tecnica.py` (~810 líneas), `silver_aux_funerarios.py` (~510), `scd.py` (~550); scripts únicos ejecutados al importarse.
- `scd.py` trae freno de apagado masivo: argumentos opcionales `PERMITIR_APAGADO_MASIVO` y `UMBRAL_APAGADO_PCT` (20 % por defecto).
- Sin `tests/`; hay `tox.init` además de `tox.ini` (nombre mal escrito). README heredado de Endosos, no fiable.

## Estado
- En Alfa: `origin/quality`, commit 6081033 (2026-11-06). Desplegado: por confirmar.
- `quality` frente a `main` (medido en GitHub el 2026-11-12): 3 commits solo en `quality`, 3 solo en `main`, 1 archivos distintos; las dos ramas divergen en 1 archivo por identificar. Ver ARQ-50 en [[auditoria-repos-2026-11]].
- Código nuevo nuestro: ninguno.

## Brechas y relacionadas
- **ARQ-48** aloja `validacion_tecnica.py` y `scd.py`, que usan las 6 mallas, en [[auditoria-repos-2026-11]].
- SCD: ARQ-01 (lectura de Gold fallida asume primera ejecución), ARQ-02 (Gold se reescribe sin atomicidad), ARQ-06 (freno sin umbral pasado por las SFN); validación que no bloquea: ARQ-03. Todas en [[auditoria-pipeline-2026-11]]. SCD que apaga por ausencia: [[inprod-primas-arl-202606]].
- El enlace Certificar con espacio vive en el JSON de Auxilios de `cfg-datalake-niif-config-s3`: [[inprod-enlaces-cognito-9411325]].
- Relacionado: [[familia-mallas-alfaprd]], [[repo-alfa-repos]]
