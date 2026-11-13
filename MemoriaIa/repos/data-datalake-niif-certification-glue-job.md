---
tipo: repo
estado: vigente
actualizado: 2026-11-12
repo: data-datalake-niif-certification-glue-job
categoria: glue
paso: "3 · Certificación de cifras (genera archivos)"
malla: transversal
---
# data-datalake-niif-certification-glue-job
> Job de Glue que genera los CSV de certificación de una malla y periodo con Athena y deja los registros del `certCode` en PENDING, lo que dispara la notificación.

## Rol en el flujo
- Paso 3 (Certificación Cifras). Último job de cada Step Function de malla; también lo lanza el evaluador compuesto. Lo comparten las mallas ALFAPRD, PCR Vida, generales, satélites y previsional.
- Entrega a la lambda trigger de notificación (cambio de estado en DynamoDB) y de ahí al job de notificación.

## Entradas y salidas
- Entra: `PERIOD`, `CONFIG_KEY` (JSON del caso), `CONFIG_BUCKET`, `GLOBAL_CONFIG_KEY`; opcional `REGENERACION_SIN_DATOS_NUEVOS`. La rama nueva agrega `EJECUCION_ID` y `EJECUCIONES_ORIGEN`.
- Ejecuta las consultas de detalle con `UNLOAD` de Athena, arma el CSV final con encabezados y BOM, lo guarda en el bucket de reportes y crea un registro por consulta en DynamoDB; genera el `certCode`.
- Sale: CSV en `niif-{env}-certification` y registros en `niif-{env}-notification-certification`.

## Recursos AWS que toca
- Athena (workgroup por defecto), buckets `niif-{env}-assets` y `niif-{env}-certification`, tabla `niif-{env}-notification-certification` con índice por `certCode`.

## Código
- En Alfa (main): `src/jobs/niif_certification.py`, script monolítico de ~850 líneas, con parches de encabezados CSV y detección de resultado vacío.
- En la rama: paquete hexagonal `src/certification_job/` (domain, application, adapters/inbound y outbound, config), `src/niif_observability/` (logs JSON y métricas), `config/job.dev.json`, `eventos/`, `scripts/package.*` y `test/` (412 pruebas, 97 % según su README). Estándar [[est-desarrollo-hexagonal]].

## Estado
- En Alfa: `origin/quality`, commit 3ae720c (2026-10-19). Desplegado el monolito (por confirmar en cada ambiente).
- Verificado contra GitHub (captura del 2026-11-12): `quality` tiene los mismos 8 archivos que exportamos y está **3 commits detrás de `main`**; qué traen esos 3 commits está por confirmar.
- Código nuevo nuestro: rama `feature/codigo-nuevo-notificacion` de `alfa-repos`: reescritura hexagonal, no desplegada ni probada en AWS.

## Brechas y relacionadas
- INC-3: el CSV nunca queda vacío por el `UNION ALL` de encabezado, así no se activa el aviso de sin datos ([[inprod-primas-arl-202606]]). COS-07, COS-08, COS-09 y ARQ-34 a ARQ-38 (consultas) en [[notificaciones-vs-arquitectura]]. ARQ-04 en [[auditoria-pipeline-2026-11]].
- Riesgo CER-H5 al reemplazarlo: [[job-certificacion]]
- Relacionado: [[repos-alfa-notificacion]], [[repo-alfa-repos]]
