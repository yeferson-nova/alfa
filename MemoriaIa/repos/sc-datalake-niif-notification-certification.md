---
tipo: repo
estado: vigente
actualizado: 2026-11-12
repo: sc-datalake-niif-notification-certification
categoria: lambda
paso: "3 · Certificación Cifras (Lambda Notificación, trigger)"
malla: transversal
---
# sc-datalake-niif-notification-certification
> Lambda trigger: al ver un registro PENDING en la tabla de notificaciones lanza el job de Glue de notificación (el que envía el correo).

## Rol en el flujo
- La dispara el stream de DynamoDB de `niif-{env}-notification-certification`, cuando el job de certificación deja registros PENDING.
- Entrega al job de Glue de notificación, que envía el correo con los enlaces Certificar / No certificar.

## Entradas y salidas
- Entra: eventos MODIFY con `status` PENDING. Ignora los que ya tienen `sendLock`, los que no tienen configBucket, configKey y globalConfigKey, y los que no son la consulta `detail#1`.
- Sale: `start_job_run` con `--useCaseId`, `--certCode`, `--emailType`, `--CONFIG_BUCKET`, `--CONFIG_KEY`, `--GLOBAL_CONFIG_KEY`, `--SNS_TOPIC_ARN`, `--REDIRECT_BASE_URL`, `--SHORT_LINKS_TABLE` y `--REGENERACION_SIN_DATOS_NUEVOS`. No pasa `--SES_SENDER`.

## Recursos AWS que toca
- Stream de `niif-{env}-notification-certification` (solo lectura del evento; no consulta la tabla).
- Job de Glue de notificación (`GLUE_JOB_NAME`, `niif-{env}-notification-certification`).
- Variables obligatorias `SNS_TOPIC_ARN`, `REDIRECT_BASE_URL` y `SHORT_LINKS_TABLE`; función `niif-{env}-notification-certification-trigger`.

## Código
- `src/notification_certification.py` (104 líneas): un solo `lambda_handler` que filtra y lanza el job; nadie escribe `sendLock`, así que la guarda no protege de lanzamientos dobles. Los errores se registran sin relanzar.
- `tests/test_lambda_function.py`: 7 pruebas, umbral 70 %. El README de Alfa es el de la lambda move, no describe este repo.

## Estado
- En Alfa: `origin/quality`, commit `6edeb67` (2026-10-14); desplegado y alineado con DEV.
- `quality` frente a `main` (medido en GitHub el 2026-11-12): 0 commits solo en `quality`, 3 solo en `main`, 0 archivos distintos; mismo contenido que `main` (los commits de diferencia son merges); la base `quality` sirve. Ver ARQ-50 en [[auditoria-repos-2026-11]].
- Código nuevo nuestro: ninguno en este repo (rama `feature/codigo-nuevo-notificacion` idéntica). Lo reemplaza la orquestadora (D1), que toma el candado, pasa `--EJECUCION_ID` y ya no envía los argumentos de enlaces cortos, porque el job nuevo los rechaza (ARQ-44). Deben desplegarse juntos.

## Brechas y relacionadas
- **ARQ-46** el README del repo es copia de otro y no lo describe, en [[auditoria-repos-2026-11]].
- ARQ-25 (`sendLock` solo se lee), ARQ-26 (traga excepciones), ARQ-40 (solo notifica si la consulta es `detail#1`), ARQ-43 (mapping sin cola de fallos ni reintentos acotados) y ARQ-44, en [[notificaciones-vs-arquitectura]].
- Relacionado: [[sc-datalake-niif-notification-certification-orchestrator]], [[orquestadora-notificaciones]], [[job-notificacion-ses]], [[repos-alfa-notificacion]], [[roadmap-notificaciones]].
