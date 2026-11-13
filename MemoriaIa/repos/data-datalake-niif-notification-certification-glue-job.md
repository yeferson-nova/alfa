---
tipo: repo
estado: vigente
actualizado: 2026-11-12
repo: data-datalake-niif-notification-certification-glue-job
categoria: glue
paso: "3 · Notificación por correo (Lambda Notificación → Email)"
malla: transversal
---
# data-datalake-niif-notification-certification-glue-job
> Job de Glue que arma y envía por SES el correo de certificación (resumen, totales, detalle y enlaces Certificar / No certificar).

## Rol en el flujo
- Paso 3. Lo lanza la lambda trigger al quedar los registros del `certCode` en PENDING (o el evaluador compuesto al cerrar el grupo). Va después del job de certificación.
- Entrega el correo a los aprobadores; los enlaces cortos redirigen vía API a la lambda de certificación.

## Entradas y salidas
- Entra (main): `useCaseId`, `certCode`, `emailType`, `CONFIG_BUCKET`, `CONFIG_KEY`, `GLOBAL_CONFIG_KEY`, `SES_SENDER`, `REDIRECT_BASE_URL`, `SHORT_LINKS_TABLE`; opcional `REGENERACION_SIN_DATOS_NUEVOS`.
- Consulta Athena (resumen y total), carga la plantilla HTML (`email.templatePath`), crea enlaces cortos, envía y marca los registros como enviados.
- Sale: correo SES, enlaces en la tabla de short links y `sesMessageId` en DynamoDB. Cierra el estado del grupo compuesto cuando aplica.

## Recursos AWS que toca
- SES, Athena, S3 `niif-{env}-assets` (JSON y plantillas), DynamoDB `niif-{env}-notification-certification`, `niif-{env}-short-links` y `niif-{env}-composite-notification-status`.

## Código
- En Alfa (main): `src/jobs/notification_certification.py`, monolito de ~725 líneas que ya envía por SES (sesv2); variables y funciones de SNS quedan solo en nombres internos.
- En la rama: paquete hexagonal `src/notificacion_certificacion/` (domain, application con caso de uso `NotificarCertificacion`, adapters ses/athena/dynamo/s3, config), `src/job_notificacion.py`, `src/niif_observability/`, `config/job.dev.json`, `scripts/package.*` y `test/` (537 pruebas, 99 %, según su README). Todo configurable por JSON global; idempotente; errores con taxonomía `NTF-*`.

## Estado
- En Alfa: `origin/quality`, commit 07966fb (2026-11-12). Desplegado: por confirmar (el monolito).
- Código nuevo nuestro: rama `feature/codigo-nuevo-notificacion`, no desplegada. Mantiene el contrato de negocio; cambia `snsMessageId` a `sesMessageId` en DynamoDB y retira argumentos que pasan al JSON global.

## Brechas y relacionadas
- ARQ-22 y ARQ-26, ARQ-27, SEG-12, SEG-13 en [[notificaciones-vs-arquitectura]]. Riesgos del paso: [[riesgos-glue-notificacion]].
- Relacionado: [[job-notificacion-ses]], [[repos-alfa-notificacion]], [[orquestadora-notificaciones]], [[repo-alfa-repos]]
