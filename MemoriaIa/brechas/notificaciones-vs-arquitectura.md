---
tipo: brecha
actualizado: 2026-11-10
---
# Notificación y certificación vs. arquitectura y calidad

Humano: `Docs\06-brechas\notificaciones-vs-arquitectura.md`. Diagrama: hoja `Página-2` de `NIIF17_ModeloSolucion_V1.0.drawio` (ver [[arquitectura-niif17]]). Contexto del flujo: [[job-notificacion-ses]].

- Diagrama: Malla → DynamoDB → Lambda Notificación → Job Glue → Athena/Zona Certificación; Mensaje (icono **SES**) → Outlook; WAF → API GW → Cognito/AD; Zona Transporte → Lambda SFTP → FIS; Lambda Notificación → Modelo Configuración Extracción → Programador → Malla (interpretación, flechas sin texto).
- SES ya coincide con el diagrama (antes SNS). Pendiente de QA.
- **ARQ-21** diagrama no refleja las 5 lambdas, enlaces cortos ni estados. **ARQ-22** el job (no la lambda) envía el correo: recomendado corregir el diagrama. **ARQ-23** No certificar no deja la tarea pendiente (Víctor L276-277: pendiente hasta que negocio avise **a mano** al operador). Verificado en DEV: modelo = tabla `niif-dev-prg-programaciones`; el manager solo actúa con `activo=true` y `scheduleEstado=PENDIENTE` y rechaza `scheduleExpression`/`periodoNegocio` = `"PENDIENTE"`; escribe `PROGRAMADO` y nadie lo limpia al terminar el `at()` (ARL sigue `PROGRAMADO` sin schedule). Propuesta sin confirmar: escribir `PENDIENTE` en esos tres campos, o `RECHAZADO` para evitar el error del stream. Detalle en el doc humano §4.1. **ARQ-24** `NIIF_CARPETAS` duplicado en copy y move. **ARQ-25** `sendLock` solo se lee, nunca se escribe. **ARQ-26** trigger/copy/move tragan excepciones y devuelven éxito. **ARQ-27** `--ENVIRONMENT='{env}'` literal en el evaluador. **ARQ-28** WAF sin verificar.
- **SEG-10** Cognito flujo implícito. **SEG-11** `state` sin firma. **SEG-12** tokens y NewImage (con correo) en logs.
- Calidad: job desplegado = monolito de 727 líneas (cambio mínimo por decisión); `reposNew` hexagonal con 163 pruebas, sin desplegar; las 4 lambdas sin pruebas ni estructura. Sonar: Maintainability B (204 olores, falta desglose por regla).
- Orden propuesto: QA → decidir ARQ-22/23 → ARQ-26/27/SEG-12 → desplegar `reposNew` → unificar lambdas → SEG-10/11 con DevOps → actualizar diagrama.

## Plan de trabajo para unificar (propuesta 2026-11-10, tiempos estimados, sin aprobar)
- **Meta:** un paquete con dominio compartido. Una lambda orquestadora del stream de `notification-certification` reemplaza a `trigger`, `copy` y `move` (decide por transición: `PENDING` inicia el job; `CERTIFICATE` copia a transporte; `NO_CERTIFICATE` archiva y deja la malla pendiente). Evaluador compuesto y API `niif-dev-certification` se mantienen aparte con el mismo dominio. Job = `reposNew`. Mapa de carpetas sale de la configuración global. Ver [[estructura-repos-propuesta]].
- **Riesgo de unir el stream:** un registro con error frena el lote. Mitigar con `ReportBatchItemFailures` y cola SQS de fallos. Cambio de 3 a 1 lambda: desplegar la nueva con el mapeo deshabilitado y conmutar.
- **Fase 0 (1-2 d):** validar paquete en QA, alinear DEV a `email-template`, `git rm` previews viejos, reanalizar Sonar con desglose de reglas.
- **Fase 1 (1 reunión):** decidir ARQ-22, ARQ-23 (qué escribir y quién limpia `PROGRAMADO`), una lambda o tres con código común, estructura de repos.
- **Fase 2 (2-3 d):** ARQ-26, ARQ-27, SEG-12, ARQ-25 sobre lo desplegado.
- **Fase 3 (5-8 d):** dominio compartido, lambda orquestadora con pruebas > 80 %, escritura de "pendiente" en `prg-programaciones`, alarma de registros atascados, DEV, luego QA vía DevOps.
- **Fase 4 (3-4 d):** desplegar `reposNew` tras QA, retirar el monolito, quitar `SNS_TOPIC_ARN` y permisos SNS.
- **Fase 5 (3-5 d, con DevOps):** SEG-10 (PKCE), SEG-11 (firmar `state`), ARQ-28 (WAF).
- **Fase 6 (1 d):** actualizar la hoja `Página-2` (ARQ-21).
- **Fuera de alcance:** T3, deploy de T2, 1c y 1d (encajan en fase 3 si se define alcance).
- **Riesgos:** cada prueba en QA mueve archivos reales y dispara SFTP a FIS (coordinar con negocio); Desarrollo sin acceso a QA/PRD alarga el calendario.
- **ARQ-29 (QA, 2026-11-10):** el enlace de descarga del CSV abre la consola de S3 vía portal SSO (`construir_url_consola_s3` en `niif_certification.py`, guardado en `s3_url`); la notificación lo acorta y la redirección solo hace 302. El usuario debe pulsar Download y necesita rol en la cuenta. Esperado (Víctor L270-271, diagrama): descarga mediada por API Gateway. Recomendado: acción `DOWNLOAD` tras login Cognito + URL prefirmada de 1 a 5 min; prefirmar en el job no sirve (vence con la sesión del rol). **SEG-13:** token de 8 hex y `/r` público. Decisión pendiente del equipo.
