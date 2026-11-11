---
tipo: tarea
actualizado: 2026-11-11
---
# Orquestadora de la cadena de notificación

- **Código:** `Backend\notificacion\lambdas\niif-dev-notification-certification-orchestrator\`, en el commit `a0e1ac0` ("orquestadora de notificaciones").
  - 404 pruebas, 97,9 % de cobertura; handler `lambda_function.lambda_handler`.
  - **No desplegada.** El detalle completo está en su `README.md`: argumentos, claves del JSON, eventos de log, errores `ORQ-*`, módulos y la sección de DevOps.
- **Reemplaza a** trigger, copy y move (D1). La auditoría previa es `Docs\06-brechas\auditoria-orquestadora-trigger-copy-move.md` (ARQ-40 a ARQ-44, ver [[notificaciones-vs-arquitectura]]).
- **Decisiones aprobadas (2026-11-11):**
  - O-1: candado transaccional (`sendLock`, `sendLockDate`, `sendAttempts`) en todos los registros del `certCode`.
  - O-2: índice nuevo `status-pendingDate-index` (status S + pendingDate S, proyección ALL). **Lo crea DevOps en Terraform; nosotros no lo creamos.**
  - O-3: se conserva la ruta histórica `<useCaseId>/<nameFile>` (`orchestrator.use_historic_source_path`).
  - O-4: se despliega junto con el job de notificación nuevo, que rechaza `--REDIRECT_BASE_URL` y `--SHORT_LINKS_TABLE` (ARQ-44).
- **Comportamiento:**
  - Solo actúa si cambia `status`.
  - `PENDING`: candado y lanzamiento del job con `--EJECUCION_ID`.
  - `CERTIFICATE`: copia solo el archivo del evento a `export/<carpeta>/` y marca `TRANSPORT` con condición.
  - `NO_CERTIFICATE`: mueve a `NO-CERTIFICADOS`, de forma idempotente, y marca `NO_TRANSPORT`. Escribir en `prg-programaciones` espera D2 y por ahora solo deja un log WARN.
  - En el lote, se detiene en el primer fallo y lo devuelve en `batchItemFailures`.
  - Reprocesador (evento sin `Records`): busca `PENDING` atascados con el índice, relanza hasta `reprocess_max_attempts` y luego manda una sola alerta SES (D5).
- **Configuración:**
  - El JSON global sale del registro (`configBucket`/`globalConfigKey`), así que previsional también funciona.
  - Las variables de entorno `CONFIG_BUCKET`/`GLOBAL_CONFIG_KEY` las usa solo el reprocesador.
  - Claves nuevas: `dynamodb.pending_index` y el bloque `orchestrator`. Ya están en `Backend\S3\niif-dev-assets\configs\certification\json_global_notif_cert.json`.
- **Pendiente:**
  1. DevOps: índice, función, mapping con `ReportBatchItemFailures` + bisect + cola de fallos, Scheduler `rate(30 minutes)` y permisos (incluido `s3:ListBucket`).
  2. Correo real de `alert_recipients` (hoy `<CORREO_SOPORTE_NIIF>`).
  3. Claves nuevas en el JSON global de previsional (`previsional/config/json_global_notif_cert.json`).
  4. Verificar si el job de certificación **viejo** escribe `pendingDate`; el nuevo sí.
  5. Prueba en DEV con Primas ARL, con confirmación previa.
- Ver también [[roadmap-notificaciones]], [[job-certificacion]] y [[job-notificacion-ses]].
