---
tipo: repo
estado: nuevo
actualizado: 2026-11-12
repo: sc-datalake-niif-notification-certification-orchestrator
categoria: lambda
paso: "3 · Certificación Cifras (orquestación de la cadena de notificación)"
malla: transversal
---
# sc-datalake-niif-notification-certification-orchestrator
> Lambda orquestadora que reemplaza a trigger, copy y move: según el cambio de estado en la tabla de notificaciones lanza el job de notificación, copia a transporte o archiva los no certificados; más un reprocesador de PENDING atascados.

## Rol en el flujo
- Un solo mapping del stream de `niif-{env}-notification-certification` (INSERT y MODIFY de PENDING, CERTIFICATE y NO_CERTIFICATE) y un disparo programado (`rate(30 minutes)`).
- Entrega al job de Glue de notificación (PENDING), al bucket de transporte para `send-via-sftp` (CERTIFICATE) o a `NO-CERTIFICADOS` (NO_CERTIFICATE).

## Entradas y salidas
- Entra: lote del stream (solo actúa si `status` cambia) o evento sin `Records` para el reprocesador.
- Sale: job lanzado con `--EJECUCION_ID` (sin los argumentos de enlaces cortos ni SNS), CSV copiado o movido y estado TRANSPORT o NO_TRANSPORT con condición; `batchItemFailures` en el lote y alerta SES única al agotar intentos.

## Recursos AWS que toca
- Tabla `niif-{env}-notification-certification` con el índice `certCode-index` y el nuevo `status-pendingDate-index` (lo crea DevOps); candado transaccional con `sendLock`, `sendLockDate` y `sendAttempts`.
- Job de Glue de notificación, bucket de reportes `niif-{env}-certification`, bucket de transporte, SES (alerta) y `niif-{env}-assets` (JSON global). La configuración sale del JSON global, sin valores por defecto.

## Código
- `src/lambda_function.py` solo delega a `adapters/inbound/lambda_handler.py`. Capas `domain` (decisiones, errores `ORQ-*`, modelos), `application` (`process_stream`, `launch_notification`, `move_files`, `reprocess_pending`, puertos), `adapters/outbound` (DynamoDB, Glue, S3, SES) y `config`. Incluye `niif_observability` (log JSON y métricas).
- `test/` con pruebas de dominio, aplicación, adaptadores y arquitectura; README: 404 pruebas, 97,9 % de cobertura. `scripts/package.sh|ps1`; `config/lambda.dev.json` y eventos de ejemplo. No hay Jenkinsfile (el repo de DevOps lo debe crear).

## Estado
- En Alfa: no existe en `main` ni en `VERSIONES.md`; nada desplegado.
- Código nuevo nuestro: rama `feature/codigo-nuevo-notificacion`. Pendiente de DevOps: índice, función, mapping con `ReportBatchItemFailures`, bisect y cola de fallos, Scheduler, permisos. Faltan el correo real de `alert_recipients` y las claves nuevas en el JSON global de previsional. Se despliega junto con el job de notificación nuevo.

## Brechas y relacionadas
- **Arquitectura:** en la propuesta es la Lambda (Notificación): reemplaza al trigger, a copy y a move. En la aprobada la Lambda (Notificación) hace todo. Ver [[arquitectura-propuesta-niif17]].
- Cierra en código ARQ-24, ARQ-25, ARQ-26, ARQ-40 a ARQ-43 y SEG-12; ARQ-23 queda parcial (D2); ARQ-44 exige el despliegue conjunto. Decisiones O-1 a O-4 aprobadas. En [[notificaciones-vs-arquitectura]].
- Relacionado: [[orquestadora-notificaciones]], [[roadmap-notificaciones]], [[job-notificacion-ses]], [[sc-datalake-niif-notification-certification]], [[sc-datalake-niif-copy-s3-certification]], [[sc-datalake-niif-mov-s3-certification]], [[est-gobierno-devops-niif]].
