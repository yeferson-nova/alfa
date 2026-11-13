---
tipo: repo
estado: vigente
actualizado: 2026-11-12
repo: sc-datalake-niif-composite_notification_evaluator
categoria: lambda
paso: "3 · Certificación Cifras (compuerta antes de la certificación)"
malla: transversal
---
# sc-datalake-niif-composite_notification_evaluator
> Lambda que decide si un caso se notifica solo o espera a su grupo (Rentas + Endosos, ARP + RVI) y, cuando el grupo está completo, deja que se lance un solo job de certificación.

## Rol en el flujo
- La invoca la Step Function de cada malla con `mode = RESOLVE_NOTIFICATION` (resolver notificación y regeneración), al final del pipeline y antes del job de certificación.
- Devuelve a la Step Function una decisión (`shouldStartCertification`, `notificationMode`, `configKey`); la Step Function lanza el job `niif-{env}-certification`, que escribe los registros PENDING con los que sigue la cadena.

## Entradas y salidas
- Entra: `useCaseId`, `period`, `loteCargueId`, `configBucket`, `configKey`, `sourceExecutionId` y `regeneracionSinDatosNuevos`; lee `composite_groups.json` del bucket de configuración.
- Sale: modos `SIMPLE`, `COMPOSITE_WAITING`, `COMPOSITE_ALREADY_LOCKED`, `COMPOSITE_READY` o `REGENERACION_SIN_DATOS_NUEVOS`. Los eventos de stream (`Records`) se ignoran en el código.

## Recursos AWS que toca
- Tabla de estado compuesto (variable `TABLE_NAME`; llave `compositeId` + `memberId`, fila `_CONTROL` como candado; estados `READY_TO_NOTIFY` y `NOTIFICATION_IN_PROGRESS`). El nombre de la tabla viene del entorno (por confirmar).
- Bucket `niif-{env}-assets` (`composite_groups.json` y JSON global).
- Job de Glue de certificación (`GLUE_JOB_NAME`) con `start_job_run`, solo en la ruta de stream, hoy inactiva.
- Función `niif-{env}-composite-notification-evaluator`.

## Código
- `src/composite_notification_evaluator.py` (528 líneas): `resolve_notification_event`, `upsert_member_ready`, `all_members_ready`, `create_control_lock` y `start_composite_job`. Exige `SNS_TOPIC_ARN`, `REDIRECT_BASE_URL` y `SHORT_LINKS_TABLE` al importar, aunque la ruta que se usa no los necesita.
- `tests/test_lambda_function.py`: 43 pruebas, umbral 70 %. El README de Alfa es una copia del de otra lambda.

## Estado
- En Alfa: `origin/quality`, commit `80ca055` (2026-10-14); desplegado.
- `quality` frente a `main` (medido en GitHub el 2026-11-12): 0 commits solo en `quality`, 3 solo en `main`, 0 archivos distintos; mismo contenido que `main` (los commits de diferencia son merges); la base `quality` sirve. Ver ARQ-50 en [[auditoria-repos-2026-11]].
- Código nuevo nuestro: rama `feature/codigo-nuevo-notificacion`, solo esqueleto hexagonal `composite_evaluator/` y `niif_observability`; el `.py` es idéntico al de Alfa. Su README dice que además lee el stream de la tabla de estado compuesto, algo que el código de Alfa no hace (por confirmar).

## Brechas y relacionadas
- **Arquitectura:** en la propuesta es la Lambda (Evaluador compuesto), llamada por la Malla de Ejecución, con el Modelo Compuesto; no está en la aprobada. Ver [[arquitectura-propuesta-niif17]].
- **ARQ-46** el README del repo es copia de otro y no lo describe, en [[auditoria-repos-2026-11]].
- ARQ-27: `--ENVIRONMENT` se envía como el texto `{env}` literal. ARQ-32: no propaga el ID de ejecución. ARQ-21: no está en el diagrama. Todas en [[notificaciones-vs-arquitectura]].
- Relacionado: [[roadmap-notificaciones]] (E3 guarda `ejecucionId` y `ejecucionesOrigen` en el compuesto), [[inventario-lambdas]], [[job-certificacion]].
