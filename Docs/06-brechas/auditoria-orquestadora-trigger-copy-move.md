# Orquestadora de la cadena de notificación: etapa 1 (auditoría, solo lectura)

Fecha: 2026-11-11. Paso 4 del roadmap (`Tareas\T1-notificaciones\roadmap-notificaciones.md`).

**Componente nuevo:** `niif-dev-notification-certification-orchestrator`, que reemplaza a tres lambdas (decisión D1):
- `niif-dev-notification-certification-trigger`;
- `niif-dev-copy-s3-cert`;
- `niif-dev-move-s3-certification`.

**Fuentes revisadas:**
- el código desplegado en DEV, en `Backend\Lamnbdas\<lambda>\`: el de las tres lambdas y el de `niif-dev-certification`, que es quien escribe `CERTIFICATE` y `NO_CERTIFICATE`;
- los mappings del stream (`EvidenciasAws\dev-uso-recursos\raw\mappings.json`) y el uso a 90 días (`lambdas-uso.tsv`);
- los JSON de caso de DEV.

## 1. Qué hace hoy cada lambda

Las tres escuchan el stream de `niif-dev-notification-certification`, cada una con su mapping. Los tres mappings tienen el mismo ajuste:
- lote de 10 registros;
- reintentos infinitos (`-1`) y edad máxima infinita;
- sin división del lote (`Bisect` desactivado);
- sin cola de fallos;
- sin `ReportBatchItemFailures`.

| | Trigger | Copy | Move |
|---|---|---|---|
| Filtro del mapping | `status = PENDING`, `INSERT` o `MODIFY` | `status = CERTIFICATE`, `MODIFY` | `status = NO_CERTIFICATE`, `MODIFY` |
| Condición en el código | Solo `MODIFY`, sin `sendLock`, y **solo `queryName == "detail#1"`**. Exige `configBucket`, `configKey` y `globalConfigKey` en el registro | El estado anterior no era `CERTIFICATE` | El estado anterior no era `NO_CERTIFICATE` |
| Qué hace | Lanza el job de Glue de notificación (`GLUE_JOB_NAME`) con `--useCaseId`, `--certCode`, `--emailType`, `--CONFIG_BUCKET`, `--CONFIG_KEY` y `--GLOBAL_CONFIG_KEY` (tomados del registro), más `--SNS_TOPIC_ARN`, `--REDIRECT_BASE_URL` y `--SHORT_LINKS_TABLE` (de sus variables de entorno) y `--REGENERACION_SIN_DATOS_NUEVOS` | Busca **todos** los registros del `certCode` en `certCode-index`. Por cada uno: busca el CSV en `<carpeta>/<nameFile>` o, si no está, en `<useCaseId>/<nameFile>` (ruta histórica); lo copia a `TRANSPORT_BUCKET/export/<carpeta>/<nameFile>` y marca `TRANSPORT` | Igual, pero copia a `REPORT_BUCKET/<carpeta>/NO-CERTIFICADOS/<nameFile>`, borra el original y marca `NO_TRANSPORT` |
| Variables de entorno | `GLUE_JOB_NAME`, `SNS_TOPIC_ARN`, `REDIRECT_BASE_URL`, `SHORT_LINKS_TABLE` | `REPORT_BUCKET`, `TRANSPORT_BUCKET`, `NOTIFICATIONS_TABLE` | `REPORT_BUCKET`, `NOTIFICATIONS_TABLE` |
| Uso en DEV (90 días) | 94 invocaciones, **15 con error** | 18 invocaciones, 0 errores | 1 invocación, 0 errores |

**Quién escribe los estados:**
- `PENDING`: el job de certificación, al final, en todos los registros del `certCode`.
- `CERTIFICATE`, `NO_CERTIFICATE` o `PENDING_APPROVERS`: la lambda `niif-dev-certification` (el API), registro por registro y con una condición: no cambia un registro que ya está en `CERTIFICATE` ni, al certificar, uno en `TRANSPORT`.
- `sendLock`: **nadie lo escribe**. El trigger solo lo lee (ARQ-25 confirmado).

## 2. Hallazgos

Ya registrados y confirmados en el código: ARQ-23 (No certificar no deja la malla pendiente), ARQ-24 (mapa de carpetas duplicado en copy y move, y ahora también en el JSON global del job de certificación), ARQ-25 (sin candado real), ARQ-26 (las tres capturan toda excepción y devuelven éxito) y SEG-12 (el trigger registra la imagen completa del registro).

**Nuevos:**

| ID | Hallazgo | Evidencia | Efecto |
|---|---|---|---|
| ARQ-40 | **El trigger solo notifica si existe una consulta llamada exactamente `detail#1`.** Es un contrato implícito: hoy los 4 JSON de caso tienen una sola consulta y se llama así, pero un caso cuya consulta tenga otro nombre nunca recibe el correo, y no queda ningún error | `notification_certification.py` L52-57; JSON de caso | Correo que nunca sale, sin aviso |
| ARQ-41 | **Copy procesa todos los registros del `certCode` por cada evento.** El API marca cada registro por separado, así que con N consultas hay N eventos y N×N copias y actualizaciones. Además, copy puede marcar `TRANSPORT` un registro que el API todavía no ha actualizado: la condición del API falla, el API responde error y deja el `certCode` a medias (**hipótesis**: hoy N = 1 en todos los casos de DEV, así que no se ha visto). Copy, move y el API leen solo la primera página del índice | `copy_s3.py` L88-120; `certification.py` L520-545 | Trabajo repetido y un posible `certCode` a medias cuando haya casos con varias consultas |
| ARQ-42 | **Move no es idempotente y confunde "sin permiso" con "no existe".** Si se reprocesa después de mover, `niif_origen` falla porque el original ya se borró, antes de ver que el destino existe; el error se traga y el registro no pasa a `NO_TRANSPORT`. `object_exists` captura toda excepción: un `AccessDenied` se lee como "no existe" | `move_s3.py` L62-104 | Registro atascado en `NO_CERTIFICATE` y errores de permisos invisibles |
| ARQ-43 | **Los mappings del stream no tienen manejo de fallos:** reintentos y edad infinitos, sin división del lote, sin cola de fallos y sin `ReportBatchItemFailures`. Hoy no se nota porque el código se traga los errores (ARQ-26); si se corrige solo el código, un registro que falle bloquearía el shard indefinidamente | `mappings.json` | Pérdida silenciosa hoy; bloqueo del stream si solo se arregla el código |
| ARQ-44 | **El job de notificación nuevo no es compatible con el trigger actual.** El trigger le pasa `--REDIRECT_BASE_URL` y `--SHORT_LINKS_TABLE`, que el job nuevo rechaza a propósito (`NTF-ENT-004`, regla de una sola fuente); `--SNS_TOPIC_ARN` lo ignora | `notification_certification.py` L80-92; `config/ajustes.py` del job nuevo | El job nuevo y la orquestadora deben desplegarse juntos (o la orquestadora primero) |

## 3. Diseño propuesto de la orquestadora (propuesta)

**Entrada:** un solo mapping sobre el stream, que configura DevOps:
- filtro: `INSERT` o `MODIFY` con `NewImage.status` en `PENDING`, `CERTIFICATE` o `NO_CERTIFICATE`;
- `ReportBatchItemFailures` y división del lote activados;
- 3 a 5 reintentos y una edad máxima;
- cola de fallos (SQS) con alarma.

Cierra ARQ-26 y ARQ-43.

**Reglas comunes:**
- Solo actúa si el estado **cambió** (`OldImage.status` distinto de `NewImage.status`). Así no reacciona a otras actualizaciones del registro.
- Cada registro del lote se procesa aparte. Si uno falla, se devuelve en `batchItemFailures` con su número de secuencia y los demás siguen.
- La configuración sale del JSON global que el job de certificación dejó en el registro (`configBucket` y `globalConfigKey`). Así previsional, que usa otro JSON global, también funciona. Las variables de entorno solo apuntan al JSON global, y solo las usa el reprocesador.
- La carpeta del caso sale de `certification.report_folders`: la misma clave que usa el job de certificación, en un solo lugar (ARQ-24).
- Logs JSON en español con `ejecucion_id` (el `ejecucionId` del registro), `request_id` (el de Lambda), `cert_code`, `use_case_id` y `periodo_negocio`. Nunca la imagen del registro ni correos (SEG-12).

| Estado nuevo | Acción | Idempotencia |
|---|---|---|
| `PENDING` | Lanza el job de notificación una sola vez por `certCode`, con `--useCaseId`, `--certCode`, `--emailType`, `--CONFIG_BUCKET`, `--CONFIG_KEY`, `--GLOBAL_CONFIG_KEY` y `--REGENERACION_SIN_DATOS_NUEVOS` (del registro), más **`--EJECUCION_ID`** (el `ejecucionId` del registro; cierra el punto 2.3). Ya no depende de `detail#1` (ARQ-40) | **Candado real (ARQ-25):** antes de lanzar, escribe `sendLock` en todos los registros del `certCode` en una sola transacción, con la condición de que no exista. El primer evento gana y los demás se saltan. Si lanzar el job falla, libera el candado y devuelve el fallo para que se reintente |
| `CERTIFICATE` | Copia **solo el CSV del registro del evento** a `export/<carpeta>/<nameFile>` del bucket de transporte y lo marca `TRANSPORT`, con la condición `status = CERTIFICATE` | Copiar dos veces da el mismo resultado; si ya está en `TRANSPORT`, no hace nada. Cierra ARQ-41 sin cambiar el API |
| `NO_CERTIFICATE` | Mueve el CSV del registro a `<carpeta>/NO-CERTIFICADOS/` y lo marca `NO_TRANSPORT` con condición. **Dejar la malla pendiente en `prg-programaciones` espera D2**: queda un puerto sin implementar | Si el destino ya existe y el original no, solo marca. Un `AccessDenied` es un error, no un "no existe" (ARQ-42) |

**Reprocesador (B.3):** un disparo programado (EventBridge) busca registros en `PENDING` con más de N minutos y vuelve a lanzar el job, que es idempotente, hasta un máximo de intentos. Si se agotan, avisa a soporte por SES (D5, parametrizado). Ver la decisión O-2.

**Configuración nueva**, en el bloque `orchestrator` del JSON global, siguiendo la regla del 2026-11-11:
- `notification_job_name`, `transport_bucket` y `transport_prefix` (`export`);
- `rejected_folder` (`NO-CERTIFICADOS`) y `use_historic_source_path` (O-3);
- `reprocess_after_minutes`, `reprocess_max_attempts` y `alert_recipients`;
- y en `dynamodb`, `pending_index` (O-2).

Los nombres de los atributos del candado (`sendLock`, `sendLockDate`, `sendAttempts`, `sendAlertDate`) son contratos fijos en el código, no configuración.

Ya existentes y compartidas con los jobs: `buckets.reports`, `dynamodb.notification_table`, `dynamodb.cert_code_index`, `certification.report_folders` y `timezone`.

## 4. Decisiones

**Aprobadas por el usuario el 2026-11-11:** O-1 a O-4 según la recomendación. D2 y D7 siguen abiertas.

| ID | Pregunta | Recomendación |
|---|---|---|
| O-1 | ¿Cómo hacer el candado de `PENDING`? | Una transacción sobre los registros del `certCode` (hasta 100). Otra opción es un registro de candado aparte, pero es una tabla o un tipo de registro más |
| O-2 | ¿Cómo encuentra el reprocesador los `PENDING` atascados? La tabla solo tiene el índice por `certCode` | Un índice nuevo por `status` (lo crea DevOps). Hacer un `Scan` cuesta más y crece con la tabla |
| O-3 | ¿Se conserva la búsqueda en la ruta histórica `<useCaseId>/<nameFile>`? | Sí, mientras queden archivos viejos sin certificar. Retirarla cuando no queden |
| O-4 | El job de notificación nuevo rechaza los argumentos viejos (ARQ-44) | Mantener el rechazo y desplegar la orquestadora y el job nuevo en el mismo cambio, con vuelta atrás conjunta |
| D2 | No certificar: ¿qué se escribe en `prg-programaciones`? | Sigue esperando a Víctor y negocio |
| D7 | ¿El API de certificar o rechazar va en la orquestadora? | Fuera de esta etapa: la orquestadora solo reacciona al stream |

## 5. Siguiente paso

**Etapa 2 hecha (2026-11-11):** ver el README de la orquestadora. Lo que sigue es el trabajo de DevOps (índice, mapping, Scheduler y permisos) y la prueba en DEV. Texto original del plan: el código en `Backend\notificacion\lambdas\niif-dev-notification-certification-orchestrator\` con la misma estructura de los jobs, pruebas y un solo README. Antes conviene decidir O-1 a O-4. Lo que depende de D2 queda como puerto sin implementar.
