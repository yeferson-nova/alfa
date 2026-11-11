# niif-dev-notification-certification-orchestrator

**Estado (2026-11-11):** código listo en local, con 404 pruebas y 97,9 % de cobertura. **No está desplegado.** Es el Paso 4, etapa 2, del roadmap (`Tareas\T1-notificaciones\roadmap-notificaciones.md`). La auditoría previa está en `Docs\06-brechas\auditoria-orquestadora-trigger-copy-move.md`.

| | |
|---|---|
| Reemplaza a | `niif-dev-notification-certification-trigger`, `niif-dev-copy-s3-cert` y `niif-dev-move-s3-certification` (decisión D1) |
| Handler | `lambda_function.lambda_handler` (Python 3.12; el código es compatible con 3.10) |
| Entradas | 1) Stream de DynamoDB de `niif-<amb>-notification-certification`, con un solo mapping. 2) Disparo programado (EventBridge Scheduler) para el reprocesador |
| Repo de DevOps | `sc-datalake-niif-notification-certification-orchestrator` |
| Brechas que cierra | ARQ-23 (parcial, espera D2), ARQ-24, ARQ-25, ARQ-26, ARQ-40, ARQ-41, ARQ-42, ARQ-43 y SEG-12 |

## Qué hace

Solo actúa cuando el **estado cambia**, es decir, cuando `OldImage.status` es distinto de `NewImage.status`. Las otras actualizaciones del registro (el candado, `SENT`, `TRANSPORT`) no disparan nada.

| Estado nuevo | Acción | Idempotencia |
|---|---|---|
| `PENDING` | Busca todos los registros del `certCode` del mismo caso y los **bloquea en una sola transacción**: escribe `sendLock` y `sendLockDate`, suma 1 a `sendAttempts` y exige que `sendLock` no exista. Si lo logra, lanza el job de notificación con los datos del registro y `--EJECUCION_ID`. No depende del nombre de la consulta (`detail#1`, ARQ-40) | El primer evento gana y los demás registran `ORQ_CANDADO_OCUPADO`. Si lanzar el job falla, libera el candado y devuelve el registro para reintento |
| `CERTIFICATE` | Copia **solo el CSV del registro del evento** a `<transport_bucket>/<transport_prefix>/<carpeta>/<nameFile>` y lo marca `TRANSPORT` con la condición `status = CERTIFICATE` | Copiar dos veces da lo mismo. Si el estado ya cambió, registra `ORQ_ESTADO_YA_CAMBIADO` y no falla |
| `NO_CERTIFICATE` | Mueve el CSV a `<carpeta>/<rejected_folder>/<nameFile>` en el bucket de reportes y lo marca `NO_TRANSPORT` con condición. Registra `ORQ_PROGRAMACION_PENDIENTE_D2`, porque dejar la malla pendiente en `prg-programaciones` espera la decisión D2 | Si el destino ya existe, no copia otra vez; borra el original si sigue ahí. Un `AccessDenied` es un error, no un "no existe" (ARQ-42) |

**Origen del CSV:** primero `<carpeta>/<nameFile>`. Si `use_historic_source_path` es `true`, también busca en `<useCaseId>/<nameFile>`, la ruta histórica (decisión O-3). La carpeta sale de `certification.report_folders`, la misma clave del job de certificación (ARQ-24).

**Lote del stream:** procesa los registros en orden. Al primer fallo se detiene y devuelve `batchItemFailures` con ese número de secuencia. Lambda reintenta desde ahí, y como cada acción es idempotente no se repiten efectos. Si un registro no trae `SequenceNumber`, falla la invocación completa.

**Reprocesador (evento programado, cualquier evento sin `Records`):**
1. Lee el JSON global de las variables `CONFIG_BUCKET` y `GLOBAL_CONFIG_KEY`.
2. Consulta el índice `dynamodb.pending_index`: `status = PENDING` y `pendingDate` anterior a ahora menos `reprocess_after_minutes`.
3. Descarta los registros con un `sendLockDate` reciente; esto evita relanzar un job que todavía corre.
4. Agrupa por caso y `certCode`.
5. Si `sendAttempts` es menor que `reprocess_max_attempts`, toma el candado con la condición `sendLock = <valor visto>` y `status = PENDING`, y relanza el job.
6. Si se agotaron los intentos, envía **una sola** alerta por SES (D5) a `alert_recipients` y marca `sendAlertDate`.
7. Si algún grupo falla, sigue con los demás y al final lanza `ORQ-PLA-005`, para que la invocación cuente como error.

`reprocess_max_attempts` cuenta el total de lanzamientos, incluido el del stream. `reprocess_after_minutes` debe ser mayor que el tiempo máximo del job de notificación.

## Configuración

Sigue la regla del 2026-11-11 (`.claude\skills\niif17-alfa\references\configuracion-por-ambiente.md`): **sin valores por defecto**, una sola fuente por valor, y validación completa antes de cualquier efecto.

### Variables de entorno (solo apuntadores, solo para el reprocesador)
| Variable | Obligatoria | Significado | Ejemplo DEV |
|---|---|---|---|
| `CONFIG_BUCKET` | Sí para el reprocesador | Bucket del JSON global | `niif-dev-assets` |
| `GLOBAL_CONFIG_KEY` | Sí para el reprocesador | Ruta del JSON global | `configs/certification/json_global_notif_cert.json` |

En el stream no se usan: el JSON global sale del propio registro (`configBucket` y `globalConfigKey`, que escribe el job de certificación). Así previsional, que usa otro JSON global, también funciona. Dentro de una invocación, cada JSON se lee una sola vez.

### Claves del JSON global que lee
| Clave | Obligatoria | Significado | Ejemplo DEV |
|---|---|---|---|
| `environment` | Sí | Ambiente, va en el asunto de la alerta | `dev` |
| `timezone` | Sí | Zona horaria de `sendLockDate` y del corte del reprocesador | `America/Bogota` |
| `buckets.reports` | Sí | Bucket de los CSV de certificación | `niif-dev-certification` |
| `dynamodb.notification_table` | Sí | Tabla de notificaciones | `niif-dev-notification-certification` |
| `dynamodb.cert_code_index` | Sí | Índice por `certCode` (compartido con los jobs) | `certCode-index` |
| `dynamodb.pending_index` | Sí | **Índice nuevo** por `status` y `pendingDate` (decisión O-2) | `status-pendingDate-index` |
| `certification.pending_status`, `certificate_status`, `no_certificate_status`, `transport_status`, `no_transport_status` | Sí | Nombres de los estados; deben ser distintos | `PENDING`, `CERTIFICATE`, ... |
| `certification.report_folders` | Sí | Carpeta de cada `useCaseId` (compartida con el job de certificación) | `{"primasArl": "primasArl", ...}` |
| `notification.ses_sender` | Sí | Remitente de la alerta (el mismo del job de notificación) | `notificaciones@...` |
| `notification.ses_configuration_set` | Sí (puede ser `null`) | Conjunto de configuración de SES | `niif-dev-notificaciones` |
| `orchestrator.notification_job_name` | Sí | Job de Glue de notificación | `niif-dev-notification-certification` |
| `orchestrator.transport_bucket` | Sí | Bucket de transporte hacia FIS | `niif-dev-transport` |
| `orchestrator.transport_prefix` | Sí | Prefijo dentro del bucket de transporte | `export` |
| `orchestrator.rejected_folder` | Sí | Subcarpeta de los no certificados | `NO-CERTIFICADOS` |
| `orchestrator.use_historic_source_path` | Sí (`true`/`false`) | Buscar también en `<useCaseId>/<nameFile>` (O-3) | `true` |
| `orchestrator.reprocess_after_minutes` | Sí (> 0) | Minutos sin avance para considerar atascado un `PENDING` | `60` |
| `orchestrator.reprocess_max_attempts` | Sí (> 0) | Lanzamientos máximos antes de alertar | `3` |
| `orchestrator.alert_recipients` | Sí (lista no vacía) | Correos de soporte para la alerta | `["<CORREO_SOPORTE_NIIF>"]` (**falta el valor real**) |

### Atributos del registro que lee o escribe
- **Lee:** `useCaseId`, `executionId`, `status`, `certCode`, `nameFile`, `emailType`, `period`, `configBucket`, `configKey`, `globalConfigKey`, `regeneracionSinDatosNuevos` y `ejecucionId`.
- **Escribe:**
  - `sendLock`, `sendLockDate` y `sendAttempts`: el candado;
  - `sendAlertDate`: la alerta enviada;
  - `status`: `TRANSPORT` o `NO_TRANSPORT`.
- Estos nombres son contratos y están fijos en `domain/records.py`.

### Argumentos con los que lanza el job de notificación
`--useCaseId`, `--certCode`, `--emailType`, `--CONFIG_BUCKET`, `--CONFIG_KEY`, `--GLOBAL_CONFIG_KEY`, `--REGENERACION_SIN_DATOS_NUEVOS` y, si el registro lo tiene, `--EJECUCION_ID`.

Ya no pasa `--REDIRECT_BASE_URL`, `--SHORT_LINKS_TABLE` ni `--SNS_TOPIC_ARN`: el job nuevo rechaza los dos primeros (ARQ-44).

## Salidas
- **Stream:** `{"batchItemFailures": [{"itemIdentifier": "<SequenceNumber>"}]}`; la lista va vacía si todo salió bien.
- **Reprocesador:** `{"relanzados": n, "omitidos": n, "alertados": n}`.
- **Logs:** una línea JSON por evento, con mensajes en español y los campos `request_id`, `ejecucion_id`, `cert_code`, `use_case_id` y `periodo_negocio`. Nunca registra la imagen del registro ni los correos (SEG-12).

## Eventos de log
| Evento | Cuándo |
|---|---|
| `ORQ_INICIO_LOTE` / `ORQ_RESUMEN_LOTE` | Inicio y fin del lote del stream (registros, procesados, ignorados, fallidos, `duracion_ms`) |
| `ORQ_REGISTRO_IGNORADO` | El estado no cambió, no es de la cadena o es un `REMOVE` |
| `ORQ_CANDADO_OCUPADO` | Otro evento ya lanzó la notificación del `certCode` |
| `ORQ_ID_FLUJO_AUSENTE` (WARN) | El registro no tiene `ejecucionId`; se lanza sin `--EJECUCION_ID` |
| `ORQ_NOTIFICACION_LANZADA` | Se lanzó el job (`job_run_id`, `registros_bloqueados`) |
| `ORQ_CANDADO_LIBERADO` (WARN) | Falló el lanzamiento y se liberó el candado |
| `ORQ_ARCHIVO_TRANSPORTADO` / `ORQ_ARCHIVO_NO_CERTIFICADO` | Copia a transporte o movimiento a rechazados |
| `ORQ_ESTADO_YA_CAMBIADO` | La condición de estado no se cumplió (otro proceso ya lo cambió) |
| `ORQ_PROGRAMACION_PENDIENTE_D2` (WARN) | Recordatorio: falta dejar la malla pendiente (D2) |
| `ORQ_ERROR_REGISTRO` (ERROR) | Falló un registro del stream; se devuelve para reintento |
| `ORQ_INICIO_REPROCESO` / `ORQ_RESUMEN_REPROCESO` | Inicio y fin del reprocesador |
| `ORQ_PENDIENTE_ATASCADO` (WARN) | `certCode` en `PENDING` por más tiempo del permitido |
| `ORQ_REINTENTOS_AGOTADOS` (ERROR) / `ORQ_ALERTA_ENVIADA` | Intentos agotados y alerta por SES |
| `ORQ_ERROR_REPROCESO` (ERROR) | Falló un `certCode` en el reprocesador |
| `ORQ_ERROR_INVOCACION` (ERROR) | Falló la invocación completa (también al arrancar) |

## Errores
Cada error lleva `codigo`, `dominio`, `severidad`, `recuperable`, `accion` y `destinatario`.

| Código | Dominio | Causa |
|---|---|---|
| `ORQ-ENT-001` | ENTRADA | Lote sin `SequenceNumber` o `Records` que no es una lista |
| `ORQ-ENT-002` | ENTRADA | Registro con formato inválido: sin `NewImage`, sin llaves, con un tipo de atributo distinto o con un `eventName` desconocido |
| `ORQ-CFG-001` | CONFIGURACION | Falta una clave del JSON global, tiene un valor inválido, el JSON está mal formado o la zona horaria no existe |
| `ORQ-CFG-002` | CONFIGURACION | El `useCaseId` no tiene carpeta en `certification.report_folders` |
| `ORQ-CFG-003` | CONFIGURACION | Faltan `CONFIG_BUCKET` o `GLOBAL_CONFIG_KEY` en el reprocesador |
| `ORQ-CFG-004` | CONFIGURACION | Recurso de AWS no encontrado (bucket, tabla, índice o job de Glue) |
| `ORQ-CFG-005` | CONFIGURACION | La ruta del JSON apunta a otro bucket |
| `ORQ-DAT-001` | DATOS | El registro no tiene `configBucket` o `globalConfigKey` |
| `ORQ-DAT-002` | DATOS | El CSV no está en ninguna ruta de origen |
| `ORQ-DAT-003` | DATOS | `nameFile` vacío o con rutas |
| `ORQ-DAT-004` | DATOS | Faltan datos para lanzar la notificación (`certCode`, `emailType`, ...) |
| `ORQ-DAT-005` | DATOS | El `certCode` tiene más de 100 registros: no caben en una transacción |
| `ORQ-CON-001` | CONCURRENCIA | Conflicto de transacción en DynamoDB (recuperable) |
| `ORQ-PER-001` | PERMISOS | `AccessDenied` en S3, DynamoDB, Glue o SES |
| `ORQ-PLA-001` | PLATAFORMA | Limitación, concurrencia de Glue o red (recuperable) |
| `ORQ-PLA-004` | PLATAFORMA | Otro error de AWS |
| `ORQ-PLA-005` | PLATAFORMA | Fallaron uno o más `certCode` en el reprocesador |
| `ORQ-PLA-999` | PLATAFORMA | Error no clasificado |

## Módulos
| Capa | Archivo | Contenido |
|---|---|---|
| — | `src/lambda_function.py` | Punto de entrada que pide DevOps; solo llama al adaptador de entrada |
| domain | `errors.py` | Errores clasificados y `classify` |
| domain | `models.py` | Modelos inmutables: registro, cambio del stream, candado, configuración, resultados |
| domain | `configuration.py` | Lectura estricta del JSON global |
| domain | `records.py` | Nombres de los atributos (contratos) y lectura de imágenes de DynamoDB y de eventos del stream |
| domain | `decisions.py` | Qué acción corresponde a cada cambio de estado |
| domain | `naming.py` | Carpeta del caso, rutas de origen, transporte y rechazados, y validación de `nameFile` |
| domain | `job_arguments.py` | Argumentos del job de notificación |
| domain | `alerts.py` | Texto de la alerta de intentos agotados, sin datos personales |
| application | `ports.py` | Puertos (`Protocol`), catálogo de eventos de log y `Dependencies` |
| application | `process_stream.py` | Caso de uso del lote del stream |
| application | `launch_notification.py` | Candado y lanzamiento del job (lo usa también el reprocesador) |
| application | `move_files.py` | Transporte de certificados y archivo de no certificados |
| application | `reprocess_pending.py` | Reprocesador de `PENDING` atascados |
| application | `global_configs.py`, `tracing.py` | Caché del JSON global por invocación y campos de traza del log |
| adapters/inbound | `lambda_handler.py` | Distingue stream y disparo programado, arma la respuesta y registra los errores |
| adapters/outbound | `dynamodb_notifications.py` | Consultas paginadas, candado transaccional y cambios condicionales |
| adapters/outbound | `s3_store.py` | JSON, `exists` (404 → no existe; 403 → error), copiar y borrar |
| adapters/outbound | `glue_jobs.py`, `ses_alerts.py`, `system.py`, `aws_errors.py` | Glue, SES v2, reloj y log JSON, y traducción de errores de AWS |
| config | `settings.py`, `composition.py` | Variables de entorno y raíz de composición (clientes reutilizados entre invocaciones) |

Las pruebas están en `test/`: dominio, casos de uso con dobles en memoria (`fakes.py`), adaptadores con `Stubber`, handler, arquitectura y calidad de código.

Para ejecutarlas: `python -m pytest --cov`, o `tox`.

Para empaquetar: `scripts/package.sh` (o `package.ps1`) genera `dist/niif-dev-notification-certification-orchestrator-1.0.0.zip`.

## Lo que debe hacer DevOps (Terraform)
1. **Índice nuevo (O-2):** en `niif-<amb>-notification-certification`, el índice global `status-pendingDate-index`:
   - llave de partición `status` (S) y de orden `pendingDate` (S);
   - proyección `ALL`;
   - el mismo modo de cobro que la tabla.

   DynamoDB lo llena en segundo plano, sin detener la tabla.
2. **La función:**
   - handler `lambda_function.lambda_handler`, Python 3.12, 120 s y 256 MB (propuesta);
   - formato de log `Text`;
   - variables `CONFIG_BUCKET` y `GLOBAL_CONFIG_KEY`.
3. **Mapping del stream** (`NEW_AND_OLD_IMAGES`):
   - filtro `eventName` en `INSERT`/`MODIFY` y `NewImage.status` en `PENDING`, `CERTIFICATE` o `NO_CERTIFICATE`;
   - `ReportBatchItemFailures`, `BisectBatchOnFunctionError`, 5 reintentos y edad máxima de 1 día;
   - cola SQS de fallos con alarma.

   Ver `config/lambda.dev.json`.
4. **Programación:** EventBridge Scheduler con `rate(30 minutes)` y entrada `{}`.
5. **Permisos mínimos del rol:**
   - `dynamodb:Query` sobre los dos índices;
   - `dynamodb:UpdateItem` sobre la tabla (también lo usa `TransactWriteItems`);
   - lectura del stream;
   - `s3:GetObject` del JSON global;
   - `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` y **`s3:ListBucket`** en el bucket de reportes. Sin `ListBucket`, S3 responde 403 en lugar de 404 y un archivo faltante se ve como error de permisos;
   - `s3:PutObject` en el bucket de transporte;
   - `glue:StartJobRun` sobre el job de notificación;
   - `ses:SendEmail` con la identidad del remitente.
6. **Despliegue conjunto (O-4):** se crea con el mapping deshabilitado, se apagan los mappings de trigger, copy y move, se despliega el job de notificación nuevo y se activa el mapping nuevo. La vuelta atrás es el paso inverso, también conjunta.

## Antes de desplegar
- Agregar `dynamodb.pending_index` y el bloque `orchestrator` a **todos** los JSON globales que se usan, incluido el de previsional (`previsional/config/json_global_notif_cert.json`). El de DEV del repositorio (`Backend\S3\niif-dev-assets\configs\certification\json_global_notif_cert.json`) ya los tiene, salvo el correo real de `alert_recipients`.
- Confirmar que el job de certificación desplegado escribe `pendingDate`. El nuevo sí; el viejo **no está verificado**. Los `PENDING` sin `pendingDate` no entran al índice y hay que resolverlos a mano una vez.
- Prueba en DEV con Primas ARL, con confirmación previa: `PENDING` → correo; certificar → `export/`; no certificar → `NO-CERTIFICADOS`; y un `PENDING` atascado → reproceso y alerta.
