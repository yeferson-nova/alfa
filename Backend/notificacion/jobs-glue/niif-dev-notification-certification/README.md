# niif-dev-notification-certification

Job de Glue que arma y **envía por Amazon SES** el correo de certificación NIIF 17. Reemplaza la versión anterior (`Backend\Glue\niif-dev-notification-certification\notification_certificacion.py`), que publicaba en SNS.

**Tarea:** T1, puntos 1a (SNS a SES) y 1b (HTML). **Estado:** implementado y probado en local (537 pruebas, 99 % de cobertura). **No desplegado ni probado contra AWS.** El job que está en QA sigue siendo el monolito de `Backend\Glue\niif-dev-notification-certification\`. Auditoría frente a ese job: `Docs\06-brechas\auditoria-job-notificacion-nuevo-vs-actual.md` (N-01, N-02, N-03, N-05, N-07 y O-01 a O-06 resueltos el 2026-11-10).

## Qué cambia frente al job anterior

| Tema | Antes | Ahora |
|---|---|---|
| Canal | `sns.publish` a un tópico, con `FilterPolicy` por destinatario reconfigurado en cada ejecución | SES (SESv2 `send_email`), un correo por destinatario, sin tópico ni suscripciones |
| Estructura | Un script de 600 líneas con todo mezclado, ejecutado al importarse | Arquitectura hexagonal: dominio, aplicación, adaptadores y configuración (ver abajo) |
| Valores fijos | Zona horaria, vigencia de enlaces, nombre del índice, longitud del token y tiempo de espera en el código | Todo viene del JSON global del ambiente, validado al arrancar y sin valores por defecto; el código no tiene cuentas, regiones, ARN, buckets ni tablas |
| Errores | Texto libre y `ValueError` | Taxonomía con código, dominio, severidad, recuperable, acción y destinatario |
| Logs | `print` de texto | Una línea JSON por evento, con mensaje en español y los campos de trazabilidad en todas las líneas (`ejecucion_id`, `request_id`, `cert_code`, `use_case_id`, `periodo_negocio`) |
| Consulta de Athena | Espera sin límite y solo la primera página | Espera con tiempo máximo y lectura de todas las páginas |
| Idempotencia | Un reintento reenviaba el correo | Si todos los registros del `certCode` ya están en estado enviado, no reenvía |
| Pruebas | Ninguna | Pruebas unitarias, de adaptadores (`Stubber`), de arquitectura y de calidad |

Se **mantiene** el contrato de negocio: mismos argumentos de cada ejecución (`useCaseId`, `certCode`, `emailType`, `CONFIG_KEY`, `REGENERACION_SIN_DATOS_NUEVOS`), mismo JSON del caso, mismo texto del correo, mismos avisos (regeneración, sin datos, disponibilidad por miembro), mismos enlaces cortos y mismo cierre de grupos compuestos.

## Configuración: argumentos y JSON

Regla del proyecto (2026-11-11): **la configuración vive en un JSON en S3 por ambiente**, con la fuente versionada en Git y desplegada por el pipeline de DevOps. Los argumentos del job solo llevan lo que cambia en cada ejecución y dónde está el JSON. Un valor está en un solo lugar. Si falta una clave o un valor no es válido, el job falla al arrancar con un error que nombra la clave; no hay valores por defecto. Para cambiar un valor (por ejemplo, el tiempo máximo de Athena) se cambia el JSON; no hace falta desplegar código.

### Argumentos de entrada

| Argumento | Obligatorio | Quién lo pone | Qué significa | Ejemplo |
|---|---|---|---|---|
| `--useCaseId` | Sí | La lambda que lanza el job, en cada ejecución | Caso de uso (malla). Debe coincidir con `useCaseId` del JSON del caso | `primasArl` |
| `--certCode` | Sí | Ídem | Código de certificación que se notifica. Con él se buscan los registros en `notification-certification` | `202606-20261110-1530` |
| `--emailType` | Sí | Ídem | Tipo de correo | `certification` |
| `--CONFIG_KEY` | Sí | Ídem | Ruta del JSON del caso dentro de `CONFIG_BUCKET` | `configs/certification/primas-arl/primasArl.json` |
| `--EJECUCION_ID` | No | Ídem | ID del flujo (nombre de la ejecución de la Step Function). Si no llega, se toma del campo `ejecucionId` del registro; si tampoco está, se registra `NTF_ID_FLUJO_AUSENTE`. **Nunca se inventa** | `primasArl-202606-<uuid v7>` |
| `--REGENERACION_SIN_DATOS_NUEVOS` | No | Ídem | `true` si es una regeneración sin datos nuevos (agrega el aviso). Por defecto `false` | `false` |
| `--CONFIG_BUCKET` | Sí | Argumento fijo del job (Terraform) | Bucket de la configuración | `niif-dev-assets` |
| `--GLOBAL_CONFIG_KEY` | Sí | Argumento fijo del job (Terraform) | Ruta del JSON global del ambiente | `configs/certification/json_global_notif_cert.json` |
| `--JOB_RUN_ID` | No | Glue | ID de la corrida; se registra como `request_id` | `jr_...` |

**Argumentos retirados:** `REDIRECT_BASE_URL`, `SHORT_LINKS_TABLE`, `SES_SENDER`, `SES_CONFIGURATION_SET`, `INDICE_CERT_CODE`, `ZONA_HORARIA`, `DIAS_VIGENCIA_ENLACE`, `LONGITUD_TOKEN_ENLACE`, `SEGUNDOS_ENTRE_CONSULTAS` y `SEGUNDOS_MAXIMOS_CONSULTA` pasaron al JSON global. Si alguno llega como argumento, el job falla con `NTF-ENT-004` y el mensaje dice en qué clave va ahora. Así no quedan dos fuentes para el mismo valor.

### JSON global (`GLOBAL_CONFIG_KEY`)

Todas las claves son obligatorias. Ejemplo de DEV: `Backend\S3\niif-dev-assets\configs\certification\json_global_notif_cert.json`.

| Clave | Qué significa | Regla | Ejemplo DEV |
|---|---|---|---|
| `environment` | Ambiente; se usa como `${env}` en las consultas | Texto | `dev` |
| `catalog` | Catálogo de datos de Athena | Texto | `AwsDataCatalog` |
| `timezone` | Zona horaria de las fechas de envío y de vigencia de enlaces | Zona IANA válida | `America/Bogota` |
| `buckets.assets` | Bucket de activos: plantillas HTML y salida de Athena | Texto | `niif-dev-assets` |
| `dynamodb.notification_table` | Tabla `notification-certification` | Texto | `niif-dev-notification-certification` |
| `dynamodb.cert_code_index` | Índice de esa tabla por `certCode` (lo usa también el job de certificación) | Texto | `certCode-index` |
| `athena.output_path` | Prefijo de salida de Athena dentro de `buckets.assets` | Texto | `athena/notifications` |
| `certification.sent_status` | Estado con el que se marca un registro enviado | Texto | `SENT` |
| `notification.short_links_table` | Tabla de enlaces cortos | Texto | `niif-dev-short-links` |
| `notification.redirect_base_url` | URL base del API que redirige los enlaces cortos | Texto | `https://<api>/dev/r` |
| `notification.ses_sender` | Remitente verificado en SES | Texto | `notificaciones@...` |
| `notification.ses_configuration_set` | Conjunto de configuración de SES (rebotes y quejas) | Texto, o `null` si no se usa | `niif-dev-notificaciones` |
| `notification.link_validity_days` | Días de vigencia de los enlaces | Entero > 0 | `7` |
| `notification.link_token_length` | Longitud del token del enlace corto (32 cierra SEG-13) | Entero de 1 a 32 | `32` |
| `notification.athena_poll_seconds` | Segundos entre consultas del estado de Athena | Número > 0 | `1` |
| `notification.athena_max_wait_seconds` | Tiempo máximo de una consulta; al pasarlo se cancela (`NTF-PLA-005`) | Número ≥ `athena_poll_seconds` | `900` |

El JSON trae otras claves (`console_url`, `s3_export`, `certification.*`) que usa el job de certificación; este job las ignora.

### JSON del caso (`CONFIG_KEY`)

| Clave | Obligatoria | Qué significa |
|---|---|---|
| `useCaseId` | Sí | Debe ser igual a `--useCaseId` (`NTF-CFG-002`) |
| `email.subject` | Sí | Asunto, con variables `${...}` |
| `email.destinations` | Sí | Destinatarios; se normalizan y se quitan los repetidos (`NTF-CFG-003` si queda vacía) |
| `email.stringVars` | No | Variables de texto del correo (nombre del caso, enlaces) |
| `email.templatePath` | No | Plantilla HTML en `buckets.assets`; sin ella el correo va en texto |
| `email.dataPresence` | No | Consultas de disponibilidad por miembro (`NTF-CFG-005`) |
| `queries.summary`, `queries.total` | No | Consultas del resumen y de los totales: `queryName`, `database`, `query` |
| `compositeNotification` | No | Si `enabled` es `true`, son obligatorios `tableName`, `groupId`, `controlMemberId` y `memberIds` (`NTF-CFG-004`) |

Los valores de DEV del job están en `config\job.dev.json`; el ejemplo de invocación, en `eventos\argumentos-job.dev.json`.

## Arquitectura

Misma estructura de carpetas que `niif-dev-certification` (estándar del 10/11: carpetas en inglés; el código interno de este job sigue en español hasta N-08):

```
src/
  job_notificacion.py            Script del job (ScriptLocation). Solo llama al adaptador de entrada
  notificacion_certificacion/
    domain/                      Reglas puras: configuración, redacción del correo, periodo, compuesto, errores
    application/                 Caso de uso NotificarCertificacion, servicios y puertos (Protocol)
    adapters/
      inbound/                   Punto de entrada del job de Glue
      outbound/                  SES, DynamoDB, Athena, S3, bitácora JSON, reloj
    config/                      Lectura de argumentos y raíz de composición
  niif_observability/            Librería común de logs (copia idéntica en cada paquete de la cadena)
test/                            Pruebas
config/job.dev.json              Definición del job para DevOps
eventos/                         Ejemplo de argumentos
scripts/package.sh, package.ps1  Empaquetado
```

Regla de dependencias: `domain <- application <- adapters`; solo `config` conoce a todos. La prueba `test\test_arquitectura.py` la verifica por análisis de imports.

Flujo del caso de uso:
1. Fija el contexto de trazabilidad.
2. Carga la configuración del caso (la global ya se leyó y validó al armar el job) y busca los registros por `certCode`.
3. Reserva el grupo compuesto si aplica.
4. Ejecuta las consultas de Athena.
5. Crea los enlaces cortos.
6. Arma el texto y el HTML (plantilla de `email.templatePath`).
7. Envía por SES, uno por destinatario. Si uno falla, sigue con los demás.
8. Marca los enviados y cierra el grupo compuesto.

Las consultas se parametrizan en modo estricto: si falta una variable, el job falla (`NTF-CFG-009`) en vez de consultar con `$period` literal.

## Cambios de contrato que hay que conocer

- DynamoDB `notification-certification`: `notificationChannel` pasa de `SNS_EMAIL` a `SES_EMAIL` y `snsMessageId` pasa a **`sesMessageId`**. Igual en la tabla del grupo compuesto. En el código extraído del repositorio ningún otro componente lee `snsMessageId`, pero las lambdas de la cadena final (`copy-s3-cert`, `move-s3-certification`, trigger) **no tienen código extraído**: hay que confirmarlo antes de desplegar.
- El asunto ya no se recorta a 100 caracteres (era una restricción de SNS).
- Se retiran del job las dependencias de `commons.zip` y de los wheels de `openpyxl`: este job no las usa.

## Errores

Cada error tiene código `NTF-<DOM>-<NNN>`:

| Código | Dominio | Cuándo |
|---|---|---|
| `NTF-ENT-001` | ENTRADA | Argumento faltante o inválido |
| `NTF-ENT-002` | ENTRADA | Periodo con formato inválido |
| `NTF-ENT-004` | ENTRADA | Llegó un argumento retirado, cuyo valor va ahora en el JSON global |
| `NTF-CFG-001` | CONFIGURACION | Falta una clave, el valor no es del tipo o del rango esperado (incluida una zona horaria inválida) o el JSON es inválido |
| `NTF-CFG-002` | CONFIGURACION | El `useCaseId` del JSON no coincide con el argumento |
| `NTF-CFG-003` | CONFIGURACION | Sin destinatarios |
| `NTF-CFG-004` | CONFIGURACION | Grupo compuesto inválido |
| `NTF-CFG-005` | CONFIGURACION | `email.dataPresence` inválido |
| `NTF-CFG-006` | CONFIGURACION | Ruta S3 de otro bucket |
| `NTF-CFG-007` | CONFIGURACION | No se pudo confirmar la disponibilidad de un miembro |
| `NTF-CFG-008` | CONFIGURACION | Recurso de AWS inexistente |
| `NTF-CON-001` | CONCURRENCIA | Estado del grupo compuesto inconsistente |
| `NTF-CON-002` | CONCURRENCIA | Condición de escritura incumplida |
| `NTF-PER-001` | PERMISOS | Acceso denegado |
| `NTF-PLA-001` | PLATAFORMA | Límite de AWS o error de red (recuperable) |
| `NTF-PLA-002` | PLATAFORMA | La consulta de Athena falló o fue cancelada |
| `NTF-PLA-003` | PLATAFORMA | SES no confirmó el identificador del mensaje |
| `NTF-PLA-004` | PLATAFORMA | Otro error de AWS |
| `NTF-PLA-005` | PLATAFORMA | La consulta de Athena superó el tiempo máximo. Se cancela en Athena y el error es **recuperable** |
| `NTF-CFG-009` | CONFIGURACION | Consulta con una variable sin valor o un marcador inválido |
| `NTF-PLA-006` | PLATAFORMA | SES no envió el correo a ningún destinatario |
| `NTF-PLA-999` | PLATAFORMA | Error inesperado |

El job registra el error y lo relanza, para que Glue marque la ejecución como fallida.

## Logs y trazabilidad

La bitácora usa la librería común `niif_observability`, que va copiada en `src\niif_observability\` (la misma copia en cada paquete de la cadena; ver `Backend\notificacion\LEEME.md`). Cada línea es un JSON con:
- `marcaTiempo`, `nivel`, `evento`, `mensaje` (en español) y `funcion`;
- **siempre** `ejecucion_id`, `request_id`, `cert_code`, `use_case_id` y `periodo_negocio` (en `null` mientras no se conozcan);
- el detalle del evento. El mensaje de un error va en `detalle`.

**Eventos:**
- `NTF_INICIO_JOB`, `NTF_CONFIGURACION_CARGADA`, `NTF_SIN_REGISTROS`, `NTF_YA_ENVIADO`, `NTF_COMPUESTO_YA_COMPLETADO`.
- `NTF_ID_FLUJO_AUSENTE` y `NTF_ID_FLUJO_DISTINTO`.
- `NTF_CONSULTA_EJECUTADA` (con `id_consulta` de Athena y `bytes_escaneados`) y `NTF_CONSULTA_FALLIDA`.
- `NTF_ENLACE_CREADO`, `NTF_AVISOS_AGREGADOS`, `NTF_PLANTILLA_NO_DISPONIBLE`, `NTF_FORMATO_CORREO` (`HTML` o `TEXTO`).
- `NTF_CORREO_ENVIADO`, `NTF_CORREO_FALLIDO`, `NTF_ENVIO_PARCIAL`, `NTF_REGISTROS_MARCADOS`, `NTF_COMPUESTO_CERRADO`.
- `NTF_FIN_JOB`, `NTF_ERROR_JOB` (también para los errores de arranque) y `NTF_RESUMEN_JOB`.

No se registran direcciones de correo ni el SQL (Ley 1581, SEG-15).

**Métricas:** en Glue las líneas EMF no se convierten en métricas (solo pasa en Lambda). Por eso el job emite `NTF_RESUMEN_JOB` con `job_exitoso`, `job_fallido`, `correos_enviados`, `correos_fallidos` y `duracion_ms`. DevOps las convierte en métricas con **filtros de métricas de CloudWatch Logs** sobre esos campos.

## Permisos mínimos del rol del job

- `ses:SendEmail` sobre la identidad remitente y, si se usa, el conjunto de configuración.
- DynamoDB: `Query` sobre el índice por `certCode` y `UpdateItem` en `notification-certification`; `PutItem` en la tabla de enlaces cortos; `GetItem` y `TransactWriteItems` en la tabla del grupo compuesto.
- Athena: `StartQueryExecution`, `GetQueryExecution`, `GetQueryResults` y `StopQueryExecution` (para cancelar una consulta que pasa del tiempo máximo); lectura del catálogo de Glue y de los datos de Gold; escritura en la ruta de salida de Athena.
- S3: lectura de `niif-dev-assets` (configuración).
- Se **retira** el permiso de publicar en el tópico SNS cuando termine la validación.

## Pruebas y calidad

Desde esta carpeta o desde la raíz del repositorio:

```
python -m pip install -r requirements-dev.txt
python -m pytest --cov --cov-report=term-missing --cov-report=xml:coverage.xml --junitxml=reports/pytest.xml
python -m pytest Backend/notificacion/jobs-glue/niif-dev-notification-certification
```

- Cobertura mínima configurada: 90 % (`pyproject.toml`).
- `test\test_calidad_codigo.py` exige: sin comentarios ni docstrings, sin valores quemados de infraestructura (cuentas, ARN, regiones, nombres de recursos, correos), funciones de menos de 40 líneas y sin excepciones silenciadas.
- `sonar-project.properties` está listo para SonarQube. Falta la línea base del repositorio (ver `pendientes`).
- `tzdata` es solo para pruebas locales en Windows; en Glue (Linux) la base de zonas horarias ya existe.

## Empaquetado y despliegue

```
bash scripts/package.sh
powershell -ExecutionPolicy Bypass -File scripts\package.ps1   (en Windows)
```

Genera en `dist\` la librería `niif-dev-notification-certification-<versión>.zip` (va en `--extra-py-files`) y el script `job_notificacion.py` (es el `ScriptLocation`). El mismo artefacto sirve para DEV, QA y PROD; cambia solo el JSON global de cada ambiente.

Antes de desplegar, en AWS:

1. Verificar la identidad del remitente en SES (dominio con SPF, DKIM y DMARC) y pedir la salida del modo *sandbox*. **El remitente de `job.dev.json` es una propuesta de T1.**
2. Crear el conjunto de configuración de SES y enviar rebotes y quejas a EventBridge o SNS con alerta.
3. Ajustar el rol del job (sección de permisos).
4. Subir la librería y el script a `niif-dev-assets/glue-resources/`.
5. Agregar el bloque `notification` al JSON global del ambiente (antes de actualizar el job).
6. Actualizar el job con `config\job.dev.json` por Terraform: **quitar** los 10 argumentos retirados, o el job falla con `NTF-ENT-004`.

**Vuelta atrás:** apuntar de nuevo el `ScriptLocation` y los argumentos al script anterior; el tópico SNS no se elimina hasta cerrar la validación.

## Pendiente

- **N-04:** el job no reenvía si todos los registros ya están en `SENT` (el desplegado sí). Confirmar con el equipo; si hace falta reenviar, que sea con una bandera explícita.
- **N-06:** acordar con DevOps la publicación (script + zip con `notificacion_certificacion` y `niif_observability`, ambos desde `src/`, que arma `scripts/package.sh`).
- **N-08:** llevar el código al estándar de hoy: identificadores en inglés y PEP 8, y `config/` de qa y prd. Ya hecho (2026-11-11): carpetas en inglés, `test/` en singular, punto de entrada en `src/`, `tox.ini` y `scripts/package.sh`.
- **ARQ-32:** para que llegue el `ejecucion_id`, el job de certificación debe guardar `ejecucionId` en `notification-certification` y el trigger debe pasar `--EJECUCION_ID`.
- **Tipo de job:** sigue siendo `glueetl` con Spark. Como ya no usa Spark, podría pasar a un job de Python (shell) o a una lambda para bajar el costo; requiere decisión y pruebas.
