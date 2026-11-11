# niif-dev-certification

Job de Glue que **genera los archivos de certificación** NIIF 17 de una malla y un periodo. Por cada consulta de detalle del caso:
1. ejecuta un `UNLOAD` en Athena;
2. arma el CSV final en el bucket de reportes;
3. crea un registro en DynamoDB.

Al final deja los registros del `certCode` en `PENDING`. Ese cambio de estado es el que dispara la notificación por correo (`niif-dev-notification-certification`).

- **Lugar en la arquitectura:** hoja `Página-2` del diagrama, flecha 43 (Malla → Modelo Notificaciones), componente 3 "Certificación Cifras" de la hoja `Página-10`.
- **Reemplaza a:** `Backend\Glue\niif-dev-certification\niif_certification.py` (lo desplegado más INC-3, decisión E1). Mantiene su contrato y agrega el ID del flujo (decisiones E2 y E3).
- **Estado (2026-11-11):** implementado y probado en local (412 pruebas, 97 % de cobertura). **No desplegado ni probado en AWS.**

## Contenido
1. [Configuración: argumentos y JSON](#configuración-argumentos-y-json)
2. [Salidas](#salidas)
3. [Flujo](#flujo)
4. [Qué hay en cada módulo](#qué-hay-en-cada-módulo)
5. [Errores](#errores)
6. [Logs y trazabilidad](#logs-y-trazabilidad)
7. [Qué cambia frente al job desplegado](#qué-cambia-frente-al-job-desplegado)
8. [Hallazgos y riesgos](#hallazgos-y-riesgos)
9. [Pruebas, empaquetado y despliegue](#pruebas-empaquetado-y-despliegue)
10. [Lo que debe hacer DevOps (solicitud)](#lo-que-debe-hacer-devops-solicitud)

## Configuración: argumentos y JSON

Regla del proyecto: la configuración vive en un **JSON en S3 por ambiente**, con la fuente en Git y desplegado por el pipeline de DevOps. Los argumentos solo llevan lo que cambia en cada ejecución y dónde está el JSON. No hay valores por defecto: si falta una clave, el job falla al arrancar y el error nombra la clave.

### Argumentos de entrada

| Argumento | Obligatorio | Quién lo pone | Qué significa | Ejemplo |
|---|---|---|---|---|
| `--PERIOD` | Sí | Step Function, en cada ejecución | Periodo que se certifica, en formato `AAAAMM` (mes de 01 a 12). Se usa como `$period` en las consultas y en el nombre del archivo | `202606` |
| `--CONFIG_KEY` | Sí | Step Function | Ruta del JSON del caso (la malla) dentro de `CONFIG_BUCKET` | `primas-arl/primasArl.json` |
| `--EJECUCION_ID` | No (debe llegar) | Step Function: `$$.Execution.Name` | ID único del flujo: el nombre de la ejecución `<malla>-<periodo>-<uuid v7>` (E2). Se guarda como `ejecucionId` en cada registro y va en cada línea de log. **Si no llega, no se inventa**: se registra `CER_ID_FLUJO_AUSENTE` y los registros quedan sin `ejecucionId` | `primasArl-202606-0190…` |
| `--EJECUCIONES_ORIGEN` | No | Evaluador compuesto | Solo en casos compuestos: IDs de todas las ejecuciones que formaron el grupo, separados por comas (E3). Se guarda como la lista `ejecucionesOrigen` | `id-rentas,id-endosos` |
| `--REGENERACION_SIN_DATOS_NUEVOS` | No | Step Function | `true` si se regenera la certificación sin datos nuevos. Cambia el formato del `certCode` (agrega segundos y un sufijo único) y se guarda en el registro. Por defecto `false` | `false` |
| `--CONFIG_BUCKET` | Sí | Argumento fijo del job (Terraform) | Bucket de la configuración | `niif-dev-assets` |
| `--GLOBAL_CONFIG_KEY` | Sí | Argumento fijo del job (Terraform) | Ruta del JSON global del ambiente | `configs/certification/json_global_notif_cert.json` |
| `--JOB_RUN_ID` | No | Glue | ID de la corrida de Glue. Se registra como `request_id` | `jr_…` |

Las Step Functions de DEV también envían `--ENVIRONMENT`, `--LOG_LEVEL` y `--LOTE_CARGUE_ID`. **El job no los usa**, igual que el desplegado: el ambiente sale del JSON global (`environment`). Se pueden retirar de las Step Functions sin efecto.

Ejemplos de invocación: `eventos\argumentos-job.dev.json` (caso simple) y `eventos\argumentos-job-compuesto.dev.json` (caso compuesto). Definición del job para DevOps: `config\job.dev.json`.

### JSON global (`GLOBAL_CONFIG_KEY`)

Todas las claves son obligatorias. Ejemplo de DEV: `Backend\S3\niif-dev-assets\configs\certification\json_global_notif_cert.json`.

| Clave | Qué significa | Regla | Ejemplo DEV |
|---|---|---|---|
| `environment` | Ambiente; es `$env` en las consultas | Texto | `dev` |
| `catalog` | Catálogo de Athena | Texto | `AwsDataCatalog` |
| `timezone` | Zona horaria del `certCode`, del nombre del archivo y de las fechas del registro | Zona IANA válida | `America/Bogota` |
| `buckets.assets` | Bucket donde Athena deja la salida y los `UNLOAD` | Texto | `niif-dev-assets` |
| `buckets.reports` | Bucket donde queda el CSV final | Texto | `niif-dev-certification` |
| `dynamodb.notification_table` | Tabla `notification-certification` | Texto | `niif-dev-notification-certification` |
| `dynamodb.cert_code_index` | Índice de esa tabla por `certCode` (lo usa también el job de notificación) | Texto | `certCode-index` |
| `athena.output_path` | Prefijo de salida de Athena dentro de `buckets.assets` | Texto | `athena/notifications` |
| `athena.unload_path` | Prefijo del `UNLOAD`; es `$unload_path` en las consultas | Texto | `athena/notifications/unload` |
| `console_url.base_url` | Portal SSO que abre la consola de S3 en el enlace del archivo. **Antes estaba quemado en el código** | Texto | `https://<portal>/start/#/console` |
| `console_url.account_id`, `console_url.role_name` | Cuenta y rol con los que el portal SSO abre la consola | Texto | `<cuenta>`, `ALFA-DEV-DATALAKE-ADM` |
| `console_url.region` | Región de la consola de S3 | Texto | `us-east-1` |
| `s3_export.csv_content_type` | `ContentType` del CSV final | Texto | `text/csv; charset=utf-8` |
| `s3_export.csv_encoding_bom` | Si el CSV empieza con BOM UTF-8 (para Excel) | `true` o `false` | `true` |
| `s3_export.field_delimiter` | Separador del encabezado que se agrega al CSV | Un carácter | `\|` |
| `certification.default_user_email` | Valor de `userEmail` en el registro | Texto | `SYSTEM_GLUE_JOB` |
| `certification.processed_status` | Estado con el que se crea cada registro | Texto | `PROCESSED` |
| `certification.pending_status` | Estado final de los registros del `certCode` | Texto | `PENDING` |
| `certification.report_folders` | Carpeta del bucket de reportes para cada `useCaseId`. **Antes estaba quemado en el código.** Un `useCaseId` que no esté aquí falla con `CER-CFG-003` antes de consultar | Objeto `useCaseId → carpeta` | `{"primasArl": "primasArl", …}` |
| `certification.athena_poll_seconds` | Segundos entre cada consulta del estado de Athena (antes, 2 fijos) | Número > 0 | `2` |
| `certification.athena_max_wait_seconds` | Tiempo máximo de una consulta. Al pasarlo se cancela en Athena y el error es recuperable (`CER-PLA-005`). Antes esperaba sin límite | Número ≥ `athena_poll_seconds` | `900` |
| `certification.index_visibility_attempts` | Intentos para ver el `certCode` en el índice antes de marcar `PENDING` (antes, 6 fijos) | Entero > 0 | `6` |
| `certification.index_visibility_wait_seconds` | Espera entre esos intentos (antes, 2 fijos) | Número > 0 | `2` |

El JSON trae otras claves (`notification`, `paths`, `region` y otros estados de `certification`) que este job no lee.

### JSON del caso (`CONFIG_KEY`)

| Clave | Obligatoria | Qué significa |
|---|---|---|
| `useCaseId` | Sí | Malla. Elige la carpeta en `certification.report_folders` y va en cada registro |
| `emailType` | Sí | Tipo de correo. Solo `cert` (sin importar mayúsculas) genera `certCode` y marca `PENDING`; con otro valor se crean los registros sin `certCode` |
| `queries.detail` | Sí, al menos una | Consultas `UNLOAD`. Cada una tiene `queryName` (obligatorio), `query` (obligatorio) y `database` (opcional). Se ejecutan en orden de `queryName` |
| `athena.defaultDatabase` | Si alguna consulta no trae `database` | Base de datos por defecto |
| `email.tableVars` | No | Encabezados del CSV. Para cada archivo se busca la tabla cuyo `queryName` sea igual al grupo del archivo (`202606-detail_1-…` → `detail`). Se usan los `columns` (`name` o la llave) unidos con `field_delimiter`. No se agregan si `export` es `false` o si no hay `columns` |

**Variables de las consultas:** `$useCaseId`, `$period`, `$periodRange` (`01/06/2026 - 30/06/2026`), `$env`, `$emailType`, `$execution_id` (ID único de la consulta), `$assets_bucket`, `$report_bucket` y `$unload_path`. Si la consulta usa otra variable, o un `$` suelto, el job falla con `CER-CFG-004` antes de ir a Athena. Para escribir un `$` literal se usa `$$`.

## Salidas

| Salida | Dónde | Contenido |
|---|---|---|
| **CSV de certificación**, uno por consulta de detalle | `s3://<buckets.reports>/<carpeta del caso>/<PERIOD>-<queryName normalizado>-<AAAAMMDD-HHMMSS>.csv` | BOM (si aplica), encabezado de `tableVars` (si aplica) y las filas del `UNLOAD`, descomprimidas y en orden de nombre de fragmento. Si Athena no dejó archivos y se confirma que no hay datos, solo lleva el encabezado de columnas |
| **Registro de notificación**, uno por consulta | Tabla `dynamodb.notification_table` | `useCaseId`, `executionId` (UUID de la consulta), `s3_url` (enlace del portal SSO a la consola de S3), `status` (`PROCESSED`), `period`, `emailType`, `queryName`, `userEmail`, `nameFile`, `creationDate`, `configBucket`, `configKey`, `globalConfigKey`, `regeneracionSinDatosNuevos`, `sinDatosPeriodo`, `certCode` (si es `cert`), **`ejecucionId`** (si llegó) y **`ejecucionesOrigen`** (si llegó) |
| **Paso a `PENDING`** | La misma tabla | Al final, todos los registros del `certCode` y del `useCaseId` pasan a `status = PENDING`, con `pendingDate`, `configBucket`, `configKey` y `globalConfigKey`. **Este cambio es el que inicia la notificación** |
| **Logs** | CloudWatch Logs del job | Una línea JSON por evento (ver [Logs y trazabilidad](#logs-y-trazabilidad)) |
| **Estado de la corrida** | Glue | `SUCCEEDED`, o `FAILED` con el error clasificado en el log |

`sinDatosPeriodo` es `true` si el `UNLOAD` no dejó archivos (confirmado) o si el CSV tiene encabezado configurado y cero filas de datos (INC-3).

## Flujo

1. Lee y valida los argumentos (`PERIOD` como `AAAAMM`).
2. Lee y valida el JSON global completo. Si algo falla, el error queda en el log como JSON y el job termina.
3. Lee el JSON del caso y busca la carpeta de reportes del `useCaseId` (falla antes de consultar si no existe).
4. Genera el `certCode` (mismo formato del desplegado):
   - normal: `<PERIOD>-<AAAAMMDD-HHMM>`;
   - con regeneración: `<PERIOD>-<AAAAMMDD-HHMMSS>-<uuid4 hex>`;
   - sin `certCode` si `emailType` no es `cert`.
5. Por cada consulta de detalle, en orden de `queryName`:
   1. sustituye las variables;
   2. corre el `UNLOAD` y espera, con un tiempo máximo;
   3. lista los fragmentos del `UNLOAD`, sin manifiesto ni metadatos. Si no hay ninguno, **confirma que no hay datos** antes de crear un archivo vacío: la consulta terminó bien, el manifiesto existe y está vacío, el destino del `UNLOAD` es el esperado y una consulta de prueba (`LIMIT 1`) no devuelve filas. Ante cualquier duda, falla (`CER-DAT-001`);
   4. arma el CSV, cuenta las filas de datos, agrega el encabezado y lo escribe en el bucket de reportes;
   5. crea el registro `PROCESSED`.
6. Si hay `certCode`: espera a que aparezca en el índice (reintentos configurables) y marca sus registros como `PENDING`.

## Qué hay en cada módulo

```
src/
  niif_certification.py          Script del job (ScriptLocation). Solo llama al adaptador de entrada
  certification_job/
    domain/                      Reglas puras, sin AWS: configuración, periodo, certCode, nombres, CSV, UNLOAD, errores
    application/                 Caso de uso GenerateCertification, confirmación de "sin datos" y puertos (Protocol)
    adapters/
      inbound/glue_job.py        Punto de entrada: arma el contexto, ejecuta, registra el error y el resumen
      outbound/                  Athena, S3, DynamoDB, reloj, IDs y bitácora JSON
    config/                      Lectura de argumentos y raíz de composición
  niif_observability/            Librería común de logs (copia idéntica en cada paquete de la cadena)
test/                            Pruebas (dominio, aplicación, adaptadores con Stubber, entrada, arquitectura y calidad)
config/job.dev.json              Definición del job para DevOps (Terraform)
eventos/                         Ejemplos de argumentos de una ejecución
scripts/package.sh, package.ps1  Empaquetado
```

Regla de dependencias: `domain ← application ← adapters`; solo `config` conecta todo. `test\test_architecture.py` la verifica.

### domain

Reglas puras del job, sin AWS ni efectos. Todo se prueba sin red ni dobles.

| Módulo | Qué contiene | Entrada | Salida |
|---|---|---|---|
| `errors.py` | Taxonomía de errores: `CertificationError` y sus clases `InputError`, `ConfigurationError`, `DataError`, `AccessDeniedError` y `PlatformError`. Cada una define dominio, severidad, si es recuperable, la acción y el destinatario | Código `CER-*` y mensaje | `as_log_fields()`: los campos del log de error (`codigo`, `dominio`, `severidad`, `recuperable`, `accion`, `destinatario`, `detalle`) |
| `models.py` | Contratos inmutables (`dataclass(frozen=True)`): `JobParameters`, `GlobalConfig` (con `ConsoleSettings`, `ExportSettings`, `CertificationSettings`, `WaitPolicy` y `RetryPolicy`), `CaseConfig`, `DetailQuery`, `HeaderSpec`, `QueryExecution`, `QueryDetails`, `ProbeResult`, `ExportedFile`, `NotificationKey` y `CertificationResult` | — | — |
| `configuration.py` | Lectura estricta de los JSON. `read_global_config` exige todas las claves (sin valores por defecto) y valida tipos y rangos. `read_case_config` lee `useCaseId`, `emailType`, `queries.detail` (ordenadas por `queryName`), `athena.defaultDatabase` y los encabezados de `email.tableVars` | El JSON ya convertido a `dict` | `GlobalConfig` o `CaseConfig`; si algo falta, `CER-CFG-001` o `CER-CFG-002` |
| `periods.py` | `validate_period` (`AAAAMM` con mes de 01 a 12) y `period_range` (`01/06/2026 - 30/06/2026`) | Texto del periodo | Periodo validado (o `CER-ENT-002`) y su rango |
| `cert_code.py` | `build_cert_code`: el mismo formato del job desplegado. Normal: `<periodo>-<AAAAMMDD-HHMM>`; con regeneración: `<periodo>-<AAAAMMDD-HHMMSS>-<sufijo>`; `None` si el tipo de correo no es `cert` | Periodo, tipo de correo, fecha local, marca de regeneración y sufijo único | `certCode` o `None` |
| `templating.py` | `render`: decodifica entidades HTML y sustituye las variables en modo estricto | Texto, variables y una etiqueta para el error | Texto final; `CER-CFG-004` si falta una variable o hay un `$` inválido |
| `naming.py` | Nombres y rutas: la carpeta del caso (`report_folder`, desde la configuración), el nombre del CSV, el prefijo del `UNLOAD`, el armado y la separación de rutas `s3://`, el filtro de fragmentos (sin manifiesto ni metadatos) y el enlace del portal SSO a la consola de S3 (`console_url`) | Configuración y datos de la consulta | Textos; `CER-CFG-003` o `CER-CFG-005` si no aplican |
| `csv_report.py` | Armado del CSV: grupo de la consulta según el nombre del archivo (`query_group`), encabezados del grupo (`headers_for_group`), unión de fragmentos con BOM y descompresión gzip (`assemble`), agregar encabezado sin duplicarlo (`add_headers`), conteo de filas de datos de INC-3 (`count_data_rows`) y encabezado de columnas de Athena (`column_header_csv`) | Bytes y encabezados | Bytes del CSV y número de filas (o `None` si no hay encabezado configurado) |
| `unload.py` | `parse_unload`: separa la consulta interna y el destino `TO '...'` de un `UNLOAD` con un analizador léxico que no cuenta paréntesis dentro de textos, identificadores o comentarios. `probe_query`: la consulta de prueba `SELECT * FROM (...) LIMIT 1` | SQL ya sustituido | `UnloadStatement(inner_query, target)`; `CER-DAT-001` si el formato no se reconoce |

### application

Casos de uso y puertos. Coordina el dominio con el mundo exterior solo a través de interfaces (`Protocol`); no importa boto3 ni los adaptadores.

| Módulo | Qué contiene | Entrada | Salida |
|---|---|---|---|
| `ports.py` | Puertos: `ConfigRepository`, `QueryRunner` (correr, describir y probar consultas), `ObjectStore` (listar, leer y escribir objetos), `NotificationRepository` (crear registros, buscar por `certCode` y marcar `PENDING`), `Clock`, `IdGenerator`, `Sleeper` y `EventLog`. También el catálogo cerrado de eventos `LogEvent` (`CER_*`) y sus mensajes en español (`EVENT_MESSAGES`) | — | — |
| `generate_certification.py` | Caso de uso `GenerateCertification` y sus dependencias (`Dependencies`). `run()` hace el flujo completo: fija la trazabilidad, lee el caso, valida la carpeta, genera el `certCode`, procesa cada consulta de detalle (sustituye, consulta, arma el CSV, lo escribe y crea el registro `PROCESSED` con `ejecucionId` y `ejecucionesOrigen`) y al final marca `PENDING` con reintentos sobre el índice | `JobParameters`, `GlobalConfig` ya validada y `Dependencies` | `CertificationResult(cert_code, files)`. Efectos: CSV en S3, registros en DynamoDB y eventos de log. Deja en `files` lo generado, aun si falla, para el resumen |
| `empty_result.py` | `EmptyResultConfirmer.confirm`: antes de crear un CSV vacío comprueba que la consulta terminó bien, que el manifiesto existe y está vacío, que el destino del `UNLOAD` es el prefijo esperado, y que la consulta de prueba no devuelve filas y sí trae columnas | Base de datos, SQL, ID de la consulta en Athena y ruta esperada | Encabezado CSV con los nombres de columna; `CER-DAT-001` ante cualquier duda |

### adapters

Conexión con el exterior. `inbound` recibe la ejecución de Glue; `outbound` implementa los puertos de `application` con AWS. Solo `outbound` y `config` usan boto3.

#### adapters/inbound

| Módulo | Qué contiene | Entrada | Salida |
|---|---|---|---|
| `glue_job.py` | `run`: lee los argumentos, arma el contexto, ejecuta el caso de uso y registra siempre `CER_RESUMEN_JOB`. Si falla, registra `CER_ERROR_JOB` con el error clasificado (`classify`: lo no previsto es `CER-PLA-999`) y lo relanza para que Glue marque la corrida como fallida. Los errores de arranque (argumentos o JSON global) también salen en JSON, con `ejecucion_id` y `request_id` si venían | `sys.argv` (o una lista en las pruebas) | `CertificationResult`, o la excepción relanzada |

#### adapters/outbound

| Módulo | Implementa | Qué hace | Errores |
|---|---|---|---|
| `aws_errors.py` | — | `translate` y `translating` convierten `ClientError` y `BotoCoreError` en errores `CER-*`: acceso denegado (`CER-PER-001`), recurso inexistente (`CER-CFG-007`), límite de AWS (`CER-PLA-001`, recuperable) y otros (`CER-PLA-004`). El mensaje lleva la operación y el código de AWS, sin datos | — |
| `athena.py` | `QueryRunner` | `AthenaQueryRunner`: `run` inicia la consulta con el catálogo y la salida configurados y espera con un número máximo de consultas de estado (`athena_max_wait_seconds / athena_poll_seconds`); al pasarlo, la cancela con `StopQueryExecution`. `describe` devuelve el estado y el manifiesto. `probe` lee hasta 2 filas y los nombres de las columnas | `CER-PLA-002` (falló o se canceló), `CER-PLA-005` (tiempo máximo, recuperable) |
| `s3_store.py` | `ConfigRepository` y `ObjectStore` | `S3ObjectStore`: `load_json` (acepta una llave o `s3://` del mismo bucket), `read`, `list_keys` (todas las páginas) y `write` con `ContentType` | `CER-CFG-001` (JSON inválido), `CER-CFG-005` (ruta de otro bucket) |
| `dynamodb_notifications.py` | `NotificationRepository` | `DynamoNotificationRepository` con el cliente de bajo nivel y `TypeSerializer`: `put` crea el registro; `find_by_cert_code` consulta el índice (todas las páginas) y filtra por `useCaseId`; `mark_pending` actualiza `status`, `pendingDate`, `configBucket`, `configKey` y `globalConfigKey` de cada llave | Los de `aws_errors.py` |
| `system.py` | `Clock`, `IdGenerator`, `EventLog` | `SystemClock` (hora local en la zona del JSON global; `CER-CFG-001` si la zona no existe), `UuidGenerator` (UUID4 para `executionId` y el sufijo del `certCode`) y `JsonEventLog` (escribe cada evento con `niif_observability.StructuredLogger`) | — |

### config

Lectura de argumentos y raíz de composición. Es la única capa que conoce a todas las demás.

| Módulo | Qué contiene | Entrada | Salida |
|---|---|---|---|
| `settings.py` | `read_parameters`: lee con `argparse` (`parse_known_args`, sin abreviaturas) los argumentos del job. Obligatorios: `PERIOD`, `CONFIG_BUCKET`, `CONFIG_KEY` y `GLOBAL_CONFIG_KEY`. Opcionales: `REGENERACION_SIN_DATOS_NUEVOS`, `EJECUCION_ID`, `EJECUCIONES_ORIGEN` (separados por comas) y `JOB_RUN_ID`. Ignora los argumentos propios de Glue y los que mandan las Step Functions sin uso (`ENVIRONMENT`, `LOG_LEVEL`, `LOTE_CARGUE_ID`) | Lista de argumentos | `JobParameters`; errores `CER-ENT-001` a `CER-ENT-004` |
| `composition.py` | `build_context`: crea la sesión de boto3; lee y valida el JSON global; arma el reloj con su zona horaria, la bitácora JSON (`niif_certificacion`), Athena (catálogo, salida y espera), S3 y DynamoDB; y entrega el caso de uso listo | `JobParameters` | `Context(use_case, log)` |

Los valores de cada ambiente **no** viven aquí: vienen del JSON global en S3 (ver [Configuración](#configuración-argumentos-y-json)).

## Errores

Código `CER-<DOM>-<NNN>`. El job registra el error en JSON y lo relanza, para que Glue marque la corrida como fallida.

| Código | Dominio | Cuándo | ¿Recuperable? |
|---|---|---|---|
| `CER-ENT-001` | ENTRADA | Falta un argumento obligatorio o viene vacío | No |
| `CER-ENT-002` | ENTRADA | `PERIOD` no es `AAAAMM` con un mes válido | No |
| `CER-ENT-003` | ENTRADA | `REGENERACION_SIN_DATOS_NUEVOS` no es `true` ni `false` | No |
| `CER-ENT-004` | ENTRADA | `EJECUCIONES_ORIGEN` con elementos vacíos o repetidos | No |
| `CER-CFG-001` | CONFIGURACION | Falta una clave del JSON, el valor no es del tipo o del rango esperado, la zona horaria no existe o el JSON es inválido | No |
| `CER-CFG-002` | CONFIGURACION | El caso no tiene `queries.detail` | No |
| `CER-CFG-003` | CONFIGURACION | El `useCaseId` no tiene carpeta en `certification.report_folders` | No |
| `CER-CFG-004` | CONFIGURACION | Una consulta usa una variable sin valor o un `$` inválido | No |
| `CER-CFG-005` | CONFIGURACION | Ruta S3 de otro bucket o inválida | No |
| `CER-CFG-006` | CONFIGURACION | Una consulta no tiene `database` y el caso no tiene `athena.defaultDatabase` | No |
| `CER-CFG-007` | CONFIGURACION | Recurso de AWS inexistente (bucket, objeto, tabla) | No |
| `CER-DAT-001` | DATOS | No se pudo confirmar que el resultado esté vacío (consulta fallida, sin manifiesto, manifiesto con archivos, destino distinto, la prueba encontró datos o no hay columnas) | No |
| `CER-PER-001` | PERMISOS | Acceso denegado | No |
| `CER-PLA-001` | PLATAFORMA | Límite de AWS o error de red | Sí |
| `CER-PLA-002` | PLATAFORMA | La consulta de Athena falló o fue cancelada | No |
| `CER-PLA-004` | PLATAFORMA | Otro error de AWS | No |
| `CER-PLA-005` | PLATAFORMA | La consulta superó `athena_max_wait_seconds`; se cancela en Athena | Sí |
| `CER-PLA-006` | PLATAFORMA | El `certCode` no apareció en el índice tras los intentos configurados | Sí |
| `CER-PLA-999` | PLATAFORMA | Error inesperado | No |

## Logs y trazabilidad

Cada línea es un JSON con `marcaTiempo`, `nivel`, `evento`, `mensaje` (en español), `funcion` (`niif_certificacion`) y **siempre** los campos de trazabilidad:
- `ejecucion_id`: el `--EJECUCION_ID`;
- `request_id`: el `--JOB_RUN_ID`;
- `cert_code`;
- `use_case_id`;
- `periodo_negocio`.

Mientras no se conozcan, van en `null`. Los errores de arranque también salen en JSON, con `ejecucion_id` y `request_id` si venían en los argumentos.

| Evento | Cuándo | Campos propios |
|---|---|---|
| `CER_INICIO_JOB` | Al empezar | `regeneracion_sin_datos_nuevos` |
| `CER_ID_FLUJO_AUSENTE` (WARN) | No llegó `--EJECUCION_ID` | — |
| `CER_CONFIGURACION_CARGADA` | JSON del caso leído | `tipo_correo`, `consultas_detalle` |
| `CER_CERT_CODE_GENERADO` | Tras generar el `certCode` | `genera_cert_code` |
| `CER_CONSULTA_EJECUTADA` | Cada `UNLOAD` terminado | `consulta`, `id_consulta`, `bytes_escaneados` |
| `CER_UNLOAD_SIN_ARCHIVOS` | El `UNLOAD` no dejó fragmentos | `id_consulta` |
| `CER_SIN_DATOS_CONFIRMADO` | Se confirmó que no hay datos | `id_consulta` |
| `CER_ARCHIVO_GENERADO` | CSV escrito | `consulta`, `archivo`, `filas_datos`, `sin_datos` |
| `CER_REGISTRO_CREADO` | Registro `PROCESSED` creado | `consulta`, `execution_id_registro`, `estado`, `sin_datos` |
| `CER_INDICE_NO_VISIBLE` (WARN) | El `certCode` aún no está en el índice | `intento`, `intentos` |
| `CER_REGISTROS_PENDIENTES` | Registros marcados `PENDING` | `registros`, `estado` |
| `CER_FIN_JOB` | Al terminar bien | `archivos_generados` |
| `CER_ERROR_JOB` (ERROR) | Cualquier falla | `codigo`, `dominio`, `severidad`, `recuperable`, `accion`, `destinatario`, `detalle` (y `fase=arranque` si falló al arrancar) |
| `CER_RESUMEN_JOB` | Siempre, al final | `job_exitoso`, `job_fallido`, `archivos_generados`, `archivos_sin_datos`, `duracion_ms` (para filtros de métricas de CloudWatch Logs) |

No se registra el SQL (SEG-15), ni correos, ni datos de las filas.

## Qué cambia frente al job desplegado

**Se mantiene igual:**
- los argumentos que mandan las Step Functions;
- el formato del `certCode`;
- el nombre y la carpeta del archivo;
- el contenido del CSV, incluido el BOM y la regla de encabezados;
- la confirmación de "sin datos" y la regla INC-3;
- los campos del registro y el paso a `PENDING`;
- el formato del enlace `s3_url`.

| Tema | Desplegado | Ahora |
|---|---|---|
| ID del flujo | No existe | `ejecucionId` y `ejecucionesOrigen` en cada registro y `ejecucion_id` en cada log (E2, E3; ARQ-32) |
| Configuración | Mapa de carpetas y portal SSO quemados; valores por defecto si faltaba una clave | Todo en el JSON global, validado al arrancar, sin valores por defecto |
| Variables de las consultas | `safe_substitute`: una variable faltante quedaba como texto y la consulta corría igual (ARQ-38) | Falla con `CER-CFG-004` antes de consultar |
| Espera de Athena | Sin límite | Tiempo máximo configurable; al pasarlo cancela la consulta (`CER-PLA-005`) |
| Carpeta del caso | Se validaba después de correr la primera consulta | Se valida antes de consultar |
| Logs | Texto con el SQL completo | JSON en español, sin SQL, con los campos de trazabilidad |
| Errores | `RuntimeError` y `ValueError` sin clasificar | Taxonomía `CER-*` |
| Búsqueda en el índice | Solo la primera página | Todas las páginas |
| `PERIOD` | Sin validar | `AAAAMM` validado |
| Valores por defecto del job | `--PERIOD=202411` y el `CONFIG_KEY` de Auxilios | Ninguno: llegan en cada ejecución |
| Dependencias | Spark (`job.init` y `commit`), `commons.zip` y wheels de `openpyxl` | Solo Python y boto3; Spark no se inicializa |
| Código | Un script de 860 líneas que parcheaba `s3.put_object` | Hexagonal, PEP 8 en inglés, funciones cortas y 412 pruebas |

## Hallazgos y riesgos

Se conservaron por paridad y se registran para decidir. No se corrigieron porque cambian el resultado que hoy ve el negocio.

| ID | Hallazgo | Efecto | Propuesta |
|---|---|---|---|
| CER-H1 | En Reserva Matemática, `tableVars.queryName` es `detail#1`, pero el grupo del archivo es `detail` | El CSV de Reserva sale **sin encabezado** y la regla INC-3 no aplica (no se pueden contar las filas) | Corregir el JSON del caso (`queryName: "detail"`) tras validarlo con negocio |
| CER-H2 | Si se confirma "sin datos" y el caso tiene encabezado en `tableVars` | El CSV queda con **dos encabezados**: el de `tableVars` (separado por `\|`) y el de Athena (separado por comas, con `\r\n`) | Usar un solo encabezado: el de `tableVars` si existe |
| CER-H3 | El `certCode` normal tiene precisión de minuto | Dos corridas del mismo periodo en el mismo minuto comparten `certCode` y mezclan registros; el job no es idempotente | Usar el `ejecucionId` para detectar un reproceso (pendiente de decisión) |
| CER-H4 | El portal SSO estaba quemado; ahora sale de `console_url.base_url` | En DEV el JSON tiene el mismo valor. **En QA y PRD no está comprobado** | Verificar el JSON global de QA y PRD antes de desplegar |
| CER-H5 | **El job lo comparten otros equipos.** Lo lanzan también las Step Functions `niif-dev-generales`, `niif-dev-pcr-vida`, `niif-dev-satelites` y `niif-dev-primas-previsional`. Previsional usa otro JSON global: `previsional/config/json_global_notif_cert.json` | Si se despliega sin actualizar **todos** los JSON globales que usan el job, sus corridas fallan con `CER-CFG-001` | Desplegar con un nombre aparte para probar, y coordinar con los dueños de esas mallas antes de reemplazar el job |
| CER-H6 | Las Step Functions envían `--LOTE_CARGUE_ID`, que ningún job usa | Se pierde un dato de trazabilidad | Propuesta: guardarlo en el log (requiere decisión) |
| CER-H7 | El job es `glueetl` con 4 workers G.1X y no usa Spark | Casi todo el tiempo (mediana de 56 s) es el arranque de Spark | Pasarlo a Glue Python shell; medir antes |

## Pruebas, empaquetado y despliegue

```
python -m pip install -r requirements-dev.txt
python -m pytest --cov --cov-report=term-missing
bash scripts/package.sh                                       (o en Windows:)
powershell -ExecutionPolicy Bypass -File scripts\package.ps1
```

- **Cobertura mínima:** 80 % en `pyproject.toml`; hoy es 97 %.
- **`test\test_code_quality.py` exige:**
  - sin comentarios ni docstrings;
  - líneas de 120 caracteres como máximo;
  - nombres PEP 8;
  - sin cuentas, ARN, regiones, nombres de recursos, correos ni portales quemados;
  - funciones de 40 líneas como máximo y 8 parámetros como máximo;
  - sin excepciones silenciadas.
- **`tox.ini` y `sonar-project.properties`:** listos para el pipeline.
- **El empaquetado genera en `dist\`:**
  - `niif-dev-certification-1.0.0.zip` (`--extra-py-files`), con `certification_job` y `niif_observability`;
  - `niif_certification.py` (`ScriptLocation`).

  El mismo artefacto sirve para todos los ambientes; solo cambia el JSON global.

Los pasos de despliegue y lo que se le pide a DevOps están en la sección siguiente.

## Lo que debe hacer DevOps (solicitud)

El job, su rol, las Step Functions y los JSON de S3 los maneja DevOps con Terraform y su pipeline. Nosotros entregamos el código, el paquete y esta lista. **No se crea ni se cambia nada a mano en la consola.**

### 1. Job de prueba aparte en DEV (etapa 3)
Hace falta para probar sin afectar a las mallas que comparten el job (CER-H5).
- **Nombre propuesto:** `niif-dev-certification-v2` (a confirmar con DevOps).
- **Definición:** la de `config\job.dev.json`, con el `Name` cambiado. Llevar el mismo rol `niif-dev-jobs-niif-certification`, más el permiso del punto 4.
- **Artefactos:** subir los dos archivos de `dist\` a rutas propias de la prueba, sin pisar los del job actual:
  - `niif_certification.py` → `ScriptLocation`;
  - `niif-dev-certification-1.0.0.zip` → `--extra-py-files`.
- **Argumentos por defecto:** solo `--CONFIG_BUCKET` y `--GLOBAL_CONFIG_KEY`. **Sin `--PERIOD` ni `--CONFIG_KEY`**, que hoy traen valores de prueba quemados (`202411` y el caso de Auxilios). Sin `commons.zip` ni los wheels de `openpyxl`.

### 2. Claves nuevas en **todos** los JSON globales que usa el job
Se agregan antes de reemplazar el job; el job anterior las ignora.
- Claves: `dynamodb.cert_code_index` y, en `certification`, `report_folders`, `athena_poll_seconds`, `athena_max_wait_seconds`, `index_visibility_attempts` e `index_visibility_wait_seconds`. Los valores de DEV están en `Backend\S3\niif-dev-assets\configs\certification\json_global_notif_cert.json`.
- En DEV hay al menos dos JSON globales: `configs/certification/json_global_notif_cert.json` y `previsional/config/json_global_notif_cert.json`.
- En QA y PRD además hay que comprobar `console_url.base_url` (CER-H4).

### 2b. Índice nuevo en la tabla de notificaciones (decisión O-2)
Lo usa la orquestadora para encontrar los `PENDING` atascados y reprocesarlos; se pide aquí porque las columnas las escribe este job.

| Dato | Valor |
|---|---|
| Tabla | `niif-<amb>-notification-certification` (en DEV: `niif-dev-notification-certification`). Su llave primaria no cambia: `useCaseId` (S) + `executionId` (S) |
| Tipo | Índice secundario **global** (GSI), nuevo |
| Nombre | `status-pendingDate-index` (va en el JSON global como `dynamodb.pending_index`) |
| Llave de partición | columna `status`, tipo String |
| Llave de orden | columna `pendingDate`, tipo String: fecha ISO 8601 con zona, por ejemplo `2026-11-11T10:00:00-05:00` |
| Proyección | `ALL`: el reprocesador necesita `certCode`, `configBucket`, `globalConfigKey`, `sendLock`, `sendAttempts`, etc. |
| Capacidad | El mismo modo de cobro que la tabla (bajo demanda o aprovisionada) |
| Quién escribe esas columnas | El job de **certificación**, al marcar `PENDING`: pone `status = PENDING` y `pendingDate`. La orquestadora y el API cambian `status` después (`SENT`, `CERTIFICATE`, `TRANSPORT`...) |
| Quién lo lee | El **reprocesador de la orquestadora**: consulta `status = PENDING AND pendingDate < (ahora - reprocess_after_minutes)` |
| Permiso | `dynamodb:Query` sobre `arn:...:table/niif-<amb>-notification-certification/index/status-pendingDate-index` para el rol de la orquestadora |
| Efecto al crearlo | DynamoDB lo llena en segundo plano; la tabla sigue funcionando. Los registros sin `pendingDate` no entran al índice |

### 3. Reemplazo del job real (después de la prueba)
- Actualizar `niif-dev-certification` con la definición de `config\job.dev.json` y los artefactos nuevos.
- Hacerlo en el mismo cambio que la orquestadora y el job de notificación nuevo (decisión O-4).

### 4. Permiso nuevo del rol
`athena:StopQueryExecution`, para cancelar una consulta que supera `athena_max_wait_seconds`.

El resto ya lo tiene el rol del job desplegado:
- S3: leer la configuración; listar y leer el prefijo del `UNLOAD` y el manifiesto en `buckets.assets`; escribir en `buckets.reports`.
- Athena: `StartQueryExecution`, `GetQueryExecution` y `GetQueryResults`, el catálogo de Glue y la lectura de Gold.
- DynamoDB: `PutItem` y `UpdateItem` en la tabla, y `Query` sobre `certCode-index`.

### 5. Step Functions (etapa 4 del paso 3)
- Agregar a la tarea del job `"--EJECUCION_ID.$": "$$.Execution.Name"` en las 6 mallas `niif-dev-<malla>` y en las que comparten el job (generales, pcr-vida, satélites y previsional).
- `--EJECUCIONES_ORIGEN` lo pasará el evaluador compuesto, que todavía es solo un esqueleto.

### 6. Vuelta atrás
Apuntar `ScriptLocation` y `--extra-py-files` a los del job anterior. Las claves nuevas del JSON no afectan al job anterior.

### Preguntas para DevOps
| # | Pregunta | Por qué |
|---|---|---|
| 1 | ¿Se puede crear el job de prueba `niif-dev-certification-v2` en DEV, y con qué nombre? | Probar sin romper a generales, pcr-vida, satélites y previsional (CER-H5) |
| 2 | ¿Quiénes son los dueños de las Step Functions generales, pcr-vida, satélites y previsional? | Avisarles antes de reemplazar el job y agregar las claves a su JSON global |
| 3 | ¿El bucket `niif-<amb>-assets` tiene versionado? ¿Los JSON globales pueden pasar al repositorio de configuración y a su pipeline? | La regla de configuración por ambiente; es la pregunta 17 de `Docs\07-entregables\preguntas-devops-repos-y-despliegue.md` |
| 4 | ¿Cuál es el valor de `console_url.base_url` en los JSON globales de QA y PRD? | CER-H4: el portal SSO dejó de estar quemado |
| 5 | ¿Se puede pasar el job a Glue Python shell (CER-H7)? | No usa Spark; el arranque de Spark es casi todo su tiempo y costo |
