# data-datalake-niif-notification-certification-glue-job

Job de Glue `niif-<env>-notification-certification`. Arma el correo de certificación NIIF 17 y lo **envía por Amazon SES**: en texto y en HTML con la plantilla de la malla. Reemplaza al script único `src/jobs/notification_certification.py`, que tenía la misma lógica en un solo archivo.

**Estado:** listo para probar en QA. Cumple las mismas condiciones que el script único que ya se probó en QA el 2026-11-10. La equivalencia se comprobó con los JSON reales de `cfg-datalake-niif-config-s3` y las plantillas reales de `tpl-datalake-niif-email-template-s3`. Ver [Equivalencia con el script anterior](#equivalencia-con-el-script-anterior).

## Qué cambia frente al script anterior

| Tema | Antes | Ahora |
|---|---|---|
| Estructura | Un script de más de 700 líneas, ejecutado al importarse | Arquitectura hexagonal: dominio, aplicación, adaptadores y configuración |
| Valores fijos | Índice `certCode-index`, zona horaria, vigencia y longitud del enlace y espera de Athena, escritos en el código | Llegan por argumentos del job |
| Errores | Texto libre y `ValueError` | Taxonomía con código, dominio, severidad, recuperable, acción y destinatario |
| Logs | `print` de texto, con los correos de los destinatarios | Una línea JSON por evento, con `ejecucion_id` y sin correos (Ley 1581) |
| Athena | Espera sin límite y solo la primera página de resultados | Espera con tiempo máximo y lectura de todas las páginas |
| Idempotencia | Un reintento reenviaba el correo | Si todos los registros del `certCode` ya están enviados, no reenvía |
| Pruebas | Ninguna | Unitarias, de adaptadores (`Stubber`), de arquitectura, de calidad y de equivalencia |

**No cambia:**
- el contrato de negocio, es decir, los argumentos que manda la lambda trigger;
- el JSON global y el del caso, incluido `email.templatePath`;
- el texto, el asunto y el HTML del correo;
- los avisos de regeneración, de falta de datos y de disponibilidad por miembro;
- los enlaces cortos y el cierre de los grupos compuestos;
- los campos que se escriben en DynamoDB.

## Argumentos del job

La lambda `niif-<env>-notification-certification-trigger` lanza el job con los siguientes argumentos:
- `--useCaseId`, `--certCode` y `--emailType`;
- `--CONFIG_BUCKET`, `--CONFIG_KEY` y `--GLOBAL_CONFIG_KEY`;
- `--REDIRECT_BASE_URL` y `--SHORT_LINKS_TABLE`;
- `--REGENERACION_SIN_DATOS_NUEVOS`;
- `--SNS_TOPIC_ARN`, que el job ignora.

Los demás argumentos van en los **argumentos por defecto del job** (Terraform):

| Argumento | Obligatorio | Valor en QA | Para qué |
|---|---|---|---|
| `--SES_SENDER` | Sí | `notificaciones@segurosalfa.com.co` | Remitente verificado en SES |
| `--INDICE_CERT_CODE` | Sí | `certCode-index` | Índice de la tabla `niif-<env>-notification-certification` por `certCode` |
| `--ZONA_HORARIA` | Sí | `America/Bogota` | Fecha de envío y vencimiento de los enlaces |
| `--DIAS_VIGENCIA_ENLACE` | Sí | `7` | Días de vigencia de los enlaces cortos |
| `--LONGITUD_TOKEN_ENLACE` | Sí | `8` | Largo del token del enlace corto (de 1 a 32) |
| `--SEGUNDOS_ENTRE_CONSULTAS` | Sí | `1` | Pausa entre consultas del estado de Athena |
| `--SEGUNDOS_MAXIMOS_CONSULTA` | Sí | Lo define Desarrollo (ver nota) | Tiempo máximo de espera por consulta de Athena |
| `--SES_CONFIGURATION_SET` | No | El conjunto de la cuenta | Rebotes y quejas |
| `--extra-py-files` | Sí | `s3://niif-<env>-assets/glue-resources/libraries/niif-<env>-notification-certification-<versión>.zip` | Librería del job |

- Los seis primeros valores de QA son los que el script anterior tenía escritos en el código, así que dan el mismo comportamiento.
- **Nota:** el script anterior esperaba a Athena sin límite. `--SEGUNDOS_MAXIMOS_CONSULTA` agrega un tope. El valor lo debe acordar el equipo; no se toma por defecto.
- Si falta un argumento obligatorio, el job falla al arrancar con `NTF-ENT-001` y no envía nada.

**Claves que el job lee del JSON global:**
- `buckets.assets`
- `dynamodb.notification_table`
- `environment`
- `athena.output_path`
- `catalog` (opcional; si no está, usa `AwsDataCatalog`)
- `certification.sent_status` (opcional; si no está, usa `SENT`)

**Claves que lee del JSON del caso:**
- `useCaseId`
- `email.subject`, `email.stringVars` y `email.destinations`
- `email.templatePath` (opcional)
- `email.dataPresence` (opcional)
- `queries.summary` y `queries.total`
- `compositeNotification` (opcional)

## Correo HTML

- La plantilla se lee de `s3://<buckets.assets>/<email.templatePath>`. La sube el repo `tpl-datalake-niif-email-template-s3` en `email-template/`.
- Estos marcadores se llenan con fragmentos HTML que arma el job:
  - `$tableSummary` y `$tableTotal`: tablas de Athena.
  - `$tableDetail`: enlaces de descarga.
  - `$certificarLink` y `$nocertificarLink`: enlaces cortos.
  - `$avisoHtml`: recuadros de aviso. Azul para la regeneración y ámbar para la falta de datos o la disponibilidad.
- Las variables de `email.stringVars` y las del caso (`$period`, `$periodRange`, `$useCaseName`, etc.) se escapan antes de insertarse. Un marcador desconocido queda igual.
- Si no hay `templatePath`, se envía solo texto.
- Si la plantilla no se puede leer, se envía solo texto y se registra `NTF_PLANTILLA_NO_LEIDA` (WARN).
- Si hay avisos y la plantilla no tiene `$avisoHtml`, se envía solo texto para no ocultar el aviso, y se registra `NTF_PLANTILLA_SIN_MARCADOR` (WARN).

## Envío por destinatario

- Se envía un correo por destinatario.
- Si falla uno, el job sigue con los demás. El fallo se registra como `NTF_ENVIO_FALLIDO`, y al final queda `NTF_ENVIO_PARCIAL` con los enviados y los fallidos.
- En DynamoDB solo quedan los destinatarios a los que sí se les envió.
- Si fallan todos, el job relanza el último error y no marca los registros.

## Arquitectura

```
src/notificacion_certificacion/
  dominio/          Reglas puras: configuración, redacción, correo HTML, periodo, compuesto y errores
  aplicacion/       Caso de uso NotificarCertificacion, servicios y puertos (Protocol)
  adaptadores/
    entrada/        Punto de entrada del job de Glue
    salida/         SES, DynamoDB, Athena, S3, bitácora JSON y reloj
  configuracion/    Lectura de argumentos y raíz de composición
scripts/
  job_notificacion.py   Script del job (ScriptLocation); solo llama al punto de entrada
  empaquetar.sh         Arma la librería y el script
```

Regla de dependencias: `dominio <- aplicacion <- adaptadores`. Solo `configuracion` conoce a todos. La prueba `tests/test_arquitectura.py` la verifica.

## Errores

Cada error tiene un código `NTF-<DOM>-<NNN>`:

| Código | Dominio | Cuándo |
|---|---|---|
| `NTF-ENT-001` | ENTRADA | Argumento faltante o inválido |
| `NTF-ENT-002` | ENTRADA | Periodo con formato inválido |
| `NTF-ENT-003` | ENTRADA | Zona horaria inválida |
| `NTF-CFG-001` | CONFIGURACION | Falta una clave, un valor no tiene el tipo esperado o el JSON es inválido |
| `NTF-CFG-002` | CONFIGURACION | El `useCaseId` del JSON no coincide con el argumento |
| `NTF-CFG-003` | CONFIGURACION | Sin destinatarios |
| `NTF-CFG-004` | CONFIGURACION | Grupo compuesto inválido |
| `NTF-CFG-005` | CONFIGURACION | `email.dataPresence` inválido |
| `NTF-CFG-006` | CONFIGURACION | Ruta S3 de otro bucket |
| `NTF-CFG-007` | CONFIGURACION | No se pudo confirmar la disponibilidad de un miembro |
| `NTF-CFG-008` | CONFIGURACION | El recurso de AWS no existe |
| `NTF-CON-001` | CONCURRENCIA | El estado del grupo compuesto es inconsistente |
| `NTF-CON-002` | CONCURRENCIA | No se cumplió la condición de escritura |
| `NTF-PER-001` | PERMISOS | Acceso denegado |
| `NTF-PLA-001` | PLATAFORMA | Límite de AWS o error de red (recuperable) |
| `NTF-PLA-002` | PLATAFORMA | La consulta de Athena falló o se canceló |
| `NTF-PLA-003` | PLATAFORMA | SES no confirmó el identificador del mensaje |
| `NTF-PLA-004` | PLATAFORMA | Otro error de AWS |
| `NTF-PLA-005` | PLATAFORMA | La consulta de Athena superó el tiempo máximo |
| `NTF-PLA-999` | PLATAFORMA | Error inesperado |

El job registra el error y lo relanza, para que Glue marque la ejecución como fallida.

## Logs

- Cada evento escribe una línea JSON con `marcaTiempo`, `nivel`, `evento`, `funcion`, `ejecucion_id` (el `JOB_RUN_ID` de Glue) y el contexto.
- Eventos posibles:
  - `NTF_INICIO_JOB`, `NTF_CONFIGURACION_CARGADA` y `NTF_FIN_JOB`.
  - `NTF_SIN_REGISTROS`, `NTF_YA_ENVIADO` y `NTF_COMPUESTO_YA_COMPLETADO`.
  - `NTF_CONSULTA_EJECUTADA` y `NTF_CONSULTA_FALLIDA`.
  - `NTF_ENLACE_CREADO`.
  - `NTF_PLANTILLA_NO_LEIDA` y `NTF_PLANTILLA_SIN_MARCADOR`.
  - `NTF_CORREO_ENVIADO`, `NTF_ENVIO_FALLIDO` y `NTF_ENVIO_PARCIAL`.
  - `NTF_REGISTROS_MARCADOS` y `NTF_COMPUESTO_CERRADO`.
  - `NTF_ERROR_JOB`.
- No se registran direcciones de correo.

## Permisos del rol `niif-<env>-jobs-notification-certification`

- **SES:** `ses:SendEmail` y `ses:SendRawEmail` sobre la identidad del remitente y, si se usa, sobre el conjunto de configuración.
- **S3:**
  - `s3:GetObject` sobre los JSON de configuración y sobre `niif-<env>-assets/email-template/*`;
  - lectura de la librería y del script en `glue-resources/`.
- **DynamoDB:**
  - `Query` sobre el índice por `certCode` y `UpdateItem` en `niif-<env>-notification-certification`;
  - `PutItem` en la tabla de enlaces cortos;
  - `GetItem` y `TransactWriteItems` en la tabla del grupo compuesto.
- **Athena:**
  - `StartQueryExecution`, `GetQueryExecution` y `GetQueryResults`;
  - lectura del catálogo de Glue y de los datos de Gold;
  - escritura en la ruta de salida de Athena.
- **SNS:** los permisos actuales se conservan durante la validación, porque sirven para la reversa.

## Pruebas y calidad

Desde la raíz del repo:

```
python -m pip install -r requirements-dev.txt
python -m pytest
python -m pytest --cov=notificacion_certificacion --cov-config=pyproject.toml --cov-report=term-missing
```

- **Resultado local del 2026-11-12:** 477 pruebas en verde y 99,9 % de cobertura. El mínimo configurado es 90 %.
- **`tests/test_calidad_codigo.py` exige:**
  - sin comentarios ni docstrings;
  - sin valores de infraestructura escritos en el código;
  - funciones de hasta 40 líneas y hasta 8 parámetros;
  - ninguna excepción silenciada.
- `--cov-config=pyproject.toml` es necesario mientras exista el `tox.ini` del repo. Su `[coverage:run] omit = src/**.py` excluye todo el código y la versión actual de `coverage` lo rechaza.
- Las pruebas requieren Python 3.10 o superior, que es el de Glue 4.0. El `tox.ini` del repo declara `py38`, y el código no corre en 3.8.

## Empaquetado

```
bash scripts/empaquetar.sh qa
```

Genera dos archivos en `dist/`:

| Archivo | Destino en S3 | Uso en el job |
|---|---|---|
| `niif-qa-notification-certification-<versión>.zip` | `s3://niif-qa-assets/glue-resources/libraries/` | `--extra-py-files` |
| `job_notificacion.py` | `s3://niif-qa-assets/glue-resources/notification/` | `ScriptLocation` del job |

El código es el mismo en DEV, QA y PRD. Solo cambian el nombre del zip y los argumentos.

## Equivalencia con el script anterior

- **Cómo se comparó:** se corrieron los dos con los mismos datos, aislando AWS. Se compararon el destinatario, el asunto, el texto, el HTML, los registros marcados y los destinatarios guardados.
- **Datos usados:**
  - los 6 JSON de caso de `cfg-datalake-niif-config-s3`, con `{env}` igual a `dev`;
  - las 6 plantillas de `tpl-datalake-niif-email-template-s3`;
  - el JSON global de `previsional/config/`.
- **Escenarios:** normal, regenerado, sin datos total, sin datos parcial, consulta de Athena fallida y miembro sin datos.
- **Resultado:** 36 de 36 iguales.
- **Control del comparador:** un cambio a propósito en el estilo o en un aviso sí lo detecta (36 y 6 diferencias).
- **No se comparó:** el flujo del grupo compuesto contra DynamoDB. Lo cubren las pruebas unitarias del repo.
- **Diferencias intencionales.** Salieron de la auditoría del 2026-11-12. Ninguna cambia el flujo normal: el trigger solo lanza el job con registros `PENDING` de `detail#1`.

  | Tema | Script anterior | Modular |
  |---|---|---|
  | Relanzar a mano un `certCode` ya enviado | Reenvía el correo | No reenvía (`NTF_YA_ENVIADO`) |
  | Destinatario repetido en `email.destinations` | Envía dos veces | Envía una vez |
  | `email.destinations` vacío | Falla después de consultar Athena y de crear los enlaces | Falla al leer la configuración, antes de cualquier efecto |
  | Token de enlace corto repetido | Sobrescribe el enlace anterior | Falla (`NTF-CON-002`) y no envía |
  | Periodo con formato inválido | Error de Python | `NTF-ENT-002` |
  | Athena | Espera sin límite y lee solo la primera página (hasta 1000 filas) | Espera hasta el tope y lee todas las páginas |
  | `nameFile` vacío en un registro de detalle | Enlace sin nombre | `Archivo CSV` |
  | Logs | Texto, con correos y URL | JSON, sin correos |

## Despliegue con el pipeline actual (pendiente con DevOps)

El `Jenkinsfile` sincroniza `src/` con `s3://niif-<env>-assets/glue-resources/notification/`. Con este repo, eso sube la carpeta `notificacion_certificacion/` suelta. **No sube el zip ni `job_notificacion.py`**, que son lo que Glue necesita. Antes de desplegar, DevOps debe definir cómo se publican los dos archivos de `scripts/empaquetar.sh`.

## Vuelta atrás

1. Revertir el commit en el repo y desplegar. El script anterior (`src/jobs/notification_certification.py`, versión SES) vuelve a `glue-resources/notification/`.
2. Apuntar el `ScriptLocation` del job al script anterior y quitar `--extra-py-files`.

Los argumentos nuevos sobran pero no afectan al script anterior.
