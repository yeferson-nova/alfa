# niif-dev-notification-certification

Job de Glue que arma y **envía por Amazon SES** el correo de certificación NIIF 17. Reemplaza la versión anterior (`Backend\Glue\niif-dev-notification-certification\notification_certificacion.py`), que publicaba en SNS.

**Tarea:** T1, punto 1a (SNS a SES). **Estado:** propuesta implementada y probada en local; **no desplegada ni probada contra AWS**.

## Qué cambia frente al job anterior

| Tema | Antes | Ahora |
|---|---|---|
| Canal | `sns.publish` a un tópico, con `FilterPolicy` por destinatario reconfigurado en cada ejecución | SES (SESv2 `send_email`), un correo por destinatario, sin tópico ni suscripciones |
| Estructura | Un script de 600 líneas con todo mezclado, ejecutado al importarse | Arquitectura hexagonal: dominio, aplicación, adaptadores y configuración (ver abajo) |
| Valores fijos | Zona horaria, vigencia de enlaces, nombre del índice, longitud del token y tiempo de espera en el código | Todo llega por argumentos del job; el código no tiene cuentas, regiones, ARN, buckets ni tablas |
| Errores | Texto libre y `ValueError` | Taxonomía con código, dominio, severidad, recuperable, acción y destinatario |
| Logs | `print` de texto | Una línea JSON por evento, con `ejecucion_id` y catálogo cerrado de eventos |
| Consulta de Athena | Espera sin límite y solo la primera página | Espera con tiempo máximo y lectura de todas las páginas |
| Idempotencia | Un reintento reenviaba el correo | Si todos los registros del `certCode` ya están en estado enviado, no reenvía |
| Pruebas | Ninguna | Pruebas unitarias, de adaptadores (`Stubber`), de arquitectura y de calidad |

Se **mantiene** el contrato de negocio: mismos argumentos de entrada (`useCaseId`, `certCode`, `emailType`, `CONFIG_BUCKET`, `CONFIG_KEY`, `GLOBAL_CONFIG_KEY`, `REDIRECT_BASE_URL`, `SHORT_LINKS_TABLE`, `REGENERACION_SIN_DATOS_NUEVOS`), mismo JSON de configuración global y del caso, mismo texto del correo, mismos avisos (regeneración, sin datos, disponibilidad por miembro), mismos enlaces cortos y mismo cierre de grupos compuestos.

## Argumentos del job

Obligatorios:

| Argumento | Para qué |
|---|---|
| `--useCaseId`, `--certCode`, `--emailType` | Identifican la solicitud (los envía la lambda `niif-dev-notification-certification-trigger`) |
| `--CONFIG_BUCKET`, `--CONFIG_KEY`, `--GLOBAL_CONFIG_KEY` | Ubicación de la configuración global y del caso |
| `--REDIRECT_BASE_URL`, `--SHORT_LINKS_TABLE` | Enlaces cortos |
| `--SES_REMITENTE` | Dirección remitente, verificada en SES |
| `--INDICE_CERT_CODE` | Índice de `notification-certification` por `certCode` |
| `--ZONA_HORARIA` | Zona horaria de las fechas de envío y de vigencia de enlaces |
| `--DIAS_VIGENCIA_ENLACE`, `--LONGITUD_TOKEN_ENLACE` | Política de enlaces cortos (longitud entre 1 y 32) |
| `--SEGUNDOS_ENTRE_CONSULTAS`, `--SEGUNDOS_MAXIMOS_CONSULTA` | Espera de Athena |

Opcionales: `--SES_CONFIGURATION_SET` (rebotes y quejas), `--REGENERACION_SIN_DATOS_NUEVOS` (`true` o `false`, por defecto `false`) y `--JOB_RUN_ID` (lo entrega Glue; se usa como `ejecucion_id`).

Los valores de DEV están en `config\job.dev.json`; el ejemplo de invocación, en `eventos\argumentos-job.dev.json`.

## Arquitectura

```
src/notificacion_certificacion/
  dominio/          Reglas puras: configuración, redacción del correo, periodo, compuesto, errores
  aplicacion/       Caso de uso NotificarCertificacion, servicios y puertos (Protocol)
  adaptadores/
    entrada/        Punto de entrada del job de Glue
    salida/         SES, DynamoDB, Athena, S3, bitácora JSON, reloj
  configuracion/    Lectura de argumentos y raíz de composición
```

Regla de dependencias: `dominio <- aplicacion <- adaptadores`; solo `configuracion` conoce a todos. La prueba `tests\test_arquitectura.py` la verifica por análisis de imports.

Flujo del caso de uso: carga la configuración, busca los registros por `certCode`, reserva el grupo compuesto si aplica, ejecuta las consultas de Athena, crea los enlaces cortos, arma el mensaje, lo envía por SES, marca los registros como enviados y cierra el grupo compuesto.

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
| `NTF-ENT-003` | ENTRADA | Zona horaria inválida |
| `NTF-CFG-001` | CONFIGURACION | Falta una clave, el valor no es del tipo esperado o el JSON es inválido |
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
| `NTF-PLA-005` | PLATAFORMA | La consulta de Athena superó el tiempo máximo |
| `NTF-PLA-999` | PLATAFORMA | Error inesperado |

El job registra el error y lo relanza, para que Glue marque la ejecución como fallida.

## Logs

Una línea JSON por evento: `marcaTiempo`, `nivel`, `evento`, `funcion`, `ejecucion_id` y el contexto. Los eventos son `NTF_INICIO_JOB`, `NTF_CONFIGURACION_CARGADA`, `NTF_SIN_REGISTROS`, `NTF_YA_ENVIADO`, `NTF_COMPUESTO_YA_COMPLETADO`, `NTF_CONSULTA_EJECUTADA`, `NTF_CONSULTA_FALLIDA`, `NTF_ENLACE_CREADO`, `NTF_CORREO_ENVIADO`, `NTF_REGISTROS_MARCADOS`, `NTF_COMPUESTO_CERRADO`, `NTF_FIN_JOB` y `NTF_ERROR_JOB`. No se registran direcciones de correo (Ley 1581). No se emiten métricas EMF: en Glue no se procesan como en Lambda.

## Permisos mínimos del rol del job

- `ses:SendEmail` sobre la identidad remitente y, si se usa, el conjunto de configuración.
- DynamoDB: `Query` sobre el índice por `certCode` y `UpdateItem` en `notification-certification`; `PutItem` en la tabla de enlaces cortos; `GetItem` y `TransactWriteItems` en la tabla del grupo compuesto.
- Athena: `StartQueryExecution`, `GetQueryExecution`, `GetQueryResults`; lectura del catálogo de Glue y de los datos de Gold; escritura en la ruta de salida de Athena.
- S3: lectura de `niif-dev-assets` (configuración).
- Se **retira** el permiso de publicar en el tópico SNS cuando termine la validación.

## Pruebas y calidad

Desde esta carpeta o desde la raíz del repositorio:

```
python -m pip install -r requirements-dev.txt
python -m pytest --cov --cov-report=term-missing --cov-report=xml:coverage.xml --junitxml=reports/pytest.xml
python -m pytest Backend/notificacion/niif-dev-notification-certification
```

- Cobertura mínima configurada: 90 % (`pyproject.toml`).
- `tests\test_calidad_codigo.py` exige: sin comentarios ni docstrings, sin valores quemados de infraestructura (cuentas, ARN, regiones, nombres de recursos, correos), funciones de menos de 40 líneas y sin excepciones silenciadas.
- `sonar-project.properties` está listo para SonarQube. Falta la línea base del repositorio (ver `pendientes`).
- `tzdata` es solo para pruebas locales en Windows; en Glue (Linux) la base de zonas horarias ya existe.

## Empaquetado y despliegue

```
bash scripts/empaquetar.sh
```

Genera en `dist\` la librería `niif-dev-notification-certification-<versión>.zip` (va en `--extra-py-files`) y el script `job_notificacion.py` (es el `ScriptLocation`). El mismo artefacto sirve para DEV, QA y PROD; cambian solo los argumentos del job.

Antes de desplegar, en AWS:

1. Verificar la identidad del remitente en SES (dominio con SPF, DKIM y DMARC) y pedir la salida del modo *sandbox*. **El remitente de `job.dev.json` es una propuesta de T1.**
2. Crear el conjunto de configuración de SES y enviar rebotes y quejas a EventBridge o SNS con alerta.
3. Ajustar el rol del job (sección de permisos).
4. Subir la librería y el script a `niif-dev-assets/glue-resources/`.
5. Actualizar el job con `config\job.dev.json` por Terraform.

**Vuelta atrás:** apuntar de nuevo el `ScriptLocation` y los argumentos al script anterior; el tópico SNS no se elimina hasta cerrar la validación.

## Pendiente

- **1b (correo en HTML):** el modelo `Mensaje` y el adaptador de SES ya admiten cuerpo HTML; falta la plantilla (`Backend\htmlNew\`) y la decisión de seguridad de los botones.
- El tipo de job sigue siendo `glueetl` con Spark. Como ya no usa Spark, podría pasar a un job de Python (shell) o a una lambda para reducir costo; requiere decisión y pruebas.
