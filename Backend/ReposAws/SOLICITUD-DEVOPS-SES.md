# Solicitud a DevOps: correo de certificación por SES (reemplaza a SNS)

**Para:** DevOps NIIF 17
**De:** Desarrollo NIIF 17 (cadena de notificación y certificación)
**Primer ambiente:** QA (rama `quality`). Luego PRD (rama `main`) con el mismo procedimiento.

## 1. Resumen
El job de Glue que envía el correo de certificación deja de publicar en SNS y pasa a enviar por **Amazon SES**, en HTML, con una plantilla guardada en S3.
- No se optimiza nada más: es la misma lógica que hoy corre en DEV, probada en QA el 2026-11-10 (job `SUCCEEDED`, correo en la bandeja, registro `SES_EMAIL` en DynamoDB).
- Las lambdas, la tabla de DynamoDB y las Step Functions no cambian.

Cuentas de referencia:

| Ambiente | Cuenta | Rama |
|---|---|---|
| DEV | 533267035849 | `develop` |
| QA | 533267205627 | `quality` |
| PRD | 992382496548 | `main` |

## 2. Cambios en los repos (los entrega Desarrollo)
| Repo | Archivo | Cambio |
|---|---|---|
| `data-datalake-niif-notification-certification-glue-job` | `src/certification_notifier/`, `scripts/`, `test/`, `pyproject.toml` | Job modular (hexagonal) con envío por SES y plantilla HTML. Reemplaza a `src/jobs/notification_certification.py`. Su resultado es igual al del script SES probado en QA (36 de 36 escenarios; ver el README del repo) |
| `cfg-datalake-niif-config-s3` | 7 JSON de caso (ver abajo) | `email.templatePath` pasa a la carpeta `email-template/` (singular). Además, dos correcciones de enlaces |
| `tpl-datalake-niif-email-template-s3` | `src/email-template/*.html` (6) | Sin cambios: ya tiene las plantillas nuevas |

Los 7 JSON de `cfg-datalake-niif-config-s3/src/`:
- `auxilios-funerarios/auxilioFunerario.json` y `auxilios-funerarios/configs/certification/auxilioFunerario.json`: `templatePath`, y se quita un espacio en `envConfig.prd.certificarLink` (`datalake- prd-niif` pasa a `datalake-prd-niif`). Ese espacio rompe hoy el botón Certificar de Auxilios en PRD (caso 9411325).
- `previsional/previsional.json`: `templatePath` (antes `email_templates/`), y `envConfig.dev.nocertificarLink` (`%2Fdev}%2F` pasa a `%2Fdev%2F`).
- `primas-arl/primasArl.json`, `primas-arl/configs/certification/primasArl.json`, `primas-rentas/primasRentasEndosos.json` y `primas-endosos/primasRentasEndosos.json`: `templatePath`.

## 3. Cambios de infraestructura (los hace DevOps)

### 3.1 Argumento por defecto del job (bloquea)
- **Recurso:** el job de Glue `niif-<env>-notification-certification`.
- **Cambio:** en `DefaultArguments`, agregar:

  | Argumento | Valor | Obligatorio |
  |---|---|---|
  | `--SES_SENDER` | `notificaciones@segurosalfa.com.co` | Sí |
  | `--INDICE_CERT_CODE` | `certCode-index` | Sí |
  | `--ZONA_HORARIA` | `America/Bogota` | Sí |
  | `--DIAS_VIGENCIA_ENLACE` | `7` | Sí |
  | `--LONGITUD_TOKEN_ENLACE` | `8` | Sí |
  | `--SEGUNDOS_ENTRE_CONSULTAS` | `1` | Sí |
  | `--SEGUNDOS_MAXIMOS_CONSULTA` | Lo confirma Desarrollo antes del despliegue | Sí |
  | `--extra-py-files` | `s3://niif-<env>-assets/glue-resources/libraries/niif-<env>-notification-certification-1.0.0.zip` | Sí |
  | `--SES_CONFIGURATION_SET` | `datalake-ses-cofiguration-set` en QA; en DEV y PRD, el de cada cuenta | No, pero recomendado |

- **Además:** el `ScriptLocation` sigue siendo `s3://niif-<env>-assets/glue-resources/notification/notification_certification.py` (mismo nombre de hoy). Cambia el contenido: ahora solo llama a la librería del zip.
- **Por qué:** la lambda `niif-<env>-notification-certification-trigger` solo pasa los argumentos de la solicitud. Sin estos argumentos por defecto, el job falla al arrancar con `NTF-ENT-001` y no envía nada. Los valores de índice, zona horaria, vigencia, token y espera son los que el script anterior tenía escritos en el código.
- **Verificar:** `aws glue get-job --job-name niif-qa-notification-certification --query "Job.DefaultArguments"`

### 3.2 Permisos del rol del job
- **Recurso:** el rol `niif-<env>-jobs-notification-certification`.
- **Cambio:** una política con dos permisos (base en `Tareas/paquete-release-notificaciones/politica-iam-ses.json` del repo de Desarrollo):
  - `ses:SendEmail` y `ses:SendRawEmail` sobre `arn:aws:ses:us-east-1:<cuenta>:identity/segurosalfa.com.co` y sobre `arn:aws:ses:us-east-1:<cuenta>:configuration-set/<conjunto>`;
  - `s3:GetObject` sobre `arn:aws:s3:::niif-<env>-assets/email-template/*`;
  - `s3:GetObject` sobre `arn:aws:s3:::niif-<env>-assets/glue-resources/libraries/*`, por el zip del job. Si el rol ya lee todo `glue-resources/`, no hace falta.
- **Si la cuenta de SES está en sandbox:** también `ses:SendEmail` sobre la identidad de cada destinatario.
- **En QA:** estos permisos se agregaron a mano el 2026-11-10. Hay que llevarlos a Terraform para que un `apply` no los borre.
- **Verificar:** `aws iam list-role-policies --role-name niif-qa-jobs-notification-certification`, y `get-role-policy` de cada política.

### 3.3 Callback de Cognito (botones Certificar y No certificar)
- **Recurso:** el pool `datalake-<env>-niif-user-pool`, cliente `datalake-<env>-niif-api`.
- **Cambio:** agregar a `CallbackURLs` la URL de `post_login` que usan los enlaces del correo:

  | Ambiente | Callback |
  |---|---|
  | DEV | `https://hfoy06vw40.execute-api.us-east-1.amazonaws.com/dev/post_login` |
  | QA | `https://d5qmm9fi64.execute-api.us-east-1.amazonaws.com/qa/post_login` |
  | PRD | `https://jdwkua9re5.execute-api.us-east-1.amazonaws.com/prd/post_login` |

- **Por qué:** en PRD el cliente solo tiene `https://localhost/post_login`. Si la URL no coincide, Cognito rechaza el login (`redirect_mismatch`). Lo apoya que en 14 días no hubo ni una llamada a `/post_login`. Falta revisar QA.
- **Verificar:** `aws cognito-idp describe-user-pool-client --user-pool-id <pool> --client-id <cliente> --query "UserPoolClient.CallbackURLs"`

### 3.4 Lo que NO se debe tocar todavía
- La variable `SNS_TOPIC_ARN` de las lambdas `niif-<env>-notification-certification-trigger` y `niif-<env>-composite-notification-evaluator`, ni el tema SNS. Las dos leen la variable al arrancar y fallan sin ella. El job nuevo recibe `--SNS_TOPIC_ARN` y lo ignora. Se limpia en un cambio posterior.
- Los permisos de SNS del rol del job, mientras dure la validación (sirven para la reversa).

## 4. Preguntas que hay que resolver antes de desplegar
| # | Pregunta | Riesgo si no se resuelve |
|---|---|---|
| P1 | El `Jenkinsfile` de `tpl-datalake-niif-email-template-s3` despliega en `alfa-niif-<env>-assets`, pero el job lee las plantillas de `niif-<env>-assets` (igual que `cfg-`). ¿Es un error de copia? Si lo es, cambiar a `niif-dev-assets`, `niif-qa-assets` y `niif-prd-assets` | Las plantillas no llegan donde el job las busca y el correo sale solo en texto |
| P2 | `cfg-` y `tpl-` sincronizan en la raíz de `niif-<env>-assets`, y el README dice que usan `--delete`. ¿El borrado se limita a las carpetas del repo o a todo el bucket? | Un despliegue de `cfg-` podría borrar `email-template/` (de `tpl-`), `glue-resources/notification/` (del job) y `athena/notifications/` |
| P3 | El job modular necesita dos archivos que arma `bash scripts/package.sh <env>`: el zip de la librería (`--extra-py-files`) y `notification_certification.py` (`ScriptLocation`). El `Jenkinsfile` actual solo sincroniza `src/` con `glue-resources/notification/`. ¿Cómo se publican esos dos archivos en el pipeline? | Sin el zip y el script, el job no arranca o sigue corriendo el script anterior |
| P6 | Las pruebas del repo requieren Python 3.10 o superior y `boto3`; la cobertura se mide con `--cov-config=pyproject.toml`. ¿Con qué Python corre el pipeline las pruebas? El `tox.ini` declara `py38`, y su `omit = src/**.py` deja la cobertura en cero | Pruebas o Sonar fallan en el pipeline |
| P4 | El pipeline reemplaza `{env}` en los JSON (el JSON global lo usa en `buckets` y `dynamodb`). ¿También copia `email.envConfig.<env>` sobre `email.stringVars`? ¿Qué pasa con una llave que se llame `{env}` (`reserva-matematica/reservaMatematicaRviArp.json`)? | Enlaces o destinatarios de otro ambiente en el correo |
| P5 | ¿La cuenta de SES de QA y la de PRD están en sandbox? ¿Cuál es el conjunto de configuración de PRD? ¿La identidad `segurosalfa.com.co` de PRD tiene DKIM verificado? | Correos rechazados (el dominio tiene DMARC `p=reject`) o el envío falla por permisos |

## 5. Orden de despliegue en QA
1. Resolver P1, P2 y P3.
2. Terraform: 3.1 (argumentos), 3.2 (permisos) y 3.3 (callback de QA).
3. Desplegar `tpl-datalake-niif-email-template-s3` (rama `quality`) y comprobar `aws s3 ls s3://niif-qa-assets/email-template/`.
4. Desplegar `cfg-datalake-niif-config-s3` (rama `quality`) y comprobar que `email.templatePath` apunta a `email-template/`:
   `aws s3 cp s3://niif-qa-assets/primas-arl/primasArl.json - | jq .email.templatePath`
5. Desplegar `data-datalake-niif-notification-certification-glue-job` (rama `quality`) según la respuesta a P3. Luego comprobar:
   - que el script de `ScriptLocation` (`glue-resources/notification/notification_certification.py`) es el nuevo, que solo llama a `certification_notifier`;
   - que el zip de `--extra-py-files` existe en `glue-resources/libraries/`.

## 6. Pruebas en QA (las acompaña Desarrollo)
| # | Prueba | Qué se espera |
|---|---|---|
| 1 | Corrida normal | Job `SUCCEEDED`. En el log aparecen `NTF_CORREO_ENVIADO` y `NTF_FIN_JOB`, y no aparece `NTF_PLANTILLA_NO_LEIDA` |
| 2 | Llegada y aspecto | El correo llega a la bandeja y se ve bien en Outlook de escritorio, Gmail y móvil |
| 3 | DynamoDB | En `niif-qa-notification-certification`: `notificationChannel = SES_EMAIL`, `sesMessageId` y `status = SENT` |
| 4 | Caso compuesto (Primas Rentas y Endosos o Reserva Matemática) | Un solo correo al completar el grupo |
| 5 | Regenerado y sin datos | Recuadro azul (regenerado) y recuadro ámbar (sin datos) |
| 6 | Botones | Certificar: `CERTIFICATE` y luego `TRANSPORT`. No certificar: `NO_CERTIFICATE` y luego `NO_TRANSPORT`. Segundo clic: 409. Usuario no aprobador: 403. Usar un caso acordado, porque Certificar dispara el SFTP |
| 7 | Destinatario inválido (solo si la cuenta está en sandbox) | El job sigue con los demás y el log muestra `NTF_ENVIO_FALLIDO` y `NTF_ENVIO_PARCIAL` |

Cuidado: no hay que relanzar un `certCode` que ya está en `CERTIFICATE` o `TRANSPORT`. Si todos sus registros ya están en `SENT`, el job no reenvía y registra `NTF_YA_ENVIADO`.

## 7. Reversa
1. Revertir el commit del repo del job y desplegar. Apuntar el `ScriptLocation` al script anterior y quitar `--extra-py-files`. El script anterior funciona porque el trigger sigue pasando `--SNS_TOPIC_ARN` y el tema SNS sigue existiendo.
2. `--SES_SENDER`, los permisos de SES, las plantillas y los `templatePath` pueden quedarse: el script anterior no los usa.
3. Los registros ya marcados con `SES_EMAIL` en DynamoDB no afectan al script anterior.

## 8. Después de QA (PRD)
- Repetir el orden de la sección 5 con la rama `main` y la cuenta 992382496548.
- En DynamoDB `niif-prd-short-links` hay 2 enlaces cortos de Auxilios guardados con el host roto. Para que el botón sirva en un periodo ya enviado, hay que regenerarlos (o reenviar el correo).
- Limpieza posterior (otro cambio): quitar `SNS_TOPIC_ARN` del trigger y del evaluador, los permisos de SNS y el tema.
