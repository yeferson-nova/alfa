# Paquete de release 2: correo de certificación por SES con HTML (T1a + T1b)

Para: DevOps / quien despliega a QA.
Estado: probado en DEV el 2026-11-09 (el job termina bien y SES acepta el mensaje). **La llegada del correo a la bandeja y los botones NO se probaron en DEV: se prueban en QA** (decisión del equipo).

## 1. Qué es
El job `niif-<amb>-notification-certification` deja de publicar en SNS y envía el correo de certificación por **SES**, en HTML, a partir de una plantilla en S3 (`email.templatePath` del JSON del caso). Si la plantilla no se puede leer, envía solo texto y lo deja en el log (`[HTML]`).

Cambios de comportamiento a conocer:
- Un correo por destinatario. Si uno falla, sigue con los demás y marca solo los enviados; si fallan todos, el job falla y no marca nada.
- En DynamoDB: `notificationChannel` pasa de `SNS_EMAIL` a `SES_EMAIL` y `snsMessageId` a `sesMessageId`. Ninguna de las lambdas de la cadena (trigger, evaluador compuesto, copy, move) lee esos campos (revisado en el código de DEV).
- El asunto ya no se recorta a 100 caracteres.
- Los enlaces `target="_blank"` llevan `rel="noopener noreferrer"` (hotspot de SonarQube). No cambia la visual del correo.

## 2. Qué se entrega
| Archivo | Destino |
|---|---|
| `niif-dev-notification-certification\notification_certification.py` | Ruta que indique `Command.ScriptLocation` del job en QA (en DEV: `s3://niif-dev-assets/glue-resources/notification/notification_certification.py`) |
| `email-template\*.html` (6) | `s3://<bucket-assets-qa>/email-template/` (singular) |
| `politica-iam-ses.json` | Política del rol del job (completar los `<...>`; ver paso 4.4) |

Hash SHA256 (también en `SHA256.txt`). Desde esta carpeta, en PowerShell; cada línea debe decir `OK`:
```powershell
Get-Content SHA256.txt | ForEach-Object {
  $h, $f = $_ -split ' \*', 2
  $real = (Get-FileHash $f -Algorithm SHA256).Hash.ToLower()
  '{0}  {1}' -f $(if ($real -eq $h) { 'OK     ' } else { 'DIFIERE' }), $f
}
```
Nota: `primas_arl_cert.html` y `reservas_matematicas_cert.html` son idénticas (misma estructura, mismo hash). Es esperado.

No cambia: las lambdas (trigger, evaluador compuesto, copy, move, certificación), la tabla de DynamoDB, las Step Functions.

## 3. Importante antes de desplegar
1. **El job necesita `--SES_SENDER` en sus argumentos por defecto.** El trigger y el evaluador compuesto **no** lo pasan en cada corrida; el job lo toma de los defaults. Sin él, el job falla al arrancar.
2. **No quitar `SNS_TOPIC_ARN` de las lambdas trigger y evaluador compuesto ni borrar el tema SNS.** Las dos leen la variable con `os.environ[...]` al cargar; sin ella no arrancan. El trigger sigue enviando `--SNS_TOPIC_ARN` al job y el job nuevo lo ignora. Es limpieza para un cambio posterior.
3. **El remitente debe ser una identidad verificada en SES de QA.** Valor de DEV: `notificaciones@segurosalfa.com.co`; en QA debe ser el del dominio con DKIM válido.
4. **La cuenta de SES de QA no se ha revisado.** Hay que confirmar si está en sandbox (en sandbox, la política debe cubrir también la identidad de cada destinatario).
5. Los nombres del job, las rutas y los buckets se confirman con `get-job` (paso 4.1). Los de abajo son los de DEV con el ambiente cambiado.

## 4. Pasos en QA
Sustituye `AMB` por `qa`.

**4.1 Averiguar el job y de dónde lee el script**
```powershell
aws glue get-jobs --query "Jobs[?contains(Name,'notif')].[Name,Command.ScriptLocation]" --output text
aws glue get-job --job-name niif-AMB-notification-certification --query "Job.DefaultArguments" --output json
```

**4.2 Respaldar y comparar lo que hay hoy**
```powershell
aws s3 cp RUTA_SCRIPT_QA notification_certification.qa.original.py
fc.exe notification_certification.qa.original.py niif-dev-notification-certification\notification_certification.py
```
Conserva `notification_certification.qa.original.py`: es la reversa. Respalda también `aws s3 sync s3://<bucket-assets-qa>/email-template/ respaldo-plantillas\` si ya existen plantillas.

**4.3 Subir el script y las plantillas, y verificar**
```powershell
aws s3 cp niif-dev-notification-certification\notification_certification.py RUTA_SCRIPT_QA
aws s3 cp email-template\ s3://<bucket-assets-qa>/email-template/ --recursive
aws s3 cp RUTA_SCRIPT_QA notification_certification.qa.desplegado.py
Get-FileHash notification_certification.qa.desplegado.py -Algorithm SHA256
```
El hash debe coincidir con el de `SHA256.txt`.

**4.4 Permisos del rol del job** (política inline nueva o la que defina DevOps por IaC; no usar políticas temporales)
- `ses:SendEmail` sobre la identidad del remitente (en QA es de dominio: `identity/<dominio>`).
- Si la cuenta sigue en sandbox: también sobre la identidad de cada destinatario.
- Si se usa un conjunto de configuración (`--SES_CONFIGURATION_SET`, opcional): también sobre su ARN.
- `s3:GetObject` sobre `<bucket-assets-qa>/email-template/*`.
- La plantilla `politica-iam-ses.json` cubre lo básico. **No** dar acceso a `pruebas/*`.

**4.5 Argumentos del job**
Agregar a los argumentos por defecto: `--SES_SENDER` = remitente verificado (obligatorio). Opcional: `--SES_CONFIGURATION_SET`. Guardar y comprobar con `get-job` que quedó (en la consola hay que pulsar Save). El argumento `--SNS_TOPIC_ARN` puede quedarse sin efecto.

**4.6 Rutas de plantilla en los JSON de caso**
Cada JSON de caso debe tener `email.templatePath` en `email-template/<plantilla>.html` (singular). Comprobar:
```powershell
aws s3 ls s3://<bucket-assets-qa>/ --recursive | findstr /i "primas-arl primas-rentas primas-endosos auxilios previsional rentas-vitalicias reserva"
```
Y para cada JSON: `aws s3 cp s3://<bucket>/<ruta>.json - | jq '.email.templatePath'`. Plantilla esperada por caso: Primas ARL → `primas_arl_cert.html`; Primas Rentas y Endosos → `primas_rentas_cert.html`; Auxilios Funerarios → `auxilio_funerario_cert.html`; Previsional → `previsional.html`; Rentas Vitalicias → `rentas_vitalicias_cert.html`; Reserva Matemática (ARP y RVI) → `reservas_matematicas_cert.html`. Si alguno apunta a `email-template/` o `email_templates/`, corregir el JSON (o dejar la plantilla también en esa ruta).

## 5. Plan de pruebas en QA (evidencia a capturar)
Avisa antes al equipo si los destinatarios son los reales. Cada Certificar o No certificar cambia el estado real del caso: Certificar copia el archivo a transporte y dispara el SFTP; No certificar lo mueve a `NO-CERTIFICADOS`. Confirma a qué SFTP envía QA antes de certificar. El script **no es idempotente**: no relances un `certCode` ya en `CERTIFICATE` o `TRANSPORT`; un reenvío sobre `SENT` sí repite el correo.

| # | Prueba | Qué comprobar |
|---|---|---|
| 1 | Una corrida normal con resumen (Primas Rentas y Endosos) | Job `SUCCEEDED`; en el log `[SES] … MessageId`; sin `[HTML] No se pudo leer` |
| 2 | Llegada y aspecto | El correo llega a la bandeja (no a basura) y se ve bien en **Outlook de escritorio, Gmail y móvil**. Captura de pantalla de cada uno |
| 3 | Registro en DynamoDB | `notificationChannel = SES_EMAIL`, `sesMessageId` presente, estado `SENT` |
| 4 | Un caso compuesto (el que usa el evaluador compuesto) | Un solo correo al completar el grupo, con el aviso de disponibilidad por miembro si aplica |
| 5 | Regenerado sin datos nuevos y sin datos | Recuadro azul (regenerado) y recuadro ámbar (sin datos), sin ocultar el total |
| 6 | Botones | Certificar: login con el usuario aprobador, estado `CERTIFICATE` y luego `TRANSPORT`. No certificar: `NO_CERTIFICATE` y luego `NO_TRANSPORT`. Segundo clic: respuesta 409. Usuario no aprobador: 403 |
| 7 | Destinatario inválido o no verificado (si la cuenta está en sandbox) | El job sigue con los demás y deja `[SES] ENVIO PARCIAL` |

Registros de verificación (solo lectura):
```powershell
aws logs filter-log-events --log-group-name /aws-glue/jobs/output --log-stream-names <JobRunId> --filter-pattern "?SES ?HTML ?ERROR" --query "events[].message" --output text
```
Si el correo no llega a pesar del `MessageId`: revisar el rastreo de mensajes del administrador del correo y DKIM/DMARC del dominio (DMARC en `p=reject` rechaza si DKIM no alinea).

## 6. Reversa
1. Subir `notification_certification.qa.original.py` a su ruta. El trigger y el evaluador siguen funcionando porque todavía pasan `--SNS_TOPIC_ARN`.
2. Mientras dure la validación no retirar los permisos de SNS del rol.
3. Las plantillas nuevas y los registros de DynamoDB con `SES_EMAIL` pueden quedarse sin efecto: el script anterior no los usa.

## 7. Pruebas unitarias
104 pruebas (flujo completo, envío parcial, HTML por tonos, validación de las plantillas contra el estándar de correo). Viven en el repo, no en este paquete (dependen de `Backend\htmlNew`). Desde `Backend\Glue\niif-dev-notification-certification`: `python -m pytest tests -q` → `104 passed`.

## 8. Evidencia en DEV
- 4 corridas: dos fallaron por permisos y se corrigieron (SES sobre cada destinatario en sandbox; lectura de la carpeta de plantillas (en DEV era `email-templates/*`); lectura del JSON de prueba); la última terminó `SUCCEEDED` con la plantilla leída y SES devolvió el `MessageId`.
- No hay evidencia de llegada a la bandeja en DEV: la identidad de DEV es de correo, con DKIM sin publicar, y el dominio tiene DMARC `p=reject`. Se valida en QA.

## 9. Fuera de este paquete
- Alarma al soporte cuando falla el proceso (1c) y escenarios adicionales de correo (1d).
- Limpieza posterior: quitar `SNS_TOPIC_ARN` de trigger y evaluador y eliminar el tema SNS.
- Hallazgo para revisar, no bloquea: en `composite_notification_evaluator.py` el argumento `--ENVIRONMENT` se envía como el texto literal `{env}`.
- Observación: al hacer No certificar, hoy el archivo se archiva y se marca `NO_TRANSPORT`, pero no se reprograma la malla.
