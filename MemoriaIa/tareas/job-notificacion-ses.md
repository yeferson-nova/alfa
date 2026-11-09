---
tipo: diseno
actualizado: 2026-11-04
---
# Job de notificación con SES (T1, punto 1a)

Cambio de SNS a SES en el job de Glue `niif-dev-notification-certification`, que es quien arma y envía el correo de certificación. **La lambda de T2 no envía correos** (solo configura y arranca la malla), y la Step Function tampoco: ver [[tareas-aws]] y [[arquitectura-niif17]].

- **Código:** `Backend\Glue\reposNew\niif-dev-notification-certification\` (carpeta nueva `reposNew`, una por job reescrito). El job anterior sigue intacto en `Backend\Glue\niif-dev-notification-certification\`.
- **Estado (2026-11-04):** implementado y probado en local; **no desplegado ni probado contra AWS**. 451 pruebas, 99,9 % de cobertura, también desde la raíz del repositorio.
- **Estándar aplicado:** [[est-desarrollo-hexagonal]] (dominio, aplicación, adaptadores de entrada y salida, configuración), sin comentarios ni docstrings, sin valores quemados, funciones de menos de 40 líneas, errores con taxonomía `NTF-<DOM>-<NNN>`, logs JSON con `ejecucion_id`. Humano: `README.md` del propio proyecto.

## Decisiones
- SES solo para el correo de negocio; las alarmas técnicas siguen en SNS ([[monitoreo-grafana]]).
- Un correo por destinatario con SESv2 `send_email`: desaparece el tópico y el `FilterPolicy` por suscripción.
- Todo lo que antes estaba fijo (zona horaria, vigencia y longitud de enlaces, índice de `certCode`, espera de Athena, remitente) llega por argumentos del job; valores de DEV en `config\job.dev.json`.
- Compatibilidad: Python 3.10 (Glue 4.0). Sin dependencias fuera del runtime (boto3). El job sigue siendo `glueetl`.
- Idempotencia nueva: si todos los registros del `certCode` ya están enviados, no reenvía.
- El modelo `Mensaje` y el adaptador ya aceptan cuerpo HTML: falta la plantilla de 1b ([[correo-notificacion]]).

## Cambios de contrato (riesgo)
- `notificationChannel`: `SNS_EMAIL` pasa a `SES_EMAIL`; `snsMessageId` pasa a `sesMessageId` (tabla de notificaciones y tabla del grupo compuesto). Ningún código extraído lee el campo viejo, pero las lambdas de la cadena final no tienen código extraído: confirmar antes de desplegar.
- El asunto ya no se recorta a 100 caracteres.

## Validación contra DEV (2026-11-09)
Hallado en el repo (`errores\aud-datos\`, `Backend\Glue\pruebas-glue\jobs.json`):
- Ya existe en DEV el job `niif-dev-notification-certification-ses-test` (creado 2026-09-16, "Prueba controlada SNS a SES", mismo remitente, script `glue-resources/notification/test/notification_certification_ses.py`, **que no está en el repo**). No se sabe si alguna ejecución terminó bien.
- El rol `niif-dev-jobs-notification-certification` tiene la política `TemporarySesTestAccess`: `ses:SendEmail` solo sobre 2 identidades de correo (enmascaradas) y el conjunto `default`. **No** cubre el remitente ni el conjunto `niif-dev-notificaciones` de `job.dev.json`.
- El job desplegado sigue en SNS (`SNS_TOPIC_ARN`, `commons.zip`).
- **1b:** el job nuevo arma solo texto plano (`PLANTILLA_CUERPO`, tablas como `col: valor`); `Mensaje.html` existe pero nadie lo llena. Las plantillas (`Backend\htmlNew\newUx\`) usan `$tableSummary` y `$tableTotal` como HTML y hoy el job entrega texto. Sin decidir: seguridad de los botones.
- Script de solo lectura para traer lo que falta: `Backend\cloudshell-inventario\validar-ses-dev.sh` (SES, DNS, IAM, Glue, S3, lambdas que lean `snsMessageId`, DynamoDB). Corrido en CloudShell DEV el 2026-11-09 (el zip trae correos, no está en el repo). Resultado:

| Punto | Resultado | Lectura |
|---|---|---|
| Modo SES | **Sandbox**, cuota 24 h = 200 | Solo envía a destinatarios verificados; pedir salida a producción |
| Identidad `notificaciones@segurosalfa.com.co` | Existe, `verificada=true`, pero **DKIM=FAILED** y **MAIL FROM=FAILED** | Verificada como correo, no como dominio autenticado: riesgo de spam/rechazo; DNS pendiente |
| Conjunto `niif-dev-notificaciones` | **No existe** | Crearlo (con destino de rebotes y quejas) o cambiar `--SES_CONFIGURATION_SET` |
| SPF / DMARC | Sin dato (CloudShell no tiene `dig`) | Pedirlo a quien administra el DNS |
| Permiso `ses:SendEmail` del rol | La simulación devolvió `implicitDeny`, pero sobre un recurso con variables sin resolver (`${Region}...`) | **No concluyente**: el script simuló mal los ARN. Lo cierto: la política `TemporarySesTestAccess` no cubre remitente ni conjunto nuevos |
| Job `ses-test` | Al menos una ejecución **SUCCEEDED** | SES ya envió desde DEV (a verificados); falta ver cuál y con qué destinatarios |
| Librería `...-1.0.0.zip` y `job_notificacion.py` | **No están** en S3 | Subirlos |
| Script de prueba SES de S3 | No se pudo descargar | Pendiente (¿permiso del usuario de CloudShell?) |
| Plantillas HTML en `niif-dev-assets` | Hay `.html` | Ver cuáles en `s3/html-en-bucket.txt` del zip |
| Lectores de `snsMessageId` / `SNS_EMAIL` | 41 lambdas `niif/datalake-dev` revisadas, **sin hallazgos** | Riesgo de cambio de contrato bajo (el aviso "OK" del script es incondicional; vale por la ausencia de `FALTA`) |
| Índice `certCode-index` en DynamoDB | Existe | Sin acción |

**Destinatarios dinámicos (2026-11-09, verificado en código):** salen de `email.destinations` del JSON del caso en S3 (`configs/certification/<caso>.json`) y se leen en **cada ejecución**, sin caché: cambiar la lista no exige desplegar. Hueco: `_enviar` no maneja error por destinatario; en sandbox, uno sin verificar hace que SES rechace el envío, aborta el bucle, los anteriores ya recibieron y nada se marca como enviado (un reintento duplica). Propuesta: tolerar el fallo por destinatario (registrar y seguir) y reportar quién falló. Alternativa a evaluar: lista en DynamoDB si se quiere cambiarla sin tocar S3.
**SPF/DMARC/DKIM:** son registros DNS públicos del dominio; se pueden leer sin ser dueños del DNS. Crearlos o corregirlos sí es de quien administra el DNS.

## Segunda corrida (2026-11-09, script corregido)
| Punto | Resultado | Lectura |
|---|---|---|
| SPF de `segurosalfa.com.co` | Publicado, incluye `include:amazonses.com`, termina en `-all` | Autoriza a SES, pero solo alinea con DMARC si el MAIL FROM usa el dominio propio |
| DMARC | Publicado con **`p=reject`** (reportes a Agari) | Política estricta |
| 3 CNAME de DKIM (`...._domainkey.segurosalfa.com.co`) | **No publicados** | Por eso DKIM=FAILED |
| MAIL FROM | FAILED | Si no es del dominio propio, el SPF no alinea |
| Permiso `ses:SendEmail` | Sí sobre `notificaciones@...` (y el correo de Manuel); **no** sobre el conjunto `niif-dev-notificaciones` | Viene de la política `TemporarySesTestAccess` (temporal); falta política permanente mínima |
| Script de prueba SES en S3 | **404**: ya no existe | El job `ses-test` no se puede volver a ejecutar tal cual |
| Destinatarios de los casos vs verificados | El script dice OK "o no hay casos leídos" | **No concluyente**: revisar si `s3/casos/` del zip tiene archivos |
| HTML en `niif-dev-assets` | 9 archivos | Falta ver cuáles (`s3/html-en-bucket.txt`) |
| Lambdas con `snsMessageId` | 41 descargadas, ninguna lo referencia | Cerrado |

**Hallazgo clave (inferido, no probado con entrega real):** con DKIM sin CNAME, MAIL FROM fallido y DMARC en `p=reject`, un correo enviado por SES con remitente `@segurosalfa.com.co` puede **fallar la alineación DMARC y ser rechazado** por el receptor, no solo ir a spam. La prueba `ses-test` fue a un correo interno; no sabemos si llegó. Antes de pasar a producción hay que pedir a DNS: los 3 CNAME de DKIM y los registros del dominio MAIL FROM (el dominio y los registros salen de `s3`/`ses\identidad-*.json` del zip).
**Referencia QA (capturas del usuario, 2026-11-09):** QA tiene una **identidad de dominio** `segurosalfa.com.co` con DKIM "Successful" y firmas "Enabled"; sus 3 CNAME (tokens `abufkjipp6...`, `e5np3kuuiy...`, `gnseaawa7q...`) **sí están publicados** en el DNS público (comprobado). El `p=none` de la captura es solo la plantilla que sugiere SES; el DMARC publicado es `p=reject`. No hay MX para `notificacionesalfa` en DNS, y QA funciona con DKIM sin MAIL FROM propio. DEV, en cambio, tiene una identidad de **correo** (no de dominio) con tokens distintos sin publicar. Cada cuenta de AWS genera tokens propios. Propuesta: en DEV crear la identidad de dominio como en QA y pedir a DNS sus 3 CNAME; el MAIL FROM (MX/TXT) queda opcional. DNS del dominio: Telmex (`ns1.telmexla.net.co`), fuera de AWS.
**QA, conjunto de configuración (captura, 2026-11-09):** la identidad de dominio tiene como **conjunto por defecto** `datalake-ses-cofiguration-set` (se lee así en la captura, con "cofiguration"; confirmar la ortografía) y grupo de IP `default`. La identidad tiene pestañas Notifications, Authorization, Configuration set, Tenants y Virtual Deliverability Manager. Consecuencia: `--SES_CONFIGURATION_SET` del job puede ir vacío (el código lo admite) y SES aplica el de la identidad; si se pone explícito, el rol necesita `ses:SendEmail` sobre ese conjunto. Pendiente: ver sus destinos de eventos (rebotes y quejas) en QA y reproducir el mismo esquema en DEV en lugar de crear `niif-dev-notificaciones`.

**Datos de la identidad (CloudShell DEV, 2026-11-09):** MAIL FROM = `notificacionesalfa.segurosalfa.com.co`, estado FAILED, `BehaviorOnMxFailure=USE_DEFAULT_VALUE` (si el MX falla, SES usa `amazonses.com` y el SPF no alinea). DKIM: **`SigningEnabled=false`**, estado FAILED, 3 tokens generados el 2026-05-26 y sin publicar. Registros a pedir a DNS (estándar de SES para us-east-1; confirmar en la consola de SES):
- 3 CNAME `<token>._domainkey.segurosalfa.com.co` -> `<token>.dkim.amazonses.com`, tokens `u2fxsnrciwuydv34fngbkiygcub63j6m`, `xlcdka72t6mhrblcozy27bsnf5qcwf5o`, `askd3cxjfuttx374ohmi5hyutbefis3y`.
- MX `notificacionesalfa.segurosalfa.com.co` -> `10 feedback-smtp.us-east-1.amazonses.com`.
- TXT `notificacionesalfa.segurosalfa.com.co` -> `"v=spf1 include:amazonses.com ~all"`.

**Plantillas HTML en `niif-dev-assets` (2026-09-02):** `email-template/` tiene 5 (`auxilio_funerario_cert`, `previsional`, `primas_arl_cert`, `rentas_vitalicias_cert`, `reservas_matematicas_cert`, ~3,9 KB c.u.); `previsional.html` además está en la raíz y en `auxilios-funerarios/`. El job desplegado envía texto por SNS y **no usa** estas plantillas; quién las lee hoy no está confirmado. Para 1b: reemplazarlas por `Backend\htmlNew\newUx\` (mismos placeholders) y que el job nuevo las cargue de S3. `matriz_iac_roles.html` (backup) no es plantilla.

**Casos de uso:** el listado de `configs/certification/` en DEV solo trajo `composite_groups.json` (más el global). El job recibe la clave del caso en `--CONFIG_KEY` en cada ejecución; en la evidencia de prod el caso Primas ARL trae 5 destinatarios. **Dónde viven los JSON de caso en DEV (listado del bucket, 2026-11-09, truncado):** en la raíz de `niif-dev-assets` por malla, no en `configs/certification/`: `primas-arl/primasArl.json`, `primas-endosos/primasRentasEndosos.json`, `primas-rentas/primasRentasEndosos.json`, `auxilios-funerarios/auxilioFunerario.json` (y copia en `auxilios-funerarios/configs/certification/`), `reserva-matematica-rvi/reservaMatematicaRvi.json`, `reserva-matematica/reservaMatematicaRviArp.json`, `previsional/previsional.json`. `primas-arl/` también tiene una copia en `configs/certification/primasArl.json` y respaldos. Ojo: `eventos\argumentos-job.dev.json` del repo usa `configs/certification/primas-arl/primasArl.json`, que no coincide con ninguna de esas rutas: corregir o confirmar qué `CONFIG_KEY` pasa la Step Function. También hay carpetas `modelo-configuracion/` y `backups/configuration-general/` del gestor de configuración (T2). **Destinatarios según el diagrama TOBE (revisado 2026-11-09, hojas Página-1, 2, 6, 9, 10):** la Malla tiene un **"Modelo Notificaciones"** (dato de configuración, en la familia de modelos Configuración Extracción / Programación / Trazabilidad) que alimenta a la **Lambda (Notificación)**, que lanza el **Job Notificación**; el "Mensaje" llega al **"Usuario Outlook Alfa"**, que entra por WAF y Active Directory (identidad externa). El diagrama **no detalla los campos** del modelo ni dice cómo se elige a cada persona; "Administración de Usuarios" (Página-5, 6 y Copia) aparece sin flechas a notificación. SES/SNS no aparecen dibujados, solo "Email". Hoy: `email.destinations` y `approvers.users` en el JSON de cada caso en S3, editado a mano. Propuesta: tratar ese JSON como el "Modelo Notificaciones" y definir destinatarios por rol.

**Destinatarios vs verificados (2026-11-09, resultado):** los 7 casos tienen **2 destinatarios** cada uno y **ninguno sin verificar**; SES DEV tiene 13 identidades verificadas. Con el sandbox actual el envío funciona en DEV; solo fallaría si alguien agrega un correo no verificado. QA y PRD son otras cuentas de AWS: su estado de SES no se conoce ni es accesible para Desarrollo. `validar-ses-dev.sh` ya apunta a estas 7 rutas.

`validar-ses-dev.sh` corregido el 2026-11-09 (permiso SES por análisis estático, DNS sin `dig`, destinatarios vs verificadas). Mejora pendiente: que imprima el dominio MAIL FROM y los registros DNS esperados, y que distinga "sin casos leídos" de "todos verificados".

Cerrados: lectores del campo viejo e índice. Abiertos: salida de sandbox, DKIM y MAIL FROM, conjunto de configuración, política IAM, subir artefactos, SPF/DMARC.

## Falta
- Verificar la identidad del remitente en SES (el de `job.dev.json` es la propuesta de T1) y salir del modo *sandbox*; conjunto de configuración para rebotes y quejas.
- Rol del job con `ses:SendEmail`; retirar el permiso de SNS al cerrar la validación.
- Subir librería y script a `niif-dev-assets/glue-resources/` y actualizar el job por Terraform.
- Línea base de SonarQube ([[est-codigo-seguro]]).
- Evaluar pasar el job a Python (shell) o lambda: ya no usa Spark.

Pendientes generales en [[pendientes]].
