---
tipo: diseno
actualizado: 2026-11-09
---
# Job de notificación con SES (T1, punto 1a)

Cambio de SNS a SES en el job de Glue `niif-dev-notification-certification`, que es quien arma y envía el correo de certificación. **La lambda de T2 no envía correos** (solo configura y arranca la malla), y la Step Function tampoco: ver [[tareas-aws]] y [[arquitectura-niif17]].

> **Vigente (2026-11-09):** la entrega de T1a+T1b se hace **sobre el script desplegado** (`Backend\Glue\niif-dev-notification-certification\notification_certification.py`), con cambio mínimo. La reescritura de `niif-dev-notification-certification` hexagonal **no se usa todavía**: es posterior a esta entrega y los cambios van por auditoría. Ver la sección "Entrega T1a+T1b sobre el job desplegado" más abajo.

- **Código reescrito (pospuesto):** `Backend\notificacion\jobs-glue\niif-dev-notification-certification\` (carpeta nueva `niif-dev-notification-certification` hexagonal, una por job reescrito). El job desplegado sigue en `Backend\Glue\niif-dev-notification-certification\`.
- **Estado de la reescritura (2026-11-04):** implementada y probada en local; **no desplegada**. 451 pruebas, 99,9 % de cobertura. No se usa en la entrega actual.

## Entrega T1a+T1b sobre el job desplegado (2026-11-09)
**Código y pruebas** (`Backend\Glue\niif-dev-notification-certification\`): `notification_certification.py` (~727 líneas), `job.json` y `tests\` (104 pruebas con `awsglue` y clientes simulados; incluyen el validador de plantillas). Las variables y funciones nuevas van **en inglés** (decisión del usuario).

| Cambio en el script | Detalle |
|---|---|
| SES en vez de SNS | SESv2 `send_email` (`send_ses`); argumento `--SES_SENDER` reemplaza `--SNS_TOPIC_ARN`; `--SES_CONFIGURATION_SET` opcional. Se borró `asegurar_filtros_sns_por_recipient` |
| Tolerancia por destinatario | Falla uno: `[SES] ERROR`, sigue con los demás y marca solo los enviados (`[SES] ENVIO PARCIAL`). Fallan todos: `RuntimeError` y no marca ni cierra el compuesto |
| Contrato en DynamoDB | `notificationChannel=SES_EMAIL` y `sesMessageId` (tabla de notificaciones y grupo compuesto) |
| HTML | Usa `email.templatePath` del JSON del caso y lo lee de S3 (`niif-dev-assets`). Valores escapados con `html.escape`. Si no puede leer la plantilla, o hay avisos y falta `$avisoHtml`, envía solo texto y lo deja en el log (`[HTML]`) |
| Avisos | Un recuadro por aviso: azul (regenerado) y ámbar (sin datos, disponibilidad por miembro) |
| Athena | `query_athena` devuelve datos estructurados: una sola ejecución por consulta alimenta el texto y el HTML. El texto del correo no cambió |
| Asunto | Ya no se recorta a 100 caracteres |

**Plantillas** (herramientas en `Backend\htmlNew\`; las 6 de producción en `Backend\notificacion\email-template\` desde 2026-11-10, antes `newUx\`; nueva `primas_rentas_cert.html`, compartida por Primas rentas y endosos, con sección de resumen), `anterior\`, `mockup\`, `previews\` (6 escenarios por plantilla) y `herramientas\` (`generar_previews.py`, `validar_plantillas.py`). Correcciones halladas al validar: `$avisoHtml` agregado; `$perido_a` en reservas reemplazado por `$periodRange` (el job nuevo no lo entrega); bloques comentados eliminados (`$redshiftLink`, resumen); `table-layout:fixed` (sin él, una tabla ancha desbordaba el correo a 390 px); banda "Acción requerida · Certificar o no certificar las cifras" con glifo, sin emoji. El mockup quedó con 4 estados (acción requerida, certificado, no certificado, fallo) y variaciones (alertas, recordatorio, regenerado, sin datos), con la regla "un estado distinto solo si cambia lo que la persona debe hacer". Solo normal, regenerado y sin datos existen hoy en el job; los demás dependen de T3, de un plazo con disparador y de T1c.

**S3 DEV:** plantillas en `niif-dev-assets/email-templates/` (plural, 6 archivos); la carpeta singular `email-template/` quedó duplicada y se borra tras la prueba. Los `templatePath` de los 7 JSON de caso tenían tres convenciones (`email-templates/`, `email-template/`, `email_templates/`): se unificaron en plural para Previsional y las dos de reserva matemática (respaldos `.bak` en CloudShell). Primas rentas y endosos apuntan a `primas_rentas_cert.html`. Existe `pruebas/primasArl-prueba.json` (copia de Primas ARL con un único destinatario de prueba). Primas rentas y endosos tienen consultas `summary` y `total`.

**Job DEV:** `--SES_SENDER` aplicado por consola y script subido a `glue-resources/notification/`. Al guardar desde la consola, Glue reescribió `--enable-metrics` y `--enable-glue-datacatalog` de `"true"` a `""` y agregó una opción a `--conf`; son inocuos. Ojo: hay que pulsar **Save** y comprobar con `aws glue get-job`.

**Primera corrida en DEV (2026-11-09, Primas ARL, `certCode` en estado SENT): SUCCEEDED**, con dos fallos de permisos del rol:
- Un destinatario recibió el correo en solo texto (MessageId devuelto); otro falló con `AccessDeniedException` en `ses:SendEmail`: **en sandbox el rol necesita permiso sobre la identidad de cada destinatario**, no solo del remitente. `TemporarySesTestAccess` no alcanzaba.
- La plantilla dio `AccessDenied` en `s3:GetObject` sobre `email-templates/*`; el job cayó a solo texto, como está diseñado.
- Corrección: política inline `niif-dev-notification-ses-templates` (`ses:SendEmail` sobre `identity/*` y `s3:GetObject` sobre `email-templates/*`), creada por CLI **sin IaC**; DevOps debe incorporarla.
- Hallazgo: el script desplegado **no tiene idempotencia**. Re-ejecutar con un `certCode` en SENT reenvía y reescribe `sentDate`, canal e id; con uno en `TRANSPORT` lo regresaría a SENT. Para pruebas usar solo `certCode` en SENT.
- Roles: el rol del job tiene 7 políticas inline (compuesto, inline propia, short-links, KMS, SNS, catálogo Gold de QA y `TemporarySesTestAccess`). Las de SNS y la temporal se retiran al cerrar la validación.

**Para QA y PRD (los opera DevOps):** (1) política del rol: `ses:SendEmail` sobre la identidad del remitente (en QA es de dominio) y `s3:GetObject` sobre `<bucket>/email-template/*`; si la cuenta sigue en sandbox, también sobre los destinatarios; si usa un conjunto de configuración explícito, sobre su ARN; (2) `--SES_SENDER` en el job y retirar `--SNS_TOPIC_ARN`; (3) script con su hash; (4) las 6 plantillas en `email-templates/`; (5) `templatePath` de los JSON; (6) retirar permisos de SNS al validar. Pendiente redactar esta solicitud en `Tareas\T1-notificaciones\`.

### Permisos IAM del rol del job: lo aprendido (2026-11-09)
Rol `niif-dev-jobs-notification-certification`. Se tropezó con **tres** denegaciones distintas en cuatro corridas; ninguna la anticipaba el diseño del script:

| # | Síntoma en el log o en el estado del run | Causa | Solución |
|---|---|---|---|
| 1 | `[SES] ERROR … AccessDeniedException … ses:SendEmail on …identity/<destinatario>` | En **sandbox**, SES evalúa el permiso también sobre la identidad de **cada destinatario**, no solo la del remitente. `TemporarySesTestAccess` solo cubría el remitente y una dirección | `ses:SendEmail` sobre `identity/*` de la cuenta (mientras la cuenta siga en sandbox) |
| 2 | `[HTML] No se pudo leer la plantilla … AccessDenied s3:GetObject` | El rol no leía `email-templates/*` | `s3:GetObject` sobre `<bucket>/email-templates/*`. El job no falla: cae a solo texto y lo deja en el log |
| 3 | Run `FAILED` al leer `pruebas/…json` y también `primas-arl/…-prueba.json` | El rol solo puede leer los JSON de caso **concretos** (permisos por objeto o prefijos exactos), no cualquier archivo de esas carpetas. Un JSON de prueba nuevo no se puede leer aunque esté junto a los reales | `s3:GetObject` sobre `<bucket>/pruebas/*` (solo DEV) y usar `--CONFIG_KEY pruebas/…` |

- **Política resultante:** inline `niif-dev-notification-ses-templates` con tres sentencias (`SesSendFromSenderAndToVerifiedRecipients`, `ReadEmailTemplates`, `ReadTestConfigs`). Creada con `aws iam put-role-policy` (CLI, **sin IaC**): DevOps debe llevarla a Terraform para que un despliegue no la borre. `put-role-policy` con el mismo nombre la **reemplaza completa**, así que al ampliarla hay que reenviar todas las sentencias.
- **Propagación:** los cambios de IAM tardan unos segundos; si el run falla justo después de aplicarla, esperar un minuto y repetir.
- **Políticas inline del rol (8 en total con la nueva):** `AllowUpdateCompositeNotificationStatus`, `niif-dev-jobs-notification-certification-inline-policy`, `niif-dev-notification-manage-short-links`, `niif-dev-notification-use-certification-kms`, `niif-dev-notification-use-certification-sns`, `ReadNiifQaGoldCatalog`, `TemporarySesTestAccess`, y la nueva. A retirar tras cerrar la validación: la de SNS y la temporal. Si el bucket cifrara con una llave KMS propia, leer plantillas necesitaría `kms:Decrypt` (no hizo falta en DEV).
- **Para QA y PRD:** en QA la identidad es de dominio (`identity/<dominio>`); si la cuenta ya salió del sandbox basta el remitente y no hace falta `identity/*`; si usa un conjunto de configuración explícito, hay que dar `ses:SendEmail` también sobre su ARN. `pruebas/*` no se lleva a QA ni PRD. Si allí se quiere probar con otra configuración, el rol debe poder leerla.
- **Diagnóstico útil:** el mensaje de AccessDenied trae el recurso exacto y la acción (`s3:GetObject on …`, `ses:SendEmail on …identity/…`); con eso se arma la sentencia sin adivinar.

**Primera corrida completa con HTML (2026-11-09 23:02 UTC): SUCCEEDED.** Con el JSON de prueba y la política ya aplicada: leyó la plantilla sin `[HTML] No se pudo leer`, envió por SES a la dirección de prueba (MessageId devuelto) y marcó SENT con `sesMessageId`. Que el correo llegue a la bandeja no está confirmado todavía (DMARC `p=reject` y DKIM de DEV sin publicar).

**Pendiente de esta entrega:** revisar el correo recibido en la dirección de prueba; revisarlo en Outlook de escritorio, Gmail y móvil; probar un caso con resumen y uno compuesto; DKIM en DNS y salida del sandbox; retirar `TemporarySesTestAccess` y los permisos de SNS; borrar `email-template/`; solicitud a DevOps; commit de los archivos nuevos (`tests\`, plantillas, `herramientas\`, `previews\`).
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
- **1b:** el job nuevo arma solo texto plano (`PLANTILLA_CUERPO`, tablas como `col: valor`); `Mensaje.html` existe pero nadie lo llena. Las plantillas (`Backend\notificacion\email-template\`) usan `$tableSummary` y `$tableTotal` como HTML y hoy el job entrega texto. Sin decidir: seguridad de los botones.
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

**Plantillas HTML en `niif-dev-assets` (2026-09-02):** `email-template/` tiene 5 (`auxilio_funerario_cert`, `previsional`, `primas_arl_cert`, `rentas_vitalicias_cert`, `reservas_matematicas_cert`, ~3,9 KB c.u.); `previsional.html` además está en la raíz y en `auxilios-funerarios/`. El job desplegado envía texto por SNS y **no usa** estas plantillas; quién las lee hoy no está confirmado. Para 1b: reemplazarlas por `Backend\notificacion\email-template\` (mismos placeholders) y que el job nuevo las cargue de S3. `matriz_iac_roles.html` (backup) no es plantilla.

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

## Flujo de certificación desplegado en DEV (verificado 2026-11-10)
Fuente: CloudShell de solo lectura + código descargado en `Backend\Lamnbdas\<lambda>\` (cada carpeta con `ORIGEN.md`). Estado de DynamoDB `niif-dev-notification-certification` con stream `NEW_IMAGE` y 3 consumidores activos:

| Transición | Lambda | Efecto |
|---|---|---|
| `PENDING` (solo `queryName=detail#1`, sin `sendLock`) | `niif-dev-notification-certification-trigger` | Lanza el job de Glue de notificación |
| `CERTIFICATE` | `niif-dev-copy-s3-cert` | Copia a `export/<carpeta>/` de `niif-dev-transport` y marca `TRANSPORT` |
| `export/` en `niif-dev-transport` | `datalake-dev-niif-send-via-sftp` (imagen de contenedor, código no descargable) | SFTP a FIS |
| `NO_CERTIFICATE` | `niif-dev-move-s3-certification` | Mueve a `<carpeta>/NO-CERTIFICADOS/`, borra el origen, marca `NO_TRANSPORT` |

- `EnvioSftpFis` NO es del flujo NIIF: lo dispara `alfa-datalake-dev-landing-prophet/landing/` (última modificación 2025-09-30). Las `datalake-dev-niif-*` (move, copy, notification) tienen los mappings en Disabled (versión antigua).
- **No certificar no reprograma nada** (ni programador ni Step Functions): ninguna lambda lo hace y no hay reglas de EventBridge sobre ese estado. Contradice lo dicho por Víctor ([[reunion-diseno-pcr]]); pendiente confirmar con él si es lo esperado.
- **Dependencia del release de SES:** el trigger pasa `--SNS_TOPIC_ARN` al job y NO `--SES_SENDER`; el evaluador compuesto no pasa ninguno. El job funciona porque `--SES_SENDER` está en sus argumentos por defecto: **el job de QA debe llevarlo**. El trigger y el evaluador leen `SNS_TOPIC_ARN` con `os.environ[...]`: no quitar esa variable ni borrar el tema SNS hasta modificarlos (limpieza posterior).
- Hipótesis por verificar: `composite_notification_evaluator.py` pasa `"--ENVIRONMENT": '{env}'` (texto literal, no f-string); ver si el job usa ese argumento.
- Seguridad: el trigger registra el evento completo de DynamoDB; `certification.py` (repo) registra el evento completo con el token (`[EVENT]`).
- Botones (revisado en `pruebas-lambda\codigo\postLogin` y `niif-dev-certification`): short link → Cognito/AzureAD (`response_type=TOKEN`) → página `/post_login` → `fetch` a `/certify` o `/not_certify` con el id_token; el `state` sin firma define método y ruta. No cambia con la migración a SES. Pendiente verificar en DEV: callback de Cognito, flujo implícito permitido y rutas con autorizador (comandos dados al usuario).
- Pruebas de aceptación de la entrega: en QA, no en DEV (decisión del usuario 2026-11-10).
- Pendientes de verificación: ImageUri y logs de `send-via-sftp`, `scheduler list`, bloque de login/API Gateway.

### Botones: cadena de login verificada en DEV (2026-11-10)
- API del flujo: `hfoy06vw40` (`datalake-dev-niif-api-gtw`), stage `dev` (desplegado 2026-10-09), con `/r`, `/redirect`, `/post_login`, `/certify` y `/not_certify` (POST, autorizador `is5r2o` = `CognitoAuthorizerDev`, pool `us-east-1_7B1QUgeNT` con proveedor AzureAD tipo OIDC) → lambda `niif-dev-certification`.
- `REDIRECT_BASE_URL` del trigger y del evaluador compuesto apunta a ese API (`.../dev/r`). El cliente de Cognito `datalake-dev-niif-api` tiene callbacks `/post_login` de los dos APIs, flujo `implicit` y scopes email/openid/profile.
- Limpieza (no bloquea): el autorizador `fmywcu` (`CognitoAuthorizer`) apunta al pool `gg6kZ9C4O`, que no existe; el API `53x06brwqh` ("Certificación API", sin `/certify`) tiene un autorizador al pool real `7B1QUgeNT` y otro al inexistente `NICsMpHz8`, y no lo usa ninguna lambda; parece resto de una versión anterior.
- Para QA: DevOps debe confirmar que callback, autorizador y `REDIRECT_BASE_URL` apuntan a QA. Falta la prueba funcional con clic real (en QA).

### Decisión: la carpeta de plantillas se llama `email-template` (singular), 2026-11-10
- Antes (2026-11-09) se había unificado a plural `email-templates/` en DEV. **El usuario decidió dejar el singular**, que es el nombre que ya existe en el bucket. El código no cambia (el job lee `email.templatePath` del JSON tal cual): solo cambian la carpeta de S3, los `templatePath` de los JSON y la política IAM (`s3:GetObject` sobre `<bucket>/email-template/*`).
- Actualizado: paquete `Tareas\paquete-release-notificaciones\` (carpeta `email-template\`, LEEME, política y hashes), `htmlNew\README.md` y las pruebas. Las notas de arriba que dicen `email-templates/` describen el estado de DEV del 2026-11-09, no lo vigente.
- Pendiente en DEV: copiar las 6 plantillas nuevas a `email-template/`, volver a poner el singular en los `templatePath` de los 3 JSON ajustados, cambiar la política `niif-dev-notification-ses-templates` al singular y luego borrar `email-templates/` y `primasArl-prueba.json`. Nadie más lee el nombre de la carpeta (verificado en el código de las lambdas).

## Prueba en QA (2026-11-10), cuenta 533267205627
- Nombres de QA usan `qa`: job `niif-qa-notification-certification`, tabla `niif-qa-notification-certification`, bucket `niif-qa-assets`, rol `niif-qa-jobs-notification-certification`. Plantillas del paquete desplegadas en `email-template/` (tamaños idénticos); además hay una ajena `parametros_actuariales_cert_v2.html`. Los JSON de caso de QA traen `templatePath` en `email-templates/` (plural, vacía): corregir (paso 4.6).
- `emailType` real es `cert` (no `certification`). Los `certCode` en SENT de Primas ARL son 54; se usó `202605-20261110-1035`.
- Identidades SES: `notificaciones@segurosalfa.com.co` y la del usuario, verificadas. Conjunto de configuración de QA: `datalake-ses-cofiguration-set` (así escrito) y `default`.
- Intentos: (1) FAILED `--SES_SENDER` requerido: el job de QA no lo trae por defecto (paso 4.5 sin aplicar); (2) AccessDenied en `pruebas/` (el rol lee JSON por archivo); (3) con el JSON real modificado: AccessDenied al leer la plantilla y a `ses:SendEmail` sobre el conjunto de configuración; (4) tras desplegar DevOps el permiso de plantillas, la plantilla ya se lee, pero `SesSendFromSender` cubre solo `identity/*` y falta `configuration-set/datalake-ses-cofiguration-set`. Hueco del paquete: la política plantilla no lo incluía; corregido en `politica-iam-ses.json` y LEEME.
- DynamoDB del `certCode` sin cambios (sigue `SNS_EMAIL`): los envíos fallidos no marcan.
- **Estado del JSON real `primas-arl/primasArl.json` en QA: modificado para la prueba (solo el correo del usuario, plantilla singular). Respaldo local en CloudShell `primasArl.qa.original.json`. Restaurar los destinatarios al terminar.**
- Política del rol tiene SNS vigente (`sns:SetSubscriptionAttributes` sobre `*`); retirar al cerrar la validación.
- **Resultado final de la prueba (2026-11-10): job SUCCEEDED en QA.** Con los permisos corregidos, el job leyó `email-template/primas_arl_cert.html`, ejecutó Athena, creó 3 enlaces cortos, envió por SES a un destinatario (`MessageId` devuelto) y marcó SENT con `sesMessageId` (`certCode` `202605-20261110-1035`, `executionId` `23392b50-...`). Pendiente: confirmar llegada a la bandeja y aspecto en Outlook, Gmail y móvil; verificar `SES_EMAIL` en DynamoDB; restaurar `primas-arl/primasArl.json`; pruebas 4 a 7 del LEEME; botones solo con caso de prueba acordado.
- **Prueba 2 parcial (2026-11-10): el correo llegó a la bandeja sin pasar por basura en QA** (identidad con dominio y DKIM válido). Falta evidencia de aspecto en Outlook de escritorio, Gmail y móvil. DynamoDB confirmado: `SES_EMAIL`, `SENT`, `sesMessageId` igual al del log. Plan de QA: pruebas 1 y 3 hechas; 2 parcial; 4 a 7 pendientes.


## Auditoría nuevo vs. desplegado (2026-11-10)
Humano: `Docs\06-brechas\auditoria-job-notificacion-nuevo-vs-actual.md`. **El nuevo aún no reemplaza al actual.**
- **Bloqueante/alto:** N-01 no arma HTML (no lee `templatePath`; falta portar `build_html_body`, tablas, avisos y archivos). N-02 se detiene en el primer destinatario fallido (el actual sigue y marca si salió al menos uno). N-03 pide `--SES_REMITENTE` (QA usa `--SES_SENDER`) y 6 argumentos obligatorios nuevos que nadie pasa.
- **Equivalente:** configuración, búsqueda por `certCode` (con paginación), compuesto (mismos estados y transacción), avisos, enlaces, cuerpo de texto, marca `SENT`. Mejoras: tiempo máximo en Athena, sin SQL ni correos en el log, errores con código, no reenvía si todo está en `SENT` (N-04: cambio por confirmar).
- **Observabilidad:** O-01 `ejecucion_id` = `JOB_RUN_ID` o un `uuid4` inventado, no el del flujo (ARQ-32). O-02 `cert_code`, `use_case_id` y `periodo` no van en cada línea. O-03 sin mensaje en español. O-04 los errores de arranque no salen en JSON. O-05 faltan eventos (fallo por destinatario, avisos, HTML/texto, `QueryExecutionId` y bytes). O-06 sin EMF.
- **Correcciones hechas (2026-11-10, local, sin desplegar):**
  - Resueltos N-01 (HTML con las 6 plantillas reales), N-02 (envío parcial), N-03 (`--SES_SENDER`), N-05 (consultas estrictas, `NTF-CFG-009`) y N-07 (token de 32).
  - Resueltos O-01 a O-06. El `ejecucion_id` llega por `--EJECUCION_ID` o por `ejecucionId` del registro; nunca se inventa. Las métricas salen como evento `NTF_RESUMEN_JOB`, porque en Glue el EMF no funciona.
  - Librería común `niif_observability` (en inglés, 10 pruebas): copia de `niif_observability` dentro de `src\` de cada paquete (decisión del usuario 2026-11-10: se duplica en Git porque la entrega es por carpeta); `Backend\notificacion\verificar_copias_comunes.py` comprueba que todas las copias sean iguales. El job tiene 513 pruebas con ella.
  - El job pasó de 451 a 486 pruebas (99 %).
  - Pendientes: N-04, N-06, N-08 y el lado de ARQ-32 (certificación y trigger).
