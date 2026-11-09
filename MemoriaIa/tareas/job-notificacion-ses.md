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
**SPF/DMARC/DKIM:** son registros DNS públicos del dominio; se pueden leer sin ser dueños del DNS (el script ahora usa `dig`, `nslookup` o DNS sobre HTTPS). Crearlos o corregirlos sí es de quien administra el DNS. Para pruebas en DEV hacia destinatarios verificados no bloquea; afecta entregabilidad (spam) y producción.
`validar-ses-dev.sh` corregido el 2026-11-09 (permiso SES por análisis estático de políticas, DNS sin `dig`, destinatarios de los casos frente a identidades verificadas); pendiente volver a correrlo.

Cerrados: lectores del campo viejo e índice. Abiertos: salida de sandbox, DKIM y MAIL FROM, conjunto de configuración, política IAM, subir artefactos, SPF/DMARC.

## Falta
- Verificar la identidad del remitente en SES (el de `job.dev.json` es la propuesta de T1) y salir del modo *sandbox*; conjunto de configuración para rebotes y quejas.
- Rol del job con `ses:SendEmail`; retirar el permiso de SNS al cerrar la validación.
- Subir librería y script a `niif-dev-assets/glue-resources/` y actualizar el job por Terraform.
- Línea base de SonarQube ([[est-codigo-seguro]]).
- Evaluar pasar el job a Python (shell) o lambda: ya no usa Spark.

Pendientes generales en [[pendientes]].
