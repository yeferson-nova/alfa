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
- Script de solo lectura para traer lo que falta: `Backend\cloudshell-inventario\validar-ses-dev.sh` (SES, DNS, IAM, Glue, S3, lambdas que lean `snsMessageId`, DynamoDB). Resultado pendiente de correr.

## Falta
- Verificar la identidad del remitente en SES (el de `job.dev.json` es la propuesta de T1) y salir del modo *sandbox*; conjunto de configuración para rebotes y quejas.
- Rol del job con `ses:SendEmail`; retirar el permiso de SNS al cerrar la validación.
- Subir librería y script a `niif-dev-assets/glue-resources/` y actualizar el job por Terraform.
- Línea base de SonarQube ([[est-codigo-seguro]]).
- Evaluar pasar el job a Python (shell) o lambda: ya no usa Spark.

Pendientes generales en [[pendientes]].
