# Registro de horas: 09/11/2026

Campos del formulario "Time Entry".

| Campo | Valor |
|---|---|
| Activity | SEGUROS ALFA S.A.-GBAS204F - Célula DataStage y AWS (KE064235-049) (Developer) |
| Start Date | 09/11/2026 |
| End Date | 09/11/2026 |
| Time Type | Día hábil, tiempo regular (el 09/11/2026 es lunes y no es festivo en Colombia). **Confirmar el nombre exacto de la opción en la lista** |
| Entry Hours | 8 (supuesto de jornada completa; **ajustar** si fueron otras horas) |

## Notes (texto para pegar en el campo)

T1 Notificaciones NIIF 17: migración del correo de certificación de SNS a SES con formato HTML. Se modificó el job de Glue niif-dev-notification-certification (envío por SES con tolerancia a fallos por destinatario, correo HTML desde plantillas en S3, avisos por tono) con 104 pruebas automáticas. Se organizaron y validaron 6 plantillas HTML para clientes de correo (Outlook, Gmail, móvil) con 6 escenarios de vista previa. Se ajustó el mockup UX (4 estados con variaciones). En AWS DEV se subieron plantillas y script, se unificaron los templatePath de los casos, se ajustó el job y se crearon los permisos IAM necesarios. Se ejecutaron pruebas del job: terminó correctamente y SES aceptó el mensaje. Se diagnosticaron 3 problemas de permisos (SES por destinatario en sandbox, lectura de plantillas y de configuración de prueba). Se identificó que el correo no llega al buzón por DMARC y DKIM del dominio. Se prepararon las solicitudes a DevOps (permisos IAM y registros DNS) y se actualizó la memoria del proyecto.

## Detalle de lo trabajado

### 1. Código del job de notificación (`notification_certification.py`)
- SNS reemplazado por SES (SESv2): parámetro `--SES_SENDER`, `--SES_CONFIGURATION_SET` opcional.
- Envío tolerante a fallos por destinatario: si uno falla, sigue con los demás y marca solo los enviados; si fallan todos, el job falla y no marca nada.
- Correo HTML a partir de `email.templatePath` (plantilla leída de S3), con valores escapados. Si no puede leer la plantilla, envía solo texto y lo registra.
- Avisos por tono: informativo (regenerado) y advertencia (sin datos, disponibilidad por miembro).
- Consultas de Athena estructuradas: una ejecución alimenta texto y HTML.
- Contrato en DynamoDB: `SES_EMAIL` y `sesMessageId`.
- 104 pruebas automáticas (flujo completo, envío parcial, HTML, plantillas).

### 2. Plantillas HTML y UX
- `Backend/htmlNew/` reorganizado: `newUx`, `anterior`, `mockup`, `previews`, `herramientas`.
- 6 plantillas de producción, incluida la nueva `primas_rentas_cert.html` (con resumen).
- Generador de vistas previas (6 escenarios por plantilla) y validador contra el estándar de HTML para correo.
- Correcciones halladas: marcador de periodo de reservas, bloques comentados, desborde en móvil (`table-layout:fixed`), banda de estado sin emoji.
- Mockup UX ajustado a 4 estados con variaciones, con la regla "un estado distinto solo si cambia la acción".

### 3. AWS DEV
- Plantillas subidas a `email-templates/`; `templatePath` de 3 casos unificados (Previsional y las dos de reserva matemática).
- Script subido y job actualizado con `--SES_SENDER`.
- Política IAM del rol (SES sobre identidades, lectura de plantillas y de configuración de prueba).
- Identidad de prueba verificada en SES y JSON de prueba con destinatario único.
- 4 ejecuciones del job: dos fallaron por permisos y se corrigieron; la última terminó correctamente y SES devolvió el identificador del mensaje.
- Diagnóstico: el correo no llega al buzón (DMARC `p=reject` y DKIM de DEV sin publicar). Se comprobó que la dirección no está en la lista de supresión de SES.

### 4. Coordinación y documentación
- Mensajes redactados para DevOps: permisos IAM y entrega para QA/PRD, y registros DNS (DKIM).
- Memoria del proyecto actualizada (notas de la entrega, permisos IAM, repo privado).
- Evidencia de ejecución del job para captura (comandos de solo lectura).

## Pendiente
- Confirmar con el administrador del correo si el mensaje quedó en cuarentena o fue rechazado (rastreo de mensajes).
- Enviar las solicitudes a DevOps y a DNS.
- Probar un caso con resumen y uno compuesto; revisar el correo en Outlook, Gmail y móvil.
- Retirar permisos temporales y de SNS al cerrar la validación.
