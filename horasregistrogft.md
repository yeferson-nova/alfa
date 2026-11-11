# Registro de horas

Campos del formulario "Time Entry". Un apartado por día, del más reciente al más antiguo.

## Datos que no cambian
| Campo | Valor |
|---|---|
| Activity | SEGUROS ALFA S.A.-GBAS204F - Célula DataStage y AWS (KE064235-049) (Developer) |
| Time Type | Día hábil, tiempo regular. **Confirmar el nombre exacto de la opción en la lista** |
| Entry Hours | 8 por día (supuesto de jornada completa; **ajustar** si fueron otras horas) |

## Resumen
| Fecha | Día | Horas | Tema |
|---|---|---|---|
| [10/11/2026](#dia-2026-11-10) | Martes | 8 | T1: notificación vs. arquitectura, prueba en QA, limpieza de DEV, job nuevo con trazabilidad y roadmap |
| [09/11/2026](#dia-2026-11-09) | Lunes | 8 | T1: migración del correo de certificación de SNS a SES con HTML |

---

<a id="dia-2026-11-10"></a>
## 10/11/2026 (martes)

| Campo | Valor |
|---|---|
| Start Date | 10/11/2026 |
| End Date | 10/11/2026 |
| Time Type | Día hábil, tiempo regular (martes, no es festivo en Colombia) |
| Entry Hours | 8 |

### Notes (texto para pegar en el campo)

T1 Notificaciones NIIF 17: se comparó la cadena de notificación y certificación con el diagrama de arquitectura y la calidad de código, y se documentaron brechas de arquitectura, seguridad y costos. Se probó en QA el correo de certificación por SES con HTML: el correo llegó a la bandeja, y se diagnosticaron y corrigieron los permisos faltantes (conjunto de configuración de SES y lectura de plantillas). Se levantó evidencia de uso de los recursos de AWS DEV y se clasificó qué está vigente, qué es de otros equipos y qué se puede retirar. Se bajó y validó el código desplegado de las lambdas y jobs de la cadena. Se auditó el job de notificación nuevo frente al desplegado y se completó (HTML, envío parcial, trazabilidad con un ID único y logs en español), con 514 pruebas automáticas; se probó en Glue de DEV con un job aparte. Se reorganizó el código en carpetas por componente, con esqueletos de las lambdas y una librería común de observabilidad. Se elaboró el roadmap de la cadena completa, con fases, decisiones y orden de ejecución.

### Detalle de lo trabajado

#### 1. Arquitectura y brechas
- Comparación de la cadena de notificación con el diagrama (`Página-2`): brechas ARQ-21 a ARQ-28 y SEG-10 a SEG-12, más ARQ-29 (el enlace de descarga abre la consola de S3) y SEG-13 (token corto).
- Análisis de la transcripción de la reunión PCR (No certificar debe dejar la malla pendiente) y revisión del modelo de configuración en DEV.
- Plan de trabajo para unificar la cadena, en fases.
- Trazabilidad: se confirmó que no hay un ID único de punta a punta (ARQ-32) y que certificar/rechazar no registra el resultado en el log (ARQ-33).
- Auditoría de las consultas de Athena: Gold sin particiones, sin tope de escaneo, filtros que impiden podar, redondeo que puede descuadrar, resumen de ARL vacío y sustitución silenciosa de variables (COS-07 a COS-09, ARQ-34 a ARQ-38, SEG-15).

#### 2. Prueba en QA del correo (SES + HTML)
- Comandos de prueba adaptados a QA y ajuste del caso de prueba.
- Diagnóstico de fallas: argumento `--SES_SENDER` faltante, permisos de lectura de plantillas y de envío sobre el conjunto de configuración de SES.
- Corrección de la política IAM del paquete de entrega y de su LEEME.
- Resultado: el job terminó correctamente y **el correo llegó a la bandeja**.
- Mensaje de estado para el equipo sobre los bloqueos de permisos de infraestructura.

#### 3. Limpieza y evidencia de AWS DEV
- Script de solo lectura para medir el uso real de lambdas, jobs, Step Functions, streams, API Gateway y WAF.
- Clasificación de recursos: vigentes, de otros equipos y candidatos a retirar. Hallazgos: API viejo sin WAF (SEG-14) y transporte a FIS que nunca funcionó (ARQ-30).
- Carpeta única de evidencias de AWS (`EvidenciasAws`), fuera de Git.
- Descarga y validación del código desplegado de las lambdas y jobs de la cadena; se actualizaron las copias que estaban desfasadas.

#### 4. Estándares y decisiones
- Lineamiento de DevOps para NIIF 17: nombres, repositorios, estructura y pruebas. Entrega por carpeta comprimida con el nombre del recurso.
- Estándar de código: PEP 8 en inglés, logs en español con un ID de trazabilidad en cada línea.
- Decisiones registradas: SES también para alertas técnicas, Grafana fuera de alcance, una sola lambda orquestadora y nombres en el estándar.

#### 5. Job de notificación nuevo
- Auditoría frente al job desplegado y correcciones: correo HTML, envío parcial, argumentos, consultas estrictas, token de 32 caracteres y cancelación de consultas que exceden el tiempo.
- Trazabilidad: ID del flujo sin inventar, contexto en cada línea, mensajes en español, errores de arranque en JSON y resumen para métricas.
- Librería común de observabilidad, copiada en cada paquete.
- Empaquetado en bash y PowerShell; prueba en Glue de DEV con un job aparte: terminó correctamente con el log completo.
- 514 pruebas automáticas.

#### 6. Organización y planificación
- Código nuevo en `Backend\notificacion\`: `email-template`, `lambdas` (esqueletos de las 5 lambdas) y `jobs-glue`.
- Roadmap de la cadena completa: fases, decisiones (D1 a D7), lista de avance y orden de ejecución.
- Memoria del proyecto y skill actualizadas.

### Pendiente
- Prueba en DEV del job nuevo con envío real y revisión del HTML.
- Decisiones del equipo: No certificar (D2), descarga del CSV (D3), reenvío (D4) y la ruta del API (D7).
- Entregar a DevOps el paquete del job nuevo con sus argumentos y permisos.

---

<a id="dia-2026-11-09"></a>
## 09/11/2026 (lunes)

| Campo | Valor |
|---|---|
| Start Date | 09/11/2026 |
| End Date | 09/11/2026 |
| Time Type | Día hábil, tiempo regular (el 09/11/2026 es lunes y no es festivo en Colombia) |
| Entry Hours | 8 (supuesto de jornada completa; **ajustar** si fueron otras horas) |

### Notes (texto para pegar en el campo)

T1 Notificaciones NIIF 17: migración del correo de certificación de SNS a SES con formato HTML. Se modificó el job de Glue niif-dev-notification-certification (envío por SES con tolerancia a fallos por destinatario, correo HTML desde plantillas en S3, avisos por tono) con 104 pruebas automáticas. Se organizaron y validaron 6 plantillas HTML para clientes de correo (Outlook, Gmail, móvil) con 6 escenarios de vista previa. Se ajustó el mockup UX (4 estados con variaciones). En AWS DEV se subieron plantillas y script, se unificaron los templatePath de los casos, se ajustó el job y se crearon los permisos IAM necesarios. Se ejecutaron pruebas del job: terminó correctamente y SES aceptó el mensaje. Se diagnosticaron 3 problemas de permisos (SES por destinatario en sandbox, lectura de plantillas y de configuración de prueba). Se identificó que el correo no llega al buzón por DMARC y DKIM del dominio. Se prepararon las solicitudes a DevOps (permisos IAM y registros DNS) y se actualizó la memoria del proyecto.

### Detalle de lo trabajado

#### 1. Código del job de notificación (`notification_certification.py`)
- SNS reemplazado por SES (SESv2): parámetro `--SES_SENDER`, `--SES_CONFIGURATION_SET` opcional.
- Envío tolerante a fallos por destinatario: si uno falla, sigue con los demás y marca solo los enviados; si fallan todos, el job falla y no marca nada.
- Correo HTML a partir de `email.templatePath` (plantilla leída de S3), con valores escapados. Si no puede leer la plantilla, envía solo texto y lo registra.
- Avisos por tono: informativo (regenerado) y advertencia (sin datos, disponibilidad por miembro).
- Consultas de Athena estructuradas: una ejecución alimenta texto y HTML.
- Contrato en DynamoDB: `SES_EMAIL` y `sesMessageId`.
- 104 pruebas automáticas (flujo completo, envío parcial, HTML, plantillas).

#### 2. Plantillas HTML y UX
- `Backend/htmlNew/` reorganizado: `newUx`, `anterior`, `mockup`, `previews`, `herramientas`.
- 6 plantillas de producción, incluida la nueva `primas_rentas_cert.html` (con resumen).
- Generador de vistas previas (6 escenarios por plantilla) y validador contra el estándar de HTML para correo.
- Correcciones halladas: marcador de periodo de reservas, bloques comentados, desborde en móvil (`table-layout:fixed`), banda de estado sin emoji.
- Mockup UX ajustado a 4 estados con variaciones, con la regla "un estado distinto solo si cambia la acción".

#### 3. AWS DEV
- Plantillas subidas a `email-templates/`; `templatePath` de 3 casos unificados (Previsional y las dos de reserva matemática).
- Script subido y job actualizado con `--SES_SENDER`.
- Política IAM del rol (SES sobre identidades, lectura de plantillas y de configuración de prueba).
- Identidad de prueba verificada en SES y JSON de prueba con destinatario único.
- 4 ejecuciones del job: dos fallaron por permisos y se corrigieron; la última terminó correctamente y SES devolvió el identificador del mensaje.
- Diagnóstico: el correo no llega al buzón (DMARC `p=reject` y DKIM de DEV sin publicar). Se comprobó que la dirección no está en la lista de supresión de SES.

#### 4. Coordinación y documentación
- Mensajes redactados para DevOps: permisos IAM y entrega para QA/PRD, y registros DNS (DKIM).
- Memoria del proyecto actualizada (notas de la entrega, permisos IAM, repo privado).
- Evidencia de ejecución del job para captura (comandos de solo lectura).

### Pendiente
- Confirmar con el administrador del correo si el mensaje quedó en cuarentena o fue rechazado (rastreo de mensajes).
- Enviar las solicitudes a DevOps y a DNS.
- Probar un caso con resumen y uno compuesto; revisar el correo en Outlook, Gmail y móvil.
- Retirar permisos temporales y de SNS al cerrar la validación.
