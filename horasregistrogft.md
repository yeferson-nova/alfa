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
| [13/11/2026](#dia-2026-11-13) | Viernes | 8 | T1: guía de prueba en QA del correo HTML por SES; spike de los bugs de Primas ARL y Primas Rentas |
| [12/11/2026](#dia-2026-11-12) | Jueves | 8 | T1: job de notificación modular con SES y HTML (477 pruebas, 36/36 contra el probado); spike de Primas ARL con las pruebas de QA |
| [11/11/2026](#dia-2026-11-11) | Miércoles | 8 | T1: entrega de SES con HTML a QA, validación contra lo desplegado y corrección de plantillas en la configuración |
| [10/11/2026](#dia-2026-11-10) | Martes | 8 | T1: prueba en QA del correo HTML por SES y job de notificación nuevo con trazabilidad |
| [09/11/2026](#dia-2026-11-09) | Lunes | 8 | T1: migración del correo de certificación de SNS a SES con HTML |

---

<a id="dia-2026-11-13"></a>
## 13/11/2026 (viernes)

| Campo | Valor |
|---|---|
| Start Date | 13/11/2026 |
| End Date | 13/11/2026 |
| Time Type | Día hábil, tiempo regular (viernes, no es festivo en Colombia) |
| Entry Hours | 8 (supuesto de jornada completa; **ajustar** si fueron otras horas) |

### Notes (texto para pegar en el campo)

T1 Notificaciones: guía de prueba en QA del envío HTML por SES sin la Step Function y diagnóstico de errores. Spikes de Primas ARL (el SCD apaga datos válidos, plan de fix) y Primas Rentas (75.181 frente a 401 registros, causa en el join).

### Detalle de lo trabajado

#### 1. Prueba en QA del correo HTML por SES
- Guía de comandos para CloudShell válida en DEV y QA: tomar los argumentos del trigger, ubicar un `certCode` existente, subir una copia de prueba del JSON con un único destinatario y lanzar solo el job de notificación.
- Se corrigió la guía: el filtro devolvía dos lambdas y solo una dispara el job de notificación.
- Diagnóstico del `AccessDenied` de QA: S3 lo devuelve cuando el archivo pedido no existe y el rol no puede listar el bucket. Se verificó la plantilla (existe, 15 KB) y se dieron comandos para revisar las otras dos claves.
- Comandos para validar que la plantilla tenga los marcadores y que todas las plantillas de los JSON desplegados existan.

#### 2. Spike del bug de Primas ARL
- Simulación del flujo completo (extracción, remediación, Silver y SCD) con Oracle, S3 y Glue simulados: reproduce el incidente, confirma que el fix de extracción funciona y muestra que el SCD sin periodo apaga datos válidos.
- Corrección propuesta y plan de entrega en 6 días hábiles, en tres líneas de trabajo.
- Preguntas para negocio sobre si cada corrida trae el acumulado o solo el periodo.

#### 3. Spike del bug de Primas Rentas
- Comparación línea por línea del SQL de composición V2.1 con el job de la Silver: misma lógica salvo el join con `prv_rentas`, al que le falta el límite inferior de la fecha de expedición.
- Explicación de la diferencia: el job cuenta las rentas vigentes que cobraron en el mes y el SQL cuenta las expedidas en el mes.
- Confirmación de que el script de producción es idéntico al del repo y alcance del problema (solo Primas Rentas).
- Fix propuesto y orden de entrega: la línea sola rompería la malla en la primera corrida; sale junto con el SCD por periodo.

### Pendiente
- Resultado de la prueba en QA con el JSON de caso de un solo destinatario.
- Respuesta de negocio sobre acumulado o solo el periodo.
- Escribir y probar el código de los fixes de ARL y Rentas.

---

<a id="dia-2026-11-12"></a>
## 12/11/2026 (jueves)

| Campo | Valor |
|---|---|
| Start Date | 12/11/2026 |
| End Date | 12/11/2026 |
| Time Type | Día hábil, tiempo regular (jueves, no es festivo en Colombia) |
| Entry Hours | 8 (supuesto de jornada completa; **ajustar** si fueron otras horas) |

### Notes (texto para pegar en el campo)

T1 Notificaciones: job de notificación modular con SES y correo HTML, 477 pruebas e idéntico al job probado en 36 de 36 escenarios; solicitud a DevOps. Spike de Primas ARL: análisis de las pruebas en QA, el SCD no distingue periodos.

### Detalle de lo trabajado

#### 1. Job de notificación modular (SES y HTML)
- Correo HTML desde `email.templatePath`, con valores escapados; si no puede leer la plantilla, envía solo texto y lo registra.
- Envío por destinatario: si uno falla sigue con los demás y marca solo los enviados; si fallan todos, el job falla y no marca nada.
- Avisos por tono (informativo y advertencia), tablas de resumen, totales y detalle, y enlaces cortos.
- 477 pruebas y 99,9 % de cobertura; el comparador contra el job probado dio 36 de 36 iguales y detecta los cambios de control.
- Migración a PEP 8 en inglés, sin tocar los contratos externos; empaquetado en zip y script por ambiente.
- Commits en el repositorio del proyecto.

#### 2. Coordinación con DevOps
- Solicitud actualizada: permisos de SES y de lectura de plantillas y del paquete, argumentos por defecto del job y publicación del zip y del script.
- Preguntas abiertas para DevOps: dónde se publica el paquete, cómo el pipeline resuelve los enlaces por ambiente y la salida del sandbox de SES.
- Requisitos del cambio de dominio de SES (identidad, DKIM, SPF y DMARC) para la reunión con Seguridad y el administrador de AWS.

#### 3. Spike del bug de Primas ARL (pruebas en QA)
- Auxilios Funerarios (periodo 202512): el freno detuvo 90 de 90 activos, todos del periodo anterior.
- Primas ARL (periodo 202509): 2.099 de 2.099; la prueba usó `=` en el filtro del periodo y producción usa `<=`, por lo que no ejercitó el fix.
- Causa: el SCD compara contra todos los activos de Gold y no conoce el periodo.
- Propuesta: paquete 1 solo con la extracción y paquete 2 con el SCD por periodo; método de prueba de ARL con una corrida base con tope y luego la incremental.

### Pendiente
- Respuestas de DevOps sobre la publicación del paquete y los enlaces por ambiente.
- Decisión sobre el valor del tiempo máximo de consulta de Athena del job.
- Prueba del job modular en QA con SES.

---

<a id="dia-2026-11-11"></a>
## 11/11/2026 (miércoles)

| Campo | Valor |
|---|---|
| Start Date | 11/11/2026 |
| End Date | 11/11/2026 |
| Time Type | Día hábil, tiempo regular (miércoles, no es festivo en Colombia) |
| Entry Hours | 8 (supuesto de jornada completa; **ajustar** si fueron otras horas) |

### Notes (texto para pegar en el campo)

T1 Notificaciones: entrega de SES con HTML a QA. Validación del job y las plantillas HTML contra lo desplegado, rutas de plantillas corregidas en 7 JSON de configuración y checklist de entrega para DevOps.

### Detalle de lo trabajado

#### 1. Validación frente a lo desplegado
- Comparación por hash del job, las plantillas y las lambdas con lo que está en DEV.
- Resultado: job con SNS en el repositorio, plantillas idénticas y lambdas sin cambios.
- Paridad SNS a SES: mismos argumentos y mismas claves del JSON global; el trigger no pasa el remitente, así que va por defecto en el job.

#### 2. Configuración de los casos
- Corrección de la ruta de las plantillas HTML (`templatePath`) en 7 JSON a `email-template/`.
- Hallazgos de configuración para DevOps: carpeta de plantillas, bucket donde se despliegan y comportamiento de `--delete` en el pipeline.

#### 3. Entrega a QA
- Paquete del job con SES y checklist de entrega para DevOps.

### Pendiente
- Confirmar con DevOps el bucket de plantillas y el `--delete` del pipeline.
- Prueba en QA del correo con SES.

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

T1 Notificaciones: prueba en QA del correo HTML por SES (llegó a la bandeja), permisos corregidos y política IAM del paquete. Job de notificación nuevo con envío parcial y trazabilidad, 514 pruebas, probado en Glue DEV.

### Detalle de lo trabajado

#### 1. Prueba en QA del correo (SES + HTML)
- Comandos de prueba adaptados a QA y ajuste del caso de prueba.
- Diagnóstico de fallas: argumento `--SES_SENDER` faltante, permisos de lectura de plantillas y de envío sobre el conjunto de configuración de SES.
- Corrección de la política IAM del paquete de entrega y de su LEEME.
- Resultado: el job terminó correctamente y **el correo llegó a la bandeja**.
- Mensaje de estado para el equipo sobre los bloqueos de permisos de infraestructura.

#### 2. Job de notificación nuevo
- Correcciones: correo HTML, envío parcial, argumentos, consultas estrictas, token de 32 caracteres y cancelación de consultas que exceden el tiempo.
- Trazabilidad: ID del flujo sin inventar, contexto en cada línea, mensajes en español, errores de arranque en JSON y resumen para métricas.
- Empaquetado en bash y PowerShell; prueba en Glue de DEV con un job aparte: terminó correctamente con el log completo.
- 514 pruebas automáticas.

### Pendiente
- Prueba en DEV del job nuevo con envío real y revisión del HTML.
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

T1 Notificaciones: migración del correo de certificación de SNS a SES con HTML. Job con 104 pruebas, 6 plantillas HTML validadas, permisos IAM en DEV, prueba exitosa y solicitudes a DevOps por DKIM y permisos.

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
- Evidencia de ejecución del job para captura (comandos de solo lectura).

### Pendiente
- Confirmar con el administrador del correo si el mensaje quedó en cuarentena o fue rechazado (rastreo de mensajes).
- Enviar las solicitudes a DevOps y a DNS.
- Probar un caso con resumen y uno compuesto; revisar el correo en Outlook, Gmail y móvil.
- Retirar permisos temporales y de SNS al cerrar la validación.
