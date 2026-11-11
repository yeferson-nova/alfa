# Auditoría: job de notificación nuevo frente al desplegado

Fecha: 2026-11-10. Revisión de código, sin cambios.
- **Actual:** `Backend\Glue\niif-dev-notification-certification\notification_certification.py`. Son 727 líneas, igual a lo desplegado en DEV, con SES y HTML.
- **Nuevo:** `Backend\notificacion\niif-dev-notification-certification\`. Es hexagonal, con 33 módulos y 451 pruebas que pasan.
- **Pregunta:** ¿el nuevo hace todo lo que hace el actual? ¿Trae un ID único y observabilidad completa?

**Veredicto: el nuevo todavía no puede reemplazar al actual.**
- **Le falta el correo en HTML (T1b)**, que ya está probado en QA.
- **Cambió el comportamiento del envío parcial.**
- **Exige argumentos nuevos** que hoy nadie le pasa.
- En observabilidad está mejor que el actual, pero **no trae el ID del flujo** y no repite el contexto en cada línea.

## 1. Lo que sí es equivalente (verificado en el código)
| Comportamiento del actual | En el nuevo |
|---|---|
| Lee la configuración global y la del caso desde S3; falla si el `useCaseId` del JSON no coincide | `leer_configuracion_caso` (`NTF-CFG-002`) |
| Busca los registros por `certCode` en el índice y filtra por `useCaseId`; si no hay, termina sin enviar | Igual, y además **pagina** la consulta (el actual solo lee la primera página) |
| Caso compuesto: valida el control y los miembros con lectura consistente y los cierra con una transacción condicional después del envío | Mismos estados, condiciones y campos (`dominio\compuesto.py`, `dynamo_compuesto.py`) |
| Variables (`env`, `period`, `periodRange`, `useCaseName`, `certCode`, `userEmail`, `executionId`) y `stringVars` | `variables_base` y `variables_finales` |
| Asunto sin saltos de línea y con el prefijo `[REGENERADO] ` | `construir_asunto` |
| Resumen y totales en texto. Si una consulta falla, pone "Error ejecutando consulta" y sigue | `ServicioConsultas.tabla`, que además registra el evento `NTF_CONSULTA_FALLIDA` |
| Avisos de `dataPresence` por miembro, de `sinDatosPeriodo` y de regeneración | `aviso_disponibilidad`, `aviso_por_registros` y `componer_texto` (mismos textos) |
| Enlaces cortos `CERTIFICATE`, `NO_CERTIFICATE` y `DOWNLOAD` (uno por cada `detail#`), con los mismos campos en la tabla | `ServicioEnlaces`. Además usa escritura condicional (no pisa un token) y el largo y la vigencia vienen por configuración |
| Cuerpo de texto del correo | Misma plantilla (`textos.py`) |
| Destinatarios en minúsculas y sin vacíos; falla si no hay ninguno | `normalizar_destinatarios`, que además quita duplicados |
| SES v2 con `EmailTags` y conjunto de configuración opcional; exige `MessageId` | `NotificadorSes` |
| Marca `SENT` con `sentDate`, `SES_EMAIL`, `sesMessageId` y `emailDestination` | `marcar_enviados` (mismos campos) |

**Mejoras del nuevo sobre el actual:**
- La espera de Athena tiene tiempo máximo; el actual podía esperar sin fin.
- Lee todas las páginas de resultados de Athena.
- Ya no escribe el SQL completo en el log (cierra SEG-15).
- No escribe el correo de cada destinatario.
- Los errores llevan código (`NTF-xxx-nnn`).
- Si todos los registros ya están en `SENT`, no reenvía.

## 2. Lo que falta o cambió (antes de reemplazar)
| # | Severidad | Hallazgo | Evidencia | Qué hacer |
|---|---|---|---|---|
| N-01 | **Bloqueante** | **No arma el HTML.** El modelo `Mensaje` y SES admiten HTML, pero el caso de uso nunca lo llena y la configuración no lee `templatePath`. Faltan:<br>• la carga de la plantilla desde `email-template/`;<br>• las tablas HTML de resumen y totales;<br>• los recuadros de aviso (azul de regeneración, ámbar de sin datos) con `$avisoHtml`;<br>• la lista de archivos con `rel="noopener noreferrer"`;<br>• el escape de variables;<br>• la regla de mandar solo texto si la plantilla no tiene `$avisoHtml` y hay avisos. | `notificar_certificacion.py` (el `Mensaje` sin `html`); `configuracion.py` (sin `templatePath`) | Portar `build_html_body`, `result_to_html`, `details_to_html` y `notice_box` del actual, con pruebas contra las 6 plantillas de `Backend\htmlNew\newUx\` |
| N-02 | **Alta** | **Envío parcial distinto.**<br>• El actual sigue con los demás si un destinatario falla, marca `SENT` si salió al menos uno y registra `ENVIO PARCIAL`.<br>• El nuevo se detiene en la primera falla y no marca nada. Si el primero ya salió, el reintento le vuelve a mandar el correo. | `_enviar` del nuevo frente al ciclo `for dest` del actual (l. 700-715) | Mantener la regla del actual: un evento por destinatario fallido y fallar solo si no salió ninguno |
| N-03 | **Alta** | **Contrato de argumentos.**<br>• Pide `--SES_REMITENTE`, pero QA está configurado con `--SES_SENDER`.<br>• Agrega 6 argumentos obligatorios que nadie pasa hoy: `INDICE_CERT_CODE`, `ZONA_HORARIA`, `DIAS_VIGENCIA_ENLACE`, `LONGITUD_TOKEN_ENLACE`, `SEGUNDOS_ENTRE_CONSULTAS` y `SEGUNDOS_MAXIMOS_CONSULTA`.<br>• El trigger no los envía, así que el job falla al arrancar si no están entre sus valores por defecto. | `ajustes.py` (`ARGUMENTOS_OBLIGATORIOS`) | Aceptar el nombre `SES_SENDER` (el mismo que ya tiene QA) y documentar los 6 como argumentos por defecto del job en el paquete para DevOps |
| N-04 | Media | **Cambia el reenvío.** El actual reenvía si se relanza un `certCode` en `SENT` (el paquete de QA lo documenta). El nuevo lo omite. Es más seguro, pero cambia un comportamiento que quizá se usa para reenviar a mano. | `_omision` (`YA_ENVIADO`) | Confirmar con el equipo. Si se necesita reenviar, que sea con una bandera explícita |
| N-05 | Media | **Sustitución silenciosa (ARQ-38) sin corregir.** Sigue usando `safe_substitute`: si falta `period`, la consulta corre con `'$period'` literal y devuelve 0 filas. | `parametrizacion.py` | Fallar si queda alguna variable sin reemplazar |
| N-06 | Media | **Empaquetado.** El punto de entrada (`scripts\job_notificacion.py`) importa el paquete, así que Glue necesita el paquete como `--extra-py-files`. El actual es un solo script. | `scripts\` y `empaquetar.sh` | Definir con DevOps cómo se publica (script + zip del paquete) y llevarlo al estándar (`src\`, `package.sh`) |
| N-07 | Baja | **Largo del token configurable** (hasta 32). Con 32 se cierra SEG-13; con 8 queda igual que hoy. | `ajustes.py` | Fijar 32 en la configuración de los ambientes |
| N-08 | Info | **No cumple aún el estándar de hoy:** código en español (debe ir en inglés con PEP 8), `tests\` en plural y punto de entrada fuera de `src\`. | Estructura | Ajustar al portarlo |
| — | Info | El enlace de descarga sigue abriendo la consola (ARQ-29). Viene del job de certificación, no de este. | — | — |

## 3. ID único y observabilidad
Lo que ya trae: una línea JSON por evento, con `marcaTiempo`, `nivel`, `evento`, `funcion` y `ejecucion_id`. Los eventos son un catálogo cerrado de 13 (`NTF_INICIO_JOB` … `NTF_ERROR_JOB`) y los errores salen clasificados con su código.

| # | Severidad | Hallazgo | Evidencia | Qué hacer |
|---|---|---|---|---|
| O-01 | **Alta** | **El `ejecucion_id` no es el del flujo.** Es el `JOB_RUN_ID` de Glue y, si no llega, se **inventa** con `uuid4()`. No permite unir el correo con la corrida de la Step Function (ARQ-32) y contradice la regla de no inventar IDs. | `ajustes.py` (`id_ejecucion=… or uuid4().hex`) | Recibir el `ejecucion_id` del flujo (por argumento o leído del registro de notificación) y registrar `job_run_id` aparte. No generar ninguno |
| O-02 | **Alta** | **El contexto no va en todas las líneas.**<br>• `cert_code` aparece solo en 5 de los 13 eventos.<br>• `use_case_id` solo en el inicio.<br>• `periodo` en ninguno.<br>Si se busca un `certCode` en CloudWatch, no salen todas las líneas de esa solicitud. | `bitacora_json.py`, llamadas a `registrar` | Fijar el contexto en la bitácora al arrancar (`ejecucion_id`, `job_run_id`, `cert_code`, `use_case_id`, `periodo`) para que salga en cada línea |
| O-03 | Media | **No hay mensaje en español.** Cada línea trae solo el código del evento, y lo decidido es que los mensajes vayan en español. | `bitacora_json.py` | Agregar un campo `mensaje` en español por cada evento del catálogo |
| O-04 | Media | **Los errores de arranque no quedan en JSON.** Si faltan argumentos o falla la composición, el error ocurre antes del `try` y solo sale la traza de Python. | `job_glue.py` (`construir(leer(...))` fuera del `try`) | Registrar `NTF_ERROR_JOB` también en esa fase |
| O-05 | Media | **Faltan eventos para ubicar la falla:**<br>• fallo de envío por destinatario (ver N-02);<br>• qué avisos se agregaron;<br>• si el correo salió con HTML o solo con texto;<br>• el `QueryExecutionId` y los bytes escaneados de Athena (sirven para costos). | Catálogo `EventoLog` | Agregarlos al catálogo |
| O-06 | Media | **No hay métricas.** No emite métricas EMF (éxito, fallo por dominio, duración), que pide el estándar §5. | — | Agregar EMF al cerrar el job |

## 4. Orden recomendado
1. N-01 (HTML), N-02 (envío parcial) y N-03 (argumentos): sin estos, el nuevo empeora lo que ya funciona en QA.
2. O-01 y O-02: el ID y el contexto en cada línea. Es la base de la parte 5 (observabilidad) y lo comparten todas las lambdas (`_shared\`).
3. O-03 a O-06, N-05 y N-07.
4. Llevarlo al estándar (N-08) al empaquetarlo.

Hasta que se cierre el punto 1, lo que se entrega sigue siendo el job actual.
