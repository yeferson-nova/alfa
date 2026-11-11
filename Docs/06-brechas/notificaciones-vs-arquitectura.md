# Notificación y certificación: brechas contra la arquitectura y la calidad de código

Fecha: 2026-11-10. Alcance: paso 3 (Certificación de cifras) en DEV. Fuente del diseño: `Docs\05-arquitectura\diagramas\NIIF17_ModeloSolucion_V1.0.drawio`, hoja `Página-2` (detalle), `Página-9` (vista por capas) y `Página-10` (vista general). Lo desplegado se tomó de DEV (solo lectura) y del código descargado en `Backend\Lamnbdas\`.

Convención: **[V]** verificado en código o en AWS DEV; **[I]** interpretación del diagrama (las flechas no llevan texto); **[?]** sin verificar.

## 1. Qué dice el diagrama (Página-2)

| Tramo | Componentes |
|---|---|
| Disparo | Malla de ejecución → **Modelo Notificaciones** (DynamoDB) → **Lambda (Notificación)** |
| Armado del correo | Lambda → **Job Notificación** (Glue) → Athena → Catálogo Zona Oro; el job escribe en **Zona Certificación** (S3) |
| Envío | Lambda → **Mensaje** (icono de Amazon SES) → Usuario Outlook Alfa |
| Decisión | Usuario → **WAF** → **API Gateway** → Lambda (Notificación) / Zona Certificación; `postLogin()` → **Cognito** ↔ Azure AD |
| Salida | Lambda → **Zona Transporte Salida** (S3) → **Lambda (SFTP)** → Transit Gateway → Firewall → SFTP de FIS |
| Reprogramación | Lambda (Notificación) → **Modelo Configuración Extracción** → **Programador** (EventBridge Scheduler) → Malla **[I]** |

## 2. Qué hay hoy en DEV

Una sola caja "Lambda (Notificación)" del diagrama son, en la práctica, **cinco lambdas, un job y tres tablas** [V]:

| Pieza real | Función |
|---|---|
| `niif-dev-notification-certification-trigger` | Stream de DynamoDB → si `PENDING` y `queryName = detail#1` y sin `sendLock`, inicia el job |
| Job `niif-dev-notification-certification` | Athena, enlaces cortos, arma HTML, **envía por SES** (T1a), marca `SENT` |
| `niif-dev-composite-notification-evaluator` | Junta mallas hermanas y dispara un solo correo |
| `niif-dev-certification` (vía API Gateway) | Recibe Certificar / No certificar y cambia el estado |
| `niif-dev-copy-s3-cert` | `CERTIFICATE` → copia a `export/` del bucket de transporte |
| `niif-dev-move-s3-certification` | `NO_CERTIFICATE` → mueve a `NO-CERTIFICADOS/` |
| `datalake-dev-niif-send-via-sftp` | Imagen de contenedor; SFTP a FIS **[?]** (imagen y logs sin revisar) |

## 3. Alineado

- Malla → DynamoDB → lambda → job de Glue: coincide con el diagrama [V].
- **Envío por SES:** el diagrama dibuja SES (icono "Mensaje"); hasta T1a el código publicaba en SNS. Con el job nuevo la implementación vuelve a coincidir con el diseño (pendiente de QA).
- Zona Transporte Salida → Lambda SFTP: existe la cadena `export/` → `send-via-sftp` [V].
- Autenticación con Cognito sobre Azure AD: existe la cadena `/r` → Cognito → `/post_login` → `/certify` [V].

## 4. Brechas de arquitectura

| ID | Brecha | Evidencia | Propuesta |
|---|---|---|---|
| ARQ-21 | **El diagrama no representa lo desplegado.** Faltan: evaluador compuesto, enlaces cortos (`/r` y su tabla), tabla del grupo compuesto, lambdas `copy`/`move` y la transición de estados (`PENDING → SENT → CERTIFICATE → TRANSPORT` / `NO_CERTIFICATE → NO_TRANSPORT`). | [V] contraste de §1 con §2 | Actualizar la hoja `Página-2` (o crear una hoja "Certificación detallada") y mantenerla como fuente de verdad. |
| ARQ-22 | **Quién envía el correo.** El diagrama dibuja Lambda → Mensaje; hoy lo envía el **job**. | [V] `send_ses` en el job | Recomendado: dejarlo en el job (ya tiene tablas y HTML) y corregir el diagrama. Alternativa: el job solo arma y la lambda envía (más piezas, sin ganancia clara). Decisión del equipo. |
| ARQ-23 | **"No certificar" no deja la tarea pendiente en el modelo de configuración.** Víctor (L276-277): debe ir al modelo de configuración y quedar pendiente hasta que negocio avise **a mano** al operador. Hoy `move_s3.py` solo archiva el archivo y marca `NO_TRANSPORT`. | [V] código de `move_s3.py` y del manager; transcripción L276-277 y L284 | Escribir en `niif-dev-prg-programaciones` (`PK = PROGRAMACION#<MALLA>`, `SK = METADATA`) los marcadores que el manager ya reconoce: ver §4.1. |
| ARQ-24 | **Mapa de carpetas duplicado** (`NIIF_CARPETAS`, 8 entradas) copiado a mano en `copy_s3.py` y `move_s3.py`. Un caso nuevo (p. ej. `primasPCR`) exige tocar los dos sin ayuda de prueba alguna. | [V] `grep` en ambos archivos | Moverlo a la configuración global del caso (JSON) o a una capa compartida. |
| ARQ-25 | **Disparo sin candado real.** El trigger solo lee `sendLock` ("no lo crea", comentario en el código). Dos eventos seguidos pueden iniciar dos jobs; el job tampoco es idempotente en la versión desplegada. | [V] líneas 43-47 del trigger; ver ARQ-17 | El job nuevo (`reposNew`) ya es idempotente; el trigger debería escribir el candado con condición. |
| ARQ-26 | **Fallos silenciosos en las tres lambdas de estado.** Trigger, `copy` y `move` capturan toda excepción, la registran y devuelven éxito (`{"status": "success"}`): el stream no reintenta ni hay cola de mensajes fallidos. Un registro puede quedar en `CERTIFICATE` sin llegar nunca a `TRANSPORT`. | [V] `logger.exception` + `return` en `move_s3.py:174-177` y `copy_s3.py:163-166` | Relanzar tras registrar (o destino `onFailure` / DLQ) y alarma por estado atascado. |
| ARQ-27 | **Parámetro con texto literal:** el evaluador compuesto envía `--ENVIRONMENT` = `'{env}'` (sin interpolar). | [V] `composite_notification_evaluator.py:206` | Verificar si algún job lo usa; si no, quitarlo. |
| ARQ-28 | **Seguridad perimetral sin confirmar.** El diagrama pone WAF delante de API Gateway. | [?] no se consultó WAF en DEV | Comprobar `wafv2 get-web-acl-for-resource` sobre la etapa `dev` (comando de solo lectura). |
| ARQ-29 | **El enlace de descarga del CSV abre la consola de AWS, no descarga.** El job `niif-<amb>-certification` guarda en `s3_url` un enlace al portal SSO que abre la consola de S3 (`construir_url_consola_s3`); la notificación lo acorta y la lambda de redirección solo hace `302`. El usuario debe pulsar Download y necesita un rol en la cuenta. Víctor (L270-271) y el diagrama (API Gateway → Zona Certificación) esperan descarga mediada por el API. | [V] código de `niif_certification.py` y de `datalake-dev-niif-short-link-redirect`; prueba en QA del 2026-11-10 (captura) | Opción A: acción `DOWNLOAD` tras el login con Cognito, lambda que valida aprobador y responde `302` a una URL prefirmada de 1 a 5 min; el job de certificación guarda bucket y clave. Prefirmar en el job no sirve (vence con la sesión del rol). |
| SEG-13 | **Token de enlace corto de 8 caracteres hexadecimales** (`uuid4()[:8]`, 32 bits) y ruta `/r` sin autenticación: si la descarga se resolviera allí, quien adivine o reenvíe el enlace obtendría datos financieros. | [V] `crear_short_link` y redirección | Token largo (≥ 128 bits) y descarga solo tras autenticar (ARQ-29, opción A). |

### 4.1 Modelo de configuración de la programación (verificado en DEV el 2026-11-10)

Tabla `niif-dev-prg-programaciones`, una fila por malla. Campos: `activo`, `scheduleEstado`, `scheduleState`, `scheduleExpression`, `scheduleName`, `scheduleTimezone`, `periodoNegocio`, `modoEjecucion`, `fechaActualizacion`.

- El manager (`niif-dev-manager-configuration`) se dispara por el stream solo si `activo = true` **y** `scheduleEstado = PENDIENTE`.
- Antes de crear el schedule **rechaza** la fila si `scheduleExpression = "PENDIENTE"` o `periodoNegocio = "PENDIENTE"` (`validar_programacion`). Es decir, el código ya prevé el valor centinela `PENDIENTE` para "falta que alguien lo programe".
- Si la fila es válida crea o actualiza el schedule en el grupo `niif-dev-programacion-configuradores` con el estado de `scheduleState` (por defecto `DISABLED`; las expresiones `at(...)` se borran solas al terminar) y escribe `scheduleEstado = PROGRAMADO`.
- **Nadie actualiza la fila cuando el schedule termina.** Primas ARL sigue en `PROGRAMADO` / `ENABLED` con `at(2026-11-04T04:19:14)` y ese schedule ya no existe en Scheduler; el grupo solo contiene `niif-dev-prueba-auxilios-f7429360` (`DISABLED`). La tabla no refleja la realidad.

**Propuesta (no confirmada) para "No certificar":** actualizar la fila de la malla con `scheduleEstado = PENDIENTE`, `scheduleExpression = "PENDIENTE"` y `periodoNegocio = "PENDIENTE"`, para que el manager no cree nada hasta que un operador cargue la fecha y el periodo reales. Riesgo: al ponerse la fila en `PENDIENTE` con `activo = true`, el stream la entrega al manager, que lanza `ProgramacionError`; con `MaximumRetryAttempts = 2` y sin destino `OnFailure` el registro se descarta tras los reintentos, solo con un error en el log. Es inofensivo, pero ruidoso. Alternativas: dejar `scheduleEstado = "RECHAZADO"` (el filtro lo ignora, sin error) y que el operador lo cambie a `PENDIENTE` al reprogramar. Hay que acordarlo con Víctor porque el operador ya trabaja con esta tabla.

Notas: `periodoNegocio` y el nombre del schedule de ARL no coinciden (`202512` frente a `202605` en `scheduleName`); RVI está en `202609`. Y `SATELITES`, `GENERALES` y `PCR_VIDA` no tienen `scheduleEstado`.

## 5. Brechas de seguridad en la cadena de botones

| ID | Brecha | Evidencia | Propuesta |
|---|---|---|---|
| SEG-10 | **Cognito con flujo implícito:** el cliente `datalake-dev-niif-api` usa el flujo implícito, en el que los tokens se entregan por el navegador. | [V] configuración del cliente de Cognito | Cambiar a código de autorización con PKCE. |
| SEG-11 | **`state` sin firma** en el redireccionamiento: no protege contra CSRF ni manipulación de la acción. | [V] cadena `/r` → `/post_login` | Firmarlo (HMAC con secreto en Secrets Manager) o usar el `state` de Cognito ligado a sesión. |
| SEG-12 | **Se registran tokens y la imagen completa de DynamoDB en CloudWatch** (el trigger hace `logger.info(new)`, con correo del usuario). | [V] trigger L36; tokens en logs de la cadena `/post_login` | Registrar solo identificadores; enmascarar correos. |

## 6. Calidad de código

Contra `Docs\08-estandares-desarrollo\` (hexagonal, sin valores quemados, taxonomía de errores, logs JSON, pruebas > 80 %):

| Pieza | Situación | Brecha |
|---|---|---|
| Job desplegado (`notification_certification.py`, 727 líneas) | Ejecuta al importar (lee argumentos a nivel de módulo); `main()` de ~120 líneas con 8 responsabilidades; nombres heredados de SNS (`subject_sns`, `mensaje_sns`); sin pruebas propias antes del cambio de SES; registra todos los argumentos. | **Cumple poco.** Se mantuvo a propósito el cambio mínimo (decisión del usuario). |
| Job nuevo en `Backend\Glue\reposNew\niif-dev-notification-certification` | Hexagonal, con taxonomía de errores, logs JSON, idempotencia, argumentos sin valores fijos; 163 pruebas pasan. | **Cumple, pero no está desplegado ni probado en AWS.** Es el camino para cerrar la brecha de calidad. |
| Lambdas trigger, evaluador, `copy`, `move` | Sin pruebas, sin estructura, `print`/`logger` de texto libre, excepciones tragadas (ARQ-26), mapa duplicado (ARQ-24), `os.environ[...]` de una variable SNS que ya no usan. | **No cumplen.** |
| Cobertura / Sonar | Quality Gate fallido: Maintainability B (204 olores en nuevo código) y 12 hotspots `Web:S5148` de las plantillas (corregidos con `rel="noopener noreferrer"`, pendiente de reanalizar). | Falta el desglose por regla de los 204 olores para decidir. |

## 7. Orden sugerido

1. **Cerrar en QA el cambio mínimo (T1a/T1b)** — ya empaquetado.
2. **Decidir ARQ-22 y ARQ-23** con Víctor (qué debe pasar al no certificar y quién envía): cambian el diseño, no solo el código.
3. **Corregir lo barato y de riesgo alto:** ARQ-26 (no tragar errores), ARQ-27, SEG-12.
4. **Desplegar el job hexagonal** (`reposNew`) en DEV tras QA y retirar el monolito; resuelve ARQ-25 y gran parte del §6.
5. **Unificar las cuatro lambdas** de estado en un paquete con estructura y pruebas (ARQ-24).
6. **Seguridad de la cadena de botones** (SEG-10, SEG-11) junto con DevOps: toca Cognito y el API.
7. **Actualizar el diagrama** (ARQ-21) cuando 2 y 4 estén decididos, para no dibujarlo dos veces.

## 8. Qué falta verificar (comandos de solo lectura)

- WAF sobre el API (ARQ-28), `ImageUri` y logs de `send-via-sftp`, y `aws scheduler list-schedules` (para ver si algo reprograma mallas).
- Rastreo de mensajes del correo de prueba (por qué no llegó a la bandeja en DEV).
