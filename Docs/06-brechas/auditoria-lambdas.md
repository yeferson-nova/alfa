# Auditoría de las lambdas del flujo NIIF 17

**Fecha:** 2026-11-05. **Complemento de:** [auditoria-pipeline-datos-vs-arquitectura.md](auditoria-pipeline-datos-vs-arquitectura.md).

**Alcance.** Las lambdas heredadas que rodean al pipeline: el gestor de configuración (6 configuradores y el programador) y las de notificación, certificación y transporte. **Queda fuera** la lambda unificada del spike de la nueva arquitectura (`Backend\Lamnbdas\inventario-aws\t2 unificadas` y `spike-primas-rentas`).

**Qué código se vio.** El inventario es la única fuente (no hay repo Git), así que la cobertura es desigual:

| Lambda | Código visto | Cómo se auditó |
|---|---|---|
| `niif-dev-configuration-auxilios-funerarios` | Sí, real (901 líneas) | Lectura del handler y de la configuración |
| `configuration-primas-arl / rentas / endosos / arp / rvi` | **Copia de la lambda anterior**, no la desplegada (`ORIGEN.md`) | Solo configuración; se asume equivalente a Auxilios (92 % igual según el inventario) |
| `niif-dev-manager-configuration` | **No** (`PENDIENTE.md`) | Solo configuración |
| `niif-dev-short-link-redirect` (y su versión `datalake-dev-`) | Sí (la anterior); la actual solo trae librerías | Código de la anterior + configuración |
| `niif-dev-certification` (aprobación) | Sí, 566 líneas | Lectura parcial: autenticación, decisión y auditoría |
| `niif-dev-composite-notification-evaluator`, `niif-dev-finalizador-ejecucion` | Sí | Estructura y condiciones; no línea por línea |
| `EnvioSftpFis`, `datalake-dev-niif-send-via-sftp` | Sí (`EnvioSftpFis`) | Lectura completa |
| `datalake-dev-niif-notification-certification`, `copy-s3`, `move-s3` | Sí | Lectura completa |
| `ALFA-DEV-DATALAKE-PipelineObserver` | Sí | Solo configuración |

**Lo que no se pudo verificar:** las políticas IAM de cada rol, las políticas de la API Gateway, el código del manager y el de `send-via-sftp`. Lo que depende de eso va marcado **hipótesis**.

---

## 1. Veredicto

Las lambdas **más nuevas** (evaluador compuesto, finalizador, configurador) están escritas con cuidado: validan entradas, usan escrituras condicionales y fallan con errores claros. Las **de la generación anterior** que mueven el dato fuera de AWS (SFTP, copia, movimiento) y la de redirección son frágiles: sin verificación de servidor, sin reintentos, sin cola de fallos, con éxito falso ante errores. Hay además tres lambdas «paralelas» (generación `datalake-dev-` y generación `niif-dev-`) sin saber con certeza cuál está activa.

| Severidad | IDs |
|---|---|
| Crítica | SEG-05, ARQ-15 |
| Alta | SEG-06, SEG-07, ARQ-16, ARQ-17, ARQ-18 |
| Media | SEG-08, SEG-09, ARQ-19, ARQ-20, COS-06 |

---

## 2. Hallazgos críticos

### SEG-05. El transporte a FIS no verifica la identidad del servidor
`EnvioSftpFis\lambda_function.py:72` usa `paramiko.AutoAddPolicy()` (también en el reintento, `:100`): acepta **cualquier clave de host**. Quien se interponga entre la lambda y `seguroalfa-sftp.ctxuse.fiscloudservices.com` puede recibir las credenciales y los archivos de cifras que se envían a los motores actuariales. Se suma:
- Autenticación por **contraseña** (`:79`) y `ENABLE_SSH_RSA_FALLBACK=true` por defecto: baja a `ssh-rsa` (SHA-1), algoritmo débil, para acomodar al servidor.
- El evento completo se escribe en el log, hasta 1 000 caracteres (`:115`).

**Corrección:** cargar la clave de host de FIS (`known_hosts` en Secrets Manager o en el paquete) y usar `RejectPolicy`; autenticación por llave; quitar el *fallback* o acotarlo a FIS con fecha de retiro.

### ARQ-15. El envío por SFTP informa éxito aunque falle, y no garantiza archivo completo
- `EnvioSftpFis` devuelve `{"status":"ok", "failures": n}` aunque `failures > 0` (`:217`) y no relanza. Como la invoca S3 de forma asíncrona, Lambda no la reintenta ni la manda a una cola: **un archivo que no llegó a FIS no deja alerta**. No tiene `DeadLetterConfig` (`dlq: None`).
- Si falla el secreto o la conexión devuelve un diccionario de error (`:126`, `:134`) en lugar de lanzar una excepción: se pierde igual.
- El archivo se escribe directamente en `/In/…` (`sftp.putfo`, `:184`) y la «verificación» es un `stat` que solo se registra (`:193`). FIS puede leer un archivo a medias; no se compara tamaño ni *checksum*. No hay renombrado atómico (subir a `.tmp` y renombrar).
- No hay idempotencia: un reintento de S3 (o dos eventos) reenvía y sobrescribe.

**Corrección:** lanzar excepción ante cualquier fallo, cola de fallos y alarma; subir a nombre temporal y renombrar; comparar tamaño y hash; registrar cada envío (quién, qué, cuándo) en DynamoDB.

---

## 3. Hallazgos altos

### SEG-06. La redirección de enlaces es un servicio sin autenticación con tokens cortos
`datalake-dev-niif-short-link-redirect\lambda_function.py` hace `get_item(token)` y responde `302` a la URL almacenada, sin ningún control. El token es `uuid4()[:8]` (32 bits) en el job de notificación heredado (`notification_certification.py:255`) y **se conserva en el job nuevo** (`LONGITUD_TOKEN_ENLACE=8`, `config\job.dev.json:33`). Los enlaces apuntan a la API de certificación (la decisión exige Cognito, ver más abajo) y al reporte en S3. Un atacante que adivine tokens obtiene las URL con `useCaseId`, `certCode` y rutas de archivo. El riesgo es la **divulgación de rutas**, no la aprobación, porque la aprobación sí se autentica. Sin límite de peticiones (`throttling`) en la API (hipótesis).
**Corrección:** token de 128 bits (`secrets.token_urlsafe(16)`), consulta con `ConsistentRead`, límite de peticiones y registro de accesos. La versión desplegada `niif-dev-short-link-redirect` solo trae librerías en el inventario: recuperar su `lambda_function.py`.

### SEG-07. La aprobación registra y expone datos personales en el log
`niif-dev-certification\certification.py:317-323` escribe el **evento completo** y **todos los *claims*** de Cognito en el log (correo, nombre, grupos, a veces teléfono) y la lista de aprobadores (`:415`). Son datos personales (Ley 1581) en CloudWatch sin retención conocida (PP-10 los había visto en la versión de pruebas; sigue en esta). La autorización por correo (`user_email not in approvers`, `:418`) es correcta, pero depende de que el IdP no permita correos no verificados (hipótesis).
**Positivo:** `ConditionExpression` impide cambiar una decisión ya tomada y hay tabla de auditoría de aprobadores (`:216-312`).

### ARQ-16. El envío es una cadena de disparadores sin entrega garantizada
Los tres disparadores de la generación anterior (`notification-certification`, `copy-s3`, `move-s3`) están **`Disabled`** en la cuenta, y `EnvioSftpFis` y `send-via-sftp` cuelgan de eventos de S3 (de dos buckets distintos: `alfa-datalake-dev-landing-prophet` y `niif-dev-transport`). **No hay en el repositorio un disparador activo que lleve un registro `PENDING` hasta el job de notificación** (FLU-10): el flujo de certificación a transporte no se puede reconstruir con lo que hay. Los que sí existen:
- Capturan toda excepción y la registran sin relanzarla (`datalake-dev-niif-notification-certification\lambda_function.py:66-69`, `copy-s3:116`): el registro del *stream* se considera procesado y **se pierde el evento**.
- Los mapeos de *stream* no tienen reintentos acotados ni destino de fallo (`MaximumRetryAttempts: -1`, `OnFailure: {}`), `BisectBatchOnFunctionError=false`.
- El nombre del job a disparar está quemado: `GLUE_JOB_NAME = "datalake-dev-niif-notification-certification"` (`:9`), distinto del job vigente `niif-dev-notification-certification`.
- Deduplicación por `sendLock` en el evento (`:33`), no por escritura condicional: dos eventos seguidos pueden disparar dos jobs.

### ARQ-17. Idempotencia y orden del configurador
`conf_auxilios.py:545-580` arma el nombre de la ejecución con `fecha_hora + request_id`. Cada invocación crea una ejecución distinta aunque sea la misma malla y el mismo periodo: dos disparos del Scheduler o los dos programadores activos (PP-03) inician el proceso **dos veces**. Además `start_execution` (`:577`) ocurre **antes** de `put_item` a `ejc-ejecuciones` (`:591`): si el registro falla, la máquina corre sin trazabilidad y su `FailState` hace `UpdateItem` sobre una fila que no existe.
**Corrección:** nombre determinista (`malla-periodo-version`) para que Step Functions rechace el duplicado; registrar primero con condición `attribute_not_exists` y luego iniciar.

### ARQ-18. Cinco configuradores son copias y el programador no se tiene
Los 6 configuradores tienen 768-901 líneas casi idénticas. De ellos, solo Auxilios es el código desplegado; para los otros 5 hay una **copia de la versión anterior** y para el manager no hay nada. Endosos y RVI aparecen sin configuración. Es imposible auditar lo que realmente corre (PP-02). Esto se resuelve con la lambda unificada del spike, que no se evalúa aquí.

---

## 4. Hallazgos medios

| ID | Hallazgo | Evidencia |
|---|---|---|
| SEG-08 | **Sin VPC.** El diagrama coloca las lambdas en la subred privada `10.156.220.0/25`. Los 6 configuradores, el evaluador, la certificación y la redirección tienen `VpcConfig` vacío; solo las dos de SFTP están en VPC. Hablan con DynamoDB y S3 por los puntos públicos de AWS. Verificar si existen *endpoints* de VPC y si la política lo permite. | `config\*.json`, hoja `Página-1` del diagrama |
| SEG-09 | **Roles y paquetes.** Cada lambda tiene su rol, bien. No se vieron las políticas. Dos tablas de aprobadores y de consultas siguen siendo del datalake anterior (`datalake-dev-use-case-approvers`, `-queries`, `-emails`: PP-13). La clave KMS solo aparece en 3 de las 11 lambdas de certificación y transporte revisadas. Las dos versiones de la redirección usan runtimes distintos (3.12 y 3.14). | `config\*.json` |
| ARQ-19 | **Lambdas paralelas de dos generaciones.** `datalake-dev-` (Python 3.14, 128 MB, 3 s) y `niif-dev-`. `copy-s3` y `move-s3` corren con **3 segundos y 128 MB** pero procesan lotes de hasta 100 registros de DynamoDB y copian objetos con `copy_object` (límite de 5 GB). El timeout es insuficiente en cuanto el lote tenga más de unos pocos archivos. | `config\datalake-dev-niif-move-s3.json` |
| ARQ-20 | **Estado de la ejecución en tres sitios** (`ejc-ejecuciones`, `pipeline-estado`, `gestor-configuracion` vía el finalizador). El finalizador está atado a ARL por política (`EventBridgeFinalizadorPrimasARL`). Confirma PP-11. | `niif-dev-finalizador-ejecucion.policy.json` |
| COS-06 | **Paquetes de 45 MB** en los configuradores con `numpy` y `pandas` que `conf_auxilios.py` no importa. Arranque en frío más lento y superficie de dependencias que mantener. Sin *tracing* activo (`PassThrough`) en ninguna lambda. | `inventario-archivo\lambdas\codigo\…` |

---

## 5. Qué está bien y se conserva

- **Configurador:** valida el periodo contra `prg-programaciones` y se niega a publicar si difieren (`conf_auxilios.py:845`); escribe el periodo en CNF con **condición optimista** y transacción (`:806-823`); valida que lo generado sea consistente antes de publicar.
- **Evaluador compuesto:** bloqueo con `ConditionExpression`, unicidad de miembros y verificación de que cada uno esté listo antes de lanzar el job.
- **Finalizador:** actualiza ejecución y lote con `ConditionExpression`, valida URI de S3 y estado.
- **Aprobación:** escritura condicional que impide revertir una certificación, tabla de auditoría con quién y cuándo, y autenticación por Cognito.
- **PipelineObserver:** es la única lambda con cola de fallos (`Observer-DLQ`).
- **`move-s3`:** idempotente (`ConditionalCheckFailedException` se trata como ya hecho) y verifica existencia antes de copiar.

---

## 6. Contraste con la evidencia de la cuenta DEV (`errores\aud-datos\`)

Ya se tiene el código **desplegado** del manager, de la redirección y de los 5 configuradores restantes, la lista real de disparadores, la API Gateway y las políticas IAM.

### 6.1 Cambia lo dicho antes

| Tema | Resultado |
|---|---|
| SEG-06 redirección | La versión **desplegada** (`short_link_redirect.py`) **no revisa `expiresAt`**: los enlaces no vencen nunca (la versión del repo sí lo hacía). La tabla `short-links` no tiene TTL y ya tiene 244 enlaces. Cuando falta la URL, la respuesta 400 **devuelve el registro completo** del token (`"item": item`) a quien llame sin autenticación. El valor por defecto de la tabla es `niif-qa-short-links` (otro ambiente). Escribe el evento completo en el log. La ruta `/r` no tiene autenticación (esperable), pero el escenario no tiene límite de peticiones (`throttling` en -1, sin planes de uso); sí tiene WAF asociado, cuyas reglas no se vieron. Sube a **alta**. |
| ARQ-16 disparadores | Los disparadores **activos** sí existen en la cuenta, pero su código no está en el repo: `niif-dev-notification-certification-trigger`, `niif-dev-copy-s3-cert` y `niif-dev-move-s3-certification` (los `datalake-dev-*` están `Disabled`). Todos con reintentos infinitos (`MaximumRetryAttempts=-1`) y sin destino de fallo. Falta recuperar su código y el de `send-via-sftp` (imagen de contenedor). |
| ARQ-17 idempotencia | Se confirma en los 5 configuradores restantes: el nombre de la ejecución lleva hora y `request_id`. Se agrava porque el manager crea el *schedule* con `RetryPolicy` de **2 reintentos y 1 hora** (`scheduler_configurators.py:134`): si el configurador falla después de `start_execution`, el Scheduler lo vuelve a invocar y lanza una **segunda ejecución** de la misma malla y periodo. Además el manager envía `publicar=True` y `ejecutar=True` fijos. |
| ARQ-18 | Ya hay código real de los 6 configuradores y del manager (carpetas `conf_*.py`, `scheduler_configurators.py`): se puede comparar contra la lambda unificada del spike. Falta la de Endosos con su configuración. |
| PP-03 | Solo `manager-configuration` está activo sobre `prg-programaciones`; `programador-configuraciones` está `Disabled`. Existe otro, `programador-cargues`, `Enabled`, sobre `programacion-cargues`. |
| SEG-09 roles | Roles acotados (ver auditoría del pipeline, 9.2). El rol del job de notificación tiene `sns:Publish` y `SetSubscriptionAttributes` sobre `*`. |
| API Gateway | `/certify` y `/not_certify` (POST) exigen Cognito (`CognitoAuthorizerDev`); `/r`, `/redirect` y `/post_login` no. Hay dos autorizadores (`CognitoAuthorizer` y `CognitoAuthorizerDev`): confirmar que ninguno apunte al *user pool* equivocado. |
| Estado | Las tablas tienen cifrado y recuperación a un punto en el tiempo, sin TTL. |

### 6.2 Sigue abierto
- Código de los 3 disparadores activos y de `send-via-sftp` (imagen).
- Reglas de WAF.
- Configuraciones de caso de la certificación, que definen los aprobadores y las consultas.

---

## 7. Orden recomendado

1. **Ya:** `AutoAddPolicy` → `RejectPolicy` y lanzar excepción ante fallo de SFTP (SEG-05, ARQ-15). Es el único punto donde el dato sale hacia un tercero.
2. Recuperar el código desplegado de manager, redirección y `send-via-sftp`, y el disparador activo de la notificación (ARQ-16, ARQ-18).
3. Nombre de ejecución determinista y registro previo (ARQ-17) al construir la lambda unificada.
4. Tokens de 128 bits y límite de peticiones (SEG-06); quitar *claims* del log (SEG-07).
5. Cola de fallos y reintentos acotados en todos los mapeos de *stream* (ARQ-16).
6. VPC, KMS y *tracing* (SEG-08, SEG-09).
