# Roadmap: cadena de notificación y certificación con la nueva arquitectura

<a id="indice"></a>
## Contenido

- [Dónde estamos](#donde-estamos)
- [Lista de avance (orden de ejecución)](#lista-de-avance)
  - [Paso 1 · Cerrar el job de notificación (`niif-dev-notification-certification`)](#paso-1)
  - [Paso 2 · Medir las consultas de Athena](#paso-2)
  - [Paso 3 · ID único de punta a punta (`niif-dev-certification` job de Glue, Step Functions)](#paso-3)
  - [Paso 4 · Orquestadora (`niif-dev-notification-certification-orchestrator`)](#paso-4)
  - [Paso 5 · Transporte a FIS (`datalake-dev-niif-send-via-sftp`)](#paso-5)
  - [Paso 6 · Resto de lambdas de la cadena](#paso-6)
    - [Paso 6.1 · Evaluador compuesto (`niif-dev-composite-notification-evaluator`)](#paso-6-1)
    - [Paso 6.2 · Certificar/rechazar (`niif-dev-certification` lambda)](#paso-6-2)
    - [Paso 6.3 · Enlace corto (`niif-dev-short-link-redirect`)](#paso-6-3)
    - [Paso 6.4 · Post-login (`niif-dev-post-login`)](#paso-6-4)
  - [Paso 7 · Mejoras de Athena](#paso-7)
  - [Paso 8 · Observabilidad y alertas](#paso-8)
  - [Paso 9 · Limpieza y cierre](#paso-9)
  - [En paralelo](#en-paralelo)
  - [Decisiones pendientes (lista)](#decisiones-pendientes-lista)
- [Fases (detalle, brechas, responsables y estimación)](#fases)
- [Decisiones que bloquean (D1 a D7)](#decisiones)
- [Orden y riesgos](#orden-y-riesgos)


Fecha: 2026-11-10. **Propuesta**: las estimaciones son en días de una persona, no tienen aprobación y no cuentan las esperas de DevOps ni de negocio.

**Alcance (2026-11-11): se entrega el flujo completo de la hoja `Página-2`**, desde el Modelo Notificaciones hasta el envío a FIS: **6 lambdas y 1 job de Glue** (incluida la Lambda SFTP). Son las 5 partes acordadas:
1. lambdas de la cadena;
2. plantillas HTML;
3. job de notificación por SES;
4. enlaces de aprobar y rechazar;
5. observabilidad completa en logs, con un ID único de punta a punta.

**Fuera:** el resto del pipeline del paso 2 (extracción, Silver, SCD). **Dentro desde el 2026-11-10:** la optimización de las consultas de Athena de la cadena y del diseño de Gold que ellas leen (fase 7: COS-07 a COS-09, ARQ-34, ARQ-36 y ARQ-37). El particionado de Gold se coordina con el dueño del paso 2.

**Reglas que aplican a todo:**
- Código en `Backend\notificacion\{lambdas,jobs-glue}\<recurso>\`, hexagonal, en inglés y con PEP 8; logs en español.
- Cada paquete lleva su copia de `niif_observability`.
- Entrega por carpeta o zip con el nombre del recurso. DevOps hace el commit, la infraestructura (Terraform) y el despliegue.
- Pruebas: 70 % o más en el pipeline y 80 % o más como meta.

Brechas citadas: `Docs\06-brechas\notificaciones-vs-arquitectura.md`, `limpieza-dev-recursos.md`, `consultas-athena-notificacion.md` y `auditoria-job-notificacion-nuevo-vs-actual.md`.

<a id="donde-estamos"></a>
## Dónde estamos (2026-11-10)
| Pieza | Estado |
|---|---|
| Plantillas HTML (T1b) | Hechas. Están en QA y en `Backend\notificacion\email-template\` |
| Job de notificación actual (monolito con SES) | En QA: el correo llega a la bandeja. En DEV, igual |
| Job de notificación nuevo (hexagonal) | Hace lo mismo que el actual, más la trazabilidad. 514 pruebas. **Probado en DEV** con un job aparte (`niif-dev-notification-certification-prueba-v2`): `SUCCEEDED`, log JSON completo, sin envío |
| Librería `niif_observability` | Hecha (log con trazabilidad y EMF para lambdas) |
| 7 lambdas de la cadena | Código actual bajado y validado contra AWS (`Backend\Lamnbdas\`). Sin reescribir |
| ID de punta a punta (ARQ-32) | El job nuevo lo recibe y no lo inventa. **Todavía no llega:** falta del lado de certificación y del trigger |
| Transporte a FIS | **Nunca funcionó de punta a punta en DEV** (ARQ-30) |

<a id="lista-de-avance"></a>
## Lista de avance
✅ = hecho y comprobado · ⬜ = pendiente. Actualizada el 2026-11-11.

**Ya hecho (antes y durante el refinamiento)**
- ✅ Correo por SES (T1a) sobre el job desplegado: en QA llega a la bandeja.
- ✅ Plantillas HTML (T1b): 6 plantillas en QA y en `Backend\notificacion\email-template\`.
- ✅ Código actual bajado de AWS y validado contra nuestras copias (`EvidenciasAws\dev-codigo-actual\`).
- ✅ Evidencia de uso de DEV y clasificación de recursos (`Docs\06-brechas\limpieza-dev-recursos.md`).
- ✅ Auditoría del job nuevo frente al desplegado (`Docs\06-brechas\auditoria-job-notificacion-nuevo-vs-actual.md`).
- ✅ Estructura de `Backend\notificacion\`: `email-template\`, `lambdas\` y `jobs-glue\`.
- ✅ Esqueletos de las 5 lambdas en `lambdas\` (sin lógica).
- ✅ Librería común `niif_observability` (log JSON con trazabilidad y EMF para lambdas), copiada en cada paquete y comparada con `verificar_copias_comunes.py`.
- ✅ Diagrama de propuesta: hoja `Página-2` de `NIIF17_ModeloSolucion_V1.0-propuesta.drawio`, con las lambdas que faltaban (72-78) y la nota de la propuesta.
- ✅ Decisiones D1 (una sola orquestadora), D5 (alertas parametrizadas) y D6 (nombres en el estándar).

**Orden de ejecución:** se trabaja de arriba hacia abajo, paso por paso. El número de fase agrupa por componente; el número de paso es el orden.

<a id="paso-1"></a>
### Paso 1 · Fase 1: cerrar el job de notificación (`niif-dev-notification-certification`, job de Glue)
_Lo más avanzado y lo primero que llega al usuario._
- ✅ HTML con las plantillas (N-01).
- ✅ Envío parcial: si falla un destinatario, sigue con los demás (N-02).
- ✅ Argumento `--SES_SENDER` (N-03).
- ✅ Consultas estrictas: si falta una variable, falla (N-05).
- ✅ Token de 32 caracteres en la configuración (N-07).
- ✅ Trazabilidad: ID del flujo sin inventar, contexto en cada línea, mensajes en español, errores de arranque en JSON, eventos nuevos y resumen para métricas (O-01 a O-06).
- ✅ `NTF_FIN_JOB` también cuando no envía.
- ✅ 1.5 Cancelar la consulta de Athena en un timeout y marcar el error como recuperable.
- ✅ Prueba en Glue de DEV sin envío (`-prueba-v2`): `SUCCEEDED` con el log completo.
- ⬜ 1.1 Prueba en DEV con envío real y HTML (alinear `email-templates/*` y `email-template/`).
- ⬜ Volver a empaquetar y subir a DEV (el zip de DEV no trae `NTF_FIN_JOB` ni 1.5).
- ⬜ 1.2 Decidir el reenvío sobre `SENT` (D4).
- ⬜ 1.3 Llevar el job al estándar: inglés y PEP 8, `test\`, punto de entrada en `src\`, `tox.ini`, `config\` de qa y prd (N-08).
- ⬜ 1.4 Paquete para DevOps: argumentos, permisos (incluido `athena:StopQueryExecution`) y retirar SNS.
- ⬜ Prueba en QA.

<a id="paso-2"></a>
### Paso 2 · Fase 7: Athena y Gold (consultas de los JSON de caso, workgroup y tablas `scd_*` de Gold): medir
_Corto. Define el tiempo máximo del job y si hay que adelantar el particionado (7.6)._
- ✅ 7.1 Medición en DEV (2026-11-11): las consultas tardan 0,8-2,3 s y hasta 10 s las `detail`; el costo es casi cero; COS-07 y COS-08 confirmados; ninguna falla por timeout. Resultado en `Docs\06-brechas\consultas-athena-notificacion.md`.
- ⬜ 7.1b Medir en PRD (lo corre el equipo de PRD, solo lectura): `AMB=prd bash medir-athena-dev.sh`. Antes, ajustar el script (espera orden): rechazar `--ejecutar` en PRD y no guardar en el zip el historial crudo de otros equipos.
- ✅ Nota (no es brecha): en DEV, el Gold de 5 mallas (rentas, endosos, auxilios, ARP y RVI) se vació el 2026-11-05 por pruebas. Para probar con datos, usar Primas ARL o volver a correr la malla que haga falta.

<a id="paso-3"></a>
### Paso 3 · Fase 2: ID único de punta a punta (`niif-dev-certification` job de Glue, Step Functions `niif-dev-<malla>`)
_La orquestadora y las alertas usan este ID._
- ✅ Etapa 1 (verificación, solo lectura, 2026-11-11): ninguna Step Function le pasa el ID al job de certificación; el ID debe ir en el `put_item` (`PROCESSED`); los argumentos por defecto del job están quemados (`PERIOD=202411`, `CONFIG_KEY` de Auxilios).
- ✅ Decisiones E1, E2 y E3 (versión base, formato del ID con UUID v7 y caso compuesto).
- ⬜ Etapa 2: cambio en el job, en local y con pruebas (espera orden explícita).
- ⬜ Etapa 3: prueba en DEV con un job de certificación aparte (confirmación antes de lanzar).
- ⬜ Etapa 4: Step Function, con DevOps o en una copia de la de ARL (confirmación manual).
- ⬜ Configurador (T2): generar el nombre de la ejecución `<malla>-<periodo>-<uuid v7>`.
- ⬜ 2.1 El job de certificación recibe `--EJECUCION_ID` y guarda `ejecucionId` (y `ejecucionesOrigen` en el compuesto) junto al `certCode`.
- ⬜ 2.2 Las 6 Step Functions pasan el argumento (DevOps, Terraform).
- ⬜ 2.3 Quien lanza el job de notificación pasa `--EJECUCION_ID` (lo hará la orquestadora, fase 3.1).
- ⬜ 2.4 Prueba automática sobre las definiciones de las Step Functions.
- ⬜ 2.5 El job de certificación calcula el resumen y los totales y los guarda junto al CSV; el job de notificación deja de consultar Athena.

<a id="paso-4"></a>
### Paso 4 · Fase 3.1: orquestadora (`niif-dev-notification-certification-orchestrator`, lambda; reemplaza a trigger, copy y move)
_Reemplaza a copy y move, que hoy pierden datos sin avisar, y lanza el job con el ID._
- ✅ Esqueleto creado.
- ⬜ Lógica por estado: `PENDING` lanza el job, `CERTIFICATE` copia a transporte, `NO_CERTIFICATE` archiva y deja pendiente la malla (esta última espera D2).
- ⬜ Reprocesador de registros `PENDING` atascados.
- ⬜ Pruebas, `package.sh` y configuración por ambiente.
- ⬜ Cambio en AWS (DevOps): desplegarla con el mapping apagado, apagar los 3 mappings viejos y activar el nuevo.

<a id="paso-5"></a>
### Paso 5 · Fase 4: transporte a FIS (`datalake-dev-niif-send-via-sftp`, lambda de contenedor)
_Necesita la orquestadora (la copia a `export/`) para probar Certificar → FIS._
- ⬜ 4.0 Restaurar la lambda, que está inactiva en DEV.
- ⬜ 4.1 Sacar el código de la imagen de ECR y reescribirla (falta decidir su nombre; esqueleto pendiente).
- ⬜ 4.2 Prueba Certificar → `export/` → SFTP → FIS.
- ⬜ 4.3 Averiguar por qué se borraron los archivos de `export/`.

<a id="paso-6"></a>
### Paso 6 · Resto de lambdas de la cadena (fases 3.3 a 3.6)
_3.4 y 3.5 dependen de D2, D3 y D7: deben estar decididas para este momento._

<a id="paso-6-1"></a>
### Paso 6.1 · Fase 3.3: evaluador compuesto (`niif-dev-composite-notification-evaluator`, lambda)
- ✅ Esqueleto creado.
- ⬜ Reescritura con el estándar; corregir `--ENVIRONMENT` con `'{env}'` literal (ARQ-27) y propagar el `ejecucion_id`.
- ⬜ Pruebas, `package.sh` y configuración por ambiente.

<a id="paso-6-2"></a>
### Paso 6.2 · Fase 3.4: certificar/rechazar (`niif-dev-certification`, lambda)
- ✅ Esqueleto creado.
- ⬜ Reescritura con log del resultado (ARQ-33), sin claims ni correos en el log (SEG-12) y sin numpy ni pandas.
- ⬜ Pruebas, `package.sh` y configuración por ambiente.
- ⬜ Decidir D7 (si el API va aquí o a la orquestadora).

<a id="paso-6-3"></a>
### Paso 6.3 · Fase 3.5: enlace corto (`niif-dev-short-link-redirect`, lambda)
- ✅ Esqueleto creado.
- ⬜ Reescritura que acepte tokens de 32 caracteres (SEG-13).
- ⬜ Descarga del CSV por el API con URL prefirmada (ARQ-29, espera D3).
- ⬜ Pruebas, `package.sh` y configuración por ambiente.

<a id="paso-6-4"></a>
### Paso 6.4 · Fase 3.6: post-login (`niif-dev-post-login`, lambda; antes `niif-post-login`)
- ✅ Esqueleto creado.
- ⬜ Reescritura con PKCE (SEG-10) y `state` firmado (SEG-11), con DevOps por Cognito.
- ⬜ Pruebas, `package.sh` y configuración por ambiente.
- ⬜ Cambio en AWS (DevOps): crear `niif-dev-post-login`, apuntarle `GET /post_login` y retirar `niif-post-login`.

<a id="paso-7"></a>
### Paso 7 · Fase 7: Athena y Gold (consultas de los JSON de caso, workgroup y tablas `scd_*` de Gold): mejoras
_Con los datos de la medición del paso 2._
- ⬜ 7.2 Workgroup con tope de escaneo.
- ⬜ 7.3 Filtros sin `CAST`.
- ⬜ 7.4 Regla de redondeo y prueba de cuadre.
- ⬜ 7.5 Conteo de Rentas y Endosos, y montos de Reserva.

<a id="paso-8"></a>
### Paso 8 · Fase 5: observabilidad y alertas (todos los componentes; CloudWatch y SES)
_Los logs ya se hacen dentro de cada componente; aquí se arman las alarmas, las métricas y la cola de fallos._
- ⬜ 5.1 Filtros de métricas sobre `NTF_RESUMEN_JOB` (job) y EMF en las lambdas.
- ⬜ 5.2 Alarmas por SES al grupo de soporte (grupo pendiente, D5).
- ⬜ 5.3 Alarma de registros atascados.
- ⬜ 5.4 Cola de fallos y `ReportBatchItemFailures` en el stream de la orquestadora.
- ⬜ 5.5 Consultas de Logs Insights por `ejecucion_id` y `cert_code`.

<a id="paso-9"></a>
### Paso 9 · Fase 6: limpieza y cierre (recursos viejos de DEV y diagrama)
_Al final, cuando lo nuevo ya reemplazó a lo viejo._
- ⬜ 6.1 Retirar el API viejo `Certificación API` (`postLogin`, `datalake-dev-niif-short-link-redirect`, `datalake-dev-niif-certification`) y su callback en Cognito.
- ⬜ 6.2 Retirar G1 (`datalake-dev-niif-*`), G2 (cargues, iniciador, finalizador y su regla) y G3 (`niif-dev-configurador-*`, `programador-configuraciones`).
- ⬜ 6.3 Borrar los jobs `niif-dev-notification-certification-ses-test` y `-prueba-v2`, las políticas temporales del rol y las copias duplicadas de JSON.
- ⬜ 6.4 Pasar la propuesta del diagrama (`NIIF17_ModeloSolucion_V1.0-propuesta.drawio`, `Página-2`) al original con el arquitecto.

<a id="en-paralelo"></a>
### En paralelo (cuando se pueda, sin bloquear los pasos)
- ⬜ 7.6 Gold particionado (con el dueño del paso 2).
- Las decisiones pendientes de la lista siguiente.

<a id="decisiones-pendientes-lista"></a>
### Decisiones pendientes
- ⬜ D2 No certificar en la programación.
- ⬜ D3 Descarga del CSV.
- ⬜ D4 Reenvío sobre `SENT`.
- ⬜ D7 ¿El API va a la orquestadora o a una lambda de certificación separada?
- ⬜ Nombre de la Lambda SFTP.
- ⬜ Consulta `summary` de ARL (negocio).

<a id="fases"></a>
## Fases
| Fase | Qué | Brechas | Quién | Estimación | Depende de |
|---|---|---|---|---|---|
| **1. Cerrar el job de notificación** | 1.1 Prueba en DEV con envío real: un `certCode` en `PENDING`, tu correo como destino y revisar el HTML (`NTF_FORMATO_CORREO=HTML`). En DEV el permiso de plantillas es `email-templates/*` (plural): alinear el JSON o el permiso.<br>1.2 Decidir el reenvío sobre `SENT` (N-04).<br>1.3 Llevarlo al estándar: inglés y PEP 8, `test\`, punto de entrada en `src\`, `tox.ini`, `config\` de qa y prd (N-08).<br>1.4 Paquete para DevOps: zip, argumentos por defecto, `s3:GetObject` sobre el script, el zip y `email-template/*`, y retirar los permisos de SNS.<br>1.5 **Resiliencia ante timeout de Athena (B.1): hecho (2026-11-10).** La consulta se cancela (`StopQueryExecution`) cuando se agota el tiempo, y `NTF-PLA-005` queda como recuperable. Requiere el permiso `athena:StopQueryExecution` en el rol. | N-04, N-06, N-08, ARQ-22 | Nosotros; DevOps en 1.4 | 4-5 | Decisión N-04 (equipo) |
| **2. ID único de punta a punta** | 2.1 Que el job de certificación reciba `--EJECUCION_ID` (`$$.Execution.Name`) y guarde `ejecucionId` junto al `certCode`.<br>2.2 Las 6 Step Functions pasan ese argumento (DevOps, Terraform).<br>2.3 Que el trigger lo pase al job.<br>2.4 Una prueba automática que revise las definiciones de las Step Functions.<br>2.5 **Resumen y totales calculados en la certificación (opción A):** el job de certificación calcula `summary` y `total` sobre los mismos datos del CSV y los guarda junto al CSV (`resumen-<certCode>.json`, con `ejecucionId` y `certCode`). La notificación lee ese archivo y **ya no consulta Athena**: desaparece el riesgo de timeout en la notificación, los números del correo cuadran con el CSV (ARQ-34) y Gold se lee una sola vez. | ARQ-32, ARQ-34 | Nosotros y DevOps | 6-8 | Fase 1; es la base de la observabilidad (fase 5) |
| **3. Reescribir las lambdas de la cadena** (cada una con su copia de `niif_observability`, sus pruebas y su zip) | 3.1 **Lambda orquestadora** (D1 decidida: una sola), `niif-<amb>-notification-certification-orchestrator` (recurso nuevo); reemplaza a trigger, copy y move, que se retiran y decide por el estado del registro: `PENDING` arranca el job de notificación; `CERTIFICATE` copia a `export/`; `NO_CERTIFICATE` archiva y deja la malla pendiente (ARQ-23). Con `ReportBatchItemFailures` y cola de fallos, cada acción idempotente, el mapa de carpetas en un solo lugar y sin errores tragados. Cubre ARQ-24, ARQ-25 (`sendLock`), ARQ-26 y SEG-12. **Incluye un reprocesador (B.3):** cada cierto tiempo busca registros en `PENDING` con más de N minutos y vuelve a lanzar el job (que es idempotente), hasta un máximo de intentos; si se agotan, alarma a soporte (D5). El cambio: desplegarla con su mapping deshabilitado, apagar los 3 mappings viejos y activar el nuevo.<br>3.2 *(incluido en 3.1)*.<br>3.3 **evaluador compuesto**: el texto `'{env}'` literal.<br>3.4 **certificar/rechazar**: log del resultado, sin claims ni correos en el log, quitar numpy y pandas; No certificar deja la malla pendiente (ARQ-23).<br>3.5 **enlace corto**: token de 32 y descarga del CSV mediada por el API con una URL prefirmada (ARQ-29).<br>3.6 **post-login**: pasa al estándar como `niif-<amb>-post-login` (D6). Se agregan PKCE y `state` firmado, con DevOps por Cognito. El callback de Cognito apunta a la URL del API, no al nombre de la lambda: cambiar el nombre solo toca la integración del API. | ARQ-23 a ARQ-27, ARQ-29, ARQ-33, SEG-10 a SEG-13 | Nosotros; DevOps en 3.1 (cambio de mappings), 3.5 y 3.6 | 10-13 | D2 y D3 |
| **4. Transporte a FIS de punta a punta** (la Lambda SFTP entra al alcance) | 4.0 **Hallazgo (2026-11-11): `datalake-dev-niif-send-via-sftp` está INACTIVA en AWS.** Lambda la desactivó por falta de uso; hay que restaurarla antes de cualquier prueba, y es una causa posible de que no tenga logs (ARQ-30). Es una imagen de contenedor (`datalake_dev_niif_send_via_sftp_lambda:20261008-2321`, x86_64) con disparador de S3.<br>4.1 Sacar su código de la imagen de ECR y reescribirla con el estándar (carpeta en `lambdas\`, con su copia de `niif_observability`).<br>4.2 Probar en DEV Certificar → copy → `export/` → SFTP.<br>4.3 Averiguar por qué se borraron los archivos de `export/`. | ARQ-30 | Nosotros y DevOps | 2-3 | Fase 3.1 |
| **5. Observabilidad y alertas** | 5.1 Filtros de métricas sobre `NTF_RESUMEN_JOB` y EMF en las lambdas.<br>5.2 Alarmas de CloudWatch → SES → grupo de soporte.<br>5.3 Alarma de registros atascados (por ejemplo, `CERTIFICATE` sin `TRANSPORT`).<br>5.4 Cola de fallos y `ReportBatchItemFailures` en los streams.<br>5.5 Consultas de Logs Insights por `ejecucion_id` y por `cert_code`. | ARQ-26, ARQ-31 | Nosotros (código y definición); DevOps (alarmas, filtros, colas) | 3-4 | Fases 2 y 3; grupo de soporte (D5) |
| **6. Limpieza de DEV y cierre** | 6.1 Retirar el API viejo (antes, quitar su callback en Cognito).<br>6.2 Quitar la regla del finalizador G2 y retirar G1 y G3, con su dueño confirmado.<br>6.3 Borrar los jobs `-ses-test` y `-prueba-v2`, las políticas temporales del rol (`TemporarySesTestAccess`, `pruebas/*`) y las copias duplicadas de los JSON de caso.<br>6.4 Actualizar la hoja `Página-2` del diagrama y llevarle al arquitecto la sugerencia de observabilidad (ARQ-31). | SEG-14, ARQ-21, ARQ-31 | DevOps (recursos de Terraform); nosotros (lista y evidencia) | 2-3 | Fases 1 a 5 desplegadas |
| **7. Optimización de Athena y de Gold** (empieza midiendo; solo se cambia lo que la medición justifique) | 7.1 **Medir:** tiempo y bytes escaneados de cada consulta de la cadena (comando en `Docs\06-brechas\consultas-athena-notificacion.md`; desde la fase 1 lo registra `NTF_CONSULTA_EJECUTADA`).<br>7.2 **Workgroup propio** de NIIF con tope de escaneo y configuración forzada; su nombre va en la configuración global (COS-08).<br>7.3 **Filtros sin `CAST`** sobre la columna de periodo (COS-09) y columnas explícitas, en los JSON de caso.<br>7.4 **Una sola regla de redondeo** y prueba de cuadre `suma(detalle) == total` (ARQ-34). Si la fase 2.5 ya está hecha, queda cubierto por construcción.<br>7.5 Revisar el conteo de Rentas y Endosos (ARQ-37) y pedir a negocio los montos de Reserva Matemática (ARQ-36).<br>7.6 **Gold particionado por periodo** (COS-07), o una tabla de vigentes por periodo. Es la causa de fondo del tiempo y del costo; se coordina con el dueño del paso 2 y con DevOps (cambio de tabla y crawler). | COS-07, COS-08, COS-09, ARQ-34, ARQ-36, ARQ-37 | Nosotros (7.1 a 7.5); dueño del paso 2 y DevOps (7.6) | 4-6 (sin contar 7.6) | 7.1 primero; 7.4 se simplifica con la fase 2.5 |
| **Configuración del correo** (en paralelo) | Consulta `summary` de ARL (ARQ-35): negocio define qué debe mostrar | ARQ-35 | Negocio y nosotros | 1 | Definición de negocio |

**Total estimado:** 35 a 47 días de trabajo propio (fases 1 a 7), más las esperas de DevOps y de negocio. No incluye el particionado de Gold (7.6).

<a id="decisiones"></a>
## Decisiones que bloquean
| # | Decisión | Quién | Bloquea |
|---|---|---|---|
| D1 | **Decidida (2026-11-10): una sola lambda orquestadora** reemplaza a trigger, copy y move. El riesgo de que un registro frene el lote se cubre con `ReportBatchItemFailures`, cola de fallos e idempotencia | Usuario | — |
| D2 | No certificar: ¿qué se escribe en `prg-programaciones` (`PENDIENTE` o `RECHAZADO`) y quién limpia `PROGRAMADO`? (ARQ-23) | Víctor / negocio | 3.4 |
| D3 | Descarga del CSV: ¿por el API con URL prefirmada, o consola solo para quien tenga acceso? ¿Los aprobadores tienen acceso a la consola? (ARQ-29) | Equipo / negocio | 3.5 |
| D4 | ¿Se permite reenviar un `certCode` que ya está en `SENT`? (N-04) | Equipo | 1.2 |
| D5 | ¿Cuál es el grupo de soporte que recibe las alertas por SES? **Mientras tanto, se deja parametrizado** (2026-11-10): la lista de destinatarios de alertas llega por configuración (variable o argumento por ambiente), nunca en el código. Se diseña y prueba con un destinatario de prueba | Negocio / soporte | Solo la puesta en marcha de 5.2 |
| D6 | **Decidida (2026-11-10):** nombres en el estándar `niif-<amb>-*`. Post-login pasa a `niif-<amb>-post-login` y la orquestadora es `niif-<amb>-notification-certification-orchestrator`. Las demás conservan su nombre actual, que ya cumple | Usuario | — |

D2, D3 y D4 están preguntadas al equipo y a la espera de respuesta (2026-11-10).

**Decisiones del paso 3 (ID único), 2026-11-11:**
- **E1.** El job de certificación se trabaja sobre la versión con el arreglo del incidente: lo desplegado más INC-3 (`contar_filas_datos`, commit `0020179`), que es la parte del incidente que le toca a este job. INC-1, INC-2 e INC-5 van en extracción y SCD (`Tareas\paquete-release-inprod\`, que solo trae esos dos scripts). **En DEV ya están desplegados:** el script de extracción y el de SCD que corren en DEV son idénticos a los del paquete (comprobado el 2026-11-11).
- **E2.** El ID es el nombre de la ejecución de la Step Function. **El configurador lo genera una sola vez** con el formato `<malla>-<periodo>-<uuid v7>` (por ejemplo `arl-202606-0193f2a4-…`) y de ahí en adelante se propaga tal cual con `$$.Execution.Name`. Ningún componente le agrega nada después.
- **E3.** En el caso compuesto se guardan los dos: `ejecucionId`, con la ejecución que completó el grupo, y `ejecucionesOrigen`, con la lista de todas las del grupo.

<a id="orden-y-riesgos"></a>
## Orden y riesgos
- **Orden recomendado:** el de la lista de avance, Paso 1 a Paso 9 (en fases: 1 → 7.1 → 2 → 3.1 → 4 → 3.3 a 3.6 → 7.2 a 7.5 → 5 → 6). La medición (7.1) va temprano porque decide cuánto tiempo máximo dar a las consultas y si hace falta adelantar el particionado (7.6). La configuración del correo va en paralelo. La orquestadora va antes que lo demás porque reemplaza a copy y move, que son los únicos que hoy pierden datos sin avisar (ARQ-26, ARQ-30).
- **Probar en QA mueve archivos reales y dispara el SFTP a FIS.** Las pruebas de Certificar se coordinan con negocio.
- **Desarrollo solo tiene acceso a DEV.** Las fases que tocan Terraform, Cognito o QA dependen del calendario de DevOps.
