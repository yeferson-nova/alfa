# Roadmap: cadena de notificación y certificación con la nueva arquitectura

Fecha: 2026-11-10. **Propuesta**: las estimaciones son en días de una persona, no tienen aprobación y no cuentan las esperas de DevOps ni de negocio.

**Alcance:** las 5 partes acordadas:
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

## Fases
| Fase | Qué | Brechas | Quién | Estimación | Depende de |
|---|---|---|---|---|---|
| **1. Cerrar el job de notificación** | 1.1 Prueba en DEV con envío real: un `certCode` en `PENDING`, tu correo como destino y revisar el HTML (`NTF_FORMATO_CORREO=HTML`). En DEV el permiso de plantillas es `email-templates/*` (plural): alinear el JSON o el permiso.<br>1.2 Decidir el reenvío sobre `SENT` (N-04).<br>1.3 Llevarlo al estándar: inglés y PEP 8, `test\`, punto de entrada en `src\`, `tox.ini`, `config\` de qa y prd (N-08).<br>1.4 Paquete para DevOps: zip, argumentos por defecto, `s3:GetObject` sobre el script, el zip y `email-template/*`, y retirar los permisos de SNS.<br>1.5 **Resiliencia ante timeout de Athena (B.1): hecho (2026-11-10).** La consulta se cancela (`StopQueryExecution`) cuando se agota el tiempo, y `NTF-PLA-005` queda como recuperable. Requiere el permiso `athena:StopQueryExecution` en el rol. | N-04, N-06, N-08, ARQ-22 | Nosotros; DevOps en 1.4 | 4-5 | Decisión N-04 (equipo) |
| **2. ID único de punta a punta** | 2.1 Que el job de certificación reciba `--EJECUCION_ID` (`$$.Execution.Name`) y guarde `ejecucionId` junto al `certCode`.<br>2.2 Las 6 Step Functions pasan ese argumento (DevOps, Terraform).<br>2.3 Que el trigger lo pase al job.<br>2.4 Una prueba automática que revise las definiciones de las Step Functions.<br>2.5 **Resumen y totales calculados en la certificación (opción A):** el job de certificación calcula `summary` y `total` sobre los mismos datos del CSV y los guarda junto al CSV (`resumen-<certCode>.json`, con `ejecucionId` y `certCode`). La notificación lee ese archivo y **ya no consulta Athena**: desaparece el riesgo de timeout en la notificación, los números del correo cuadran con el CSV (ARQ-34) y Gold se lee una sola vez. | ARQ-32, ARQ-34 | Nosotros y DevOps | 6-8 | Fase 1; es la base de la observabilidad (fase 5) |
| **3. Reescribir las lambdas de la cadena** (cada una con su copia de `niif_observability`, sus pruebas y su zip) | 3.1 **Lambda orquestadora** (D1 decidida: una sola), `niif-<amb>-notification-certification-orchestrator` (recurso nuevo); reemplaza a trigger, copy y move, que se retiran y decide por el estado del registro: `PENDING` arranca el job de notificación; `CERTIFICATE` copia a `export/`; `NO_CERTIFICATE` archiva y deja la malla pendiente (ARQ-23). Con `ReportBatchItemFailures` y cola de fallos, cada acción idempotente, el mapa de carpetas en un solo lugar y sin errores tragados. Cubre ARQ-24, ARQ-25 (`sendLock`), ARQ-26 y SEG-12. **Incluye un reprocesador (B.3):** cada cierto tiempo busca registros en `PENDING` con más de N minutos y vuelve a lanzar el job (que es idempotente), hasta un máximo de intentos; si se agotan, alarma a soporte (D5). El cambio: desplegarla con su mapping deshabilitado, apagar los 3 mappings viejos y activar el nuevo.<br>3.2 *(incluido en 3.1)*.<br>3.3 **evaluador compuesto**: el texto `'{env}'` literal.<br>3.4 **certificar/rechazar**: log del resultado, sin claims ni correos en el log, quitar numpy y pandas; No certificar deja la malla pendiente (ARQ-23).<br>3.5 **enlace corto**: token de 32 y descarga del CSV mediada por el API con una URL prefirmada (ARQ-29).<br>3.6 **post-login**: pasa al estándar como `niif-<amb>-post-login` (D6). Se agregan PKCE y `state` firmado, con DevOps por Cognito. El callback de Cognito apunta a la URL del API, no al nombre de la lambda: cambiar el nombre solo toca la integración del API. | ARQ-23 a ARQ-27, ARQ-29, ARQ-33, SEG-10 a SEG-13 | Nosotros; DevOps en 3.1 (cambio de mappings), 3.5 y 3.6 | 10-13 | D2 y D3 |
| **4. Transporte a FIS de punta a punta** | 4.1 Conseguir el código de `datalake-dev-niif-send-via-sftp` (imagen de ECR).<br>4.2 Probar en DEV Certificar → copy → `export/` → SFTP.<br>4.3 Averiguar por qué se borraron los archivos de `export/`. | ARQ-30 | Nosotros y DevOps | 2-3 | Fase 3.1 |
| **5. Observabilidad y alertas** | 5.1 Filtros de métricas sobre `NTF_RESUMEN_JOB` y EMF en las lambdas.<br>5.2 Alarmas de CloudWatch → SES → grupo de soporte.<br>5.3 Alarma de registros atascados (por ejemplo, `CERTIFICATE` sin `TRANSPORT`).<br>5.4 Cola de fallos y `ReportBatchItemFailures` en los streams.<br>5.5 Consultas de Logs Insights por `ejecucion_id` y por `cert_code`. | ARQ-26, ARQ-31 | Nosotros (código y definición); DevOps (alarmas, filtros, colas) | 3-4 | Fases 2 y 3; grupo de soporte (D5) |
| **6. Limpieza de DEV y cierre** | 6.1 Retirar el API viejo (antes, quitar su callback en Cognito).<br>6.2 Quitar la regla del finalizador G2 y retirar G1 y G3, con su dueño confirmado.<br>6.3 Borrar los jobs `-ses-test` y `-prueba-v2`, las políticas temporales del rol (`TemporarySesTestAccess`, `pruebas/*`) y las copias duplicadas de los JSON de caso.<br>6.4 Actualizar la hoja `Página-2` del diagrama y llevarle al arquitecto la sugerencia de observabilidad (ARQ-31). | SEG-14, ARQ-21, ARQ-31 | DevOps (recursos de Terraform); nosotros (lista y evidencia) | 2-3 | Fases 1 a 5 desplegadas |
| **7. Optimización de Athena y de Gold** (empieza midiendo; solo se cambia lo que la medición justifique) | 7.1 **Medir:** tiempo y bytes escaneados de cada consulta de la cadena (comando en `Docs\06-brechas\consultas-athena-notificacion.md`; desde la fase 1 lo registra `NTF_CONSULTA_EJECUTADA`).<br>7.2 **Workgroup propio** de NIIF con tope de escaneo y configuración forzada; su nombre va en la configuración global (COS-08).<br>7.3 **Filtros sin `CAST`** sobre la columna de periodo (COS-09) y columnas explícitas, en los JSON de caso.<br>7.4 **Una sola regla de redondeo** y prueba de cuadre `suma(detalle) == total` (ARQ-34). Si la fase 2.5 ya está hecha, queda cubierto por construcción.<br>7.5 Revisar el conteo de Rentas y Endosos (ARQ-37) y pedir a negocio los montos de Reserva Matemática (ARQ-36).<br>7.6 **Gold particionado por periodo** (COS-07), o una tabla de vigentes por periodo. Es la causa de fondo del tiempo y del costo; se coordina con el dueño del paso 2 y con DevOps (cambio de tabla y crawler). | COS-07, COS-08, COS-09, ARQ-34, ARQ-36, ARQ-37 | Nosotros (7.1 a 7.5); dueño del paso 2 y DevOps (7.6) | 4-6 (sin contar 7.6) | 7.1 primero; 7.4 se simplifica con la fase 2.5 |
| **Configuración del correo** (en paralelo) | Consulta `summary` de ARL (ARQ-35): negocio define qué debe mostrar | ARQ-35 | Negocio y nosotros | 1 | Definición de negocio |

**Total estimado:** 35 a 47 días de trabajo propio (fases 1 a 7), más las esperas de DevOps y de negocio. No incluye el particionado de Gold (7.6).

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

## Orden y riesgos
- **Orden recomendado:** 1 → 7.1 (medir) → 2 → 3.1 (orquestadora) → 4 → 3.3 a 3.6 → 7.2 a 7.5 → 5 → 6. La medición (7.1) va temprano porque decide cuánto tiempo máximo dar a las consultas y si hace falta adelantar el particionado (7.6). La configuración del correo va en paralelo. La orquestadora va antes que lo demás porque reemplaza a copy y move, que son los únicos que hoy pierden datos sin avisar (ARQ-26, ARQ-30).
- **Probar en QA mueve archivos reales y dispara el SFTP a FIS.** Las pruebas de Certificar se coordinan con negocio.
- **Desarrollo solo tiene acceso a DEV.** Las fases que tocan Terraform, Cognito o QA dependen del calendario de DevOps.
