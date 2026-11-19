# Roadmap de observabilidad NIIF 17 (jobs de Glue y lambdas)

**Fecha:** 2026-11-19 (niveles ajustados el mismo día)
**Estado:** propuesta para aprobar. No se ha cambiado código ni infraestructura.
**Alcance:** lo nuestro en AWS: jobs de Glue del medallón (paso 2), Step Functions de las mallas, y lambdas y jobs de notificación, certificación y transporte (paso 3).
**Reemplaza a:** `monitoreo-continuo-grafana.md` (borrador del 2026-10-30). Sigue vigente la propuesta de alertas en `Tareas\T1-notificaciones\propuesta-observabilidad-eventos.md` (plano técnico y de negocio, ServiceNow), que aquí queda como fase 6.

## 1. Qué buscamos y hasta dónde llegamos
1. **Seguir una petición de punta a punta.** Con un solo ID (`ejecucion_id`) se ve todo lo que pasó en una corrida: configurador → Step Function → cada job de Glue → certificación → correo → Certificar / No certificar → copia a transporte → envío a FIS.
2. **Saber cuánto dura cada parte**: la corrida, cada job y cada paso dentro del job (leer, validar, escribir…).
3. **Contexto completo en cada línea (MDC):** quién, qué malla, qué periodo, qué paso, qué `cert_code`, qué versión de configuración.
4. **Responder preguntas técnicas y de negocio** en producción (sección 6).
5. **Dejar todo publicado y documentado** para que otro equipo arme los tableros (Grafana, CloudWatch Dashboards, QuickSight o Power BI). **Nosotros no construimos los tableros** ni elegimos la herramienta (decisión del usuario, 2026-11-19): la entrega es independiente de quién los haga y con qué.
6. **Que sirva para avisar en el futuro:** las mismas métricas y eventos alimentan alarmas (fase 6).

### Los tres niveles: logs, métricas y trazas (confirmado por el usuario, 2026-11-19)
| Nivel | Pregunta que responde | Qué se emite | Dónde queda |
|---|---|---|---|
| **Logs** | ¿Qué pasó exactamente, con qué datos de contexto y qué error? | Una línea JSON por evento con el contrato v1 (sección 4): contexto completo (MDC), conteos, error clasificado | CloudWatch Logs (Logs Insights) y copia en S3 para Athena |
| **Métricas** | ¿Cuántas veces, cuánto tarda, cuántas filas, cuántos errores? ¿Hay que avisar? | EMF desde el código (`DuracionMs`, `Filas*`, `Errores`, `Reintentos`) y métricas nativas de Glue, Lambda, Step Functions y API Gateway | CloudWatch Metrics; base de las alarmas (fase 6) |
| **Trazas** | ¿Por dónde pasó la petición de punta a punta y cuánto duró cada tramo? | Lambdas, Step Functions y API Gateway: X-Ray activo. Glue (sin soporte de X-Ray): traza por logs, con `ejecucion_id` como ID de traza y `PASO_INICIO`/`PASO_FIN` como tramos con `duracion_ms`. La Step Function en nivel `ALL` da el tramo de cada job | X-Ray / CloudWatch ServiceLens y Logs Insights |

Las preguntas técnicas y de negocio (sección 6) se responden combinando los tres.

## 2. Lo que ya había en la memoria (verificado el 2026-11-19)
| Afirmación | Fuente | Resultado |
|---|---|---|
| Estándar de logs: JSON en español, `ejecucion_id`, `request_id`, `cert_code`, catálogo cerrado, EMF | `08-estandares-desarrollo\estandar-lambdas-python.md` §5 | **Sirve.** Es la base del contrato (sección 4) |
| Reglas de trazabilidad y métricas por paso | skill, `diseno-escalable-resiliente.md` §5 | **Sirve, con un ajuste:** usaba `ts` y `periodo`; el código usa `marcaTiempo` y `periodo_negocio`. Se corrige a lo que dice el código |
| ARQ-32: la Step Function no pasa el ID a los jobs | `notificaciones-vs-arquitectura.md` | **Sigue vigente.** Los 47 `startJobRun` de las 8 definiciones no llevan `$$.Execution.Name`; `$$.Execution.Id` solo va a DynamoDB (estado de la corrida) |
| La certificación guarda `ejecucionId` | memoria de `job-certificacion` | **Cierto solo en la rama** `feature/codigo-nuevo-notificacion` (`generate_certification.py` l. 208). En `main` sigue el `uuid4` por consulta |
| Librería común `niif_observability` | traspaso de sesión | **Existe en la rama feature**, 7 copias iguales (logger JSON con contexto `bind()` = MDC, y EMF). **No mide duración** ni trae `malla`, `etapa`, `paso`, `resultado`, `config_version` |
| Grafana fuera de alcance; dejamos logs, métricas y alarmas | `monitoreo-grafana` | **Sirve.** Es justo el límite de este roadmap |
| "Origen de logs sin decidir" | `monitoreo-continuo-grafana.md` | **Desactualizado.** Se propone decidir en la fase 0 (sección 5) |
| "Soporte no recibe correos" | `monitoreo-continuo-grafana.md` §1 | **Desactualizado.** El 2026-11-10 se decidió que las alertas técnicas salen por SES al grupo de soporte; ServiceNow sigue como opción |
| Logs de Step Functions apagados (ARQ-12) | `auditoria-pipeline-datos-vs-arquitectura.md` | **Confirmado en IaC:** el módulo `step-functions` 5.0.2 se llama sin `logging_configuration` |

## 3. Diagnóstico por repo (código de `main` en `alfa-repos`, 2026-11-19)
Leyenda: **Texto** = mensajes libres de `logging`/`print`; **JSON** = una línea JSON por evento.

### Jobs de Glue
| Repo | Formato | ID de flujo | Duración | Conteos | Riesgos | Cambio | Fase |
|---|---|---|---|---|---|---|---|
| `rmd-ext-estandar` (extracción, remediación, SCD, validación; lo usan las 6 mallas) | Texto (138 líneas) | No | Sí, en texto (lectura, count, escritura por tabla) | En texto | — | Pasar a eventos con `duracion_ms` y conteos; es el de mayor alcance | 2a |
| `auxilios-funerarios` (Silver, validación técnica y Gold SCD genéricos) | Texto | No | No | Parcial, texto | — | Igual al anterior | 2a |
| `certification` | Texto | `executionId` = `uuid4` por consulta (ARQ-32) | No | No | SQL completo en log (SEG-15) | Ya resuelto en la rama feature; desplegarla | 2a |
| `notification-certification` | `print` texto | No | No | 1 | **Correos de destinatarios y enlaces en el log (SEG-17)** | Ya resuelto en la rama feature (eventos `NTF_*`); desplegarla | 2a |
| `primas-arl` (Bronce y Silver) | Texto | No | No | 1 | — | Instrumentación mínima | 2b |
| `primas-rentas`, `primas-endosos`, `reserva-matematica-arp`, `reserva-matematica-rvi` (Silver) | Texto | No | No | 1 a 4 | — | Instrumentación mínima | 2b |
| `pcr-emisiones` | Texto | No | Sí, en texto | No | `except Exception: pass` (l. 2988) | Instrumentación mínima y quitar el `pass` | 2c |
| `primas-previsional` | `print` (18) + texto | No (usa `LOTE_CARGUE_ID`) | No | 2 | `except Exception: pass` (`gestor_archivos_previsional.py` l. 191) | Instrumentación mínima | 2c |
| `emisiones-vida` | `print` (4) | No | No | No | — | Instrumentación mínima (versión de prueba) | 2c |

Común a los 12: ninguno recibe `--EJECUCION_ID` ni registra el `JOB_RUN_ID`; el `logging.basicConfig` escribe a *stderr*, y Glue lo manda al grupo de errores (hipótesis, confirmar en DEV con el comando del anexo).

### Lambdas
| Repo | Formato | ID de flujo | Duración | Riesgos | Cambio | Fase |
|---|---|---|---|---|---|---|
| `conf-*` (6) y `configuration-general` | JSON con `request_id` | **Crean el ID raíz** (`<malla>-<periodo>-auto-<marca>-<req8>`) y el registro `EJC#` | No | `except: pass` en `case_registry.py` l. 155 | Emitir `EJECUCION_INICIADA` con el ID raíz | 3 |
| `notification-certification` (trigger) | Texto | Solo `cert_code` | No | Registra `NewImage` completa (SEG-12); éxito falso (ARQ-26) | Lo reemplaza la orquestadora (rama feature) | 3 |
| `copy-s3-certification`, `mov-s3-certification` | Texto | `executionId` del registro (no es el del flujo) | No | Éxito falso (ARQ-26) | Los reemplaza la orquestadora | 3 |
| `composite_notification_evaluator` | `print` JSON | `sourceExecutionId` | No | — | Pasar a la librería | 3 |
| `certification` (Certificar / No certificar) | Texto | No | No | Correo del aprobador en log (SEG-07); no registra el resultado (ARQ-33) | Un evento por salida, aprobador enmascarado | 3 |
| `short-link-redirect` | `print` | No | No | **Registra el evento HTTP completo y `token -> URL` (SEG-17)** | Evento sin token ni cabeceras | 3 |
| `send-via-sftp-lambda` | Texto | No | Sí, texto | 5 `except: pass` (ARQ-56) | Leer el ID de los metadatos del objeto S3; evento por archivo | 3 |
| `get-files-sftp` | Texto | No | Sí, texto | 4 `except: pass` (ARQ-56) | Evento por archivo, propio ID de corrida | 3 |
| `configurador-mallas`, `notification-certification-orchestrator`, `post-login` | — | — | — | Solo en ramas feature | Nacen con la librería v2 | 3 |

### Infraestructura (IaC)
| Pieza | Hoy | Brecha |
|---|---|---|
| 47 jobs de Glue (dos repos IaC) | `--enable-continuous-cloudwatch-log`, `--enable-metrics`, `--enable-job-insights`: **sí en todos** | Falta `--enable-observability-metrics`; sin retención declarada para `/aws-glue/*` |
| 8 Step Functions | `xray` en la política; **sin `logging_configuration`** | ARQ-12 / ARQ-52 |
| Lambdas (módulo `lambda` 7.9.0) | Política de logs y de X-Ray adjunta; **sin `tracing_mode`** ni retención | ARQ-52, COS-10 |
| API Gateway | Logs de ejecución INFO, métricas y X-Ray: sí. **Sin logs de acceso** | ARQ-52 |
| Alarmas, reglas de EventBridge para fallos, tableros, filtros de suscripción | **Ninguna** (la única regla es el horario de `get-files-sftp`) | ARQ-51 |
| SNS | Solo `certification_email` (suscripción por correo) | No hay tema de alertas técnicas |
| Estado de corrida | `ejc-ejecuciones` (una fila por ejecución, 6 mallas) y `pipeline-estado` (llave `amb#malla#periodo`: **se sobrescribe**, solo queda el último estado) | ARQ-55 |

## 4. Contrato de log v1 (lo que debe emitir cada componente)
Se respeta lo que ya usa el código (`niif_observability`) y se agrega lo que falta. **Nombres estables: cambiar uno rompe los tableros.**

**Siempre:** `marcaTiempo` (ISO-8601 UTC), `nivel`, `evento`, `mensaje` (español), `funcion` (componente), `esquema` = `niif17-log/1`, `ambiente`, `ejecucion_id`, `request_id` (`aws_request_id` en lambda, `JOB_RUN_ID` en Glue).

**Contexto de negocio (MDC, se fija una vez con `bind`):** `malla`, `use_case_id`, `periodo_negocio`, `cert_code` (desde la certificación), `lote_cargue_id` (si aplica), `config_version`.

**Por paso:** `etapa` (`configuracion`, `extraccion`, `validacion`, `remediacion`, `silver`, `gold`, `certificacion`, `notificacion`, `aprobacion`, `transporte`), `paso` (nombre estable, ej. `leer_oracle`, `escribir_gold`), `duracion_ms`, `resultado` (`OK`, `ERROR`, `PARCIAL`, `OMITIDO`).

**Datos (sin datos personales):** `filas_entrada`, `filas_salida`, `filas_cuarentena`, `suma_control` (suma del importe principal), `origen`, `destino` (tabla o prefijo, nunca la fila), `bytes`.

**Error:** `error_codigo`, `error_dominio`, `error_severidad`, `error_recuperable` (taxonomía de HU-GI-06) y `error_tipo` (clase de la excepción). Sin valores de filas, SQL completo, tokens ni correos.

**Eventos comunes de ciclo de vida** (todos los componentes; cada uno puede tener además los suyos, como los `NTF_*` del job de notificación):
| Evento | Quién | Campos clave |
|---|---|---|
| `EJECUCION_INICIADA` | configuradores | `ejecucion_id` raíz, `malla`, `periodo_negocio`, `modo` |
| `COMPONENTE_INICIO` / `COMPONENTE_FIN` | cada job y lambda | `duracion_ms`, `resultado`, totales |
| `PASO_INICIO` / `PASO_FIN` | cada paso interno | `etapa`, `paso`, `duracion_ms`, conteos |
| `CUADRE_RESULTADO` | validación, remediación, SCD | `filas_entrada = filas_salida + filas_cuarentena`, diferencia |
| `ERROR_CLASIFICADO` | todos | campos de error |
| `CERTIFICACION_GENERADA`, `NOTIFICACION_ENVIADA`, `DECISION_REGISTRADA`, `TRANSPORTE_COPIADO`, `ARCHIVO_ENVIADO_SFTP` | cadena del paso 3 | `cert_code`, `resultado`, aprobador enmascarado |

**Métricas EMF** (como dice la skill): espacio `NIIF17/<componente>`; dimensiones solo `ambiente`, `malla`, `etapa`, `resultado`; métricas `DuracionMs`, `FilasEntrada`, `FilasSalida`, `FilasCuarentena`, `Errores`, `Reintentos`. **Nunca** `ejecucion_id`, `cert_code` ni periodo como dimensión.

**Librería `niif_observability` v2** (cambio a la que ya existe): agregar `paso(...)` como bloque `with` que emite `PASO_INICIO` y `PASO_FIN` con `duracion_ms` y `resultado` (también si hay excepción, que se relanza), los campos nuevos del contrato, enmascarado de correos y tokens, y la variante para Glue (escribe a *stdout*, toma `JOB_RUN_ID`). Una sola fuente: en Glue por `--extra-py-files` desde el bucket de assets (propuesta), y en lambdas una copia verificada por `verificar_copias_comunes.py`.

## 5. Cómo viaja el ID y a dónde van los logs
```
Configurador ──crea ejecucion_id──► Step Function (nombre de ejecución)
   │                                  │ $$.Execution.Name → --EJECUCION_ID en cada tarea
   │                                  ▼
   │                      Jobs de Glue: extracción → validación → remediación → Silver → Gold → certificación
   │                                                                                    │ guarda ejecucionId en notification-certification
   │                                                                                    ▼
   │                      Orquestadora (stream) → job de notificación (--EJECUCION_ID del registro) → correo
   │                      Enlace corto (tabla short-links guarda ejecucion_id) → certificar (lee el registro)
   │                      Copia a transporte: metadatos S3 x-amz-meta-ejecucion-id y cert-code → send-via-sftp los lee
   ▼
Todo log ──► CloudWatch Logs (retención por ambiente)
               ├─► Logs Insights, métricas EMF, alarmas y trazas X-Ray (Grafana con origen CloudWatch)
               └─► (opcional) filtro de suscripción → Firehose → S3 zona de observabilidad (Parquet por fecha) → Athena (histórico de logs)
```
- **Grupos de mallas (evaluador compuesto):** cuando Rentas + Endosos o ARP + RVI se certifican juntas, una certificación nace de **varias** corridas. El job de certificación de la rama feature ya guarda `ejecucionesOrigen` (lista) junto a su `ejecucion_id`. Los logs y el Modelo Trazabilidad deben llevar esa lista para unir la traza de cada malla con la certificación común.
- **Duración sin tocar código:** con los logs de la Step Function en nivel `ALL` y `include_execution_data = false` se obtiene la hora de entrada y salida de cada estado (cada job), por ejecución. Glue ya publica `ExecutionTime` y DPU por corrida. El código solo aporta la duración de los pasos internos.
- **X-Ray** activo en lambdas y Step Functions como complemento (Glue no lo soporta; ahí la traza es por logs).
- **Destino (decisión propuesta):** CloudWatch Logs como origen único (opción A del borrador anterior). La copia a S3 para histórico y Athena es opcional y solo se hace si se exige retención larga (ver 7.1). OpenSearch o Elastic solo si aparece un requisito de búsqueda o retención que esto no cubra.

## 6. Preguntas que se deben poder responder (y con qué)
| Pregunta | Señal | Cómo |
|---|---|---|
| ¿Dónde falló la corrida X y con qué error? | Logs + trazas | Logs Insights: `fields marcaTiempo, funcion, etapa, paso, evento, error_codigo, mensaje \| filter ejecucion_id = "<id>" \| sort marcaTiempo asc` |
| ¿Qué paso es el más lento por malla? | Logs + trazas | `filter evento = "PASO_FIN" \| stats avg(duracion_ms), pct(duracion_ms, 95), max(duracion_ms) by malla, etapa, paso` |
| ¿Cuánto tardó cada job en la última corrida? | Trazas | Logs de la Step Function (`TaskStateEntered` / `TaskStateExited`) o `COMPONENTE_FIN` |
| ¿Qué jobs fallaron esta semana? | Métricas | EventBridge `Glue Job State Change` FAILED/TIMEOUT y métrica `Errores` |
| ¿Cuántos reintentos hubo y en qué paso? | Logs + trazas | métrica `Reintentos` por `etapa` |
| ¿Cuadran las filas entre Bronce, Silver y Gold? | Logs | `filter evento = "CUADRE_RESULTADO" \| fields malla, periodo_negocio, etapa, filas_entrada, filas_salida, filas_cuarentena` |
| ¿El total de montos se mantuvo entre capas? | Logs | `suma_control` por `etapa` y `ejecucion_id` |
| ¿Se certificó la malla M del periodo P, quién (enmascarado) y cuándo? | Logs | `DECISION_REGISTRADA` + tabla de aprobaciones |
| ¿Cuánto tarda el negocio en certificar? | Logs | diferencia entre `NOTIFICACION_ENVIADA` y `DECISION_REGISTRADA` por `cert_code` (Athena) |
| ¿Qué certificaciones llevan más de N días pendientes? | Logs | tabla `notification-certification` (índice `status-pendingDate-index` de la orquestadora) |
| ¿Llegó el archivo a FIS? | Logs + trazas | `ARCHIVO_ENVIADO_SFTP` con `cert_code` |
| ¿Cuánto cuesta cada malla en Glue y Athena? | Métricas | DPU-horas de Glue; `QueryExecutionId` y bytes escaneados en `PASO_FIN` de certificación y notificación |

## 7. Roadmap de cambios
Duraciones estimadas, por confirmar. Cada fase entrega código en zip por recurso y pide a DevOps lo de IaC, según el estándar de gobierno DevOps.

| Fase | Qué | Repos | Entregable / criterio de cierre | Estimado |
|---|---|---|---|---|
| **0. Contrato y decisiones** | Aprobar contrato v1, catálogo de eventos comunes, nombres de métricas, retención por ambiente, destino de logs y los tres niveles | memoria, `Docs\08`, skill | Contrato aprobado; `niif_observability` v2 especificada | 1-2 d |
| **1. ID de punta a punta (ARQ-32)** | `--EJECUCION_ID.$ = $$.Execution.Name` (y `--MALLA`, `--PASO.$ = $$.State.Name`) en los 47 `startJobRun`; en la carga útil de las tareas Lambda; certificación guarda `ejecucionId`; `short-links` guarda `ejecucion_id`; copia a transporte escribe metadatos S3; prueba automática sobre las ASL | `iac-...-operations-manager` (datafiles), `certification` (rama feature), orquestadora, `short-link-redirect`, `send-via-sftp` | Script que falla si una tarea no lleva el ID; una corrida en DEV con el mismo ID en todos los logs | 3-4 d |
| **2a. Glue de mayor alcance** | Librería v2 en Glue; `rmd-ext-estandar` y `auxilios-funerarios` (genéricos de las 6 mallas); desplegar `certification` y `notification` de la rama feature | 4 repos de Glue | Cada paso con `PASO_FIN`, conteos, `CUADRE_RESULTADO` y `suma_control` | 5-6 d |
| **2b. Silver por malla** | Instrumentación mínima (sin reescribir): MDC al arrancar, pasos con duración, conteos, errores clasificados | `primas-arl`, `primas-rentas`, `primas-endosos`, `reserva-matematica-arp`, `reserva-matematica-rvi` | Igual a 2a | 3-4 d |
| **2c. Resto de Glue** | Igual a 2b y quitar los `except: pass` | `pcr-emisiones`, `primas-previsional`, `emisiones-vida` | Igual a 2a | 2-3 d |
| **3. Lambdas** | Desplegar la orquestadora (reemplaza trigger, copy, move: cierra ARQ-26, SEG-12); `certification` (ARQ-33, SEG-07); `short-link-redirect` (SEG-17); evaluador; configuradores (`EJECUCION_INICIADA`); SFTP (ARQ-56) | 16 repos de lambdas | Una línea JSON por evento, sin datos personales; pruebas de que el ID entra y sale | 5-7 d |
| **4. IaC de observabilidad (DevOps)** | Step Functions: logs `ALL` sin datos de ejecución; `tracing_mode = Active` en lambdas y Step Functions; retención por grupo de logs; `--enable-observability-metrics`; logs de acceso del API en JSON; grupo de trabajo de Athena con tope | 2 repos IaC | Solicitud a DevOps redactada y aplicada en DEV | 2-3 d nuestros + DevOps |
| **5. Publicar para tableros** | Consultas guardadas de Logs Insights (`aws_cloudwatch_query_definition`); opcional, si se exige retención larga: filtro de suscripción → Firehose → S3 Parquet → tabla de Glue y vistas de Athena (ver 7.1); diccionario de campos, eventos y métricas; guía de conexión de los orígenes (CloudWatch Logs y Metrics, X-Ray, Athena), válida para cualquier herramienta | IaC + `Docs\` | Cualquier equipo puede armar tableros con cualquier herramienta usando solo la guía, sin tocar código. No incluye tableros | 3-4 d |
| **6. Alertas (futuro)** | EventBridge (Glue y Step Functions FAILED/TIMEOUT) y alarmas sobre las métricas → SNS `niif-<amb>-alertas-tecnicas` → correo de soporte por SES y, si se aprueba, ServiceNow | IaC | Según `propuesta-observabilidad-eventos.md` | por definir |

**Orden recomendado:** 0 → 1 → 4 (la parte de Step Functions y retención, que da duración por job sin código) → 2a → 3 → 2b → 2c → 5 → 6. La fase 1 es la que más valor da: sin ID común, ningún tablero puede unir la corrida con su correo y su transporte.

## 7.1 Impacto en la infraestructura y en el diagrama
Verificado el 2026-11-19 en `NIIF17_ModeloSolucion_V1.0.drawio`: solo la `Página-1` dibuja **CloudWatch** y un **Modelo Trazabilidad** (icono de DynamoDB), sueltos y sin ninguna conexión. La `Página-2` (pasos 2 y 3) no dibuja nada de observabilidad (ARQ-31). No aparecen X-Ray, reglas de EventBridge (solo EventBridge Scheduler), SNS de alertas, Firehose ni una zona de logs en S3.

El flujo de datos no cambia: medallón, certificación, notificación y transporte siguen iguales. La observabilidad va al lado.

| Tipo de cambio | Qué | ¿Servicio nuevo? | ¿Toca el diagrama? |
|---|---|---|---|
| **Solo código** | Librería v2, eventos, MDC, duración por paso, quitar datos sensibles | No | No |
| **Configuración de lo que ya existe** | `--EJECUCION_ID` en las ASL; logs de Step Functions; retención de los grupos de logs; `--enable-observability-metrics` en Glue; logs de acceso del API; consultas guardadas de Logs Insights; métricas EMF (no crean recursos) | No: CloudWatch ya está en la `Página-1` | Solo conectar CloudWatch a los componentes |
| **Usar lo que el diagrama ya trae** | El historial por paso (ARQ-55) va en el **Modelo Trazabilidad**; se propone que sea la tabla `ejc-ejecuciones`, que ya existe, con una fila por paso | No | Conectarlo a la Step Function y a los jobs |
| **Servicio que no está en el diagrama** | X-Ray (trazas) | Sí, pero sin costo de operación: es un ajuste de las lambdas, las Step Functions y el API | Agregar el icono; aval del arquitecto |
| **Servicios nuevos opcionales** | Firehose → S3 → Athena para histórico (fase 5) y EventBridge + SNS para alertas (fase 6) | Sí | Sí; aval del arquitecto |

**Arquitectura propuesta (`NIIF17_ModeloSolucion_V1.0-propuesta.drawio`, nuestra):** tampoco dibuja observabilidad. Solo cambia la `Página-2`, y su nota dice que las 6 lambdas y el job de notificación "escriben logs JSON con el mismo `ejecucion_id` y `cert_code` de punta a punta". Eso cubre solo la cadena de certificación (fase 3 de este roadmap). No incluye los jobs del medallón (paso 2), las métricas ni las trazas. Sus piezas nuevas encajan con el roadmap: el Modelo Enlaces cortos guarda el `ejecucion_id` (fase 1), la orquestadora reemplaza a trigger, copy y move (fase 3) y el evaluador compuesto exige trazas con varias corridas de origen (sección 5). Como el diagrama es nuestro, ahí se dibujó la observabilidad que se le llevará al arquitecto (2026-11-19): elementos 79 a 81 en la `Página-2` y 82 a 84 en la `Página-1`, con una nota en cada hoja.

**Recomendación:**
- Hacer primero lo que no requiere servicios nuevos: código, configuración y el Modelo Trazabilidad. Eso da logs, métricas y trazas de punta a punta.
- Logs y métricas quedan listos para tableros directamente desde CloudWatch, que es un origen de datos estándar de Grafana y de otras herramientas.
- Dejar Firehose, S3 y Athena como opción, solo si se exige guardar los logs más tiempo del que conviene en CloudWatch (por ejemplo, 7 años).
- Llevarle al arquitecto la propuesta de dibujo para la `Página-2` (cierra ARQ-31): CloudWatch y X-Ray conectados a lambdas, Step Functions, Glue y API, y el Modelo Trazabilidad conectado a la Step Function.

## 8. Brechas nuevas
| ID | Brecha | Evidencia |
|---|---|---|
| ARQ-51 | No hay alarmas, reglas de EventBridge para fallos, tableros ni filtros de suscripción en IaC: nada avisa cuando una malla falla | búsqueda en los dos repos IaC (solo existe la regla horaria de `get-files-sftp`) |
| ARQ-52 | Configuración de logs incompleta en IaC: Step Functions sin `logging_configuration`, lambdas sin `tracing_mode`, API sin logs de acceso, Glue sin `--enable-observability-metrics` | `step_functions/main.tf`, `lambdas/*/main.tf`, `api_gateway/parameters.tf`, `glue_jobs/parameters.tf` |
| ARQ-53 | Los 12 repos de Glue en `main` registran texto libre: sin `ejecucion_id`, sin `JOB_RUN_ID`, sin duración ni conteos estructurados | barrido de `main`, sección 3 |
| ARQ-54 | El contrato de log no está unificado: la skill usaba `ts`/`periodo`, el código `marcaTiempo`/`periodo_negocio`; la librería no tiene `malla`, `etapa`, `paso`, `duracion_ms`, `resultado` ni `config_version` | `niif_observability/structured_log.py` (rama feature) |
| ARQ-55 | `pipeline-estado` usa la llave `amb#malla#periodo` y se sobrescribe: solo queda el último estado, no el historial por paso | definiciones ASL, tarea `RegistrarPipelineCompleto` |
| ARQ-56 | Errores tragados (`except: pass`) fuera de la cadena ya auditada (ARQ-26): `send-via-sftp` (5), `get-files-sftp` (4), `primas-previsional`, `pcr-emisiones`, `configuration-general` | barrido de `main` |
| SEG-17 | Datos sensibles en logs no cubiertos por SEG-07/SEG-12: el job de notificación en `main` registra correos de destinatarios y enlaces; `short-link-redirect` registra el evento HTTP completo y `token -> URL` | `notification_certification.py` l. 690-707; `short_link_redirect.py` l. 35 y 72 |
| COS-10 | Sin retención declarada en los grupos de logs de lambdas, Step Functions y Glue (por confirmar: el módulo deja "nunca expira"); con logs `ALL` y más eventos, la ingesta y el almacenamiento crecen sin tope | IaC sin `cloudwatch_logs_retention_in_days` |

Brechas ya conocidas que este roadmap cierra: ARQ-12, ARQ-26, ARQ-31 (dibujar la observabilidad en el diagrama, a sugerir al arquitecto), ARQ-32, ARQ-33, SEG-07, SEG-12, SEG-15.

## 9. Decisiones que necesitamos
1. ~~Los tres niveles~~: resuelto, son logs, métricas y trazas (sección 1).
2. Retención por ambiente (propuesta: DEV y QA 30 días; PRD 1 año en CloudWatch y 7 años en S3 por la evidencia de certificación, por confirmar contra la norma).
3. Destino: CloudWatch + copia a S3/Athena (propuesta) frente a OpenSearch o Elastic corporativo.
4. Instrumentación mínima en los Silver por malla (propuesta) o esperar a reescribirlos en hexagonal.
5. ~~Quién arma los tableros~~: resuelto, fuera de nuestro alcance. La fase 5 deja los orígenes publicados y documentados para quien sea.

## Anexo: verificación en DEV (solo lectura, CloudShell)
```bash
export AWS_PAGER=""
aws logs describe-log-groups --log-group-name-prefix /aws-glue --query 'logGroups[].[logGroupName,retentionInDays]' --output table
aws logs describe-log-groups --log-group-name-prefix /aws/lambda/niif-dev --query 'logGroups[].[logGroupName,retentionInDays]' --output table
aws stepfunctions describe-state-machine --state-machine-arn <arn-de-niif-dev-primas-arl> --query '{logs:loggingConfiguration,tracing:tracingConfiguration}'
aws cloudwatch describe-alarms --alarm-name-prefix niif --query 'MetricAlarms[].AlarmName'
```
