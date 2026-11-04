# Rendimiento y costos del proceso NIIF 17: problemas actuales

**Fuente:** el código y la configuración de los 12 jobs de Glue, los 3 crawlers, las 6 Step Functions y las lambdas, extraídos el 2026-11-03 de la cuenta `533267035849` (DEV). **No se vieron ejecuciones reales**: no hay tiempos, DPU-horas ni facturas. Por eso cada punto indica si es un **hecho** (está en el código o la configuración) o **inferido** (se deduce, y hay que medirlo).

Para entender cómo funciona cada pieza, ver `Docs\05-arquitectura\jobs-glue.md` y `flujo-integral-niif17.md`. La lista priorizada para producción está en `revision-pre-produccion.md`.

Los IDs `RYC-xx` son locales; se consolidan (como `COS-xx` y `ARQ-xx`) en la matriz de brechas de la fase 3.

---

## 1. Cómo medir antes de optimizar

Hoy no se sabe cuánto cuesta una ejecución. La lambda `ALFA-DEV-DATALAKE-PipelineObserver` ya publica por cada job la duración, las **DPU-horas** (`dpuHours`) y el estado, en el espacio de métricas `ALFA/DATALAKE`, y por cada paso de la Step Function su duración. Con eso se obtiene el costo real de las últimas ejecuciones **sin instrumentar nada**:

| Pregunta | Dónde se responde |
|---|---|
| ¿Qué job tarda más y cuánto cuesta? | `GlueJobDurationSeconds` y registros `GLUE_JOB` (con `dpuHours`) en CloudWatch Logs del observer |
| ¿Cuánto dura cada paso de la Step Function? | Registros `SFN_STEP` (con `durationSec` y `target`) |
| ¿Cuánto escanea Athena? | Historial de consultas de Athena: `DataScannedInBytes` por consulta (filtrar el grupo `primary`) |
| ¿Cuánto cuestan los crawlers? | Métricas `CrawlerDurationSeconds` en los registros `GLUE_CRAWLER` |

Precio de referencia (de lista, us-east-1; **verificar**): Glue y crawlers, 0,44 USD por DPU-hora; Athena, 5 USD por TB escaneado.

---

## 2. Rendimiento

| ID | Severidad | Hallazgo | Tipo | Evidencia | Qué hacer |
|---|---|---|---|---|---|
| RYC-01 | Alta | **La extracción desde Oracle lee cada tabla con una sola conexión y una tabla tras otra.** Las 4 workers se pagan, pero solo una lee. Como la extracción suele ser la etapa más lenta, es el primer lugar para ganar tiempo. | Inferido | No hay `partitionColumn`, `numPartitions` ni `hashfield` en la lectura JDBC; el job recorre las tablas en bucle (`extraccion_estandar_optimized.py` líneas 1344-1367) | Medir cuánto de la ejecución es extracción. Si domina, partir la lectura por una columna numérica o `hashfield`, o reducir workers si va a seguir siendo secuencial |
| RYC-02 | Alta | **Los 3 crawlers rastrean toda la capa en cada ejecución y se comparten entre las 6 mallas.** Cada pipeline dispara 3 recorridos completos de `niif/` (Bronce, Silver y Gold), aunque solo cambió una malla. | Hecho | `RecrawlPolicy = CRAWL_EVERYTHING`, rutas `.../niif/`; Silver ya tiene 94 tablas | Un crawler por malla o con rutas e *exclusions*; o evitar el crawler: registrar las tablas una vez y agregar solo la partición nueva (`ALTER TABLE ... ADD PARTITION`) |
| RYC-03 | Alta | **Catálogo desactualizado cuando dos mallas corren a la vez.** Si el crawler ya está corriendo, la Step Function espera a que termine y sigue; pero ese recorrido pudo empezar **antes** de que sus datos llegaran. La certificación consulta Gold por Athena y podría no ver datos de esa malla. | Inferido | Los estados `CrawlerBronze/Silver/Gold` atrapan `Glue.CrawlerRunningException` y vuelven a esperar, sin volver a lanzar | Tras esperar, lanzar de nuevo el crawler y comprobar que las particiones de esa malla existen; o crawlers por malla (RYC-02) |
| RYC-04 | Alta | **`niif-dev-remediacion-estandar` admite 1 ejecución a la vez y lo usan las 6 mallas.** Si dos mallas llegan a remediación a la vez, la segunda recibe `ConcurrentRunsExceededException`. La Step Function solo reintenta `Glue.AWSGlueException`, así que falla. | Hecho (el efecto es inferido) | `ExecutionProperty.MaxConcurrentRuns = 1` (el resto: 10); `Retry` solo de `Glue.AWSGlueException` | Subir el límite (los jobs ya reciben la ruta por argumento) y reintentar también ese error en la Step Function |
| RYC-05 | Media | **Acciones repetidas de Spark.** `scd` hace 12 conteos y 6 `cache`; `silver-primas-rentas`, 8 conteos; `extraccion`, 4 conteos y 3 `collect`. Cada conteo sin caché relee el origen. | Hecho | Conteo por regex en los scripts | Un solo conteo tras materializar; sacar los conteos que solo sirven para el log; cachear solo lo que se reutiliza |
| RYC-06 | Media | **La validación técnica construye un DataFrame por regla y columna y los une.** Con tablas anchas el plan crece y se hacen muchas pasadas. | Inferido | `validacion_tecnica.py` líneas 619-750 (bucle por regla, `unionByName`) | Calcular todas las reglas en una sola pasada con agregados condicionales; extraer filas solo si el conteo es mayor que cero |
| RYC-07 | Media | **Reserva matemática ARP y RVI producen tablas de 440 y 458 columnas** (las temporales, de 494 y 511), con lógica actuarial transcrita de SQL en scripts de 1 159 y 1 247 líneas. Son los jobs con más riesgo de quedarse cortos de memoria con 4 G.1X. | Inferido | `Backend\Glue\catalogo\`; tamaño de los scripts | Revisar el *Spark UI* de una ejecución real antes de cambiar el tipo de worker; y que las consultas de Athena pidan columnas, no `*` |
| RYC-08 | Media | **Las ejecuciones no se pueden paralelizar de forma segura.** Comparten el archivo de estado por malla en S3, los 3 crawlers y la remediación de un solo cupo. | Hecho | `parametros_extraccion.json` con ruta fija por malla; RYC-02 y RYC-04 | Un `ejecucion_id` en la ruta del estado; ver `revision-pre-produccion.md` |
| RYC-09 | Media | **Athena sin límite de espera ni de escaneo.** El job de certificación espera la consulta con un `while True` sin tope; si se queda colgada, el job corre hasta su timeout de 48 h. Cuando una consulta devuelve vacío, el job la ejecuta **otra vez** para confirmarlo. | Hecho | `niif_certification.py` líneas 505-515 y 838; sin `WorkGroup` | Grupo de trabajo propio con tope de bytes por consulta, número máximo de intentos y confirmar el vacío con el manifiesto de `UNLOAD` en vez de repetir |

---

## 3. Costos

| ID | Severidad | Hallazgo | Tipo | Evidencia | Qué hacer |
|---|---|---|---|---|---|
| RYC-10 | Alta | **Un job colgado cuesta unos 84 USD.** Los 12 jobs tienen timeout de 2 880 minutos (48 h), 4 workers y 0 reintentos propios: 4 DPU × 48 h × 0,44 USD. La Step Function tampoco tiene un tiempo límite global ni por paso. | Hecho | `Timeout = 2880`; sin `TimeoutSeconds` en la máquina | Timeout por job según su duración real (p. ej., 2 a 3 veces el percentil 95) y `TimeoutSeconds` por tarea y global |
| RYC-11 | Alta | **Athena en el grupo `primary` sin tope de bytes escaneados.** Cada ejecución lanza varias consultas (`summary`, `total`, `detail`) sobre Gold; con las tablas anchas, un cambio en una consulta puede escanear mucho más sin aviso. Es el punto de costo de Athena que exige vigilar el marco del proyecto. | Hecho | `athena.start_query_execution` sin `WorkGroup`; los resultados se escriben en el bucket de *assets* | Grupo de trabajo con `BytesScannedCutoffPerQuery`, configuración obligatoria, resultados cifrados y alerta de presupuesto |
| RYC-12 | Media | **Tres recorridos completos de crawler por pipeline**, mínimo de facturación de 10 minutos por recorrido, con 6 mallas. El costo crece con las particiones (una tabla por lote en Silver). | Inferido | RYC-02 y `Backend\Glue\catalogo\` | Ver RYC-02 |
| RYC-13 | Media | **Registro excesivo.** Los 12 jobs tienen logs continuos, *job insights* y Spark UI (logs de eventos en S3) siempre activos, y el job de certificación escribe en el log cada consulta completa. No se conoce la retención de los grupos de logs. | Hecho (retención: por confirmar) | `job.json` de cada job | Retención de 30 días, activar *insights* y Spark UI solo para diagnóstico y no registrar la consulta completa |
| RYC-14 | Media | **Crecimiento de almacenamiento sin regla de retención conocida.** Cada ejecución deja datos por lote en Bronce y en la remediación, copias históricas de Silver (`historico/silver_preparado_<fecha>`), tablas `tmp_`, CSV de certificación y resultados de Athena. En el inventario solo hay notificaciones y versionado de los buckets, no reglas de ciclo de vida. | Inferido | Rutas de los jobs y del catálogo | Reglas de ciclo de vida por prefijo, acordes con la retención del estándar de remediación |
| RYC-15 | Media | **El observador lee el historial completo de cada ejecución** (hasta 10 000 eventos) de todas las Step Functions de la cuenta, no solo las de NIIF. | Hecho | `index.py` líneas 24-28 y SF-13 | Limitar a las máquinas `niif-dev-*` o leer el historial solo cuando haga falta el detalle por paso |
| RYC-16 | Baja | **Lambdas con paquetes de 45 MB** por `pandas` y `numpy` que no se importan, a 512 MB y 300 s. | Hecho | LMB-05 | Quitar las librerías sobrantes |
| RYC-17 | Baja | **El sondeo de los crawlers cada 20 s** no es un costo (unos 3 estados por vuelta, centavos por mes), pero **no tiene tope de vueltas**: un crawler colgado deja viva la ejecución. | Hecho | Estados `Esperar...`, SF-06 | Contador de vueltas |

---

## 4. Lo que no se puede decir todavía

- **Cuánto cuesta y cuánto tarda hoy una ejecución completa.** Hace falta el historial real (sección 1).
- **Cuánto escanea Athena.** Se necesita el historial de consultas y los JSON de consultas de cada caso (en S3).
- **Si DynamoDB está en modo bajo demanda o aprovisionado.** El inventario de DynamoDB salió vacío.
- **La retención de los logs y las reglas de ciclo de vida de S3.** No se extrajeron.
