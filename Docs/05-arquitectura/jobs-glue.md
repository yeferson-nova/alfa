# Los jobs de Glue del proceso NIIF 17: qué hace cada uno

**Fuente:** el código y la definición de los 12 jobs (`Backend\Glue\<job>\` con su `.py` y `job.json`), más los 3 crawlers (`Backend\Glue\crawlers\`). Cuenta `533267035849`, us-east-1, DEV, extraídos el 2026-11-03. Este documento **solo explica cómo funcionan**. Los problemas de rendimiento y costo están en `Docs\06-brechas\rendimiento-y-costos.md` y la lista de correcciones en `Docs\06-brechas\revision-pre-produccion.md`.

> Lo marcado **(inferido)** sale de leer el código; no se vieron ejecuciones ni los JSON de configuración que viven en S3.

---

## 1. Visión general

Hay **12 jobs**: 5 genéricos que usan las 6 mallas, 6 Silver (uno por malla) y 1 de envío de correo.

| Etapa | Job | Genérico o por malla | Líneas | Lo invoca |
|---|---|---|---|---|
| 1. Extracción | `niif-dev-extraccion-estandar-optimized` | Genérico (la malla cambia el JSON de configuración) | 1 887 | Step Function |
| 2. Validación técnica | `niif-dev-val-tecnica` | Genérico | 810 | Step Function |
| 3. Remediación | `niif-dev-remediacion-estandar` | Genérico | 681 | Step Function |
| 4. Silver | `niif-dev-silver-<malla>` ×6 | Uno por malla | 513 a 1 247 | Step Function |
| 5. Gold | `niif-dev-scd` | Genérico | 496 | Step Function |
| 6. Certificación | `niif-dev-certification` | Genérico (el caso se pasa por argumento) | 851 | Step Function |
| 7. Correo | `niif-dev-notification-certification` | Genérico | 620 | Trigger del stream `PENDING` (**no** la Step Function) |

**Configuración común** (los 12 jobs): Glue 4.0, 4 workers G.1X fijos, timeout 2 880 minutos (48 h), 0 reintentos propios, *job bookmark* desactivado, métricas, *job insights*, Spark UI y logs continuos en CloudWatch. Cada job tiene su **propio rol IAM** (`niif-dev-jobs-<nombre>` o `niif-dev-<nombre>`); no se vieron sus políticas. La Step Function agrega hasta 2 reintentos por error de Glue.

---

## 2. Cómo se pasan el trabajo: el archivo de estado

Los jobs no se invocan entre sí ni intercambian datos en memoria. Cada uno recibe por argumento la **ruta del archivo `parametros_extraccion.json`** de la malla (`s3://niif-dev-assets/<malla>/parametros_extraccion.json`), lo lee, comprueba lo que dejó el anterior, trabaja, y **reescribe el mismo archivo** con su resultado. La Step Function lo lee una vez (después de la extracción) para decidir si hay datos nuevos.

| Lo escribe | Campos principales | Lo exige el siguiente |
|---|---|---|
| Extracción | `estado_extraccion`, `lote_cargue_id`, `datasets_extraidos` (con `ruta_dataset_destino`), `requiere_procesamiento_caso`, `hay_datos_nuevos`, `checkpoint_extraccion`, `metricas_extraccion`, `periodo_negocio`, `modo_ejecucion` | Validación: `estado_extraccion = SUCCESS` y `lote_cargue_id` |
| Validación técnica | `estado_validacion` (`SUCCESS` / `FAILED`), `estado_hallazgos_validacion`, `total_errores_validacion`, `ruta_validacion_tecnica`, `requiere_revision_analitica`, `validacion_tecnica_detiene_flujo` (siempre `False`), `modo_flujo_post_validacion` | Remediación |
| Remediación | `estado_remediacion`, `estado_disponibilizacion_silver`, `datasets_disponibilizados_silver`, `datasets_copiados_silver`, `ruta_remediacion_historica`, `total_registros_remediacion` | Silver: `estado_remediacion = SUCCESS` |
| Silver | `estado_preparacion_silver`, `ruta_silver_preparada`, `lotes_usados_preparacion_silver`, `total_registros_silver_preparada`, `modo_escritura_silver_preparada` | Gold: `ruta_silver_preparada` |
| Gold (SCD) | `estado_gold_scd`, `estado_consolidacion_gold`, `ruta_gold_scd`, `metricas_gold_scd`, `gold_vacio_inicializado` | La certificación consulta Gold por Athena |

Dos reglas del contrato que hay que conocer:
- **La validación `FAILED` no corta el flujo.** Silver acepta `estado_validacion` en `SUCCESS` o `FAILED`; solo exige que la remediación esté en `SUCCESS`.
- **El `lote_cargue_id`** es un identificador de la carga (un número). Todo se particiona y se rastrea por él: Bronce, remediación y los archivos de estado.

---

## 3. Un job a la vez

### 3.1 `niif-dev-extraccion-estandar-optimized`
- **Entrada:** `--RUTA_CONFIG_EXTRACCION` (JSON de la malla en S3) y la conexión Glue `datalake-dev-alfaprd-dev-connection` hacia **Oracle**.
- **Qué hace:** recorre las tablas y ciclos declarados en el JSON y, por cada una, lee Oracle con `SELECT * FROM <esquema>.<tabla> [WHERE ...]` (fetch de 10 000 filas, tiempos de conexión y lectura de 30 s). Guarda en Bronce en Parquet (`alfa-datalake-dev-bronce/niif/...`) con el `lote_cargue_id`. Cuenta los registros para saber si hay datos nuevos. Aplica **segmentos**: excluye llaves ya procesadas (consultando Bronce y Silver) para no repetir trabajo.
- **Reanudación:** mantiene un *checkpoint* en el JSON de estado (por tabla, segmento y ciclo). Si falla a medias y se reanuda, salta lo que ya salió bien.
- **Salida:** datasets en Bronce y el JSON de estado con `datasets_extraidos`, `requiere_procesamiento_caso` y métricas.
- **Notas:** `persist` en memoria y disco, cuatro conteos y tres `collect`. Una sola conexión JDBC por tabla, sin particionar la lectura (inferido: no hay `partitionColumn` ni `hashfield`).

### 3.2 `niif-dev-val-tecnica`
- **Entrada:** `--RUTA_CONFIG_VALIDACION` y el JSON de estado (exige extracción en `SUCCESS`).
- **Qué hace:** por cada dataset extraído aplica las reglas **por columna** declaradas en el JSON: `NULO` (nulos no permitidos), `TIPO`, `LONGITUD` y `PRECISION_ESCALA`. Une los registros fallidos en una tabla con la regla, la columna y el lote.
- **Con errores:** escribe los fallos en `validacion_tecnica_<malla>` (Silver), marca `estado_validacion = FAILED` y sigue; el código lo deja explícito: *"Validación técnica NO detiene el flujo… El flujo continuará hacia remediación"*.
- **Sin errores:** **borra** la carpeta de errores anteriores de esa malla (`limpiar_prefijo_s3`) y marca `SUCCESS`.

### 3.3 `niif-dev-remediacion-estandar`
- **Entrada:** `--RUTA_PARAMETROS_EXTRACCION`, `--RUTA_ORIGEN_REMEDIACION` (la validación técnica) y `--RUTA_DESTINO_REMEDIACION`.
- **Qué hace:** toma los datasets extraídos y los **disponibiliza en Silver**: copia los lotes normales y, donde hubo hallazgos, trata los lotes aparte (inferido; el detalle de las reglas de remediación vive en el JSON de configuración). Escribe en `alfa-datalake-dev-silver/niif/remediacion/<malla>/lote_cargue_id=N/`.
- **Nota:** las ejecuciones guardan una copia histórica por lote (`ruta_remediacion_historica`).

### 3.4 `niif-dev-silver-<malla>` (6 jobs)
- **Entrada:** `--RUTA_PARAMETROS_EXTRACCION` y `--S3_SLV_PREP_PATH` (destino). Reserva matemática agrega `--PER_CORTE` (periodo de corte) y `--S3_TMP_PREP_PATH`. Auxilios Funerarios agrega `--FEC_DESDE` y `--FEC_HASTA`.
- **Qué hace:** une y transforma los datasets disponibilizados en la tabla **`silver_preparado_<malla>`**, la que consume Gold. Exige remediación en `SUCCESS`. Si la entrada funcional viene vacía, escribe una salida vacía con el esquema correcto.
- **Por malla:**

| Job | Líneas | Entradas (datasets) | Lo particular |
|---|---|---|---|
| `silver-primas-rentas` | 659 | 7 datasets: movimientos de pago, distribución de vigencia, pensiones, rentas (`prv_rentas`), saldos de matemática, orígenes y conceptos | Cruza los 7 en una sola salida |
| `silver-primas-endosos` | 556 | (inferido) endosos del previsional | — |
| `silver-primas-arl` | 548 | (inferido) tablas de ARL (`arpprod`) | — |
| `silver-auxilios-funerarios` | 513 | (inferido) tablas de auxilio funerario | Recibe `FEC_DESDE` / `FEC_HASTA` desde la Step Function |
| `silver-reserva-matematica-arp` | 1 159 | (inferido) cálculos actuariales y siniestros ARP | Salida de **440 columnas**; deja una tabla temporal `tmp_preparacion_` de 494 |
| `silver-reserva-matematica-rvi` | 1 247 | Rentas vitalicias | Transcribe SQL actuarial por pasos (`rm_rvi_0` a `rm_rvi_13`, `FIRST_VALUE`, pivotes de beneficiarios); salida de **458 columnas**; tabla temporal de 511; sobrescribe la salida por `PER_CORTE` |

### 3.5 `niif-dev-scd` (Gold)
- **Entrada:** `--RUTA_CONFIG_SCD` y `--RUTA_PARAMETROS_EXTRACCION` (de donde toma `ruta_silver_preparada`).
- **Qué hace:** consolida Silver en **Gold como dimensión de cambio lento (SCD)** en `alfa-datalake-dev-gold/niif/scd_<malla>/`, conservando la procedencia (`LOTE_CARGUE_ID`) de cada registro. Si no hay datos, **inicializa un Gold vacío** con el esquema. Puede marcarse `SKIPPED` con un motivo.
- **Nota:** 12 conteos y 6 `cache`; escribe en un solo archivo (`coalesce(1)`) en la inicialización.

### 3.6 `niif-dev-certification`
- **Entrada:** `--PERIOD`, `--LOTE_CARGUE_ID`, `--CONFIG_BUCKET`, `--CONFIG_KEY` (JSON del caso), `--GLOBAL_CONFIG_KEY`, `--ENVIRONMENT` y, si aplica, `--REGENERACION_SIN_DATOS_NUEVOS`. La Step Function los toma del resultado de la lambda `composite-notification-evaluator`.
- **Qué hace:** (inferido) no usa Spark para el trabajo real: no hay lecturas ni escrituras de DataFrames. Ejecuta con **Athena** las consultas del caso (`summary`, `total`, `detail`) contra Gold, descarga los resultados como CSV (`UNLOAD`) al bucket de reportes, genera un **`certCode`** y escribe filas en `niif-dev-notification-certification` (`PROCESSED`). Luego las pasa a `PENDING` cuando el `certCode` ya es visible en el índice `certCode-index` (hasta 6 intentos de 2 s).
- **Casos especiales:** si una consulta no devuelve filas, lo confirma con una segunda consulta y lo marca `sinDatosPeriodo`. Con `REGENERACION_SIN_DATOS_NUEVOS` vuelve a generar los archivos desde el Gold existente.

### 3.7 `niif-dev-notification-certification`
- **Entrada:** `useCaseId`, `certCode`, `emailType`, `CONFIG_BUCKET`, `CONFIG_KEY`, `GLOBAL_CONFIG_KEY`, `SNS_TOPIC_ARN`, `REDIRECT_BASE_URL` (API Gateway `/r`) y `SHORT_LINKS_TABLE`.
- **Qué hace:** lee las filas del `certCode`, arma las tablas de resumen y totales con Athena, crea los **enlaces cortos** (Certificar, No certificar y descarga de cada CSV) y **publica por SNS** un mensaje de **texto plano** por destinatario, con `FilterPolicy` por `recipient`. Marca las filas `SENT` y cierra el grupo compuesto (si lo hay) con una transacción condicional.
- **Salidas:** SNS `niif-dev-certification-email`, enlaces en `niif-dev-short-links` y estado `SENT` en `notification-certification`.

---

## 4. Los 3 crawlers

| Crawler | Recorre | Base del catálogo | Política |
|---|---|---|---|
| `niif-dev-crawler-bronze` | `s3://alfa-datalake-dev-bronce/niif/` | `niif-dev-bronze-catalog-db` | Rastrea **todo** cada vez (`CRAWL_EVERYTHING`), combina esquemas compatibles, nivel de tabla 5, índice de particiones |
| `niif-dev-crawler-silver` | `s3://alfa-datalake-dev-silver/niif/` | `niif-dev-silver-catalog-db` | Ídem, nivel 5 |
| `niif-dev-crawler-gold` | `s3://alfa-datalake-dev-gold/niif/` | `niif-dev-gold-catalog-db` | Ídem, nivel 3 |

- **Son compartidos:** los 6 pipelines usan los mismos 3 crawlers, y cada uno recorre la capa completa, no solo su malla.
- **Sin programación propia:** solo los arranca la Step Function.
- **Esquema:** `UPDATE_IN_DATABASE` y `DEPRECATE_IN_DATABASE`: si desaparece una carpeta, el crawler quita la tabla del catálogo.
- **Para qué sirven:** los jobs de Spark leen por ruta de S3, así que el catálogo importa sobre todo para **Athena**: la certificación consulta las tablas de Gold.

---

## 5. Código que se repite entre jobs

Los jobs no comparten librería: cada uno trae su propia copia de las mismas funciones.

| Función | En cuántos jobs |
|---|---|
| `cargar_json_s3` | 11 |
| `guardar_json_s3`, `ensure_trailing_slash` | 10 |
| `normalizar_tabla_relativa` | 7 |
| `obtener_segmento_preparacion`, `obtener_lotes_preparacion`, `leer_silver_base` | 6 |
| `parse_s3_uri` | 4 |

Ninguno tiene pruebas automáticas.
