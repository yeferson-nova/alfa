# Auditoría del pipeline de datos (Bronce, Plata, Oro y certificación) contrastada con la arquitectura

**Fecha:** 2026-11-05. **Alcance:** los 12 jobs de Glue de `Backend\Glue\niif-dev-*`, las 6 Step Functions `niif-dev-<malla>`, los 3 crawlers y el catálogo, las reglas de EventBridge y el job de notificación. Las lambdas tienen su propia auditoría: [auditoria-lambdas.md](auditoria-lambdas.md). Queda fuera la lambda unificada del spike de la nueva arquitectura (`t2 unificadas`, `spike-primas-rentas`).

**Fuente de verdad de la arquitectura:** `Docs\05-arquitectura\diagramas\NIIF17_ModeloSolucion_V1.0.drawio`, hojas `Página-6` (detalle de zonas), `Página-2` y `Página-9` (certificación y transporte), `Página-1` (infraestructura) y `Página-3`/`Página-5` (programación).

**Método.** Se leyó el código completo de SCD, validación y remediación, y las partes críticas de extracción, certificación, notificación y los 6 Silver. Cada hallazgo cita archivo y línea. Se ejecutaron las pruebas que existen (18 del freno de SCD y 451 del job de notificación nuevo: todas pasan). **No se vio** ninguna ejecución real, ningún JSON de configuración de `niif-dev-assets` (salvo la evidencia de ARL en `errores\`), ninguna consulta de certificación ni las políticas IAM. Lo que depende de eso va marcado **hipótesis**.

Los hallazgos ya documentados en [revision-pre-produccion.md](revision-pre-produccion.md) (PP-xx) se citan, no se repiten. Aquí se confirman contra el código, se corrigen o se amplían.

---

## 1. Veredicto

El pipeline **no es confiable para cifras regulatorias en su estado actual**. Las piezas de control existen en el papel (estados, checkpoints, reconciliación de lotes), pero hay cinco puntos donde un dato malo, incompleto o vacío avanza hasta el correo de certificación sin que nada lo detenga, y dos puntos donde Gold puede perder su historial.

| Severidad | Cantidad | IDs |
|---|---|---|
| Crítica | 6 | ARQ-01, ARQ-03, ARQ-04, ARQ-05, ARQ-06, CUM-01 |
| Alta | 10 | ARQ-02, ARQ-07 a ARQ-10, SEG-01 a SEG-03 (ver 9.2), COS-01, COS-02 |
| Media | 8 | ARQ-11 a ARQ-14, SEG-04, COS-03 a COS-05 |

---

## 2. Contraste con la arquitectura (diagrama TOBE)

| Componente del diagrama | Qué hay en el código | Estado |
|---|---|---|
| Modelo de programación (DynamoDB, Stream, Pipe, EventBridge, Lambda) | Stream de `prg-programaciones` → `manager-configuration` → Scheduler → configurador por malla. No hay Pipe. Hay dos programadores activos (PP-03). | Diverge |
| Modelo de configuración de extracción | JSON por malla en `niif-dev-assets`, publicado por el configurador. | Cumple, con estado mutable (ARQ-08) |
| Job Extracción (Bronce) | `extraccion_estandar_optimized.py`. JDBC a Oracle por conexión de Glue. | Cumple |
| **Job Consolidación (Bronce)** | No existe como job. La consolidación (append, exclusión de llaves, reutilización de snapshots) está dentro de la extracción. | Falta |
| Rastreador y Catálogo Bronce | 1 crawler para todas las mallas sobre `s3://…-bronce/niif/` | Cumple con riesgo de concurrencia (ARQ-09) |
| **Job Validación (Plata)** | Lee de Bronce, no de Plata; solo revisa columna por columna; **nunca detiene** (ARQ-03) | Cumple en forma, no en función |
| **Job Remediación (Plata)** | Copia Bronce → Silver base y guarda los errores en histórico. **No corrige ni aparta nada** (ARQ-03) | Nombre no corresponde a la función |
| Job Transformación (Plata) | 6 jobs `silver-<malla>` (preparada) | Cumple, con pérdida silenciosa de filas (ARQ-05) |
| **Job Consolidación (Plata)** | No existe. Silver preparada se sobrescribe completa en cada corrida. | Falta |
| Job Consolidación (Oro) | `scd.py` (SCD tipo 2) | Cumple, con defectos graves (ARQ-01, 02, 06, 07) |
| Rastreadores y Catálogo Plata y Oro | 1 crawler por capa, `CRAWL_EVERYTHING`, `UPDATE_IN_DATABASE` | Cumple con ruido (ARQ-09, COS-03) |
| Zona Aterrizaje / Zona Recursos | `niif-dev-assets` hace de recursos, configuración, **estado** y resultados de Athena a la vez | Mezcla de funciones (SEG-03) |
| Orquestación (Malla) | 6 máquinas de 40 estados, idénticas en estructura | Cumple, copiadas (ARQ-10) |
| Modelo de notificaciones (Job Notificación, Email) | Job `notification-certification` por SNS (texto plano). Reescritura a SES hecha, no desplegada. Los disparadores de stream heredados están **Disabled**. | Parcial |
| Zona Certificación (Athena IDE) | `niif-dev-certification` (Glue) arma los CSV; Athena sin workgroup (SEG-02) | Cumple con brecha |
| API Gateway, Cognito, Entra ID | Lambda de certificación lee los *claims* de Cognito y valida al aprobador | Cumple (ver auditoría de lambdas) |
| Zona Transporte Salida → Lambda SFTP → FIS | `EnvioSftpFis`, `send-via-sftp` | Cumple con brechas (ver auditoría de lambdas) |
| Modelo de trazabilidad (SonarQube, CloudWatch, EventBridge) | Sonar no se aplica a los jobs; logs de Step Functions apagados; reglas de cierre solo para ARL (ARQ-12) | Diverge |
| VPC `10.156.220.0/23`, subred privada | Los jobs usan la conexión de Glue. Las lambdas de configuración, redirección y certificación **no** tienen VPC; solo las dos de SFTP. | Diverge (ver auditoría de lambdas) |

**Flujo real de una malla** (idéntico en las 6): `ExtraccionEstandar` → `LeerParametros` → `EvaluarModoEjecucion`/`EvaluarDatosNuevos` → `CrawlerBronze` → `ValidacionTecnica` → `Remediacion` → `SilverJob` → `CrawlerSilver` → `GoldSCD` → `CrawlerGold` → `RegistrarPipelineCompleto` → `ResolverNotificacion` → `DispararNotificacionCertificacion`. Ningún estado intermedio tiene una decisión (`Choice`) sobre calidad, vacío o cuadre: solo sobre «hay datos nuevos» y «el crawler terminó».

---

## 3. Hallazgos críticos

### ARQ-01. SCD reemplaza Gold completo si la lectura de Gold falla por cualquier motivo
`scd.py:312-323` envuelve la lectura de Gold en `except Exception` y, ante cualquier error, concluye «primera ejecución». La rama de primera ejecución (`scd.py:328-347`) escribe la entrada con `mode("overwrite")` sobre `ruta_gold`.
**Escenario:** un error transitorio de S3, un permiso retirado, un archivo Parquet corrupto o un cambio de esquema hace fallar `spark.read.parquet(ruta_gold)`. El job marca éxito, reemplaza Gold con solo la foto de Silver y **se pierde todo el historial SCD** (versiones cerradas, fechas de vigencia). Contrasta con `inicializar_gold_vacio` (`scd.py:72-104`), que sí distingue «no existe» de «no se puede leer».
**Corrección:** distinguir ausencia (`filesystem.exists`) de error de lectura; ante error, fallar. Prohibir el overwrite de Gold salvo bandera explícita de inicialización.

### ARQ-02. Gold se reescribe sobre la misma ruta que se lee, sin atomicidad (alta tras la evidencia de la cuenta)
`scd.py:495-500` hace `df_final.write.mode("overwrite")` sobre `ruta_gold`, la misma ruta de `df_gold` (`scd.py:313`). Spark lo tolera solo porque los DataFrames están en caché (`.cache()` en 313, 363, 364, 486), y el caché puede perderse si un ejecutor cae. Overwrite en S3 borra y luego escribe: si el job muere en medio, Gold queda vacío o parcial y no hay versión anterior que restaurar. **Actualización con la cuenta (sección 9):** el bucket de Gold sí tiene versionado habilitado, así que el historial se puede recuperar desde la versión anterior; el riesgo baja de crítico a alto, pero la reescritura sigue sin ser atómica.
**Corrección:** escribir a una ruta nueva por ejecución (`…/scd/v=<ejecucion_id>/`), verificar el cuadre y recién entonces publicar el puntero; o Delta/Iceberg con transacciones.

### ARQ-03. La validación no bloquea y la remediación no remedia
Confirma PP-01 y FLU-12 contra el código y agrega:
- `validacion_tecnica_detiene_flujo` se escribe fijo en `False` con y sin errores (`validacion_tecnica.py:776` y `:798`) y se repite en la remediación (`remediacion_estandar.py:648`). SCD acepta `FAILED` y sigue (`scd.py:197-201`).
- La «remediación» copia Bronce → Silver base (`remediacion_estandar.py:474-480`) y copia los errores a un histórico con estado `PENDIENTE_ANALITICA` (`:561`). **Nada lee ese estado**: no hay aprobación, cuarentena ni corrección. Las filas con error siguen a Silver y Gold. Los argumentos `RUTA_CONFIG_REMEDIACION` que pasan ARP y RVI **no los lee el script** (`remediacion_estandar.py:213-221` solo resuelve 4 argumentos).
- Una tabla sin columna `LOTE_CARGUE_ID` **se omite sin error** (`validacion_tecnica.py:550-552`): se queda sin validar.
- Si la lectura de Bronce falla por cualquier causa (permisos, corrupción), el `except Exception` (`:532-544`) lo registra como «SIN_FILAS» y continúa.
- Las reglas son solo tipo, nulo, longitud y precisión por columna (`:619-717`). No hay unicidad de llave, rangos, cuadre contra la fuente, coherencia entre campos ni totales de control.
**Corrección:** reglas declaradas con severidad (HU-TR-02); una regla bloqueante detiene la máquina de estados (`Choice` + `Fail`); cuarentena de filas; cuadre `entra = sale + cuarentena` (HU-TR-04).

### ARQ-04. Una entrada vacía, parcial o vieja llega hasta el correo de certificación
Hay cuatro caminos verificados:
1. **Silver sin guarda de vacío.** Solo ARP (`…arp.py:1091`) y RVI (`…rvi.py:1172`) fallan si el resultado tiene 0 filas. Rentas (`silver-primas-rentas.py:559`), Endosos (`:474`), ARL (`:462`) y Auxilios (`:447`) cuentan y escriben `overwrite` aunque sea 0. Rentas solo valida que las **entradas** no estén vacías (`:379-407`), no la salida de los joins.
2. **SCD con entrada vacía** marca `SKIPPED`, conserva Gold y termina bien (`scd.py:277-306`). La Step Function no revisa `estado_gold_scd`: sigue a `CrawlerGold` y certifica con Gold viejo.
3. **Extracción con 0 filas** no sobrescribe Bronce y marca `datos_nuevos=False` (`extraccion_estandar_optimized.py:1422-1434, 1496-1532`). El flujo va a `SuccessSinDatosNuevos` → `ResolverRegeneracion` → `CertificarRegeneracion` y **certifica de nuevo con datos anteriores**. Si el 0 se debe a un `WHERE` mal resuelto o a la fuente, queda indistinguible de «no hubo cambios».
4. **Incremental + SCD que apaga por ausencia**: es el incidente de [Primas ARL 202606](../../MemoriaIa/brechas/inprod-primas-arl-202606.md); el diseño sigue permitiéndolo (ver ARQ-06 y ARQ-07).
**Corrección:** guarda `total_salida > 0` (o contra un mínimo histórico) en los 6 Silver; `Choice` sobre `estado_*` en la Step Function; distinguir «sin cambios confirmados» de «cero filas» en la extracción.

### ARQ-05. Silver pierde filas y montos sin avisar
- **Inner joins** sin cuadre entre la tabla de hechos y el resultado: Auxilios (6, `silver_aux_funerarios.py:392-421`), Endosos (3, `:359-372`), Rentas (6, `:451-482`), ARP (6 o más, `:402-418`), RVI (`:343-374`). Una fila sin equivalencia en una tabla de apoyo (`equiv_tipos`, `cgr_origenes`, `prv_rentas_notas`) desaparece del Silver y de la certificación.
- **`dropDuplicates` por llave sin orden:** ARL (`:459`), ARP (`:1088`), RVI (`:1166`). Si dos filas comparten llave con valores distintos, Spark conserva una **arbitraria**: el monto puede cambiar entre corridas, y SCD lo interpreta como un cambio real (versión nueva falsa).
- Valores quemados en cálculo de reserva: radicado `2011028468000000` (ARP `:493`), corte `20200110` (RVI `:462`).
- ARL castea la identificación a `LongType` (`:455`): NIT con dígito de verificación, pasaportes o ceros a la izquierda se pierden o quedan nulos.
**Corrección:** cuadre hechos-entran vs salida-más-descartados con tolerancia cero para montos; `left` join + regla de calidad sobre lo no encontrado; desempate determinista (`row_number` con orden) o fallo si hay duplicados; parametrizar constantes.

### ARQ-06. El freno de apagado masivo NO está desplegado y no se puede desplegar con la configuración actual
`scd.py:34-42` exige `UMBRAL_APAGADO_PCT`. Ni `niif-dev-scd/job.json` (`DefaultArguments` sin ese valor) ni los `GoldSCD` de las 6 Step Functions (solo pasan `RUTA_CONFIG_SCD` y `RUTA_PARAMETROS_EXTRACCION`) lo suministran: **al desplegar este script, `getResolvedOptions` falla en toda ejecución**. Además la copia en `estructura-nueva\…\scd\src\scd.py` es la versión **sin** freno: si el repo nuevo se arma desde ahí, el control del incidente se pierde. El freno tampoco cubre cierres masivos por cambio de hash ni llaves nulas o duplicadas.
**Actualización con la cuenta:** el script desplegado de `niif-dev-scd` tiene el mismo hash que `estructura-nueva\…\scd.py`, es decir, **sin freno**. El control del incidente de ARL no existe hoy en DEV, y ninguno de los 6 `config_scd_*.json` define `umbral_apagado_pct`.
**Corrección:** valor por defecto explícito y obligatorio en el `job.json`; una sola copia del script; ampliar el freno a cierres por cambio.

### CUM-01. El repositorio contiene evidencia de producción, correos reales y el ID de cuenta
El skill del proyecto declara que el repo es **público**; no pude comprobar su visibilidad (`gh` no está instalado). Versionados en Git están:
- `errores\evidencia-primas-arl-202606\` (ejecuciones de `niif-prd-*`, configuración real de PRD, consultas de Athena).
- Correos de personas en `Backend\SNS\*.subs.json` y en `Backend\htmlNew\`.
- El ID de cuenta `533267035849` en 330 archivos.
Ley 1581 (datos personales) y los lineamientos del banco exigen que esto no esté en un repositorio con acceso amplio.
**Corrección:** confirmar visibilidad hoy; si es público, pasarlo a privado, rotar lo expuesto y limpiar el historial (`git filter-repo`); mover la evidencia a un almacén controlado.

---

## 4. Hallazgos altos

### ARQ-07. SCD: llave, hash y detección de nulos frágiles
- No valida unicidad de llave en la entrada: con duplicados, el `full join` multiplica filas (`scd.py:377`).
- Las filas «nuevas» y «apagadas» se detectan con `col(llave_ref).isNull()` sobre **la primera columna llave** (`:385`, `:398`). Si esa llave puede ser nula, una fila existente se trata como nueva en cada corrida (el `join` nunca empareja nulos) y se duplica.
- El hash convierte todo a texto y trata `NULL` igual que `''` (`:266`): un campo que pasa de nulo a vacío no genera versión. Un decimal con otra escala (`10.5` y `10.50`) sí genera una falsa. El separador `||` no se escapa.
- `FEC_INI_VIG_REG` es `current_timestamp()` del momento de la corrida (UTC), no la fecha de negocio (`:272`). Reprocesar un periodo cierra versiones con la fecha de hoy y rompe la reproducibilidad.
- No hay verificación posterior: `activos_final = entrada` y `filas_final = históricos + …` no se comprueban.

### ARQ-08. El estado del pipeline es un archivo único, mutable y sin `ejecucion_id`
`parametros_extraccion.json` (una ruta fija por malla en `niif-dev-assets`) lo leen y reescriben completo los 5 jobs (`guardar_json_s3` en extracción tras **cada tabla**, `:1326`; en validación, remediación, Silver y SCD). Dos ejecuciones se pisan; un reproceso sobrescribe la evidencia de la corrida anterior; una escritura de un job puede borrar campos de otro. Es PP-04/FLU-02, ahora confirmado: además guarda `str(e)` del error de Oracle (`:1706`), que puede incluir URL y usuario de la conexión.

### ARQ-09. Concurrencia entre mallas
- `niif-dev-remediacion-estandar` tiene `MaxConcurrentRuns=1`, y en la Step Function su `Retry` solo cubre `Glue.AWSGlueException`, no `Glue.ConcurrentRunsExceededException` (que sí figura en el paso de certificación). Dos mallas que lleguen a la vez: la segunda cae en `FailState`.
- Los 3 crawlers se comparten. Al capturar `CrawlerRunningException` la máquina espera y acepta el `LastCrawl.Status` de **cualquier** rastreo en curso, incluso uno iniciado antes de que esta malla escribiera sus datos. El catálogo puede quedar sin los datos nuevos y la máquina avanza (hipótesis lógica sobre la definición ASL; falta reproducirlo).

### ARQ-10. Seis máquinas copiadas, con argumentos distintos y no promovibles
Las 6 definiciones tienen los **mismos 40 estados**. Los de la sección PP-19 se matizan: el *drift* está en los argumentos, no en la estructura:
- ARL pasa `S3_SLV_PREP_PATH=…/silver_preparado_primas-arl/` (con guion) mientras todas las demás mallas y el `job.json` usan guion bajo.
- Auxilios pasa `FEC_DESDE/FEC_HASTA` de la entrada; ARL, ARP y RVI pasan `PER_CORTE`; Endosos y Rentas no pasan periodo y el job lo lee del JSON de estado.
- `ambiente`, ARNs, buckets y la tabla de estado están quemados con `dev`. Pasar a QA/PRD exige editar 6 archivos de 1 500 líneas. No hay `TimeoutSeconds` en ningún estado y el *logging* está en `OFF`.

### SEG-01. Datos personales en todas las capas y en los errores
Extracción hace `SELECT *` (`extraccion_estandar_optimized.py:296`) sin minimización. La validación guarda el **valor incorrecto en crudo** (`valor_incorrecto`, `validacion_tecnica.py:394-399`) en Silver, sin enmascarar: identificaciones, nombres o fechas de nacimiento pueden quedar en una tabla de errores consultable. ARL lleva `NOM_TERCERO` e identificación hasta Gold. Los jobs no tienen `SecurityConfiguration` (cifrado de logs, *bookmarks* y salida de Glue) en ninguno de los 12 `job.json`. El `WHERE` de cada tabla se escribe en el log (`:1360`).

### SEG-02. Athena sin workgroup, sin tope y con espera infinita
Confirma PP-06: `start_query_execution` no recibe `WorkGroup` (`niif_certification.py:490-499`) y los resultados van al bucket de *assets*. Se agrega: la espera es `while True` con `sleep(2)` y **sin tiempo máximo** (`:505-515`); un cuelgue consume hasta las 48 h del timeout del job.

### SEG-03. La configuración, el estado y los resultados comparten bucket
`niif-dev-assets` guarda JSON de configuración que determina el SQL a Oracle, el estado del pipeline, los resultados de Athena y los fragmentos de UNLOAD. Quien tenga escritura ahí controla qué se extrae y qué se certifica. El `WHERE` de la configuración se concatena en la consulta (`extraccion_estandar_optimized.py:288-298`, `:1342-1359`) sin validación. **Hipótesis:** los permisos de cada rol sobre ese bucket no se vieron; solo se puede afirmar que el diseño no separa configuración de estado.

### COS-01. Timeouts de 48 h y capacidad uniforme
Los 12 jobs: `Timeout=2880`, `G.1X`, 4 workers, `MaxRetries=0`, Glue 4.0 y sin escalado automático. Los Silver de reserva matemática (440-460 columnas) y los de primas (14-27) tienen la misma capacidad. Confirma PP-14 y FLU-04.

### COS-02. Validación: una pasada por regla, dos veces
`df.unpersist()` se ejecuta antes de evaluar la unión de fallos (`validacion_tecnica.py:740` frente a `:750` y `:765`). Cada regla (columnas × 3 o 4 reglas) vuelve a leer Bronce desde S3, y el conteo y la escritura repiten todo: el costo es `columnas × reglas × 2` lecturas. Refina PP-22. Una sola pasada con agregados condicionales lo reduce a una.

---

## 5. Hallazgos medios

| ID | Hallazgo | Evidencia |
|---|---|---|
| ARQ-11 | **Reproducibilidad.** `LOTE_CARGUE_ID` es la hora en segundos (`ext:1041`); `NRO_REGISTRO` es `monotonically_increasing_id` (`:1479-1483`) y se usa como `num_fila` en la tabla de errores (`val:376-381`): no identifica la fila entre corridas. Bronce se sobrescribe por defecto (`:1018`); no se lee de Oracle con una foto consistente entre tablas (sin SCN). | Hipótesis para la foto consistente |
| ARQ-12 | **Cierre de ejecución.** En `Backend\Eventos` solo hay regla `finalizar-ejecucion` para `primas-arl`. Las otras 5 mallas quedarían sin el cierre de trazabilidad (confirmar en la cuenta; el inventario puede estar incompleto). | `Eventos\default.niif-dev-finalizar-ejecucion-primas-arl.regla.json` |
| ARQ-13 | **Fechas y zonas.** Los jobs usan `datetime.now()` sin zona (UTC en Glue) para `fecha_proceso`, `fecha_preparacion` y particiones; la notificación usa `America/Bogota`. Rentas filtra el mes por `FEC_INICIAL` pero Silver es «foto completa» para SCD: si la llave de SCD no incluye el periodo, cada mes apaga el anterior (hipótesis; falta ver `config_scd_primas_rentas.json`). | `silver-primas-rentas.py:413-430`, `scd.py:244-251` |
| ARQ-14 | **Calidad de código.** Los 12 jobs son scripts de 500-1 900 líneas sin funciones testeables. Hay 18 pruebas en total para SCD (solo el freno, cargado por `ast`). El repo de Rentas con Jenkinsfile tiene Sonar apuntando a `**/*.json` (no analiza Python), `sonar.coverage.exclusions=src/**.py`, `--cov-fail-under=0`, `sonar.tests` comentado y un `tox.init` duplicado. Incumple el estándar propio (hexagonal, cobertura > 80 %) y GTC-GDD-003. Solo el job de notificación nuevo lo cumple (451 pruebas, `fail_under = 90`, prueba de arquitectura). | `data-datalake-niif-primas-rentas-glue-job\` |
| SEG-04 | **Certificación.** Parche global de `s3.put_object` para inyectar encabezados (`niif_certification.py:88, 212-235`); `job.commit()` también en el camino de error (`:789-792`); lista de carpetas autorizadas quemada (`:581`); *defaults* de otro caso (`PERIOD=202411`, PP-18); sin cuadre entre el CSV y Gold (filas y suma de montos); el CSV completo se arma en memoria (`:601-612`, riesgo de OOM en reservas de 440 columnas); posible inyección de fórmulas al abrir en Excel. | `niif_certification.py` |
| COS-03 | **Crawlers.** 1 por capa sobre todo `niif/` con `CRAWL_EVERYTHING`, ejecutado por cada malla (6 × 3): 94 tablas en Silver (7 por lote y 2 `tmp_`). `UPDATE_IN_DATABASE` en Gold propaga cambios de esquema sin revisión. PP-16/PP-17/FLU-09 confirmados en `crawlers\`. | `Backend\Glue\crawlers\` |
| COS-04 | **SCD recalcula el `full join` 6 veces** (5 `count()` y la escritura, `scd.py:454-458, 495`) porque el `join` no se cachea, y reescribe todo el histórico sin particionar en cada corrida. El costo crece con la historia. | `scd.py` |
| COS-05 | **Extracción** sin lectura paralela (`:1344-1366`, sin `partitionColumn`/`numPartitions`), con `ReadTimeout` de 30 s (`:1349`) que puede cortar lecturas lentas, y con la fuente leída dos veces (conteo y escritura) si `persistir_dataframe` está apagado. | `extraccion_estandar_optimized.py` |

---

## 6. Qué está bien y se conserva

Hallado en el código, no supuesto:
- **Inicialización segura de Gold vacío:** `errorifexists`, comprueba legibilidad y esquema (`scd.py:72-104`).
- **Reconciliación de lotes** entre Bronce y Silver antes de copiar, con error si faltan (`remediacion_estandar.py:337-394`; `validacion_tecnica.py:559-586`).
- **Montos en `DECIMAL(17,2)`** en los Silver de primas y rentas; el catálogo de las tres capas no contiene ninguna columna `double` ni `float` (458, 956 y 225 columnas decimales en Bronce, Plata y Oro).
- **Endosos** falla si una referencia no es única (`silver_primas_endosos.py:461-464`).
- **Resultado vacío de UNLOAD** se confirma con una consulta de comprobación antes de crear un CSV vacío (`niif_certification.py:799-847`).
- **Reanudación** con checkpoint por tabla, segmento y ciclo en la extracción.
- **Freno de apagado masivo** diseñado y probado (18 pruebas), pendiente de ARQ-06.
- **Job de notificación nuevo:** arquitectura hexagonal, 451 pruebas que pasan, cobertura exigida de 90 %.
- **Cierre condicional y bloqueo transaccional** en la notificación compuesta.

---

## 7. Orden recomendado

| Fase | Qué | IDs |
|---|---|---|
| 0. Hoy | Confirmar visibilidad del repo; si es público, cerrarlo. Respaldar Gold (versionado o copia). | CUM-01, ARQ-02 |
| 1. Proteger Gold | Corregir el `except` de SCD; desplegar el freno con su argumento; una sola copia de `scd.py`; guarda de vacío en los 6 Silver; `Choice` sobre estados. | ARQ-01, ARQ-04, ARQ-06 |
| 2. Confiar en la cifra | Compuerta de calidad bloqueante, cuadre entre capas, joins y duplicados deterministas. Requiere reglas del negocio. | ARQ-03, ARQ-05, ARQ-07 |
| 3. Aislar ejecuciones | `ejecucion_id` en rutas de estado, escritura por versión en Gold, concurrencia coherente. | ARQ-02, ARQ-08, ARQ-09 |
| 4. Plantilla y promoción | Una sola Step Function parametrizada, timeouts, logs, cierre de ejecución para las 6 mallas. | ARQ-10, ARQ-12, COS-01 |
| 5. Seguridad de datos | Minimización y enmascarado, `SecurityConfiguration`, workgroup de Athena. | SEG-01 a SEG-03 |
| 6. Costo y calidad de código | Una pasada de validación, SCD cacheado, crawlers por malla, pruebas y Sonar real. | COS-02 a COS-05, ARQ-14 |

---

## 8. Lo que no se pudo verificar (lista original; la sección 9 indica qué se cerró)

1. Los JSON de `niif-dev-assets` (`config_scd_*`, `config_val_tec_*`, `config_extraccion_*`): determinan llaves del SCD, reglas de validación y `WHERE`. Solo se vio el de ARL en `errores\`.
2. Las consultas de certificación y el cuadre de totales que hagan: viven en la configuración, no en el repo.
3. Políticas IAM de cada rol de job; versionado y cifrado de los buckets de Bronce, Plata y Oro.
4. Que el script de `niif-dev-scd` desplegado en S3 sea el mismo que está en el repo.
5. Tiempos, DPU-horas y bytes de Athena reales: todo lo de costo es por inspección de código.
6. La visibilidad del repositorio en GitHub.

---

## 9. Contraste con la evidencia de la cuenta DEV (`errores\aud-datos\`)

Recolectada con `Backend\cloudshell-inventario\auditoria-faltantes.sh` (solo lectura, 279 archivos). Los únicos errores del script fueron «no existe política de ciclo de vida o de bucket», esperables.

### 9.1 Confirmado por la evidencia

| Hallazgo | Evidencia real |
|---|---|
| ARQ-03 la validación no bloquea | Las **6 mallas** terminaron su última ejecución con `estado_validacion=FAILED` (11, 13, 19, 25, 91 y 100 errores). Cinco llegaron a `estado_gold_scd=SUCCESS`; RVI no llegó a Gold. Los datos con error ya están en Gold. |
| ARQ-06 el freno no está | El script de `niif-dev-scd` desplegado tiene el mismo hash que la copia de `estructura-nueva` (sin freno). Los 6 `config_scd_*` no definen `umbral_apagado_pct`. |
| ARQ-12 cierre de ejecución solo para ARL | Reglas `niif-dev*` en EventBridge: solo `finalizar-ejecucion-primas-arl` y una de prueba de SES. |
| Repo = desplegado | 11 de 12 scripts tienen el **mismo hash** que el repo; solo difiere `scd` (el repo trae el freno sin desplegar). Cierra el punto 4 de la sección 8. |
| ARQ-02, lectura mientras se sobrescribe | Una corrida de RVI falló con `o6505.count. File not present on S3`, síntoma de leer una ruta que se está reescribiendo. |
| SEG-02 Athena | Único workgroup `primary`, sin `EnforceWorkGroupConfiguration`, sin métricas ni tope de bytes. Resultados en `alfa-datalake-dev-assets/athena/`. |

### 9.2 Corregido o matizado

| Hallazgo | Qué cambió |
|---|---|
| ARQ-02 versionado | Bronce, Plata, Oro, `niif-dev-assets`, `niif-dev-certification` y `niif-dev-transport` tienen **versionado, cifrado KMS y bloqueo de acceso público**. Gold es recuperable. El bucket `alfa-datalake-dev-niif-transport` no tiene versionado y usa AES256: es inconsistente con el resto. |
| SEG-03 roles | La hipótesis de roles amplios se **refuta en gran parte**: cada rol de job está acotado por carpeta y malla (el de SCD solo escribe en sus `scd_<malla>/`). Comodines solo en servicios que lo exigen. **Excepciones:** el rol del job de notificación tiene `sns:Publish` y `SetSubscriptionAttributes` sobre `*` (cualquier tópico, incluidos los de alertas críticas); el de Silver de ARL escribe y borra en todo `silver/niif/*`. |
| COS-01 timeouts y Athena | En DEV las corridas duran de 46 s a 5 min (mediana) y como máximo 1,8 DPU-horas (extracción). Las últimas 50 consultas de Athena escanearon 0,38 GB en total y 9 fallaron. El costo actual en DEV es bajo; el riesgo es de control y de escala en PRD. |
| PP-03 dos programadores | `niif-dev-programador-configuraciones` está **Disabled**: solo `manager-configuration` escucha `prg-programaciones`. Resuelto en DEV; confirmar QA y PRD. |
| Fallos reales | En las últimas 10 corridas fallaron: SCD 1, Auxilios 3, ARL 2, Rentas 2, RVI 3. Causas: `AccessDenied` por rutas fuera de la política del rol (SCD, Auxilios, ARL), script inexistente en S3 (ARL), Silver base vacío (Rentas) y 0 registros (RVI). Las guardas de entrada sí funcionan donde existen; los permisos por malla se rompen en cuanto cambia una ruta. |

### 9.3 Nuevo, a partir de la configuración

- **ARQ-13 (se precisa).** Las llaves de SCD son `NID_RENTA` (Rentas y Endosos), `NID_POLIZA + NID_ENDOSO` (ARL) y, en reserva matemática, **incluyen el corte** (`PER_CORTE`, `PER_PAGO`). El Silver de ARP escribe en una carpeta por corte (`…/corte_202512/`) y SCD lee solo esa. Con el siguiente corte, todos los activos del corte anterior estarán «ausentes»: se apagarán, o el freno, si se despliega, detendrá la ejecución todos los meses. Hay que decidir cuál es el comportamiento esperado. Se necesita la consulta 9.5.
- **Histórico en Gold.** Rentas: 298 450 filas contra 74 295 de entrada activa, con 0 apagados en la última corrida; unas 224 000 filas (75 %) son versiones cerradas. ARL: 1 826 de 3 925. ARP: 499 de 665. Coherente con cierres masivos anteriores.
- **Llave de Rentas.** Silver agrupa por renta y periodo de pago, pero la llave de SCD es solo `NID_RENTA`. Si una renta trae dos periodos de pago en el mismo mes, la entrada tendría llaves duplicadas y el `full join` las multiplicaría. La última corrida (`sin_cambio = entrada = 74 295`) no lo muestra; no se descarta para otros periodos.
- **Reglas de validación.** Las 6 mallas usan solo `columna`, `tipo`, `nulo`, `longitud_maxima`, `precision` y `escala`. Cantidad de reglas: 11, 13, 19, 42, 105 y 138. En reserva matemática (Silver de 440 a 460 columnas) las reglas cubren las tablas de entrada, no el resultado.
- **Extracción.** `accion_si_supera_limite=ADVERTIR` (límite de 1 000 000 de unidades): solo avisa. `modo_ejecucion=INICIAL` en todas, con `excluir_llaves_segmento=true`. Los `WHERE` por periodo están dentro de la configuración y los reescribe el configurador. Los periodos de DEV no coinciden: cinco mallas en `202512` y RVI en `202609`.
- **DynamoDB.** Las 8 tablas del flujo tienen cifrado y recuperación a un punto en el tiempo. Ninguna tiene TTL; `short-links` ya acumula 244 enlaces.

### 9.4 Sigue abierto

1. Las configuraciones de caso de la certificación (`<malla>/configs/certification/*.json`: consultas de Athena y aprobadores) **no se recolectaron**; el script solo trajo `configs/certification/`. Sin ellas no se puede verificar el cuadre de totales.
2. Reglas de WAF (límite de peticiones).
3. Código de la imagen de `datalake-dev-niif-send-via-sftp` (es contenedor).
4. Visibilidad del repositorio (CUM-01).

### 9.5 Consultas de Athena para cerrar ARQ-13

Ejecutar en la base `niif-dev-gold-catalog-db` y pegar el resultado (son solo conteos, sin datos personales):

```sql
SELECT per_corte, act_reg, COUNT(*) AS filas FROM scd_reserva_matematica_arp GROUP BY per_corte, act_reg ORDER BY per_corte, act_reg;
SELECT per_corte, act_reg, COUNT(*) AS filas FROM scd_reserva_matematica_rvi GROUP BY per_corte, act_reg ORDER BY per_corte, act_reg;
SELECT act_reg, COUNT(*) AS filas, COUNT(DISTINCT nid_renta) AS rentas FROM scd_primas_rentas GROUP BY act_reg;
SELECT act_reg, COUNT(*) AS filas, COUNT(DISTINCT nid_renta) AS rentas FROM scd_primas_endosos GROUP BY act_reg;
SELECT act_reg, COUNT(*) AS filas FROM scd_primas_arl GROUP BY act_reg;
```
