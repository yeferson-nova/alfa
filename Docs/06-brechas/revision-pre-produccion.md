# Revisión técnica antes de producción

**Rol de este documento:** es la mirada de un ingeniero senior sobre lo que existe hoy en DEV (lambdas, Step Functions, jobs de Glue, crawlers, tablas de DynamoDB y reglas de EventBridge), para decidir **qué hay que corregir antes de pasar a producción** y en qué orden. No propone cambios de arquitectura ya: eso se valida con el equipo.

**Fuentes:** `Docs\05-arquitectura\flujo-integral-niif17.md` (cómo funciona), `jobs-glue.md` (los jobs), `rendimiento-y-costos.md` (este mismo directorio), y los inventarios `Backend\Lamnbdas\inventario-aws\lambdas.md` y `step-functions-niif.md`. Marcas: **hecho** (está en el código o la configuración) e **inferido** (se deduce, hay que confirmarlo). No se vieron ejecuciones reales.

Los IDs (`LMB`, `SF`, `FLU`, `RYC`) son locales y se consolidan como `SEG`, `COS`, `ARQ` y `CUM` en la matriz de la fase 3.

---

## 1. Veredicto

**Hoy no está listo para producción.** El flujo funciona de punta a punta en DEV y tiene piezas buenas (sección 4), pero hay riesgos que, en una aseguradora y con cifras que van a certificación y al cierre contable, no se pueden llevar a producción:

1. **No hay forma de garantizar que lo que se certifica sea correcto.** La validación técnica nunca detiene el flujo y no hay cuadre ni reglas de consistencia.
2. **El código desplegado es la única fuente.** No hay repositorio Git, pruebas ni pipeline de despliegue.
3. **Dos ejecuciones al mismo tiempo se pisan** (estado, crawlers, remediación), y **dos programadores escuchan la misma tabla** y pueden iniciar el proceso dos veces.
4. **No queda evidencia de lo que pasó:** logs de las Step Functions apagados y muchos fallos sin aviso.

| Severidad | Cantidad | Qué significa |
|---|---|---|
| **Bloqueante** | 5 | No pasar a producción sin resolverlo |
| **Alto** | 9 | Resolver antes, o con un plan aprobado y fecha |
| **Medio** | 9 | Planificar en las siguientes iteraciones |

---

## 2. Lista priorizada

### Bloqueantes

| ID | Categoría | Hallazgo | Evidencia | Corrección propuesta | Tarea |
|---|---|---|---|---|---|
| PP-01 | Calidad de datos | **La validación técnica nunca detiene el flujo** y solo revisa cada columna (nulo, tipo, longitud, precisión). Sin duplicados, rangos ni cuadre, se puede certificar y transportar a FIS un dato erróneo. | FLU-12; `validacion_tecnica.py` líneas 759-801 | Compuerta de calidad con reglas declaradas y severidad; que una regla bloqueante detenga el flujo; cuadre entrada = salida + rechazados (HU-TR-02 y HU-TR-04) | T3 |
| PP-02 | Gobierno del código | **No hay repositorio Git, pruebas ni pipeline.** El código desplegado es la única fuente, sin versión, sin revisión, sin línea base de SonarQube y sin cobertura (exige > 80 %). Hay 6 lambdas con copias casi idénticas y 12 jobs con funciones repetidas. | LMB-01, LMB-04, `jobs-glue.md` sec. 5; lineamientos de versionamiento y código seguro | Repositorios según el estándar, infraestructura como código (Terraform) y pipeline; pruebas antes de tocar nada más | T2 |
| PP-03 | Arquitectura | **Dos programadores activos sobre la misma tabla.** `manager-configuration` (con filtro) y `programador-configuraciones` (sin filtro) tienen su trigger habilitado sobre `prg-programaciones`: una programación puede iniciar el proceso dos veces con contratos distintos. | SF-01, SF-02, LMB-02 | Dejar un solo programador y deshabilitar el otro; confirmar cuál generación es la oficial | T2 |
| PP-04 | Concurrencia | **Dos ejecuciones simultáneas se pisan.** El estado vive en un archivo fijo por malla; los 3 crawlers y `remediacion` (1 ejecución a la vez) se comparten entre las 6 mallas. | FLU-02, SF-05, RYC-03, RYC-04 | `ejecucion_id` en la ruta del estado, límite de concurrencia coherente y reintento del error de concurrencia | T2 |
| PP-05 | Auditoría y respuesta | **Sin evidencia ni alerta ante fallos.** Los logs de ejecución de las Step Functions están apagados, las lambdas escriben en texto plano sin `ejecucion_id` común y los fallos del configurador, de la notificación, del transporte y del SFTP no avisan a nadie. | SF-10, LMB-07, FLU-13 | Logs estructurados con `ejecucion_id`, logs de la Step Function en CloudWatch, y alertas a soporte | T1c |

### Altos

| ID | Categoría | Hallazgo | Evidencia | Corrección propuesta | Tarea |
|---|---|---|---|---|---|
| PP-06 | Seguridad | **Athena sin grupo de trabajo, sin tope de bytes y sin tiempo límite**; resultados en el bucket de *assets*. | FLU-03, RYC-09, RYC-11 | Grupo de trabajo propio con tope, cifrado y presupuesto; tiempo límite en el job | T1 |
| PP-07 | Calidad del cálculo | **La lógica actuarial vive en scripts de 1 159 y 1 247 líneas sin pruebas**, con comentarios de ajustes por persona y fecha (por ejemplo, "AJUSTE ... 202606"). Es el cálculo que se certifica. | `silver_reserva_matematica_*.py` | Pruebas de equivalencia contra el SQL original y datos de control antes de cambiar nada | T3 |
| PP-08 | Correo | **El correo sale en texto plano por SNS**, sin HTML, con asunto cortado a 100 caracteres, y el propio código admite que *"SNS aceptado no demuestra entrega"*. | FLU-11, FLU-14 | Pasar a SES con plantillas HTML y seguimiento de entrega (rebotes y quejas) | T1a y T1b |
| PP-09 | Confiabilidad | **El stream de programaciones reintenta sin fin y sin destino de fallo.** Un registro defectuoso bloquea el *shard* hasta 24 h. | LMB-03 | Límite de reintentos y cola de fallos | T2 |
| PP-10 | Cumplimiento | **Datos personales en el log y valores por defecto de QA** en la lambda de certificación; política de invocación con comodín. | Hallazgo del análisis de `niif-dev-certification` en `pruebas-lambda\` | Quitar los datos de identidad del log (Ley 1581), valores por defecto que fallen en vez de apuntar a QA, y política mínima | T1 |
| PP-11 | Arquitectura | **El estado de una ejecución se escribe en tres lugares** (`ejc-ejecuciones`, `pipeline-estado` y el finalizador de ARL) y la Step Function termina antes de certificar; no hay estado de punta a punta. | FLU-01, SF-04, SF-09 | Un solo dueño del estado y un estado que cubra hasta el transporte | T1 y T2 |
| PP-12 | Arquitectura | **Las programaciones se escriben a mano en DynamoDB** (es el punto de entrada de todo) sin validación ni auditoría. | FLU-08 | Entrada validada con registro de quién y cuándo | T2 |
| PP-13 | Dependencias | **Los correos de NIIF dependen de tablas del datalake anterior** (`datalake-dev-use-case-queries`, `-emails`, `-approvers`). Un cambio en ese proyecto rompe la certificación. | FLU-05 | Tablas propias de NIIF o contrato formal con ese proyecto | T1 |
| PP-14 | Costos | **Timeouts de 48 h** en los 12 jobs (cerca de 84 USD por job colgado) y sin límite de tiempo en la Step Function. | RYC-10 | Timeouts según la duración real | T2 |

### Medios

| ID | Categoría | Hallazgo | Evidencia | Corrección propuesta |
|---|---|---|---|---|
| PP-15 | Rendimiento | Extracción de Oracle sin lectura paralela y tablas secuenciales (inferido). | RYC-01 | Medir y, si domina el tiempo, particionar la lectura |
| PP-16 | Rendimiento y costo | Crawlers que rastrean toda la capa en cada ejecución y 94 tablas en Silver, entre ellas una por lote. | RYC-02, RYC-12, FLU-09 | Crawler por malla o registrar particiones directamente |
| PP-17 | Datos | La validación técnica **borra** la carpeta de errores si no hay fallos, y los crawlers quitan del catálogo las tablas cuya carpeta desaparece (`DEPRECATE_IN_DATABASE`): una limpieza puede romper consultas de Athena. | `validacion_tecnica.py` línea 793; configuración de los crawlers | Mantener el historial de errores y cambiar la política de borrado del catálogo |
| PP-18 | Calidad | Valores por defecto cruzados en los jobs: RVI apunta a rutas de ARP, y certificación trae `PERIOD=202411` y el caso de Auxilios. La Step Function los sobrescribe y por eso hoy no falla. | FLU-06, FLU-07 | Quitar los valores por defecto o hacer que fallen |
| PP-19 | Calidad | Las 6 máquinas `niif-dev-<malla>` son copias con *drift*, `RegistrarPipelineCompleto` captura su error y sigue, y `FailState` puede perder el error original. | SF-07, SF-08, SF-11 | Una plantilla única de Step Function |
| PP-20 | Costos | Logs continuos, *insights* y Spark UI siempre activos; sin retención conocida; consultas completas en el log. | RYC-13 | Retención y diagnóstico bajo demanda |
| PP-21 | Costos | Datos por lote, copias históricas y tablas `tmp_` sin regla de retención conocida. | RYC-14 | Ciclo de vida por prefijo |
| PP-22 | Rendimiento | Conteos y `collect` repetidos en los jobs; la validación crea un DataFrame por regla. | RYC-05, RYC-06 | Una pasada con agregados |
| PP-23 | Costos | El observador lee el historial completo de **todas** las ejecuciones de la cuenta. | RYC-15, SF-13 | Limitar a `niif-dev-*` |

---

## 3. Orden recomendado

| Fase | Qué se hace | Por qué primero |
|---|---|---|
| **0. Visibilidad** | Recuperar el código que falta (trigger de notificación, copy, move, SFTP, las 5 lambdas de la imagen y el manager), bajar los JSON de configuración de `niif-dev-assets`, y medir una ejecución real (tiempos, DPU-horas, bytes de Athena) | Sin esto, varias correcciones se hacen a ciegas |
| **1. Base** (PP-02, PP-03, PP-05) | Repositorio, pruebas y pipeline; un solo programador; logs estructurados y alertas | Todo lo demás se corrige con red de seguridad |
| **2. Consolidar** (T2, PP-04, PP-09, PP-11, PP-12, PP-14) | La lambda general estandarizada, `ejecucion_id`, concurrencia y un solo estado | Elimina la duplicación y la carrera |
| **3. Confianza en las cifras** (T3, PP-01, PP-07) | Validación bloqueante, cuadre y pruebas de equivalencia | Requiere la definición funcional del negocio |
| **4. Correo y seguridad** (T1, PP-06, PP-08, PP-10, PP-13) | SES con HTML, alertas, grupo de trabajo de Athena y limpieza de datos personales | Se apoya en la base y en los logs |
| **5. Optimización** (PP-15 a PP-23) | Solo con las mediciones de la fase 0 | Evita optimizar lo que no es el cuello de botella |

---

## 4. Lo que ya está bien y hay que conservar

- **Un rol IAM por job**, en vez de uno compartido con permisos amplios (falta revisar las políticas).
- **Sin credenciales en el código de los jobs:** Oracle se lee a través de una conexión de Glue.
- **Manejo de fallos en la Step Function:** cada paso tiene `Catch`, deja el paso fallido y `reanudar_desde` en `pipeline-estado`, y la ejecución termina en `FAILED`.
- **Reanudación de la extracción** con *checkpoint* por tabla, segmento y ciclo.
- **Observabilidad técnica ya montada:** `PipelineObserver` publica métricas y avisa por SNS cuando falla una Step Function.
- **Controles de duplicados en la notificación:** bloqueo transaccional en el grupo compuesto, cierre condicional y detección de "ya enviado".
- **Trazado de la Step Function activado** (X-Ray) y *job bookmark* apagado de forma explícita.

---

## 5. Decisiones que necesitamos del equipo
1. ¿Cuál generación del gestor es la oficial: la de `manager-configuration` y `configuration-<malla>`, o la anterior (`programador-configuraciones` y `configurador-<malla>`)? (PP-03)
2. ¿Quién crea hoy las programaciones y cómo? (PP-12)
3. ¿Quién define las reglas de negocio de reserva matemática y para cuándo? (PP-01, PP-07)
4. ¿Los jobs `niif-dev-*`, las Step Functions y los crawlers están en Terraform o se crearon a mano? Las lambdas de trabajo llevan la etiqueta `ManagedBy = Terraform-Terragrunt`; de lo demás no se sabe. (PP-02)
5. ¿La cuenta de producción es la misma o una nueva? Cambia el diseño de nombres, permisos y claves KMS.


---

## Anexo. Hallazgos del flujo integral (FLU-01 a FLU-14)

Texto original de los hallazgos que se citan como evidencia arriba. Los IDs son locales.

| ID | Categoría | Hallazgo | Evidencia |
|---|---|---|---|
| FLU-01 | Arquitectura | **Dos tablas registran el estado de la misma ejecución:** `ejc-ejecuciones` (una fila por ejecución) y `pipeline-estado` (una por caso y periodo). Con el `finalizador` de ARL (SF-04) son tres escritores. | Step Function: 18 escrituras a EJC y 54 a pipeline-estado |
| FLU-02 | Arquitectura | **El bus entre jobs es un archivo fijo por malla** (`parametros_extraccion.json`). Dos ejecuciones simultáneas o un reproceso se pisan el estado; no hay `ejecucion_id` en la ruta. | Los 5 jobs leen y reescriben la misma ruta |
| FLU-03 | Seguridad / datos | Los jobs generan los datos de certificación con **Athena sin grupo de trabajo** (grupo `primary`, sin tope de bytes escaneados) sobre Gold. Es el principal punto de costo de Athena del flujo. | `niif_certification.py`, `start_query_execution` sin `WorkGroup` |
| FLU-04 | Costos | Los 11 jobs tienen `Timeout = 2880` (48 h), 4 workers G.1X y 0 reintentos. Un job colgado cuesta dos días de cómputo. | `job.json` de cada job |
| FLU-05 | Arquitectura | **Las tablas de certificación consultan tablas del datalake anterior** (`datalake-dev-use-case-queries`, `-emails`, `-approvers`). Un cambio en ese proyecto rompe nuestros correos. | Variables de entorno de `niif-dev-certification` y del trigger |
| FLU-06 | Calidad | Los valores por defecto de los jobs están cruzados: `silver-reserva-matematica-rvi` apunta a rutas de ARP (`reserva_matematica_arp`, `config_extraccion_primas_arl.json`). La Step Function los sobrescribe y por eso no falla, pero una ejecución manual escribiría la malla equivocada. | `job.json` de RVI y ARP |
| FLU-07 | Calidad | `niif-dev-certification` trae por defecto `PERIOD=202411` y la configuración de Auxilios Funerarios. Si falla el paso de argumentos, certifica el caso equivocado. | `job.json` de certification |
| FLU-08 | Arquitectura | **No hay una pantalla ni un servicio que cree las programaciones**: se escriben a mano en DynamoDB. Es el punto de entrada de todo el proceso y no tiene validación ni auditoría. | No se encontró lambda, API ni regla que lo haga (por confirmar) |
| FLU-09 | Arquitectura | El crawler de Silver crea una tabla por cada `lote_cargue_id` y `ejecucion_preparacion` y deja copias `tmp_` e históricas (94 tablas en Silver, entre ellas 7 `lote_cargue_id_*`, 3 `ejecucion_preparacion_*` y 2 `tmp_preparacion_*`). Ensucia el catálogo y la consulta en Athena. | `Backend\Glue\catalogo\` |
| FLU-10 | Brecha del inventario | **Faltan el código de `notification-certification-trigger`, `copy-s3-cert`, `move-s3-certification` y `send-via-sftp`.** El envío del correo sí se conoce (job de notificación); el transporte a FIS está inferido solo por sus triggers. | Código ausente en `pruebas-lambda\codigo\` |
| FLU-11 | Arquitectura (T1a, T1b) | **El correo sale en texto plano por SNS.** SNS no envía HTML ni controla el diseño, y el asunto se corta a 100 caracteres. Para filtrar por destinatario el job reescribe el `FilterPolicy` de las suscripciones en cada envío. El propio código reconoce que *"SNS aceptado no demuestra entrega en bandeja"*. Solo se usa SES en una lambda de prueba (`testNotifications`). | `notification_certification.py` líneas 78-156, 578 y 605 |
| FLU-12 | Calidad de datos (T3) | **La validación técnica nunca detiene el flujo:** `validacion_tecnica_detiene_flujo` se escribe fijo en `False`, con o sin errores. Además solo valida cada columna por separado (`NULO`, `TIPO`, `LONGITUD`, `PRECISION_ESCALA`). No hay reglas de duplicados, rangos, cuadre de volúmenes ni coherencia entre campos. El resultado (`requiere_revision_analitica`) tampoco llega al correo de certificación. | `validacion_tecnica.py` líneas 759-801 |
| FLU-13 | Observabilidad (T1c) | **Solo hay aviso cuando falla la Step Function.** Un fallo del configurador, del job de notificación, de las lambdas de transporte o del SFTP termina sin alerta, y un correo rebotado tampoco se detecta. | Ver sección 2.F |
| FLU-14 | Calidad (T1b) | El cuerpo del correo se arma con una sola cadena larga en el código; para cambiar el texto hay que modificar el job. Las 5 plantillas HTML existentes no están conectadas. | `notification_certification.py` línea 578 |
