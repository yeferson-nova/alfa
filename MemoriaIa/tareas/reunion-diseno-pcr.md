---
tipo: tarea
actualizado: 2026-11-10
---
# Reunión: explicación del diseño PCR (2026-11-10)

Expositor: Victor Daniel Cortes Restrepo. Transcripción cruda (se actualiza durante la reunión): `MemoriaIa\reuniones\pcr\trascipticones.txt`. Diagrama del programador: `MemoriaIa\reuniones\pcr\programador-diagrama.png`.
Todo lo de abajo es **dato del expositor**; lo que yo deduzco va marcado "propuesta". Relacionado: [[familia-mallas-alfaprd]], [[arquitectura-niif17]], [[preguntas-abiertas]].

## Contexto previo (de la memoria)
- PCR = "Pasivo por Cobertura Restante" es **inferido**; no se ha confirmado en la reunión (hasta ahora no lo definieron).
- Flujos PCR (Tradicional, Vida, Vida Deudores) vienen de ACSELE; solo hay jobs de prueba en `Backend\Glue\pruebas-glue\scripts\`.

## Qué explicaron

### Visión general (bloques)
1. **Bodega NIIF 17** en AWS (arquitectura medallón / data lake). Prepara la información para el negocio.
2. **Certificación de cifras** (notificación + certificación por usuarios): validan las cifras antes de que lleguen a las herramientas de cálculo actuarial. Es gobierno de información. **Nuestro alcance = estos 2 bloques.**
3. **Cálculo actuarial:** el área actuaria toma los datos del data lake, aplica sus modelos y lo ejecuta en **MFIS** (con herramientas de apoyo).
4. **Herramienta contable** (nombre transcrito "contintool", a confirmar) + reportería para el negocio.
5. **DataStage** homologa las cuentas contables en asientos para que los libros queden diligenciados directamente en **SAT** (transcrito; confirmar).
- Pregunta mía pendiente: qué pasa en el proceso cuando el usuario **rechaza** al certificar (Víctor: "ya lo vemos").

### El programador (modelo programático)
- Es la capa que **gobierna las mallas de ejecución** en el tiempo.
- Hay una tabla en **DynamoDB** con un **modelo programático por malla**: configuración de cómo se programa cada malla durante un periodo.
- La programación se define **por rango**: el usuario dice, por ejemplo, "malla Primas PCR mensual durante los próximos 2 años", el día (el 1 de cada mes) y la hora (3:00 a.m.). El programador **crea las programaciones de los 12 meses** (ejemplo dado) para que la malla se ejecute.
- Flujo del diagrama (numeración del dibujo; **propuesta** de lectura, falta confirmar):
  1. DynamoDB Stream (detector de cambios) detecta cambios en la tabla del modelo de programación.
  2. Lambda de automatización recibe el evento.
  3-4. La Lambda lee y escribe de vuelta en DynamoDB (modelo de programación).
  5. Lambda → EventBridge **Pipe** (enrutador).
  6. EventBridge (programación de ejecución) crea las ejecuciones programadas.
- Región `us-east-1`, cuenta/entorno `alfa-datalake`.

### Cambios del medallón para PCR
- Antes no existía el bucket de **zona de aterrizaje**: se usaba otro bucket como zona *drop*. El **arquitecto de datos** pidió separarlas:
  - **Zona de aterrizaje** → datos crudos (landing/traslado).
  - **Zona de ingreso** → la zona drop (nombre: "zona de ingreso").
- (La transcripción dice que son 2 cambios; el segundo aún no aparece.)

### Medallón explicado por Víctor (transcripción hasta ~09:23)
- **Zona de ingreso** (antes *drop*): donde los usuarios dejan archivos para cargar. **Zona de aterrizaje**: parte del medallón, datos crudos. El job de extracción lee configuración viva, toma de las fuentes y deja en aterrizaje.
- **Crudas:** solo llevan **número de lote de cargue** (trazabilidad) y **número de fila** (identificador del registro en la extracción).
- **Consolidación Bronce:** componente propio que versiona por **universos** (grandes volúmenes): inicio de vigencia, fin de vigencia, versión y activo/inactivo. Es **linaje del registro**, no del dato. Luego los **crawlers** sincronizan el catálogo.
- **Plata = Validación + Remediación + Consolidación**, más un sub-punto de **transformaciones**:
  - *Validación:* reglas de **calidad** (ej. completitud/nulos) o de **remediación** (ej. cambiar Ã por X por solicitud de negocio, válida para un momento/caso de uso). Genera una **tabla de diagnóstico** por registro, lote y atributo (se aplicó o no la regla).
  - *Remediación:* retoma el diagnóstico y aplica las reglas pedidas. Las de calidad las engrosa **Analítica**; NIIF 17 solo tiene un pool pequeño de remediación.
  - Componente **agnóstico, sin librerías** de terceros: la idea de Alfa es **no depender de un servicio** (poder migrar a Databricks/Azure).
  - *Consolidación Plata:* dos salidas, **auditoría** (trazabilidad: quién/cuándo cambió el dato, p. ej. "Núñez con Ñ vs n" para contraloría) y **tabla de registro** (dato ya remediado). La trazabilidad no se pierde. **Manu** puso la trazabilidad dentro de remediación (decisión del desarrollador "donde le quedara más fresco"); la consolidación solo consolida el histórico final.
  - Crawlers de nuevo tras consolidar.
- **Transformación (Plata/Oro):** el **único job que no es estándar**; usa un **framework de preparación de información (BI, cruces)**. Primero saca atributos; respeta **círculos de cálculo** (ej. promedio = 1.ª operación contar+sumar, 2.ª dividir) por tiempos y optimización. 
- **Aclaración (09:26-09:27):** el framework **no es código ni aplicativo**; es un **modelo de preparación / marco de trabajo**. No hay nada estándar en la información, así que no se entrega un código reutilizable: se define un **orden obligatorio de pasos** (ej. primero los cruces/joins, luego atributos, luego operaciones). Un ingeniero de datos o líder técnico lo conoce y **devuelve** (rechaza) las preparaciones que no lo respeten por no estar optimizadas. Está en la **documentación** y en **videos** que Víctor grabó con Manuel; él la va a **refinar y actualizar en estos días** y se la pasará a Yeferson (ver acciones).
- **Control de ciclos de variables:** no operar sin control; si PCR necesita más capacidad, **el operador sube la infraestructura/grupo de recursos** (diagnóstico de dimensionamiento). Por eso la transformación va **aislada en un job propio** en Plata.
- **Consolidación Oro:** mismo modelo de consolidación que Bronce, pero **sin número de registro** (en Oro los registros van por información, ya no por dato). Se mantiene **solo el lote de cargue** como punto de control (reprocesos, eliminar cargas). Va al bucket Oro, se sincroniza el catálogo y **genera un evento al modelo de notificaciones** para liberar notificación y certificación.
- Diagrama (hoja con pasos 26-42): Job Consolidación Plata (28-29), Rastreador Plata (32-33), **Job Transformación Zona Plata (34-35)**, Job Consolidación Oro (38-39), Rastreador Oro (41-42), buckets (26, 30, 40). Confirma que Consolidación Plata y Oro **sí aparecen como jobs en el diagrama**.

### Modelo de preparación y costo (09:27-09:31)
- Orden del modelo (ejemplo de Víctor): primero **cruces** (inner join), luego los left; después sacar **todos los atributos**, marcar cuáles van a la salida final y cuáles se **operan**; por último formar cada **granularidad**. Se basa en **teoría de conjuntos** aplicada a grandes volúmenes, no en una herramienta. **El líder técnico revisa** que cada transformación lo cumpla.
- **Dato de GFT:** la primera versión de preparación de PCR tardaba **3 días** por mes; la versión actual de Manuel tarda **~3 horas**, **con la misma infraestructura**. PCR procesa **~14.000 millones de registros por mes**.
- Principio: **serverless = menos tiempo, menos costo.** Primero ingeniería del dato/información, **después** subir infraestructura. Excepción: el **punto de equilibrio (breakpoint)**: si subir el grupo de recursos termina más barato por acortar el tiempo, se sube. Alfa exige no generar sobrecostos por código mal estructurado.
- Yeferson debe aprender el modelo para **diagnosticar** preparaciones (dónde va el cruce, etc.) y optimizar.

### Notificación y certificación (09:32-09:37)
- Al terminar Gold se dispara un **action** del modelo de notificaciones → **Lambda de notificaciones**. Maneja **3 tipos de mensaje: totales, resúmenes y detalle**, en la cantidad que se quiera de cada uno; vienen de **queries configurados en DynamoDB**.
- Totales y resumen: el job de notificación consulta Athena, **devuelve solo los valores** y se arman en **HTML5**. El detalle (una "sábana" grande): el job consulta Athena y **deja el archivo en la zona de certificación**; avisa que terminó y se arma el correo HTML5, enviado por **SES**.
- El usuario entra por el **directorio activo** (usuario del negocio). El correo tiene botones **Certifico / No certifico** y un **link de ingreso** (API Gateway → zona de certificación) para **descargar el archivo** y revisarlo.
- **Si no certifica:** va al **modelo de configuración (programador)**: la tarea queda **pendiente** hasta que el negocio avise al **operador** para que **active la tarea programada** y se vuelva a ejecutar. **Corrigen las fuentes**: en capa intermedia no se corrige nada salvo remediación ya definida (los errores **tipificados**, comunes, que en otros core se remedian en capa media con trazabilidad; aquí se trasladó al gobierno del data lake).
- **Si certifica:** la Lambda mueve el archivo de la zona de certificación a la **zona de transporte**; un evento llama un **put por SFTP a FIS** y lo refresca. Con eso los modelos actuariales toman la data certificada.
- Víctor evoluciona la arquitectura (diagramas C4 y detalle técnico); publicará en las **próximas 2 semanas** en una carpeta compartida (si no hay acceso, avisar a **Zulmi**). No es un arquitecto rígido: da lineamientos, el equipo propone. **Ya hay versiones desplegadas en producción**: avisar de impactos.

### Optimización: 3 tipos (09:39-09:53)
1. **Del servicio** (infraestructura): la define Ingeniería/DevOps; el líder técnico **justifica** si subió infraestructura o cambió el balance.
2. **De la preparación**: la dicta el framework (ver arriba).
3. **De la extracción:** compleja porque en Alfa **no hay réplica ni infraestructura de contingencia**: se le pega **directo a producción de los core** (ej. Acsel-e). Antes, integradores (Glue, DataStage) **arrebataban recursos** al core y lo ralentizaban o lo tumbaban (ancharon la banda y se desbalanceó el otro lado).
   - **Ciclo:** conjunto de grupos administrados con la infraestructura; al terminar se **suelta la conexión y se deja descansar la fuente** (ej. 5 minutos) antes del siguiente. Similar a un pool.
   - **Conjunto (grupo):** grupo de tablas con una forma. Ej. grupo 1 con 6 tablas, grupo 2 con 3, grupo 3 con 1, grupo 4 con 1.
   - **Forma:** **paralela** (extrae a la vez), **cascada** (una tras otra) o **segmentada** (una tabla partida en varios conjuntos, ejecutados "de a uno").
   - **Unidad de costo = filas × columnas** de la tabla (10 filas × 10 columnas = 100). El **límite del ciclo** se fija en unidades de costo (ej. "máximo 12.000 millones por ciclo contra Acsel-e"); si los conjuntos lo superan, se parte.
   - **Pivote:** la tabla madre (en Acsel-e, `context operation`, con las emisiones de pólizas). Genera un **ID de dependencia**: atributos clave que sirven de insumo a **n tablas**, que solo extraen los registros de esos IDs. Hay que entender el origen para armar el modelo (hacer ejercicios en Acsel-e).
   - **Resultado en PCR Vida:** en vez de 14.000 M de registros, con dependencias se extraen ~**120 M** (≈ 4 M × 18 tablas + 20 M × 2 tablas); por eso bajó de 3 días a 3 horas.
   - Es el método de la "vieja guardia" (SAP BI de los 90), difícil de optimizar hoy; extracción y preparación van de la mano porque el framework se basa en esto.
- **Para ti:** pedirle a **Manuel** que explique cómo configuró esto en los servicios y los pasos de desarrollo, para trabajar igual; llevar aportes a Ingeniería.

## Validación contra lo que tenemos
**Extracción (nuevo, 2026-11-10; `extraccion_estandar_optimized.py`):** el job **ya tiene** el modelo: lee `ciclos` ordenados (línea 1033), con segmentos, `forma` y `dependencia` (tabla origen y llaves) por tabla, y checkpoint de ciclo/segmento. **Diferencias:** (a) solo ejecuta en **CASCADA**; si la forma es otra, avisa y la trata como cascada (líneas 1136-1140); la **paralela no está implementada** (coincide con RYC-01); (b) **no encontré unidades de costo** ni límite por ciclo; (c) **no encontré pausa/reposo ni cierre de conexión** entre ciclos (propuesta: confirmar con Manuel). Falta ver si las 6 mallas ALFAPRD configuran ciclos y dependencias en su JSON o van planas.

**Notificación:** coincide con lo desplegado: 3 tipos de consulta (`summary`, `total`, `detail`), SES, HTML5, botones certificar/no certificar y SFTP a FIS. Pendiente: ¿"no certifico" reprograma realmente el modelo de configuración? Hoy no lo he visto en el código (verificar).
 (propuesta, 2026-11-10)
| Lo que dijo Víctor | Lo que hay en repo/memoria | Resultado |
|---|---|---|
| Crudas con lote de cargue y número de fila | `extraccion_estandar_optimized.py` maneja `lote_cargue_id` y importa `monotonically_increasing_id`; falta confirmar que el id de fila se persista | Coincide en lote; fila por confirmar |
| Consolidación Bronce versiona (vigencia, versión, activo) | No existe como job: va dentro de la extracción ([[familia-mallas-alfaprd]]). El SCD (`scd.py`) versiona por hash, cierra por cambio y apaga ausentes en **Gold** | **Diferencia:** el diseño lo ubica en Bronce, lo desplegado lo hace en Gold |
| Validación genera diagnóstico y remediación lo aplica | Validación nunca bloquea (`detiene_flujo=False`) y remediación no corrige ni aparta; `PENDIENTE_ANALITICA` nadie lo lee (ARQ-03) | **Brecha:** existen, pero no cumplen la función descrita |
| Consolidación Plata con auditoría + tabla final | No existe como job (auditoría 2026-11-05); la trazabilidad es parcial | **Brecha** (ya conocida); lo de Manu en remediación explica por qué |
| Componente agnóstico, sin depender de servicios | Todo el stack está atado a Glue, Athena, Step Functions, DynamoDB, EventBridge | Principio de diseño que hoy no se cumple por completo (nota: no es defecto en sí, es criterio de Alfa) |
| Transformación = único job no estándar con framework de BI (corregido: es un **modelo/marco de trabajo**, no código) | Los 6 Silver son scripts por malla, con joins, `dropDuplicates` y lotes de respaldo a mano. Nada en el repo documenta un orden de pasos (cruces → atributos → operaciones) ni se revisa que se cumpla | **Brecha de cumplimiento, no de código:** obtener la documentación y videos de Víctor y revisar los Silver contra ese orden. Mi búsqueda de "framework" en el código no aplica |
| Consolidación Oro sin número de registro, solo lote | Gold se arma por SCD con `lote_cargue_id` (`scd.py:181`) | Coincide en el lote; falta ver si Gold conserva columnas de fila |
| Gold dispara evento al modelo de notificaciones | Hoy la Step Function `niif-<amb>-<malla>` encadena SCD → certificación → notificación | Parece equivalente; confirmar si hay EventBridge de por medio |
| Círculos de cálculo ordenados por optimización | RYC y ARQ-05: joins sin cuadre y `dropDuplicates` sin orden (monto arbitrario) | Relevante: el framework resolvería justo esto |

## Costos de Athena y Glue en simple (para leer en la reunión)
Cifras de lista us-east-1 (**verificar**); detalle en `Docs\06-brechas\rendimiento-y-costos.md`.
- **Athena** cobra **5 USD por TB leído** (no por tiempo). 1 GB ≈ 0,005 USD. Cuesta poco si leemos pocas columnas y particiones; sube si se lee todo.
- En **DEV** hoy: máx. 0,38 GB por ejecución ≈ **0,002 USD**. No es un problema hoy.
- **Riesgo real:** consultas de certificación sobre Gold **sin tope de bytes** (RYC-09, RYC-11), tablas de hasta **458 columnas** (RYC-07) y `SELECT *`. Con PCR (14.000 M de registros) un `SELECT *` mal puesto puede leer cientos de GB: **0,5 USD por cada 100 GB, 5 USD por TB**, por consulta, y se repite cada mes.
- **Glue** cobra **0,44 USD por DPU-hora** y es donde está el gasto grande. Ejemplo (propuesta, supone 4 DPU como los jobs actuales): 3 días = 72 h × 4 × 0,44 ≈ **127 USD**; 3 horas ≈ **5 USD**. Es el argumento de Víctor: optimizar el dato baja ~96 % el costo.
- Un job colgado cuesta ~**84 USD** (timeout de 48 h, RYC-10).
- **Medidas propuestas:** grupo de trabajo de Athena con tope de bytes por consulta, pedir solo columnas, timeouts por job, y medir con las métricas `dpuHours` del observador.

## Decisiones / acuerdos
_(ninguno todavía)_

## Dudas para preguntar
- ¿Qué significa PCR exactamente y qué productos cubre?
- ¿Cuál es el esquema de la tabla DynamoDB del modelo programático (claves, campos: malla, rango, día, hora, zona horaria)?
- ¿Es EventBridge Scheduler o reglas? ¿Cuántas programaciones se crean y qué pasa al editar/borrar un rango (idempotencia, el Stream re-dispara)?
- ¿Cómo se relaciona el programador con las Step Functions `niif-<amb>-<malla>` y con las lambdas `configuration-<malla>` actuales (¿reemplaza a los programadores de EventBridge existentes?)
- ¿Qué pasa cuando el usuario rechaza en la certificación? **Parcial (2026-11-10):** Víctor dijo que debe ir al modelo de configuración y dejar la tarea pendiente de reprogramar. En DEV solo se archiva en `NO-CERTIFICADOS` y se marca `NO_TRANSPORT`, sin reprogramar (ver [[job-notificacion-ses]]). Confirmar con Víctor.
- ¿Cuál es el 2.º cambio del medallón en PCR? (parece ser la zona de aterrizaje vs ingreso; confirmar si hay otro)
- ~~¿Dónde está el framework?~~ Respondido: es un modelo de preparación documentado (docs + videos de Víctor y Manuel). Falta: ¿aplica a los 6 Silver de ALFAPRD o solo a PCR?
- ¿Quién tiene la consolidación Bronce con "universos" y vigencias: está en la extracción o en otro job?
- ¿Las reglas de validación/remediación se configuran en JSON (`config_val_tec_*`) y quién las mantiene (Analítica)?

## Acciones derivadas
- [ ] Víctor refina y actualiza la documentación del framework de preparación (y videos con Manuel) y se la pasa a Yeferson en estos días.
- [ ] Hablar con **Manuel**: cómo configuró ciclos, conjuntos, formas y dependencias, y los pasos de desarrollo.
- [ ] Revisar las publicaciones de Víctor (carpeta compartida, próximas 2 semanas; C4 y detalle técnico); si no hay acceso, escribir a **Zulmi**.
- [x] Verificar en código qué hace "No certifico": hecho 2026-11-10, no reprograma (detalle en [[job-notificacion-ses]]). Falta preguntar a Víctor si es lo esperado.
- [ ] Aprender el modelo de preparación y de extracción (hacer ejercicios en Acsel-e); Víctor ofreció sesión sobre teoría de conjuntos.
- [ ] Revisar el JSON de extracción de las 6 mallas (ciclos, dependencias) y proponer unidades de costo y pausa entre ciclos.
- [ ] Al recibirla: contrastar los 6 Silver contra el orden del framework y registrar brechas (`ARQ-xx`).
