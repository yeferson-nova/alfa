# Plan de trabajo: repo `data-datalake-niif-pcr-emisiones-glue-job`

Parte de la [malla 1 de PCR (Emisiones)](README.md). Base: la rama `quality` del repo con la versión de QA del 2026-11-25, donde `extraccion_estandar_emisiones.py` tiene 853 líneas. Las líneas citadas son de esa versión. Todo lo de esta página se comprobó leyendo el código. Lo que falta confirmar está marcado.

## Qué hay hoy en el repo
| Archivo | Estado |
|---|---|
| `src/jobs/extraccion_estandar_emisiones.py` | El job. Un solo archivo. La versión de QA agrega una advertencia cuando el SQL del núcleo común difiere entre casos (`:334-349`) |
| `src/jobs/validacion_tecnica_emisiones.py` | 1.105 líneas, con cambios en QA. La Step Function de Emisiones **no lo usa**: usa la validación técnica genérica (`estandar/validacion_tecnica.py`). *Por confirmar* quién lo usa |
| `tests/` | **Sin pruebas.** Solo `conftest.py` con una sesión de Spark |
| `tox.ini` y `sonar-project.properties` | La cobertura excluye `src/jobs/*` y su umbral es 0, así que el pipeline no exige el 70 % que pide el gobierno de DevOps |
| `requirements.txt` | Trae librerías que el job no usa: `nltk`, `pandas`, `openpyxl` y `requests` |
| `README.md` | Es la plantilla inicial, sin llenar. Además dice `glue_resources` y el `Jenkinsfile` dice `glue-resources` |
| `Jenkinsfile` | `glueJobsClientesPipeline` sube `src/` a `s3://niif-<amb>-assets/glue-resources/pcr-emisiones` |

**Cómo corre (validado en la configuradora `conf_emisiones.py` y en la IaC de QA):**
- **Configuración:** la configuradora arma el JSON de cada caso desde DynamoDB (`cnf-mallas`, ítems `EXTRACCION` y `CICLO#001`) y lo publica en `archivoActivoUri`. El job lee esas rutas. **Verificado en DEV el 2026-11-25:** DynamoDB guarda plantillas (`${FECHA_INICIO}`, `${PRODUCTOS}`, etc.) y los JSON de `cfg-datalake-niif-config-s3` son una salida ya renderizada, con las mismas tablas (25, 45 y 59). Esos ítems no están en ningún repo.
- **Versiones en DEV (verificado el 2026-11-25):** el script de extracción desplegado es idéntico al de `quality` (853 líneas, mismo sha256), así que las mediciones de DEV valen. La configuradora tampoco tiene diferencias ya: la versión de QA del 25 nov (18:33) es igual a la de DEV (1237 líneas) y ya no exige un número fijo de tablas. Valida que núcleo y rama no estén vacíos, que no haya tablas duplicadas y que las fases de rama sean 4 a 9.
- **Identificación de la corrida:** el `id_ejecucion` es `EMISIONES-<periodo>-<hora>`, no el nombre de la ejecución de la Step Function (ARQ-32). El lote es nuevo en cada corrida (`conf_emisiones.py:172-173`).
- **Orden de los casos:** pcr_vida, generales y satélites. El núcleo común se extrae con la configuración del primer caso activo (`:824-829`).
- **Recursos:** la IaC de la rama `quality` (sincronizada el 20 nov) tiene G.2X × 10, timeout de 2880 minutos y `max_concurrent_runs = 3`, igual a lo desplegado en DEV.

## Qué hace lento al job (verificado en código y configuración)
| Hallazgo | Evidencia |
|---|---|
| **Generales y Satélites (90 tablas) corren una por una.** Sus segmentos tienen `forma = CASCADA_POR_FASE`, que el código no reconoce como paralela | `:217`, `:767-768` |
| PCR Vida y el núcleo corren en paralelo, con máximo 3 tablas a la vez; el tope está fijo en el código | `:764` |
| Los 3 casos corren uno tras otro | `:832-846` |
| Cada tabla se lee con una sola sesión JDBC. Solo 2 tablas de PCR Vida usan `partitionColumn`, con 4 particiones | `:577-585`; JSON de PCR Vida |
| El pivote `CONTEXTOPERATION` (331 millones de filas) se vuelve a filtrar en 115 de las 121 consultas SQL configuradas (PCR Vida 16 de 17, Generales 44 de 45, Satélites 55 de 59, contando el núcleo). Las tablas `DEPENDENCIA` lo hacen con `EXISTS` anidados | `:390-521` |
| No hay pausa entre ciclos (Víctor, 20 nov, L289-293) | `execute_phases` |
| Se lee de Oracle dentro de la escritura (Spark es perezoso) y después se relee el parquet para contar | `:696-718` |
| `oracle_parallel` (19 tablas) arma un hint que se borra antes de leer | `:446-457`, `:545-550` |
| `fetchsize`: 50.000 en PCR Vida y 20.000 en Generales y Satélites. El núcleo usa el del primer caso | JSON, `:552` |

## Lo que dice la medición (DEV, 25 nov, corridas existentes)
- **El clúster está sin uso:** CPU de ejecutores en 0,00 de promedio, heap en 2 % y el 95 % del tiempo de Spark en etapas de una sola tarea. Hoy pagamos unos 100 DPU-hora por corrida para usar una sola sesión.
- **El tiempo lo pone el costo de cada consulta, no el volumen:** 154 filas tardan 19 minutos, 45 filas 9 y 8 filas 6. Ocho tablas de menos de 1.000 filas suman 47 minutos. Eso apunta al refiltrado del pivote en cada consulta.
- **Los casos van en serie:** núcleo 30 min, PCR Vida unos 90, Generales 67 y Satélites 135, para un total de 321 minutos.
- **El conteo no pesa** (1 o 2 segundos por tabla), así que sale de las prioridades.

## Cómo vamos a trabajar el repo (en orden)

### 0. Línea base y red de seguridad (sin cambiar nada del comportamiento)
1. Rama `feature/` desde `quality`, en el repo corporativo.
2. Medir en DEV con las corridas que ya existen: `medir-tiempos-dev.sh` (tiempo por tabla) y `medir-uso-glue-dev.sh` (uso del clúster).
3. Pruebas de caracterización antes de tocar código:
   - el SQL que genera cada una de las tablas de los 3 casos, guardado como referencia;
   - `validate_execution`, `output_path`, `runtime_for` y el orden y la concurrencia de `execute_phases`, con dobles de Spark y S3.

   Hoy el módulo no se puede importar en pruebas porque crea `SparkContext` y llama a `job.init` al cargarse (`:33-40`).
4. Prueba de equivalencia: conteo y suma de control por tabla frente al Bronce actual del mismo periodo. La diferencia debe ser 0.

### 1. Código modular y limpio (los resultados no cambian)
Diseño detallado y de dónde sale cada función: [diseno-modular-extraccion.md](diseno-modular-extraccion.md).
1. Arquitectura hexagonal, según `Docs\08-estandares-desarrollo\`:
   - **dominio:** el modelo de configuración, el compilador de SQL y el planificador de fases y del pool;
   - **puertos:** fuente Oracle, almacenamiento, checkpoint, publicación del runtime, logs y métricas;
   - **adaptadores:** Spark JDBC, Glue y S3;
   - **raíz de composición:** el script del job, que queda delgado.
2. Empaquetado de los módulos: otros jobs ya usan `--extra-py-files` (por ejemplo, `libraries/commons.zip`). Hay que preguntarle a DevOps si `glueJobsClientesPipeline` empaqueta `src/` en un zip o qué alternativa usar.
3. Limpieza:
   - imports sin uso (`calendar`, `time`, `StorageLevel`) y el hint de `oracle_parallel` que nunca llega a Oracle;
   - en `requirements.txt`, las librerías ajenas;
   - el README real del repo.
4. Pruebas con cobertura de 70 % o más (apuntamos al 80 %), y subir el umbral de `tox.ini` y Sonar (lo confirma DevOps).

### 2. Deuda técnica, junto con su configuración (JSON, `cnf-mallas` y configuradora en el mismo cambio)
| Brecha | En el código | Corrección |
|---|---|---|
| ARQ-61 | Si falta la ruta, el Bronce común cae en PRD (`:52-54`, `:587-588`), y la ruta Silver se arma con `replace` de QA (`:614-615`) | Rutas obligatorias en la configuración, sin valores por defecto |
| ARQ-63 | El checkpoint se reusa por defecto (`:641-645`) y la extracción termina `SUCCESS` con calidad `INCOMPLETA` (`:802`, `:809`) | Reanudación explícita y con aviso |
| ARQ-67 | Si el SQL del núcleo difiere entre casos, solo se advierte y se usa el del primer caso (`:334-349`). **Confirmado en DynamoDB de DEV:** Generales difiere en 5 tablas del núcleo (`AGREGATEDPOLICY`, `PREPOLICY`, `EVENTDCO`, `RISKUNITDCO`, `PRODUCT`) | Un solo núcleo en la configuración, o falla |
| Config muerta | `persistir_dataframe`, `ventana_extraccion`, `filtros_efectivos`, `reglas_arquitectura`, `salida_runtime`, `modo_ejecucion` y `oracle_parallel` llegan al job y no se usan. Por tabla: `fetchsize` (el job solo lee el de `fuente`, `:552`), `alcance`, `tipo_tabla` y `orden_fase` | Quitarlas, o usarlas, en el JSON, en DynamoDB y en la configuradora |
| ARQ-60 | Hay copias del job en DEV | Una sola versión |

### 3. Observabilidad en 3 niveles
Hoy el job imprime eventos JSON sueltos con `print`: `EMISIONES_INICIO`, `FASE_INICIO`, `TABLA_FIN` y `CASO_EXTRACCION_OK`, este último con el runtime completo. `TABLA_FIN` no lleva `id_ejecucion`, no hay evento de error ni duración en milisegundos.
1. **Logs:** contrato v1 en cada línea (`ejecucion_id`, caso, fase, tabla, evento, duración, filas y estado) y error clasificado.
2. **Métricas:** por tabla y por fase: duración de lectura y escritura, filas, sesiones activas y pausa, con nombres estables.
3. **Trazas:** `PASO_INICIO`/`PASO_FIN` con el `ejecucion_id` igual al nombre de la ejecución de la Step Function (`--EJECUCION_ID`, que pone DevOps en la IaC).

Además, medir la lectura real: hoy "escritura" incluye la lectura de Oracle.

### 4. Tiempo: 90 minutos como máximo, sin tocar Oracle
1. Pausa entre ciclos y pool de peticiones configurables por malla en DynamoDB. **Validado con Víctor.**
2. **Concurrencia parametrizable.** Se deja la que hay hoy (hasta 3 tablas en las fases `PARALELO`, de a una en `CASCADA_POR_FASE` y particiones según la tabla) y se vuelve un parámetro de la configuración. El valor por defecto reproduce el comportamiento actual. Si a futuro cambia, se ajusta sin cambiar código.
3. Leer el pivote una sola vez y reutilizar sus llaves, como propone Víctor. Exige probar equivalencia.
4. Reanudación explícita (checkpoint y "¿Existe ese lote?").
5. **Usar el clúster al máximo,** con los mismos recursos de hoy (10 G.2X). Con 1 sesión a Oracle:
   - pasar trabajo de Oracle a Spark: leer el pivote una vez y filtrar las tablas hijas en Spark con un semijoin; hacer en Spark el `DISTINCT` y la deduplicación;
   - repartir los datos leídos (`repartition`) para que la escritura y las transformaciones usen todos los ejecutores.

   Cada cambio se mide (filas que viajan por la sesión y tiempo) y pasa la prueba de equivalencia.
6. Ajustes menores, cada uno con su prueba de equivalencia: `fetchsize` único, conteo sin releer el parquet y `DISTINCT`.

### 5. Entrega
- Zip con el nombre del recurso.
- Cambios de configuración: JSON, DynamoDB y configuradora.
- Solicitud a DevOps: `--EJECUCION_ID`, empaquetado y tamaño del clúster.
- Evidencia de antes y después en `validaciones\salidas\`.
- Demostración a Víctor: la corrida tabla por tabla y la optimizada.

## Notas para DevOps (no son nuestra tarea ni nos bloquean)
- **Empaquetado:** ¿cómo sube el pipeline (`glueJobsClientesPipeline`) los módulos de `src/`? Por ejemplo, un zip en `--extra-py-files`, como `libraries/commons.zip` en otros jobs. Se necesita para desplegar el código modular, no para escribirlo.
- **Configuración en DynamoDB:** ¿quién cargó los registros de Emisiones en `cnf-mallas` y de dónde? No están en ningún repo.
- **Tamaño del clúster:** en las corridas medidas, la CPU de los ejecutores está en 0,00 de promedio y la memoria llega como máximo al 12 % (10 G.2X, unos 100 DPU-hora por corrida). El tamaño lo decide DevOps.
- *Propuesta:* que el tamaño se pueda configurar por corrida (`WorkerType` y `NumberOfWorkers` en la tarea de Glue de la Step Function, tomados de la configuración) en vez de quedar fijo en la IaC.

## Preguntas abiertas
1. ¿Quién usa `validacion_tecnica_emisiones.py`?

Las preguntas a DevOps están en las notas de arriba. La concurrencia no es pregunta: se deja la de hoy y se vuelve parametrizable.
