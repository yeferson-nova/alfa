# Roadmap de cambios: modularización de la extracción

Orden de los cambios por repo, como lista de verificación. Las historias para Jira salen de aquí: [backlog.md](backlog.md). Cada paso dice en qué repo va (o si es una consulta sin repo), de qué tipo es y qué lo bloquea. El objetivo es que cada commit tenga un solo tema, se pueda revertir solo y se lea limpio en el historial.

Elaborado el 2026-11-20 sobre la rama `quality` de cada repo (estado del 2026-11-18 al 20; por ejemplo, `data-datalake-niif-rmd-ext-estandar-glue-job` en `0af0ca8` y `cfg-datalake-niif-config-s3` en `a345332`). Lo que no está verificado aparece como **decisión** o **por verificar**.

## Índice
- [Piezas](#piezas)
- [Glosario de brechas](#glosario-de-brechas)
- [Análisis en tres órdenes](#análisis-en-tres-órdenes)
  - [Orden 1: dependencias (qué necesita qué)](#orden-1-dependencias-qué-necesita-qué)
  - [Orden 2: tipo de cambio (para separar ramas y commits)](#orden-2-tipo-de-cambio-para-separar-ramas-y-commits)
  - [Orden 3: despliegue y reversa (por pieza)](#orden-3-despliegue-y-reversa-por-pieza)
- [Roadmap (lista de verificación)](#roadmap-lista-de-verificación)
  - [Fase 0. Preparación (sin cambio de comportamiento)](#fase-0-preparación-sin-cambio-de-comportamiento)
  - [Fase 1. Refactor hexagonal (mismos resultados)](#fase-1-refactor-hexagonal-mismos-resultados)
  - [Fase 2. Fixes (una rama bugfix/ por brecha)](#fase-2-fixes-una-rama-bugfix-por-brecha)
  - [Fase 3. Observabilidad (rama feature/obs-extraccion)](#fase-3-observabilidad-rama-featureobs-extraccion)
  - [Fase 4. Modelo de ciclos (rama feature/ciclos-extraccion, ARQ-57)](#fase-4-modelo-de-ciclos-rama-featureciclos-extraccion-arq-57)
  - [Fase 5. Lectura por tramos, opción D (rama feature/lectura-rangos, COS-11)](#fase-5-lectura-por-tramos-opción-d-rama-featurelectura-rangos-cos-11)
  - [Fase 6. Cierre](#fase-6-cierre)
- [Vista por repo (cuándo se toca cada uno)](#vista-por-repo-cuándo-se-toca-cada-uno)

## Piezas
| Clave | Pieza | Qué es | Quién la cambia |
|---|---|---|---|
| R1 | `data-datalake-niif-rmd-ext-estandar-glue-job` | Proceso de extracción estándar (6 mallas ALFAPRD) | Desarrollo |
| R2 | `cfg-datalake-niif-config-s3` | JSON de extracción en Git (copia coherente; la de S3 la sobrescribe el configurador) | Desarrollo |
| R3 | `sc-datalake-niif-configuration-general` | Configurador: lee `cnf-mallas` y publica el JSON | Desarrollo |
| R4 | `data-datalake-niif-pcr-emisiones-glue-job` | Proceso de extracción de Emisiones (Acsel-e) | Desarrollo |
| R5 | `iac-analytics-datalake-niif-operations-manager` | Step Functions, jobs e IAM | DevOps (solo lectura para Desarrollo) |
| DDB | DynamoDB `niif-{env}-cnf-mallas` | Configuración de origen. **Ningún repo la carga** | Script de Desarrollo en DEV; en QA y PRD, DevOps |
| ORA | Consulta Oracle | Diccionario de Oracle, sin repo | QA |
| DEV | Consulta DEV | CloudShell o corrida de prueba en DEV, sin repo | Desarrollo |
| ? | Decisión | Algo que hay que preguntar o decidir antes del paso | Desarrollo, DevOps o Arquitectura |

## Glosario de brechas
Cada ID es una brecha registrada en `Docs\06-brechas\`. La ficha completa, con la evidencia y la línea del código, está en el documento fuente.

| ID | Prioridad | Qué es | Fuente |
|---|---|---|---|
| ARQ-57 | Alta | El proceso de extracción no aplica el modelo de ciclos: no pausa entre ciclos, solo ejecuta en cascada y el tope de unidades de costo solo avisa. | `auditoria-extraccion-2026-11.md` |
| ARQ-58 | Media | Las duraciones de la extracción engañan: la "lectura" se mide al crear el DataFrame (Spark lee después, en el conteo) y la duración total se cierra antes del último paso. | `auditoria-extraccion-2026-11.md` |
| ARQ-59 | Media | El configurador no deja evolucionar la extracción: exige un número fijo de ciclos y de registros, y tiene valores por defecto que apuntan a algo concreto. | `auditoria-extraccion-2026-11.md` |
| ARQ-60 | Media | El proceso de extracción de Emisiones tiene copias: varios jobs y al menos 4 copias del script en S3, sin saber cuál es la vigente. | `auditoria-extraccion-2026-11.md` |
| COS-05 | — | La extracción no lee en paralelo (sin `partitionColumn`/`numPartitions`), tiene un `ReadTimeout` de 30 s y puede leer la fuente dos veces (conteo y escritura). | `auditoria-pipeline-datos-vs-arquitectura.md` |
| COS-11 | Alta | Las dependencias entre tablas son solo metadata: las tablas hijas se leen completas aunque solo se necesite una parte. | `auditoria-extraccion-2026-11.md` |
| RYC-01 | Alta | La extracción lee cada tabla con una sola conexión y una tabla tras otra: se pagan 4 workers y solo uno lee. | `rendimiento-y-costos.md` |

## Análisis en tres órdenes

### Orden 1: dependencias (qué necesita qué)
Hechos verificados en el código que fijan el orden:
- **Una clave nueva de configuración viaja por tres piezas: DDB → R3 → JSON de S3 → job.** El configurador arma cada tabla con una lista fija de campos (`orden`, `schema`, `tabla`, `tabla_relativa`, `tipo_tabla`, `where`, `dependencia`, `segmentacion_horizontal`; `conf_arl.py`, `construir_ciclos`). **Una clave nueva por tabla o por ciclo se pierde si R3 no la conoce.** En cambio, el bloque `fuente` se copia entero (`construir_extraccion`), así que una clave nueva ahí pasa sin tocar R3.
- **Emisiones copia `ciclos` entero** (`conf_emisiones.py:351`, `copy.deepcopy`). Ahí una clave nueva por ciclo o por tabla pasa sin tocar el configurador.
- **El proceso de extracción de Emisiones fue reescrito en `quality` el 2026-11-20** (de 3392 a 819 líneas, más un `validacion_tecnica_emisiones.py` nuevo). La versión actual:
  - **valida su configuración** (`validate_config`): rechaza un valor de `estrategia_lectura` que no conoce. En R4 el orden es **código primero, configuración después**;
  - ya tiene la clave `estrategia_lectura` (`SQL_CONFIGURADA`, `SQL_CONFIGURADA_UNA_TABLA`, `DEPENDENCIA`): la opción D usa esa misma clave con un valor nuevo, no una paralela;
  - aplica las dependencias en Oracle con `EXISTS` contra la tabla origen (opción B) y ya proyecta columnas explícitas (`oracle_projection`);
  - tiene un checkpoint por tabla en S3 (`_control/_SUCCESS.json`) y exige `id_ejecucion`, que hoy sale de `conf_emisiones` (`idEjecucion` o uno generado);
  - no tiene timeouts fijos. 2 tablas de PCR Vida tienen `lectura_paralela` habilitada (hasta 16 particiones).
- **El proceso de extracción estándar no valida el esquema del JSON:** lee claves puntuales con `.get`. La versión vieja ignora una clave nueva, así que la configuración puede ir **antes** que el código.
- **Glue ignora los argumentos que el script no pide.** Por eso `--EJECUCION_ID` puede llegar desde la Step Function antes que el código que lo usa (*comportamiento de `getResolvedOptions`; se confirma en DEV en el paso 3.3*).
- **Para probar "una tabla por ciclo"** (primera corrida del modelo de Víctor) hay que poder cambiar el número de ciclos, y hoy R3 lo exige fijo (ARQ-59). El arreglo de R3 va antes.
- **El proceso de extracción de Emisiones no tiene pruebas** (`tests/` vacío). Su red de pruebas va antes de cualquier cambio en R4.
- **La opción D necesita los datos de Oracle** (tipos e índices de las llaves) para decidir tabla por tabla entre A, C y D.

### Orden 2: tipo de cambio (para separar ramas y commits)
| Tipo | Rama (V3 del banco: `feature`, `bugfix`, `chore`…) | Tipo de commit | Qué entra |
|---|---|---|---|
| Preparación | `chore/…` | `test`, `chore` | Rebase, red de pruebas, línea base |
| Refactor sin cambio de resultados | `feature/modular-…` | `refactor` | Estructura hexagonal, mismas salidas |
| Fix | `bugfix/…` | `fix` | ARQ-58, ARQ-59, COS-05, ARQ-60 |
| Observabilidad | `feature/obs-…` | `feat` | Log v1, `ejecucion_id`, métricas |
| Feature de rendimiento | `feature/ciclos-…` y `feature/lectura-rangos-…` | `feat`, `perf` | Pausa y presupuesto (ARQ-57); lectura por tramos (COS-11) |

Reglas:
- Una rama por tema. Se usa **el mismo nombre de rama en cada repo que el tema toque**: así cada repo recibe su propio cambio y se revierte solo.
- Un tema por PR, con menos de 400 líneas sin contar pruebas ([[est-pull-requests]]).
- El ticket de Jira va en el nombre de la rama y del commit (V3 5.5 y 5.8). **Todavía no lo tenemos (decisión 0.1).**
- **No se mezclan tipos en un commit:** un `refactor` no corrige nada, y un `fix` no reestructura.

### Orden 3: despliegue y reversa (por pieza)
| Pieza | Cómo se despliega | Cómo se revierte | Regla de orden |
|---|---|---|---|
| R3 (lambda) | Jenkins (zip) | Versión anterior de la lambda | Va **antes** que DDB: primero sabe leer la clave nueva, sin exigirla |
| DDB | Script con respaldo previo de los ítems | Restaurar el respaldo | Va después de R3 y antes que el job |
| R2 (JSON en Git) | Jenkins (sync a S3) | Commit anterior | Igual que DDB. En S3 manda lo que publica R3 |
| R1 (Glue estándar) | Jenkins (script a S3) | Script anterior: la configuración nueva no le hace daño | Va **último** y exige las claves: si faltan, falla con un error clasificado, sin valores por defecto |
| R4 (Glue Emisiones) | Jenkins (script a S3) | Script anterior, siempre que la configuración nueva no tenga valores que no conozca | Valida su configuración: **el código va antes** y acepta las claves nuevas; después se cargan |
| R5 (IaC) | DevOps | DevOps | Los argumentos nuevos van **antes** que el código que los usa |

Patrón: **ampliar → poblar → usar**. Así cada pieza se puede revertir sin romper a las demás.

## Roadmap (lista de verificación)

### Fase 0. Preparación (sin cambio de comportamiento)
- [ ] **0.1 ? Decisión (DevOps, lunes 2026-11-23):**
  - ticket Jira para ramas y commits;
  - qué configurador corre en QA y PRD (la general, las por malla o T2);
  - quién carga `cnf-mallas` en QA y PRD (ningún repo lo hace);
  - de dónde salen las 13 políticas del rol;
  - quién reescribió el proceso de extracción de Emisiones en `quality` el 20 nov, para no cruzar cambios.
- [ ] **0.2 ORA, Consulta Oracle:** QA corre `consultas-oracle-alfaprd.sql` y `consultas-oracle-acsele.sql`. Va en paralelo: solo bloquea la fase 5.
- [ ] **0.3 DEV, Consulta DEV:** línea base del proceso de extracción estándar en las 4 mallas sin medir (Endosos, ARP, RVI y Auxilios), con el método de `medir-tiempos-rentas.sh`. ARL y Rentas ya están medidas (tanda 1).
- [ ] **0.4 DEV, Consulta DEV:** línea base del proceso de extracción de Emisiones (duración, filas y unidades por tabla).
- [ ] **0.5 R1, `chore`:** rebasar la rama `feature/modular-rmd-ext-estandar` (red de caracterización y esqueleto modular) sobre `quality` (`0af0ca8`, que ya trae el fix del SCD) y dejar la red de caracterización en verde.
- [ ] **0.6 R4, `chore`, commit `test`:** red de caracterización del proceso de extracción de Emisiones sobre la versión reescrita el 20 nov (solo pruebas, sin tocar `src/`). Necesita saber en 0.1 quién la está cambiando.
- [ ] **0.7 R1 y R4, por verificar en el código:** quién lee cada campo de `parametros_extraccion.json` (validación, SCD, remediación). Antes del paso 2.3, que cambia sus duraciones.

### Fase 1. Refactor hexagonal (mismos resultados)
- [ ] **1.1 R1, `feature/modular-…`, commits `refactor`, uno por módulo:** configuración, lectura JDBC, escritura, control y parámetros. Cada uno con la red en verde y las mismas salidas.
- [ ] **1.2 DEV, Consulta DEV:** corrida de R1 refactorizado frente a la línea base 0.3: mismas filas, misma suma de control y `exceptAll` igual a 0.
- [ ] **1.3 R4, `feature/modular-…`, commits `refactor`:** lo mismo para Emisiones. Necesita el paso 0.6.
- [ ] **1.4 DEV, Consulta DEV:** equivalencia de R4 frente a la línea base 0.4.

### Fase 2. Fixes (una rama `bugfix/` por brecha)
- [ ] **2.1 R3, `bugfix/arq-59-conteos`:** validar la estructura y no el número de ciclos y registros en los 6 `conf_*.py` de ALFAPRD (`conf_emisiones` no tiene conteos fijos), con pruebas. No toca configuración.
- [ ] **2.2 R3, `bugfix/arq-59-defectos`:** quitar los valores por defecto `MALLA_PK = MALLA#PRIMAS_ARL` y el ambiente `"dev"` (`lambda_function.py:248`). Si faltan, error clasificado. Si en 0.1 aparecen lambdas por malla desplegadas, también ellas (son otros repos).
- [ ] **2.3 R1, `bugfix/arq-58-medicion`:** medir la lectura después de la acción que lee, y la duración total al final real del job. Necesita el paso 0.7.
- [ ] **2.4 COS-05, `bugfix/cos-05-timeout`** (timeout de lectura configurable, en orden):
  - [ ] 2.4.1 **? Decisión:** el valor del timeout, sacado de la lectura más larga medida en 0.3 y 0.4. No se supone.
  - [ ] 2.4.2 **DDB:** agregar la clave en `fuente` del ítem `EXTRACCION`. R3 no cambia, porque `fuente` se copia entera.
  - [ ] 2.4.3 **R2:** la misma clave en los 6 JSON.
  - [ ] 2.4.4 **R1:** leer la clave en vez de los `30000` fijos.
  - El proceso de Emisiones no tiene timeout fijo en la versión actual: no aplica.
- [ ] **2.5 ARQ-60:**
  - [ ] 2.5.1 **DEV, Consulta DEV:** qué copia del script usa cada uno de los 8 jobs de Emisiones.
  - [ ] 2.5.2 **R5, pedido a DevOps:** dejar una sola versión por pipeline y retirar las copias. Sin código de Desarrollo.

### Fase 3. Observabilidad (rama `feature/obs-extraccion`)
- [ ] **3.1 ? Decisión:**
  - aprobar el contrato de log v1 ([[roadmap-observabilidad]], hoy es una propuesta);
  - dónde vive el módulo común de logs para Glue: una copia por paquete, como `niif_observability`, o como librería en R2.
- [ ] **3.2 R5, pedido a DevOps:** `--EJECUCION_ID` = `$$.Execution.Name` en las 6 Step Functions y en `niif-snf-emisiones`. Va antes que el código, porque Glue ignora el argumento mientras nadie lo use.
- [ ] **3.3 DEV, Consulta DEV:** confirmar que los jobs actuales siguen corriendo con el argumento nuevo.
- [ ] **3.4 R1, `feat`:** log v1 (`PASO_INICIO`/`PASO_FIN`, `duracion_ms`, filas, unidades, densidad) con `ejecucion_id`. Se exige el argumento, sin valor por defecto.
- [ ] **3.5 R4, `feat`:** lo mismo. **Por verificar:** su JSON ya trae `id_ejecucion` desde `conf_emisiones`; hay que ver si es el mismo `execution_name`, para no tener dos fuentes del ID.

### Fase 4. Modelo de ciclos (rama `feature/ciclos-extraccion`, ARQ-57)
- [ ] **4.1 ? Decisión:**
  - la pausa por ciclo de ALFAPRD (Víctor dio 1 a 7 minutos, y 3 para Acsel-e);
  - el tamaño del pool, que empieza en 1.
- [ ] **4.2 R3, `feat`:** que el configurador conozca `pausa_segundos`, `presupuesto_unidades_ciclo` y el pool por ciclo, sin exigirlos. Necesita el paso 2.1.
- [ ] **4.3 DDB:** las claves en los ítems `CICLO#`, con respaldo previo.
- [ ] **4.4 R2:** las mismas claves en los 6 JSON.
- [ ] **4.5 R1, `feat`:** aplicar la pausa y el presupuesto, y registrar las unidades por ciclo para aprender el tope. Exige las claves.
- [ ] **4.6 DEV, Consulta DEV:** primera corrida de una tabla por ciclo (necesita los pasos 2.1, 4.3 y 4.5) y medición frente a la línea base.
- [ ] **4.7 R4 + DDB + R2 (`emisiones.json`, `pcr_vida`, `generales`, `satelites`):** lo mismo para Emisiones. **Por verificar:** que `conf_emisiones` deje pasar las claves sin cambios.

### Fase 5. Lectura por tramos, opción D (rama `feature/lectura-rangos`, COS-11)
- [ ] **5.1 ? Decisión, con los resultados de 0.2:**
  - estrategia por tabla (A, C o D);
  - foto consistente (`AS OF SCN`, si hay `FLASHBACK`);
  - zona temporal para los tramos (*propuesta*);
  - referencia de equivalencia para ALFAPRD (*propuesta:* la salida del job actual).
- [ ] **5.2 R3, `feat`:** que conozca `estrategia_lectura`, `columna_rango` y `llaves_por_tramo` por tabla, sin exigirlos.
- [ ] **5.3 DDB:** las claves por tabla en los ítems `CICLO#`, con respaldo.
- [ ] **5.4 R2:** las mismas claves en los 6 JSON.
- [ ] **5.5 R1, `feat`/`perf`:** tramos `BETWEEN` en secuencia por la única conexión, semijoin exacto en Spark, checkpoint por tramo en `ejc-ejecuciones` (misma `PK`, `SK = EJECUCION#<execution_name>#TRAMO#<tabla>#<n>`, *propuesta*) y escritura en la zona temporal.
- [ ] **5.6 DEV, Consulta DEV:** prueba de equivalencia (conteo, suma de control y `exceptAll` igual a 0) y medición.
- [ ] **5.7 R4 + DDB + R2:** lo mismo para Emisiones.
- [ ] **5.8 ORA, Consulta Oracle:** equivalencia de Emisiones contra `acsele_alfa_bi.em_*`, si hay acceso según 0.2.
- [ ] **5.9 ORA:** recomendaciones de índices a Víctor, que las lleva al arquitecto. No se envían directo a la base.

### Fase 6. Cierre
- [ ] **6.1 DEV, Consulta DEV:** medición final frente a la línea base en todas las mallas. Mostrar a Víctor los resultados tabla por tabla y optimizados.
- [ ] **6.2 R5, pedido a DevOps:** ajustar el número de workers del proceso de extracción según la medición. La lectura sigue siendo secuencial por la única conexión, así que el resto de workers se paga sin usarse (RYC-01).
- [ ] **6.3 Memoria y Docs:** cerrar las brechas ARQ-57, ARQ-58, ARQ-59, ARQ-60, COS-05 y COS-11 con su evidencia.

## Qué va en secuencia y qué en paralelo
El roadmap **no es una sola fila**. Las fases marcan el orden de las dependencias, y dentro de ellas hay carriles que avanzan a la vez.

**Siempre en secuencia (cadenas que no se pueden saltar):**
- En cada clave nueva de configuración: R3 → DDB → R2 → R1/R4 (ampliar → poblar → usar). Pasa en 2.4, 4.x y 5.x.
- En cada proceso de extracción: red de pruebas → refactor → fixes → observabilidad → ciclos → tramos. R1: 0.5 → 1.1 → 2.3 → 3.4 → 4.5 → 5.5. R4: 0.6 → 1.3 → 3.5 → 4.7 → 5.7.
- Cada cambio de código se compara contra la línea base antes del siguiente (1.2, 1.4, 4.6, 5.6).
- 3.2 (IaC) → 3.3 (confirmar en DEV) → 3.4 (código que exige el argumento).
- 2.1 (ciclos sin conteo fijo) → 4.2 → 4.6 (una tabla por ciclo).
- La opción D va después de los ciclos (5.5 después de 4.5), porque el presupuesto de cada ciclo se gasta por tramos.

**En paralelo (no se esperan entre sí):**
| Carril | Pasos | Puede empezar |
|---|---|---|
| Oracle | 0.2 | Ya. Solo bloquea la fase 5 |
| Líneas base en DEV | 0.3, 0.4 | Ya |
| Proceso de extracción estándar (R1) | 0.5 → 1.1 → … | Ya |
| Proceso de extracción de Emisiones (R4) | 0.6 → 1.3 → … | Ya. Su carril va en paralelo al de R1, pero cada fase de R4 sigue el patrón probado antes en R1 |
| Configurador (R3) | 2.1 → 2.2 | Ya: es otro repo y no depende del refactor de R1 |
| Pedidos a DevOps | 2.5.2, 3.2 | Después de 0.1 y 2.5.1. 3.2 se puede pedir pronto, porque Glue ignora el argumento mientras no se use |
| Decisiones | 0.1, 2.4.1, 3.1, 4.1, 5.1 | Cada una antes de su paso, no antes de la fase |

## Vista por repo (cuándo se toca cada uno)
| Repo o pieza | Pasos, en orden |
|---|---|
| R1 estándar | 0.5 → 1.1 → 2.3 → 2.4.4 → 3.4 → 4.5 → 5.5 |
| R4 Emisiones | 0.6 → 1.3 → 3.5 → 4.7 → 5.7 |
| R3 configurador | 2.1 → 2.2 → 4.2 → 5.2 |
| R2 JSON | 2.4.3 → 4.4 → 5.4 (y los de Emisiones en 4.7 y 5.7) |
| DDB `cnf-mallas` | 2.4.2 → 4.3 → 5.3 (y Emisiones en 4.7 y 5.7) |
| R5 IaC (DevOps) | 2.5.2 → 3.2 → 6.2 |
| ORA Consulta Oracle | 0.2 → 5.8 → 5.9 |
| DEV Consulta DEV | 0.3, 0.4 → 1.2, 1.4 → 2.5.1 → 3.3 → 4.6 → 5.6 → 6.1 |
