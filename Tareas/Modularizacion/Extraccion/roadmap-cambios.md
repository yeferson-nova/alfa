# Roadmap de cambios: modularización de la extracción

Orden de los cambios por repo, como lista de verificación. Cada paso dice en qué repo va (o si es una consulta sin repo), de qué tipo es y qué lo bloquea. El objetivo es que cada commit tenga un solo tema, se pueda revertir solo y se lea limpio en el historial.

Elaborado el 2026-11-20 sobre `alfa-repos` main `3d5f623` (quality del 2026-11-18 al 20). Lo que no está verificado aparece como **decisión** o **por verificar**.

## Piezas
| Clave | Pieza | Qué es | Quién la cambia |
|---|---|---|---|
| R1 | `data-datalake-niif-rmd-ext-estandar-glue-job` | Extractor estándar (6 mallas ALFAPRD) | Nosotros |
| R2 | `cfg-datalake-niif-config-s3` | JSON de extracción en Git (copia coherente; la de S3 la sobrescribe el configurador) | Nosotros |
| R3 | `sc-datalake-niif-configuration-general` | Configurador: lee `cnf-mallas` y publica el JSON | Nosotros |
| R4 | `data-datalake-niif-pcr-emisiones-glue-job` | Extractor de Emisiones (Acsel-e) | Nosotros |
| R5 | `iac-analytics-datalake-niif-operations-manager` | Step Functions, jobs e IAM | DevOps (solo lectura para nosotros) |
| DDB | DynamoDB `niif-{env}-cnf-mallas` | Configuración de origen. **Ningún repo la carga** | Script nuestro en DEV; en QA y PRD, DevOps |
| ORA | Consulta Oracle | Diccionario de Oracle, sin repo | Julio |
| DEV | Consulta DEV | CloudShell o corrida de prueba en DEV, sin repo | Nosotros |
| ? | Decisión | Algo que hay que preguntar o decidir antes del paso | Usuario, DevOps o arquitecto |

## Análisis en tres órdenes

### Orden 1: dependencias (qué necesita qué)
Hechos verificados en el código que fijan el orden:
- **Una clave nueva de configuración viaja por tres piezas: DDB → R3 → JSON de S3 → job.** El configurador arma cada tabla con una lista fija de campos (`orden`, `schema`, `tabla`, `tabla_relativa`, `tipo_tabla`, `where`, `dependencia`, `segmentacion_horizontal`; `conf_arl.py`, `construir_ciclos`). **Una clave nueva por tabla o por ciclo se pierde si R3 no la conoce.** En cambio, el bloque `fuente` se copia entero (`construir_extraccion`), así que una clave nueva ahí pasa sin tocar R3.
- **Emisiones copia `ciclos` entero** (`conf_emisiones.py`, `copy.deepcopy`). Ahí una clave nueva por ciclo o por tabla pasa sin tocar el configurador (*por verificar con una prueba* en su paso).
- **El extractor estándar no valida el esquema del JSON:** lee claves puntuales con `.get`. La versión vieja ignora una clave nueva, así que la configuración puede ir **antes** que el código.
- **Glue ignora los argumentos que el script no pide.** Por eso `--EJECUCION_ID` puede llegar desde la Step Function antes que el código que lo usa (*comportamiento de `getResolvedOptions`; se confirma en DEV en el paso 3.3*).
- **Para probar "una tabla por ciclo"** (primera corrida del modelo de Víctor) hay que poder cambiar el número de ciclos, y hoy R3 lo exige fijo (ARQ-59). El arreglo de R3 va antes.
- **El extractor de Emisiones no tiene pruebas** (`tests/` vacío). Su red de pruebas va antes de cualquier cambio en R4.
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
- Una rama por tema. Se usa **el mismo nombre de rama en cada repo que el tema toque**: así `generar-parches.ps1` saca un parche por repo y cada repo se revierte solo.
- Un tema por PR, con menos de 400 líneas sin contar pruebas ([[est-pull-requests]]).
- El ticket de Jira va en el nombre de la rama y del commit (V3 5.5 y 5.8). **Todavía no lo tenemos (decisión 0.1).**
- **No se mezclan tipos en un commit:** un `refactor` no corrige nada, y un `fix` no reestructura.

### Orden 3: despliegue y reversa (por pieza)
| Pieza | Cómo se despliega | Cómo se revierte | Regla de orden |
|---|---|---|---|
| R3 (lambda) | Jenkins (zip) | Versión anterior de la lambda | Va **antes** que DDB: primero sabe leer la clave nueva, sin exigirla |
| DDB | Script con respaldo previo de los ítems | Restaurar el respaldo | Va después de R3 y antes que el job |
| R2 (JSON en Git) | Jenkins (sync a S3) | Commit anterior | Igual que DDB. En S3 manda lo que publica R3 |
| R1 / R4 (Glue) | Jenkins (script a S3) | Script anterior: la configuración nueva no le hace daño | Va **último** y exige las claves: si faltan, falla con un error clasificado, sin valores por defecto |
| R5 (IaC) | DevOps | DevOps | Los argumentos nuevos van **antes** que el código que los usa |

Patrón: **ampliar → poblar → usar**. Así cada pieza se puede revertir sin romper a las demás.

## Roadmap (lista de verificación)

### Fase 0. Preparación (sin cambio de comportamiento)
- [ ] **0.1 ? Decisión (DevOps, lunes 2026-11-23):**
  - ticket Jira para ramas y commits;
  - qué configurador corre en QA y PRD (la general, las por malla o T2);
  - quién carga `cnf-mallas` en QA y PRD (ningún repo lo hace);
  - de dónde salen las 13 políticas del rol.
- [ ] **0.2 ORA, Consulta Oracle:** Julio corre `consultas-oracle-alfaprd.sql` y `consultas-oracle-acsele.sql`. Va en paralelo: solo bloquea la fase 5.
- [ ] **0.3 DEV, Consulta DEV:** línea base del extractor estándar en las 4 mallas sin medir (Endosos, ARP, RVI y Auxilios), con el método de `medir-tiempos-rentas.sh`. ARL y Rentas ya están medidas (tanda 1).
- [ ] **0.4 DEV, Consulta DEV:** línea base del extractor de Emisiones (duración, filas y unidades por tabla).
- [ ] **0.5 R1, `chore`:** rebasar `feature/modular-rmd-ext-estandar` (commit `5676ef5`) sobre `main` `3d5f623` y dejar la red de caracterización en verde.
- [ ] **0.6 R4, `chore`, commit `test`:** red de caracterización del extractor de Emisiones (solo pruebas, sin tocar `src/`).
- [ ] **0.7 R1 y R4, por verificar en el código:** quién lee cada campo de `parametros_extraccion.json` (validación, SCD, remediación). Antes del paso 2.3, que cambia sus duraciones.

### Fase 1. Refactor hexagonal (mismos resultados)
- [ ] **1.1 R1, `feature/modular-…`, commits `refactor`, uno por módulo:** configuración, lectura JDBC, escritura, control y parámetros. Cada uno con la red en verde y las mismas salidas.
- [ ] **1.2 DEV, Consulta DEV:** corrida de R1 refactorizado frente a la línea base 0.3: mismas filas, misma suma de control y `exceptAll` igual a 0.
- [ ] **1.3 R4, `feature/modular-…`, commits `refactor`:** lo mismo para Emisiones. Necesita el paso 0.6.
- [ ] **1.4 DEV, Consulta DEV:** equivalencia de R4 frente a la línea base 0.4.

### Fase 2. Fixes (una rama `bugfix/` por brecha)
- [ ] **2.1 R3, `bugfix/arq-59-conteos`:** validar la estructura y no el número de ciclos y registros en todos los `conf_*.py`, con pruebas. No toca configuración.
- [ ] **2.2 R3, `bugfix/arq-59-defectos`:** quitar los valores por defecto `MALLA_PK = MALLA#PRIMAS_ARL` y el ambiente `"dev"` (`lambda_function.py:248`). Si faltan, error clasificado. Si en 0.1 aparecen lambdas por malla desplegadas, también ellas (son otros repos).
- [ ] **2.3 R1, `bugfix/arq-58-medicion`:** medir la lectura después de la acción que lee, y la duración total al final real del job. Necesita el paso 0.7.
- [ ] **2.4 COS-05, `bugfix/cos-05-timeout`** (timeout de lectura configurable, en orden):
  - [ ] 2.4.1 **? Decisión:** el valor del timeout, sacado de la lectura más larga medida en 0.3 y 0.4. No se supone.
  - [ ] 2.4.2 **DDB:** agregar la clave en `fuente` del ítem `EXTRACCION`. R3 no cambia, porque `fuente` se copia entera.
  - [ ] 2.4.3 **R2:** la misma clave en los 6 JSON.
  - [ ] 2.4.4 **R1:** leer la clave en vez de los `30000` fijos.
  - [ ] 2.4.5 **R4:** lo mismo con sus timeouts fijos (`120000` y `21600000`).
- [ ] **2.5 ARQ-60:**
  - [ ] 2.5.1 **DEV, Consulta DEV:** qué copia del script usa cada uno de los 8 jobs de Emisiones.
  - [ ] 2.5.2 **R5, pedido a DevOps:** dejar una sola versión por pipeline y retirar las copias. Sin código nuestro.

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
- [ ] **6.2 R5, pedido a DevOps:** ajustar el número de workers del extractor según la medición. La lectura sigue siendo secuencial por la única conexión, así que el resto de workers se paga sin usarse (RYC-01).
- [ ] **6.3 Memoria y Docs:** cerrar las brechas ARQ-57, ARQ-58, ARQ-59, ARQ-60, COS-05 y COS-11 con su evidencia.

## Vista por repo (cuándo se toca cada uno)
| Repo o pieza | Pasos, en orden |
|---|---|
| R1 estándar | 0.5 → 1.1 → 2.3 → 2.4.4 → 3.4 → 4.5 → 5.5 |
| R4 Emisiones | 0.6 → 1.3 → 2.4.5 → 3.5 → 4.7 → 5.7 |
| R3 configurador | 2.1 → 2.2 → 4.2 → 5.2 |
| R2 JSON | 2.4.3 → 4.4 → 5.4 (y los de Emisiones en 4.7 y 5.7) |
| DDB `cnf-mallas` | 2.4.2 → 4.3 → 5.3 (y Emisiones en 4.7 y 5.7) |
| R5 IaC (DevOps) | 2.5.2 → 3.2 → 6.2 |
| ORA Consulta Oracle | 0.2 → 5.8 → 5.9 |
| DEV Consulta DEV | 0.3, 0.4 → 1.2, 1.4 → 2.5.1 → 3.3 → 4.6 → 5.6 → 6.1 |
