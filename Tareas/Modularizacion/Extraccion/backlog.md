# Backlog: modularización de la extracción (épica EXT)

Backlog en formato Scrum para Jira. Sale del [roadmap de cambios](roadmap-cambios.md): cada historia agrupa los pasos del roadmap que se entregan y se prueban juntos. Los IDs `EXT-xx` son provisionales hasta tener las llaves de Jira. Los puntos los estima el equipo. Importación: `backlog-jira.csv` (ver al final).

## Calendario

- **Inicio:** lunes 23 de noviembre de 2026. **Escenario comprometido:** Rápido (ritmo sostenido, con algún reproceso y respuestas externas en 3 a 5 días).
- **Una fecha se cumple cuando el cambio llega a QA y QA lo aprueba.** Sin congelamiento de cambios en diciembre.
- **Cierre comprometido: viernes 8 de enero de 2027** (aprobado en QA). La semana del 11 al 15 de enero queda de holgura.
- Un desarrollador. Sprints de 2 semanas. Festivos: 8 dic, 25 dic, 1 ene y 11 ene.
- *Supuesto por confirmar:* QA tarda 3 días hábiles en aprobar cada entrega. Si tarda más, se corre la columna "Aprobado en QA", no el desarrollo.

| Sprint | Fechas | Días hábiles | Festivos |
|---|---|---|---|
| S1 | 23 nov a 4 dic | 10 | — |
| S2 | 7 dic a 18 dic | 9 | 8 dic |
| S3 | 21 dic a 1 ene | 8 | 25 dic y 1 ene |
| S4 | 4 ene a 15 ene | 9 | 11 ene |

| Hito | Qué cierra | Historias | En DEV | Aprobado en QA |
|---|---|---|---|---|
| H1 | Preparación: líneas base, redes de pruebas, spikes | EXT-03 a EXT-07 | 27 nov | No aplica (solo pruebas y mediciones) |
| H2 | Refactor de los dos procesos y fixes (ARQ-58, ARQ-59, COS-05, ARQ-60) | EXT-08 a EXT-14 | 10 dic | 15 dic |
| H3 | Observabilidad: ejecucion_id y log v1 | EXT-15 a EXT-17 | 15 dic | 18 dic |
| H4 | Ciclos con pausa y presupuesto (ARQ-57) | EXT-18, EXT-19 | 21 dic | 24 dic |
| H5 | Lectura por tramos (COS-11) | EXT-20, EXT-21 | 4 ene | 7 ene |
| H6 | Cierre: medición final y ajuste de workers | EXT-22, EXT-23 | 5 ene | 8 ene |

**Fechas que dependen de otros** (si se atrasan, se mueve el hito que bloquean):
- Ticket de Jira y respuestas de DevOps (EXT-01): **24 nov**. Sin ticket no se nombran ramas ni commits según la norma V3.
- `--EJECUCION_ID` aplicado por DevOps en DEV (EXT-15): pedirlo el 23 nov; hace falta antes del **10 dic** (bloquea H3).
- Resultados de Oracle (EXT-02): hacen falta antes del **18 dic** (bloquean H5).
- Revisiones de PR (2 revisores) y promoción a QA en cada entrega.

**Si el plan se atrasa:** se recorta alcance antes de mover el cierre. EXT-19 y EXT-21 (ciclos y tramos del proceso de extracción de Emisiones) pasan a una segunda iteración.

## Cómo leerlo

- **Orden:** el número dice la secuencia sugerida. Un ítem se puede tomar cuando todo lo de "Bloqueada por" está terminado. Por eso hay carriles que avanzan en paralelo (ver el roadmap).
- **Tipos:** Historia (entrega código o configuración), Spike (investigación con resultado), Tarea (decisión o pedido a otro equipo). Las subtareas van en el orden en que se hacen: primero el configurador, luego `cnf-mallas`, luego los JSON y al final el job.
- **Roles:** "operador de la malla" en fixes y features; "desarrollador" en preparación y refactor (*propuesta*).
- **Piezas:** R1 = data-datalake-niif-rmd-ext-estandar-glue-job · R2 = cfg-datalake-niif-config-s3 · R3 = sc-datalake-niif-configuration-general · R4 = data-datalake-niif-pcr-emisiones-glue-job · R5 = iac-analytics-datalake-niif-operations-manager (DevOps) · DDB = DynamoDB cnf-mallas · ORA = consulta Oracle · DEV = consulta o corrida en DEV.

## Definición de hecho (todas las historias)

- [ ] Red de pruebas en verde y cobertura del código nuevo mayor a 80 %
- [ ] La configuración cambia en el mismo cambio (JSON, cnf-mallas y configurador)
- [ ] Equivalencia contra la línea base cuando la historia toca datos
- [ ] PR con la plantilla del banco, un solo tema, menos de 400 líneas sin pruebas
- [ ] Brecha cerrada en Docs\06-brechas con su evidencia

## Vista rápida

| # | ID | Sprint | Fecha objetivo | Tipo | Carril | Título | Bloqueada por | Brecha | Prioridad |
|---|---|---|---|---|---|---|---|---|---|
| 01 | EXT-01 | S1 | 24 nov | Tarea (chore) | Preparación | Decisiones de arranque con DevOps | — | - | Alta |
| 02 | EXT-02 | S2 | 18 dic | Spike (spike) | Oracle | Diccionario Oracle de las llaves de extracción | — | COS-11 | Alta |
| 03 | EXT-03 | S1 | 23 nov | Historia (chore) | Línea base | Línea base del proceso de extracción estándar | — | - | Alta |
| 04 | EXT-04 | S1 | 25 nov | Historia (chore) | Línea base | Línea base del proceso de extracción de Emisiones | — | - | Alta |
| 05 | EXT-05 | S1 | 25 nov | Historia (chore) | Proceso de extracción estándar | Red de pruebas del proceso de extracción estándar al día | — | - | Alta |
| 06 | EXT-06 | S1 | 27 nov | Historia (test) | Proceso de extracción de Emisiones | Red de pruebas del proceso de extracción de Emisiones | — | - | Alta |
| 07 | EXT-07 | S1 | 27 nov | Spike (spike) | Proceso de extracción estándar | Quién lee parametros_extraccion.json | — | ARQ-58 | Media |
| 08 | EXT-08 | S1 | 2 dic | Historia (refactor) | Proceso de extracción estándar | Proceso de extracción estándar en módulos hexagonales | EXT-03, EXT-05 | - | Alta |
| 09 | EXT-09 | S2 | 10 dic | Historia (refactor) | Proceso de extracción de Emisiones | Proceso de extracción de Emisiones en módulos hexagonales | EXT-04, EXT-06, EXT-08 | - | Alta |
| 10 | EXT-10 | S1 | 3 dic | Historia (fix) | Configurador | Configurador sin número fijo de ciclos | EXT-01 | ARQ-59 | Media |
| 11 | EXT-11 | S1 | 4 dic | Historia (fix) | Configurador | Configurador sin valores por defecto | EXT-01, EXT-10 | ARQ-59 | Media |
| 12 | EXT-12 | S1 | 4 dic | Historia (fix) | Proceso de extracción estándar | Duraciones reales en la extracción | EXT-07, EXT-08 | ARQ-58 | Media |
| 13 | EXT-13 | S2 | 7 dic | Historia (fix) | Fixes | Timeout de lectura configurable | EXT-03, EXT-04, EXT-08, EXT-09 | COS-05 | Media |
| 14 | EXT-14 | S2 | 10 dic | Historia (fix) | Fixes | Una sola versión del proceso de extracción de Emisiones | EXT-01 | ARQ-60 | Media |
| 15 | EXT-15 | S2 | 10 dic | Historia (obs) | Observabilidad | ejecucion_id hasta los procesos de extracción | EXT-01 | ARQ-32 | Media |
| 16 | EXT-16 | S2 | 14 dic | Historia (obs) | Observabilidad | Log v1 en el proceso de extracción estándar | EXT-12, EXT-15 | ARQ-53 | Media |
| 17 | EXT-17 | S2 | 15 dic | Historia (obs) | Observabilidad | Log v1 en el proceso de extracción de Emisiones | EXT-09, EXT-15, EXT-16 | ARQ-53 | Media |
| 18 | EXT-18 | S2 | 18 dic | Historia (feat) | Rendimiento | Pausa y presupuesto por ciclo (estándar) | EXT-10, EXT-16 | ARQ-57 | Alta |
| 19 | EXT-19 | S3 | 21 dic | Historia (feat) | Rendimiento | Pausa y presupuesto por ciclo (Emisiones) | EXT-17, EXT-18 | ARQ-57 | Alta |
| 20 | EXT-20 | S3 | 29 dic | Historia (feat) | Rendimiento | Lectura por tramos de llave (estándar) | EXT-02, EXT-18 | COS-11 | Alta |
| 21 | EXT-21 | S4 | 4 ene | Historia (feat) | Rendimiento | Lectura por tramos de llave (Emisiones) | EXT-19, EXT-20 | COS-11 | Alta |
| 22 | EXT-22 | S4 | 4 ene | Tarea (chore) | Oracle | Recomendaciones de índices de Oracle | EXT-02, EXT-20 | COS-11 | Baja |
| 23 | EXT-23 | S4 | 5 ene | Historia (chore) | Cierre | Cierre: medición final y ajuste de workers | EXT-21 | RYC-01 | Media |

## Detalle

### EXT-01 · Decisiones de arranque con DevOps
**Tarea** · S1, objetivo 24 nov · tipo `chore` · rama `-` · carril Preparación · prioridad Alta · brecha - · roadmap 0.1

Como desarrollador quiero conocer el ticket, el configurador vigente, quién carga cnf-mallas y el origen de las políticas del rol para no suponer nada al empezar.

Criterios de aceptación:
- [ ] Ticket Jira definido para ramas y commits
- [ ] Se sabe qué configurador corre en QA y PRD (general, por malla o T2)
- [ ] Se sabe quién carga cnf-mallas en QA y PRD
- [ ] Se sabe de qué repo salen las 13 políticas del rol del proceso de extracción

Piezas: R5 (consulta) · Bloqueada por: —

### EXT-02 · Diccionario Oracle de las llaves de extracción
**Spike** · S2, objetivo 18 dic · tipo `spike` · rama `-` · carril Oracle · prioridad Alta · brecha COS-11 · roadmap 0.2

Como desarrollador quiero el tipo, el índice y el tamaño de cada tabla y llave, y los permisos de flashback para decidir tabla por tabla cómo leer (A, C o D).

Criterios de aceptación:
- [ ] consultas-oracle-alfaprd.sql y consultas-oracle-acsele.sql corridos con el usuario de servicio
- [ ] Resultados en CSV, con la base y el usuario de la consulta 0
- [ ] Se sabe si hay FLASHBACK y cuánto UNDO_RETENTION hay en cada base

Piezas: ORA · Bloqueada por: —

### EXT-03 · Línea base del proceso de extracción estándar
**Historia** · S1, objetivo 23 nov · tipo `chore` · rama `chore/linea-base-extraccion` · carril Línea base · prioridad Alta · brecha - · roadmap 0.3

Como desarrollador quiero la duración, las filas y las unidades por tabla de las 6 mallas para comparar cada cambio contra el estado actual.

Criterios de aceptación:
- [ ] Endosos, ARP, RVI y Auxilios medidas (ARL y Rentas ya lo están)
- [ ] Por tabla: duración, filas, columnas y unidades
- [ ] Salidas de referencia guardadas para la equivalencia

Piezas: DEV · Bloqueada por: —

### EXT-04 · Línea base del proceso de extracción de Emisiones
**Historia** · S1, objetivo 25 nov · tipo `chore` · rama `chore/linea-base-extraccion` · carril Línea base · prioridad Alta · brecha - · roadmap 0.4

Como desarrollador quiero la duración, las filas y las unidades por tabla del proceso de extracción de Emisiones para comparar cada cambio contra el estado actual.

Criterios de aceptación:
- [ ] Medidas las mallas que usa (Emisiones, PCR Vida, Generales y Satélites)
- [ ] Por tabla: duración, filas, columnas y unidades
- [ ] Salidas de referencia guardadas para la equivalencia

Piezas: DEV · Bloqueada por: —

### EXT-05 · Red de pruebas del proceso de extracción estándar al día
**Historia** · S1, objetivo 25 nov · tipo `chore` · rama `feature/modular-rmd-ext-estandar` · carril Proceso de extracción estándar · prioridad Alta · brecha - · roadmap 0.5

Como desarrollador quiero la red de caracterización sobre la quality actual para refactorizar sin cambiar resultados.

Criterios de aceptación:
- [ ] La rama de trabajo parte de quality (0af0ca8, con el fix del SCD)
- [ ] La red de caracterización corre en verde

Piezas: R1 · Bloqueada por: —

### EXT-06 · Red de pruebas del proceso de extracción de Emisiones
**Historia** · S1, objetivo 27 nov · tipo `test` · rama `chore/red-pruebas-emisiones` · carril Proceso de extracción de Emisiones · prioridad Alta · brecha - · roadmap 0.6

Como desarrollador quiero pruebas que fijen el comportamiento actual del proceso de extracción de Emisiones para poder cambiarlo sin romper resultados.

Criterios de aceptación:
- [ ] Pruebas de caracterización de la lectura, las dependencias y la escritura
- [ ] No se toca src/
- [ ] Corren en verde en local

Piezas: R4 · Bloqueada por: —

### EXT-07 · Quién lee parametros_extraccion.json
**Spike** · S1, objetivo 27 nov · tipo `spike` · rama `-` · carril Proceso de extracción estándar · prioridad Media · brecha ARQ-58 · roadmap 0.7

Como desarrollador quiero saber qué jobs leen cada campo de parametros_extraccion.json para cambiar las duraciones sin romper a nadie.

Criterios de aceptación:
- [ ] Lista de campos y de jobs que los leen (validación, SCD, remediación)
- [ ] Conclusión: qué se puede cambiar y qué no

Piezas: R1, R4 (lectura) · Bloqueada por: —

### EXT-08 · Proceso de extracción estándar en módulos hexagonales
**Historia** · S1, objetivo 2 dic · tipo `refactor` · rama `feature/modular-rmd-ext-estandar` · carril Proceso de extracción estándar · prioridad Alta · brecha - · roadmap 1.1, 1.2

Como desarrollador quiero el proceso de extracción estándar separado en dominio, puertos y adaptadores para corregir y mejorar cada parte sin tocar las demás.

Criterios de aceptación:
- [ ] Mismas salidas: red en verde
- [ ] En DEV, igual a la línea base: filas, suma de control y exceptAll = 0
- [ ] Un commit refactor por módulo

Subtareas (en orden):
1. Módulo de configuración
2. Adaptador de lectura JDBC
3. Adaptador de escritura
4. Control y parámetros de extracción
5. Equivalencia en DEV contra la línea base

Piezas: R1 · Bloqueada por: EXT-03, EXT-05

### EXT-09 · Proceso de extracción de Emisiones en módulos hexagonales
**Historia** · S2, objetivo 10 dic · tipo `refactor` · rama `feature/modular-pcr-emisiones` · carril Proceso de extracción de Emisiones · prioridad Alta · brecha - · roadmap 1.3, 1.4

Como desarrollador quiero el proceso de extracción de Emisiones con la misma estructura que el estándar para mantener los dos procesos de extracción con un solo patrón.

Criterios de aceptación:
- [ ] Mismas salidas: red en verde
- [ ] En DEV, igual a su línea base: filas, suma de control y exceptAll = 0

Subtareas (en orden):
1. Módulos según el patrón de EXT-08
2. Equivalencia en DEV contra la línea base

Piezas: R4 · Bloqueada por: EXT-04, EXT-06, EXT-08

### EXT-10 · Configurador sin número fijo de ciclos
**Historia** · S1, objetivo 3 dic · tipo `fix` · rama `bugfix/arq-59-conteos` · carril Configurador · prioridad Media · brecha ARQ-59 · roadmap 2.1

Como operador de la malla quiero agregar o quitar ciclos y registros sin que falle el configurador para probar una tabla por ciclo y ajustar la malla sin desplegar.

Criterios de aceptación:
- [ ] Valida la estructura, no el número de ciclos ni de registros
- [ ] Aplica a todos los conf_*.py, con pruebas
- [ ] Una malla con un ciclo más publica bien su JSON

Piezas: R3 · Bloqueada por: EXT-01

### EXT-11 · Configurador sin valores por defecto
**Historia** · S1, objetivo 4 dic · tipo `fix` · rama `bugfix/arq-59-defectos` · carril Configurador · prioridad Media · brecha ARQ-59 · roadmap 2.2

Como operador de la malla quiero que el configurador falle si falta la malla o el ambiente para que nunca publique la configuración de otra malla u otro ambiente.

Criterios de aceptación:
- [ ] Sin MALLA_PK = MALLA#PRIMAS_ARL por defecto
- [ ] Sin ambiente "dev" por defecto
- [ ] Si faltan, error clasificado

Piezas: R3 · Bloqueada por: EXT-01, EXT-10

### EXT-12 · Duraciones reales en la extracción
**Historia** · S1, objetivo 4 dic · tipo `fix` · rama `bugfix/arq-58-medicion` · carril Proceso de extracción estándar · prioridad Media · brecha ARQ-58 · roadmap 2.3

Como operador de la malla quiero que la duración de lectura y la total sean las reales para saber dónde se va el tiempo de la corrida.

Criterios de aceptación:
- [ ] La lectura se mide después de la acción que lee
- [ ] La duración total cierra al final real del job
- [ ] Los lectores de parametros_extraccion.json siguen funcionando

Piezas: R1 · Bloqueada por: EXT-07, EXT-08

### EXT-13 · Timeout de lectura configurable
**Historia** · S2, objetivo 7 dic · tipo `fix` · rama `bugfix/cos-05-timeout` · carril Fixes · prioridad Media · brecha COS-05 · roadmap 2.4

Como operador de la malla quiero fijar el timeout de lectura por configuración para que una lectura lenta no corte la extracción.

Criterios de aceptación:
- [ ] Sin la clave, el job falla con un error clasificado
- [ ] Con la clave, R1 y R4 usan ese valor
- [ ] Equivalencia contra la línea base = 0 diferencias

Subtareas (en orden):
1. Decisión: valor según la lectura más larga medida
2. cnf-mallas: clave en fuente (con respaldo)
3. cfg-datalake-niif-config-s3: clave en los JSON
4. Proceso de extracción estándar lee la clave
5. Proceso de extracción de Emisiones lee la clave

Piezas: DDB, R2, R1, R4 · Bloqueada por: EXT-03, EXT-04, EXT-08, EXT-09

### EXT-14 · Una sola versión del proceso de extracción de Emisiones
**Historia** · S2, objetivo 10 dic · tipo `fix` · rama `-` · carril Fixes · prioridad Media · brecha ARQ-60 · roadmap 2.5

Como operador de la malla quiero una sola versión del proceso de extracción de Emisiones desplegada por pipeline para saber qué código corre en cada job.

Criterios de aceptación:
- [ ] Inventario en DEV de qué copia usa cada job
- [ ] DevOps deja una sola versión y retira las copias

Subtareas (en orden):
1. Inventario en DEV
2. Pedido a DevOps

Piezas: DEV, R5 · Bloqueada por: EXT-01

### EXT-15 · ejecucion_id hasta los procesos de extracción
**Historia** · S2, objetivo 10 dic · tipo `obs` · rama `-` · carril Observabilidad · prioridad Media · brecha ARQ-32 · roadmap 3.2, 3.3

Como operador de la malla quiero que cada corrida de extracción reciba el ejecucion_id para seguir una corrida de punta a punta.

Criterios de aceptación:
- [ ] Las 6 Step Functions y niif-snf-emisiones pasan --EJECUCION_ID = $$.Execution.Name
- [ ] En DEV, los jobs actuales siguen corriendo con el argumento nuevo

Subtareas (en orden):
1. Pedido a DevOps (IaC)
2. Confirmación en DEV

Piezas: R5, DEV · Bloqueada por: EXT-01

### EXT-16 · Log v1 en el proceso de extracción estándar
**Historia** · S2, objetivo 14 dic · tipo `obs` · rama `feature/obs-extraccion` · carril Observabilidad · prioridad Media · brecha ARQ-53 · roadmap 3.1, 3.4

Como operador de la malla quiero logs JSON por paso con ejecucion_id, duración, filas y unidades para ubicar una falla o una lentitud sin leer el código.

Criterios de aceptación:
- [ ] Eventos PASO_INICIO y PASO_FIN con duracion_ms, filas, unidades y densidad
- [ ] Cada línea lleva ejecucion_id; sin el argumento, el job falla
- [ ] Contrato de log v1 aprobado antes de empezar

Subtareas (en orden):
1. Decisión: contrato v1 y dónde vive el módulo común
2. Implementación en el proceso de extracción estándar

Piezas: R1 · Bloqueada por: EXT-12, EXT-15

### EXT-17 · Log v1 en el proceso de extracción de Emisiones
**Historia** · S2, objetivo 15 dic · tipo `obs` · rama `feature/obs-extraccion` · carril Observabilidad · prioridad Media · brecha ARQ-53 · roadmap 3.5

Como operador de la malla quiero los mismos logs v1 en el proceso de extracción de Emisiones para ver las dos extracciones de la misma forma.

Criterios de aceptación:
- [ ] Mismos eventos y campos que EXT-16
- [ ] Una sola fuente del ID: se aclara el id_ejecucion que ya trae su JSON

Piezas: R4 · Bloqueada por: EXT-09, EXT-15, EXT-16

### EXT-18 · Pausa y presupuesto por ciclo (estándar)
**Historia** · S2, objetivo 18 dic · tipo `feat` · rama `feature/ciclos-extraccion` · carril Rendimiento · prioridad Alta · brecha ARQ-57 · roadmap 4.1 a 4.6

Como operador de la malla quiero una pausa y un presupuesto de unidades por ciclo, configurables para no saturar Oracle por la única conexión de servicio.

Criterios de aceptación:
- [ ] Pausa y presupuesto por ciclo leídos de la configuración
- [ ] Se registran las unidades por ciclo para aprender el tope
- [ ] Corrida de una tabla por ciclo en DEV, medida contra la línea base

Subtareas (en orden):
1. Decisión: pausa de ALFAPRD y tamaño del pool
2. Configurador conoce las claves sin exigirlas
3. cnf-mallas: claves en CICLO# (con respaldo)
4. cfg-datalake-niif-config-s3: claves en los 6 JSON
5. Proceso de extracción estándar aplica pausa y presupuesto
6. Medición en DEV

Piezas: R3, DDB, R2, R1 · Bloqueada por: EXT-10, EXT-16

### EXT-19 · Pausa y presupuesto por ciclo (Emisiones)
**Historia** · S3, objetivo 21 dic · tipo `feat` · rama `feature/ciclos-extraccion` · carril Rendimiento · prioridad Alta · brecha ARQ-57 · roadmap 4.7

Como operador de la malla quiero el mismo modelo de ciclos en el proceso de extracción de Emisiones para no saturar Acsel-e por la única conexión de servicio.

Criterios de aceptación:
- [ ] Mismos criterios que EXT-18
- [ ] Se verifica que conf_emisiones deja pasar las claves

Subtareas (en orden):
1. cnf-mallas de Emisiones
2. JSON de Emisiones
3. Proceso de extracción de Emisiones
4. Medición en DEV

Piezas: DDB, R2, R4 · Bloqueada por: EXT-17, EXT-18

### EXT-20 · Lectura por tramos de llave (estándar)
**Historia** · S3, objetivo 29 dic · tipo `feat` · rama `feature/lectura-rangos` · carril Rendimiento · prioridad Alta · brecha COS-11 · roadmap 5.1 a 5.6

Como operador de la malla quiero que las tablas hijas lean solo las llaves que necesitan, por tramos para no leer completas tablas de millones de filas.

Criterios de aceptación:
- [ ] Tramos BETWEEN en secuencia por la única conexión y semijoin exacto en Spark
- [ ] Checkpoint por tramo para retomar
- [ ] Equivalencia contra la referencia = 0 diferencias

Subtareas (en orden):
1. Decisión: estrategia por tabla, foto consistente, zona temporal y referencia
2. Configurador conoce las claves
3. cnf-mallas: claves por tabla
4. cfg-datalake-niif-config-s3: claves
5. Proceso de extracción estándar lee por tramos
6. Equivalencia y medición en DEV

Piezas: R3, DDB, R2, R1 · Bloqueada por: EXT-02, EXT-18

### EXT-21 · Lectura por tramos de llave (Emisiones)
**Historia** · S4, objetivo 4 ene · tipo `feat` · rama `feature/lectura-rangos` · carril Rendimiento · prioridad Alta · brecha COS-11 · roadmap 5.7, 5.8

Como operador de la malla quiero la misma lectura por tramos en el proceso de extracción de Emisiones para bajar la carga sobre Acsel-e.

Criterios de aceptación:
- [ ] Mismos criterios que EXT-20
- [ ] Equivalencia contra acsele_alfa_bi.em_*, si hay acceso

Subtareas (en orden):
1. cnf-mallas de Emisiones
2. JSON de Emisiones
3. Proceso de extracción de Emisiones
4. Equivalencia contra em_*

Piezas: DDB, R2, R4, ORA · Bloqueada por: EXT-19, EXT-20

### EXT-22 · Recomendaciones de índices de Oracle
**Tarea** · S4, objetivo 4 ene · tipo `chore` · rama `-` · carril Oracle · prioridad Baja · brecha COS-11 · roadmap 5.9

Como desarrollador quiero recomendaciones de índices basadas en lo medido para que Arquitectura las evalúe con la célula dueña de la base.

Criterios de aceptación:
- [ ] Entregadas a Víctor, que las lleva al arquitecto
- [ ] Nada se envía directo a la base

Piezas: ORA · Bloqueada por: EXT-02, EXT-20

### EXT-23 · Cierre: medición final y ajuste de workers
**Historia** · S4, objetivo 5 ene · tipo `chore` · rama `-` · carril Cierre · prioridad Media · brecha RYC-01 · roadmap 6.1 a 6.3

Como operador de la malla quiero la medición final de las mallas y los workers ajustados a lo que se usa para mostrar la mejora y no pagar capacidad ociosa.

Criterios de aceptación:
- [ ] Medición final contra la línea base en todas las mallas
- [ ] Resultados tabla por tabla mostrados a Víctor
- [ ] DevOps ajusta los workers según la medición
- [ ] Brechas cerradas con su evidencia

Subtareas (en orden):
1. Medición final en DEV
2. Pedido a DevOps: workers
3. Cierre de brechas

Piezas: DEV, R5 · Bloqueada por: EXT-21

## Importar a Jira

- Archivo: `backlog-jira.csv` (UTF-8, separado por comas). Una fila por épica, historia, spike, tarea y subtarea.
- `Issue Id` y `Parent Id` arman la jerarquía (épica → historia → subtarea). Las columnas `Bloqueada por` (repetidas) se mapean al enlace *is blocked by*.
- `Sprint` lleva el nombre del sprint (`EXT S1` a `EXT S4`) y `Due Date` la fecha objetivo (AAAA-MM-DD). Los sprints deben existir en el tablero antes de importar.
- **Por verificar al importar:** los nombres de los tipos de issue (`Epic`, `Story`, `Task`, `Sub-task` o su versión en español), el tipo `Spike` (si no existe, usar `Task` con la etiqueta `spike`), los valores de prioridad y el mapeo de los enlaces. Dependen de cómo está configurado el proyecto en Jira.
