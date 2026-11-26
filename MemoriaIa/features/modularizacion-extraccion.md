---
tipo: feature
estado: vigente
actualizado: 2026-11-25
---
# Modularización de la extracción

> Rediseño del proceso de extracción estándar según el modelo de ciclos de Víctor: una conexión de servicio, pool controlado por el job, pausa entre ciclos y dependencias por rangos de llave (opción D, en refinamiento). Sin código todavía.

Humano: `Tareas\Modularizacion\Extraccion\README.md` y `diseno-lectura-por-rangos.md`. Reunión: [[reunion-extraccion-victor]]. Paraguas: [[modularizacion]].

## Decisiones (usuario, 2026-11-20)
- **Entregable (decisión del usuario, 2026-11-20):** cerrar la deuda técnica y optimizar la extracción que ya existe, iterándola: código Python con estándares de industria (hexagonal, PEP 8), observabilidad y menos carga en Oracle. Alcance: **los dos procesos de extracción**, el estándar (6 mallas ALFAPRD) y `extraccion-estandar-emisiones` (Acsel-e: Emisiones, PCR Vida, Generales y Satélites); los dos son jobs de Glue de los pasos 2 y 3, nuestro alcance.
- Sin ODS ni bases de lectura: todo va a la Oracle actual y eso no cambia. Una sola conexión de servicio; el pool de peticiones es nuestro.
- Las consultas base (`M1` a `M4` en `Docs\05-arquitectura\Reuniones\Extraccion\`) son la referencia de resultados: se pueden afinar, pero sus resultados no cambian.
- No hay un tope de unidades dado por nadie: se aprende con la volumetría (transcripción L418-432, L515, L535). Ya no quedan preguntas para Víctor.
- Estrategia de dependencias: **D** (rangos de llave + semijoin exacto). A (tabla completa) solo cuando la tabla completa cabe en un tramo; C (`IN` en lotes de 900) solo para llaves de texto o dispersas; B (join en Oracle, la "relacional" de PCR) descartada.
- AWS DMS no está en la arquitectura (ni diagrama, ni IaC, ni memoria): fue solo un ejemplo del patrón. No se usa.
- "Tabla chica" se decide con datos medidos (`parametros_extraccion.json` o `ALL_TABLES.NUM_ROWS`), nunca con la etiqueta `tipo_tabla`.

## Hechos verificados
- Las consultas base hacen `CREATE TABLE acsele_alfa_bi.em_N AS SELECT /*+ parallel(3) */ ... em_{N-1} JOIN acsele_alfa.<tabla>`: la dependencia es un join con el paso anterior, que está indexado. Las `em_*` se conservan hasta certificar la malla 4.
- El script de DEV es igual al repo (sha `792c1d26`). Servicios: ALFAPRD en DEV → ALPRODAC; Emisiones → ACSSOP; PCR Vida → ACSDES.
- Brechas ARQ-57, COS-11, ARQ-58 y ARQ-59: [[auditoria-extraccion-2026-11]]. Las previas: RYC-01 y COS-05.
- **Rol del proceso de extracción (DEV, 2026-11-20):** tiene DynamoDB `GetItem`, `PutItem`, `UpdateItem` y `Query` sobre `niif-{env}-ejc-ejecuciones` y `niif-{env}-pipeline-estado` por la política administrada común `glue-jobs-common-policy` (IaC `operations-manager`, `glue_jobs/data.tf:2928-2943`, adjunta en DEV). No tiene acceso a `cnf-mallas` ni lo necesita: la configuración le llega en el JSON que publica el configurador. Las 10 políticas en línea no tienen DynamoDB. **13 políticas no están en `alfa-repos`** (ni en la `quality` del 2026-11-20 de los 33 repos): 9 en línea (`...-gestor-configuracion` y 8 `pcr-vida-*`) y 4 administradas (`satelites-assets`, `validacion-oracle-emisiones`, `extraccion-estandar-emisiones-s3`, `glue-read-acsele-connections`). El rol se comparte con Emisiones, PCR Vida y Satélites.
- **Las 6 Step Functions** que usan el proceso de extracción (ARL, Rentas, Endosos, ARP, RVI y Auxilios) solo le pasan `--RUTA_CONFIG_EXTRACCION` (sin `--EJECUCION_ID`). PCR Vida salió en `quality` del 2026-11-18: ahora va por `niif-snf-emisiones` con el job `extraccion-estandar-emisiones`.
- **Sync del 2026-11-20** (`alfa-repos` main `3d5f623`): el script del proceso de extracción no cambió (sha `792c1d26`, igual a DEV). El fix del SCD (`bugfix/scd-freno-periodo`) ya está en `quality`: pruebas idénticas, solo difieren líneas de `logging`.
- **`ejc-ejecuciones`** (103 ítems en DEV) ya guarda `executionName`, `estado`, `fechaInicio`/`fechaFin`, `malla` y `periodoNegocio`: es la fuente natural del `ejecucion_id`. **`cnf-mallas`** (67 ítems) guarda los ciclos con `orden`, `activo` y `segmentos`.
- **`datalake-dev-adm-extraccion-costo`, `-parametro` y `-ejecucion`** existen (IaC `iac-analytics-datalake-niif`, `database/parameters.tf:154-210`), vacías desde 2025-09-21 y sin código que las use. Prefijo viejo `datalake-`, no `niif-`: no se adoptan sin aval.
- Evidencia interna: `EvidenciasAws\dev-extraccion\2026-11-20-tanda1.md` y `2026-11-20-tanda3-dynamodb-iam.md`.

## Repos afectados
- **Cambian (nuestros):** [[data-datalake-niif-rmd-ext-estandar-glue-job]] (proceso de extracción estándar), [[data-datalake-niif-pcr-emisiones-glue-job]] (proceso de extracción de Emisiones; sus copias en DEV son ARQ-60), [[cfg-datalake-niif-config-s3]] (6 `config_extraccion_*.json` de ALFAPRD y los de Emisiones: `emisiones.json`, `pcr_vida`, `generales`, `satelites`), [[sc-datalake-niif-configuration-general]] (`conf_*.py` y `conf_emisiones.py`, ARQ-59) y [[sc-datalake-niif-configurador-mallas]] si es el que queda.
- **Cambian (DevOps; nosotros entregamos el cambio):** [[iac-analytics-datalake-niif-operations-manager]]: `--EJECUCION_ID` en las 6 Step Functions y en `niif-snf-emisiones`. Permisos de DynamoDB: no hacen falta si el checkpoint va en `ejc-ejecuciones`.
- **Se rozan:** las 6 configuradoras por malla (tienen el mismo módulo copiado: si alguna corre, pisa las claves nuevas); el rol IAM es uno solo para los dos procesos de extracción; los lectores de Bronce (validación técnica en [[data-datalake-niif-auxilios-funerarios-glue-job]] y crawlers) si cambia la forma de escribir.

## Roadmap de cambios
- `Tareas\Modularizacion\Extraccion\roadmap-cambios.md`: fases 0 (preparación) → 1 (refactor) → 2 (fixes) → 3 (observabilidad) → 4 (ciclos) → 5 (opción D) → 6 (cierre), con una rama por tema y el mismo nombre de rama en cada repo.
- Orden de despliegue verificado: **ampliar → poblar → usar.** El configurador primero conoce la clave sin exigirla (lista fija de campos por tabla en `construir_ciclos`; `fuente` se copia entera), después se carga `cnf-mallas` (ningún repo la carga) y al final el job la exige. El proceso de extracción no valida el esquema y la versión vieja ignora las claves nuevas. El repo de Emisiones no tiene pruebas.

- Backlog Scrum para Jira: `Tareas/Modularizacion/Extraccion/backlog.md` y `backlog-jira.csv` (épica EXT, 23 ítems, 40 subtareas; IDs provisionales hasta tener el ticket). Vista para gerencia en `resumen-ejecutivo.md` (6 temas, calendario y dependencias, sin IDs técnicos). Una sola épica y roles "operador de la malla" y "desarrollador" son *propuesta*.

- **Calendario (decisión del usuario, 2026-11-20):** inicio el lunes 2026-11-23; escenario **Rápido**; sprints de 2 semanas (S1 23 nov-4 dic, S2 7-18 dic, S3 21 dic-1 ene, S4 4-15 ene); **cada fecha = llegó a QA y QA lo aprobó**; sin congelamiento en diciembre. Cierre comprometido: **2027-01-08** (aprobado en QA), con holgura del 11 al 15 ene. *Supuesto:* QA aprueba en 3 días hábiles. Si hay atraso, se recortan EXT-19 y EXT-21 antes de mover el cierre. Escenarios descartados: Acelerado (21 dic) y Conservador (18 ene, ≈ 2 meses).

- **Validación final del plan (2026-11-20, contra `quality`):** el proceso de extracción de Emisiones se **reescribió en `quality` el 2026-11-20** (3392 → 819 líneas, más `validacion_tecnica_emisiones.py`). La versión nueva valida su configuración (rechaza valores de `estrategia_lectura` que no conoce: en R4 va primero el código), ya tiene `estrategia_lectura` (`SQL_CONFIGURADA`, `SQL_CONFIGURADA_UNA_TABLA`, `DEPENDENCIA`), aplica dependencias con `EXISTS` (opción B), proyecta columnas, tiene checkpoint por tabla en S3 (`_control/_SUCCESS.json`), exige `id_ejecucion` (de `conf_emisiones`: `idEjecucion` o generado) y no tiene timeouts fijos. Lo que se dijo antes de Emisiones (particiones 2-16 por `sample_query`, llaves materializadas, timeouts `120000`/`21600000`) era de la versión vieja. Corregidos el backlog (EXT-01, 06, 10, 13, 17, 19, 20, 21) y el roadmap.
- Proceso estándar: la consulta sigue siendo `SELECT * FROM schema.tabla WHERE …` (`extraccion_estandar_optimized.py:286`): ninguna historia proyecta columnas (*propuesta* de historia nueva, pendiente de aprobación).

## Qué falta para empezar (validado contra el código, 2026-11-20)
- **Proceso de extracción estándar:** solo la orden del usuario. La rama `feature/modular-rmd-ext-estandar` (un commit, `5676ef5`) no contiene el `main` `3d5f623`: rebasarla. Su red de caracterización (`test_flujo_arl.py`, `test_scd*.py`) cubre el flujo ARL de punta a punta.
- **Proceso de extracción de Emisiones:** no tiene red de caracterización ni línea base en DEV (la tanda 1 solo midió ARL y Rentas). Construirlas antes de tocarlo. Orden: primero el estándar.
- **Configuración en el mismo cambio:** los 6 JSON de ALFAPRD y los de Emisiones (`emisiones.json`, `pcr_vida`, `generales`, `satelites`), `conf_*.py` y `conf_emisiones.py`.
- **`--EJECUCION_ID`:** no bloquea el código, pero sí el despliegue: sin valor por defecto el job lo exige, así que el cambio de IaC de DevOps sale junto con el despliegue.

## Puntos abiertos de D
- Los tramos no deben cambiar la salida de Bronce que leen la validación y los crawlers: *propuesta*, tramos en una zona temporal y en Bronce la misma salida de hoy.
- Checkpoint: en `ejc-ejecuciones` (el rol ya puede escribir ahí). El configurador arma la llave de la ejecución `PK = EJC#<MALLA>#<periodo>`, `SK = EJECUCION#<execution_name>` (`conf_arl.py:530-542`) y la Step Function solo la actualiza. *Propuesta* para el tramo: misma `PK`, `SK = EJECUCION#<execution_name>#TRAMO#<tabla>#<n>`. `execution_name` ya es el `ejecucion_id` y viaja en la entrada de la Step Function.
- Equivalencia en ALFAPRD: las `em_*` solo sirven para Acsel-e. *Propuesta:* comparar contra la salida del job actual en el mismo periodo (conteo, suma de control y `exceptAll` = 0). Falta la confirmación del usuario.

## Por verificar (QA, Oracle)
- Scripts: PCR en `Tareas/Modularizacion/PCR/01-emisiones/validaciones/` (`scripts/` y `salidas/`, ver [[proceso-pcr]]); ALFAPRD en `Tareas/Modularizacion/Extraccion/alfaprd/validaciones/scripts/consultas-oracle-alfaprd.sql`.
- Tipo e índice de cada columna de join; acceso a `ALL_TABLES.NUM_ROWS`; permiso `FLASHBACK` y `UNDO_RETENTION` (foto consistente entre tramos); acceso de lectura a `acsele_alfa_bi.em_*` para la prueba de equivalencia. Ver [[preguntas-abiertas]].

## Alcance confirmado (2026-11-25, usuario)
- **Nuestro trabajo es optimizar lo que ya existe.** Manuel construye la versión base de las mallas; algunas están en desarrollo (Cedidas, Reaseguros, PCR y la de 99 tablas). No las construimos nosotros: se optimizan cuando existan.
- **Se empieza por PCR:** la malla de Emisiones que ya corre (`niif-snf-emisiones`: PCR Vida, Generales, Satélites). Meta de Víctor: extracción en 2-3 h. Medido en DEV (historial 16-24 nov): las corridas completas del job tardan 172-427 min (3-7 h, ~100 DPU-h, G.2X x10); hubo corridas de 32 y 70 min (*hipótesis:* reusan checkpoints o son parciales, falta el detalle por lote). Lo de Manuel (30-60 min) coincide solo con esas. La Step Function `niif-dev-emisiones` tuvo 1 éxito de 10 (393 min, 321 del job). ALFAPRD sigue en el alcance (hay espacio para optimizar), después de PCR.
- Detalle del modelo de 5 mallas en [[reunion-extraccion-victor]].
- **Causas de tiempo en PCR, verificadas en código (QA `fbfeded`, 2026-11-25, 853 líneas; corregido el 2026-11-25):**
  - 115 de 121 SQL configuradas refiltran el pivote `CONTEXTOPERATION` (PCR Vida 16/17, Generales 44/45, Satélites 55/59, contando el núcleo); las `DEPENDENCIA` (7, solo PCR Vida) lo hacen con `EXISTS` anidados (`:390-521`). Víctor (L1023): extraer el pivote una vez y usar sus llaves.
  - `DISTINCT`: viene escrito en las SQL configuradas (PCR Vida 15, Generales 21, Satélites 36). `usar_distinct` (por defecto true) solo aplica a las tablas `DEPENDENCIA` (`:459-473`). Corrige el dato anterior ("108 tablas").
  - `oracle_parallel` está en 19 tablas, pero `strip_oracle_hints` borra el hint antes de leer (`:446-457`, `:545-550`): configuración muerta.
  - **Generales y Satélites corren sus 90 tablas una por una:** sus segmentos tienen forma `CASCADA_POR_FASE`, que el código no reconoce como paralela (`:217`, `:767-768`). Corrige el dato anterior ("98 de 108 en cascada"). PCR Vida y el núcleo sí van en `PARALELO` (máx. 3, tope fijo `:764`). Los 3 casos van en secuencia (`:832-846`); sin pausa entre fases.
  - Lote nuevo en cada corrida: no reusa lo extraído.
  - Medición por tabla: script `Tareas/Modularizacion/PCR/01-emisiones/validaciones/scripts/medir-tiempos-dev.sh` (pendiente de correr). **No ejecuta el job** (pedido de Manuel): lee las corridas existentes (Glue, Step Functions, `parametros_extraccion.json` y los checkpoints `_control/_SUCCESS.json` de Bronce, que guardan tiempos y filas por tabla y lote).

## Diccionario Acsel-e (consultas 1 y 2, 2026-11-25)
Archivos: `Tareas/Modularizacion/PCR/01-emisiones/validaciones/salidas/consulta1-llaves-indices.xlsx` y `consulta2-tamanos.xlsx`. Usuario personal, base COREPROD; la conexión de Glue usa el servicio ACSSOP (usuario en secreto, no se abre). Confirmar con Julio que ACSSOP es COREPROD.
- Visibles las 93 tablas y las 144 llaves. Estadísticas del 22-23 nov 2026.
- Llaves: 132 NUMBER, 5 DATE, 7 VARCHAR2 → los rangos (opción D) aplican a casi todas. 9 llaves sin índice y 6 solo como 2.ª-4.ª columna: candidatas a recomendación de índice (Víctor → Yair).
- Tamaño: 31.000 millones de filas en total. Las más grandes son de M1 y ya se extraen: THIRDPARTYMOVEMENTPOLICY 7.547 M, OPENITEM y OPENITEMREFERENCE 2.043 M c/u, EVENTDCO 1.926 M, COBERTURA y COVERAGEDCO 1.245 M c/u. Pivote CONTEXTOPERATION: 331 M filas, particionada, TIME_STAMP con índice propio.
- Particionadas: THIRDPARTYMOVEMENTPOLICY, OPENITEM, OPENITEMREFERENCE, CONTEXTOPERATION, PRERISKUNITS, STPS_RELATIONAGREEMENTPOLICY. Falta su llave de partición (ALL_PART_KEY_COLUMNS) para aprovechar la poda.

## Foco: lado Glue, no Oracle (usuario, 2026-11-25)
Oracle (índices, DDL) es poco probable que se toque. Se optimiza lo que pasa después: pool de sesiones sobre la única cuenta de servicio, concurrencia y recursos del job. Verificado en código (`extraccion_estandar_emisiones.py`). **Las líneas citadas son de la versión de QA del 25 nov (853 líneas); las que se dieron antes eran de la del 20 nov (819) y ya se actualizaron aquí, en el README de la malla y en el plan (`Tareas\Modularizacion\PCR\01-emisiones\plan-repo-pcr-emisiones.md`)** ([[proceso-pcr]]). Correcciones: Generales y Satélites (90 tablas) son 100 % secuenciales porque la forma `CASCADA_POR_FASE` no se reconoce como paralela; las 2 tablas con lectura paralela son de PCR Vida y usan `partitionColumn` con 4 particiones (no `hashpartitions`); el pivote se refiltra en 115 de 121 SQL configuradas.

- Lectura JDBC: 1 sesión por tabla (`:577-585`). **Las 2 tablas con lectura paralela son de PCR Vida** (`STPO_POLICYPARTICIPATIONDCO`, `PRETHIRDPARTY`) y usan `partitionColumn` con 4 particiones (rango 0-70.000 M), no `hashpartitions` (corregido 2026-11-25).
- Concurrencia: tope fijo 3 (`:764`) y solo con modo `PARALELO`; Generales y Satélites (90 tablas) quedan en 1 sesión por la forma `CASCADA_POR_FASE` (`:217`, `:767-768`). Casos en secuencia (`:832-846`).
- Escritura sin `repartition`: la lectura de Oracle ocurre dentro de la escritura (Spark perezoso) y el conteo relee el parquet (`:696-718`). `dropDuplicates` si hay `deduplicar_por`.
- *Hipótesis* (falta medir): con G.2X x10 (80 vCPU, ~100 DPU-h por corrida) casi todo el clúster está ocioso. Script: `Tareas/Modularizacion/PCR/01-emisiones/validaciones/scripts/medir-uso-glue-dev.sh` (CloudWatch + event logs de corridas existentes).
- **No hay diferencia entre DEV y la IaC:** la IaC vigente de `quality` también tiene G.2X x10, timeout 2880 y `max_concurrent_runs` 3; la diferencia que vi era de una copia vieja (12 nov). No es brecha.
- Reintentos: de 34 corridas (16-24 nov) solo 8 exitosas; cada reintento crea lote nuevo y no reusa checkpoints (ver ARQ-63).
- **Validación contra Víctor (2026-11-25):** una sola conexión de servicio; DBA no da más conexiones; Analítica abrió 2 pools y tumbó la base (L182-208). Sí se maneja el pool de peticiones (L208) y la forma paralela dentro de un conjunto "por esa misma conexión", dentro del 65 % de unidades (L374, L418-432). **No validado:** si "paralela" admite varias sesiones JDBC del mismo usuario (cada partición de Spark abre una). Hasta confirmarlo, leer una tabla en k particiones y correr los 3 casos a la vez quedan como *pregunta*, no como plan. El código ya abre hasta 3 sesiones a la vez (`:764`), y 4 por tabla en 2 tablas de PCR Vida con `partitionColumn`: confirmar que eso está permitido.
- Reanudar: respaldado por Víctor (checkpoint en DynamoDB, L563-569; "¿Existe ese lote?", 25 nov L79-83), pero debe ser explícito y con aviso (ARQ-63), no reusar por defecto.

## Configuración real de Emisiones en DEV (`cnf-mallas` y PRG, 2026-11-25)
Salida: `Tareas/Modularizacion/PCR/01-emisiones/validaciones/salidas/cnf-mallas-emisiones-dev-20261125-2309/` (sin correos). Cada caso tiene 6 ítems: `CICLO#001`, `EXTRACCION`, `GENERAL`, `REMEDIACION`, `SCD` y `VALIDACION`.
- **La fuente es DynamoDB, no Git.** La tabla está en la IaC (`stacks/database/parameters.tf:126-133`, solo PK/SK); los ítems no están en ningún repo (sin seed ni cargador). Los JSON de `cfg-datalake-niif-config-s3` son una **salida ya renderizada** (fechas, status, productos y compañías reemplazados); DynamoDB guarda plantillas con `${FECHA_INICIO}`, `${FECHA_FIN}`, `${STATUS_OPERACION}`, `${PRODUCTOS}` y `${COMPANIAS}`. Mismas tablas en ambos (25, 45 y 59).
- **Contradicción por confirmar:** PCR Vida tiene 25 tablas en DynamoDB (`CICLO#001` sin cambios desde 16 nov), pero la configuradora de `quality` exige 27 (`conf_emisiones.py:58`, `validate_config`) y fallaría. Aun así la corrida del 24 nov pasó y la configuradora escribió `EXTRACCION` a las 06:00:16Z. **Confirmado (2026-11-25):** la lambda `niif-dev-configuration-general` (desplegada 2026-11-21 01:44 UTC) no es la de `quality`: su `conf_emisiones.py` tiene el `lambda_handler` en la línea 904 (en `quality`, 678 líneas y handler en la 508) y no contiene `"expected"` ni `len(core) != 7`. Según el `Jenkinsfile`, DEV se despliega desde `develop`. **Comparado en DEV (2026-11-25):** el script de extracción desplegado es **idéntico** al de `quality` (853 líneas, sha256 `2086a5a0cb8ebc03…`), así que las mediciones del job en DEV valen para el plan. La configuradora no: `conf_emisiones.py` de DEV tiene 1237 líneas (sha256 `f2cddac65b1e6fe6…`) frente a 678 en `quality` (`de08ef10b606804b…`). La configuración que recibe el job en DEV sale de esa versión. **Resuelto con la sincronización de QA del 25 nov 18:33 (`a1238a4`):** `quality` ya trae esa misma configuradora (1237 líneas, sha256 `f2cddac65b1e6fe6…`, igual a DEV). Ya no exige conteos fijos (27/45/59): valida núcleo y rama no vacíos, sin tablas duplicadas y fases de rama 4 a 9 (`validate_config`, `:473-510`). La extracción no cambió en esa sincronización.
- **ARQ-67 confirmada en DEV:** el núcleo crudo de Generales difiere en 5 tablas (`AGREGATEDPOLICY`, `PREPOLICY`, `EVENTDCO`, `RISKUNITDCO`, `PRODUCT`). El job usa el del primer caso (PCR Vida) y solo advierte.
- **La configuradora escribe en la configuración:** en cada corrida actualiza el ítem `EXTRACCION` (filtros efectivos, `idEjecucion`, `loteCargueId`). Mezcla estado de la corrida con configuración.
- **PRG:** los 3 casos activos, `periodoNegocio` 202606, `status` [2], sin `idEjecucion` ni `loteCargueId` (lote nuevo cada corrida). El `scheduleName` dice `202608` con periodo 202606 (inconsistencia menor).
- **Configuración que el job ignora:** `fetchsize` por tabla (en Generales y Satélites; el job solo lee el de `fuente`, `:552`), `alcance`, `tipo_tabla`, `orden_fase`, `persistir_dataframe` y `reglas_arquitectura`. `oracle_parallel` (19 tablas) se borra antes de leer.
- Confirmado igual que en el repo: la forma `CASCADA_POR_FASE` en todas las ramas, las 2 tablas de PCR Vida con `partitionColumn` (4 particiones), `fetchsize` 50.000/20.000 y las fases de rama 4 a 9.

## Medición en DEV con corridas existentes (2026-11-25)
Salidas: `Tareas/Modularizacion/PCR/01-emisiones/validaciones/salidas/medir-tiempos-dev-20261125-2348.txt` y `medir-uso-glue-dev-20261125-2350.txt`. Corridas de 282, 308 y 321 min (G.2X x10, 94-107 DPU-h). El script de DEV es el mismo de QA.
- **Clúster sin uso (hipótesis confirmada):** CPU de ejecutores promedio 0,00 y heap 2 % en las 3 corridas. 386 de 424 etapas de Spark tienen 1 sola tarea y suman el 95 % del tiempo de etapas.
- **Corrida del 24 nov (lote 20261124060016), en serie:** núcleo 30 min → PCR Vida termina 07:59 (148 min de tablas en ~90 min de reloj, por las fases en paralelo) → Generales +67 min (66 min de tablas) → Satélites +135 min (134 min). Total 321 min.
- **El tiempo lo pone el costo de cada consulta en Oracle, no el volumen:** `PREAGREEMENT` de Satélites, 154 filas en 19 min; `STPS_AGREEMENTSTATICDAT`, 45 filas en 9 min; `STTS_ROLE`, 8 filas en 6 min. 8 tablas con menos de 1.000 filas suman 47 min. Coincide con el refiltrado del pivote en cada SQL.
- Más lentas: `INSURANCEOBJECTDCO` 22 min y `OAVDEUDORES` 21 min (PCR Vida, 3,7 M filas), `THIRDPARTYMOVEMENTPOLICY` 18 min (36,7 M), `STPO_POLICYPARTICIPATIONDCO` 14 min (7,3 M, con 4 particiones).
- **El conteo no pesa:** 1-2 s por tabla. Se quita de las prioridades.
- 13 tablas opcionales quedan `SKIPPED_NOT_AVAILABLE` (6 de Generales y 7 de Satélites: mascotas, decenal, pet lover, exequias, protección): calidad `INCOMPLETA` y extracción `SUCCESS` (ARQ-63).
- La métrica `numberAllExecutors` no tiene datos en CloudWatch para este job.

## Decisión: pool de sesiones parametrizable (usuario, 2026-11-25)
- Hoy el pool es de **1 sesión** (una conexión de servicio, pool nuestro). Debe ser un parámetro (presupuesto de sesiones por malla, en `cnf-mallas`), por defecto 1; si a futuro se permiten 2 o 3, el job reparte sin cambiar código: tablas a la vez, casos a la vez y particiones por tabla salen del mismo presupuesto.
- **Ojo, hoy el código ya pasa de 1:** hasta 3 tablas a la vez en el núcleo y en PCR Vida (`:764`) y 4 particiones en 2 tablas de PCR Vida. Con el tope en 1, PCR Vida pasaría de unos 90 min de reloj a unos 148 (su tiempo de tablas). La ganancia con 1 sesión tiene que venir del costo por consulta (pivote una vez) y no de la concurrencia.
- **Clúster (decisión del usuario, 2026-11-25):** se queda como hoy (G.2X x10). Bajarlo **no es nuestra tarea**: queda como nota para DevOps (CPU 0,00 y heap máx. 12 % medidos). Nuestra tarea es **usarlo al máximo**: con 1 sesión a Oracle, pasar trabajo de Oracle a Spark (leer el pivote una vez y filtrar las hijas en Spark con un semijoin; `DISTINCT`/dedup en Spark) y repartir los datos leídos (`repartition`) para que la escritura y las transformaciones usen todos los ejecutores. Tamaño por corrida: *propuesta* para DevOps, que la Step Function pase `WorkerType`/`NumberOfWorkers` desde la configuración.
