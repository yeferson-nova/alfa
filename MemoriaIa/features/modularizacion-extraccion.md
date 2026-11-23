---
tipo: feature
estado: vigente
actualizado: 2026-11-20
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
- Scripts: `Tareas/Modularizacion/Extraccion/consultas-oracle-alfaprd.sql` y `consultas-oracle-acsele.sql`.
- Tipo e índice de cada columna de join; acceso a `ALL_TABLES.NUM_ROWS`; permiso `FLASHBACK` y `UNDO_RETENTION` (foto consistente entre tramos); acceso de lectura a `acsele_alfa_bi.em_*` para la prueba de equivalencia. Ver [[preguntas-abiertas]].
