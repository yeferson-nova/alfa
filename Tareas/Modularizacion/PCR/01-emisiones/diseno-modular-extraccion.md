# Diseño modular: proceso de extracción de Emisiones

Parte de la [malla 1 de PCR (Emisiones)](README.md). Repo `data-datalake-niif-pcr-emisiones-glue-job`, versión de QA del 2026-11-25. Hoy todo el job está en un solo archivo, `src\jobs\extraccion_estandar_emisiones.py`, de 853 líneas.

**Objetivo:** reorganizar el código según el estándar hexagonal (`Docs\08-estandares-desarrollo\estandar-lambdas-python.md`, secciones 2 y 9) **sin cambiar el comportamiento**. El SQL que llega a Oracle, las rutas, los checkpoints, el `parametros_extraccion.json` y los eventos de log quedan iguales. La deuda técnica, la observabilidad y la optimización vienen después, cada una con su prueba y su medición.

## Estructura propuesta
```
src/
├── jobs/
│   └── extraccion_estandar_emisiones.py   punto de entrada del job (misma ruta que usa la IaC): solo llama a la composición
└── emissions_extraction/
    ├── domain/                 reglas puras, sin Spark ni AWS
    │   ├── period.py           ventana del mes
    │   ├── execution_context.py  contexto de la corrida (ejecucion_id, job_run_id, malla, caso, periodo, lote)
    │   ├── naming.py           nombre y ruta relativa de cada tabla
    │   ├── configuration.py    normalizar y validar la configuración, aplanar núcleo y ramas
    │   ├── execution.py        validar que los casos compartan periodo, lote, id y conexión; firma del núcleo
    │   ├── sql_compiler.py     armar el SQL de cada estrategia (configurada, directa, dependencia)
    │   ├── read_plan.py        decidir cómo se lee cada tabla (JDBC simple, por columna o por hash) y con qué opciones
    │   ├── phase_plan.py       orden de las fases y cuántas tablas a la vez (concurrencia)
    │   ├── paths.py            rutas de Bronce, Silver y checkpoint
    │   └── results.py          resultado por tabla y runtime por caso
    ├── transformations.py      transformaciones de Spark puras (columnas de linaje, deduplicación)
    ├── application/
    │   ├── ports.py            interfaces (Protocol): lector Oracle, escritor de datasets, almacén de objetos, credenciales, reloj y telemetría (logs, métricas y tramos)
    │   ├── events.py           catálogo de eventos (los mismos nombres de hoy)
    │   ├── extract_table.py    caso de uso: extraer una tabla (checkpoint, SQL, lectura, transformación, escritura y conteo)
    │   ├── run_phases.py       caso de uso: correr las fases con su concurrencia
    │   └── run_extraction.py   caso de uso: núcleo una vez, luego cada caso, y publicar su runtime
    ├── adapters/
    │   ├── inbound/
    │   │   └── glue_job.py     leer los argumentos del job (RUTAS_CONFIG_EXTRACCION)
    │   └── outbound/
    │       ├── s3_store.py     leer, escribir y verificar objetos en S3
    │       ├── glue_connection.py  resolver las credenciales de la conexión de Glue (o su secreto)
    │       ├── oracle_reader.py    lectura JDBC con Spark y lectura por hash con Glue
    │       ├── parquet_writer.py   escribir el parquet y contar
│       ├── clock.py            hora actual (para fijarla en las pruebas)
    │       └── stdout_events.py    imprimir los eventos JSON (como hoy)
    └── config/
        ├── settings.py         constantes y valores por defecto de hoy (casos, estrategias, región)
        └── composition.py      raíz de composición: crea Spark, Glue y los clientes, y conecta puertos con adaptadores
```

**Regla de dependencias:** `domain ← application ← adapters`; solo `config` conoce a todos. Una prueba de arquitectura (`tests/test_architecture.py`) revisa los imports y falla si una capa importa lo que no debe. Spark solo se importa en `transformations.py` y en los adaptadores.

## De dónde sale cada pieza (líneas de la versión de QA)
| Hoy (función) | Líneas | Destino |
|---|---|---|
| Arranque: `getResolvedOptions`, `SparkContext`, `GlueContext`, `Job`, clientes `boto3` | 23-54 | `config/composition.py` y `adapters/inbound/glue_job.py` (ya no al importar el módulo) |
| `SUPPORTED_CASES`, `SUPPORTED_STRATEGIES`, `DEFAULT_COMMON_BRONZE`, `REGION` | 23-54 | `config/settings.py` |
| `utc_now` | 57-58 | `adapters/outbound/clock.py`, detrás de un puerto de reloj |
| `parse_s3_uri`, `read_json`, `write_json`, `s3_exists` | 61-93 | `adapters/outbound/s3_store.py` |
| `month_window` | 96-105 | `domain/period.py` |
| `jdbc_options` | 108-186 | `adapters/outbound/glue_connection.py` |
| `table_name`, `table_relative` | 198-207 | `domain/naming.py` |
| `flatten_branch_tables`, `flatten_core_tables`, `normalize_config`, `validate_config` | 210-270 | `domain/configuration.py` |
| `load_active_configs` | 273-285 | `application/run_extraction.py` (lee con el puerto del almacén) |
| `core_signature`, `validate_execution` | 288-355 | `domain/execution.py` (el aviso del núcleo distinto se emite como evento desde la aplicación) |
| `normalize_sql`, `render_sql`, `oracle_projection`, `dependency_operator`, `compile_dependency_sql`, `compile_dependency_predicate`, `compile_sql`, `strip_oracle_hints` | 189-195, 358-546 | `domain/sql_compiler.py` |
| `read_oracle`: decidir opciones | 549-571, 577-584 | `domain/read_plan.py` |
| `read_oracle`: leer con Spark o Glue | 572-576, 585 | `adapters/outbound/oracle_reader.py` |
| `common_bronze_base`, `output_path`, `silver_destination`, `checkpoint_uri` | 587-619 | `domain/paths.py` |
| `optional_table_result`, resultado de `extract_table`, `runtime_for` | 622-634, 722-749, 786-814 | `domain/results.py` |
| `extract_table`: flujo | 637-670, 682-694, 750-751 | `application/extract_table.py` |
| `extract_table`: `dropDuplicates` y columnas de linaje | 671-681 | `transformations.py` |
| `extract_table`: escribir y contar | 696-720 | `adapters/outbound/parquet_writer.py` |
| `table_map_for`, `execute_phases` | 754-784 | `domain/phase_plan.py` (cuántas a la vez) y `application/run_phases.py` (ejecutar) |
| `main` | 817-849 | `application/run_extraction.py` y `config/composition.py` |
| `print(json.dumps(...))` de cada evento | varias | `application/events.py` y `adapters/outbound/stdout_events.py` |

## Lo que se mantiene a propósito
- **El comportamiento, incluidos los defectos conocidos:**
  - el Bronce común cae en PRD y la ruta Silver se arma con un `replace` (ARQ-61);
  - el checkpoint se reutiliza por defecto (ARQ-63);
  - si el núcleo difiere entre casos, solo se advierte (ARQ-67);
  - el tope de 3 tablas a la vez;
  - el hint de `oracle_parallel`, que se borra antes de leer.

  Cada uno se corrige después, en su propio cambio y junto con su configuración.
- **La concurrencia queda igual, pero como parámetro.** El valor por defecto es el de hoy (3), en `config/settings.py`. Se vuelve configurable por malla cuando se toque la configuración (JSON, `cnf-mallas` y configuradora).
- **Los nombres de eventos y sus campos** (`EMISIONES_INICIO`, `FASE_INICIO`, `TABLA_FIN`, etc.) no cambian. La observabilidad en 3 niveles viene después.
- **La ruta del script** (`src/jobs/extraccion_estandar_emisiones.py`) no cambia, porque es la que usa la IaC.

## Pruebas
- **Las referencias de SQL de las 129 tablas no cambian.** Si una sola consulta cambia, la prueba falla.
- **Las pruebas del flujo se pasan a la nueva API** en el mismo commit en que se mueve cada función, con los mismos valores esperados. Hoy sustituyen funciones dentro del script; después usan dobles de los puertos, que es lo que pide el estándar.
- **Nuevas pruebas:**
  - `tests/test_architecture.py`: los imports por capa;
  - pruebas unitarias del dominio;
  - `transformations.py` con un Spark local (en Docker).
- **Cobertura:** el paquete `emissions_extraction` entra a la cobertura (hoy `tox.ini` y Sonar excluyen `src/jobs/*`), con un mínimo de 70 %. Apuntamos al 80 %.
- **Python:** el código debe correr en 3.8 (la imagen de pruebas de Jenkins) y en 3.10 (Glue 4.0).

## Orden de los commits (cada uno con las pruebas en verde en Docker)
1. Esqueleto del paquete y prueba de arquitectura.
2. Dominio: periodo, nombres, configuración, ejecución y rutas.
3. Dominio: compilador de SQL y plan de lectura (las referencias de SQL no cambian).
4. Aplicación: puertos, catálogo de eventos, fases, extracción por tabla y extracción completa.
5. Adaptadores y composición: el script queda delgado.
6. Limpieza sin cambio de comportamiento: imports sin uso (`calendar`, `time`, `StorageLevel`), `requirements.txt`, README del repo y umbral de cobertura en `tox.ini` y Sonar.

## Identificador de ejecución y observabilidad (se prepara aquí, se activa después)

### Cómo nace y viaja hoy (verificado en código y en la IaC de QA)
- **Lo genera la configuradora dentro de la Step Function.** El estado `ConfigurarEmisiones` la invoca solo con `{"codigoMalla": "EMISIONES"}`, así que ella arma `idEjecucion = EMISIONES-<periodo>-<hora>` (`conf_emisiones.py:164-171`), porque la programación no trae uno. **No es el nombre de la ejecución de la Step Function.** Cuando la configuradora arranca la Step Function (`ejecutar=true`), le pone ese nombre (`:1154-1220`), pero adentro la Step Function la vuelve a invocar y nace otro ID.
- **Viaja solo por el JSON de configuración:** el job de extracción lo lee de `id_ejecucion` y lo escribe en una columna de Bronce (`:677`) y en el `parametros_extraccion.json`.
- **Ninguna tarea de Glue de la Step Function recibe `--EJECUCION_ID`** (0 referencias a `$$.Execution.Name`). La certificación recibe periodo y lote; el evaluador de notificación recibe `$$.Execution.Id` (ARQ-32).
- **En los logs del job** solo 3 eventos llevan `id_ejecucion` (`EMISIONES_INICIO`, `CASO_EXTRACCION_OK` y `EMISIONES_EXTRACCION_OK`). `FASE_INICIO` y `TABLA_FIN` no lo llevan. No hay métricas ni trazas.

### Cómo debe quedar (contrato del roadmap de observabilidad)
| Dónde | Qué |
|---|---|
| **Origen único** | `ejecucion_id = $$.Execution.Name`. Quien arranca la Step Function le pone el nombre: la configuradora con `ejecutar=true` o la programación |
| **Step Function (DevOps)** | Pasa `"ejecucionId.$": "$$.Execution.Name"` en el payload de la configuradora y `"--EJECUCION_ID.$": "$$.Execution.Name"` en cada tarea de Glue (extracción, validación, remediación, Silver, SCD y certificación) |
| **Configuradora (nuestra)** | Usa el `ejecucionId` que le llega, en vez de generar uno, y lo escribe en el JSON de cada caso |
| **Job de extracción (nuestro)** | Lee `--EJECUCION_ID` y lo compara con el `id_ejecucion` de la configuración. Si no coinciden, falla con un error clasificado. Así el ID es el mismo en el 100 % de los pasos |
| **Nivel 1, logs** | Cada línea en JSON con `ejecucion_id`, `request_id` (= `JOB_RUN_ID` de Glue), `malla`, `caso`, `paso`, `tabla`, `duracion_ms`, filas y `resultado`, en español |
| **Nivel 2, métricas** | EMF con dimensiones de pocos valores (`malla`, `caso`, `paso`): duración por tabla, fase y caso, filas, sesiones activas y pausa. El `ejecucion_id` va como propiedad, no como dimensión, para no multiplicar las métricas ni su costo |
| **Nivel 3, trazas** | Step Functions y lambdas con X-Ray (en la IaC, DevOps, con aval del arquitecto). En Glue, que no tiene X-Ray, la traza se arma con los eventos `PASO_INICIO`/`PASO_FIN`, unidos por `ejecucion_id` y `JOB_RUN_ID` |
| **Dato y evidencia** | La columna `id_ejecucion` en Bronce (ya existe), el `parametros_extraccion.json` y el registro por paso en `ejc-ejecuciones` (HU-GI-05: 7 años) |

### Qué deja listo este diseño (sin cambiar el comportamiento todavía)
- `domain/execution_context.py`: contexto inmutable de la corrida (`ejecucion_id`, `job_run_id`, malla, caso, periodo y lote). Viaja por todos los casos de uso.
- Un puerto de telemetría en `application/ports.py` con logs, métricas y tramos (`PASO_INICIO`/`PASO_FIN`). En este diseño, su adaptador imprime los mismos eventos de hoy; después se cambia por el contrato v1 y EMF **sin tocar dominio ni aplicación**.
- `adapters/inbound/glue_job.py` acepta `--EJECUCION_ID` como opcional. Pasa a obligatorio cuando DevOps lo agregue a la Step Function.

## Para desplegarlo (no bloquea escribir el código)
- **Empaquetado (pregunta a DevOps):** el paquete `emissions_extraction` tiene que llegar al job, por ejemplo como zip en `--extra-py-files`, como `libraries/commons.zip` en otros jobs. Hay que confirmar cómo lo sube `glueJobsClientesPipeline`.
- **Prueba de equivalencia en DEV:** una corrida de la versión nueva para el periodo 202606 se compara tabla por tabla con la línea base (conteo, suma de control y diferencia exacta igual a 0), y se registra su tiempo en `tiempos\registro-tiempos.csv`.
