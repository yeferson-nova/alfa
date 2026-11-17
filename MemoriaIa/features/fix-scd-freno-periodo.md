---
tipo: feature
estado: vigente
actualizado: 2026-11-13
---
# Fix Primas ARL: SCD por periodo, extracción y calidad del código

> Corrección completa del flujo de Primas ARL tras el incidente [[inprod-primas-arl-202606]]: problema raíz del SCD (Slowly Changing Dimension, dimensión de cambio lento), 33 hallazgos validados con simulación y plan de 6 días hábiles para entregar a QA. Sin código escrito todavía.

**Informes (humano):** `Tareas\Fixs\primasARL\informe-tecnico.md` y `informe-negocio.md`, con sus `.pdf`. Evidencia: `Tareas\Fixs\primasARL\pruebas\` (arneses, no se citan en los informes). Prompt reutilizable: `Tareas\Fixs\prompt-auditoria-fix.md`.

## Repos y rama
- Principal: [[data-datalake-niif-rmd-ext-estandar-glue-job]], rama `bugfix/scd-freno-periodo` desde `develop` `56eaf0c`. Desde ese commit el repo trae `scd.py` y `validacion_tecnica.py` (idénticos a los de [[data-datalake-niif-auxilios-funerarios-glue-job]]). El fix de extracción del incidente ya está ahí (idéntico al de `Tareas\paquete-release-inprod\`, solo cambian finales de línea).
- También se entregan: [[data-datalake-niif-primas-arl-glue-job]] (ARL-02), certificación y notificación (ARL-04) y `cfg-datalake-niif-config-s3` (ARL-01, ARL-04), cada uno con una rama del mismo nombre.
- Decisión de estructura (reunión con DevOps, 2026-11-13): los jobs genéricos (extracción, validación técnica, remediación y SCD) van en un solo repo modular `data-datalake-niif-estandar-glue-job` con `src/comun/` y `src/jobs/<job>/`. Hoy el repo se llama `rmd-ext-estandar`.

## Problema raíz
El SCD (dimensión de cambio lento) asume que la entrada es la foto completa y apaga lo ausente. Con entrada parcial (incremental o de un solo periodo) apaga datos válidos. El freno (20 %) lo detiene: en QA, Auxilios Funerarios 90/90 y ARL 2.099/2.099 (filtro `PERIODO = 'P'` en QA y `<=` en PRD). Corrección: SCD-02 (`columna_periodo` opcional por malla), ARL-01 (alinear filtro por ambiente), ARL-03 (qué columna define el periodo). Depende de 4 preguntas a BA ([[preguntas-abiertas]]).

## Hallazgos (IDs de los informes)
- SCD-01..10 en `scd.py`: 2 críticos (01 sobrescribe Gold si falla la lectura; 02 problema raíz), 3 altos, 3 medios, 2 bajos.
- EXT-01..10 en la extracción: 2 altos (01 la reanudación mezcla periodos; 02 los cambios del origen no llegan a Gold), 5 medios, 3 bajos. EXT-03 corregido tras la simulación: Bronce se sobrescribe, Silver base acumula lotes (la remediación usa modo de recuperación).
- COD-01..07 de estructura: scripts sin `main`, 0 pruebas, `sonar.coverage.exclusions=src/**.py`, `fail_under = 0`, funciones copiadas en 5 jobs, valores por defecto silenciosos.
- ARL-01..06 en otros componentes (sin criticidad, con responsable).
- Total 33: 2 críticos, 7 altos, 12 medios, 6 bajos y 6 de otros componentes.

## Simulación (jobs reales, sin modificar)
Cadena extracción, remediación, Silver ARL y SCD con Oracle, S3 y Glue simulados (PySpark 3.5.5 y Python 3.11 con uv; Glue 4.0 usa Spark 3.3). Confirmó: el fix funciona (2 lotes, apaga 0), sin el fix reproduce el incidente (18/18), problema raíz con `=` (10/10) y con `<=` al recorrer un periodo anterior (10/20), EXT-01, EXT-02 y EXT-04. No simulado: concurrencia entre mallas, validación técnica real, impacto de ARL-02 con datos de PRD.

## Plan (decidido por el usuario)
Todo el desarrollo de código en 6 días hábiles, del 17 al 24 de noviembre de 2026 (el 16 es festivo), en tres líneas: A `scd.py`; B extracción y remediación; C módulo común, pruebas, Silver ARL y certificación. Día 5 prueba completa local y DEV; día 6 paquetes por repo. Después los paquetes pasan a DevOps para el despliegue en QA (no se estiman tiempos de DevOps). Supuesto no comprobado: el Silver de cada malla lee los datos guardados por lote sin cambiar su código.

## Pendiente
- Enviar a BA las 4 preguntas antes del jueves 19 de noviembre.
- Escribir el código (no se ha tocado ningún repo) y convertir los escenarios simulados en pruebas `pytest`.
- Pedir a DevOps: versionado de Gold, alarmas y tiempos de la Step Function, crawlers del guardado por lote.
- Decidir si se borra el artifact publicado por error (`claude.ai/artifact/M9Kr4mbiU15x5bc1VpyGG2`); la regla ahora es solo `.md` local.
