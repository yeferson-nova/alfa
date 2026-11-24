---
tipo: auditoria
estado: vigente
actualizado: 2026-11-23
---
# Auditoría de la extracción frente al modelo de ciclos (2026-11-20)

> Cinco brechas nuevas de los procesos de extracción (estándar y Emisiones), contrastado con el lineamiento de Víctor y medido en DEV. Las previas siguen: RYC-01 (una conexión, tablas en secuencia) y COS-05 (`ReadTimeout` de 30 s).

Humano: `Docs\06-brechas\auditoria-extraccion-2026-11.md`. Plan: [[modularizacion-extraccion]].

- **ARQ-57 (alta):** no hay pausa entre ciclos; solo cascada; unidades por tabla, después de leer, con un tope que solo avisa (lo superan 8 tablas).
- **COS-11 (alta):** las dependencias son solo metadata; las hijas se leen completas (`acu_personas_naturales` 1,2 millones de filas para 7.002 afiliaciones; pivote de Rentas sin filtro, 22,6 millones).
- **ARQ-58 (media):** la duración de la lectura está mal medida (Spark lee en el conteo) y la duración total excluye la reconciliación final (*hipótesis*).
- **ARQ-59 (media):** el configurador exige un número fijo de ciclos y de registros (`conf_*.py`). Corregido 2026-11-20: `MALLA_PK` no cae al valor por defecto en la configuradora general, que lo fija por malla antes de importar el módulo (`lambda_function.py:264-266`); el defecto `MALLA#PRIMAS_ARL` sigue en el módulo y aplica si corre en una lambda por malla. Además el ambiente cae a `"dev"` si falta el contexto AWS (`lambda_function.py:248`).
- **ARQ-60 (media):** en DEV hay 8 jobs y al menos 4 copias del proceso de extracción de Emisiones en rutas distintas de S3, con corridas de hasta 8 horas (162 DPU-hora). Es nuestro alcance (corregido 2026-11-20: antes se anotó "de otro equipo").
- **Revisión 2026-11-23 (caso [[inprod-pcr-emisiones-202606]]), código de `quality` del 2026-11-20:**
  - **ARQ-61 (alta):** rutas quemadas por ambiente en la extracción de Emisiones: Silver derivada con `replace("alfa-datalake-qa-bronce", ...)` (`:581`, solo sirve en QA), Bronce común por defecto de PRD (`:52-53`); los JSON de `pcr_vida` de val/rem/SCD apuntan a DEV.
  - **ARQ-62 (alta):** la configuradora general solo acepta `dev|qa|prod` (`lambda_function.py:21`, `conf_emisiones.py:35`) y PRD se llama `niif-prd-*` → PRD corre otro código (ARQ-50). Por confirmar con la huella de PRD.
  - **ARQ-63 (alta):** checkpoint `_control/_SUCCESS.json` del mismo periodo y lote → `REUSED` sin leer Oracle (`:607-611`); lote fijo en PRG (`conf_emisiones.py:172`); `SUCCESS` con `calidad_fuentes=INCOMPLETA` (`:768-776`).
  - **ARQ-64 (descartada en QA 2026-11-23, filas = llaves):** Silver Generales repite montos por fila (movimientos por operación-póliza, base unida a unidades de riesgo solo por operación: `silver_generales.py:184`, `:804`).
  - **ARQ-65 (media):** Silver con productos/compañía fijos (`silver_generales.py:20-21`), `status` 2 por defecto (`:146`); exige `--RUTA_CONFIG_SCD`: la IaC `77de48cb` no lo pasa, pero la SF desplegada en DEV y QA sí (2026-11-23); confirmar en PRD.
- Descartado con evidencia (no repetir): "la exclusión relee Oracle tres veces", ver [[inprod-primas-rentas-202606]] §8.
