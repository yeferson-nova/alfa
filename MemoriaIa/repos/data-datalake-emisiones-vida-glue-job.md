---
tipo: repo
estado: vigente
actualizado: 2026-11-12
repo: data-datalake-emisiones-vida-glue-job
categoria: glue
paso: "2 · Silver (preparación)"
malla: transversal
---
# data-datalake-emisiones-vida-glue-job
> Silver de PCR Vida (Vida Deudores): arma las emisiones de vida con módulos SQL de Spark sobre las tablas extraídas de ACSELE. Versión de prueba.

## Rol en el flujo
- Preparación Silver del caso `pcr_vida`, paso 2. No es una de las 6 mallas ALFAPRD. Va después de la extracción PCR (`data-datalake-niif-pcr-emisiones-glue-job`).
- Entrega a SCD y certificación genéricos, por confirmar (el flujo PCR sigue en diseño).

## Entradas y salidas
- Entra: `RUTA_PARAMETROS_EXTRACCION`, `S3_SLV_PREP_PATH` y opcionales `CAPA_ENTRADA` (BRONZE o SILVER) y `RUTA_CONFIG_PREPARACION` (producto y compañía; hay valores por defecto en el código).
- Exige runtime con `estado_extraccion` SUCCESS y `nombre_caso` pcr_vida; en Silver exige validación y remediación hechas.
- Sale: Parquet en una carpeta única `ejecucion_preparacion=<uuid>`, modo `errorifexists`, con informe de conteos, operaciones con y sin salida y totales de importes (prima, comisión, costo de recaudo, valor asegurado).

## Recursos AWS que toca
- S3 (runtime, Silver preparada), cliente STS. Sin DynamoDB ni Athena. Se despliega a `glue-resources/emisiones-vida`.

## Código
- `src/jobs/silver_vida_deudores.py` (~210 líneas): módulos SQL (base, movimientos, tomador, maestro, agente, asegurado, final); valida pivote sin IDs nulos o duplicados, periodo y conteos contra el runtime; trata cadena vacía como NULL (regla Oracle).
- Bloquea la ejecución si la cuenta AWS no es la de DEV (cuenta fija en el código, mala práctica; no se reproduce aquí). Su docstring dice "versión de prueba, no certificada por QA".
- Tiene `tests/` solo con `conftest.py` y paquetes vacíos; `Dockerfile` y `Makefile` de plantilla.

## Estado
- En Alfa: `origin/quality`, commit ef89236 (2026-11-06). Desplegado: por confirmar. Código nuevo nuestro: ninguno.
- `quality` frente a `main` (medido en GitHub el 2026-11-12): 8 commits solo en `quality`, 0 solo en `main`; `main` no tiene lo de `quality`. Ver ARQ-50 en [[auditoria-repos-2026-11]].

## Brechas y relacionadas
- **ARQ-45** `silver_vida_deudores.py` compara la cuenta AWS contra un número fijo, en [[auditoria-repos-2026-11]].
- Dependencia de cuenta fija: incumple el estándar de no quemar valores ([[est-desarrollo-hexagonal]]); sin ID de brecha asignado.
- Relacionado: [[reunion-diseno-pcr]], [[familia-mallas-alfaprd]], [[repo-alfa-repos]]
