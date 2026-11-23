# PCR Emisiones: cifras de junio de 2026 distintas entre PRD y QA

## Qué pasó

Negocio compartió dos archivos con lo emitido en junio de 2026 y pidió que la extracción coincida con ellos:

- `Resumen emision 202606.xlsx`: totales por compañía y producto (Generales, Pymes, Vida y Satélites).
- `RRC SIN CONTABILIDAD ALFA 202606.xlsx`: el detalle de Generales del que sale el resumen.

Se reporta que la data de PRD trae cifras distintas a las de QA. **No sabemos todavía si PRD y QA tienen los mismos datos de origen.** En teoría sí, pero hay que comprobarlo antes de concluir que el problema está en nuestro proceso. Por eso este trabajo no corrige nada todavía: mide cada capa en cada ambiente y muestra en cuál empieza la diferencia.

## Lo que ya está comprobado (con código, sobre los Excel)

Se comprobó con `diagnostico/validar_referencia.py`:

| Comprobación | Resultado |
|---|---|
| El resumen de Generales sale del detalle RRC tomando solo `Mes Produccion = 202606` | **PASS**, los 5 productos y las 6 columnas cuadran al centavo |
| Filas del detalle RRC | 4.478; 3.752 son de junio y 726 son de meses de producción anteriores (julio 2025 a mayo 2026) que vienen dentro del mismo corte de junio y **no** entran al resumen |
| El propio Excel frente a otro informe del negocio (fila `E1-A1`) | Generales: IVA de comisión de recaudo difiere en **63.748.417,52** (el archivo lo anota como "Diferencia en la base de RRC"); prima, -259,73. Vida: comisión, 4.053,80; costo de recaudo, -42.900 |
| Vida (RRC) frente a Satélites, mismo producto | VidaAp: comisión 57.720,10 frente a 53.666,30; IVA de comisión con diferencias de centavos a pesos en 5 productos |

Conclusión: la referencia es consistente consigo misma para Generales, pero **no es una sola cifra**: el negocio tiene dos fuentes que no cuadran entre sí. Hay que acordar con negocio contra cuál se valida.

## Dónde puede nacer la diferencia (hipótesis por medir)

Cada hipótesis sale de leer el código de la rama `quality` de cada repo (foto del 2026-11-20). Ninguna se da por cierta hasta ver la evidencia de los dos ambientes.

| # | Hipótesis | De dónde sale | Cómo se mide |
|---|---|---|---|
| H1 | La Oracle ACSELE de QA y la de PRD no tienen los mismos datos de junio (copia de otra fecha, cierres posteriores) | No se ha medido nunca | `conciliacion-oracle-acsele.sql`, igual en las dos bases |
| H2 | PRD no corre el mismo código que QA | En 12 de 33 repos `quality` y `main` difieren (ARQ-50). Además `sc-datalake-niif-configuration-general` (`src/lambda_function.py:21`, `src/conf_emisiones.py:35`, commit `e881f637`) solo acepta los ambientes `dev`, `qa` y `prod`, y los recursos de producción se llaman `niif-prd-*`: con ese código PRD no podría configurar la malla | Sección `codigo`: huella sha256 de cada script y de la lambda en cada ambiente |
| H3 | La configuración de la corrida es distinta: ventana, status, productos o compañías del PRG (`PROGRAMACION#EMISIONES`), o las SQL de `cnf-mallas` | `src/conf_emisiones.py:519-535` arma la ventana y los filtros desde el PRG de cada ambiente | Sección `configuracion` |
| H4 | Rutas quemadas por ambiente | `data-datalake-niif-pcr-emisiones-glue-job`, `src/jobs/extraccion_estandar_emisiones.py:581` deriva la ruta Silver reemplazando el texto `alfa-datalake-qa-bronce` (solo funciona en QA); `:52-53` usa por defecto el Bronce de PRD. En `cfg-datalake-niif-config-s3` (`a3453320`), los JSON de `src/pcr_vida/` de validación, remediación y SCD apuntan a rutas de DEV | Alertas `*-AMBIENTE-AJENO` y `RUN-SILVER-EN-BRONCE` sobre lo desplegado |
| H5 | Una reejecución reutiliza datos viejos | `extraccion_estandar_emisiones.py:607-611`: si existe el checkpoint del mismo periodo y lote, la tabla no se vuelve a leer de Oracle; el lote sale del PRG si está fijo (`conf_emisiones.py:172-173`). Además `:768-776` termina en `SUCCESS` aunque la calidad de fuentes sea `INCOMPLETA` | Alertas `CFG-LOTE-FIJO`, `RUN-DATASET-REUSED`, `RUN-FUENTES-INCOMPLETAS`; fechas de los checkpoints |
| H6 | El Silver de Generales repite montos por fila | `data-datalake-emisiones-vida-glue-job`, `src/jobs/silver_generales.py:804` suma los movimientos por (operación, póliza), pero la base se une con las unidades de riesgo solo por operación (`:184`): si una operación tiene varias unidades de riesgo, la prima queda en varias filas | Consulta `silver_<caso>_totales_por_operacion` frente a `silver_<caso>_totales` |
| H7 | "Lo emitido en junio" no se define igual | El Excel filtra por mes de producción; el proceso filtra por la fecha de la operación (`TIME_STAMP`) dentro de junio. Las 726 filas de meses anteriores dentro del corte apuntan a esta diferencia | El comparador cruza el Silver con las dos variantes del Excel (solo junio y todo el corte) |
| H8 | Las columnas no son equivalentes | El Silver de Generales no produce "IVA comisión intermediario" (`silver_generales.py:748-785`). El mapeo de las demás columnas es una **propuesta** (`diagnostico/mapeo-columnas-propuesta.json`) | Totales por concepto (Oracle Q5 y Bronce) para confirmar qué conceptos forman cada columna |
| H9 | El Silver no usa los filtros del PRG | `silver_generales.py:20-21` tiene la lista de productos y la compañía fijas en el código; `:146` toma `status = 2` si el PRG no trae status | Sección `configuracion` (filtros efectivos) frente al código |
| H10 | El Silver de Generales no recibe un argumento obligatorio | `silver_generales.py:1175-1181` exige `--RUTA_CONFIG_SCD`; en `iac-analytics-datalake-niif-operations-manager` (`77de48cb`) ni la Step Function `niif-snf-emisiones.json` (`PreparacionSilver_GENERALES`) ni el job (`stacks/analytics/glue_jobs/parameters.tf:909-935`) lo pasan | Alerta `ORQ-FALTA-ARGUMENTO` y corridas del job |

## Cómo se ejecuta

Todo es de solo lectura. La única escritura es la de Athena, que guarda el resultado de cada consulta en el bucket de resultados de su workgroup.

1. **Referencia (local).** Convierte los Excel en totales por producto:
   ```
   python diagnostico/validar_referencia.py --rrc "RRC SIN CONTABILIDAD ALFA 202606.xlsx" --resumen "Resumen emision 202606.xlsx" --salida referencia_esperada.json
   ```
2. **Diagnóstico en cada ambiente (CloudShell, us-east-1).** Desarrollo lo corre en DEV; DevOps lo corre en QA y en PRD. Se suben los dos archivos `diagnostico-pcr-emisiones.sh` y `diagnostico_pcr_emisiones.py` y se ejecuta:
   ```
   bash diagnostico-pcr-emisiones.sh qa 202606 primary
   bash diagnostico-pcr-emisiones.sh prd 202606 primary
   ```
   Cada corrida deja `evidencia-pcr-emisiones-<ambiente>-<fecha>.tar.gz`. Si el ambiente de producción no se llama `prd` en los recursos, usar su nombre real.
3. **Oracle (DBA, en la base ACSELE de QA y en la de PRD).**
   - `diagnostico/conciliacion-oracle-acsele.sql`: las mismas consultas en las dos bases. Exportar cada bloque a CSV (`Q1.csv` a `Q6.csv`).
   - `oracle/<caso>_conteos_por_tabla.sql` y `oracle/<caso>_movimientos_por_producto_concepto.sql` dentro de cada evidencia: son las SQL que usó la extracción **en ese ambiente**. Comparan Oracle con lo que llegó a Bronce.
4. **Comparar (local).** Descomprimir las dos evidencias, poner los CSV de Oracle en `<evidencia>/oracle_resultados/` (mismo nombre en los dos ambientes) y correr:
   ```
   python diagnostico/comparar_ambientes.py --a <evidencia-qa> --b <evidencia-prd> --referencia referencia_esperada.json --mapeo diagnostico/mapeo-columnas-propuesta.json --salida informe-comparacion.md
   ```
   El informe abre con un veredicto por capa: fuente Oracle, código, orquestación, configuración, extracción, Bronce y Silver. **La primera capa que difiere es donde nace la diferencia.** Si la primera es la fuente Oracle, la diferencia viene de los datos y no de los jobs.

## Permisos que necesita el diagnóstico (lectura)

- Glue: `GetJob`, `GetJobRuns`, `GetConnection` (se pide sin clave), `GetDatabases`, `GetTables`.
- S3: `GetObject`, `ListBucket` y `ListBucketVersions` sobre `niif-<amb>-assets`, `alfa-datalake-<amb>-bronce` y `alfa-datalake-<amb>-silver`; escritura solo en el bucket de resultados del workgroup de Athena.
- Lambda: `GetFunction` sobre `niif-<amb>-configuration-general`.
- Step Functions: `ListStateMachines`, `DescribeStateMachine`, `ListExecutions`, `GetExecutionHistory`.
- DynamoDB: `GetItem` y `Query` sobre `niif-<amb>-prg-programaciones` y `niif-<amb>-cnf-mallas`.
- Athena: `StartQueryExecution`, `GetQueryExecution`, `GetQueryResults` en el workgroup indicado.
- KMS: `Decrypt` si los buckets o las tablas están cifrados con llave propia.

Si falta un permiso, esa sección queda anotada en `resumen.json` como error y las demás siguen.

## Datos personales

- El diagnóstico no consulta columnas de personas: solo cuenta filas y suma montos por producto, concepto y periodo.
- La evidencia trae nombres de recursos, rutas y la URL JDBC de la conexión: se guarda en la carpeta de evidencias, fuera del repositorio.
- El archivo `RRC SIN CONTABILIDAD ALFA 202606.xlsx` (hoja `Data`) trae nombres de tomadores, y los `.msg` traen nombres y correos. No deben quedar en el repositorio.

## Archivos

| Archivo | Para qué |
|---|---|
| `diagnostico/validar_referencia.py` | Valida los Excel y genera los totales esperados |
| `diagnostico/diagnostico-pcr-emisiones.sh` y `diagnostico_pcr_emisiones.py` | Diagnóstico de solo lectura por ambiente |
| `diagnostico/conciliacion-oracle-acsele.sql` | Comparación de la fuente Oracle entre QA y PRD |
| `diagnostico/comparar_ambientes.py` | Veredicto por capa e informe |
| `diagnostico/mapeo-columnas-propuesta.json` | Propuesta de equivalencia entre columnas del Excel y del Silver |
