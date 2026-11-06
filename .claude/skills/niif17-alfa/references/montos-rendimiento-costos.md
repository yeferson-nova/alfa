# Montos, rendimiento y costos en las ETL

Lineamiento pedido por el usuario (2026-11-05): en las ETL se cuidan con el mismo rigor **los cálculos de montos y valores**, el **rendimiento**, el **costo de cómputo** (Glue) y el **costo de las consultas** (Athena). Base: ETL GTC-GDD-010 (`[[est-etl]]`), HU-SG-11 (`[[hu-seguridad-acceso]]`), `Docs\06-brechas\rendimiento-y-costos.md` (RYC-01 a RYC-17).

## 1. Cálculo de montos y valores (primas, reservas, siniestros)
- **Tipo:** `DecimalType(p, s)` en Spark y `DECIMAL(p, s)` en Athena para todo importe y tasa. Precisión y escala declaradas en la metadata del dataset (`prs-estructuras` / contrato), no inferidas. Prohibido `double`/`float` para dinero, reservas o factores actuariales.
- **Lectura desde Oracle:** fijar el tipo de `NUMBER` al leer (`customSchema` o casteo inmediato). `NUMBER` sin precisión llega como `decimal(38,10)` o `double` según el conector: verificar y fijarlo.
- **Operaciones:** Spark puede perder escala en multiplicaciones y divisiones de decimales grandes (`spark.sql.decimalOperations.allowPrecisionLoss`). Conocer el comportamiento, castear el resultado a la escala del contrato y probarlo con casos límite.
- **Redondeo:** una sola regla (modo y escala) por concepto, documentada, aplicada en **un solo lugar** y al final del cálculo, no en pasos intermedios. Si la regla actuarial dice otra cosa, manda la regla actuarial.
- **Sumas:** agregar en decimal; nunca sumar valores ya redondeados para "cuadrar" un total. El total del correo, del CSV y del archivo a FIS es el **mismo número**, calculado una vez y transportado como texto en el manifiesto.
- **Nulos:** un importe nulo no es cero. Tratar nulos según metadata (cuarentena o valor declarado) y contarlos como métrica; un `sum` que ignora nulos en silencio esconde faltantes.
- **Signo y unidades:** declarar moneda, signo (débito/crédito, devoluciones, endosos negativos) y unidad (pesos, miles). Validar rangos (p. ej. valores asegurados no negativos, precedente de Nuevos Cargues).
- **Cuadre de valores:** además de `entra = sale + cuarentena` en filas, cuadrar **sumas de importes** entre capas y contra cifras de control de la fuente o del periodo anterior, con umbral en metadata.
- **Pruebas obligatorias** de toda transformación con montos: valores grandes, escala máxima, negativos, nulos, cero, redondeo en el límite (x,xx5), y equivalencia contra el SQL o resultado actuarial original.

## 2. Rendimiento (Spark en Glue)
- **Medir antes de optimizar:** duración por etapa, DPU-horas y Spark UI de una ejecución real (`PipelineObserver` ya publica `dpuHours`). Optimizar solo el cuello de botella medido.
- **Leer menos:** columnas explícitas, filtro por periodo empujado a la fuente (`predicate pushdown`), lectura JDBC paralela (`partitionColumn`, `numPartitions` o `hashfield`) cuando la extracción domine.
- **Acciones:** cada `count`, `collect` o `show` relee el plan. Contar una vez tras materializar; `cache`/`persist` solo lo que se reutiliza y liberarlo; nada de conteos solo para el log.
- **Uniones:** `broadcast` para tablas pequeñas de catálogo; evitar productos cruzados; revisar sesgo de llaves (AQE activo en Glue 4.0).
- **Una pasada:** reglas de validación y agregados con expresiones condicionales en una sola agregación, no un DataFrame por regla.
- **Escritura:** archivos de 128 MB a 1 GB aprox.; evitar miles de archivos pequeños y también `coalesce(1)` en datos grandes. Particionar por columnas de filtro reales (periodo, malla), no por columnas de alta cardinalidad.
- **Tablas anchas** (Reserva Matemática, 440+ columnas): seleccionar solo lo necesario en cada paso; validar memoria con Spark UI antes de cambiar el tipo de worker.

## 3. Costo de cómputo (Glue, Step Functions, Lambda)
- **Dimensionar por medición:** tipo y número de workers según el job real; considerar *auto scaling* de Glue o Flex para jobs no urgentes. Un job que no usa Spark va a Python shell o Lambda.
- **Timeouts reales** (2-3× el p95) en job y en Step Function (`TimeoutSeconds` por tarea y global). Hoy un job colgado cuesta ~84 USD (RYC-10).
- **Crawlers:** no recorrer toda la capa por ejecución; registrar particiones (`ALTER TABLE ADD PARTITION` o API del catálogo) o un crawler por malla con exclusiones.
- **Logs:** Spark UI y *job insights* solo para diagnóstico; retención definida; no escribir consultas completas ni datos en el log.
- **Almacenamiento:** ciclo de vida por prefijo (lotes, históricos, `tmp_`, resultados de Athena), respetando la retención de evidencia de 7 años donde aplique.
- **Lambdas:** paquetes pequeños, memoria ajustada a la medición, sin dependencias que no se importan.

## 4. Costo de consultas (Athena)
Athena cobra por bytes escaneados (precio de lista de referencia ~5 USD/TB, verificar). Reglas:
- **Workgroup propio** por área y ambiente con configuración obligatoria, `BytesScannedCutoffPerQuery`, resultados cifrados en ubicación propia con ciclo de vida corto, y alerta de presupuesto por periodo (HU-SG-11). Nunca el grupo `primary`.
- **Formato columnar** (Parquet/ORC/Delta) comprimido; nunca CSV para tablas consultadas.
- **Particiones** alineadas con los filtros (periodo, malla) y consultas que **siempre** filtran por la partición; considerar *partition projection* para evitar crawlers.
- **Columnas explícitas**, nunca `SELECT *`, sobre todo en tablas de 400+ columnas.
- **Una consulta por necesidad:** no repetir la misma consulta para "confirmar" un vacío (usar el manifiesto del `UNLOAD` o el conteo ya obtenido); reutilizar resultados con *query result reuse* cuando aplique.
- **Agregar en Athena, no en la lambda:** traer totales, no el dataset completo a memoria.
- **Consultas versionadas en metadata** y revisadas en PR: un cambio de consulta puede multiplicar el escaneo sin aviso.
- **Medir:** `DataScannedInBytes` por consulta y por caso; registrarlo como métrica con el `useCaseId` (no con `ejecucion_id`).

## 5. Qué entregar al proponer un cambio de ETL
Impacto en montos (tipos, redondeo, cuadre), en tiempo (medido o estimado y cómo se medirá), en costo de cómputo (DPU-horas) y en costo de consultas (bytes escaneados). Si no hay medición, decirlo y proponer cómo obtenerla. Hallazgos de costo con ID `COS-xx`.
