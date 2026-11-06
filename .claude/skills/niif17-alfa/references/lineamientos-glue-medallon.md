# Lineamientos para jobs de Glue y el medallón

Base: estándar ETL GTC-GDD-010 (`[[est-etl]]`), HU de gobierno de ingestas, trazabilidad, linaje y remediación (`[[hu-gobierno-ingestas]]`, `[[hu-trazabilidad]]`, `[[hu-linaje]]`, `[[hu-remediacion]]`), estándar propio sección 9 (`[[est-desarrollo-hexagonal]]`).

## 1. Estructura del código de un job
- Mismo esqueleto hexagonal que las lambdas: `src/<paquete>/{dominio, aplicacion, adaptadores/{entrada,salida}, configuracion}`, `config/job.<amb>.json`, `tests/`, `pyproject.toml`, `sonar-project.properties`, `README.md`.
- **Spark es infraestructura.** Tres piezas separadas:
  1. **Transformaciones puras:** reciben y devuelven DataFrames (o estructuras de dominio), sin leer ni escribir. Se prueban con Spark local y datos ficticios.
  2. **Lectura y escritura:** adaptadores de catálogo, S3, JDBC, Athena, DynamoDB.
  3. **Script principal (entrada):** lee argumentos, valida configuración, arma las piezas, llama al caso de uso. Sin lógica.
- Si el job ya no usa Spark para trabajo real (certificación, notificación), evaluar Python shell o lambda: más barato y más simple.
- **Librería común** para lo que hoy se copia en 11 jobs (`cargar_json_s3`, `guardar_json_s3`, `parse_s3_uri`, lectura de lotes): un solo paquete versionado, no copias.
- Funciones de menos de 40 líneas, complejidad cognitiva < 15. Los scripts de 1 000+ líneas son el antiejemplo.

## 2. Configuración y argumentos
- Todo por argumentos del job y metadata (`config_*.json`, DynamoDB). Validar al arrancar; si falta un argumento, **fallar**, nunca usar un default de otra malla, de otro periodo o de otro ambiente (antiejemplos FLU-06 y FLU-07).
- Timeouts según duración real (2-3× el p95), no 48 h. Reintentos coherentes con la Step Function. Concurrencia máxima coherente con cuántas mallas comparten el job.
- Logs continuos sí; Spark UI y *job insights* solo para diagnóstico; retención de logs definida.

## 3. Reglas por capa
**Extracción (fuente → Bronce)**
- Columnas explícitas, nunca `SELECT *`; filtro por periodo; lectura paralela por columna de partición cuando el volumen lo justifique (medir primero).
- Conexión por Glue Connection o Secrets Manager; nunca credenciales en código.
- Particionar por `lote_cargue_id` / `ejecucion_id`; registrar filas leídas y escritas por tabla.
- **Incremental con cuidado:** si se excluyen llaves ya extraídas, el contrato debe entregar a las capas siguientes la referencia a los lotes de respaldo. Una extracción incremental que el SCD interpreta como foto completa **borra datos** (incidente `[[inprod-primas-arl-202606]]`).

**Validación (compuerta de calidad, HU-TR-02)**
- Reglas declaradas por objeto y transición: tipo (estructura, calidad, negocio, conciliación), versión, umbral, severidad.
- Severidad alta **detiene** el flujo; media publica parcial y manda a cuarentena; todo resultado se registra (`aud_resultado_reglas_calidad`). Hoy la validación nunca detiene (PP-01, FLU-12): no repetir ese patrón en código nuevo.
- Una sola pasada con agregados condicionales, no un DataFrame por regla.
- Nunca borrar el historial de errores cuando una corrida sale limpia.

**Remediación y cuarentena (HU-TR-03, HU-RM)**
- Ningún registro se descarta sin traza. Cuarentena `cuarentena_<capa>_<objeto>` con volumen, causa, responsable y estado.
- No completar datos con valores ficticios (veracidad, Ley 1581). Valores por defecto de nulidad solo si están declarados en metadata.

**Silver preparada**
- Lógica de negocio y actuarial en funciones puras con pruebas de **equivalencia** contra el SQL original y datos de control antes de cambiarla (PP-07).
- Sin ajustes "por persona y fecha" en código: si una regla cambia por periodo, va en metadata versionada.

**Gold (SCD)**
- Definir explícitamente el modo de carga: **foto completa** o **incremental**. Solo la foto completa puede cerrar registros por ausencia; en incremental, la ausencia no significa baja.
- Validar antes de escribir: si la entrada es una fracción anómala del Gold activo (umbral en metadata), detener y alertar.
- Escritura atómica y reversible: versión anterior recuperable (versionado S3 o formato con *time travel*), nunca sobrescritura sin respaldo.
- Cuadre obligatorio: `activos_antes + insertados − cerrados − apagados = activos_después`, registrado como métrica.

**Certificación (Athena sobre Gold)**
- Workgroup propio con tope de bytes por consulta, resultados cifrados en ubicación propia con ciclo de vida corto.
- Tiempo máximo de espera y número de intentos; nunca `while True`.
- Contar filas reales (sin encabezado) para decidir "sin datos". Un CSV con solo encabezado es **vacío**.
- Totales en DECIMAL, calculados una sola vez, viajando como texto en el manifiesto; nunca recalcular con `float`.

## 4. Catálogo y crawlers
- Preferir registrar la tabla una vez y agregar la partición nueva; si se usa crawler, uno por malla o con exclusiones, no toda la capa.
- Evitar `DEPRECATE_IN_DATABASE` que borra tablas al limpiar carpetas.
- Esperar al crawler no garantiza ver los datos propios si ya corría: verificar la partición.

## 5. Contrato entre etapas
- Objetivo: **manifiesto inmutable por ejecución** (`.../ejecucion=<id>/manifiesto.json`) con conteos, sumas, checksum, esquema, versión, periodo y estado de cada regla. Cada etapa lee el manifiesto de la anterior y escribe el suyo; nadie reescribe uno ajeno.
- Mientras exista `parametros_extraccion.json`, no agregar más campos sin documentarlos en `jobs-glue.md`.

## 6. Observabilidad del job
- Eventos del catálogo cerrado (`CORRIDA_INICIADA`, `LECTURA_*`, `ESCRITURA_*`, `REGLA_EVALUADA`...), una línea JSON con `ejecucion_id`, `malla`, `periodo_negocio`, `etapa`.
- Métricas mínimas: filas entrada, salida, cuarentena; duración; resultado. **Alarma si entrada − salida − cuarentena ≠ 0.** Nunca `ejecucion_id` como dimensión de métrica (costo).
- Emitir evento de linaje (OpenLineage) con lo leído y escrito cuando el estándar lo active.

## 7. Pruebas mínimas
Transformaciones puras con Spark local; adaptadores con `Stubber`; caso de uso con dobles en memoria; prueba de arquitectura; escenarios obligatorios: entrada vacía, foto parcial, reproceso del mismo periodo, columna eliminada, regla bloqueante, Athena sin filas, ejecución concurrente.
