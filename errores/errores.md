# Incidente: certificación Primas ARL periodo 202606 sin datos en prod

**Fecha del correo:** 2026-11-01 (CSV `202606-detail_1-20261101-222339.csv`)
**Caso:** `useCaseId = primasArl`, periodo `202606`
**Origen:** correo SNS "NIIF 17 - Certificación Primas ARL Periodo 202606" enviado desde prod (cuenta `992382496548`).

## 1. Qué se vio

| Sección del correo | Texto recibido | Significado en el código |
|---|---|---|
| Resumen | "No hay resumen configurado." | `queries.summary` del JSON del caso está vacío o no existe. Es un problema de configuración. |
| Totales | "No hay datos" | Existe una consulta `total`, pero Athena devolvió solo el encabezado, sin filas. |
| Archivos generados | `202606-detail_1-20261101-222339.csv` | El detalle sí se exportó. |
| Aviso "No se encontraron datos procesados..." | No aparece | El detalle no quedó marcado `sinDatosPeriodo`, así que probablemente sí tiene filas. |

**Contraste:** en QA, con datos replicados de prod, el correo sale completo. La diferencia está en lo que cambia entre entornos (configuración, catálogo, permisos o versión), no en los datos.

**Restricción:** no hay acceso a prod. Los pasos marcan quién puede ejecutarlos.

> **No certificar con este correo.** No hacer clic en "Certificar" ni en "No certificar" hasta aclarar el incidente: esos enlaces registran la decisión legal.

Código de referencia (DEV):
- `Backend\Glue\niif-dev-notification-certification\notification_certification.py` (líneas 321-403 consultas Athena y tablas, 578 armado del mensaje).
- `Backend\Glue\niif-dev-certification\niif_certification.py` (líneas 573-620 exportación del CSV y `sinDatosPeriodo`).

---

## 2. Paso a paso de validación

### Paso 1. Revisar el CSV de detalle (sin acceso a prod)
**Quién:** quien recibió el correo.
1. Abrir el enlace de "Archivos generados" del correo y descargar el CSV.
2. Contar filas y comprobar que son del periodo 202606.
3. **Resultado esperado:** el CSV tiene filas.
   - Si tiene filas: los datos llegaron; el fallo está en `summary`/`total` (pasos 2 a 4).
   - Si está vacío: el problema está antes, en la extracción o en Gold (pasos 5 y 6).

### Paso 2. Comparar el JSON del caso entre QA y prod
**Quién:** quien tenga acceso a S3 de prod (o pegue el archivo).
1. Ubicar `CONFIG_BUCKET` y `CONFIG_KEY` en los argumentos de la ejecución del job `niif-prd-notification-certification` en Glue.
2. Descargar el JSON del caso `primasArl` de prod y el de QA.
3. Comparar (diff):
   - [ ] `queries.summary`: existe y tiene el mismo número de consultas que en QA.
   - [ ] `queries.total`: existe y es idéntica a la de QA.
   - [ ] `database` de cada consulta: apunta a la base de **prod** (`niif-prd-gold-catalog-db`), no a QA ni DEV.
   - [ ] Filtros de periodo y estado en `total` (formato `202606`, no `2026-06`).
   - [ ] `email.dataPresence`, si está definido.
4. **Resultado esperado:** ambos JSON equivalentes salvo nombres de entorno.
   - Si falta `queries.summary` en prod: esa es la causa de "No hay resumen configurado".

### Paso 3. Revisar el JSON global de prod
**Quién:** acceso a S3 de prod.
Archivo: `configs/certification/json_global_notif_cert.json`.
- [ ] `environment` = `prd`.
- [ ] `buckets.assets` es el bucket de prod.
- [ ] `athena.output_path` válido en prod.
- [ ] `catalog` correcto (por defecto `AwsDataCatalog`).
- [ ] `dynamodb.notification_table` es la tabla de prod.

### Paso 4. Ejecutar la consulta `total` a mano en Athena (prod)
**Quién:** acceso de lectura a Athena en prod.
1. Tomar el texto de la consulta `total` del JSON del caso (paso 2).
2. Sustituir las variables (`period` = `202606`, `env` = `prd`, etc.) y ejecutarla.
3. **Resultado esperado:** devuelve filas.
   - Si devuelve filas: el problema es de ejecución del job (permisos, catálogo, momento de ejecución; ver pasos 6 y 7).
   - Si no devuelve filas: la consulta o los datos de Gold no coinciden con el detalle. Revisar filtros y tabla.

### Paso 5. Validar los datos en Gold
**Quién:** acceso de lectura a Athena en prod.
- [ ] Existe la tabla `scd_primas_arl` en `niif-prd-gold-catalog-db`.
- [ ] Tiene registros del periodo 202606.
- [ ] Cantidad en Gold = cantidad en Silver (`silver_preparado_primas_arl`) para el mismo `lote_cargue_id`.
- [ ] En `parametros_extraccion.json` de `primas-arl`: `estado_gold_scd` distinto de `SKIPPED` y `gold_vacio_inicializado = false`.

### Paso 6. Validar el crawler de Gold y el momento de ejecución
**Quién:** acceso a Glue en prod.
1. Revisar en `niif-prd-crawler-gold` la hora de su última ejecución.
2. Comparar con la hora de la notificación (**2026-11-01 22:23**).
3. **Resultado esperado:** el crawler terminó **antes** de que la notificación consultara Gold.
   - Si terminó después: Athena no veía aún las particiones. Reenviar la notificación.

### Paso 7. Revisar los logs del job de notificación
**Quién:** acceso a CloudWatch en prod.
Log group del job `niif-prd-notification-certification`. Buscar la ejecución del 2026-11-01 y estas líneas:
- [ ] `[TABLES] tipo=summary, queries=N`: si `N = 0`, confirma el paso 2.
- [ ] `[TABLES] tipo=total, queries=N`.
- [ ] `[ATHENA] Query final`: copiarla y ejecutarla a mano (paso 4).
- [ ] `[ATHENA] Query SUCCEEDED` o `FAILED`, con su `Reason`.
- [ ] Errores de `AccessDenied` o Lake Formation.

### Paso 8. Revisar las filas de DynamoDB
**Quién:** acceso a DynamoDB en prod.
Tabla `niif-prd-notification-certification`, filtrando por el `certCode` del correo (empieza por `202606-`).
- [ ] Una fila `detail#...` con `s3_url` y `nameFile`.
- [ ] El campo `sinDatosPeriodo` de cada fila (`true` indica CSV vacío).
- [ ] El `status` (`PENDING` / `SENT`).

### Paso 9. Revisar permisos y versión desplegada
**Quién:** acceso a IAM y Glue en prod.
- [ ] El rol del job tiene permisos de Athena, Glue Catalog y S3 de prod.
- [ ] El rol tiene permisos de Lake Formation sobre la base `gold` (sin ellos, Athena puede devolver cero filas sin error claro).
- [ ] El script del job en prod es el mismo que en QA (comparar versión o hash del archivo en S3).

---

## 3. Matriz de diagnóstico

| Evidencia | Causa probable | Acción |
|---|---|---|
| `queries.summary` ausente en JSON de prod | Configuración incompleta | Copiar la sección desde QA, validar y reenviar |
| `database` apunta a otro entorno | JSON mal parametrizado | Corregir y reenviar |
| `total` a mano devuelve filas, el job no | Permisos, catálogo o momento | Pasos 6 y 9; reenviar |
| `total` a mano sin filas, el detalle con filas | Filtro distinto entre `total` y `detail` | Alinear filtros de `total` |
| CSV vacío y Gold sin 202606 | Datos no llegaron a Gold | Revisar extracción, Silver y SCD |
| CSV vacío y Gold **con** 202606 pero todo `act_reg = 0` (caso real de este incidente) | El SCD apagó los registros activos (entrada parcial) | Ver sección 6 y `analisis-flujo-primas-arl-202606.md` |
| Crawler terminó después del correo | Catálogo desactualizado | Reenviar la notificación |

## 4. Cierre

- [ ] Causa identificada y registrada.
- [ ] Corrección aplicada en prod (configuración, permisos o reejecución).
- [ ] Correo reenviado con Resumen y Totales con datos.
- [ ] Totales del correo cuadran con el CSV de detalle (conteo y suma).
- [ ] Solo entonces se certifica el periodo 202606.
- [ ] Evidencia (capturas, logs, diff de JSON) adjunta al ticket.

## 5. Mejoras para evitar que se repita (propuesta)

- Hoy un `summary` vacío se muestra igual que "sin resumen configurado" y el correo sale de todas formas. Propuesta: que el job **falle o alerte** si un caso no tiene `summary` o `total` configurados, en vez de enviar un correo para certificar.
- Validar el JSON del caso en el despliegue (existencia de `summary`, `total`, base y periodo) y compararlo entre entornos como parte del pipeline.

---

## 6. Qué muestra el CSV de Athena y cómo se llegó a las conclusiones

### 6.1 De dónde sale
Es el resultado de esta consulta, ejecutada en Athena (prod) sobre la tabla Gold de Primas ARL:

```sql
SELECT act_reg, per_proceso_point, COUNT(*) AS registros
FROM "niif-prd-gold-catalog-db"."scd_primas_arl"
GROUP BY 1, 2
ORDER BY 1 DESC, 2 DESC;
```
Archivo: `errores/evidencia-primas-arl-202606/athena/SELECT act_reg, per_proceso_point COUNT() AS registros.csv` (49 filas).

### 6.2 Cómo leerlo
Cada fila es un grupo: cuántos registros (`registros`) hay con ese `act_reg` y ese `per_proceso_point` (periodo de proceso, formato `AAAAMM`).

| Columna | Significado |
|---|---|
| `act_reg = 1` | Registro **activo** (vigente) en Gold. Es el único que ven las consultas `total` y `detail` del correo (`WHERE act_reg = 1`) |
| `act_reg = 0` | Registro **inactivo** (histórico o apagado por el SCD) |
| `per_proceso_point` | Periodo del endoso. Las consultas del correo filtran `= '202606'` |
| (vacío) | `per_proceso_point` nulo: el endoso no tiene fecha de proceso |

Las únicas dos filas con `act_reg = 1` del CSV:

| act_reg | per_proceso_point | registros |
|---|---|---|
| 1 | 202610 | 206 |
| 1 | (nulo) | 3 |

Todas las demás filas (47) tienen `act_reg = 0`.

### 6.3 Paso a paso de las validaciones
Cada paso responde una pregunta, se calcula sobre el CSV y se compara con otra fuente independiente (las métricas del SCD en `parametros_extraccion_primas_arl.json`). Así ninguna conclusión depende de una sola fuente.

| # | Pregunta | Cómo se calcula | Resultado | Conclusión |
|---|---|---|---|---|
| 1 | ¿Cuántos registros activos hay en Gold? | Sumar `registros` de las filas con `act_reg = 1` | 206 + 3 = **209** | Solo quedan 209 activos |
| 2 | ¿Coincide con lo que el SCD dijo que insertó? | Comparar con `metricas_gold_scd.total_insertados_nuevos` | 209 = **209** | Los únicos activos son los registros nuevos de la corrida del 2 de noviembre |
| 3 | ¿Algún activo es del periodo 202606? | Contar filas con `act_reg = 1` y `per_proceso_point = 202606` | **0** | La consulta `total` (que filtra periodo 202606 y `act_reg = 1`) devuelve 0 filas → "Totales: No hay datos" |
| 4 | ¿El CSV cuadra con el Gold final del SCD? | Sumar **todas** las filas y comparar con `total_gold_final` | 1.207.452 = **1.207.452** | El CSV es completo y confiable |
| 5 | ¿Cuántos inactivos hay y cuadran con el Gold previo? | Sumar `registros` con `act_reg = 0` y comparar con `total_gold_anterior` | 1.207.243 = **1.207.243** | Gold nuevo = Gold previo + 209; nada se borró, todo lo anterior quedó inactivo |
| 6 | ¿Qué pasó con el periodo 202606? | `registros` con `act_reg = 0` y `per_proceso_point = 202606` | **2.094** inactivos, 0 activos | El periodo existía en Gold y quedó inactivo (no es que no llegara). Un total correcto habría mostrado hasta 2.094 |
| 7 | ¿Hay datos sin periodo? | `registros` con `act_reg = 0` y `per_proceso_point` vacío | **1.100.009** (91,1 % de Gold) | Esos registros nunca aparecen en la certificación de ningún periodo (sin causa determinada, por analizar) |

### 6.4 Cómo se encadenan las conclusiones
1. **Pasos 1 a 3:** Gold solo tiene 209 activos y ninguno es de 202606 → la consulta `total` no puede devolver filas → "No hay datos". El CSV de detalle sale con solo el encabezado porque su consulta siempre incluye una fila de encabezado (`UNION ALL`, `0 AS orden`).
2. **Pasos 4 y 5:** el CSV coincide **exactamente** con las métricas del SCD (1.207.452 y 1.207.243). Esto prueba que el CSV es completo y que el SCD no perdió filas: las dejó inactivas.
3. **De las métricas del SCD** (no del CSV): `total_registros_apagados_por_ausencia = 1.203.789` = `registros_excluidos_por_segmento` de `par_endosos`. El CSV solo muestra el resultado (todo inactivo); las métricas explican el porqué: la entrada del SCD tenía 209 filas y apagó todo lo activo que no venía en ella.
4. **Conclusión:** el fallo está en los jobs de Glue (extracción incremental + SCD que apaga por ausencia). El resumen vacío es aparte (configuración del JSON del caso).

### 6.5 Qué NO prueba el CSV
- **No distingue** cuántos de los 1.207.243 inactivos eran activos antes y cuántos ya eran históricos. Ese reparto (1.203.789 apagados y 3.454 históricos) se **deduce** de las métricas del SCD y de la lógica de `scd.py`, no del CSV.
- **No explica por qué** los 209 nuevos son de octubre (202610) si el `WHERE` de la extracción era `PERIODO <= '202606'`. Es una hipótesis sin verificar (columnas distintas: `PERIODO` contra `per_proceso_point`).
- **No dice nada** de los 13 hallazgos de validación técnica ni de por qué hay 1,1 millones de registros sin periodo.

### 6.6 Cómo reproducir las validaciones

**Opción A: script (recomendado).** Hace los 4 primeros chequeos de forma automática y sale con código 0 si todos pasan:
```
python -I errores/validar-athena.py errores/evidencia-primas-arl-202606
```

**Opción B: a mano en PowerShell**, sobre el mismo CSV:
```powershell
$csv = Import-Csv "errores\evidencia-primas-arl-202606\athena\SELECT act_reg, per_proceso_point COUNT() AS registros.csv"
($csv | Where-Object { $_.act_reg -eq '1' } | Measure-Object registros -Sum).Sum                                  # 1) 209
@($csv | Where-Object { $_.act_reg -eq '1' -and $_.per_proceso_point -eq '202606' }).Count                        # 3) 0
($csv | Measure-Object registros -Sum).Sum                                                                        # 4) 1207452
($csv | Where-Object { $_.act_reg -eq '0' } | Measure-Object registros -Sum).Sum                                  # 5) 1207243
($csv | Where-Object { $_.act_reg -eq '0' -and $_.per_proceso_point -eq '202606' }).registros                      # 6) 2094
($csv | Where-Object { $_.act_reg -eq '0' -and -not $_.per_proceso_point }).registros                              # 7) 1100009
```
Los valores del comentario de cada línea son los obtenidos con el CSV real.

**Opción C: contraste con las métricas del SCD** (para los pasos 2, 4 y 5), con el archivo `errores/evidencia-primas-arl-202606/s3/parametros_extraccion_primas_arl.json`, campo `metricas_gold_scd`:
```
total_insertados_nuevos = 209
total_gold_anterior     = 1.207.243
total_gold_final        = 1.207.452
total_registros_apagados_por_ausencia = 1.203.789
```

**Opción D: directamente en Athena** (si hay acceso), para repetir el mismo conteo con el dato vivo:
```sql
SELECT act_reg, COUNT(*) AS registros
FROM "niif-prd-gold-catalog-db"."scd_primas_arl"
GROUP BY 1;   -- esperado hoy: act_reg=1 → 209 ; act_reg=0 → 1.207.243
```
