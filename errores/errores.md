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
Archivo: `EvidenciasAws/prd-primas-arl-202606/subida-equipo/athena/SELECT act_reg, per_proceso_point COUNT() AS registros.csv` (49 filas).

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
python -I errores/validar-athena.py EvidenciasAws/prd-primas-arl-202606/subida-equipo
```

**Opción B: a mano en PowerShell**, sobre el mismo CSV:
```powershell
$csv = Import-Csv "EvidenciasAws\prd-primas-arl-202606\subida-equipo\athena\SELECT act_reg, per_proceso_point COUNT() AS registros.csv"
($csv | Where-Object { $_.act_reg -eq '1' } | Measure-Object registros -Sum).Sum                                  # 1) 209
@($csv | Where-Object { $_.act_reg -eq '1' -and $_.per_proceso_point -eq '202606' }).Count                        # 3) 0
($csv | Measure-Object registros -Sum).Sum                                                                        # 4) 1207452
($csv | Where-Object { $_.act_reg -eq '0' } | Measure-Object registros -Sum).Sum                                  # 5) 1207243
($csv | Where-Object { $_.act_reg -eq '0' -and $_.per_proceso_point -eq '202606' }).registros                      # 6) 2094
($csv | Where-Object { $_.act_reg -eq '0' -and -not $_.per_proceso_point }).registros                              # 7) 1100009
```
Los valores del comentario de cada línea son los obtenidos con el CSV real.

**Opción C: contraste con las métricas del SCD** (para los pasos 2, 4 y 5), con el archivo `EvidenciasAws/prd-primas-arl-202606/subida-equipo/s3/parametros_extraccion_primas_arl.json`, campo `metricas_gold_scd`:
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

---

## 7. Pasos en la consola web de AWS para seguir el proceso

Todos son de **solo lectura**. Cuenta de prod (`992382496548`), región **us-east-1** (Norte de Virginia). No hacer clic en "Certificar" ni "No certificar" del correo, y no borrar nada de S3.

### Paso 1. Confirmar el estado actual de Gold (Athena)
1. Consola → buscar **Athena** → **Query editor**.
2. Arriba a la izquierda: *Data source* `AwsDataCatalog`, *Database* `niif-prd-gold-catalog-db`.
3. Pegar y ejecutar:
   ```sql
   SELECT act_reg, COUNT(*) AS registros
   FROM "scd_primas_arl"
   GROUP BY 1;
   ```
4. **Qué mirar:** `act_reg = 1` → 209 y `act_reg = 0` → 1.207.243 (si nadie corrigió Gold desde el 2 de noviembre). Si los números cambiaron, alguien ya intervino.

### Paso 2. Ver si hay respaldo o versiones de Gold (S3)
1. Consola → **S3** → *Buckets* → `alfa-datalake-prd-gold`.
2. Pestaña **Properties** (Propiedades) → sección **Bucket Versioning**: ¿dice *Enabled* (Habilitado) o *Suspended/Disabled*?
3. Pestaña **Objects** → abrir `niif/` → `scd_primas_arl/`.
4. Activar el interruptor **List versions** (Mostrar versiones). Buscar archivos `.parquet` con *Last modified* **anterior al 2 de noviembre de 2026 (03:17 UTC)**.
5. **Qué concluir:** si hay versiones anteriores, se puede **restaurar Gold sin recargar** (es la mejor opción). Si el versionado está deshabilitado, no hay forma de recuperarlo desde S3 y habría que recargar.

### Paso 3. Ver los segmentos del periodo 202606 (S3)
1. **S3** → bucket `niif-prd-assets` → carpeta `segmentos/`.
2. Debes ver 5 carpetas `niif17_202606_alfaprd_...`: `arpprod_par_endosos`, `arpprod_par_polizas`, `arpprod_par_afiliaciones`, `general_acu_personas_juridicas`, `general_acu_personas_naturales`.
3. Abrir `niif17_202606_alfaprd_arpprod_par_endosos/` → `llaves/`.
4. **Qué mirar:** carpetas `lote_cargue_id=<número>`. Debe haber una de la corrida del **5 de octubre** (llaves de ~1,2 millones de endosos) y una de la del **2 de noviembre**: `lote_cargue_id=1793588486` (209 llaves). Fíjate en la fecha de *Last modified* de cada una.
5. **Esto es lo que "limpiar los segmentos" borraría.** No lo borres: si hay que hacerlo, antes se copia a otra ruta.

### Paso 4. Confirmar las cifras en el log del job SCD (Glue + CloudWatch)
1. Consola → **AWS Glue** → *ETL jobs* → `niif-prd-scd` → pestaña **Runs**.
2. Abrir la corrida del **2 de noviembre ~03:17 UTC** (id empieza por `jr_f93edfd1...`).
3. En el detalle de la corrida: **Output logs** (abre CloudWatch).
4. Buscar (Ctrl + F) estas líneas:
   - `Registros nuevos insertados      : 209`
   - `Registros apagados por ausencia  : 1203789`
   - `Gold activos actuales` (debe ser ~1.203.789)
5. **Qué concluir:** si coinciden, queda confirmado en el log de prod el apagado masivo.

### Paso 5. Confirmar la exclusión en el log de extracción (Glue + CloudWatch)
1. **AWS Glue** → *ETL jobs* → `niif-prd-extraccion-estandar-optimized` → **Runs**.
2. Abrir la corrida del **2 de noviembre ~03:00 UTC** (id `jr_b6f2322a...`).
3. **Output logs** → buscar `exclusión respaldada por clave y lote. Filas excluidas=`.
4. **Qué mirar:** para `arpprod/par_endosos` debe decir `Filas excluidas=1203789`.

### Paso 6. Ver por qué entraron endosos de octubre (Athena, punto sin verificar)
1. **Athena** → *Database* `niif-prd-silver-catalog-db`.
2. Ejecutar:
   ```sql
   SELECT periodo,
          date_format(fec_proceso_point, '%Y%m') AS per_proceso,
          COUNT(*) AS registros
   FROM "alfaprd_arpprod_par_endosos"
   WHERE lote_cargue_id = 1793588486
   GROUP BY 1, 2
   ORDER BY 1, 2;
   ```
3. **Qué mirar:** si `periodo` es ≤ 202606 pero `per_proceso` es 202610, queda **confirmada la hipótesis** (el filtro de extracción usa `PERIODO` y la certificación usa el periodo de `FEC_PROCESO_POINT`). Si `periodo` ya es 202610, el `WHERE` no se aplicó como se esperaba.

### Paso 7. Revisar la configuración del caso (S3)
1. **S3** → `niif-prd-assets` → `primas-arl/` → `primasArl.json` → **Open** o **Download**.
2. Buscar `"summary"`. **Qué mirar:** `"summary": []` (vacío) significa que no hay resumen configurado.
3. Comparar con el mismo archivo en la cuenta de **QA**: ahí `summary` sí debe traer una consulta.
4. Ver también `"destinations"` (quién recibe el correo).

### Paso 8. Ver quién recibió el correo (SNS y DynamoDB)
1. Consola → **SNS** → *Topics* → `niif-prd-certification-email` → pestaña **Subscriptions**.
2. **Qué mirar:** cada suscripción de correo con *Status* `Confirmed` o `Pending confirmation`, y en el detalle el **Subscription filter policy** (`{"recipient": ["correo"]}`).
3. Consola → **DynamoDB** → *Tables* → `niif-prd-notification-certification` → **Explore table items**.
4. Elegir **Query**, índice `certCode-index`, `certCode` = `202606-20261101-2223`.
5. **Qué mirar:** `status` (`SENT`), `sentDate`, `emailDestination` (lista real de destinatarios) y `sinDatosPeriodo` (esperado `false`, aunque no haya datos, por el encabezado del CSV).

### Paso 9. Ver lo que imprimió el job de notificación (CloudWatch)
1. Consola → **CloudWatch** → *Logs* → *Log groups* → `/aws-glue/jobs/output`.
2. En el buscador de *Log streams*, pegar el id de la corrida de notificación: `jr_314ef2e44d8e530794f849fec841eeecf5e6c49ce7985d679cbc50e1c1c88be8`.
3. Abrir el stream y buscar:
   - `[TABLES] tipo=summary, queries=0` (confirma que no hay resumen).
   - `[TABLES] tipo=total, queries=1`.
   - `[ATHENA] Query SUCCEEDED` o `FAILED`.
4. **Qué concluir:** si la consulta `total` terminó en `SUCCEEDED` y devolvió 0 filas, queda cerrado el punto de los totales.

### Paso 10. Recorrido visual de la ejecución (Step Functions)
1. Consola → **Step Functions** → *State machines* → `niif-prd-primas-arl` → **Executions**.
2. Abrir `arl-202606-auto-20261102T030032Z-976ae7fc` (estado *Succeeded*).
3. Pestaña **Table view** (Vista de tabla): ver la hora de inicio de cada paso. Referencias de la evidencia (UTC, 2 de noviembre): extracción 03:00:32, validación técnica 03:09:15, remediación 03:12:16, Silver 03:14:17, SCD 03:17:23, certificación 03:22:58 y notificación 03:23:43 (la ejecución terminó a las 03:24:02).
4. Clic en `ResolverNotificacion` → **Output**: `notificationMode = SIMPLE`, `configKey = primas-arl/primasArl.json`.
5. Clic en `GoldSCD` → **Input/Output** para ver los argumentos del job.

### Qué decide cada paso

| Paso | Si resulta… | Entonces… |
|---|---|---|
| 1 | Sigue 209 activos | Gold no se ha corregido |
| 2 | Hay versiones de Gold anteriores al 2 nov | Restaurar desde la versión (mejor opción) |
| 2 | Sin versionado | Recarga: respaldar segmentos y re-ejecutar (con las consecuencias de la sección 9 del análisis) |
| 4 y 5 | Coinciden las cifras | Confirmado el mecanismo con el log de prod |
| 6 | `periodo` ≤ 202606 y `per_proceso` = 202610 | Hipótesis confirmada: filtro por `PERIODO` ≠ periodo de certificación |
| 7 | `summary` vacío | Corregir configuración (copiar de QA) |
| 8 | Suscripción pendiente o sin filtro | Corregir destinatarios antes de reenviar |

---

## 8. Cómo verlo con los logs que ya tenemos (sin entrar a AWS)

La evidencia capturada (`EvidenciasAws/prd-primas-arl-202606/subida-equipo/`) ya permite reconstruir qué pasó, minuto a minuto, sin acceso a prod.

### 8.1 Qué registros hay y para qué sirven

| Archivo | Qué contiene | ¿Sirve? |
|---|---|---|
| `stepfunction/02-estados-entrada-salida.json` | Cada estado de la Step Function con hora, entrada y salida | Sí: línea de tiempo y orden del flujo |
| `stepfunction/03-fallos-y-reintentos.json` | Fallos y reintentos de la ejecución | Sí: está vacío (`[]`) → **no hubo fallos ni reintentos** |
| `glue/job-runs-relevantes.jsonl` | Corridas de los jobs de Glue: estado, inicio, fin, id, argumentos | Sí: todos `SUCCEEDED`, sin mensaje de error |
| `glue/<job>.runs.json` | Últimas 50 corridas de cada job (**de todas las mallas**) | Sí, filtrando por la hora de la ejecución |
| `logs/lambda-niif-prd-composite-notification-evaluator.json` | Log real de la lambda: `notificationMode`, `configKey`, duración | Sí |
| `s3/parametros_extraccion_primas_arl.json` | Métricas que escribió cada job (extracción, Silver, SCD) | Sí: es el "resultado" de cada job |
| `athena/*.csv` | Estado real de Gold (`act_reg` por periodo) | Sí (sección 6) |
| `logs/niif-prd-certification_..._output/error.*` | Log de salida de Glue | **No**: están **vacíos** (2 bytes): falló la captura por el error de credenciales de CloudShell |

### 8.2 Verlo todo junto (un comando)
```
python -I errores/ver-logs-evidencia.py EvidenciasAws/prd-primas-arl-202606/subida-equipo
```
No necesita AWS. Imprime cinco bloques: **A** estados de la Step Function en orden, **B** corridas de Glue de esa ejecución con duración, **C** log de la lambda, **D** métricas escritas por los jobs, **E** límites de lo capturado. Fragmentos de la salida real:

```
Ejecucion Step Function: estado=SUCCEEDED  2026-11-02 03:00:32 -> 03:24:02 UTC

B. GLUE
   niif-prd-extraccion-estandar-optimized   SUCCEEDED  03:00:32  03:07:29   416
   niif-prd-val-tecnica                     SUCCEEDED  03:09:15  03:11:18   123
   niif-prd-remediacion-estandar            SUCCEEDED  03:12:16  03:14:11   114
   niif-prd-silver-primas-arl               SUCCEEDED  03:14:17  03:15:56    99
   niif-prd-scd                             SUCCEEDED  03:17:23  03:19:39   136
   niif-prd-certification                   SUCCEEDED  03:22:58  03:23:52    53
   niif-prd-notification-certification      SUCCEEDED  03:23:43  03:24:31    48

C. LAMBDA   03:22:58  [RESOLVE_NOTIFICATION_RESULT] {"notificationMode": "SIMPLE", ... "configKey": "primas-arl/primasArl.json"}

D. Extraccion : registros extraidos=4,857  excluidos par_endosos=1,203,789  llaves nuevas=209
   Validacion : estado=FAILED  errores=13  (el flujo continua por diseno)
   Silver     : registros preparados=209  modo=overwrite
   Gold SCD   : entrada=209  insertados=209  apagados por ausencia=1,203,789  gold anterior=1,207,243  gold final=1,207,452
```

### 8.3 Cómo leer la línea de tiempo (qué concluye cada parte)

| Qué se ve | Qué significa |
|---|---|
| Todos los jobs `SUCCEEDED` y `03-fallos-y-reintentos.json` = `[]` | El flujo **no falló**: nada alertó porque técnicamente todo salió bien |
| Estados: `GoldSCD` 03:17:22 → `CrawlerGold` 03:20:12 → fin del crawler 03:22:56 → `ResolverNotificacion` 03:22:56 → certificación 03:22:58 | El crawler de Gold **terminó antes** de la certificación. **Descarta** la hipótesis de que Athena consultó con el catálogo sin actualizar (fila "Crawler terminó después del correo" de la sección 3) |
| Lambda: `notificationMode = SIMPLE`, `configKey = primas-arl/primasArl.json`, lote `1793588486` | El correo usó solo el JSON del caso; no es un caso compuesto |
| Extracción: `excluidos par_endosos = 1.203.789`, `llaves nuevas = 209` | La extracción fue **incremental** (solo 209 de ~1,2 millones) |
| Silver: `209`, `overwrite` | Silver quedó solo con los nuevos |
| SCD: `entrada = 209`, `apagados por ausencia = 1.203.789` | **Aquí está el fallo**: apagó todo lo que no venía en la entrada |
| Validación técnica `FAILED` con 13 errores | Hallazgos de datos; el flujo continúa por diseño (no es la causa) |

El orden de los hechos es el del fallo: **extracción incremental (03:00) → Silver parcial (03:14) → SCD apaga lo ausente (03:17) → certificación consulta Gold (03:22) → correo vacío (03:23)**.

### 8.4 Verlo a mano (PowerShell, sin instalar nada)
Desde la raíz del repo (`alfa`):

```powershell
$b = "EvidenciasAws\prd-primas-arl-202606\subida-equipo"

# Log de la lambda (hora UTC)
$e = Get-Content "$b\logs\lambda-niif-prd-composite-notification-evaluator.json" -Raw | ConvertFrom-Json
$e.events | ForEach-Object { "{0:HH:mm:ss}Z  {1}" -f ([DateTimeOffset]::FromUnixTimeMilliseconds($_.timestamp).UtcDateTime), $_.message.Trim() }

# Corridas de Glue del SCD (el log group base y el estado)
(Get-Content "$b\glue\niif-prd-scd.runs.json" -Raw | ConvertFrom-Json).JobRuns |
  Select-Object JobRunState, StartedOn, ExecutionTime, LogGroupName | Format-Table -AutoSize

# Métricas del SCD
(Get-Content "$b\s3\parametros_extraccion_primas_arl.json" -Raw | ConvertFrom-Json).metricas_gold_scd
```
Ojo: `glue\<job>.runs.json` mezcla corridas de **todas las mallas** (por ejemplo, el SCD tiene una corrida del 3 de noviembre a las 03:31 que no es de esta ejecución). La corrida de Primas ARL es la del **2 de noviembre 03:17:23**. El `LogGroupName` que muestra es el prefijo `/aws-glue/jobs`; los logs de salida están en `/aws-glue/jobs/output` y `/aws-glue/jobs/error`.

### 8.5 Qué no se puede ver con estos logs y cómo obtenerlo
Faltan las **líneas de texto** de los jobs de Glue (por ejemplo `Registros apagados por ausencia : 1203789` o `[TABLES] tipo=summary, queries=0`). Las cifras ya están confirmadas por las métricas (sección D del script) y por el CSV de Athena, pero para ver el log literal:
- En la consola web: pasos 4, 5 y 9 de la sección 7.
- Con el script `recolectar-evidencia-prd.sh` o los comandos de `aws logs get-log-events` (si las credenciales de CloudShell funcionan).
