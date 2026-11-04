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
