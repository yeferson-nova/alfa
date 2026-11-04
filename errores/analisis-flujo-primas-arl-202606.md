# Análisis del incidente: certificación Primas ARL 202606 sin datos (prod)

**Correo:** "NIIF 17 - Certificación Primas ARL Periodo 202606", 2026-11-01 22:23 (hora Colombia).
**Ejecución:** `niif-prd-primas-arl` → `arl-202606-auto-20261102T030032Z-976ae7fc` (2026-11-02 03:00 a 03:24 UTC), estado `SUCCEEDED`.
**Evidencia:** carpeta `Evidencia/` (recolectada con `recolectar-evidencia-prd.sh`, no se sube a git por contener datos de prod).
**Validación con código:** `validar-analisis.py` → **17 de 17 verificaciones PASS**.

## Conclusión

> El fallo se origina en los **jobs de Glue** de Primas ARL, no en la infraestructura: la extracción incremental (`EXCLUIR_EXTRAIDOS`) y la preparación Silver entregan solo los registros nuevos (209), y el job `niif-prd-scd` los interpreta como la foto completa, apagando **1.203.789** registros activos de Gold (`ACT_REG = 0`). El flujo no falló técnicamente (Step Function y todos los jobs en `SUCCEEDED`). Adicionalmente, el JSON del caso no tiene consulta de resumen (`queries.summary = []`), lo cual es configuración y no un job de Glue.

| Job de Glue | Qué hace | Evidencia |
|---|---|---|
| `niif-prd-scd` (causa principal) | Trata la entrada como foto completa y apaga lo ausente | `scd.py` líneas 360-367; 1.203.789 apagados |
| `niif-prd-extraccion-estandar-optimized` | Con `EXCLUIR_EXTRAIDOS` entrega solo los nuevos | 209 extraídos, 1.203.789 excluidos |
| `niif-prd-silver-primas-arl` | Escribe con `overwrite` solo el lote actual | `total_registros_silver_preparada` = 209 |
| `niif-prd-certification` (efecto) | El CSV de detalle siempre trae encabezado, no detecta "sin datos" | `niif_certification.py` líneas 573-620 |

## 1. Resumen

El correo salió con "Resumen: No hay resumen configurado" y "Totales: No hay datos". Son **dos problemas distintos** y un tercero que impidió notarlos:

| # | Problema | Dónde | Gravedad |
|---|---|---|---|
| 1 | El JSON del caso no tiene consulta de resumen (`queries.summary = []`) | Configuración `primas-arl/primasArl.json` | Media |
| 2 | **El SCD de Gold apagó 1.203.789 registros activos**: tras la corrida solo quedan 209 con `act_reg = 1` | Job `niif-prd-scd` (diseño) | **Alta** |
| 3 | El CSV de detalle nunca queda vacío (siempre lleva encabezado), así que no se avisa de "sin datos" | Job `niif-prd-certification` / consulta `detail` | Media |

Ningún job falló: todo terminó en `SUCCESS`, por eso nada alertó.

### Dónde está la evidencia de cada problema

Rutas relativas a la carpeta `errores/`. Para abreviar, `EV` = `Evidencia/evidencia-primas-arl-202606/evidencia-primas-arl-202606` (la carpeta queda anidada porque el `.tar.gz` se extrajo dentro de otra con el mismo nombre). La carpeta `Evidencia/` no se versiona en git; vive en el equipo local donde se descargó.

| # | Problema | Archivo de evidencia | Campo o línea |
|---|---|---|---|
| 1 | Resumen vacío | `EV/s3/config_caso_1_primasArl.json` | `queries.summary` = `[]` (línea 16); `queries.total` (línea 17) y `queries.detail` (línea 9) sí existen |
| 1 | Resumen vacío | `EV/00-resumen.txt` | Línea 30: `"summary":0,"total":1,"detail":1,"dataPresence":0` |
| 1 | Resumen vacío | `Evidencia/LOG.txt` | Salida de `jq '.queries.total'` ejecutada en CloudShell (texto de la consulta `total#1`) |
| 2 | SCD apagó 1.203.789 registros | `EV/s3/parametros_extraccion_primas_arl.json` | `metricas_gold_scd` (entrada 209, anterior 1.207.243, apagados 1.203.789, final 1.207.452) |
| 2 | Extracción incremental | `EV/s3/parametros_extraccion_primas_arl.json` | `datasets_extraidos[arpprod/par_endosos].segmentacion_horizontal`: `modo_segmentacion`, `total_llaves_segmento` = 209, `registros_excluidos_por_segmento` = 1.203.789 |
| 2 | Extracción incremental | `EV/s3/config_extraccion_primas_arl.json` | `where` de `par_endosos` = `PERIODO <= '202606'` y modo `EXCLUIR_EXTRAIDOS` |
| 2 | Silver con solo 209 | `EV/s3/parametros_extraccion_primas_arl.json` | `total_registros_silver_preparada` = 209, `modo_escritura_silver_preparada` = `overwrite` |
| 2 | Periodo ya corrido antes | `EV/stepfunction/ejecuciones-recientes.json` | Dos ejecuciones `arl-202606-auto-…`: 2026-10-05 y 2026-11-02 |
| 2 | Flujo terminó bien | `EV/stepfunction/01-estado-y-entrada.json` y `EV/glue/job-runs-relevantes.jsonl` | Estado `SUCCEEDED` y corridas de los jobs en `SUCCEEDED` |
| 2 | Código del SCD | `Backend/Glue/niif-dev-scd/scd.py` | Líneas 340-367 (`full join` y apagado por ausencia con `ACT_REG = 0`) |
| 3 | CSV nunca vacío | `EV/s3/config_caso_1_primasArl.json` | `queries.detail[0].query`: `UNION ALL` con fila de encabezado (`0 AS orden`) |
| 3 | `sinDatosPeriodo` no se activa | `Backend/Glue/niif-dev-certification/niif_certification.py` | Líneas 573-620 (`exportar_csv_raw`) |
| — | Entrada de la ejecución y `configKey` usado | `EV/stepfunction/01-estado-y-entrada.json` y `EV/stepfunction/05-configKey-resueltos.json` | `primas-arl/primasArl.json`, modo `SIMPLE` |
| — | Archivo original descargado | `Evidencia/evidencia-primas-arl-202606.tar.gz` | Paquete completo tal como salió de CloudShell |

Las verificaciones de estas rutas se ejecutan con `python -I validar-analisis.py Evidencia/evidencia-primas-arl-202606/evidencia-primas-arl-202606`.

## 2. Flujo y dónde falló

```
Programación del gestor (origen PROGRAMACION_GESTOR, 03:00 UTC)
  │  entrada: malla PRIMAS_ARL, periodoNegocio 202606, modoEjecucion NORMAL
  ▼
[1] Extracción (extraccion-estandar-optimized)          OK  — pero INCREMENTAL
  │  WHERE PERIODO <= '202606' + EXCLUIR_EXTRAIDOS por NID_ENDOSO
  │  extrajo 209 endosos; excluyó 1.203.789 ya extraídos
  ▼
[2] Validación técnica (val-tecnica)                    FAILED con 13 hallazgos, el flujo continúa (por diseño)
  ▼
[3] Remediación (remediacion-estandar)                  OK
  ▼
[4] Silver (silver-primas-arl)                          OK — overwrite: queda SOLO con los 209
  ▼
[5] Gold SCD (scd)                              ❌ AQUÍ SE ROMPE EL DATO
  │  full join: Silver preparada (209) contra Gold activo
  │  todo lo que no viene en la entrada → ACT_REG = 0 ("apagados por ausencia")
  │  apagó 1.203.789; insertó 209; Gold activo final = 209
  ▼
[6] Certificación (certification)               ⚠ detecta mal "sin datos"
  │  total: act_reg = 1 AND per_proceso_point = '202606' → 0 filas
  │  detail: UNION ALL con fila de encabezado → CSV con solo encabezado
  │  sinDatosPeriodo = false (porque el UNLOAD siempre trae la fila de encabezado)
  ▼
[7] Notificación (notification-certification)   ⚠ resumen y totales vacíos, igual envía
     resumen: queries.summary = []  → "No hay resumen configurado."
     totales: 0 filas               → "No hay datos"
     correo enviado a certificadores con enlaces Certificar / No certificar
```

## 3. Evidencia por punto (todo comprobado con `validar-analisis.py`)

### 3.1 El flujo terminó bien (no es falla técnica)
- Step Function `SUCCEEDED`; extracción, remediación, disponibilización Silver, preparación Silver y Gold SCD en `SUCCESS`.
- Modo de notificación `SIMPLE` (`primasArl` no está en ningún grupo de `composite_groups.json`).

### 3.2 Resumen vacío (configuración)
- `primas-arl/primasArl.json`: `queries.summary = []`, `queries.total` con 1 consulta, `queries.detail` con 1.
- El código del job de notificación muestra "No hay resumen configurado." cuando la lista de resumen está vacía.

### 3.3 La extracción fue incremental
- Existen dos ejecuciones del mismo periodo: `2026-10-05` y `2026-11-02`.
- `par_endosos`: `modo_segmentacion = EXCLUIR_EXTRAIDOS`, llave `NID_ENDOSO`.
- `total_registros = 209`; `registros_excluidos_por_segmento = 1.203.789`.
- Silver preparada: `total_registros_silver_preparada = 209`, modo `overwrite` (sin histórico).

### 3.4 El SCD apagó todo lo que no vino
Métricas de `metricas_gold_scd`:

| Métrica | Valor |
|---|---|
| `total_entrada_silver_preparada` | 209 |
| `total_gold_anterior` | 1.207.243 |
| `total_insertados_nuevos` | 209 |
| `total_registros_sin_cambio` | 0 |
| `total_versiones_cerradas_por_cambio` | 0 |
| `total_registros_apagados_por_ausencia` | **1.203.789** |
| `total_gold_final` | 1.207.452 (= anterior + insertados) |

- **Coincidencia exacta:** apagados por ausencia (1.203.789) = excluidos por segmento (1.203.789).
- Código (`scd.py`, líneas 340-367): el `full join` entre entrada y Gold activo marca `ACT_REG = 0` a todo registro activo que no esté en la entrada.
- **Deducido de las métricas:** activos antes = 1.203.789; históricos antes = 3.454; activos después = **209**.

### 3.5 Por qué los totales y el CSV salen vacíos
- Las consultas `total` y `detail` filtran `act_reg = 1` y `per_proceso_point = '$period'`. Con solo 209 activos, no devuelven filas del periodo.
- La consulta `detail` hace `UNION ALL` con una fila de encabezado (`0 AS orden`), así el UNLOAD siempre genera archivo. En `niif_certification.py` (`exportar_csv_raw`) `sinDatosPeriodo` solo se activa si el UNLOAD no genera archivos, por lo que nunca se activa aquí.

## 4. Lo que NO está comprobado

| Punto | Estado |
|---|---|
| Que los 209 activos no son del periodo 202606 | **Deducido** de que el total salió sin filas. Los logs de Athena no se recolectaron (error de credenciales de CloudShell). Se confirma con la consulta de la sección 6. |
| Por qué en QA el correo sale bien | **Hipótesis**: en QA fue una carga inicial completa. No verificado. |
| Si existe versionado o respaldo de `scd_primas_arl` en el bucket Gold | **Sin verificar** |
| Los 13 hallazgos de validación técnica | No son la causa (el flujo continúa por diseño). No analizados a fondo. |

## 5. Impacto y riesgos

- **Gold de prod quedó con 209 registros activos** de Primas ARL. Cualquier otro periodo que se certifique ahora saldrá vacío.
- La certificación del 5 de octubre ya no es reproducible desde Gold activo.
- El correo con enlaces **Certificar / No certificar** se envió sin resumen ni totales. No se debe certificar con él.
- El mismo patrón (extracción incremental con `EXCLUIR_EXTRAIDOS` + SCD que apaga por ausencia) puede estar en las otras mallas que usen los mismos jobs. No revisado.

## 6. Cómo confirmar el punto pendiente (solo lectura, Athena en prod)

```sql
SELECT act_reg, per_proceso_point, COUNT(*) AS registros
FROM "niif-prd-gold-catalog-db"."scd_primas_arl"
GROUP BY 1, 2
ORDER BY 1 DESC, 2 DESC;
```
Resultado esperado si el análisis es correcto: `act_reg = 1` con 209 registros en total, ninguno con `per_proceso_point = '202606'`.

## 7. Acciones recomendadas

| # | Acción | Prioridad |
|---|---|---|
| 1 | No volver a ejecutar el flujo de Primas ARL ni certificar el periodo hasta resolverlo | Inmediata |
| 2 | Verificar si `alfa-datalake-prd-gold` tiene versionado de S3 o respaldo de `niif/scd_primas_arl/` y restaurar el estado previo a la corrida del 2 de noviembre | Inmediata |
| 3 | Ejecutar la consulta de la sección 6 para confirmar | Alta |
| 4 | Agregar `queries.summary` al JSON de prod `primas-arl/primasArl.json` (tomándolo del de QA) | Alta |
| 5 | Corregir el diseño: que el SCD no apague por ausencia cuando la entrada es incremental (o que la extracción no excluya llaves cuando alimenta un SCD de foto completa) | Alta |
| 6 | Detectar "sin datos" en el detalle contando filas reales sin contar el encabezado, y no enviar el correo si total y resumen están vacíos | Media |
| 7 | Revisar si las otras mallas comparten el mismo riesgo | Media |
| 8 | Comparar el JSON del caso y el de extracción entre QA y prod | Media |

## 8. Archivos relacionados en esta carpeta

| Archivo | Qué es |
|---|---|
| `errores.md` | Paso a paso inicial para validar el incidente |
| `recolectar-evidencia-prd.sh` | Script de CloudShell que recolecta la evidencia (solo lectura) |
| `validar-analisis.py` | Verificaciones con código de este análisis (`python -I validar-analisis.py <carpeta-evidencia>`) |
| `replicar-fallo-en-dev.md` y `replicar-fallo-dev.sh` | Cómo replicar el fallo en DEV, validar destinatarios y agregar un correo |
| `simular-limpieza-segmentos.py` | Simulación de la propuesta "limpiar los segmentos" (sección 9) |
| `Evidencia/` | Evidencia recolectada de prod (no se versiona; datos sensibles) |

## 9. Propuesta de prod: "limpiar los segmentos de Primas ARL"

Quien ejecutó los jobs en prod propone limpiar los segmentos. Esta sección explica qué es y qué efecto tiene.

### 9.1 Qué es un segmento
La extracción guarda, por cada tabla segmentada y por periodo, **la lista de llaves que ya extrajo** (por ejemplo los `NID_ENDOSO`). Está en S3:

```
s3://niif-prd-assets/segmentos/niif17_<periodo>_alfaprd_<esquema>_<tabla>/llaves/lote_cargue_id=<lote>/   (Parquet)
```

- Cada corrida agrega una carpeta `lote_cargue_id=<lote>` con las llaves que **extrajo en esa corrida** (`guardar_llaves_segmentacion_s3`, líneas 778-856 de `extraccion_estandar_optimized.py`).
- En la siguiente corrida del mismo periodo, con `modo = EXCLUIR_EXTRAIDOS`, la extracción lee **todas** esas llaves y **excluye de Oracle** las que ya estén (si además existen en Bronce o Silver de su lote) (`aplicar_exclusion_llaves_segmento`, líneas 617-775).
- Si la carpeta `.../<segmento_id>/llaves/` **no existe**, no excluye nada y extrae todo (líneas 662-663).

Para Primas ARL 202606 hay **5 segmentos** (de la evidencia): `par_endosos`, `par_polizas`, `par_afiliaciones`, `acu_personas_juridicas` y `acu_personas_naturales`.

### 9.2 Qué significa "limpiar"
Borrar o vaciar esas carpetas de llaves para que la próxima corrida **no excluya nada** y extraiga todo (`PERIODO <= '202606'`). No borra datos de Bronce, Silver ni Gold: solo "olvida" qué se extrajo.

### 9.3 Qué efecto tiene (simulado con la lógica del SCD)
`simular-limpieza-segmentos.py` aplica la lógica de `scd.py` (líneas 335-437) con los números reales de prod. Primero reproduce el fallo (209 insertados, 1.203.789 apagados, 209 activos: coincide con prod) y luego prueba la propuesta:

| Escenario | Activos en Gold | Observación |
|---|---|---|
| Después del 2 nov (el fallo) | 209 | Estado actual |
| Limpiar segmentos + re-ejecutar (extracción completa) | **1.203.998** | Recupera los activos |
| Qué deja atrás | Total 2.411.241 filas | **1.203.789 llaves con dos filas** (una cerrada el 2 nov y otra activa reiniciada en versión 1): historial duplicado |
| Siguiente re-ejecución incremental del periodo (ej. 150 nuevos) | 150 | **El fallo se repite** |

**Conclusión:** limpiar segmentos es un **parche** que recupera Gold, pero no corrige la causa (el SCD apaga por ausencia con entrada incremental) y deja el historial SCD duplicado.

### 9.4 Cuidados si se hace
- Limpiar los **5 segmentos** del periodo, no solo el de endosos (si no, los cruces de Silver quedan incompletos). *Deducido del código, no probado.*
- **Respaldar antes de tocar** (`aws s3 sync` del prefijo `segmentos/niif17_202606_*` a otra ruta); mover, no borrar.
- Mejor orden: restaurar Gold desde respaldo o versión de S3 anterior al 2 de noviembre (si existe); el parche solo si no hay respaldo; y después corregir el SCD.

### 9.5 Cómo ejecutar la simulación
```
python -I simular-limpieza-segmentos.py
```
- No necesita AWS ni librerías: solo Python 3. Tarda unos segundos.
- `-I` ejecuta Python en modo aislado (ignora variables de entorno y la carpeta actual al importar).
- Construye Gold en memoria como una lista de filas (1.207.243 en total), aplica la función `scd()` y verifica cada resultado con `assert`. Si el modelo no reprodujera el fallo de prod, el script se detiene con error.
- Código de salida `0` = todas las verificaciones pasaron. Es una simulación de la **lógica**, no una ejecución sobre datos reales.
