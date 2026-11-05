---
tipo: brecha
actualizado: 2026-11-05
---
# Incidente en prod: certificación Primas ARL periodo 202606 con resumen y totales vacíos

Humano: `errores\` (`errores.md`, `analisis-flujo-primas-arl-202606.md`, `replicar-fallo-en-dev.md`). Relacionado: [[arquitectura-niif17]], [[riesgos-glue-notificacion]], [[inventario-lambdas]], [[hu-trazabilidad]], [[job-notificacion-ses]]. Preguntas y pendientes: [[preguntas-abiertas]], [[pendientes]].

IDs de la matriz (fase 3): **sin asignar**. Usar `INC-1`…`INC-6` de abajo y mapear a `ARQ`/`CUM` cuando exista `matriz-brechas.md`.

## 1. Qué pasó (hechos comprobados)
- Correo de prod del 2026-11-01 22:23 (hora Colombia): asunto "NIIF 17 - Certificación Primas ARL Periodo 202606"; **Resumen: "No hay resumen configurado"**, **Totales: "No hay datos"**, CSV `202606-detail_1-…csv`, con enlaces Certificar / No certificar. No se debe certificar con ese correo.
- Ejecución `arl-202606-auto-20261102T030032Z-976ae7fc` de la Step Function `niif-prd-primas-arl`: 2026-11-02 03:00:32 a 03:24:02 UTC, **SUCCEEDED**, sin fallos ni reintentos. Origen `PROGRAMACION_GESTOR`, modo `NORMAL`, lote `1793588486`. Había una corrida previa del **mismo periodo** el 2026-10-05 16:45 UTC.
- Los 7 jobs de Glue terminaron `SUCCEEDED`. La validación técnica quedó `FAILED` con 13 hallazgos, pero el flujo continúa por diseño (no es la causa).
- El crawler de Gold terminó a las 03:22:56, antes de la certificación (03:22:58): **se descarta** que Athena consultara con catálogo desactualizado.
- Notificación en modo `SIMPLE`, `configKey = primas-arl/primasArl.json`; `primasArl` no está en `composite_groups.json`.

## 2. Cadena causal (INC-1 a INC-3, causa raíz en Glue)
1. **INC-1 Extracción incremental:** `par_endosos` usa `WHERE PERIODO <= '202606'` + `EXCLUIR_EXTRAIDOS` por `NID_ENDOSO`. Como el segmento del periodo ya existía (corrida del 5 oct), excluyó **1.203.789** llaves y trajo **209** nuevos. Hay 5 segmentos con exclusión (par_endosos, par_polizas, par_afiliaciones, acu_personas_juridicas, acu_personas_naturales).
2. **Silver parcial:** `silver-primas-arl` escribe con `overwrite` solo los lotes de `lotes_segmento`, que en los 5 segmentos es `[1793588486]` (solo el lote actual) → **209 registros**.
3. **INC-2 SCD apaga por ausencia:** `scd.py` (líneas 335-437) hace `full join` entre la entrada y Gold activo y pone `ACT_REG = 0` a lo que no viene (líneas 360-367). No tiene bandera para desactivarlo. Métricas: entrada 209, insertados 209, sin cambio 0, cerradas 0, **apagados 1.203.789**; Gold 1.207.243 → 1.207.452.
4. **Resultado en Gold (Athena real):** activos = **209** (206 del periodo 202610 + 3 con `per_proceso_point` nulo); **0 activos de 202606**; 202606 tiene **2.094 inactivos**. Total y detalle filtran `act_reg = 1` y el periodo → 0 filas.
5. **INC-3 CSV nunca vacío:** la consulta `detail` hace `UNION ALL` con una fila de encabezado, así el UNLOAD siempre genera archivo y `sinDatosPeriodo` no se activa (`niif_certification.py`, `exportar_csv_raw`). El correo salió sin aviso de "sin datos". **No asumir que el CSV de detalle trae filas.**
6. **INC-4 Resumen vacío (configuración, no Glue):** `queries.summary = []` en `primas-arl/primasArl.json`. `total` y `detail` sí existen.

La Step Function no decide nada de esto: solo arranca `GoldSCD` pasando `--RUTA_CONFIG_SCD` y `--RUTA_PARAMETROS_EXTRACCION`.

## 3. Hallazgo de diseño (hipótesis fuerte, sin verificar)
`scd.py` (líneas 207-210) **asume que Silver preparada es la foto completa**, "conformada por varios lotes reutilizados". En el código de **DEV**, la extracción, al excluir llaves, guarda en `lotes_segmento` los lotes de respaldo **más** el actual (`combinar_lotes_respaldados`, marca `LOTES_RESPALDO_EXCLUSION_V2`), de modo que Silver relee todo y el SCD no apaga nada. En prod los 5 segmentos muestran **solo el lote actual** y `lotes_respaldo_exclusion` nulo. **Hipótesis INC-5:** el job de extracción desplegado en prod **no es la misma versión** que el código de DEV del repo. Comprobar: comparar el script de `niif-prd-extraccion-estandar-optimized` con el repo (buscar `combinar_lotes_respaldados`) y preguntar si QA ya tiene esa versión.

## 4. Hallazgos secundarios
- **INC-6:** 1.100.009 registros (91,1 % de Gold) tienen `per_proceso_point` nulo → nunca salen en la certificación de ningún periodo. Causa sin analizar.
- Los 209 "nuevos" son de 202610 pese al `WHERE PERIODO <= '202606'`. Hipótesis: `PERIODO` (columna de `par_endosos`) ≠ `per_proceso_point` (deriva de `FEC_PROCESO_POINT`). Sin verificar.
- Ningún job ni la Step Function alerta cuando el correo sale con resumen o totales vacíos: el flujo envía igual.
- Las otras mallas usan los mismos jobs genéricos (extracción y SCD): riesgo similar, no revisado.

## 5. Qué NO está comprobado
- Versión desplegada de la extracción en prod (INC-5).
- De qué lote(s) vienen los 1.203.789 excluidos (solo se ve la corrida previa del 5 oct; el campo de lotes de respaldo está vacío).
- Si el bucket Gold de prod tiene versionado de S3 o respaldo anterior al 2 nov (decide si se puede restaurar).
- Si QA se comporta igual (el usuario dijo que allí el correo sale completo, con datos replicados).
- Los logs de texto de los jobs de Glue **no se capturaron** (vacíos, falló CloudShell con error de credenciales 500). Las cifras se sustentan en las métricas de `parametros_extraccion.json` y en el CSV de Athena.

## 6. Acciones propuestas (no ejecutadas)
1. No volver a correr el flujo de Primas ARL ni certificar el periodo hasta resolverlo.
2. Ver versionado de Gold y restaurar desde antes del 2 nov (mejor opción); verificar con la consulta de `act_reg` por periodo.
3. "Limpiar los segmentos" (propuesta del equipo de prod) es **parche, no solución**: la simulación (`simular-limpieza-segmentos.py`) recupera 1.203.998 activos pero deja 1.203.789 llaves con dos filas (historial duplicado) y el fallo se repite en la siguiente re-ejecución incremental. Si se hace: limpiar los **5** segmentos, respaldar antes (copiar, no borrar).
4. Corregir la causa: que la extracción conserve los lotes de respaldo (alinear prod con el código de DEV) o que el SCD no apague por ausencia en cargas incrementales.
5. Agregar `queries.summary` al JSON del caso (copiar de QA); no enviar el correo de certificación si resumen y totales están vacíos; detectar "sin datos" contando filas reales sin el encabezado.
6. Replicar y probar la corrección en DEV con `replicar-fallo-dev.sh` (flujo completo, valida destinatarios y agrega un correo).

## 7. Artefactos (rutas relativas a la raíz del repo)
- Documentos: `errores\errores.md` (paso a paso, CSV de Athena, consola web, logs), `errores\analisis-flujo-primas-arl-202606.md` (conclusión, flujo, evidencia por problema, sección 9 limpiar segmentos), `errores\replicar-fallo-en-dev.md`.
- Scripts: `recolectar-evidencia-prd.sh` (CloudShell, solo lectura), `replicar-fallo-dev.sh` (**solo DEV**), `validar-analisis.py` (17/17 PASS), `validar-athena.py` (4/4 PASS), `simular-limpieza-segmentos.py`, `ver-logs-evidencia.py`.
- Evidencia: `errores\evidencia-primas-arl-202606\` (incluye `athena\` con el CSV) y `errores\Evidencia\` (ignorada en `.gitignore`).
- Estado de pruebas: los scripts de validación corren sobre la evidencia real; `replicar-fallo-dev.sh` solo se probó con un `aws` simulado, **no contra AWS real**.

## 8. Cautelas
- **Sin acceso directo a prod** desde este entorno. El usuario opera la consola y CloudShell de prod y trae capturas o archivos. Todo comando contra prod es de solo lectura.
- El repo `yeferson-nova/alfa` es **público**. El equipo subió la evidencia de prod (commit `inprod1`), que contiene correos de personas, identificadores de Cognito y de cuentas. Decisión pendiente (redactar, reescribir historial o pasar el repo a privado). No subir más evidencia sin redactar.
- La Step Function de DEV tiene **fijas** la ruta de la config de extracción y la del caso: la entrada `rutaConfigExtraccion` se ignora. Para cambiar el `WHERE` en pruebas hay que modificar y restaurar ese archivo.
- Distinguir **corrida** (5 oct) de **periodo** (202610): los 1.203.789 son de muchos periodos, cargados en una corrida anterior.

## 9. Datos de apoyo

**Excluidos por segmento (periodo 202606, 2 nov):** par_endosos 1.203.789 (209 llaves nuevas), par_polizas 36.928 (67), par_afiliaciones 7.013 (5), acu_personas_juridicas 4.938 (3), acu_personas_naturales 1.228.714 (4.540). `cgr_tipos_endosos`, `gar_tipos_endosos_arp` y `cgr_sucursales_alfa` no se segmentan.

**Código analizado (repo, extraído de DEV; prod no se ha extraído):**
- `Backend\Glue\niif-dev-scd\scd.py`: regla de apagado líneas 340-367; comentario "foto completa" 207-210.
- `Backend\Glue\niif-dev-extraccion-estandar-optimized\extraccion_estandar_optimized.py`: exclusión 617-775, guardado de llaves 778-856, lotes de respaldo (`combinar_lotes_respaldados`, línea 1412).
- `Backend\Glue\niif-dev-silver-primas-arl\silver_primas_arl.py`: unión de 5 tablas y `dropDuplicates(NID_POLIZA, NID_ENDOSO)` sin orden (líneas 416-460).
- `Backend\Glue\niif-dev-certification\niif_certification.py` (`exportar_csv_raw`, líneas 573-620) y `Backend\Glue\niif-dev-notification-certification\notification_certification.py` (SNS 78-156 y 598-612).
- Carpeta `Backend\StepFunctions\`, archivo que termina en `stateMachine_niif-dev-primas-arl.json`: la ruta de la config de extracción, la de SCD y el `configKey` del caso están **fijos** en la definición.

**Cómo se decide a quién llega el correo (verificado en el código):** el JSON del caso trae `email.destinations` y `approvers.users`. El job de notificación publica **un mensaje SNS por destinatario** en el tópico `niif-<env>-certification-email` con el atributo `recipient`; antes de enviar pone a cada suscripción de correo **confirmada** el `FilterPolicy {"recipient": ["<correo>"]}`. Recibe quien esté en `destinations` **y** tenga suscripción confirmada. Los envíos reales quedan en DynamoDB `niif-<env>-notification-certification` (`emailDestination`, `sentDate`). Para agregar un correo en DEV: `replicar-fallo-dev.sh agregar-email <correo>` (edita el JSON, crea la suscripción; el dueño debe confirmarla), luego `filtro <correo>`; `aislar <correo>` deja la prueba enviando solo a ese correo y `restaurar` lo revierte.

**Cómo verlo en la consola web de AWS:** `errores\errores.md` sección 7 (10 pasos de solo lectura: Athena, versionado de Gold, segmentos en S3, logs de Glue, SNS/DynamoDB, Step Functions) y sección 8 (línea de tiempo con los registros capturados, `ver-logs-evidencia.py`). Horas UTC del 2 nov: extracción 03:00:32, validación 03:09:15, remediación 03:12:16, Silver 03:14:17, SCD 03:17:23, certificación 03:22:58, notificación 03:23:43.

**Para el usuario sin jerga:** el segmento ya existía de la corrida del 5 oct; el 2 nov la extracción trajo solo lo nuevo (209) y el SCD lo tomó como "todo lo que existe", apagando el resto. Es la lógica correcta para carga completa y errónea para incremental.
