---
tipo: brecha
actualizado: 2026-11-06
---
# Incidente en prod: certificación Primas ARL periodo 202606 con resumen y totales vacíos

Humano: `errores\` (`errores.md`, `analisis-flujo-primas-arl-202606.md`, `replicar-fallo-en-dev.md`). Relacionado: [[arquitectura-niif17]], [[riesgos-glue-notificacion]], [[inventario-lambdas]], [[hu-trazabilidad]], [[job-notificacion-ses]]. Preguntas y pendientes: [[preguntas-abiertas]], [[pendientes]]. Familia de mallas a la que pertenece Primas ARL: [[familia-mallas-alfaprd]].

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
`scd.py` (líneas 207-210) **asume que Silver preparada es la foto completa**, "conformada por varios lotes reutilizados". En el código de **DEV**, la extracción, al excluir llaves, guarda en `lotes_segmento` los lotes de respaldo **más** el actual (`combinar_lotes_respaldados`, marca `LOTES_RESPALDO_EXCLUSION_V2`), de modo que Silver relee todo y el SCD no apaga nada. En prod los 5 segmentos muestran **solo el lote actual** y `lotes_respaldo_exclusion` nulo. **Hipótesis INC-5:** el job de extracción desplegado en prod **no es la misma versión** que el código de DEV del repo. **Confirmada el 2026-11-05**, ver 3b. Falta solo saber si QA tiene la versión nueva.

## 3b. INC-5 CONFIRMADA (2026-11-05, diff contra el script real de prod)
Copia del script de prod: `errores\extraccion_estandar_optimized.py` (sin datos sensibles; 1.863 líneas vs 1.886 del repo). Prod y repo son idénticos salvo 3 bloques de la exclusión por llaves:
1. Falta la función `combinar_lotes_respaldados` (inicio del archivo).
2. Falta `metadata["lotes_respaldo_exclusion"]` (marca `LOTES_RESPALDO_EXCLUSION_V2`) al final de `aplicar_exclusion_llaves_segmento`.
3. Tras contar registros, prod recupera los lotes anteriores (`obtener_lotes_desde_ruta_llaves`) **solo si la exclusión deja 0 filas**. Con 209 filas nuevas `lotes_segmento` queda en `[lote_actual]` → Silver 209 → el SCD apaga 1.203.789. El repo siempre fija respaldos + lote actual.
`scd.py` **no tiene el defecto**: hace lo que diseñó; la extracción le entregó una entrada incompleta.

## 3c. Cambios que se deben hacer (plan, nada desplegado)
**Objetivo del fix (usuario, 2026-11-05):** que este fallo no se repita en prod ni obligue a **eliminar un periodo** para recuperarlo; los tickets en prod son **multas para el equipo**, por eso debe ser resiliente. Es un fix **transitorio** mientras se corrige el pipeline completo. **La recuperación de 202606 ya la hizo el equipo** (el periodo está resuelto); lo que queda es solo el fix preventivo.
| Cambio | Archivo | Obligatorio | Nota |
|---|---|---|---|
| **Fix de la causa** | `extraccion_estandar_optimized.py` (dentro del job de Glue `niif-prd-extraccion-estandar-optimized`, no es lambda) | Sí | Reemplazar el script de prod por el del repo; es el **único** cambio, sin infra nueva (sin recursos, permisos ni cambio de configuración o de Step Function). Son 3 bloques que **dependen entre sí**: el 3 (fijar `lotes_segmento` tras excluir) llama a la función del 1 (`combinar_lotes_respaldados`) con el dato del 2 (`lotes_respaldo_exclusion`). Sin el 1 → error de nombre; sin el 2 → falla "Exclusion sin lotes de respaldo validados" en las 5 tablas con exclusión; 1 y 2 sin el 3 → no falla pero no arregla. Por eso se entrega el **archivo completo**, congelado y adjunto (con commit o hash), no "la versión del repo", porque el repo seguirá cambiando. Reversa: el script de prod guardado en `errores\`. Campo nuevo en `parametros_extraccion.json` (`lotes_respaldo_exclusion`): no probado en ejecución real, confirmar en QA. Confirmar antes si QA ya tiene esta versión. |
| Freno de apagado masivo | `niif-dev-scd\scd.py` + `tests\test_freno_apagado.py` (18 pruebas) | No (seguro) | **Defecto corregido 2026-11-06 (commit `c0682a5`, umbral opcional con defecto 20 %; antes:** `getResolvedOptions` exige `--UMBRAL_APAGADO_PCT` y la Step Function no lo pasa (solo `RUTA_CONFIG_SCD` y `RUTA_PARAMETROS_EXTRACCION`) → el SCD de todas las mallas fallaría al arrancar. Hacerlo opcional, umbral desde `umbral_apagado_pct` del JSON del caso, por defecto 20 % (propuesta); calibrar por malla con `metricas_gold_scd`. Probar en DEV con `replicar-fallo-dev.sh`. |
| Correo con totales vacíos | `notification_certification.py` / `niif_certification.py` | No (seguro) | **Decisión 2026-11-06: NO va en este release; se resuelve en la tarea de notificaciones (T1, [[tareas-aws]]).** Diseño en 3d. Original: contar filas reales sin el encabezado del CSV; si total y detalle están en 0, no enviar y avisar a soporte. Agregar `queries.summary` al JSON de prod (copiar de QA). |
| Alarma de fallo | Step Function / CloudWatch | No | **Trabajo futuro del monitoreo (nuestro, no se pide a otro equipo), no va en este release.** Si el freno salta, `GoldSCD` → `RegistrarFallo_GoldSCD` → `FailState` y no se avisa a nadie; solo comprobado en la definición de DEV. |

Orden del fix: (1) congelar el script corregido (commit o hash) y adjuntarlo; (2) release lo prueba en QA; (3) release lo sube a prod; (4) primera corrida posterior: `lotes_segmento` con varios lotes y SCD apagando ~0. Mientras no llegue a prod, una re-ejecución incremental puede repetir el fallo (considerar pausar la programación de Primas ARL).
Mensaje al equipo técnico/monitoreo (Teams): redactado en la conversación del 2026-11-05; solo describe el fix (3 cambios, sin infra, reversa, transitorio) y no menciona 202606 ni alertas.
Decisión de método: el fix mínimo va **antes** y separado de la migración a la estructura modular (ver [[estructura-repos-propuesta]]); la migración parte del script ya corregido con pruebas de caracterización que incluyan este caso (209 nuevas + ~1,2 M excluidas).
Dónde verlo en AWS: Glue → ETL jobs → `niif-prd-extraccion-estandar-optimized` → pestaña Script y Job details (Script path); el flujo se ve en Step Functions (`niif-prd-primas-arl`, definición y ejecuciones), no en Glue Workflows.

## 3d. Validación en DEV (2026-11-06) y diseño del ítem 3
**Release 1 = extracción + freno del SCD (el correo vacío queda para T1).** Commits en `origin/main`: `c0682a5` (umbral opcional), `f48f8b8` (función duplicada). El script de extracción en S3 DEV es igual en código al del repo (solo difiere en una línea en blanco PEP 8).
- **Ítem 1 (DEV, 2 corridas ARL):** base 2045 registros → Gold 2045 activos; incremental 54 nuevos, 2045 excluidos, `lotes_segmento` = 2 lotes (respaldo + actual) → Gold **2099** activos, `total_registros_apagados_por_ausencia` = 0. Antes del fix habría quedado en ~54.
- **Ítem 2 (DEV):** el SCD de DEV (script compartido `glue-resources/auxilios-funerarios/scd.py`, job `niif-dev-scd`, usado por todas las mallas) no tenía el freno y el Step Function **no pasa** `--UMBRAL_APAGADO_PCT`. Con el script nuevo: arranca sin el argumento, camino sano `SUCCESS` (2099 → 2099); con Silver parcial (100 de 2099) **frenó**: `SCD_APAGADO_MASIVO: se apagarian 1999 de 2099 (95.24% > umbral 20.0%). Gold no se modifico.` y Gold quedó intacto (14 objetos, 143842 bytes).
- **Método de la prueba del freno:** el rol `niif-dev-jobs-scd` solo lee rutas exactas (AccessDenied en `prueba-freno/` y en archivos nuevos de `primas-arl/`); se probó reemplazando temporalmente el parquet de Silver preparada por uno parcial (generado con Athena `UNLOAD` desde Gold, no `CTAS`: Lake Formation niega crear tablas en la base Silver) y se restauró con respaldo local. Cuidado: el `UNLOAD` falla con `HIVE_PATH_ALREADY_EXISTS` si se repite.
- **Ítem 3, hallazgos (código de Backend):** (a) el detalle nunca se marca `sinDatosPeriodo` porque la consulta de prod trae la fila de encabezado (`UNION ALL`); el encabezado coincide con el de `tableVars`, así que contar `líneas - 1` sirve (función `contar_filas_datos`, evaluar **fuera** del bucle de fragmentos para no marcar falso vacío); (b) con total en 0 filas `ejecutar_athena` devuelve `No hay datos` y el correo sale igual; (c) prod no tiene `dataPresence` y tiene `queries.summary = []`; (d) el código ya envía un aviso "No se encontraron datos procesados..." cuando detecta vacío (decisión previa del equipo: avisar, con botones); (e) si la entrada de Silver llega vacía el SCD marca `SKIPPED` y sigue, y la certificación corre sobre Gold vacío; (f) `ejecutar_athena` devuelve el texto `Error ejecutando consulta` y el correo sale igual.
- **Propuesta para T1:** parte B (marcar `sinDatosPeriodo` contando filas sin encabezado) + `queries.summary` en config; parte A (fallar con `SIN_DATOS_NO_ENVIAR` si total y detalle vacíos, salvo casos compuestos con `dataPresence`) **depende de la decisión del líder**: ¿se puede certificar un periodo en cero? Si sí, enviar aviso **sin** botones Certificar/No certificar. Sin flujo de soporte hoy, el fallo solo se vería en la Step Function (alarma = trabajo futuro). Probar también en otra malla: ambos jobs son compartidos.

## 4. Hallazgos secundarios
- **INC-6:** 1.100.009 registros (91,1 % de Gold) tienen `per_proceso_point` nulo → nunca salen en la certificación de ningún periodo. Causa sin analizar.
- Los 209 "nuevos" son de 202610 pese al `WHERE PERIODO <= '202606'`. Hipótesis: `PERIODO` (columna de `par_endosos`) ≠ `per_proceso_point` (deriva de `FEC_PROCESO_POINT`). Sin verificar.
- Ningún job ni la Step Function alerta cuando el correo sale con resumen o totales vacíos: el flujo envía igual.
- Las otras mallas usan los mismos jobs genéricos (extracción y SCD): riesgo similar, no revisado.

## 5. Qué NO está comprobado
- ~~Versión desplegada de la extracción en prod (INC-5)~~: confirmada, ver 3b. Sigue sin saberse la versión de QA.
- De qué lote(s) vienen los 1.203.789 excluidos (solo se ve la corrida previa del 5 oct; el campo de lotes de respaldo está vacío).
- ~~Si el bucket Gold de prod tiene versionado de S3~~: ya no aplica, la recuperación de 202606 la hizo el equipo.
- Si QA se comporta igual (el usuario dijo que allí el correo sale completo, con datos replicados).
- Los logs de texto de los jobs de Glue **no se capturaron** (vacíos, falló CloudShell con error de credenciales 500). Las cifras se sustentan en las métricas de `parametros_extraccion.json` y en el CSV de Athena.

## 6. Acciones propuestas (no ejecutadas)
1. No volver a correr el flujo de Primas ARL ni certificar el periodo hasta resolverlo.
2. ~~Ver versionado de Gold y restaurar~~: hecho por el equipo de prod.
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
