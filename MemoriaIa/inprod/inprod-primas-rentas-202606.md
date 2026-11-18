---
tipo: inprod
estado: vigente
actualizado: 2026-11-13
---
# Incidente: Primas Rentas 202606 con 75.181 registros en Silver frente a 401 del SQL de composición

> La Silver preparada de Primas Rentas trae todas las rentas vigentes que cobraron en el mes (75.181) en vez de solo las expedidas en el periodo (401). Causa: al join con `prv_rentas` le falta el límite inferior del "Ajuste" de la V2.1 (línea 464 del job, línea 320 del SQL), y así está en prod. El fix de una línea no puede salir solo: requiere el SCD por periodo y un plan para el Gold actual de prod.

Relacionado: [[data-datalake-niif-primas-rentas-glue-job]], [[familia-mallas-alfaprd]], [[inprod-primas-arl-202606]] (SCD que apaga por ausencia y freno del 20 %), [[pendientes]], [[preguntas-abiertas]]. Tarea humana: `Tareas\T4-primas-rentas-composicion\`.

IDs locales de esta nota: `PR-1` a `PR-5`, sin ID `ARQ` asignado todavía.

## 1. Origen (reunión del 2026-11-13 con Victor Cortes y Julio Mesias)
- Victor descargó del SharePoint de arquitectura la "composición" de Primas Rentas y la ejecutó en **SIPREN NIIF**, la réplica Oracle de Sipren Tierra que solo usa el proyecto. Dio **401 filas** para 202606. Julio repitió la prueba y le dio lo mismo.
- En el sistema la malla reporta **75.181**. Lo esperado es de 400 a 600 por periodo.
- Encargo: revisar la **transformación (Silver preparada)**, no la extracción, y homologar el job con el SQL.
- SQL usado: `Tareas\T4-primas-rentas-composicion\AnalisisComposicionInformacion_PrimasRentas_V2.1.sql`. Es fuente de arquitectura y no se edita. El SharePoint guarda la documentación de arquitectura; la ingeniería va en los repos.
- Accesos: el usuario no tiene usuario en SIPREN NIIF. La credencial que tiene es de Acsel-E Soporte, que solo sirve para PCR. Hay que pedirla a Andrés. Mientras tanto, **Julio puede correr las consultas**.

## 2. Contexto de negocio (diagrama de Victor)
- NIIF 17 tiene dos áreas sponsor: **Actuaría**, que es con la que trabajamos y la dueña del data lake, y Contabilidad, que por ahora no.
- Casos de uso de Actuaría y su origen:

| Caso de uso | Preparaciones | Origen |
|---|---|---|
| Primas ARL | Primas ARL | Sipren Tierra (SIPREN NIIF) |
| Primas Rentas Vitalicias | **Primas Rentas** + Primas Endosos | Sipren Tierra |
| Auxilios Funerarios | Auxilio Funerario | Sipren Tierra |
| Reserva Matemática | RM ARP (riesgos laborales), RM RVI (rentas y previsionales) | Sipren Tierra |
| Primas de Riesgo en Curso (PCR) | Emitidas (vida, generales, satélites), Cedidas, Reaseguros, PCR Totalizadas, PCR Detalladas, Comisiones PCR | Acsel-E (Soporte). Las lleva Manuel |
| Primas Previsionales | Emitidas, 13 Y, Cargue Único, Parámetros | Excel (instrumento de negocio) |

- Sipren Plus es otro sistema distinto. "Sipren Tierra" es como lo llaman en la compañía.

## 3. Causa (comparación del SQL V2.1 con el job, 2026-11-13)
Job: `alfa-repos\glue\data-datalake-niif-primas-rentas-glue-job\src\jobs\silver-primas-rentas.py`. Es la traducción a Spark del SQL: las mismas 7 tablas, los mismos joins, el `max(nid_calculo)` por renta y el filtro `cod_concepto = 1`. Corresponde a una versión **anterior** a la V2.1.

- **PR-1 (causa, CONFIRMADA en QA el 2026-11-17).** El join con `prv_rentas` es distinto:
  - SQL V2.1, línea 320, marcado `-- Ajuste`: `prvrnts.fec_expedicion between fecha_inicio_periodo_pago_renta and fecha_fin_periodo_pago_renta`. Las dos fechas no vienen de Oracle: se calculan en las líneas 65 y 66 desde `periodo_pago` (el día 1 y el último día del mes). Para 202606 quedan 01/06/2026 y 30/06/2026, así que **solo pasan las rentas expedidas en junio: 401**.
  - Job, línea 464: `FECHA_FIN_PERIODO_PAGO_RENTA >= FEC_EXPEDICION`. **Le falta el límite inferior** (`>= fecha_inicio`) y deja pasar todas las rentas expedidas antes: **75.181**. El job ya calcula las dos fechas (líneas 431 a 446), pero no usa la de inicio.
  - **Por qué tantas (usuario, 2026-11-13):** una renta vigente paga una mesada cada mes, así que todas las vigentes aparecen en `anp_movimientos_pago` todos los meses. El job cuenta las vigentes que cobraron en junio (cerca de 75 mil, el universo de pensionados); el SQL cuenta las nuevas del mes, que traen la prima única (`VLR_PRIMA_UNICA`). No hay duplicados ni datos corruptos: el job mide otra cosa.
  - El filtro de pagos `fec_inicial` en junio (línea 34 del SQL, línea 430 del job) es igual en los dos y no explica la diferencia.
  - Prueba opcional en Oracle: la V2.1 con la línea 320 cambiada a `and prvrnts.fec_expedicion <= anpmvmntspg.fecha_fin_periodo_pago_renta` debería dar cerca de 75.181.
- **Evidencia en QA (2026-11-17, Athena, solo lectura, script `revisar-gold.sh` en CloudShell):** Gold `niif-qa-gold-catalog-db.scd_primas_rentas` con `act_reg = 1` tiene un solo periodo, **202606**, con **75.189 activas**, de las cuales **401** cumplen `per_expedicion_renta = per_pago_renta` (exacto al SQL de Victor) y **75.189 rentas distintas**: no hay duplicados. Victor reportó 75.181; la diferencia de 8 no está explicada (otra corrida o ambiente).
- **PR-2.** Falta la columna `RAMO`. El SQL saca `prvrnts.ramo_point as ramo` y el job no la tiene.
- **PR-3.** El job agrupa también por `LOTE_CARGUE_ID` (línea 506) y el SQL no. Si `lotes_segmento` junta los lotes de respaldo con el actual (el fix de INC-5), una renta podría salir repetida. **En QA no ocurre** (0 llaves duplicadas); se mantiene como riesgo y limpieza.
- **Coincide con el SQL:**
  - el filtro `FEC_INICIAL` del mes con el extremo 23:59:59;
  - la condición sobre saldos `fin_periodo >= fec_corte_calculo`;
  - los inner joins;
  - `VAL_REG_INVALIDO_NULL` y `VAL_REG_INVALIDO`.

## 4. Alcance
- **Afecta solo a Primas Rentas**, que es parte del caso Rentas Vitalicias. Cada malla tiene su propio job Silver.
- Primas Endosos filtra `e.FEC_EXPEDICION` dentro del periodo (`silver_primas_endosos.py`, línea 358), así que no tiene el problema.
- Reserva Matemática RVI usa `fin_periodo >= r.FEC_EXPEDICION` (`silver_reserva_matematica_rvi.py`, línea 349). Probablemente está bien, porque una reserva es un saldo que necesita el acumulado. No se ha comparado con su SQL de composición.
- Lo que arrastra hacia adelante: Gold queda con 75.181 activas, la certificación muestra totales inflados y, si se certifica, el dato inflado llega a FIS.
- **Confirmado en prod (2026-11-13).** El script de prod (`s3://niif-prd-assets/glue-resources/primas-rentas/silver-primas-rentas.py`) es **idéntico** al del repo (commit `853c725`): 658 líneas y solo cambia el fin de línea. Copia guardada en `Tareas\T4-primas-rentas-composicion\silver-primas-rentas-prod.py`. PR-1, PR-2 y PR-3 están en prod. DEV y QA no se han comparado. No se revisan los periodos certificados ni los conteos en Athena: la simulación de Victor y el código bastan (decisión del usuario).
- La llave del SCD en el repo (`cfg-datalake-niif-config-s3`, `primas-rentas/config_scd_primas_rentas.json`) es solo `NID_RENTA`, sin periodo, con `LOTE_CARGUE_ID` como columna fija. PR-4 aplica si se cambia la Silver. Falta confirmar que el JSON desplegado sea igual.

## 5. Por qué la línea sola no basta (PR-4 y PR-5, deducido del código y sin probar)
| # | Problema | Qué pasaría si solo se corrige la línea 464 |
|---|---|---|
| PR-5 | **El Gold actual** tiene cerca de 75 mil rentas activas. QA: 75.189 activas, todas de 202606 | En la primera corrida llegan 401 y el SCD apagaría **74.788 (99,5 %)** en QA. Prod: por medir con la misma consulta. El freno del 20 % detiene la malla; sin freno se borra lo activo, como en ARL |
| PR-4 | **La llave del SCD es solo `NID_RENTA`, sin periodo.** El SCD hace full join y apaga por ausencia ([[inprod-primas-arl-202606]], INC-2) | En julio llegan unas 500 nuevas y el SCD apaga las 401 de junio. El freno detiene la malla **todos los meses** |
| PR-3 | `groupBy` por `LOTE_CARGUE_ID` | Una renta podría salir dos veces si los pagos vienen en varios lotes |
| PR-2 | Falta `RAMO` | No cambia el conteo. Agregarla obliga a cambiar el JSON del SCD y el esquema de Gold, así que puede ir aparte |

- Hoy no se nota porque cada mes llegan casi las mismas 75 mil rentas vigentes y el SCD apaga muy pocas.
- Es el mismo hallazgo de "SCD por periodo" que salió en QA con Auxilios Funerarios y ARL ([[pendientes]]).
- **Conclusión:** la línea arregla el número, pero si sale sola a prod rompe la malla en la primera corrida. Tiene que ir junto con el SCD por periodo y con un plan para el Gold actual.

## 6. Fix (propuesta, nada ejecutado)
**Cambio en el job** (`silver-primas-rentas.py`), igual a la línea 320 del SQL:
```python
# línea 464, hoy
(col("mp.FECHA_FIN_PERIODO_PAGO_RENTA") >= col("r.FEC_EXPEDICION")),
# corregido
(col("r.FEC_EXPEDICION").between(
    col("mp.FECHA_INICIO_PERIODO_PAGO_RENTA"),
    col("mp.FECHA_FIN_PERIODO_PAGO_RENTA"))),
```
Además, quitar `col("mp.LOTE_CARGUE_ID")` del `groupBy` (línea 506), porque el SQL no agrupa por lote. Hay que revisar qué pasa con la columna fija `LOTE_CARGUE_ID` del SCD.

**Orden:**
1. ~~Confirmar PR-1~~: hecho. Victor simuló en Oracle (401), el código de prod es igual al del repo y la explicación de las rentas vigentes lo cuadra.
2. ~~Preguntar a negocio solo periodo o acumulado~~: **resuelto**. Victor dijo 401, de 400 a 600 por periodo: solo las rentas expedidas en el periodo.
3. ~~Revisar la llave del SCD~~: es `NID_RENTA` sin periodo, así que PR-4 aplica.
4. Ajustar el job: la línea 464 (PR-1) y el `groupBy` (PR-3). **Hecho (2026-11-18, commit `172b17c`, sin push; prueba en DEV con `Tareas\Fixs\primasRentas\probar-silver-rentas-dev.sh` y su guía `pruebas-dev-silver-rentas.md`. En DEV y QA todo se despliega como en PRD: código por PR a la rama del ambiente y Jenkins, infraestructura por IaC; el script de pruebas no sube código, solo comprueba el despliegue, respalda datos, corre Silver, SCD (`PERMITIR=true` si frena) y crawler `niif-dev-crawler-gold`, y verifica en Athena):** en `alfa-repos`, rama `fix/primas-rentas-periodo-expedicion` (desde `main`, sin ticket por decisión del usuario), cambio en `silver-primas-rentas.py` (`between` y `max(LOTE_CARGUE_ID)`) y `tests/` nuevo (5 pruebas con Glue, S3 y parquet simulados; PySpark 3.5.5 con uv). Con el fix pasan 5 de 5; con el job original de `main` fallan 4 de 5 (renta antigua, renta del mes anterior y renta duplicada por lote). El `tox.ini` falla con el `coverage` actual por el patrón `src/**.py`; es la plantilla de DevOps (igual en 10 repos de Glue), no se toca; se corre con `-o addopts=""`. README del repo reescrito (antes era el de Endosos), con el de `rmd-ext-estandar` como guía. El SQL Oracle compara contra el último día a las 00:00, así que una expedición del último día con hora queda fuera; el job hace lo mismo. **Alcance (2026-11-18, decisión del usuario): el fix solo corrige la cifra; no agrega ni quita columnas.** `RAMO` (PR-2) queda fuera; si se pide, sería otro cambio (job, `config_scd_primas_rentas.json` y Gold). El resto del job coincide con el SQL (filtro `FEC_INICIAL`, cruces, saldos `>=`, `max` de cálculo, concepto 1, `VAL_REG_*`).
5. **SCD por periodo (PR-4)**: que compare y apague solo dentro del mismo periodo. Es el pendiente compartido con Auxilios Funerarios y ARL.
6. **Plan para el Gold actual de prod (PR-5)**: decidir qué pasa con las ~75 mil activas. Se pueden cerrar de forma controlada, o dejar que el SCD por periodo las ignore si pertenecen a otros periodos.
7. Pruebas de caracterización (el job no tiene, ARQ-47) con el caso 401 / 75.181: renta expedida antes del periodo con pago en el mes (no pasa) y renta expedida en el mes (pasa).
8. Release: los pasos 4, 5 y 6 salen juntos a prod, nunca solo el 4.

**Verificación opcional en Athena** (solo lectura, la Silver preparada guarda solo la última corrida; si ya corrió otro periodo, usar Gold `scd_primas_rentas` con `act_reg = 1` y `per_pago_renta = '202606'`):
```sql
SELECT per_pago_renta, COUNT(*) AS total_job,
       COUNT_IF(per_expedicion_renta = per_pago_renta) AS expedidas_en_el_periodo,
       COUNT(DISTINCT nid_renta) AS rentas_distintas
FROM <base_silver>.<tabla_silver_preparada_primas_rentas>
GROUP BY per_pago_renta;
```
Lectura: `expedidas_en_el_periodo` cerca de 401 confirma PR-1; `rentas_distintas` menor que `total_job` indica PR-3.

**Mensaje a Victor y Julio (Teams, redactado, sin confirmar envío):** el SQL V2.1 cuenta solo las rentas expedidas en junio, mientras el job cuenta todas las que recibieron pago en junio (todas las vigentes); se pide confirmar que lo correcto es contar solo las expedidas en el periodo.

## 7. Regla de Gold: un solo periodo activo (2026-11-17, decisión del usuario)
- En Gold solo queda activo el último periodo cargado; los anteriores pasan a histórico (`ACT_REG = 0`). Ver [[fix-scd-freno-periodo]].
- PR-4 se replantea: que julio apague las 401 de junio es lo **esperado**. Lo que falla es el freno, que hay que medir solo sobre el mismo periodo.
- PR-5: re-ejecutar 202606 con el job corregido apaga 74.788 del **mismo** periodo (99,5 %). El freno saltaría con razón, así que hace falta una excepción controlada de una sola vez: argumento `--PERMITIR_APAGADO_MASIVO true` del job SCD solo en esa corrida (ya existe en `scd.py`).
- Verificado en código (2026-11-18): Rentas usa el SCD genérico `niif-{env}-scd` (`glue-resources/estandar/scd.py`, repo [[data-datalake-niif-rmd-ext-estandar-glue-job]]). La rama `bugfix/scd-freno-periodo` (commit `2337531`) ya cierra el periodo anterior sin freno usando `periodo_negocio` y `periodo_activo.json` en Gold; no requiere `columna_periodo` ni cambiar `config_scd_primas_rentas.json`. Lo único propio de Rentas es el job Silver (línea 464 y `groupBy`) y sus pruebas.

## Hallazgo lateral: Primas Endosos (fuera de este fix)
- En QA, `scd_primas_endosos` tiene **1 llave duplicada** entre los vigentes (llave `nid_renta`) y 0 llaves vacías. Una renta puede tener varios endosos, así que `nid_renta` sola podría no ser la llave correcta. Por revisar: el JSON del SCD de endosos y qué renta está duplicada. Las otras 5 tablas de Gold de QA tienen 0 duplicados y 0 llaves vacías.

## Pendiente de la reunión
- Sesión en la tarde con Julio para revisar el tema de notificación.
