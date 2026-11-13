---
tipo: inprod
estado: vigente
actualizado: 2026-11-13
---
# Incidente: Primas Rentas 202606 con 75.181 registros en Silver frente a 401 del SQL de composición

> La Silver preparada de Primas Rentas trae el acumulado de rentas con pago en el mes (75.181) en vez de solo las expedidas en el periodo (401). Causa probable: falta el "Ajuste" de la V2.1 en el join con `prv_rentas`. El arreglo en Silver pide también un SCD por periodo.

Relacionado: [[data-datalake-niif-primas-rentas-glue-job]], [[familia-mallas-alfaprd]], [[inprod-primas-arl-202606]] (SCD que apaga por ausencia y freno del 20 %), [[pendientes]], [[preguntas-abiertas]]. Tarea humana: `Tareas\T4-primas-rentas-composicion\`.

IDs locales de esta nota: `PR-1` a `PR-4`, sin ID `ARQ` asignado todavía.

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

- **PR-1 (causa probable, hipótesis fuerte).** El join con `prv_rentas` es distinto:
  - SQL V2.1, línea 320, marcado `-- Ajuste`: `fec_expedicion BETWEEN inicio_periodo AND fin_periodo`. Trae solo las rentas expedidas en el mes, que son 401.
  - Job, línea 464: `FECHA_FIN_PERIODO_PAGO_RENTA >= FEC_EXPEDICION`. Trae todas las rentas expedidas hasta fin de mes que cobraron en junio, que son 75.181.
  - Con esto no hay duplicados: el job lleva el universo completo de pensionados.
- **PR-2.** Falta la columna `RAMO`. El SQL saca `prvrnts.ramo_point as ramo` y el job no la tiene.
- **PR-3.** El job agrupa también por `LOTE_CARGUE_ID` (línea 506) y el SQL no. Si `lotes_segmento` junta los lotes de respaldo con el actual (el fix de INC-5), una renta podría salir repetida.
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
- No se sabe si esta versión del job está en prod. Si lo está, hay que revisar qué periodos de Primas Rentas se certificaron con estos totales.

## 5. Problema que aparece con el arreglo (PR-4, deducido del código y sin probar)
- Si Silver lleva solo las rentas del mes, el periodo siguiente traería unas 500 y el SCD las compararía contra las 401 de junio que están activas. El SCD hace full join y apaga por ausencia ([[inprod-primas-arl-202606]], INC-2), así que apagaría las de junio.
- El freno del 20 % lo detendría, porque apagaría el 100 % de lo activo. La malla fallaría todos los meses.
- Es el mismo hallazgo de "SCD por periodo" que salió en QA con Auxilios Funerarios ([[pendientes]]).
- Hoy no se nota porque, con el acumulado en Silver, el SCD casi no apaga nada.
- Conclusión: el arreglo de la Silver **no basta solo**. Primas Rentas necesita también el SCD por periodo, salvo que la llave del SCD ya incluya el periodo (por revisar en su JSON).

## 6. Plan (propuesta, nada ejecutado)
1. **Confirmar PR-1 sin tocar código.** Julio corre la V2.1 cambiando la línea 320 a `and prvrnts.fec_expedicion <= anpmvmntspg.fecha_fin_periodo_pago_renta`:
   - si da cerca de 75.181, PR-1 queda confirmada;
   - si no, revisar también PR-3.
2. **Confirmar con Victor o el arquitecto** si Gold de Primas Rentas debe tener solo las rentas nuevas de cada mes o el acumulado.
3. **Revisar la llave del SCD** en el JSON de configuración de Primas Rentas.
4. Con eso resuelto, ajustar el job:
   - cambiar el join (PR-1);
   - agregar `RAMO` (PR-2);
   - sacar `LOTE_CARGUE_ID` del `groupBy` o justificarlo (PR-3).
   Además, coordinar el SCD por periodo (PR-4).
5. Como el job no tiene pruebas (ARQ-47), agregar pruebas de caracterización con el caso 401 / 75.181.

## Pendiente de la reunión
- Sesión en la tarde con Julio para revisar el tema de notificación.
