---
tipo: tarea
estado: vigente
actualizado: 2026-11-25
---
# Reunión con Víctor: componente de extracción (2026-11-20)

> Víctor explicó el modelo de extracción (ciclos, unidades de costo, conjuntos, formas, dependencias, pausa y checkpoint) y pidió medir tabla por tabla durante unas 2 semanas. Fuente: transcripción de Teams (no es manual).

Fuente: `Docs\05-arquitectura\Reuniones\Extraccion\ExplicacionExtraccion.txt` (los números son líneas), 2 imágenes y las consultas base `M1`-`M4`. Modelo y decisiones en [[modularizacion-extraccion]]. Antecedente: el bloque de optimización de la extracción en [[reunion-diseno-pcr]].

## Puntos
- **Una conexión de servicio** por origen: Analítica abrió dos y tumbó la base (L182-208). **El pool de peticiones sí lo manejamos nosotros.**
- **Pausa entre ciclos** de 1 a 7 minutos (Acsel-e: 3) para que la base cierre el result set y se reorganice; va en DynamoDB, por malla (L220-305). Promete una ganancia de al menos 40 % en volúmenes grandes; él mismo pide comprobarlo (L321-333): es *hipótesis por medir*.
- **Unidades de costo** = filas × columnas (L50-54); usar como máximo el 65 % (L394-406). El tope no es un dato: se aprende con la volumetría (L418-432).
- **Dependencias:** al cerrar un conjunto, se guardan en S3 las llaves del siguiente; se parten en bloques parejos, con checkpoint en DynamoDB para retomar (L442-569, L940-985). "¿3? ¿33?" (L946) = 3 dependencias de ~33 millones (100 millones ÷ 30 millones).
- **Primera corrida:** un ciclo, un conjunto, una tabla, en cascada (L515); después se agrupa (L606-616, L932).
- **Periodo:** se configura el periodo y el job calcula las fechas de inicio y fin (L684-704). El configurador ya lo hace con `${FECHA_INICIO}` y `${FECHA_FIN}`.
- **Remediación:** la copia de Bronce a Silver existe porque las reglas son activables (L731-805). Ver [[data-datalake-niif-rmd-ext-estandar-glue-job]].
- **Índices y estadísticas de Oracle:** solo como recomendaciones, a Víctor; él las lleva al arquitecto Yair y a la célula dueña (L100-134). El historial de la base (AWR/ASH) se pide por medio de Andrés (L909-916).
- **Lo que Víctor pidió ver** (L1101-1109): el resultado de Emisiones (unas 99 tablas) tabla por tabla y optimizado. No es el entregable: el entregable lo define el usuario (deuda técnica cerrada y extracción optimizada, ver [[modularizacion-extraccion]]).

## Flujo completo PCR (2026-11-25, `Victor Daniel Cortes Restrepo.txt` + `image copy 2.png` y `image copy 3.png`)
- **4 mallas en cadena, cada una independiente** (extracción → preparación → certificación) por reprocesos: **Emisiones → Primas Cedidas → Reaseguros → Primas PCR**. Ajustar una no recertifica las anteriores. Certifican: Operaciones (Emisiones y Cedidas), Reaseguros, Actuaría (PCR). Quién certifica aún "toca hablarlo".
- **Malla 5 (última):** extracción de **99 tablas** para Analítica, primeros de cada mes. Solo extracción y automatizarla; se arma al final sumando las ~20 tablas que falten.
- **Decisión "¿Existe ese lote?"**: si el periodo/lote ya está en Bronce, no se extrae; se retoma de Bronce y sigue la preparación. Julio prueba cada malla y esa decisión.
- **Meta de tiempo:** extracción de Emisiones en 2-3 h (3-4 h aceptable); 8 h no. Motivo: el cierre pasa de 17 a 7 días con NIIF 17. Lo que dijo de ARL, Auxilios y Reserva Matemática (de 3-4 días a horas) es el tiempo de verificación del cierre, **no** que sus consultas estén al tope: hubo trabajo en las consultas, pero hay espacio para optimizar (corrección del usuario, 2026-11-25). ALFAPRD sigue en el alcance.
- **Validado en código (`quality`, 3d5f623):**
  - Cadena real en las consultas: M2 usa `EM_*` de M1; M3 usa `EM_*` y `PC_*`; M4 usa `EMISIONES`, `PRIMAS_CEDIDAS` y `REASEGUROS`.
  - Tablas de origen `ACSELE_ALFA`: M1 78 (las 78 ya en PCR Vida 25, Generales 45, Satélites 59), M2 17 (faltan 8, las de reaseguro), M3 14 (faltan 7, entre ellas `EXT_RISKRESERVEALFA`), M4 4 (falta `PARTICIPACION`). Únicas: 94; extraídas 78; faltan 16.
  - Malla 1 existe de punta a punta (`niif-snf-emisiones`: configurador, extracción, val/rem/Silver x3, SCD, notificación, certificación). Mallas 2-4: no existen.
  - "¿Existe ese lote?" no está: el job reusa `_control/_SUCCESS.json` solo con el mismo periodo **y** lote, y `conf_emisiones` crea un lote nuevo (fecha y hora) en cada corrida salvo que llegue `loteCargueId` (`conf_emisiones.py:172-173`).
  - En DEV hubo corridas de Emisiones de hasta 8 h (ARQ-60).
  - Hallazgo: `silver_destination` cae a reemplazar `alfa-datalake-qa-bronce` por `qa-silver` (`extraccion_estandar_emisiones.py:578`): ambiente QA fijo como respaldo.
- **Contraste con el plan (2026-11-25):** cubiertos: unidades, una conexión, pausa en DynamoDB, 65 %, conjuntos y formas, una tabla por ciclo, índices como recomendación, resultado tabla por tabla. Faltan: mallas 2-5, "¿existe ese lote?", meta de 2-3 h, comprobar la regla de la pausa (≥40 % menos, L321-333), guardar en la configuración cuántas dependencias deja cada ciclo (L535) e historial del DBA vía Andrés (L914). Desvío: Víctor pide dependencias como lista de valores (`IN`, L487-493) y el plan eligió rangos (opción D): validarlo con él. Checkpoint: él dice DynamoDB (L563), no S3. Imagen `image.png`: el bloque "Dependencias → 40000" no está explicado (pregunta).
