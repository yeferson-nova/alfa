---
tipo: tarea
estado: vigente
actualizado: 2026-11-20
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
