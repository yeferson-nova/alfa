---
tipo: feature
estado: vigente
actualizado: 2026-11-25
---
# Proceso PCR: optimización, modularización y observabilidad

> Plan global de PCR (4 mallas en cadena). Objetivos por malla: extracción en 1,5 h como máximo, código modular y limpio, y observabilidad en 3 niveles. Empieza por la malla 1 (Emisiones). Humano: `Tareas\Modularizacion\PCR\README.md`.

## Qué es PCR (Víctor, 25 nov; [[reunion-extraccion-victor]])
- 4 mallas en cadena; cada una extrae, prepara y se certifica antes de la siguiente:
  1. **Emisiones** (primas emitidas; certifica Operaciones). **Existe:** `niif-snf-emisiones`, repo [[data-datalake-niif-pcr-emisiones-glue-job]], casos PCR Vida, Generales y Satélites.
  2. **Primas Cedidas** (certifica Operaciones), `M2`.
  3. **Reaseguros** (certifica el área de Reaseguros), `M3`.
  4. **Primas PCR** (certifica Actuaría), `M4`. Casi no lee Oracle: reutiliza lo de las mallas 1 a 3.
- La malla de 99 tablas para Analítica no es PCR. "PCR Vida" es un caso de la malla 1, no Primas PCR.
- **Alcance:** optimizar lo que existe. Manuel construye la versión base de cada malla; una malla entra al plan cuando está construida.

## Objetivos (decisión del usuario, 2026-11-25)
1. **Extracción en 1,5 h como máximo por malla.** Víctor pide de 2 a 3 h (3 a 4 h aceptable) para Emisiones. Línea base de Emisiones en DEV: 172 a 427 min ([[modularizacion-extraccion]]).
2. **Modular y limpio:** hexagonal, PEP 8, código limpio ([[est-desarrollo-hexagonal]]); la deuda técnica se cierra junto con su configuración ([[preferencias-usuario]]).
3. **Observabilidad en 3 niveles = logs, métricas y trazas** ([[roadmap-observabilidad]]).

## Reglas
- Oracle no se toca (sin índices ni DDL): se optimiza el lado Glue (pool de peticiones, pausa, concurrencia y recursos). Una sola conexión de servicio.
- Medir antes de cambiar, con corridas existentes. Resultados iguales (diferencia 0). No suponer.

## Carpetas (decisión del usuario, 2026-11-25)
- `Tareas\Modularizacion\PCR\README.md` (global) y una carpeta por malla: `01-emisiones`, `02-primas-cedidas`, `03-reaseguros`, `04-primas-pcr`.
- Dentro de cada malla: `README.md` y `validaciones\scripts\` (scripts y SQL) y `validaciones\salidas\` (resultados, con el nombre del script y la fecha). Las salidas de PCR **sí van en Git**, sin datos personales, IDs de cuenta, IP ni claves.
- ALFAPRD sigue en `Tareas\Modularizacion\Extraccion\` (`alfaprd\validaciones\scripts\`).

## Estado
- Malla 1: línea base medida; faltan tiempo por tabla, uso del clúster y la respuesta de Víctor sobre sesiones simultáneas ([[preguntas-abiertas]]). Detalle técnico en [[modularizacion-extraccion]].
- Mallas 2 a 4: pendientes (Manuel).
