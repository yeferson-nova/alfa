---
tipo: feature
estado: vigente
actualizado: 2026-11-20
---
# Modularización de componentes (tarea transversal)

> Tarea abierta el 2026-11-20: pasar los jobs genéricos del medallón a código hexagonal, cerrando en el mismo cambio su deuda técnica (con su configuración) y su observabilidad. Abierta a más componentes.

Humano: `Tareas\Modularizacion\README.md`.

## Qué es la tarea
- Cada componente sale con tres cosas: código modular con red de caracterización, brechas cerradas junto con su configuración (JSON, `cnf-mallas` y configurador, regla 12 de [[preferencias-usuario]]) y observabilidad (contrato de log v1 y `ejecucion_id`, fases 2a y 2c de [[roadmap-observabilidad]]).
- Reglas: medir antes de cambiar; los resultados no cambian (prueba de equivalencia); nada supuesto.
- Repo base: [[data-datalake-niif-rmd-ext-estandar-glue-job]], rama `feature/modular-rmd-ext-estandar` (solo la red de caracterización y las carpetas; le faltan los 6 commits de `bugfix/scd-freno-periodo`, hay que rebasarla).

## Componentes
| Componente | Nota | Estado |
|---|---|---|
| Extracción (estándar y Emisiones) | [[modularizacion-extraccion]] | En diseño (opción D) |
| Validación técnica | (por crear) | Sin empezar |
| Remediación | [[data-datalake-niif-rmd-ext-estandar-glue-job]] (reglas activables) | Sin empezar |
| SCD | [[fix-scd-freno-periodo]] | Corregido sin modular |

Componente nuevo: crear `Tareas\Modularizacion\<Componente>\` y una nota `modularizacion-<componente>` enlazada aquí.

Pendientes: [[pendientes]]. Brechas: [[auditoria-extraccion-2026-11]].
