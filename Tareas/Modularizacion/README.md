# Modularización de componentes

Tarea transversal, abierta el 2026-11-20. Reúne el trabajo de pasar los jobs genéricos del medallón a código modular (arquitectura hexagonal). En el mismo cambio se cierran los temas técnicos abiertos de cada componente y se deja la observabilidad lista.

## Objetivo
Cada componente que se modulariza sale con tres cosas, sin dejar ninguna para después:

1. **Código modular:** dominio puro, adaptadores de AWS y Oracle, y una raíz de composición, según el estándar `Docs\08-estandares-desarrollo\estandar-lambdas-python.md`. Una red de pruebas de caracterización asegura que los resultados no cambian.
2. **Deuda técnica cerrada:** las brechas del componente (IDs `ARQ`, `COS`, `SEG`) se corrigen junto con su configuración: el JSON de `cfg-datalake-niif-config-s3`, el registro de DynamoDB `cnf-mallas` y lo que publica la lambda configuradora. Un arreglo que solo toca código no se da por cerrado.
3. **Observabilidad:** contrato de log v1, `ejecucion_id` de punta a punta, duración real de cada paso y conteos (`Docs\06-brechas\roadmap-observabilidad.md`, fases 2a y 2c).

## Reglas
- **Medir antes de cambiar.** Toda optimización se justifica con una medición en DEV, antes y después.
- **Los resultados no cambian.** Cada cambio pasa una comparación de equivalencia: conteo, suma de control de montos y diferencia exacta igual a 0.
- **No suponer.** Lo que no está comprobado en código, en DEV o en un documento se marca como *hipótesis* o *propuesta*.
- **DevOps despliega.** Nosotros entregamos el código y la configuración en un zip con el nombre del recurso.

## Componentes
Repo base: `alfa-repos\glue\data-datalake-niif-rmd-ext-estandar-glue-job\` (jobs genéricos que usan las 6 mallas ALFAPRD). Rama de trabajo: `feature/modular-rmd-ext-estandar` (por ahora solo la red de caracterización y las carpetas vacías).

| Componente | Carpeta | Estado |
|---|---|---|
| Extracción (Oracle a Bronce; estándar y Emisiones) | [`Extraccion\`](Extraccion/README.md) | En diseño: modelo de Víctor validado contra el código y medido en DEV |
| Validación técnica | (por crear) | Sin empezar |
| Remediación (Bronce a Silver base) | (por crear) | Sin empezar. Diseño ya conocido: reglas activables por configuración (ver la ficha del repo en la memoria) |
| SCD (Silver a Gold) | (por crear) | Corregido sin modular en `bugfix/scd-freno-periodo` (`Tareas\Fixs\primasARL\`) |

Cada componente nuevo lleva su carpeta aquí, con un `README.md` (problema, estado verificado, brechas, plan) y sus diseños.

## Orden propuesto
1. **Extracción.** Es la etapa que más tiempo y costo consume, la que más carga la fuente Oracle y la que tiene un lineamiento nuevo de Víctor (2026-11-20).
2. **Remediación y validación técnica.** Comparten el contrato `parametros_extraccion.json`.
3. **SCD.** Ya está corregido; se modulariza al final, sobre la red de pruebas que ya existe.
