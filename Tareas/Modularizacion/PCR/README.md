# PCR: optimización, modularización y observabilidad

Abierta el 2026-11-25. Forma parte de [`Tareas\Modularizacion\`](../README.md).

## Qué es PCR
PCR es un proceso de **4 mallas en cadena**. Cada una extrae, prepara y se certifica antes de que corra la siguiente (reunión con Víctor del 2026-11-25, `Docs\05-arquitectura\Reuniones\Extraccion\`).

| # | Malla | Qué produce | Certifica | Consulta base | Estado |
|---|---|---|---|---|---|
| 1 | [Emisiones](01-emisiones/README.md) | Primas emitidas (PCR Vida, Generales y Satélites) | Operaciones | `M1_EMISIONES.sql` | **Existe y corre.** Es la que se optimiza primero |
| 2 | [Primas Cedidas](02-primas-cedidas/README.md) | Primas cedidas a reaseguradores | Operaciones | `M2_PRIMAS_CEDIDAS.sql` | En construcción (línea base de Manuel) |
| 3 | [Reaseguros](03-reaseguros/README.md) | Primas de reaseguros | Área de Reaseguros | `M3_REASEGUROS.sql` | En construcción (línea base de Manuel) |
| 4 | [Primas PCR](04-primas-pcr/README.md) | `pcr_detalle`, `pcr_totales` y `pcr_comisiones` | Actuaría | `M4_PRIMAS_PCR.sql` | En construcción (línea base de Manuel) |

La malla de las 99 tablas para Analítica (primer día de cada mes) no es parte de PCR.

**Nuestro trabajo es optimizar lo que ya existe.** Manuel construye la versión base de cada malla. Cuando una malla queda construida, entra a este plan con su propia carpeta.

## Objetivos (se aplican a cada malla)
1. **Velocidad de extracción: 1,5 horas como máximo.** La meta de Víctor es de 2 a 3 horas para Emisiones (3 a 4 horas es aceptable); la nuestra es más exigente. Hoy, la extracción completa de Emisiones tarda de 3 a 7 horas en DEV.
2. **Código modular y limpio:** arquitectura hexagonal, PEP 8 y código limpio, según `Docs\08-estandares-desarrollo\estandar-lambdas-python.md`. La deuda técnica se cierra junto con su configuración (JSON de `cfg-datalake-niif-config-s3`, `cnf-mallas` y la lambda configuradora).
3. **Observabilidad en 3 niveles** (`Docs\06-brechas\roadmap-observabilidad.md`):
   - **Logs:** JSON con el contrato de log v1 y el `ejecucion_id` de punta a punta.
   - **Métricas:** duración real y conteos de cada paso, como métricas con nombres estables para conectar tableros después.
   - **Trazas:** X-Ray en lambdas, Step Functions y API. En Glue, que no tiene X-Ray, la traza se arma con los logs (`ejecucion_id` y los tramos `PASO_INICIO`/`PASO_FIN`).

## Reglas
- **Oracle no se toca.** Se optimiza lo que pasa después: el pool de peticiones, la concurrencia y los recursos del job de Glue. Las recomendaciones de índices solo se entregan a Víctor.
- **Una sola conexión de servicio.** El DBA no da más conexiones. Lo que manejamos es el pool de peticiones, con pausas entre ciclos.
- **Medir antes de cambiar,** con corridas que ya existen siempre que se pueda.
- **Los resultados no cambian.** Cada cambio se compara contra la versión anterior: conteo, suma de control de montos y diferencia exacta igual a 0.
- **No suponer.** Lo que no está comprobado en código, en DEV o en las reuniones se marca como *hipótesis* o *propuesta*.

## Cómo se organizan las carpetas
```
PCR\
├── README.md                  este documento: el proceso completo
└── NN-<malla>\
    ├── README.md              alcance, línea base, hallazgos y plan de la malla
    └── validaciones\
        ├── scripts\           scripts y consultas de validación (solo lectura)
        └── salidas\           lo que devolvió cada script, con el mismo nombre del script y la fecha
```
Las salidas no deben llevar datos personales, IDs de cuenta, IP ni claves.
