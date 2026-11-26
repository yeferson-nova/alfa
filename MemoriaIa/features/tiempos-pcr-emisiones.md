---
tipo: feature
estado: vigente
actualizado: 2026-11-25
---
# Registro de tiempos: extracción de PCR Emisiones

> Tiempos medidos por paso (job, caso y tabla) de la línea base y de cada cambio, para el gráfico de la entrega. Meta: 90 min como máximo ([[proceso-pcr]]).

## Cómo se registra (cada cambio)
1. En CloudShell de DEV: `medir-tiempos-dev.sh` (y `medir-uso-glue-dev.sh` si cambia el uso del clúster). Lee corridas existentes; no lanza el job.
2. Guardar la salida en `Tareas\Modularizacion\PCR\01-emisiones\validaciones\salidas\`.
3. En el equipo: `python consolidar-tiempos.py <salida.txt> --version "<versión>" --sesiones "<sesiones Oracle>"` (en `validaciones\scripts\`). Agrega o reemplaza las filas de esa medición en `Tareas\Modularizacion\PCR\01-emisiones\tiempos\registro-tiempos.csv` (niveles `job`, `caso` y `tabla`). Ese CSV es la fuente del gráfico.
4. Agregar una fila a la tabla de abajo, con la versión y el cambio.

## Historial (DEV, mismo periodo 202606)
| Fecha | Versión / cambio | Sesiones Oracle | Lote | Job (min) | Núcleo + PCR Vida | Generales | Satélites | DPU-h |
|---|---|---|---|---|---|---|---|---|
| 2026-11-24 | QA `2086a5a0`, **línea base** | hasta 3 + 4 particiones | 20261124060016 | 321 | 119,5 | 66,8 | 135,3 | 107,1 |
| 2026-11-20 | QA `2086a5a0`, línea base | igual | 20261120213537 | 308 | — | — | — | 102,8 |
| 2026-11-19 | QA `2086a5a0`, línea base | igual | 20261119013542 | 282 | — | — | — | 94,0 |
| 2026-11-18 | versión anterior a la QA vigente (*por confirmar*) | — | 20261118054447 | 427 | — | — | — | 142,3 |

Los minutos por caso son de reloj (los casos van en serie); solo la corrida del 24 nov tiene la hora de fin de cada caso. Suma del tiempo de tablas en el lote del 24 nov: núcleo 29,8, PCR Vida 145,8, Generales 65,9 y Satélites 133,2 min (PCR Vida corre fases en paralelo).

## Tablas más lentas de la línea base (lote 20261124060016)
`INSURANCEOBJECTDCO` 22 min y `OAVDEUDORES` 21 min (3,7 M filas), `PREAGREEMENT` de Satélites 19 min (154 filas), `THIRDPARTYMOVEMENTPOLICY` 18 min (36,7 M), `STPO_POLICYPARTICIPATIONDCO` 14 min (7,3 M). Detalle y causas en [[modularizacion-extraccion]].
