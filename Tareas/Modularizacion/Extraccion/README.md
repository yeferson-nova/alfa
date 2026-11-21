# Modularización de la extracción (Oracle a Bronce)

Abierta el 2026-11-20. Forma parte de [`Tareas\Modularizacion\`](../README.md).

## Entregable
Cerrar la deuda técnica y optimizar la extracción que ya existe, iterándola: código Python con estándares de industria (hexagonal, PEP 8), observabilidad y menos carga en Oracle, con los mismos resultados. Cubre **los dos procesos de extracción**: el estándar (6 mallas ALFAPRD) y `extraccion-estandar-emisiones` (Acsel-e: Emisiones, PCR Vida, Generales y Satélites). Los dos son jobs de Glue de los pasos 2 y 3, nuestro alcance.

## Problema
La extracción lee las fuentes Oracle (ALFAPRD y Acsel-e) directamente, **sin ODS ni bases de lectura, y eso no va a cambiar**. Hay **una sola conexión de servicio** por origen. El pool de peticiones (cuántas lecturas, de qué tamaño y cuándo reposar) es responsabilidad del job. Hoy el job no administra esa carga: lee tablas completas una tras otra, sin pausa, y el tiempo varía mucho entre corridas.

## Fuentes
- Reunión con Víctor (2026-11-20): `Docs\05-arquitectura\Reuniones\Extraccion\ExplicacionExtraccion.txt` (transcripción de Teams, no es manual) y 2 imágenes.
- Consultas base de PCR: `M1_EMISIONES.sql`, `M2_PRIMAS_CEDIDAS.sql`, `M3_REASEGUROS.sql` y `M4_PRIMAS_PCR.sql`, en la misma carpeta. **Son la referencia de resultados:** se pueden afinar en rendimiento, pero sus resultados no pueden cambiar.
- Código: `data-datalake-niif-rmd-ext-estandar-glue-job`, `src\jobs\extraccion_estandar_optimized.py` (estándar, 6 mallas ALFAPRD), y `data-datalake-niif-pcr-emisiones-glue-job`, `src\jobs\extraccion_estandar_emisiones.py` (Acsel-e).

## Modelo de extracción (lineamiento de Víctor, 2026-11-20)
| Concepto | Qué es | Transcripción |
|---|---|---|
| Ciclo | Esfuerzo acondicionado a la infraestructura de la fuente | L44-46 |
| Unidades de costo | Filas × columnas que se van a extraer | L50-54 |
| Tope del ciclo | Usar como máximo el 65 % de la capacidad observada. **No hay un número fijo:** se aprende midiendo | L394-406, L418-432 |
| Pausa entre ciclos | Cerrar la conexión y esperar de 1 a 7 minutos (Acsel-e: empezar con 3). Se configura en DynamoDB, por malla | L220-228, L293, L303-305 |
| Conjunto | Grupo de tablas dentro de un ciclo. Los conjuntos corren en cascada | L352-370 |
| Forma | Cascada, paralela o segmentada | L364-368 |
| Dependencias | Al terminar un conjunto, se guardan en S3 las llaves que necesita el siguiente, que las usa como lista de valores | L442-452, L487-493 |
| Dependencia segmentada | Partida en bloques parejos, con checkpoint en DynamoDB para retomar | L503-511, L556-569, L958-985 |
| Aprendizaje | Primera corrida: una tabla por ciclo. La configuración guarda cuántas filas y dependencias deja cada ciclo | L515, L535, L606-616 |
| Índices en Oracle | No los cambiamos: se entregan como recomendación a Víctor, que los lleva al arquitecto Yair y a la célula dueña | L100-122 |

## Estado verificado (código y DEV, 2026-11-20)
- El script desplegado en DEV (`niif-dev-extraccion-estandar-optimized`) es idéntico al del repo.
- **Sin pausa entre ciclos.** Solo ejecuta en cascada: las formas paralela y segmentada solo dejan un aviso en el log.
- **Las dependencias del estándar son solo metadata.** Las tablas hijas se leen completas, por ejemplo `acu_personas_naturales` (1.210.373 filas) cuando solo se necesitan las de 7.002 afiliaciones. El pivote de Rentas no tiene filtro (22,6 millones de filas en cada corrida).
- **Unidades de costo:** se calculan después de leer y por tabla. El tope de 1.000.000 está en `ADVERTIR` y lo superan 8 tablas.
- **La duración de la lectura está mal medida:** Spark no lee hasta el conteo (9 s de lectura frente a 583 s de conteo en `anp_movimientos_pago`). Además, la duración total no incluye la reconciliación final (*hipótesis*: 214 s en el JSON frente a 746 s del job).
- **Configurador:** `Ciclos esperados=N` y `CNF esperada=8` están fijos, así que no deja agregar ciclos. La configuradora general fija `MALLA_PK` por malla antes de importar el módulo, así que ahí no aplica el valor por defecto `MALLA#PRIMAS_ARL` del módulo. Ese defecto, y el ambiente que cae a `dev`, se quitan igual.
- **Línea base en DEV (ALFAPRD):** entre 0,6 y 1,1 millones de unidades por segundo con una conexión. Con la misma configuración, ARL tardó entre 266 y 1172 s y Rentas entre 1115 y 2716 s.
- Brechas: ARQ-57, ARQ-58, ARQ-59 y COS-11 en `Docs\06-brechas\auditoria-extraccion-2026-11.md`. Las conocidas de antes: RYC-01 y COS-05. Qué es cada una: [glosario de brechas](roadmap-cambios.md#glosario-de-brechas).

## Estrategia para aplicar las dependencias
| Opción | Qué hace | Decisión |
|---|---|---|
| A. Tabla completa | Lee todo y filtra en Spark | Solo cuando la tabla completa cabe en un tramo (regla en el diseño) |
| B. Join con el pivote en Oracle | Estrategia "relacional" de PCR | No: repite el pivote en cada hija y carga la fuente |
| C. Llaves en `IN` | Lotes de 900 (Oracle no acepta más de 1000 valores) | Solo como respaldo para llaves de texto o muy dispersas |
| **D. Rangos de llave + filtro exacto** | Tramos `BETWEEN` en Oracle y semijoin exacto en Spark | **Elegida, en refinamiento:** [diseno-lectura-por-rangos.md](diseno-lectura-por-rangos.md) |

AWS DMS no está en la arquitectura (ni en el diagrama ni en la IaC). Se citó solo como ejemplo del patrón de rangos y **no se usa**.

## Repos afectados
| Repo | Qué cambia | Quién |
|---|---|---|
| `data-datalake-niif-rmd-ext-estandar-glue-job` | El proceso de extracción estándar: modular, pausa, tramos, medición real, log v1 | Desarrollo |
| `data-datalake-niif-pcr-emisiones-glue-job` | El proceso de extracción de Emisiones: lo mismo, más una sola versión (ARQ-60) | Desarrollo |
| `cfg-datalake-niif-config-s3` | Los 6 `config_extraccion_*.json` de ALFAPRD (ARL, Rentas, Endosos, ARP, RVI y Auxilios) y los de Emisiones (`emisiones.json`, `pcr_vida`, `generales`, `satelites`) | Desarrollo |
| `sc-datalake-niif-configuration-general` | Los `conf_*.py` y `conf_emisiones.py`: sin conteos fijos, sin valores por defecto y las claves nuevas | Desarrollo |
| `sc-datalake-niif-configurador-mallas` (T2) | Las mismas claves nuevas, si es el configurador que queda | Desarrollo |
| `iac-analytics-datalake-niif-operations-manager` | `--EJECUCION_ID` en las 6 Step Functions y en `niif-snf-emisiones` | DevOps (Desarrollo entrega el cambio) |

Se rozan:
- Las 6 configuradoras por malla tienen el mismo módulo copiado: si alguna corre, pisa las claves nuevas.
- Los dos procesos de extracción usan el mismo rol IAM: un cambio de permisos afecta a ambos.
- La validación técnica y los crawlers leen Bronce: los tramos no deben cambiar esa salida.

Verificado en DEV el 2026-11-20:
- El rol del proceso de extracción ya puede leer y escribir en `niif-{env}-ejc-ejecuciones` (y en `pipeline-estado`) por la política común de los jobs, que está en la IaC. El checkpoint por tramo no necesita permisos nuevos. No tiene acceso a `cnf-mallas` ni lo necesita, porque la configuración le llega en el JSON.
- El rol tiene 16 políticas y 13 no están en ninguno de los repos que tiene Desarrollo (rama `quality`): 9 en línea y 4 administradas, de PCR Vida, Emisiones y Satélites. Se preguntó a DevOps quién las creó.
- Las tablas `datalake-dev-adm-extraccion-*` existen, pero están vacías desde 2025-09-21 y ningún código las usa. No se adoptan sin aval.

## Qué falta para empezar (validado contra el código, 2026-11-20)
| Para | Falta |
|---|---|
| Proceso de extracción estándar | Solo la orden. La rama modular (un commit) hay que rebasarla sobre el `main` nuevo; su red de pruebas ya cubre el flujo ARL |
| Proceso de extracción de Emisiones | Su red de pruebas y su línea base en DEV: no existen. Por eso va después del estándar |
| Opción D | Resultados de QA; dónde se escriben los tramos (*propuesta:* zona temporal, Bronce igual que hoy); referencia de equivalencia para ALFAPRD (*propuesta:* la salida del job actual en el mismo periodo) |
| Desplegar | `--EJECUCION_ID` en las Step Functions (DevOps), junto con el código: el job lo exigirá |

Checkpoint de cada tramo (*propuesta*): en `ejc-ejecuciones`, con la misma `PK` que ya arma el configurador (`EJC#<MALLA>#<periodo>`) y `SK = EJECUCION#<execution_name>#TRAMO#<tabla>#<n>`. No choca con el ítem de la ejecución y el rol ya tiene permiso.

## Plan
Orden detallado por repo, separado por fix, refactor, observabilidad y features, en [roadmap-cambios.md](roadmap-cambios.md). Backlog para Jira (épica EXT, 23 ítems y 41 subtareas): [backlog.md](backlog.md) y `backlog-jira.csv`. Resumen:

1. Diseño de la opción D cerrado y validado con datos (este README y el documento de diseño).
2. Datos de Oracle con QA ([consultas-oracle-alfaprd.sql](consultas-oracle-alfaprd.sql) y [consultas-oracle-acsele.sql](consultas-oracle-acsele.sql)): tipos e índices de las llaves foráneas, estadísticas de tamaño y acceso de lectura a `acsele_alfa_bi.em_*`.
3. Rebasar `feature/modular-rmd-ext-estandar` sobre `quality` (`0af0ca8`, ya trae el fix del SCD) y llevar la red de caracterización a verde.
4. Red de pruebas y línea base del proceso de extracción de Emisiones.
5. Modularizar los dos procesos de extracción (primero el estándar): pausa por ciclo, lectura por tramos, medición real y contrato de log v1. En el mismo cambio, la configuración: JSON, `cnf-mallas` y configurador sin conteos fijos.
6. Medir en DEV: una tabla por ciclo, línea base frente a la versión nueva y comparación de equivalencia.
7. Mostrar a Víctor los resultados: la corrida tabla por tabla y la optimizada.
