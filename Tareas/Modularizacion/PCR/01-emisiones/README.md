# Malla 1 de PCR: Emisiones (primas emitidas)

Parte de [PCR](../README.md). Certifica Operaciones.

## Alcance
- **Código:** repo `data-datalake-niif-pcr-emisiones-glue-job`, `src\jobs\extraccion_estandar_emisiones.py` (rama `quality`).
- **En DEV:** job `niif-dev-extraccion-estandar-emisiones`, primer paso de la Step Function `niif-dev-emisiones`.
- **Qué extrae:** un solo job lee los 3 casos (PCR Vida, Generales y Satélites). Primero lee una vez el núcleo común (7 tablas) y después cada caso. Son las 78 tablas de Acsel-e que usa `M1_EMISIONES.sql`.
- **Fuente:** Oracle Acsel-e, conexión `datalake-dev-acsele-sop-connection` (servicio ACSSOP).

## Línea base (DEV, corridas del 16 al 24 de noviembre)
| Qué | Valor |
|---|---|
| Extracción completa | 172 a 427 min. Las 3 últimas tardaron 321, 308 y 282 min |
| Cómputo | G.2X × 10 workers, unos 100 DPU-hora por corrida |
| Corridas | 34 en total: 8 exitosas, el resto fallidas o detenidas |
| Step Function completa | 1 exitosa de 10 (393 min, de los cuales 321 fueron de extracción) |
| **Meta** | **90 min como máximo** |

Detalle en `validaciones\salidas\medir-tiempos-dev-20261125-2037.txt`.

## Qué hace lento al job (verificado en el código)
| Hallazgo | Dónde |
|---|---|
| Cada tabla se lee con una sola tarea de Spark: no hay `partitionColumn`, y solo 2 tablas usan `hashpartitions` | `:515-551` |
| Hasta 3 tablas a la vez, con ese tope fijo en el código, y solo en las fases `PARALELO`. 98 de 108 tablas van en cascada | `:730` |
| Los 3 casos corren uno tras otro | `:798-803` |
| La misma tarea que lee escribe el parquet, y después se vuelve a leer el parquet para contar | `:663-683` |
| No hay pausa entre ciclos: Glue suelta la conexión y vuelve a pegarse sin descanso (Víctor, 20 nov, L289-293) | `execute_phases` |
| 112 de las 128 consultas vuelven a filtrar contra el pivote `CONTEXTOPERATION` (331 millones de filas) con `EXISTS` anidados | `compile_dependency_sql` |
| Cada corrida crea un lote nuevo, así que un reintento no aprovecha lo ya extraído | `conf_emisiones.py:172` |

*Hipótesis por medir:* casi todo el clúster está sin uso mientras una sola sesión lee de Oracle. Se mide con `medir-uso-glue-dev.sh`.

## Validado con Víctor y por validar
- **Validado:** una sola conexión de servicio y el pool de peticiones lo manejamos nosotros (20 nov, L182-208). Pausa de 1 a 7 minutos por malla, configurada en DynamoDB (L293, L303). Reanudar desde un checkpoint (L563-569) y la decisión "¿Existe ese lote?" (25 nov, L79-83). La reanudación debe ser explícita y con aviso (ARQ-63).
- **Por validar:** la forma paralela, que según Víctor corre "por esa misma conexión" (L374), ¿acepta varias sesiones JDBC a la vez con el mismo usuario? ¿Cuántas como máximo? De esa respuesta depende leer una tabla en particiones o correr los 3 casos a la vez.

## Validaciones
| Script (`validaciones\scripts\`) | Dónde corre | Salida (`validaciones\salidas\`) | Resultado |
|---|---|---|---|
| `medir-tiempos-dev.sh` | CloudShell de DEV | `medir-tiempos-dev-20261125-2037.txt` | Secciones 1 a 3 completas (job y Step Function). La 4 falló y ya está corregida: hay que volver a correrla para tener las secciones 4 y 5 |
| `medir-uso-glue-dev.sh` | CloudShell de DEV | (pendiente) | Uso del clúster y etapas de Spark de las corridas existentes |
| `consultas-oracle-acsele.sql`, consulta 0 | Oracle (base COREPROD) | `consulta0-base-usuario.png` | Corrió con un usuario personal. Falta confirmar que el servicio ACSSOP apunta a COREPROD |
| Consulta 1: llaves e índices | Oracle | `consulta1-llaves-indices.xlsx` | 144 llaves: 132 `NUMBER`, 5 `DATE` y 7 `VARCHAR2`. 9 sin índice y 6 solo como 2.ª a 4.ª columna de un índice |
| Consulta 2: tamaños | Oracle | `consulta2-tamanos.xlsx` | 31.000 millones de filas en total. Las más grandes: `THIRDPARTYMOVEMENTPOLICY` (7.547 M), `OPENITEM` y `OPENITEMREFERENCE` (2.043 M cada una), `EVENTDCO` (1.926 M), `COBERTURA` y `COVERAGEDCO` (1.245 M cada una) |
| Consultas 4 y 5: tablas `em_*` | Oracle | `consulta4-tablas-em.png`, `consulta5-permisos-em.png` | 0 filas con el usuario personal |
| Consulta 3a: flashback | Oracle | `consulta3a-flashback-*.png` | 0 filas con el usuario personal |
| Consulta 3b: retención de UNDO | Oracle | `consulta3b-undo-error.png` | ORA-00942. Se pide el valor al DBA |

El usuario de servicio no se usa: su clave la custodia otro equipo. Los permisos se le piden al DBA con la variante 3d del SQL.

## Siguientes pasos
1. Volver a correr `medir-tiempos-dev.sh` (tiempo por tabla) y correr `medir-uso-glue-dev.sh` (uso del clúster).
2. Preguntarle a Víctor cuántas sesiones simultáneas se permiten.
3. Con esas mediciones, priorizar el plan del lado de Glue: pausa y pool de peticiones configurables, reanudación explícita, tamaño del clúster y, si Víctor lo permite, lectura en paralelo.
