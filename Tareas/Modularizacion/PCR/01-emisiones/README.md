# Malla 1 de PCR: Emisiones (primas emitidas)

Parte de [PCR](../README.md). Certifica Operaciones.

## Alcance
- **Código:** repo `data-datalake-niif-pcr-emisiones-glue-job`, `src\jobs\extraccion_estandar_emisiones.py` (rama `quality`, versión de QA del 2026-11-25, 853 líneas).
- **Plan de trabajo del repo:** [plan-repo-pcr-emisiones.md](plan-repo-pcr-emisiones.md).
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
| Cada tabla se lee con una sola sesión JDBC. Solo 2 tablas de PCR Vida usan `partitionColumn`, con 4 particiones | `:577-585` |
| Generales y Satélites (90 tablas) corren una por una: la forma `CASCADA_POR_FASE` no se reconoce como paralela. En PCR Vida y en el núcleo se leen como máximo 3 tablas a la vez, con ese tope fijo en el código | `:217`, `:764-768` |
| Los 3 casos corren uno tras otro | `:832-846` |
| La lectura de Oracle ocurre dentro de la escritura, y después se vuelve a leer el parquet para contar | `:696-718` |
| No hay pausa entre ciclos: Glue suelta la conexión y vuelve a pegarse sin descanso (Víctor, 20 nov, L289-293) | `execute_phases` |
| 115 de las 121 consultas SQL configuradas vuelven a filtrar contra el pivote `CONTEXTOPERATION` (331 millones de filas). Las tablas `DEPENDENCIA` lo hacen con `EXISTS` anidados | `:390-521` |
| Cada corrida crea un lote nuevo, así que un reintento no aprovecha lo ya extraído | `conf_emisiones.py:172` |

*Hipótesis por medir:* casi todo el clúster está sin uso mientras una sola sesión lee de Oracle. Se mide con `medir-uso-glue-dev.sh`.

## Validado con Víctor y por validar
- **Validado:** una sola conexión de servicio y el pool de peticiones lo manejamos nosotros (20 nov, L182-208). Pausa de 1 a 7 minutos por malla, configurada en DynamoDB (L293, L303). Reanudar desde un checkpoint (L563-569) y la decisión "¿Existe ese lote?" (25 nov, L79-83). La reanudación debe ser explícita y con aviso (ARQ-63).
- **Concurrencia (decisión del 2026-11-25):** se deja la que hay hoy y se vuelve parametrizable. No se le pregunta a Víctor: si a futuro cambia el número de sesiones, se ajusta en la configuración.

## Validaciones
| Script (`validaciones\scripts\`) | Dónde corre | Salida (`validaciones\salidas\`) | Resultado |
|---|---|---|---|
| `medir-tiempos-dev.sh` | CloudShell de DEV | `medir-tiempos-dev-20261125-2037.txt` | Secciones 1 a 3 completas (job y Step Function). La 4 falló y ya está corregida: hay que volver a correrla para tener las secciones 4 y 5 |
| Consulta de DynamoDB (`cnf-mallas` y `prg-programaciones`) | CloudShell de DEV | `cnf-mallas-emisiones-dev-20261125-2309\` | 6 ítems por caso. DynamoDB guarda plantillas y los JSON del repo son la salida renderizada, con las mismas tablas (25, 45 y 59). El núcleo de Generales difiere en 5 tablas (ARQ-67). PCR Vida tiene 25 tablas y la configuradora de `quality` exige 27: hay que confirmar qué código está desplegado |
| `medir-tiempos-dev.sh` (segunda corrida) | CloudShell de DEV | `medir-tiempos-dev-20261125-2348.txt` | Tiempo y filas por tabla de los últimos 5 lotes. En la corrida del 24 nov (321 min) los casos van en serie: núcleo 30 min, PCR Vida unos 90, Generales 67 y Satélites 135. Hay tablas pequeñas que tardan minutos: 154 filas en 19 min, 45 en 9 y 8 en 6. El conteo tarda 1 o 2 segundos |
| `medir-uso-glue-dev.sh` | CloudShell de DEV | `medir-uso-glue-dev-20261125-2350.txt` | **El clúster está sin uso:** CPU de ejecutores en 0,00 de promedio y heap en 2 %. El 95 % del tiempo de Spark se va en etapas con una sola tarea (386 de 424) |
| `probar-despliegue-dev.sh` (`verificar`, `ejecutar [ID]`, `seguir <JobRunId>`) | CloudShell de DEV | `PruebasPCREmisiones\probar-despliegue-dev-verificar-20261126-1431.txt` | **Prueba 1 (26 nov), FAIL:** el script modular ya está en DEV (desplegado el 26 nov 14:08 UTC), pero el paquete `emissions_extraction` no se subió (solo hay 2 objetos en el prefijo) y el job no tiene `--extra-py-files`. El job fallará al arrancar. No se lanzó nada. Qué hace el script: verifica que el script desplegado sea el modular y que el paquete llegue por `--extra-py-files` (hoy la IaC del job no lo tiene). Si pasa, respalda la configuración y el runtime, corre la configuradora sin ejecutar la malla, lanza el job y compara filas y ruta Silver por tabla con la corrida anterior |
| `consultas-oracle-acsele.sql`, consulta 0 | Oracle (base COREPROD) | `consulta0-base-usuario.png` | Corrió con un usuario personal. Falta confirmar que el servicio ACSSOP apunta a COREPROD |
| Consulta 1: llaves e índices | Oracle | `consulta1-llaves-indices.xlsx` | 144 llaves: 132 `NUMBER`, 5 `DATE` y 7 `VARCHAR2`. 9 sin índice y 6 solo como 2.ª a 4.ª columna de un índice |
| Consulta 2: tamaños | Oracle | `consulta2-tamanos.xlsx` | 31.000 millones de filas en total. Las más grandes: `THIRDPARTYMOVEMENTPOLICY` (7.547 M), `OPENITEM` y `OPENITEMREFERENCE` (2.043 M cada una), `EVENTDCO` (1.926 M), `COBERTURA` y `COVERAGEDCO` (1.245 M cada una) |
| Consultas 4 y 5: tablas `em_*` | Oracle | `consulta4-tablas-em.png`, `consulta5-permisos-em.png` | 0 filas con el usuario personal |
| Consulta 3a: flashback | Oracle | `consulta3a-flashback-*.png` | 0 filas con el usuario personal |
| Consulta 3b: retención de UNDO | Oracle | `consulta3b-undo-error.png` | ORA-00942. Se pide el valor al DBA |

El usuario de servicio no se usa: su clave la custodia otro equipo. Los permisos se le piden al DBA con la variante 3d del SQL.

## Registro de tiempos (para el gráfico de la entrega)
Cada medición se consolida en `tiempos\registro-tiempos.csv`, con un nivel de detalle por job, por caso y por tabla:
```
python validaciones\scripts\consolidar-tiempos.py validaciones\salidas\<salida>.txt --version "<versión>" --sesiones "<sesiones Oracle>"
```
La línea base (versión de QA, corrida del 24 de noviembre) es de 321 minutos: núcleo y PCR Vida 119,5, Generales 66,8 y Satélites 135,3. Cada cambio agrega su medición con el nombre de su versión.

## Siguientes pasos
1. ~~Código modular sobre la red de pruebas, sin cambiar el comportamiento.~~ **Hecho (2026-11-25):** paquete hexagonal, script de entrada de 10 líneas, 155 pruebas y 98 % de cobertura. Falta definir el empaquetado con DevOps y una corrida de equivalencia en DEV.
2. Concurrencia: se deja la que hay hoy y se vuelve parametrizable por configuración.
3. Optimización del lado de Glue, con cada cambio medido y registrado en `tiempos\registro-tiempos.csv`.
