# Diseño: lectura de dependencias por rangos de llave (opción D)

Estado: **propuesta en refinamiento** (2026-11-20). Nada de esto está implementado. Lo marcado *por verificar* no se ha comprobado.

## Idea
Una consulta base hace `em_1 INNER JOIN policydco ON ctx_id = operationpk`. En Oracle, `em_1` es una tabla con índice; en AWS es un Parquet en S3 y no se puede escribir en la fuente. La opción D obtiene las mismas filas de la tabla hija en tres pasos:

1. **Rangos.** Se ordenan las llaves distintas de la dependencia y se parten en tramos contiguos `[desde, hasta]`, cada uno con el mismo número de llaves.
2. **Oracle.** Por cada tramo, una consulta simple: `SELECT <columnas> FROM schema.tabla WHERE fk BETWEEN :desde AND :hasta [AND where propio]`.
3. **Spark.** Semijoin exacto contra las llaves de la dependencia. Las filas de más que trajo el rango, por los huecos entre llaves, se descartan.

**Equivalencia.** El semijoin entrega exactamente las filas de la hija que el `INNER JOIN` toca. El join completo, con sus columnas y su multiplicidad, se rearma después en Spark igual que en la consulta base. Las llaves nulas no entran en ninguno de los dos casos.

## Configuración (por tabla, en `cnf-mallas` y en el JSON)
| Clave | Ejemplo | Origen del valor |
|---|---|---|
| `estrategia_lectura` | `RANGOS` \| `COMPLETA` \| `IN_LOTES` | Se decide con las reglas de abajo |
| `columna_rango` | `OPERATIONPK` | La consulta base (columna del join) |
| `llaves_por_tramo` | Aprendido | Medición: primera corrida con una tabla por ciclo |
| `columnas` | Lista explícita | La consulta base. Sin `SELECT *`: menos columnas, menos unidades |
| `pausa_segundos` (por ciclo) | 180 en Acsel-e para empezar | Víctor (L303-305), luego se ajusta midiendo |
| `presupuesto_unidades_ciclo` | Aprendido | 65 % de la capacidad observada (L394-406) |

## Reglas de decisión, sin umbrales inventados
1. **¿Cuándo leer la tabla completa (A)?** Cuando las unidades de la tabla completa no superan las de un tramo: leerla entera cuesta lo mismo o menos que pedir un solo rango. El tamaño de la tabla completa sale, en este orden:
   - de la última extracción medida (`parametros_extraccion.json`: `total_registros × total_columnas`);
   - si no hay medición, de las estadísticas de Oracle (`ALL_TABLES.NUM_ROWS`, `LAST_ANALYZED`). Es una consulta al diccionario, sin tocar la tabla. *Por verificar* que el usuario de servicio la pueda leer y qué tan recientes son las estadísticas;
   - **nunca de la etiqueta `tipo_tabla`.** En el JSON de ARL, `COMPLEMENTARIA` va de 7.002 filas (`par_afiliaciones`) a 1.210.373 (`acu_personas_naturales`): la etiqueta no dice el tamaño.
2. **¿Cuándo usar rangos (D)?** La llave debe ser **numérica o fecha**, y la densidad (llaves del tramo ÷ (`hasta` − `desde` + 1)) debe ser suficiente para que la sobrelectura no supere lo que se ahorra. La densidad se calcula en el Parquet de la dependencia, antes de consultar Oracle.
3. **Llaves de texto o muy dispersas: lotes `IN` (C).** Con texto, el orden de Oracle (`NLS_SORT`) puede no coincidir con el de Spark: un `BETWEEN` podría dejar filas por fuera. Por eso el texto no va por rangos.
4. **Llave compuesta** (por ejemplo `NID_POLIZA`, `COD_SUCURSAL_ALFA`, `MODULO`): el rango va sobre la columna que tenga índice en Oracle, y el semijoin usa todas las columnas.

## Pool y una sola conexión
- **Los tramos se leen en secuencia.** No se usan `predicates` ni `hashpartitions` de Spark JDBC: Spark abre una conexión por tarea concurrente, y eso rompe la regla de una sola conexión de servicio.
- El tamaño del pool queda en configuración. Empieza en 1 y solo sube si la medición lo justifica y Víctor lo acepta.
- Cada tramo suma sus unidades al presupuesto del ciclo. Al llegar al 65 %, el ciclo se cierra, corre la pausa y sigue el siguiente.
- *Por verificar* en el log: que cada lectura de Glue JDBC abra y cierre su conexión, es decir, que no quede una sesión abierta durante la pausa.

## Checkpoint y reanudación
- Cada tramo escribe su propio Parquet (`.../tramo=<n>/`, en sobrescritura). Repetir un tramo deja el mismo resultado.
- Registro por tramo en DynamoDB (Víctor, L561-565): `ejecucion_id`, tabla, tramo, `desde`, `hasta`, estado, filas, unidades y duración. Al retomar, se saltan los tramos `SUCCESS`.
- Por definir: la tabla de DynamoDB (propuesta: el Modelo Trazabilidad, `ejc-ejecuciones`, ver `Docs\06-brechas\roadmap-observabilidad.md`).

## Consistencia entre tramos (punto abierto)
- Una consulta base (`CREATE TABLE ... AS SELECT`) lee una sola foto de la base. Los tramos se leen en momentos distintos de una base viva.
- Opción estándar en Oracle: leer todos los tramos con `AS OF SCN <scn>`. Exige el permiso `FLASHBACK` sobre las tablas y suficiente `UNDO_RETENTION`. *Por verificar con Julio.*
- Si no hay *flashback*, hay que medir si el periodo ya cerrado cambia durante la extracción, comparando conteos al inicio y al final.

## Observabilidad (contrato de log v1)
- Cada tramo emite `PASO_INICIO` y `PASO_FIN` con `ejecucion_id`, tabla, tramo, filas, unidades, densidad, sobrelectura y `duracion_ms`.
- La duración se mide **después de la acción que de verdad lee** (la escritura del tramo), no al crear el DataFrame. Eso corrige ARQ-58.
- Al final de cada tabla se registran las filas por llave (`filas_hija ÷ llaves`). Es el dato que permite aprender el tamaño del tramo (P3 de la reunión).

## Datos que ya se tienen (DEV)
- PCR Vida (Acsel-e): la pivote `contextoperation` tiene 3.928.778 filas. La mayoría de las hijas tiene ~3,9 millones (relación 1 a 1). Las que multiplican: `thirdpartymovementpolicy` 36.690.115 (unas 9 por llave), `stps_relationagreementpolicy` 18.578.682 y `stpo_policyparticipation` 7.290.626.
- ALFAPRD: tiempos y unidades por tabla de ARL y Rentas en el README de esta carpeta.

## Por verificar antes de implementar (con Julio, en Oracle)
1. Tipo de dato de cada columna de join (`ALL_TAB_COLUMNS`) y si tiene índice (`ALL_IND_COLUMNS`). Si falta un índice, va como recomendación a Víctor (no lo pedimos nosotros).
2. Si el usuario de servicio puede leer `ALL_TABLES.NUM_ROWS` y qué tan recientes son las estadísticas.
3. Permiso de `FLASHBACK` y `UNDO_RETENTION`.
4. Dónde están las tablas `acsele_alfa_bi.em_*` y si se pueden leer, para la prueba de equivalencia.

## Prueba de equivalencia
Para cada paso `em_N`: conteo, suma de control de montos y diferencia exacta en Spark (`exceptAll` en los dos sentidos, que debe dar 0) entre nuestro resultado y la tabla `em_N` de la consulta base. Una optimización que no pase esta prueba no entra.
