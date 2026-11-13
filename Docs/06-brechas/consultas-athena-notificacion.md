# Consultas de Athena de la certificación y la notificación (DEV)

Fecha: 2026-11-10.
- **Fuentes:** los 4 JSON de caso vigentes de `s3://niif-dev-assets/`, guardados en `EvidenciasAws\dev-config-notificacion\` (fuera de Git); el catálogo de Gold (`Backend\Glue\catalogo\tablas-niif-dev-gold-catalog-db.json`, foto del 2026-11-03); el workgroup de Athena (`EvidenciasAws\aud-datos-cuenta\athena\`); y el código de los dos jobs.
- **Quién ejecuta qué:** el job de certificación corre `detail` (un `UNLOAD` a CSV) y el de notificación corre `summary` y `total` para armar el correo. Las consultas viven en el JSON de cada caso, no en el código.

| Caso (JSON) | detail | summary | total | Tablas de Gold |
|---|---|---|---|---|
| `primas-arl/primasArl.json` | sí | **no tiene** | sí | `scd_primas_arl` |
| `primas-rentas/primasRentasEndosos.json` | sí | sí | sí | `scd_primas_endosos` + `scd_primas_rentas` |
| `auxilios-funerarios/auxilioFunerario.json` | sí | sí | sí | `scd_auxilios_funerarios` |
| `reserva-matematica/reservaMatematicaRviArp.json` | sí (52.000 caracteres) | sí | sí | `scd_reserva_matematica_{rvi,arp}` + `reserva_matematica_cargue_unico` |

## Costos (Athena)
| ID | Hallazgo | Evidencia | Corrección |
|---|---|---|---|
| COS-07 | **Ninguna tabla de Gold que se consulta está particionada.** Cada consulta lee la tabla completa (todos los periodos y todas las versiones del SCD) para quedarse con un periodo y `act_reg = 1`. Se paga por todo el historial en cada certificación y en cada reenvío, y el costo crece cada mes. | Catálogo: `PartitionKeys` vacío en las 7 tablas | Particionar Gold por periodo (con `act_reg` como segundo nivel o una tabla de vigentes) y filtrar por la partición |
| COS-08 | **Las consultas no tienen tope de costo.** Ningún job pasa `WorkGroup`, así que todo corre en `primary`, que no tiene `BytesScannedCutoffPerQuery` y tiene `EnforceWorkGroupConfiguration = false`. | `start_query_execution` sin `WorkGroup` en los dos jobs; `workgroups.json` | Workgroup propio de NIIF con tope de escaneo y configuración forzada; nombre del workgroup en la configuración global |
| COS-09 | **El filtro de periodo convierte la columna:** `CAST(per_corte AS VARCHAR) = '$period'` sobre un `int`, y `CAST(... AS VARCHAR)` sobre columnas que ya son `string`. Así se pierde el filtrado por estadísticas de Parquet y, cuando haya particiones, también la poda. | SQL de los 4 casos | Comparar la columna en su tipo (`per_corte = 202606`, `per_proceso_point = '202606'`) |

## Montos y cuadre
| ID | Hallazgo | Evidencia | Corrección |
|---|---|---|---|
| ARQ-34 | **El detalle y el total pueden no cuadrar por redondeo.** Gold guarda `decimal(17,2)` y las consultas redondean a pesos (`DECIMAL(38,0)`) en lugares distintos:<br>• **ARL:** el detalle redondea cada fila y el total suma sin redondear y redondea al final.<br>• **Rentas y Endosos:** el detalle redondea cada grupo, mientras summary y total suman con 7 decimales.<br>• **Auxilios:** el total suma valores ya redondeados.<br>Lo que el aprobador ve en el correo puede diferir en pesos de la suma del CSV que certifica, y los centavos se pierden. | SQL de los 4 casos; tipos del catálogo | Una sola regla de redondeo, declarada en la configuración. Total = suma exacta del mismo conjunto que el detalle. Prueba de cuadre (`suma(detalle) == total`) antes de enviar |
| ARQ-35 | **ARL no tiene consulta `summary`,** ni en DEV ni en PRD: el correo de ARL sale siempre sin resumen. Es la causa del resumen vacío del incidente de Primas ARL 202606, y sigue igual. | `queries.summary = []` | Agregar el resumen de ARL (propuesta: por sucursal o por mes contable, con conteo y valor). Que el job **no envíe** si falta una consulta obligatoria |
| ARQ-36 | **El resumen y el total de Reserva Matemática no tienen montos.** El `summary` solo lista `origen_reserva`, `periodo` y `origen_pension` (`SELECT DISTINCT`), y el `total` solo cuenta registros. Se certifica una reserva sin ver su valor. | SQL de `reservaMatematicaRviArp.json` | Confirmar con negocio qué montos deben verse (reserva total por origen) |
| ARQ-37 | **Rentas y Endosos cuenta `count(distinct nro_endoso)` mezclando dos cosas.** En la parte de rentas, `nro_endoso` es el número de renta. Si un número de endoso coincide con uno de renta, el conteo queda corto. Además, el detalle usa `UNION`, que quita filas idénticas. (**Hipótesis**: hay que medirlo con datos.) | SQL de `primasRentasEndosos.json` | Contar con una llave compuesta (`naturaleza` + número) y usar `UNION ALL`. Verificar con una consulta de conteo en DEV |

## Seguridad y fallos silenciosos
| ID | Hallazgo | Evidencia | Corrección |
|---|---|---|---|
| ARQ-38 | **Si falta una variable, la consulta corre igual y no avisa.** El job sustituye las variables con `Template.safe_substitute`: si falta `period`, el texto `'$period'` queda tal cual en el SQL, la consulta devuelve 0 filas y el correo sale "sin datos" sin ningún error. | `parametrizar()` en `notification_certification.py` (l. 191-200) | Usar `substitute` (falla si falta una variable), validar `period` como `AAAAMM` antes de sustituir y fallar si queda algún `$` sin reemplazar |
| SEG-15 | **El log guarda el SQL completo** (`[ATHENA] Query final`) en cada corrida. No trae datos personales, pero sí la estructura de Gold; con el detalle de Reserva son 52.000 caracteres por corrida. | `query_athena()` l. 309 | Registrar solo `queryName`, el ID de Athena y los bytes escaneados (que además sirven para medir el costo) |
| — | El detalle de ARL lleva `nid_numero_identificacion_tercero` y `nom_tercero`. Según el usuario son datos de ejemplo; revisar antes de producción ([[preguntas-abiertas]]). | SQL de ARL | — |

## Configuración duplicada (riesgo de corregir la copia equivocada)
- `primas-endosos/primasRentasEndosos.json` pesa 13.429 bytes y el vigente, `primas-rentas/primasRentasEndosos.json`, pesa 13.690. Las Step Functions usan el de `primas-rentas/`.
- `primas-arl/configs/certification/primasArl.json` y `auxilios-funerarios/configs/certification/auxilioFunerario.json` son copias que no se usan.
- `reserva-matematica-rvi/reservaMatematicaRvi.json` (2026-11-09) y `previsional/previsional.json` son nuevos y sin dueño confirmado.
- Propuesta: una sola ubicación por caso, versionada en el repositorio de configuración. Las copias se retiran cuando se confirme su dueño.

## Cómo medir antes de cambiar (DEV, solo lectura)
Bytes escaneados de las últimas corridas: `aws athena get-query-execution --query-execution-id <id> --query 'QueryExecution.Statistics.DataScannedInBytes'`. Los IDs salen del log `[ATHENA] QueryExecutionId`. Con ese dato se calcula el ahorro de COS-07.

## Medición en DEV (2026-11-11)
Script: `Backend\cloudshell-inventario\medir-athena-dev.sh` (solo lectura). Datos en `EvidenciasAws\dev-medicion-athena\`, fuera de Git. Cubre el historial de Athena de los últimos 45 días (232 consultas sobre Gold de NIIF) y las corridas de los 2 jobs de la cadena.

| Qué | Resultado |
|---|---|
| Tiempo de nuestras consultas (`summary`, `total`, disponibilidad) | **0,8 a 2,3 s.** Las `detail` (UNLOAD) tardan hasta unos 10 s. La consulta más lenta del historial tardó 10,3 s |
| Escaneo | Hasta 1 MB por consulta nuestra y 40 MB la mayor del historial. En total, 0,37 GB en 45 días |
| Costo de Athena | **USD 0,01 en 45 días.** En DEV no tiene peso |
| Duración de los jobs | Certificación: mediana 56 s y máximo 201 s. Notificación: mediana 47 s y máximo 138 s. **Casi todo es el arranque de Glue con Spark** (`glueetl`), no Athena |
| Fallas de los jobs (18 de 92 en certificación y 29 de 100 en notificación) | Casi todas son de **permisos durante el desarrollo**, entre agosto y noviembre: `AccessDenied` en DynamoDB, S3, KMS y SNS. También hubo una tabla inexistente de Reserva, un error de código (`NameError`) y un conflicto del control compuesto. **Ninguna falla fue por timeout de Athena** |
| COS-07 | **Confirmado:** ninguna tabla de Gold está particionada (10 de 10) |
| COS-08 | **Confirmado:** el workgroup `primary` no tiene tope de escaneo y no fuerza su configuración |

**Conclusiones:**
- **En DEV el riesgo de timeout de Athena es bajo.** El tiempo máximo del job (`SEGUNDOS_MAXIMOS_CONSULTA = 900`) sobra; se puede bajar a 120-300 s. **Pero DEV tiene muy pocos datos:** el Gold de ARL en PRD tiene más de 1,2 millones de filas. Antes de decidir el particionado (7.6) hay que medir en PRD. El mismo script sirve en modo solo lectura con `AMB=prd bash medir-athena-dev.sh`, sin `--ejecutar`, y lo corre el equipo de PRD.
- **El tiempo de los jobs está en el arranque de Spark.** Ni la notificación ni la certificación usan Spark para calcular nada. Pasarlos a Glue Python shell, o a una lambda en el caso de la notificación, bajaría la duración y el costo. Queda como propuesta: hay que medirlo.
- **Nota (no es brecha):** en DEV, el Gold de 5 mallas (`scd_primas_rentas`, `scd_primas_endosos`, `scd_auxilios_funerarios`, `scd_reserva_matematica_arp` y `scd_reserva_matematica_rvi`) se vació el 2026-11-05 en un solo borrado. DEV es ambiente de pruebas y eso es esperable. Para probar con datos: usar Primas ARL, que conserva su Gold, o volver a correr la malla que haga falta.
- 178 consultas del historial no coinciden con el SQL actual de los JSON de caso. Son versiones anteriores de las consultas o consultas manuales; no cambian las conclusiones.
