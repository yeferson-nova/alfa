# PCR Emisiones: cifras de junio de 2026 distintas entre PRD y QA

## Qué pasó

Negocio compartió dos archivos con lo emitido en junio de 2026 y pidió que la extracción coincida con ellos:

- `Resumen emision 202606.xlsx`: totales por compañía y producto (Generales, Pymes, Vida y Satélites).
- `RRC SIN CONTABILIDAD ALFA 202606.xlsx`: el detalle de Generales del que sale el resumen.

Se reporta que la data de PRD trae cifras distintas a las de QA. **No sabemos todavía si PRD y QA tienen los mismos datos de origen.** En teoría sí, pero hay que comprobarlo antes de concluir que el problema está en nuestro proceso. Por eso este trabajo no corrige nada todavía: mide cada capa en cada ambiente y muestra en cuál empieza la diferencia.

## Lo que ya está comprobado (con código, sobre los Excel)

Se comprobó con `diagnostico/validar_referencia.py`:

| Comprobación | Resultado |
|---|---|
| El resumen de Generales sale del detalle RRC tomando solo `Mes Produccion = 202606` | **PASS**, los 5 productos y las 6 columnas cuadran al centavo |
| Filas del detalle RRC | 4.478; 3.752 son de junio y 726 son de meses de producción anteriores (julio 2025 a mayo 2026) que vienen dentro del mismo corte de junio y **no** entran al resumen |
| El propio Excel frente a otro informe del negocio (fila `E1-A1`) | Generales: IVA de comisión de recaudo difiere en **63.748.417,52** (el archivo lo anota como "Diferencia en la base de RRC"); prima, -259,73. Vida: comisión, 4.053,80; costo de recaudo, -42.900 |
| Vida (RRC) frente a Satélites, mismo producto | VidaAp: comisión 57.720,10 frente a 53.666,30; IVA de comisión con diferencias de centavos a pesos en 5 productos |

Conclusión: la referencia es consistente consigo misma para Generales, pero **no es una sola cifra**: el negocio tiene dos fuentes que no cuadran entre sí. Hay que acordar con negocio contra cuál se valida.

## Dónde puede nacer la diferencia (hipótesis por medir)

Cada hipótesis sale de leer el código de la rama `quality` de cada repo (foto del 2026-11-20). Ninguna se da por cierta hasta ver la evidencia de los dos ambientes.

| # | Hipótesis | De dónde sale | Cómo se mide |
|---|---|---|---|
| H1 | La Oracle ACSELE de QA y la de PRD no tienen los mismos datos de junio (copia de otra fecha, cierres posteriores) | No se ha medido nunca | `conciliacion-oracle-acsele.sql`, igual en las dos bases |
| H2 | PRD no corre el mismo código que QA | En 12 de 33 repos `quality` y `main` difieren (ARQ-50). Además `sc-datalake-niif-configuration-general` (`src/lambda_function.py:21`, `src/conf_emisiones.py:35`, commit `e881f637`) solo acepta los ambientes `dev`, `qa` y `prod`, y los recursos de producción se llaman `niif-prd-*`: con ese código PRD no podría configurar la malla | Sección `codigo`: huella sha256 de cada script y de la lambda en cada ambiente |
| H3 | La configuración de la corrida es distinta: ventana, status, productos o compañías del PRG (`PROGRAMACION#EMISIONES`), o las SQL de `cnf-mallas` | `src/conf_emisiones.py:519-535` arma la ventana y los filtros desde el PRG de cada ambiente | Sección `configuracion` |
| H4 | Rutas quemadas por ambiente | `data-datalake-niif-pcr-emisiones-glue-job`, `src/jobs/extraccion_estandar_emisiones.py:581` deriva la ruta Silver reemplazando el texto `alfa-datalake-qa-bronce` (solo funciona en QA); `:52-53` usa por defecto el Bronce de PRD. En `cfg-datalake-niif-config-s3` (`a3453320`), los JSON de `src/pcr_vida/` de validación, remediación y SCD apuntan a rutas de DEV | Alertas `*-AMBIENTE-AJENO` y `RUN-SILVER-EN-BRONCE` sobre lo desplegado |
| H5 | Una reejecución reutiliza datos viejos | `extraccion_estandar_emisiones.py:607-611`: si existe el checkpoint del mismo periodo y lote, la tabla no se vuelve a leer de Oracle; el lote sale del PRG si está fijo (`conf_emisiones.py:172-173`). Además `:768-776` termina en `SUCCESS` aunque la calidad de fuentes sea `INCOMPLETA` | Alertas `CFG-LOTE-FIJO`, `RUN-DATASET-REUSED`, `RUN-FUENTES-INCOMPLETAS`; fechas de los checkpoints |
| H6 | El Silver de Generales repite montos por fila | `data-datalake-emisiones-vida-glue-job`, `src/jobs/silver_generales.py:804` suma los movimientos por (operación, póliza), pero la base se une con las unidades de riesgo solo por operación (`:184`): si una operación tiene varias unidades de riesgo, la prima queda en varias filas | Consulta `silver_<caso>_totales_por_operacion` frente a `silver_<caso>_totales` |
| H7 | "Lo emitido en junio" no se define igual | El Excel filtra por mes de producción; el proceso filtra por la fecha de la operación (`TIME_STAMP`) dentro de junio. Las 726 filas de meses anteriores dentro del corte apuntan a esta diferencia | El comparador cruza el Silver con las dos variantes del Excel (solo junio y todo el corte) |
| H8 | Las columnas no son equivalentes | El Silver de Generales no produce "IVA comisión intermediario" (`silver_generales.py:748-785`). El mapeo de las demás columnas es una **propuesta** (`diagnostico/mapeo-columnas-propuesta.json`) | Totales por concepto (Oracle Q5 y Bronce) para confirmar qué conceptos forman cada columna |
| H9 | El Silver no usa los filtros del PRG | `silver_generales.py:20-21` tiene la lista de productos y la compañía fijas en el código; `:146` toma `status = 2` si el PRG no trae status | Sección `configuracion` (filtros efectivos) frente al código |
| H10 | El Silver de Generales no recibe un argumento obligatorio | `silver_generales.py:1175-1181` exige `--RUTA_CONFIG_SCD`; en `iac-analytics-datalake-niif-operations-manager` (`77de48cb`) ni la Step Function `niif-snf-emisiones.json` (`PreparacionSilver_GENERALES`) ni el job (`stacks/analytics/glue_jobs/parameters.tf:909-935`) lo pasan | Alerta `ORQ-FALTA-ARGUMENTO` y corridas del job |

## Resultados de la primera corrida: DEV y QA (2026-11-23)

Evidencia: diagnóstico en DEV y en QA, y consultas Q1 a Q4 de `conciliacion-oracle-acsele.sql` en la Oracle de QA (capturas). Falta PRD y faltan Q5 y Q6.

| Capa | Resultado | Evidencia |
|---|---|---|
| Fuente | DEV y QA leen **la misma Oracle** (misma conexión, servicio ACSSOP). La base llega hasta el 18/11/2026 (Q1) | `fuente/conexiones.json` de las dos evidencias; Q1 |
| Extracción QA frente a Oracle QA | **Cuadra al registro** en 18 conteos: operaciones de junio con estado 2 (3.928.778, Q2), Vida Deudores (3.703.183), IncendioTerremotoDeudores (203.161), MiHogarSeguro (95), ProtecciónFinanciera (314), Practiseguro (10.776 operaciones y 10.568 pólizas), SalvaFactura (6.908), VidaAp (560), ProtecciónSalud (60), ProtecciónVida (111) | Runtime de QA frente a Q2 y Q4 |
| Tablas que no existen en la fuente | 13 tablas dan `ORA-00942` y la extracción las omite (`SKIPPED_NOT_AVAILABLE`): en Generales `POMASCOTAS`, `PODECENAL`, `URMASCOTAS`, `URDECENAL`, `URPROTECCIONFINANCIERA`, `OAMASCOTAS`; en Satélites `POPETLOVER`, `URPETLOVER`, `UREXEQUIASTRADICIONAL`, `URPROTECCIONSALUD`, `URPROTECCIONVIDA`, `OAPETLOVER`, `OAEXEQUIASTRADICIONAL`. Son tablas de atributos (no de montos), pero muestran que esta Oracle **no tiene la misma estructura** que se espera de producción | Runtime, campo `motivo` |
| Código | DEV y QA corren **exactamente el mismo código** (sha256 igual en los 7 jobs y en la lambda configuradora) | `codigo/` |
| Orquestación | Misma Step Function. La desplegada **sí pasa** `--RUTA_CONFIG_SCD` al Silver de Generales: H10 no aplica en DEV ni en QA | `orquestacion/`, argumentos de las corridas |
| Configuración | Mismos filtros en el PRG (status 2, mismos productos y compañías); ventana de junio | `configuracion/prg_programacion_emisiones.json` |
| Extracción DEV frente a QA | Mismo periodo y misma Oracle, pero **conteos distintos** (DEV 4.006.114 operaciones, QA 3.928.778; unas tablas suben y otras bajan). DEV extrajo entre el 19 y el 21 de noviembre y QA el 22: el contenido de la Oracle cambió entre esas fechas (*hipótesis: recarga de la base; confirmar con el DBA*) | Runtime de las dos evidencias |
| DEV como referencia | **No sirve como línea base**: tiene corridas manuales recientes con configuraciones de prueba (complementos de Pymes, uniones de lotes) | `orquestacion/corridas_glue.json` |
| Montos (Silver y Bronce) | Medidos en la segunda corrida (ver la sección siguiente) | `evidencia-pcr-emisiones-qa-20261123T215411Z` |

Errores del diagnóstico en esta corrida (ya corregidos en el script):
- Silver: la consulta sumaba también `idproducto` y Athena la rechazó por nombre ambiguo; en Satélites tomó la tabla de la carpeta `controles`.
- Bronce: tomó tablas de cargas completas antiguas (idénticas en DEV y QA) y las sumó sin filtro (15 GB por consulta). **Esos CSV no deben usarse.** Ahora el script consulta solo la tabla de la carpeta exacta del lote del runtime y, si no la encuentra, no consulta.

Para repetir solo la parte de datos (rápido, sin volver a bajar código):
```
SECCIONES=configuracion,datos bash diag-emisiones-v2.sh qa 202606 primary
```

**Conclusión parcial:** en QA, la extracción reproduce fielmente la Oracle a la que está conectada. Si PRD da cifras distintas, lo primero es saber a qué Oracle se conecta PRD y si es la misma información que ACSSOP (que no tiene 13 tablas y cambia entre fechas). Eso se responde corriendo el diagnóstico en PRD y Q1 a Q6 en su Oracle.

## Montos de QA frente al Excel (segunda corrida, 2026-11-23)

Silver preparada de QA, lote `20261122234023`, frente al Excel. Diagnóstico `v2026-11-23.2`, sin errores.

| Caso | Resultado |
|---|---|
| **Satélites** (7 productos) | Prima, comisión, comisión de recaudo e IVA de recaudo **iguales al centavo** en los 7 productos. Solo falta el IVA de la comisión del intermediario (ver abajo) |
| **PCR Vida** (Vida Deudores, 48897) | Prima (112.802.518.236,08) y comisión **iguales al centavo**. Comisión de recaudo +42.900,00 e IVA de recaudo +8.151,00 frente al RRC: es **exactamente** la diferencia que el Excel reporta entre RRC y `E1-A1`, o sea que QA coincide con `E1-A1` |
| **Generales** 54959, 54979, 55059 | Prima y comisión de recaudo **iguales**. En 55059 la prima es `prima + prima_vida` y el recaudo `costo_recaudacion + costo_recaudacion_vida`. Comisión de 54959 +0,08: la misma diferencia RRC frente a `E1-A1` |
| **Generales** 52897 | Prima +259,98 y comisión de recaudo +260,00 frente al RRC; la diferencia RRC frente a `E1-A1` del Excel es de 259,73 y 260,00 |
| **Generales, Pymes 55139** | **No llega**: no hay movimientos de Pymes en Bronce ni en Silver, aunque la Oracle tiene 45 operaciones de junio (Q4). Falta el 100 %: prima 56.404.532,99 |
| **Generales, IVA de comisión de recaudo** | El Silver da 0 en 52897 y 55059 (el Excel trae 633.791.830,29 y 68.391.290,21). ACSELE sí trae esos montos, pero con conceptos que el Silver no tiene en sus listas (detalle abajo) |
| **IVA de comisión del intermediario** | No existe como concepto en los movimientos de ACSELE en ningún caso. En Satélites el Excel es el 19 % de la comisión: lo calcula negocio |
| **Grano del Silver** (H6, ARQ-64) | En todos los productos hay una fila por (operación, póliza): **la prima no se repite**. Descartado en QA |

Cómo se reproducen las cifras de IVA de comisión de recaudo de Generales con los conceptos del Bronce de QA:
- 55059: `IVACOMISIONRECAUDOACC + DES + SUS` menos sus devoluciones (`…DEV`) = **68.391.290,21**, igual al RRC.
- 52897: `IVACOSTORECAUDOINCE + TERR + RC` − devoluciones + rehabilitaciones − anulaciones = 633.791.879,69; el RRC trae 633.791.830,29 (49,40 menos; *por explicar*).
- Total de `E1-A1` (640.578.074,35) = 52897 calculado así + 55059 solo `SUS` + Pymes: **exacto**. El RRC suma también `ACC` y `DES` de 55059; por eso los dos informes del negocio difieren en unos 63,7 millones.

**Conclusión:** en QA nuestra extracción y el Silver reproducen la fuente y cuadran con el Excel en Satélites, en PCR Vida y en la prima y el recaudo de Generales. Las diferencias reales y nuestras son tres, todas de Generales: (1) Pymes no se extrae; (2) el Silver no calcula el IVA de comisión de recaudo con los conceptos que trae ACSELE; (3) el IVA de la comisión del intermediario no sale de ACSELE (hay que definir con negocio si se calcula). El resto de las diferencias frente al RRC coincide con las que el propio Excel reporta entre RRC y `E1-A1`: hay que definir con negocio cuál es la cifra oficial.

Siguiente paso para Pymes: correr `diagnostico/diagnostico-pymes-55139.sql` en la Oracle de QA. Cuenta cuántas operaciones de Pymes sobreviven a cada paso de la consulta de movimientos.

## Oracle: Q5 y Q6 (2026-11-24, `ResultadosAWS\sql5.xlsx` y `sql6.xlsx`)

DEV y QA leen la misma Oracle (ACSSOP), así que este resultado vale para los dos.

**Q5 (movimientos por producto y concepto) frente al Bronce extraído:**

| Producto | Combinaciones en Oracle | Iguales en Bronce (filas y total al centavo) |
|---|---|---|
| 48897, 52897, 54899, 54919, 54939, 54959, 54979, 55039, 55059, 55060, 55099, 55119 | 293 | **293** |
| 55139 Pymes | 69 | **0** |

- **La extracción copia la Oracle sin perder nada** en los 12 productos.
- **Pymes sí tiene movimientos en la Oracle**, con la misma llave de póliza (`POLICYID`) que usa la extracción. Calculados desde la Oracle dan **exactamente el RRC**:
  - prima = `PRIMA{EEYE, INC, ROT, SUS, TERR}` menos sus `…DEV` = **56.404.532,99**;
  - comisión de recaudo = `COMISIONRECAUDO{…}` menos `…DEV` = **11.280.902,12**;
  - IVA de recaudo = `IVACOMISIONRECAUDO{…}` menos `…DEV` = **2.143.371,37**.
- Por lo tanto **Pymes se pierde en la extracción, no en la fuente.** Q5 cruza los movimientos solo con la operación y la póliza. La consulta de extracción exige además que existan PREPRODUCT con compañía `'Seguros Alfa S.A.'`, PREPOLICY, EVENTDCO y RISKUNITDCO para la operación. Una de esas condiciones descarta a Pymes; `diagnostico-pymes-55139.sql` dice cuál.
- **Segundo defecto de Pymes, en el Silver (PCE-1b):** las listas de conceptos de `silver_generales.py` solo tienen `PRIMASUS` y `COMISIONRECAUDOSUS`. Les faltan `PRIMAINC`, `PRIMATERR`, `PRIMAEEYE`, `PRIMAROT` (y sus `DEV`), lo mismo para la comisión de recaudo, y todo `IVACOMISIONRECAUDO…`. Aunque se arregle la extracción, el Silver tomaría solo la sustracción: 15.326.346,61 de prima (27 %) y 3.065.267,72 de recaudo.

**Q6 (movimientos por día de junio):** 30 días, todos con movimientos. Queda como línea base para compararla con la de PRD.

## Causa de PCE-1: Pymes sí se extrae, pero sin sus pólizas (2026-11-24)

`diagnostico-pymes-55139.sql` en la Oracle (ACSSOP):

| Paso | Operaciones de Pymes |
|---|---|
| 1. Operaciones de junio en estado 2 | 45 |
| 2 a 5. Con PREPRODUCT (compañía), PREPOLICY, EVENTDCO y RISKUNITDCO | 45 |
| 6 y 7. Con movimientos, por operación y por operación + póliza | 45 |
| Movimientos con la póliza de la operación | 468, total 188.114.812,96 |

**Ninguna condición de la consulta de movimientos descarta a Pymes.** El total es igual a Q5 de Pymes (468 filas, 188.114.812,96).

Contraste con la evidencia de la corrida (igual en DEV, lote `20261124060016`, y en QA, lote `20261122234023`):
- La tabla de movimientos de Generales tiene **7.176.727** filas según el runtime de la extracción.
- El cruce del Bronce con `agregatedpolicy` (que da el producto) suma **7.176.259**.
- La diferencia es **468**: los movimientos de Pymes **sí están en el Bronce**, pero su póliza no está en `agregatedpolicy`.

Por qué: `agregatedpolicy` es una tabla **compartida** del núcleo (alcance `COMUN`). Se extrae **una vez por lote** y la usan los tres casos (misma ruta, misma hora de escritura y 3.848.632 filas en los tres). Pero cada caso trae una consulta distinta para ella:

| Tabla compartida | Generales | PCR Vida y Satélites |
|---|---|---|
| `AGREGATEDPOLICY`, `PREPOLICY`, `EVENTDCO`, `RISKUNITDCO`, `PRODUCT` | Productos con **55139** | Productos **sin 55139** |
| `CONTEXTOPERATION`, `POLICYDCO` | Igual en los tres | Igual en los tres |

La copia compartida quedó sin las pólizas de Pymes, o sea, sin la versión de Generales. El Silver une los movimientos con la póliza para saber el producto, así que los 468 movimientos se pierden **sin error ni alerta**. Pymes se configuró solo en Generales; el núcleo compartido no se ajustó (ARQ-67).

- **Confirmado en Athena DEV** (2026-11-24; Bronce DEV, lote `20261124060016`), con `diagnostico/confirmar-pymes-bronce-dev.sql`. Sus tablas son de DEV y no sirve tal cual en QA ni PRD:
  - **Q-A:** 468 movimientos sin póliza, de 2 pólizas y 45 operaciones, total 188.114.812,96. Igual a la Oracle.
  - **Q-B:** el `agregatedpolicy` compartido tiene 52897 (203.007 pólizas), 54959 (95), 54979 (116), 55059 (14) y 55099 (2.714). **No tiene 55139.**
  - **Q-C:** los movimientos sin póliza son 69 conceptos, y los 69 son iguales a Q5 de Pymes en la Oracle (filas y total al centavo). Archivo: `ResultadosAWS\d91a6061-57ea-45a5-b02d-84316227e063.csv`.
  - Conclusión: todos los movimientos sin póliza son de Pymes, y todo Pymes está en el Bronce sin póliza.
- *Hipótesis no comprobada:* el código del extractor no está en este espacio, así que no se sabe por qué gana la versión de Vida y Satélites. Si depende del orden de ejecución, otra corrida (por ejemplo la de PRD) podría traer Pymes y dar cifras distintas.

**Corrección propuesta (con su configuración, en el mismo cambio):**
1. Consulta del núcleo compartido con **todos** los productos de los casos de la programación (unión), generada por la configuradora y no copiada a mano en cada malla. Como mínimo, agregar 55139 a las 5 tablas en `MALLA#PCR_VIDA` y `MALLA#SATELITES` de `cnf-mallas` y en sus JSON publicados.
2. La extracción falla si una tabla `COMUN` llega con consultas distintas entre casos (hoy el resumen ya muestra hashes distintos).
3. Control en la validación técnica: movimientos sin póliza > 0 → alerta con filas y monto.
4. PCE-1b (conceptos de Pymes en el Silver), para que el monto llegue completo.


DEV, lote `20261124060016`, diagnóstico `v2026-11-23.3`, sin errores. Comparado con QA (lote `20261122234023`):

| Capa | Resultado |
|---|---|
| Extracción (filas por tabla) | **Igual** a QA en los tres casos: Generales 203.689, Satélites 21.695 y Vida 3.703.183 filas de movimientos |
| Bronce (movimientos por producto y concepto) | **Igual**: 0 diferencias en 139, 117 y 37 combinaciones |
| Silver (totales por producto) | **Igual**: 0 valores distintos |
| Configuración | Solo difiere la programación (fecha y nombre del schedule) y el JSON de validación técnica |

**DEV y QA tienen hoy los mismos datos.** La diferencia de la primera corrida de DEV venía de haber extraído en otra fecha. Por eso, frente al Excel, DEV da exactamente lo mismo que QA (tabla de la sección anterior).

- **Pymes (55139) tampoco llega en DEV.** El filtro de la programación y la configuración de extracción de DEV ya incluyen 55139, pero no hay ni un movimiento de Pymes en Bronce ni en Silver. Que esté configurado no basta: el corte está en la consulta de movimientos o en la fuente (`diagnostico-pymes-55139.sql`).
- **Satélites, solo los 7 productos.** El Silver trae 54899, 54919, 54939, 55039, 55060, 55099 y 55119. Cuadran con el bloque de Satélites del resumen en todo, salvo el IVA de comisión del intermediario (lo calcula negocio). Frente al bloque de Vida del RRC, 54919 difiere en la comisión: 53.666,30 contra 57.720,10.
- **Agentes: coherentes con las comisiones.** En todos los productos, la comisión del intermediario está solo en filas con agente; las filas sin agente tienen comisión 0. Los productos sin comisión en el RRC (52897, 55059, 55060 y 55119) no tienen agente en ninguna fila. En Vida, el 99 % de las filas (69 % de la prima) no tiene agente y su comisión es 0.
- **Tomadores: la columna del Silver de Generales es una lista.** `silver_generales.py` arma el tomador con `concat_ws(",", collect_list(nombre))` por póliza, sin quitar repetidos. En 52897, 203.142 de 203.161 filas traen comas; en 54959, 54979 y 55059, todas. Se comprobó por huella que en 55059 un tomador del RRC aparece en el Silver como el mismo nombre repetido 46 veces. Por eso el cruce por nombre completo no sirve: la versión `2026-11-24.1` separa cada nombre y cruza también por acuerdo.
- **El tomador del RRC no es siempre el del Silver.** En 52897, el RRC trae 6 tomadores en 35 acuerdos (el tomador del acuerdo), mientras que el Silver trae unos 17.000 (el de cada póliza). Para 52897 el cruce válido es **por acuerdo**.

### Acuerdos y tomadores en DEV (versión 2026-11-24.1, evidencia `…dev-20261124T133737Z`)

**Por acuerdo (Generales frente al RRC):**

| Producto | Acuerdos RRC | En el Silver | Montos |
|---|---|---|---|
| 52897 | 35 | 35 | Iguales, salvo `INC-27192`: prima +259,98 y recaudo +260,00. **Toda la diferencia de 52897 está en ese acuerdo** |
| 54959 | 3 | 3 | Iguales, salvo comisión de `HGR-004M` +0,08 (la diferencia RRC frente a `E1-A1`) |
| 54979 | 1 | 1 | Iguales |
| 55059 | 4 | 4 | Iguales |
| 55139 (Pymes) | 2 | 0 | Faltan `PYM-001A` (37.446.071,04) y `PYM-001M` (18.958.461,95) |

Ningún acuerdo del Silver sobra ni falta, salvo los dos de Pymes. **El dinero está bien asignado a cada acuerdo.**

**Forma de la columna tomador (ARQ-66, cuantificada):**
- Generales: en todas las filas con coma es el **mismo nombre repetido**. Solo 4 filas de 52897 traen tomadores distintos.
- Repeticiones: hasta 224 veces en 52897 (promedio 42) y hasta 878 en 55059 (promedio 517).
- Satélites y Vida no tienen el problema: un solo nombre por fila.

**Tomadores (cada nombre por separado):**
- **55059:** los 3 tomadores cruzan con montos iguales.
- **52897:** 21 de los 35 pares acuerdo-tomador del RRC cruzan con montos iguales (salvo `INC-27192`). En los otros 14 acuerdos, el RRC pone toda la prima (1.875.071.447,69) a nombre del tomador del acuerdo. El Silver reparte exactamente la misma suma entre 8.812 tomadores de póliza. Es el mismo dinero con **otra definición de tomador**: pregunta para negocio.
- **54959 y 54979:** ningún nombre cruza tal cual, pero los montos sí. En 54959, 52 de 71 tomadores del RRC tienen en el Silver una sola pareja con la misma prima y comisión en el mismo acuerdo. Es el mismo tomador escrito distinto. La versión `2026-11-24.2` agrega huellas con las palabras en orden alfabético y sin tildes, además del largo y el número de palabras del nombre, para saber qué cambia sin sacar nombres.

**Resultado de la versión 2026-11-24.2 (evidencia `…dev-20261124T141403Z`):** ningún tomador más cruza al cambiar el orden de las palabras ni al quitar tildes. En las 52 parejas por monto de 54959, el largo del nombre difiere entre −22 y +11 caracteres, y solo 5 tienen el mismo largo. **No es el mismo nombre escrito distinto: es otra persona.** En Hogar y Mascotas, el "Tomador" del RRC no es el rol tomador del Silver, aunque la prima por póliza cuadre. La versión `2026-11-24.3` busca los nombres del RRC en los otros roles de persona del Silver de Generales (`asegurado`, `pagador`, `informacion_pagador_maestro`, `nombre_empleador`), siempre por huella.

**Resultado de la versión 2026-11-24.3 (evidencia `…dev-20261124T142345Z`): a qué rol del Silver llama "Tomador" el RRC**

| Producto | Rol del Silver que coincide | Pares acuerdo + nombre con la misma prima |
|---|---|---|
| 52897 Deudores | `informacion_pagador_maestro` (la entidad del acuerdo) | 29 de 35, más `INC-27192` con el mismo nombre y +259,98. En los 5 restantes (`INC-2184`, `INC-2185`, `INC-2194`, `INC-2195` e `INC-27480`) el acuerdo tiene un solo pagador maestro y el total cuadra: solo cambia cómo está escrito el nombre de 2 entidades |
| 55059 Protección Financiera | `informacion_pagador_maestro` y `tomador` (son el mismo) | 7 de 7 |
| 54959 Hogar | Ninguno | 0 de 71. Los roles tienen datos (tomador 68, asegurado 72, pagador 69 y empleador 11 nombres distintos), pero ningún nombre es el del RRC |
| 54979 Mascotas | Ninguno | 0 de 90 (tomador 84 y pagador 84 nombres; asegurado vacío) |

En Hogar y Mascotas la prima por póliza cuadra con un nombre distinto. Por huellas no se puede avanzar más: hay que comparar los nombres a mano, dentro de AWS y sin sacarlos (consulta en la sección siguiente).

**Consulta para comparar nombres (Athena, consola de DEV; el resultado no se guarda ni se copia):**
```sql
SELECT numero_poliza_matriz, nro_poliza,
       element_at(split(tomador, ','), 1)   AS tomador,
       element_at(split(asegurado, ','), 1) AS asegurado,
       element_at(split(pagador, ','), 1)   AS pagador,
       ROUND(SUM(prima) + SUM(COALESCE(prima_vida, 0)), 2) AS prima
FROM "niif-dev-silver-catalog-db"."periodo_negocio_202606_6e79eadfb3ce118951c520825050a9cf"
WHERE CAST(periodo_negocio AS varchar) = '202606' AND CAST(idproducto AS varchar) = '54959'
GROUP BY 1, 2, 3, 4, 5
HAVING ROUND(SUM(prima) + SUM(COALESCE(prima_vida, 0)), 2) IN (529684.71, 510348.53, 351072.47)
```
En el RRC (hoja `Data`, producto 54959) esas primas son de los tomadores de `HGR-001M` (529.684,71) y `HGR-004M` (510.348,53 y 351.072,47).

**Resultado de la comparación (2026-11-24):**
- Las 3 pólizas traen el mismo nombre en tomador, asegurado y pagador, y la prima es igual al centavo a la del RRC.
- El nombre del RRC es **otra persona**: 0 palabras en común y similitud de texto de 0,22 a 0,31.
- Las huellas de esos nombres son exactamente las de la evidencia. Esto confirma que el cálculo de huellas en Athena y en local coincide.

**Hipótesis:** la Oracle de DEV y QA (ACSSOP) tiene enmascarados los nombres de personas naturales, como corresponde a un ambiente no productivo (Ley 1581), y el RRC viene de producción. Encaja con todo lo medido:
- cuadran los nombres de entidades (el pagador maestro de 52897 y 55059);
- no cuadra ningún nombre de persona natural (Hogar y Mascotas, en ningún rol);
- pólizas, acuerdos y montos cuadran al centavo.

Si se confirma, los nombres de personas solo se pueden validar en PRD (sección `terceros` del diagnóstico). En DEV y QA, la validación de tomadores se hace por acuerdo, póliza y monto.

## Cómo se ejecuta

Todo es de solo lectura. La única escritura es la de Athena, que guarda el resultado de cada consulta en el bucket de resultados de su workgroup.

1. **Referencia (local).** Convierte los Excel en totales por producto:
   ```
   python diagnostico/validar_referencia.py --rrc "RRC SIN CONTABILIDAD ALFA 202606.xlsx" --resumen "Resumen emision 202606.xlsx" --salida referencia_esperada.json
   ```
2. **Diagnóstico en cada ambiente (CloudShell, us-east-1).** Desarrollo lo corre en DEV; DevOps lo corre en QA y en PRD. Se suben los dos archivos `diag-emisiones-v2.sh` y `diag_emisiones_v2.py` y se ejecuta:
   ```
   bash diag-emisiones-v2.sh qa 202606 primary
   bash diag-emisiones-v2.sh prd 202606 primary
   ```
   Cada corrida deja `evidencia-pcr-emisiones-<ambiente>-<fecha>.tar.gz`. Si el ambiente de producción no se llama `prd` en los recursos, usar su nombre real.
3. **Oracle (DBA, en la base ACSELE de QA y en la de PRD).**
   - `diagnostico/conciliacion-oracle-acsele.sql`: las mismas consultas en las dos bases. Exportar cada bloque a CSV (`Q1.csv` a `Q6.csv`).
   - `oracle/<caso>_conteos_por_tabla.sql` y `oracle/<caso>_movimientos_por_producto_concepto.sql` dentro de cada evidencia: son las SQL que usó la extracción **en ese ambiente**. Comparan Oracle con lo que llegó a Bronce.
4. **Comparar (local).** Descomprimir las dos evidencias, poner los CSV de Oracle en `<evidencia>/oracle_resultados/` (mismo nombre en los dos ambientes) y correr:
   ```
   python diagnostico/comparar_ambientes.py --a <evidencia-qa> --b <evidencia-prd> --referencia referencia_esperada.json --mapeo diagnostico/mapeo-columnas-propuesta.json --salida informe-comparacion.md
   ```
   El informe abre con un veredicto por capa: fuente Oracle, código, orquestación, configuración, extracción, Bronce y Silver. **La primera capa que difiere es donde nace la diferencia.** Si la primera es la fuente Oracle, la diferencia viene de los datos y no de los jobs.

### Acuerdos, tomadores y agentes (sección `terceros`, versión 2026-11-24.3)

El diagnóstico consulta en el Silver preparado, por producto:
- completitud de tomador, agente e identificación del tomador;
- totales por acuerdo (`numero_poliza_matriz` y `nombre_acuerdo`; el código del acuerdo no es dato personal);
- forma de la columna tomador (mismo nombre repetido o varios tomadores distintos en la fila);
- totales por cada tomador de la lista, por separado, y por agente;
- huellas alternas de cada tomador (palabras en orden alfabético, y además sin tildes), largo y número de palabras, para encontrar el mismo tomador escrito distinto;
- solo en Generales (donde hay RRC): huellas de los otros roles de persona (asegurado, pagador, pagador maestro, empleador), para saber a qué rol llama "Tomador" el RRC.

**No saca nombres de AWS:** usa una huella SHA-256 del nombre normalizado (espacios colapsados, sin bordes, mayúsculas). Luego, en local:
```
python diagnostico/validar_terceros.py --rrc "RRC SIN CONTABILIDAD ALFA 202606.xlsx" --evidencia <carpeta evidencia> --salida informe-terceros.md
```
- Acuerdos: se cruzan con la columna `Acuerdo` del RRC en prima, comisión del intermediario y comisión de recaudo (solo Generales; el RRC no trae Vida ni Satélites). El script elige solo la columna del Silver que coincide.
- Tomadores: mismo cruce por producto y tomador, y por producto, acuerdo y tomador. Muestra los que están solo en un lado y los que tienen montos distintos, con el acuerdo del RRC para ubicarlos. `--mostrar-nombres` los muestra solo en pantalla.
- Agentes: el RRC no tiene columna de agente; se informa cuántos hay por producto y cuánta prima queda sin agente.

Para repetir solo esta parte: `SECCIONES=configuracion,terceros bash diag-emisiones-v2.sh dev 202606 primary`.

## Permisos que necesita el diagnóstico (lectura)

- Glue: `GetJob`, `GetJobRuns`, `GetConnection` (se pide sin clave), `GetDatabases`, `GetTables`.
- S3: `GetObject`, `ListBucket` y `ListBucketVersions` sobre `niif-<amb>-assets`, `alfa-datalake-<amb>-bronce` y `alfa-datalake-<amb>-silver`; escritura solo en el bucket de resultados del workgroup de Athena.
- Lambda: `GetFunction` sobre `niif-<amb>-configuration-general`.
- Step Functions: `ListStateMachines`, `DescribeStateMachine`, `ListExecutions`, `GetExecutionHistory`.
- DynamoDB: `GetItem` y `Query` sobre `niif-<amb>-prg-programaciones` y `niif-<amb>-cnf-mallas`.
- Athena: `StartQueryExecution`, `GetQueryExecution`, `GetQueryResults` en el workgroup indicado.
- KMS: `Decrypt` si los buckets o las tablas están cifrados con llave propia.

Si falta un permiso, esa sección queda anotada en `resumen.json` como error y las demás siguen.

## Datos personales

- El diagnóstico no consulta columnas de personas: solo cuenta filas y suma montos por producto, concepto y periodo.
- La evidencia trae nombres de recursos, rutas y la URL JDBC de la conexión (sin usuario ni clave).
- El archivo `RRC SIN CONTABILIDAD ALFA 202606.xlsx` (hoja `Data`) trae nombres de tomadores, y los `.msg` traen nombres y correos. No deben quedar en el repositorio.

## Archivos

| Archivo | Para qué |
|---|---|
| `diagnostico/validar_referencia.py` | Valida los Excel y genera los totales esperados |
| `diagnostico/diag-emisiones-v2.sh` y `diag_emisiones_v2.py` | Diagnóstico de solo lectura por ambiente |
| `diagnostico/conciliacion-oracle-acsele.sql` | Comparación de la fuente Oracle entre QA y PRD |
| `diagnostico/comparar_ambientes.py` | Veredicto por capa e informe |
| `diagnostico/mapeo-columnas-propuesta.json` | Equivalencia entre columnas del Excel y del Silver (validada en QA donde se indica) |
| `diagnostico/validar_terceros.py` | Cruza acuerdos y tomadores del Silver con el RRC por producto (tomadores por huella del nombre) y resume la completitud de agentes |
| `correo-entrega-evidencias.md` | Correo final a negocio con los 3 hallazgos, sus cifras y la lista de evidencias (respaldo) |
| `Tablero-PCR-Emisiones-202606.xlsx` | Tablero para presentar: RRC frente al sistema por caso, producto, métrica y acuerdo; Oracle frente a la extracción; agentes, tomadores y hallazgos. Sin datos personales |
| `diagnostico/diagnostico-pymes-55139.sql` | Encuentra en qué paso de la consulta de movimientos se pierde Pymes (resultado: en ninguno) |
| `diagnostico/confirmar-pymes-bronce-dev.sql` | Athena DEV: movimientos de Generales sin póliza en el `agregatedpolicy` compartido (Pymes) |
