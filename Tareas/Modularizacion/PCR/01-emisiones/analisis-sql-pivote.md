# Análisis de las SQL de extracción de Emisiones frente al pivote `CONTEXTOPERATION`

Fecha: 26 nov 2026. Fuente: las 129 consultas que arma el job (núcleo común y ramas de PCR Vida, Generales y Satélites) con la configuración de DEV del periodo 202606. Se cruzaron con:
- los tiempos y las filas de la corrida del 24 nov (lote `20261124060016`);
- el tamaño de cada tabla en Oracle (`consulta2-tamanos.xlsx`) y sus índices (`consulta1-llaves-indices.xlsx`).

Detalle tabla por tabla: `validaciones/salidas/analisis-sql-pivote-20261126.csv`. Script: `validaciones/scripts/analizar-sql-pivote.py`.

## Qué se encontró

**1. Casi todas las consultas vuelven a filtrar el pivote.**
- 116 de las 129 consultas leen `CONTEXTOPERATION`; 10 de ellas, 2 o 3 veces.
- Las 115 tablas distintas que se extraen suman 372 minutos de lectura y escritura en la corrida del 24 nov.

**2. El filtro es el mismo "contexto de negocio" repetido.**
- Es un join de 6 a 8 tablas grandes: `CONTEXTOPERATION` (331 M filas), `POLICYDCO` (331 M), `AGREGATEDPOLICY`, `PREPRODUCT`, `EVENTDCO` (1.926 M), `RISKUNITDCO` (331 M), y a veces `PRODUCT` y `PREPOLICY`, con periodo, estado 2 y productos del caso.
- Por caso hay solo dos variantes de ese contexto:

| Caso | Variante principal | Variante con `PREPOLICY` |
|---|---|---|
| Satélites | 38 consultas | 6 |
| Generales | 32 consultas | 6 |
| PCR Vida | — | 7 |

- Oracle resuelve ese join completo en cada consulta, aunque devuelve siempre el mismo conjunto: unos 3,9 millones de operaciones del periodo.

**3. El tiempo no depende del tamaño de la tabla, sino del contexto que se recalcula.**

| Tamaño de la tabla en Oracle | Tablas de rama | Tiempo hoy | Filas que salen |
|---|---|---|---|
| hasta 100 mil filas | 46 | 56 min | 7.186 |
| 100 mil a 1 millón | 9 | 10 min | 49.369 |
| 1 a 10 millones | 14 | 56 min | 1,2 M |
| 10 a 100 millones | 17 | 66 min | 31,8 M |
| más de 100 millones | 22 | 152 min | 84,7 M |

Ejemplos de tablas pequeñas que tardan mucho:

| Tabla | Filas en Oracle | Tiempo | Filas que salen |
|---|---|---|---|
| `PREAGREEMENT` | 1.313 | 19 min | 154 |
| `STPS_AGREEMENTSTATICDAT` | 387 | 9 min | 45 |
| `STTS_ROLE` | 21 | 6 min | 8 |

**4. Cómo se une cada tabla al contexto** (tablas de rama que usan el pivote):
- **45 tablas** se unen directo por una llave del contexto: `OPERATIONPK`, `PK` = `RISK_DCOID` / `POLICY_DCOID` / `IDDCOEVENT`, `STATIC` = `PRODUCTID` / `POLICY_ID`, `AGREGATEDPARENTID` = `AGREGATEDPOLICYID`.
- **45 tablas** se unen a través de otra tabla que también se extrae: participaciones `P`/`D`, objetos asegurados `I`, relación de convenios `R`/`X`.
- **El resto** usa SQL propia (`WITH`, `GROUP BY MIN`) o no usa el pivote: `PRETHIRDPARTY`, `INSURANCEOBJECTDCO`, `OAVDEUDORES`, `URVDEUDORES`, `OPENITEMREFERENCE`, `UAADETAIL`, `PROPERTY` y tablas de catálogo.

## Propuesta (pendiente de aprobación)
Todo por configuración y tabla por tabla. Por defecto, cada tabla sigue con su SQL de hoy, así que nada cambia hasta que se active en la configuración.

1. **Contexto una vez por caso.** Una consulta a Oracle por variante, cuyo resultado (unos 3,9 M filas) queda en memoria de Spark para todo el caso. Usa el clúster que hoy está ocioso.
2. **Tablas pequeñas: tabla completa y filtro en Spark.** Es para las tablas de hasta un tamaño configurable (medido en Oracle, no por la etiqueta `tipo_tabla`): 55 tablas que hoy suman 66 min. Se leen enteras, con solo sus filtros propios (`STATUS=1`, `AMMOUNT<>0`…), y se filtran en Spark con el mismo cruce de la SQL. Leer 1.313 filas toma segundos.
3. **Tablas medianas y grandes: rangos de llave y filtro exacto en Spark.** Se leen por tramos de la llave indexada (`OPERATIONPK`, `PK`…), calculados desde el contexto, y se filtran exacto en Spark. Es la opción D ya acordada. Antes hay que medir qué tan densos son los IDs del periodo, con los datos de Bronce en DEV, sin tocar Oracle.
4. **Las tablas con SQL propia** se dejan como están en esta fase.

**Cómo se prueba que las cifras no cambian:**
- Para cada tabla que cambie de estrategia, una prueba de equivalencia en DEV: se leen ambas formas del mismo periodo y se comparan filas, columnas y un hash del contenido.
- La tabla pasa a la estrategia nueva solo si coincide.

**Estimado:** las tablas pequeñas pasan de 66 min a pocos minutos. El resto depende de la densidad de las llaves (*hipótesis*, por medir).

**Recomendaciones para Oracle** (se entregan al final; el job no depende de ellas):
- índices en las llaves de unión que no tienen índice (9 llaves sin índice y 6 que solo están como 2.ª a 4.ª columna);
- poda por partición en `CONTEXTOPERATION`.
