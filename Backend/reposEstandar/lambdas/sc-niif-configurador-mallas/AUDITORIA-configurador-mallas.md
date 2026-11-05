# Auditoría: `sc-niif-configurador-mallas` frente a las seis lambdas `niif-dev-configuration-<malla>`

**Fecha:** 2026-11-04. **Versión auditada:** 1.1.0 (catálogo DEV 2.0.0).
**Objetivo:** comprobar que la lambda general hace todo lo que hacen las seis lambdas actuales, que sigue el estándar del proyecto (`Docs\08-estandares-desarrollo\estandar-lambdas-python.md`) y que no tiene valores quemados.

**Resultado:** la lambda nueva cubre las acciones de las seis. Había 8 diferencias: 6 se corrigieron en la versión 1.1.0 y 2 se dejaron a propósito (filas 7 y 8 de la tabla del punto 3). Hay 245 pruebas que pasan, con 99 % de cobertura de ramas. Entre ellas hay **36 pruebas de equivalencia**, que ejecutan el código de cada lambda actual y comparan sus efectos con los de la nueva. Quedan pendientes que dependen de AWS (punto 6).

---

## 1. Qué se comparó y con qué límites

| Lambda actual | Código usado en la comparación | Configuración desplegada |
|---|---|---|
| `niif-dev-configuration-auxilios-funerarios` | `conf_auxilios.py` (**el desplegado**) | Sí |
| `niif-dev-configuration-primas-rentas` | `lambda_function.py`, copia de la versión anterior | Sí |
| `niif-dev-configuration-primas-endosos` | Copia de la versión anterior | **No** |
| `niif-dev-configuration-primas-arl` | Copia de la versión anterior | Sí |
| `niif-dev-configuration-reserva-matematica-rvi` | Copia de la versión anterior | **No** |
| `niif-dev-configuration-reserva-matematica-arp` | Copia de la versión anterior | Sí |

- Las cinco copias no son el código desplegado (ver `ORIGEN.md` de cada carpeta). Los handlers desplegados (`conf_rentas.py`, `conf_arl.py`, `conf_res_mat_arp.py`) y sus variables `PRG_PK`/`PRG_SK` muestran que la versión real ya lee el periodo de PRG, como auxilios.
- El código de `niif-dev-manager-configuration` no está en el repositorio (`PENDIENTE.md`), así que no se pudo verificar qué evento envía.

---

## 2. Funcionalidades: lambdas actuales frente a la nueva

**Verificado por equivalencia** significa que la prueba ejecuta el código actual y el nuevo con los mismos datos y obtiene el mismo resultado.

| # | Funcionalidad de las lambdas actuales | Mallas | En la nueva | Cómo se verificó |
|---|---|---|---|---|
| 1 | Leer todos los registros CNF de la malla con lectura consistente | 6 | Sí (además pagina) | Equivalencia |
| 2 | Exigir el número exacto de registros CNF (rentas y RVI 9, ARL, ARP y auxilios 8, endosos 7) | 6 | Sí: faltantes y sobrantes; los ciclos salen de `totalCiclos` | Unitarias |
| 3 | Leer el periodo de la programación (PRG) y rechazar un evento con otro periodo | Auxilios (y las desplegadas, por sus variables) | Sí, para las 6 | Unitarias y equivalencia (auxilios) |
| 4 | Aplicar el periodo a `controlExtraccion` (cambio del periodo anterior y marcadores `${...}`) | 6 | Sí | Equivalencia |
| 5 | Ajustar los límites de fecha de los `where` | 6, con reglas distintas | Sí, con columnas y operadores por malla en el catálogo | Equivalencia (las 6 reglas reproducidas) |
| 6 | Ajustar `PERIODO = 'YYYYMM'` en los `where` | Auxilios | Sí (`columnaPeriodo`) | Equivalencia |
| 7 | Ajustar y validar el periodo de la segmentación horizontal | Auxilios | Sí (`ajustarSegmentacion`) | Equivalencia |
| 8 | Construir el JSON de extracción (ciclos, segmentos y tablas ordenados; estructura en PRS `SRC`) | 6 | Sí | Equivalencia |
| 9 | Tablas con `where` y `dependencia` aunque sean nulos | Rentas, endosos, ARL y auxilios | Sí (`tablaExtraccion`) | Equivalencia |
| 10 | Tablas sin nulos y con `reutilizacion_compartida` | RVI y ARP | Sí (`tablaExtraccion`) | Equivalencia |
| 11 | Construir los JSON de validación y SCD (estructura Gold en PRS `GLD`) | 6 | Sí | Equivalencia |
| 12 | Leer la plantilla de remediación y validar `nombre_caso` | 6 | Sí | Equivalencia |
| 13 | Estado de remediación fijo `PROVISIONAL_SIN_REGLAS_TRANSFORMATIVAS` | Rentas, endosos, ARL y auxilios | Sí (`remediacion`) | Equivalencia |
| 14 | Estado según haya reglas transformativas (`DEFINIDA_...` o `PROVISIONAL_...`) | RVI y ARP | Sí (`remediacion.estadoConReglas`) | Equivalencia y unitarias |
| 15 | Validar los JSON generados (no vacíos, sin marcadores, periodo coherente) | 6 | Sí | Unitarias |
| 16 | Actualizar el periodo en CNF con transacción y condición sobre el valor anterior | 6 | Sí (adaptador real) | Equivalencia: misma transacción |
| 17 | Publicar los 4 JSON en S3 con metadatos `origen` y `malla` | 6 | Sí (agrega `ejecucion-id`) | Equivalencia |
| 18 | `ejecutar=true` exige `publicar=true`; `ejecutar` toma el valor de `publicar` por defecto | 6 | Sí | Unitarias |
| 19 | Arrancar la Step Function de la malla | 6 | Sí (ARN armado con la cuenta y región de la función) | Equivalencia |
| 20 | Entrada de la Step Function con `rutaConfigExtraccion` | Rentas, endosos, ARL y auxilios | Sí (`entradaStepFunction`) | Equivalencia |
| 21 | Entrada sin `rutaConfigExtraccion` | RVI y ARP | Sí | Equivalencia |
| 22 | `fecDesde` y `fecHasta` en la entrada | Auxilios | Sí | Equivalencia |
| 23 | `origen` por defecto `CONFIGURADOR` y `modoEjecucion` por defecto `NORMAL` | 6 | Sí (`valoresPorDefecto`) | Equivalencia |
| 24 | Registrar la ejecución en `ejc-ejecuciones` (`INICIADA`) | 6 | Sí | Equivalencia |
| 25 | Malla tomada de `MALLA_PK` | 6 | Sí (para la migración T2-A); en T2-B llega en el evento | Unitarias |
| 26 | Respuesta (`estado`, `malla`, `periodoNegocio`, `registrosCNF`, `publicados`, `executionArn`, `ejcPK`, ...) | 6 | Sí | Equivalencia |

---

## 3. Hallazgos y cómo quedaron

| # | Hallazgo (versión 1.0.0) | Impacto | Estado en 1.1.0 |
|---|---|---|---|
| 1 | RVI y ARP aceptan `DEFINIDA_CON_REGLAS_TRANSFORMATIVAS` cuando hay reglas; el catálogo solo permitía `PROVISIONAL_...` | RVI y ARP fallaban con `CFG-CNF-007` si tenían reglas | **Corregido:** `remediacion.estadoSinReglas` / `estadoConReglas` |
| 2 | RVI y ARP copian `reutilizacionCompartida` y omiten `where`/`dependencia` nulos | El JSON de extracción cambiaba y Glue podía perder la reutilización | **Corregido:** `tablaExtraccion.camposOpcionales` / `omitirNulos` |
| 3 | RVI y ARP no envían `rutaConfigExtraccion` a la Step Function | Entrada distinta | **Corregido:** `entradaStepFunction.incluirRutaConfigExtraccion` |
| 4 | `origen` por defecto era `INVOCACION_DIRECTA` (antes, `CONFIGURADOR`) y estaba quemado en el código | Cambiaba la Step Function y el registro EJC; valor de negocio quemado | **Corregido:** `valoresPorDefecto` en el catálogo |
| 5 | Los operadores de fecha eran fijos (`>=`, `<`, `<=`) | No podía reproducir la regla de cada malla | **Corregido:** `periodo.operadoresFecha` |
| 6 | La segmentación se ajustaba siempre | No podía reproducir las mallas que no la ajustan | **Corregido:** `periodo.ajustarSegmentacion` |
| 7 | Se aceptaban registros CNF extra que no fueran ciclos | Menos estricta que las seis | **Corregido:** se rechaza cualquier sobrante |
| 8 | `estructurasPRSValidadas` no estaba en la respuesta | Respuesta distinta | **Corregido:** se calcula (antes era un número fijo en cada copia) |
| 9 | README y guía con 256 MB; las seis usan 512 MB | Riesgo de tiempo o memoria | **Corregido:** 512 MB |

**Diferencias que se mantienen a propósito** (detalle en el README):
- `publicar` y `ejecutar` deben ser booleanos;
- el nombre de la ejecución es determinista;
- se agrega `ejecucionId`;
- se retira `comparaciones`, que siempre salía vacía;
- el registro EJC no se sobrescribe;
- `modoEjecucion` solo acepta los valores de `modosEjecucion`;
- hay idempotencia por clave.

---

## 4. Estándar del proyecto

| Punto del estándar | Estado |
|---|---|
| Nombres (repositorio `sc-...`, paquete, recursos AWS, artefacto semver) | Cumple |
| Estructura (`config/`, `eventos/`, `scripts/`, `src/<paquete>/{dominio,aplicacion,adaptadores/{entrada,salida},configuracion}`, `tests/`) | Cumple |
| Regla de dependencias entre capas | Cumple: `tests/test_arquitectura.py` |
| Sin valores quemados | Cumple. En `src/` solo quedan contratos con nombre (prefijos de llave `MALLA#`, `EJC#` y `EJECUCION#`, SK `SRC` y `GLD`, marcadores `${...}`, campo `reglasTransformativas`). Todo lo de negocio está en el catálogo y lo de infraestructura en variables de entorno; la cuenta y la región salen del ARN de la función |
| "El motor no decide, obedece" | Cumple: el catálogo y el entorno se validan al arrancar, sin valores implícitos (`CFG-CNF-001` y `CFG-CNF-002`) |
| Taxonomía de errores, logs JSON con `ejecucion_id` y métricas EMF | Cumple |
| Idempotencia | Cumple |
| Pruebas: unitarias, caso de uso con dobles, adaptadores con `Stubber`, handler y arquitectura | Cumple, y se agrega la equivalencia |
| Cobertura mayor al 80 % | 99 % de ramas (mínimo configurado 85 %) |

---

## 5. Pruebas

```
pytest --cov      ->  245 passed, cobertura 99.13 %
```

`tests/test_equivalencia_lambdas_actuales.py` ejecuta 6 comparaciones por malla, 36 en total:
- los 4 JSON;
- los metadatos de S3;
- la transacción sobre CNF;
- la entrada de la Step Function;
- el registro EJC;
- la respuesta.

Para comprobar que detecta diferencias, se probaron tres cambios a propósito:

| Cambio provocado | Resultado de la prueba |
|---|---|
| Reglas de periodo de auxilios aplicadas a rentas | Detecta que `config_extraccion.json` cambia |
| RVI sin `estadoConReglas` | Falla con `CFG-CNF-007` |
| ARP sin `omitirNulos` | Detecta que `config_extraccion.json` cambia |

---

## 6. Pendientes antes de reemplazar las lambdas

1. **Bajar el código desplegado** de las cinco mallas `POR_CONFIRMAR` y del manager (`recolectar-faltantes.sh`), ponerlo en sus carpetas y volver a correr la prueba de equivalencia. Si la regla de periodo real de alguna malla difiere de la de auxilios, basta con ajustar su bloque `periodo` en el catálogo.
2. **Bajar la configuración desplegada** de endosos y RVI.
3. **Confirmar con el código del manager** qué `modoEjecucion` y qué tipos de `publicar` y `ejecutar` envía.
4. **Hacer la prueba de equivalencia en DEV con datos reales:** para el mismo periodo, los 4 JSON de la nueva deben ser iguales a los de la lambda actual. Comparar el JSON ya leído, no el texto: el orden de las llaves puede cambiar.
5. **Crear en el IaC** la tabla de idempotencia, el rol y las alarmas.
