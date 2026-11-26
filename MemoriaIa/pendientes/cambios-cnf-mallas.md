---
tipo: pendiente
estado: vigente
actualizado: 2026-11-26
---
# Cambios acumulados de `cnf-mallas` y la configuradora

> Lista única de lo que hay que cambiar en DynamoDB `cnf-mallas`, en la lambda configuradora y en los JSON de `cfg-datalake-niif-config-s3` por los cambios de código. Se acumula aquí y se aplica cuando se toque ese repo. Nada de esto está aplicado.

## Por qué van juntos
- La fuente es DynamoDB (plantillas con `${FECHA_INICIO}`, `${PRODUCTOS}`…); los JSON de S3 son salida renderizada. Los ítems no están en ningún repo ([[modularizacion-extraccion]] §Configuración real).
- La configuradora (`sc-datalake-niif-configuration-general`, `conf_emisiones.py`) reescribe el JSON y **descarta las claves que no conoce**: cada clave nueva va a `cnf-mallas` y a la configuradora en el mismo cambio, o se pierde en la siguiente corrida ([[preferencias-usuario]], regla no negociable).
- Al aplicar: respaldo del ítem antes, cambio en DEV, corrida de prueba y luego QA/PRD por DevOps.

## Emisiones (`MALLA#PCR_VIDA`, `MALLA#GENERALES`, `MALLA#SATELITES`)
| # | Dónde | Cambio | Origen | Estado |
|---|---|---|---|---|
| 1 | `rutas` | Publicar `s3_silver_base` en Generales y Satélites (hoy se deduce de Bronce y sale `ADVERTENCIA_RUTA_SILVER_DERIVADA`) | Fase 2 | pendiente |
| 2 | `rutas` | Mantener `s3_bronze_comun` en los 3 casos: ahora es obligatoria, sin valor por defecto | Fase 2 | ya publicada; no quitar |
| 3 | `control_extraccion` | Dejar `permitir_reanudacion` explícito (el valor por defecto pasó a `false`; hoy DEV lo trae en `true`) | Fase 2 | ya publicada; decidir el valor |
| 4 | Núcleo común | Igualar en Generales las 5 tablas del núcleo que difieren (`AGREGATEDPOLICY`, `PREPOLICY`, `EVENTDCO`, `RISKUNITDCO`, `PRODUCT`); incluir 55139 en el núcleo de PCR Vida y Satélites, o núcleo único con la unión de productos (ARQ-67) | Fase 2 | pendiente |
| 5 | `control_extraccion` | `nucleo_comun_estricto`: `true` solo después del punto 4 | Fase 2 | pendiente |
| 6 | Raíz, `control_extraccion` y tablas | Dejar de publicar claves sin efecto: `ventana_extraccion`, `filtros_efectivos`, `reglas_arquitectura`, `salida_runtime`, `modo_ejecucion`, `persistir_dataframe` y, por tabla, `fetchsize`, `alcance`, `tipo_tabla`, `orden_fase`, `oracle_parallel` | Fase 2 | pendiente; *propuesta:* `fetchsize` por tabla podría volverse efectivo en la fase 4 |
| 7 | `control_extraccion` | Claves de concurrencia (valores de hoy por defecto): `max_concurrencia_jdbc` 3, `modos_paralelos` [`PARALELO`, `PARALLEL`], `max_particiones_jdbc` 16, `casos_en_paralelo` false, `max_sesiones_oracle` (sin tope), `pausa_entre_fases_segundos` 0 | Fase 4 (`dba617a`) | pendiente; valores a acordar con Víctor |
| 7b | Caso y tablas de rama | `contextos_spark` por caso (SQL del contexto de negocio, `max_filas`) y `filtro_spark` por tabla (`modo`, `consulta_lectura`, `cruces`, `distinct`, `max_filas`). Primero en `SOMBRA`; `ACTIVO` solo con equivalencia probada en DEV | Fase 4 (`85ca142`) | pendiente; bloques por generar desde el análisis de SQL |
| 8 | Configuradora | ID con formato `V2` retrocompatible (`ejecucionId` y `formatoIdEjecucion` opcionales en el evento; por defecto el formato actual) | Fase 1 | pendiente; diseño en [[modularizacion-extraccion]] |
| 9 | Configuradora | No mezclar estado de la corrida (filtros efectivos, `idEjecucion`, `loteCargueId`) con la configuración en el ítem `EXTRACCION` | Hallazgo 25 nov | *propuesta* |

Relacionado, pero no es `cnf-mallas` (DevOps): Step Function `niif-snf-emisiones` con `ejecucionId` y `--EJECUCION_ID`, y el empaquetado del job ([[pendientes]]).

## Otras mallas
| Malla | Cambio | Estado |
|---|---|---|
| ARL | `where` con `PERIODO = 'P'` en vez de `<=` ([[fix-scd-freno-periodo]]) | Git cambiado; QA y PRD pendientes |
