---
tipo: pendiente
estado: vigente
actualizado: 2026-11-26
---
# Cambios acumulados de `cnf-mallas` y la configuradora

> Lista única de lo que hay que cambiar en DynamoDB `cnf-mallas`, en la lambda configuradora y en los JSON de `cfg-datalake-niif-config-s3` por los cambios de código. Se acumula aquí y se aplica cuando se toque ese repo. Nada de esto está aplicado.

## Por qué van juntos
- La fuente es DynamoDB (plantillas con `${FECHA_INICIO}`, `${PRODUCTOS}`…); los JSON de S3 son salida renderizada. Los ítems no están en ningún repo ([[modularizacion-extraccion]] §Configuración real).
- La configuradora (`sc-datalake-niif-configuration-general`, `conf_emisiones.py` `build_config`, rama `main`) reescribe el JSON en cada corrida ([[preferencias-usuario]], regla no negociable). Qué conserva (revisado 2026-11-26):
  - Copia enteros (con `deepcopy`) `rutas`, `controlExtraccion`, `fuente` (ítem `EXTRACCION`) y `nucleoComun` y `ciclos` (ítem `CICLO#001`): **una clave nueva dentro de esos bloques o dentro de una tabla pasa sin tocar código**.
  - La raíz del JSON es una lista fija: **una clave nueva en la raíz se pierde** si no se agrega a `build_config`.
  - Solo renderiza marcadores (`${FECHA_INICIO}`, `${PRODUCTOS}`…) en `consulta_sql` de cada tabla (`apply_sql_filters`); cualquier otro campo con SQL queda sin renderizar.
- Ítems por caso: `CICLO#001`, `EXTRACCION`, `GENERAL`, `REMEDIACION`, `SCD`, `VALIDACION`. La extracción solo usa `EXTRACCION` y `CICLO#001`; los otros cuatro son de los demás jobs.
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
| 7b | Caso y tablas de rama | `contextos_spark` por caso (SQL del contexto de negocio, `max_filas`) y `filtro_spark` por tabla (`modo`, `consulta_lectura`, `cruces`, `distinct`, `max_filas`). Primero en `SOMBRA`; `ACTIVO` solo con equivalencia probada en DEV | Fase 4 (`85ca142`) | pendiente. **Requiere código en la configuradora:** `contextos_spark` va en la raíz (se pierde) y las SQL de `contextos_spark.consulta_sql` y `filtro_spark.consulta_lectura` llevan marcadores que hoy no se renderizan. Las claves del 7 viven en `control_extraccion` y pasan solas |
| 8 | Configuradora | ID con formato `V2` retrocompatible (`ejecucionId` y `formatoIdEjecucion` opcionales en el evento; por defecto el formato actual) | Fase 1 | pendiente; diseño en [[modularizacion-extraccion]] |
| 9 | Configuradora | No mezclar estado de la corrida (filtros efectivos, `idEjecucion`, `loteCargueId`) con la configuración en el ítem `EXTRACCION` | Hallazgo 25 nov | *propuesta* |

## Cómo se entrega (*propuesta*, en espera de DevOps)
- **Bloqueo:** no sabemos si los ítems tienen repo ni cómo se cambian en QA y PRD. Pregunta en [[preguntas-abiertas]] (§`cnf-mallas`). No armar nada hasta la respuesta (usuario, 2026-11-26).
- Tabla `niif-<amb>-cnf-mallas` (PK `MALLA#<CASO>`, SK por ítem), creada vacía por la IaC (`stacks/database/parameters.tf:126`). DEV: 67 ítems, PITR activado. Copia de los ítems de Emisiones: `Tareas/Modularizacion/PCR/01-emisiones/validaciones/salidas/cnf-mallas-emisiones-dev-20261125-2309/`.
- Si no hay repo: paquete en `Tareas\...\01-emisiones\entregables\cnf-mallas\` con `README.md`, `cambios.json` (cambios puntuales por caso e ítem, no el ítem completo) y scripts `aplicar` (sin `--aplicar` respalda y muestra la diferencia; con `--aplicar` escribe y verifica) y `revertir`. Cambios puntuales porque QA y PRD pueden tener ítems distintos de DEV: `update-item SET` sobre `controlExtraccion.*` y `rutas.*`; `filtro_spark` ubicado por `tabla_relativa` y solo si la `consulta_sql` coincide con la usada para generar el bloque.
- Dos entregas: **A** = puntos 1, 3 y 7 (sin código en la configuradora; puede ir con el PR de las fases 1–3). **B** = 7b y 6, después del PR de la configuradora. 4 y 5 esperan la decisión del núcleo común.
- No toca `sc-datalake-niif-manager-configuration`: el programador solo lee `prg-programaciones`.

Relacionado, pero no es `cnf-mallas` (DevOps): Step Function `niif-snf-emisiones` con `ejecucionId` y `--EJECUCION_ID`, y el empaquetado del job ([[pendientes]]).

## Otras mallas
| Malla | Cambio | Estado |
|---|---|---|
| ARL | `where` con `PERIODO = 'P'` en vez de `<=` ([[fix-scd-freno-periodo]]) | Git cambiado; QA y PRD pendientes |
