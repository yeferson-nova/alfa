---
tipo: contexto
estado: vigente
actualizado: 2026-11-23
---
# Índice de la memoria de IA

> Punto de entrada. Léelo primero, elige la nota que el tema pida y abre solo esa. Las notas son cortas y se enlazan con `[[nombre]]` (como en Obsidian); el nombre es único en toda la memoria, sin ruta ni extensión.

**Proyecto:** NIIF 17 de Seguros Alfa, todo en AWS. Nos toca el **sistema de jobs de Glue (medallón)** y las **lambdas y jobs de notificación y certificación** (pasos 2 y 3 del diagrama). Todo análisis busca brechas de seguridad, costos (Athena) y arquitectura contra los estándares del banco.

## Si necesitas…, lee
| Necesitas | Lee |
|---|---|
| Retomar el trabajo en una sesión nueva | [[traspaso-sesion]], luego [[pendientes]] |
| Entender el flujo de punta a punta | [[arquitectura-niif17]] y después [[00-mapa-repos]] |
| Tocar una pieza de la cadena de notificación o certificación (lambdas, enlaces, evaluador, orquestadora) | [[arquitectura-propuesta-niif17]]: las dos arquitecturas difieren ahí |
| Saber qué hace un repo y qué papel juega en el flujo | su ficha en `repos\` (lista en [[00-mapa-repos]]) |
| Dónde está el código y cómo se pasa al PC corporativo | [[repo-alfa-repos]] (comandos en `COMANDOS-ALFA-REPOS.md`, raíz del repo) |
| Dónde está cada archivo o carpeta | [[rutas-workspace]] (las rutas `Backend\` son históricas) |
| Estado de un cambio nuestro (SES, orquestadora, certificación, T2, T3) | `features\`: empezar por [[tareas-aws]] y [[roadmap-notificaciones]] |
| Un incidente en producción | `inprod\`: [[inprod-primas-arl-202606]] (su fix: [[fix-scd-freno-periodo]]), [[inprod-primas-rentas-202606]], [[inprod-enlaces-cognito-9411325]], [[inprod-pcr-emisiones-202606]] |
| Brechas de seguridad, costos o arquitectura (IDs `SEG`, `COS`, `ARQ`, `CUM`) | `auditorias\`: [[brechas-seguridad]], [[auditoria-pipeline-2026-11]], [[notificaciones-vs-arquitectura]], [[auditoria-repos-2026-11]] |
| Qué falta por hacer, o dudas sin resolver | [[pendientes]], [[preguntas-abiertas]] |
| Qué hay que cambiar en `cnf-mallas` o en la configuradora | [[cambios-cnf-mallas]] |
| Observabilidad: logs, ID de punta a punta, duraciones, tableros y alertas | [[roadmap-observabilidad]] |
| Optimizar el proceso PCR (Emisiones, Primas Cedidas, Reaseguros, Primas PCR) | [[proceso-pcr]], luego [[modularizacion-extraccion]] |
| Modularizar un componente (extracción, remediación, validación, SCD) y cerrar su deuda técnica | [[modularizacion]], luego la nota del componente (ej. [[modularizacion-extraccion]]) |
| Cómo escribir código (estructura, errores, logs, pruebas, nombres) | [[est-desarrollo-hexagonal]] y [[est-gobierno-devops-niif]] |
| Formato de un PR (título, base, plantilla de descripción) | [[est-pull-requests]] |
| Qué exige un estándar o una historia de usuario | `estandares\` y `hu\` |
| Cómo prefiere trabajar el usuario | [[preferencias-usuario]] |
| Crear la ficha de un repo | [[plantilla-ficha-repo]] |

## Estructura
| Carpeta | Contenido | `tipo` |
|---|---|---|
| `meta\` | Protocolo de trabajo: traspaso, rutas, preferencias, el repo del código, plantillas | contexto, referencia, preferencia |
| `contexto\` | Arquitectura, marco normativo, familia de mallas, entorno local, inventarios | contexto, referencia |
| `repos\` | Una ficha por repo de Alfa (35): qué hace, rol en el flujo, recursos, estado, brechas | repo |
| `features\` | Una nota por cambio nuestro: diseño, estado, decisiones | feature |
| `inprod\` | Incidentes en producción: síntoma, causa, evidencia, arreglo | inprod |
| `auditorias\` | Brechas y auditorías con ID único | auditoria |
| `pendientes\` | Trabajo por hacer y preguntas abiertas | pendiente |
| `estandares\` | Fichas de los lineamientos del banco y del equipo | estandar |
| `hu\` | Fichas de las historias de usuario transversales | hu |
| `reuniones\` | Notas de reuniones | tarea |
| `archivo\` | Notas obsoletas, solo para consulta | cualquiera, `estado: obsoleto` |

Cada nota empieza con `tipo`, `estado` (`vigente`, `en-pausa` u `obsoleto`) y `actualizado`; las fichas de repo agregan `repo`, `categoria`, `paso` y `malla`. El estándar completo está en `CLAUDE.md`.

## Fuera de la memoria
- `Docs\`: documentación para humanos (`Docs\00-LEEME.md`); los estándares originales del banco están en `Docs\02-estandares-banco\` y no se editan.
- `Tareas\`: tareas asignadas y sus entregables. `errores\`: documentos de incidentes.
- `EvidenciasAws\`: extracciones de las cuentas AWS (fuera de Git, con ARN y datos de cuenta).
- Repo hermano `alfa-repos`: **todo el código**, el mapa de versiones (`VERSIONES.md`) y los scripts, incluido `scripts\validar-memoria.py`, que revisa esta memoria.

## Todas las notas
**meta:** [[traspaso-sesion]] (estado al día, leer primero) · [[repo-alfa-repos]] · [[rutas-workspace]] · [[preferencias-usuario]] · [[plantilla-ficha-repo]]

**contexto:**
- [[arquitectura-niif17]]: pasos 1 a 12 del diagrama aprobado, con el detalle del paso 3 (nos competen 2 y 3)
- [[arquitectura-propuesta-niif17]]: nuestra segunda arquitectura (lambdas que faltan en la `Página-2`) y dónde difiere de la aprobada
- [[familia-mallas-alfaprd]]: las 6 mallas, jobs del diagrama frente a los desplegados
- [[inventario-lambdas]]: lambdas y Step Functions de DEV, con la evidencia de la limpieza
- [[repos-alfa-notificacion]]: hallazgos R-01 a R-06 de los repos de la cadena de notificación
- [[marco-normativo]]: Ley 1581, Decreto 1377 y circulares de la SFC
- [[entorno-local-localstack]]: pruebas locales con LocalStack

**repos:** [[00-mapa-repos]] (tabla de los 35 repos, con enlace a cada ficha)

**features:**
- [[tareas-aws]]: las tres tareas asignadas (T1 notificaciones, T2 configuración, T3 validaciones)
- [[roadmap-notificaciones]]: pasos 1 a 9 de la cadena de notificación y decisiones
- [[job-notificacion-ses]] · [[job-certificacion]] · [[orquestadora-notificaciones]]
- [[correo-notificacion]]: formato del correo de certificación
- [[monitoreo-grafana]]: observabilidad (Grafana fuera de alcance)
- [[roadmap-observabilidad]]: roadmap de logs, trazas y duración de punta a punta (2026-11-19, propuesta)
- [[modularizacion]]: tarea transversal de modularizar componentes, cerrar deuda y observabilidad (2026-11-20)
- [[proceso-pcr]]: plan global de PCR (4 mallas): extracción en 1,5 h como máximo, código modular y observabilidad en 3 niveles; carpetas por malla con validaciones
- [[tiempos-pcr-emisiones]]: registro de tiempos (job, caso y tabla) de la línea base y de cada cambio, para el gráfico de la entrega
- [[modularizacion-extraccion]]: modelo de ciclos de Víctor y lectura de dependencias por rangos (opción D, en diseño)
- [[productos-genericos-emisiones]]: productos como dato (catálogo, mapa de conceptos y controles), piloto Pymes; sin tocar la Oracle ni hacerle espejo (propuesta, 2026-11-25)
- [[diseno-propuesto]]: cambios propuestos en el paso 3 (sin confirmar)
- [[fix-scd-freno-periodo]]: fix completo de Primas ARL (SCD por periodo, extracción, calidad del código), 33 hallazgos y plan de 7 días

**inprod:** [[inprod-primas-arl-202606]] · [[inprod-primas-rentas-202606]] · [[inprod-enlaces-cognito-9411325]] · [[inprod-pcr-emisiones-202606]] (PRD frente a QA, diagnóstico por capas)

**auditorias:** [[brechas-seguridad]] · [[auditoria-pipeline-2026-11]] · [[notificaciones-vs-arquitectura]] · [[riesgos-glue-notificacion]] · [[auditoria-repos-2026-11]] · [[auditoria-observabilidad-2026-11]] · [[auditoria-extraccion-2026-11]]

**pendientes:** [[pendientes]] · [[preguntas-abiertas]] · [[cambios-cnf-mallas]]

**reuniones:** [[reunion-diseno-pcr]] · [[reunion-extraccion-victor]]

**estandares:** [[est-etl]] · [[est-datastage]] · [[est-versionamiento-codigo]] · [[est-codigo-seguro]] · [[est-databrew]] · [[est-nuevos-cargues]] · [[est-gobierno-devops-niif]] · [[est-desarrollo-hexagonal]] · [[est-pull-requests]]

**hu:** [[hu-gobierno-ingestas]] · [[hu-linaje]] · [[hu-trazabilidad]] · [[hu-devops]] · [[hu-remediacion]] · [[hu-seguridad-acceso]]

**archivo:** [[estructura-repos-propuesta]] (obsoleta: los nombres no siguen el estándar de DevOps)

## Reglas de lectura
1. Ubica el tema en la tabla de arriba y abre una sola nota. Sigue los enlaces solo si hace falta.
2. Si una nota y el código discrepan, manda el código; corrige la nota y la fecha `actualizado`.
3. Lo no confirmado se marca "propuesta" o "por confirmar". Las dudas van a [[preguntas-abiertas]] y el trabajo a [[pendientes]].
4. Tema nuevo: crear la nota en la carpeta que le toque y agregarla aquí. Repo nuevo: ficha según [[plantilla-ficha-repo]] y una fila en [[00-mapa-repos]].
5. Antes de cerrar una sesión: `python alfa-repos\scripts\validar-memoria.py` debe terminar sin errores.
