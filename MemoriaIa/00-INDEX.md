---
tipo: contexto
estado: vigente
actualizado: 2026-11-12
---
# Índice de la memoria de IA

> Punto de entrada. Léelo primero, elige la nota que el tema pida y abre solo esa. Las notas son cortas y se enlazan con `[[nombre]]` (como en Obsidian); el nombre es único en toda la memoria, sin ruta ni extensión.

**Proyecto:** NIIF 17 de Seguros Alfa, todo en AWS. Nos toca el **sistema de jobs de Glue (medallón)** y las **lambdas y jobs de notificación y certificación** (pasos 2 y 3 del diagrama). Todo análisis busca brechas de seguridad, costos (Athena) y arquitectura contra los estándares del banco.

## Si necesitas…, lee
| Necesitas | Lee |
|---|---|
| Retomar el trabajo en una sesión nueva | [[traspaso-sesion]], luego [[pendientes]] |
| Entender el flujo de punta a punta | [[arquitectura-niif17]] y después [[00-mapa-repos]] |
| Saber qué hace un repo y qué papel juega en el flujo | su ficha en `repos\` (lista en [[00-mapa-repos]]) |
| Dónde está el código y cómo se pasa al PC corporativo | [[repo-alfa-repos]] (comandos en `COMANDOS-ALFA-REPOS.md`, raíz del repo) |
| Dónde está cada archivo o carpeta | [[rutas-workspace]] (las rutas `Backend\` son históricas) |
| Estado de un cambio nuestro (SES, orquestadora, certificación, T2, T3) | `features\`: empezar por [[tareas-aws]] y [[roadmap-notificaciones]] |
| Un incidente en producción | `inprod\`: [[inprod-primas-arl-202606]], [[inprod-enlaces-cognito-9411325]] |
| Brechas de seguridad, costos o arquitectura (IDs `SEG`, `COS`, `ARQ`, `CUM`) | `auditorias\`: [[brechas-seguridad]], [[auditoria-pipeline-2026-11]], [[notificaciones-vs-arquitectura]], [[auditoria-repos-2026-11]] |
| Qué falta por hacer, o dudas sin resolver | [[pendientes]], [[preguntas-abiertas]] |
| Cómo escribir código (estructura, errores, logs, pruebas, nombres) | [[est-desarrollo-hexagonal]] y [[est-gobierno-devops-niif]] |
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
- [[arquitectura-niif17]]: pasos 1 a 14 del diagrama TOBE (nos competen 2 y 3)
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
- [[diseno-propuesto]]: cambios propuestos en el paso 3 (sin confirmar)

**inprod:** [[inprod-primas-arl-202606]] · [[inprod-enlaces-cognito-9411325]]

**auditorias:** [[brechas-seguridad]] · [[auditoria-pipeline-2026-11]] · [[notificaciones-vs-arquitectura]] · [[riesgos-glue-notificacion]] · [[auditoria-repos-2026-11]]

**pendientes:** [[pendientes]] · [[preguntas-abiertas]]

**reuniones:** [[reunion-diseno-pcr]]

**estandares:** [[est-etl]] · [[est-datastage]] · [[est-versionamiento-codigo]] · [[est-codigo-seguro]] · [[est-databrew]] · [[est-nuevos-cargues]] · [[est-gobierno-devops-niif]] · [[est-desarrollo-hexagonal]]

**hu:** [[hu-gobierno-ingestas]] · [[hu-linaje]] · [[hu-trazabilidad]] · [[hu-devops]] · [[hu-remediacion]] · [[hu-seguridad-acceso]]

**archivo:** [[estructura-repos-propuesta]] (obsoleta: los nombres no siguen el estándar de DevOps)

## Reglas de lectura
1. Ubica el tema en la tabla de arriba y abre una sola nota. Sigue los enlaces solo si hace falta.
2. Si una nota y el código discrepan, manda el código; corrige la nota y la fecha `actualizado`.
3. Lo no confirmado se marca "propuesta" o "por confirmar". Las dudas van a [[preguntas-abiertas]] y el trabajo a [[pendientes]].
4. Tema nuevo: crear la nota en la carpeta que le toque y agregarla aquí. Repo nuevo: ficha según [[plantilla-ficha-repo]] y una fila en [[00-mapa-repos]].
5. Antes de cerrar una sesión: `python alfa-repos\scripts\validar-memoria.py` debe terminar sin errores.
