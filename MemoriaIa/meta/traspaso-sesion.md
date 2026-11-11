---
tipo: contexto
actualizado: 2026-11-11
---
# Traspaso entre sesiones (leer primero en una sesión nueva o remota)

Este es el estado al 2026-11-11 para que otra sesión de Claude (local o remota) siga sin perder contexto. **La memoria local de Claude (`~/.claude/...`) está vacía; todo vive en este repo.** Hay que leer `CLAUDE.md` (el índice), esta nota y la skill `.claude\skills\niif17-alfa\`.

## Lo que no está en el repo
- `EvidenciasAws\` está en `.gitignore`: las evidencias de AWS (DEV y prod) solo existen en la máquina del usuario. Las conclusiones están resumidas en las notas y en `Docs\06-brechas\`.
- Las carpetas `dist/` (zips empaquetados) se regeneran con `scripts/package.sh`.
- Una sesión remota **no tiene acceso a AWS**. Los pasos de AWS se le entregan al usuario como comandos para CloudShell o pasos de consola, y solo para DEV.

## Estado de la cadena de notificación (detalle en [[roadmap-notificaciones]])
| Componente | Carpeta | Estado |
|---|---|---|
| Job de notificación (SES) | `Backend\notificacion\jobs-glue\niif-dev-notification-certification\` | 537 pruebas; configuración ya en el JSON global (bloque `notification`). En DEV el job aún tiene los argumentos viejos. Ver [[job-notificacion-ses]] |
| Job de certificación | `Backend\notificacion\jobs-glue\niif-dev-certification\` | 412 pruebas, no desplegado. Ver [[job-certificacion]] |
| Orquestadora | `Backend\notificacion\lambdas\niif-dev-notification-certification-orchestrator\` | 404 pruebas, no desplegada, espera a DevOps. Ver [[orquestadora-notificaciones]] |
| Evaluador compuesto, API de certificación, redirección de enlaces, post-login | `Backend\notificacion\lambdas\<recurso>\` | Solo esqueleto (carpetas y `niif_observability`) |

**Paso siguiente según el roadmap** (`Tareas\T1-notificaciones\roadmap-notificaciones.md`):
- Paso 5: transporte a FIS / Lambda SFTP.
- Paso 6: el resto de lambdas.
- En paralelo: las pruebas en DEV de la certificación y de la orquestadora, que requieren confirmación del usuario.

**Decisiones abiertas:** D2 (No certificar en `prg-programaciones`), D3, D4, D7, CER-H1 a CER-H7 y el nombre de la Lambda SFTP.

## Reglas que no se negocian (de [[preferencias-usuario]])
- Responder en español y conciso. No suponer: validar con código o evidencia y marcar las hipótesis.
- **No cambiar código sin una orden explícita** ("ejecuta", "hazlo", "aprobado, ejecuta"). Mientras tanto, solo análisis y documentación.
- Trabajamos solo en DEV; cada paso en AWS se confirma antes. QA y PRD son de DevOps.
- Configuración: un JSON en S3 por ambiente con la fuente en Git, sin valores por defecto ni duplicados. Ver `.claude\skills\niif17-alfa\references\configuracion-por-ambiente.md`.
- Código: hexagonal, PEP 8 con identificadores en inglés y logs JSON en español, sin comentarios ni docstrings. Funciones de hasta 40 líneas y hasta 8 parámetros, líneas de hasta 120 caracteres, compatible con Python 3.10. Estructura común de carpetas en inglés y un solo `README.md` por componente. Ver [[est-desarrollo-hexagonal]].
- Git: el commit y el push a `main` se hacen solo cuando el usuario lo pide, **sin línea de coautor** y con un mensaje muy corto en español.

## Cómo verificar el código
- En cada componente: `python -m pytest --cov` (o `tox`). Las pruebas incluyen arquitectura (dependencias entre capas) y calidad (sin valores quemados, largo de funciones y líneas).
- Las copias de la librería común: `python Backend\notificacion\verificar_copias_comunes.py`. Las 7 copias de `niif_observability` deben ser iguales; si se cambia una, se copia a todas.
