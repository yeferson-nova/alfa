---
tipo: contexto
actualizado: 2026-11-11
---
# Traspaso entre sesiones (leer primero en una sesión nueva o remota)

Este es el estado al 2026-11-11 para que otra sesión de Claude (local o remota) siga sin perder contexto. **La memoria local de Claude (`~/.claude/...`) está vacía; todo vive en este repo.** Hay que leer `CLAUDE.md` (el índice), esta nota y la skill `.claude\skills\niif17-alfa\`. **Ojo:** `.claude` está en `.gitignore`, así que la skill solo existe en la máquina del usuario. Una sesión remota no la tiene y se guía por esta nota y por [[est-desarrollo-hexagonal]].

## Pausa de la migración (2026-11-11)
**Decisión del usuario:** se detiene la migración de la cadena de notificación y certificación. Mientras siga en pausa no se escribe código, no se lanza nada en DEV y no se envían solicitudes nuevas a DevOps. Solo se retoma cuando el usuario lo pida de forma explícita.

**Estado congelado:**
- Roadmap (`Tareas\T1-notificaciones\roadmap-notificaciones.md`):
  - Paso 1: hecho en código, sin envío real en DEV.
  - Paso 2: medido en DEV; falta PRD.
  - Paso 3: etapas 1 y 2 hechas; la 3 (prueba en DEV) está sin hacer.
  - Paso 4: etapas 1 y 2 hechas (orquestadora); espera a DevOps.
  - Pasos 5 a 9: sin empezar.
- Último commit propio de la migración: `f144291` "detalle indice pendientes". Después se subieron commits del caso Cognito (`24e1f19` a `df86ae8`, más el merge `527ced7`).
- **Nada del código nuevo está desplegado.** Lo único creado en DEV:
  - el job de prueba `niif-dev-notification-certification-prueba-v2`;
  - el código en `s3://niif-dev-assets/pruebas/notificacion-v2/`;
  - la política `niif-dev-notification-ses-templates`, creada por CLI y sin IaC (ver [[job-notificacion-ses]]).
- Solicitudes a DevOps **redactadas, pero sin confirmar que se enviaron**:
  - sección DevOps del README de la orquestadora (índice `status-pendingDate-index` en `niif-<amb>-notification-certification`, función, mapping, Scheduler y permisos);
  - sección "Lo que debe hacer DevOps" del README del job de certificación (job `-v2`, claves de los JSON globales, `athena:StopQueryExecution`, `--EJECUCION_ID`, rollback).
- El JSON global de DEV en Git (`Backend\S3\niif-dev-assets\configs\certification\json_global_notif_cert.json`) ya tiene los bloques `notification` y `orchestrator` y `dynamodb.pending_index`. **No se ha subido a S3.** `alert_recipients` sigue con el marcador `<CORREO_SOPORTE_NIIF>`.

**Al retomar, en este orden:**
1. Preguntar al usuario si DevOps respondió o si creó algo (índice, función, job `-v2`).
2. Revisar si cambió algo en DEV desde el 2026-11-11 (código desplegado de los jobs y las lambdas, JSON globales, tabla). Usar comandos de solo lectura para CloudShell.
3. Correr las pruebas de los 3 componentes y `verificar_copias_comunes.py`, y comprobar que siguen en verde.
4. Seguir con las pruebas en DEV (certificación etapa 3 con Primas ARL y orquestadora), confirmando cada paso. Luego el Paso 5.

**Abierto fuera de la migración:**
- el caso en prod [[inprod-enlaces-cognito-9411325]] (espacio en el host del enlace de Auxilios);
- el incidente [[inprod-primas-arl-202606]] (paquete de release del fix de extracción);
- las decisiones del repo en [[pendientes]] (`.gitignore` sin commitear y evidencia de prod en `0c5931a`).

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
