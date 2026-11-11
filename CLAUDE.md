# Memoria IA (estilo Obsidian)

La memoria del proyecto está en `C:\Users\rdpuser\Git\MemoriaIa\`. Este archivo solo contiene el estándar y el índice. El contenido vive en notas separadas dentro de esa carpeta.

Al iniciar o al perder contexto: leer este índice y abrir solo las notas que el tema requiera.

## Contexto de trabajo
- Todo el stack es AWS. Contexto: estándares bancarios colombianos; el marco normativo se toma de los documentos entregados ([[marco-normativo]]).
- Todo análisis busca brechas de **seguridad**, **costos** (en especial Athena) y **arquitectura**, contra los estándares de `Docs\02-estandares-banco\`.

## Skill del proyecto (cargar al perder contexto)
- Skill `niif17-alfa` en `.claude\skills\niif17-alfa\SKILL.md`: lineamientos de arquitectura, calidad, código seguro, patrones de seguros, montos y costos (Glue/Athena) y uso de esta memoria. Sin código; las referencias se cargan por tema.
- Alcance del equipo: **sistema de jobs de Glue (medallón) y lambdas/jobs de notificación y certificación**.

## Dos formas de documentar
- **Para humanos:** `C:\Users\rdpuser\Git\Docs\`. Prosa clara, tablas y contexto completo para quien no conoce el proyecto. Mapa en `Docs\00-LEEME.md`.
- **Para IA (memoria):** `C:\Users\rdpuser\Git\MemoriaIa\`. Notas cortas y enlazadas, según el estándar de abajo.
- Si un tema tiene ambas, la nota de IA resume y apunta al documento humano (ejemplo: [[brechas-seguridad]]). No copiar el contenido completo.

## Estructura
- `Tareas\`: tareas asignadas para humanos. `00-LEEME.md` (contexto del proyecto) y una carpeta por asignación (`T1-notificaciones`, `T2-lambdas-gestor-configuracion`, `T3-validaciones-reserva-matematica`) con `README.md` y sus entregables.
- `Docs\`: `02-estandares-banco` (solo lectura, nombres originales), `03-historias-usuario`, `04-manuales`, `05-arquitectura`, `06-brechas`, `07-entregables`, `08-estandares-desarrollo` (estándar propio de código, ver [[est-desarrollo-hexagonal]]).
- `MemoriaIa\`: `meta`, `contexto`, `tareas`, `estandares`, `hu`, `brechas`.
- `Backend\`: solo código.
- Detalle archivo por archivo en [[rutas-workspace]].

## Arquitectura general del proyecto
- Fuente de verdad: `Docs\05-arquitectura\diagramas\NIIF17_ModeloSolucion_V1.0.drawio` (draw.io, varias hojas; ~2,9 MB, no leerlo completo: extraer solo la hoja necesaria). Hojas: `Página-10`, `Página-3`, `Página-5`, `Página-1`, `Página-6`, `Página-2`, `Página-9`, `Copia de Página-5`.
- Resumen del flujo en [[arquitectura-niif17]]. Otros diagramas de la carpeta (`flujo-niif17-asis`, `recorrido-dato-niif17`) son derivados propios.

## Estándar (memoria IA)
- Una nota = un tema, en `MemoriaIa/<subcarpeta>/<nombre-en-kebab-case>.md`. Nombres únicos en toda la memoria.
- Enlazar notas con `[[nombre-de-nota]]` (sin extensión ni subcarpeta), como en Obsidian.
- Cada nota inicia con frontmatter:
  ```
  ---
  tipo: contexto | tarea | diseno | referencia | pendiente | preferencia | estandar | hu | brecha
  actualizado: AAAA-MM-DD
  ---
  ```
- Fichas de `estandares\` y `hu\`: reglas verificables (qué exige el documento), 10 a 20 líneas, con la ruta del archivo fuente.
- Brechas con ID único: `SEG-xx` (seguridad), `COS-xx` (costos), `ARQ-xx` (arquitectura), `CUM-xx` (cumplimiento). Mismo ID en la nota IA y en `Docs\06-brechas\`.
- Al aprender algo nuevo: actualizar la nota existente antes de crear otra. Si es un tema nuevo, crear la nota y agregarla al índice.
- No duplicar información entre notas; enlazar.
- Marcar lo no confirmado como "propuesta". Registrar dudas en [[preguntas-abiertas]] y trabajo por hacer en [[pendientes]].
- Los archivos fuente del banco no se renombran ni se editan.
- Idioma: español.

## Índice (notas en `MemoriaIa/`)
**meta**
- [[traspaso-sesion]] - **leer primero en una sesión nueva o remota.** **Migración EN PAUSA desde el 2026-11-11.** Tiene el estado al día, qué no está en el repo, reglas y cómo verificar
- [[rutas-workspace]] - dónde está cada archivo
- [[preferencias-usuario]]
- [[preguntas-abiertas]]
- [[pendientes]] - incluye el plan por fases de organización

**contexto**
- [[arquitectura-niif17]] - pasos 1 a 14 del diagrama TOBE (nos competen 2 y 3)
- [[entorno-local-localstack]] - LocalStack en `Backend\localstack\`: servicios habilitados, límites y qué probar por tarea
- [[inventario-lambdas]] - 7 lambdas de trabajo (6 `configuration-<malla>` + manager, a consolidar en una general) y 6 Step Functions `niif-dev-<malla>`; el resto movido a `pruebas-*` (humano: `Backend\Lamnbdas\inventario-aws\`)
- [[familia-mallas-alfaprd]] - las 6 mallas ALFAPRD (incluye Primas ARL), jobs del diagrama (9) vs desplegados (12) y plan: subir el fix y auditar la familia
- [[marco-normativo]] - Ley 1581, Decreto 1377, circulares básicas de la SFC y lineamientos internos de Alfa

**tareas**
- [[tareas-aws]] - tareas asignadas (humano: `Tareas\`)
- [[diseno-propuesto]] - cambios propuestos en el paso 3
- [[correo-notificacion]] - formato del correo de certificación
- [[roadmap-notificaciones]] - roadmap de la cadena de notificación: pasos 1 a 9, componentes, decisiones D1-D7 y E1-E3 y estado (humano con ✅/⬜: `Tareas\T1-notificaciones\roadmap-notificaciones.md`)
- [[job-notificacion-ses]] - job de Glue de notificación reescrito para enviar por SES (T1a), en `Backend\notificacion\`; implementado, no desplegado
- [[job-certificacion]] - job de Glue de certificación reescrito (412 pruebas, no desplegado); riesgo CER-H5: lo comparten otras mallas
- [[orquestadora-notificaciones]] - lambda que reemplaza trigger, copy y move (404 pruebas, no desplegada); O-1 a O-4 aprobadas; espera a DevOps (índice `status-pendingDate-index`)
- [[estructura-repos-propuesta]] - estructura de repos y de archivos (lambdas, jobs de Glue, config, librería), decisiones del usuario, plantilla de DevOps y tablas por malla; propuesta en iteración
- [[reunion-diseno-pcr]] - notas de la reunión de explicación del diseño PCR (2026-11-10), en curso
- [[monitoreo-grafana]] - observabilidad: Grafana fuera de alcance (2026-11-10); logs, métricas y alarmas listos, alertas técnicas por SES; antes: propuesta de dos planos (técnico/negocio, SNS, EventBridge, ServiceNow), pendiente de validar (humano: `Tareas\T1-notificaciones\propuesta-observabilidad-eventos.md`)

**estandares** (lineamientos y manuales de Alfa)
- [[est-etl]] - estándar ETL GTC-GDD-010: nombres, validación, logging, cuadre de volúmenes
- [[est-datastage]] - estándar DataStage v6 (paso 8; reglas generalizables de validación y notificación)
- [[est-versionamiento-codigo]] - Git/GitHub V3: repos, ramas, tags, commits
- [[est-codigo-seguro]] - SonarQube GTC-GDD-003: línea base, cobertura > 80 %, comité de cambios
- [[est-databrew]] - manual de recetas Glue DataBrew (estado actual de preparación)
- [[est-nuevos-cargues]] - manual de Nuevos Cargues: precedente de aprobación por correo
- [[est-gobierno-devops-niif]] - **gobierno DevOps NIIF 17** (2026-11-09): nombres `niif-{env}-*`, repos `sc-datalake-niif-*` / `data-datalake-niif-*`, `src/lambda_function.py` como handler + módulos hexagonales, `test/`, cobertura 70 %; entregamos un zip con el nombre de la lambda y DevOps hace el commit
- [[est-desarrollo-hexagonal]] - **estándar propio** para todo código Python: arquitectura hexagonal, sin valores quemados, errores con taxonomía, logs JSON/EMF, idempotencia, pruebas > 80 % (humano: `Docs\08-estandares-desarrollo\`)

**hu** (estándares transversales obligatorios)
- [[hu-gobierno-ingestas]] - metadata en DynamoDB, taxonomía de errores, eventos y alarmas
- [[hu-linaje]] - linaje automático OpenLineage
- [[hu-trazabilidad]] - compuerta de calidad, cuarentena y cuadre (clave para la tarea 3)
- [[hu-devops]] - CI/CD, IaC Terraform y convención de nombres AWS
- [[hu-remediacion]] - patrones de API de aprobación, notificaciones y retención
- [[hu-seguridad-acceso]] - IAM Identity Center, Lake Formation, KMS y Athena con topes de costo

**brechas**
- [[brechas-seguridad]] - riesgos de seguridad y mejoras (humano: `Docs\06-brechas\seguridad.md`)
- [[riesgos-glue-notificacion]] - riesgos del paso Glue → Notificación y patrón de manifiesto + Step Functions (humano: `Tareas\riesgos-flujo-glue-notificacion.md`)
- [[auditoria-pipeline-2026-11]] - auditoría validada en código del pipeline (Bronce/Plata/Oro, SCD, certificación) y de las lambdas, contrastada con el diagrama (humano: `Docs\06-brechas\auditoria-pipeline-datos-vs-arquitectura.md` y `auditoria-lambdas.md`)
- [[notificaciones-vs-arquitectura]] - brechas de la cadena de notificación/certificación contra la hoja `Página-2` del diagrama y contra el estándar de código (ARQ-21 a ARQ-28, SEG-10 a SEG-12; humano: `Docs\06-brechas\notificaciones-vs-arquitectura.md`)
- [[inventario-lambdas]] §evidencia 2026-11-10 - limpieza de DEV: vigentes, nuevos y candidatos a retirar con evidencia de uso, SEG-14 y ARQ-30 (humano: `Docs\06-brechas\limpieza-dev-recursos.md`)
- [[notificaciones-vs-arquitectura]] §consultas - SQL de Athena de certificación/notificación: COS-07 a COS-09, ARQ-34 a ARQ-38, SEG-15 (humano: `Docs\06-brechas\consultas-athena-notificacion.md`)
- [[inprod-primas-arl-202606]] - incidente en prod: certificación Primas ARL 202606 vacía; extracción incremental + SCD que apaga por ausencia (Gold quedó con 209 activos); hechos, hipótesis y pendientes (humano: `errores\`)
- [[inprod-enlaces-cognito-9411325]] - incidente en prod: el enlace Certificar de Auxilios Funerarios tiene un espacio en el host de Cognito (no resuelve DNS); diagnóstico hecho, la corrección no se ha entregado
