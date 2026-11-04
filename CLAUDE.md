# Memoria IA (estilo Obsidian)

La memoria del proyecto está en `C:\Users\rdpuser\Git\MemoriaIa\`. Este archivo solo contiene el estándar y el índice. El contenido vive en notas separadas dentro de esa carpeta.

Al iniciar o al perder contexto: leer este índice y abrir solo las notas que el tema requiera.

## Contexto de trabajo
- Todo el stack es AWS. Contexto: estándares bancarios colombianos; el marco normativo se toma de los documentos entregados ([[marco-normativo]]).
- Todo análisis busca brechas de **seguridad**, **costos** (en especial Athena) y **arquitectura**, contra los estándares de `Docs\02-estandares-banco\`.

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
- [[rutas-workspace]] - dónde está cada archivo
- [[preferencias-usuario]]
- [[preguntas-abiertas]]
- [[pendientes]] - incluye el plan por fases de organización

**contexto**
- [[arquitectura-niif17]] - pasos 1 a 14 del diagrama TOBE (nos competen 2 y 3)
- [[entorno-local-localstack]] - LocalStack en `Backend\localstack\`: servicios habilitados, límites y qué probar por tarea
- [[inventario-lambdas]] - 7 lambdas de trabajo (6 `configuration-<malla>` + manager, a consolidar en una general) y 6 Step Functions `niif-dev-<malla>`; el resto movido a `pruebas-*` (humano: `Backend\Lamnbdas\inventario-aws\`)
- [[marco-normativo]] - Ley 1581, Decreto 1377, circulares básicas de la SFC y lineamientos internos de Alfa

**tareas**
- [[tareas-aws]] - tareas asignadas (humano: `Tareas\`)
- [[diseno-propuesto]] - cambios propuestos en el paso 3
- [[correo-notificacion]] - formato del correo de certificación
- [[monitoreo-grafana]] - monitoreo y propuesta de dos planos (técnico/negocio, SNS, EventBridge, ServiceNow), pendiente de validar (humano: `Tareas\T1-notificaciones\propuesta-observabilidad-eventos.md`)

**estandares** (lineamientos y manuales de Alfa)
- [[est-etl]] - estándar ETL GTC-GDD-010: nombres, validación, logging, cuadre de volúmenes
- [[est-datastage]] - estándar DataStage v6 (paso 8; reglas generalizables de validación y notificación)
- [[est-versionamiento-codigo]] - Git/GitHub V3: repos, ramas, tags, commits
- [[est-codigo-seguro]] - SonarQube GTC-GDD-003: línea base, cobertura > 80 %, comité de cambios
- [[est-databrew]] - manual de recetas Glue DataBrew (estado actual de preparación)
- [[est-nuevos-cargues]] - manual de Nuevos Cargues: precedente de aprobación por correo
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
