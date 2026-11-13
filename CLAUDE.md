# Memoria IA (estilo Obsidian)

La memoria del proyecto está en `C:\Users\rdpuser\Git\alfa\MemoriaIa\`. Este archivo solo contiene el estándar. El contenido vive en notas separadas dentro de esa carpeta y su **punto de entrada es `MemoriaIa\00-INDEX.md`**.

Al iniciar o al perder contexto: leer `MemoriaIa\00-INDEX.md` y abrir solo las notas que el tema requiera.

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
- `MemoriaIa\`: `meta`, `contexto`, `repos`, `features`, `inprod`, `auditorias`, `pendientes`, `estandares`, `hu`, `reuniones`, `archivo`. Detalle en [[00-INDEX]].
- `Backend\`: ya no tiene código (desde 2026-11-12 vive en el repo `alfa-repos`, ver [[repo-alfa-repos]]); quedan maquetas HTML y documentos. La evidencia de AWS está en `EvidenciasAws\backend\` (fuera de Git).
- Detalle archivo por archivo en [[rutas-workspace]].

## Arquitectura general del proyecto
- Fuente de verdad: `Docs\05-arquitectura\diagramas\NIIF17_ModeloSolucion_V1.0.drawio` (draw.io, varias hojas; ~2,9 MB, no leerlo completo: extraer solo la hoja necesaria). Hojas: `Página-10`, `Página-3`, `Página-5`, `Página-1`, `Página-6`, `Página-2`, `Página-9`, `Copia de Página-5`.
- Resumen del flujo en [[arquitectura-niif17]]. Otros diagramas de la carpeta (`flujo-niif17-asis`, `recorrido-dato-niif17`) son derivados propios.

## Estándar (memoria IA)
- Una nota = un tema, en `MemoriaIa/<subcarpeta>/<nombre-en-kebab-case>.md`. Cada nota abre con una línea de resumen (`> ...`) para decidir si hace falta leerla. Nombres únicos en toda la memoria.
- Enlazar notas con `[[nombre-de-nota]]` (sin extensión ni subcarpeta), como en Obsidian.
- Cada nota inicia con frontmatter:
  ```
  ---
  tipo: contexto | referencia | repo | feature | inprod | auditoria | pendiente | preferencia | estandar | hu | tarea
  estado: vigente | en-pausa | obsoleto | nuevo (nuevo solo en fichas de repo)
  actualizado: AAAA-MM-DD
  ---
  ```
- Fichas de `estandares\` y `hu\`: reglas verificables (qué exige el documento), 10 a 20 líneas, con la ruta del archivo fuente.
- Brechas con ID único: `SEG-xx` (seguridad), `COS-xx` (costos), `ARQ-xx` (arquitectura), `CUM-xx` (cumplimiento). Mismo ID en la nota IA y en `Docs\06-brechas\`. Último usado: SEG-16, COS-09, ARQ-49, CUM-01.
- Un repo = una ficha en `MemoriaIa/repos/<nombre-del-repo>.md`, con la plantilla [[plantilla-ficha-repo]]. Una carpeta por tipo de nota: cambios nuestros en `features`, incidentes en `inprod`, brechas en `auditorias`.
- Validar antes de cerrar: `python C:\Users\rdpuser\Git\alfa-repos\scripts\validar-memoria.py` (frontmatter, enlaces, índice y fichas).
- Al aprender algo nuevo: actualizar la nota existente antes de crear otra. Si es un tema nuevo, crear la nota y agregarla al índice.
- No duplicar información entre notas; enlazar.
- Marcar lo no confirmado como "propuesta". Registrar dudas en [[preguntas-abiertas]] y trabajo por hacer en [[pendientes]].
- Los archivos fuente del banco no se renombran ni se editan.
- Idioma: español.

## Índice
El índice vive en `MemoriaIa\00-INDEX.md`: tabla "si necesitas X, lee Y", estructura de carpetas y lista de todas las notas. **Cada nota nueva se agrega ahí** (y cada repo nuevo, en `MemoriaIa\repos\00-mapa-repos.md`). Lectura obligatoria al iniciar:
- [[00-INDEX]] - mapa de la memoria
- [[traspaso-sesion]] - estado al día y qué no está en el repo (migración EN PAUSA desde 2026-11-11)
- [[repo-alfa-repos]] - el código vive en el repo `alfa-repos`, no aquí
- [[00-mapa-repos]] - los 35 repos de Alfa y su rol en el flujo
