---
tipo: estandar
estado: vigente
actualizado: 2026-11-26
---
# Estándar de Pull Requests del equipo

> Formato obligatorio de todo PR: título, base, plantilla de descripción y lista de verificación. Propuesta del equipo (2026-11-17) sobre la V3 del banco ([[est-versionamiento-codigo]]).

Fuentes: `Docs\08-estandares-desarrollo\estandar-pull-requests.md` (reglas) y `Docs\08-estandares-desarrollo\plantillas\pull_request_template.md` (plantilla que se pega tal cual en la descripción).

## Reglas verificables
- **Título:** `<tipo>-<TICKET>-<asunto>` (V3 5.8), minúsculas, presente, máximo 72 caracteres. **Sin ticket Jira:** `<tipo>(<alcance>): <asunto>`, ej. `fix(primas-rentas): filtrar rentas expedidas en el periodo`.
- **Base:** `develop` para feature, bugfix y chore; GitHub propone `main` y se cambia a mano. Promoción `develop` → `quality` → `main`, cada paso con su PR. Hotfix desde `main` y se lleva a `quality` y `develop`.
- **Descripción:** siempre la plantilla del banco-equipo, con sus secciones en este orden: Parámetros (tabla: tipo, alcance, ticket o brecha, base, ambiente probado, riesgo, cambio de configuración o IaC, reversible), Qué cambia y por qué, Cómo se probó, Impacto (cifras, configuración, costo, otros equipos), Cómo revertirlo, Lista de verificación, Para el revisor.
- **No se borran secciones:** lo que no aplica se deja "no aplica" con una frase. Solo se quitan los bloques de la lista marcados "borrar si no aplica" (Glue, Lambdas, IaC).
- **Casillas:** marcar solo lo comprobado; lo pendiente (pipeline, cobertura) queda sin marcar con la razón.
- **Tamaño:** menos de 400 líneas sin pruebas ni generados; si no, partir o justificar. Un solo tema por PR.
- **Revisión:** 2 revisores mínimos, pipeline y Quality Gate en verde, cobertura del código nuevo > 80 %.
- **Sin línea de Claude Code ni coautor** en la descripción (preferencia del usuario, [[preferencias-usuario]]).
- **Sin temas nuestros (2026-11-26):** ni códigos de brecha (`ARQ-xx` y demás) ni conceptos internos en título ni descripción. El campo "Ticket o ID de brecha" lleva solo el ticket Jira; sin ticket, "no aplica".

Ejemplo aplicado: PR de [[inprod-primas-rentas-202606]] (2026-11-18).
