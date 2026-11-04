---
tipo: estandar
actualizado: 2026-11-03
---
# Lineamiento de versionamiento del código fuente (V3)

Fuente: `Docs\02-estandares-banco\versionamiento-seguro\VERSIONAMIENTODECODIGOV3.pdf` (14 págs.). Aplica a código y objetos de BD de Seguros Alfa. Ojo: [[hu-devops]] cita la **V2**; el entregado es la V3. Verificar diferencias si surgen conflictos.

## Reglas verificables
- **Herramienta:** Git centralizado en GitHub; organizaciones, equipos y permisos por nivel (lectura, escritura, admin).
- **Repositorios (5.2):** kebab-case `<tipo>-<proyecto>-<complemento>`; tipos `lib, web, msa, iac, db, sc` (serverless), `doc, soa, mob, data, ma`. Ej.: `msa-sipren-calculations`.
- **PR (5.3.4-5.3.6):** revisores obligatorios y número mínimo (ej. 2); ramas protegidas `main`, `quality`, `develop`; CI/CD en verde antes de fusionar.
- **Branching (5.4):** Git Flow (feature desde develop → PR → tag → despliegue Jenkins → PR entre ramas de ambiente) o integración selectiva (feature → QA → pre-producción → main tras 7 días).
- **Ramas (5.5-5.6):** minúsculas, alfanumérico y guiones; `feature|bugfix|hotfix/<TICKET>`, `release/X.Y.Z`, `docs/`, `experiment/` (no se fusiona), `chore/`.
- **Tags (5.7):** semver MAJOR.MINOR.PATCH.
- **Commits (5.8):** `<type>-<TICKET>-<subject>`; tipos `ci, docs, feat, fix/hotfix/patch, perf, refactor, n, test, style, chore, revert`; subject ≤ 72, minúsculas, presente; ticket obligatorio en mayúsculas; usuario y correo corporativo `@segurosalfa.com.co`.
- **Controles (6):** el líder técnico valida en el PR buenas prácticas y **ausencia de código quemado**; revisión periódica de la coordinación.

## Relevancia
- Todo cambio de las tareas ([[tareas-aws]]) debe ir en repositorio con este flujo. Lambdas del paso 3: tipo `sc` (serverless) o `data` según [[hu-devops]].
- "Sin código quemado" respalda la brecha de secretos en código ([[brechas-seguridad]] #9).
- Commits de este proyecto: `feat-<TICKET>-...` (falta conocer el ticket Jira, ver [[preguntas-abiertas]]).
