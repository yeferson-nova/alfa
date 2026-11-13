---
tipo: repo
estado: vigente
actualizado: 2026-11-12
repo: sc-datalake-niif-certification
categoria: lambda
paso: "3 · Certificación Cifras (enlace Certificar / No certificar)"
malla: transversal
---
# sc-datalake-niif-certification
> Lambda del API de aprobación: recibe `POST /certify` y `POST /not_certify`, valida al aprobador y registra la decisión (CERTIFICATE o NO_CERTIFICATE) en la tabla de notificaciones.

## Rol en el flujo
- Paso 3, hoja `Página-2`. La dispara API Gateway (autorizador Cognito) cuando el aprobador entra por el enlace del correo, después de `short-link-redirect` y `post-login`.
- Su escritura en la tabla genera el evento de stream que consumen copy y move (o la orquestadora nueva) para llevar el CSV a transporte o a no certificados.

## Entradas y salidas
- Entra: evento de API Gateway con claims de Cognito (el correo del usuario) y cuerpo con `useCaseId`, `certCode` y `period`. La ruta define la decisión.
- Sale: respuesta HTTP 200, 400, 401, 403, 404, 409 o 500; actualiza todos los registros del `certCode` con `processedBy`, `processedDate` y `decision`.

## Recursos AWS que toca
- Tabla `niif-{env}-notification-certification` (consulta por el índice `certCode-index` y `update_item` con condición).
- Tabla de auditoría de aprobadores `niif-{env}-approval-audit-log` (voto por correo y `pendingApprovals`).
- Bucket `niif-{env}-assets`: JSON global y JSON del caso (`approvers.users` y `approvers.required`); la ruta sale del registro (`configBucket`, `configKey`).
- Función `niif-{env}-certification` (Jenkinsfile; artefacto en `niif-{env}-lambda-artifacts`).

## Código
- `src/certification.py` (566 líneas, todo en un archivo): verifica que el correo esté en la lista de aprobadores, responde 409 si el estado ya es CERTIFICATE, NO_CERTIFICATE, TRANSPORT o NO_TRANSPORT y aplica el multiaprobador. Un NO_CERTIFICATE cancela de inmediato; el estado intermedio es `PENDING_APPROVERS` hasta completar los votos.
- `tests/test_lambda_function.py`: 72 pruebas, umbral de cobertura 70 %. Los valores por defecto de las variables de entorno apuntan a QA.
- El README de Alfa es una copia del de la lambda trigger (no describe este repo).

## Estado
- En Alfa: `origin/quality`, commit `22c6b5e` (2026-08-27) según `VERSIONES.md`; desplegado.
- `quality` frente a `main` (medido en GitHub el 2026-11-12): 0 commits solo en `quality`, 3 solo en `main`; `main` tiene commits que `quality` no tiene (por revisar si son merges o cambios de PRD). Ver ARQ-50 en [[auditoria-repos-2026-11]].
- Código nuevo nuestro: rama `feature/codigo-nuevo-notificacion` de `alfa-repos`, solo esqueleto: carpetas hexagonales vacías `certification_decision/{domain,application,adapters,config}` y `niif_observability`. `certification.py` es idéntico al de Alfa; el README nuevo describe la meta (log del resultado, sin claims en el log). Falta la lógica y `src/lambda_function.py`.

## Brechas y relacionadas
- **ARQ-46** el README del repo es copia de otro y no lo describe, en [[auditoria-repos-2026-11]].
- ARQ-33 (no deja log del resultado) y SEG-12 (escribe evento y claims con el correo en el log) en [[notificaciones-vs-arquitectura]]. D7 (si el API va a la orquestadora o aquí) en [[roadmap-notificaciones]].
- SEG-10 y SEG-11 (flujo implícito, `state` sin firma) y SEG-14 (API viejo) en [[notificaciones-vs-arquitectura]] e [[inventario-lambdas]].
- Relacionado: [[sc-datalake-niif-short-link-redirect]], [[sc-datalake-niif-post-login]], [[sc-datalake-niif-notification-certification-orchestrator]], [[inprod-enlaces-cognito-9411325]], [[est-gobierno-devops-niif]].
