# niif-dev-composite-notification-evaluator

**Estado: esqueleto (2026-11-10).** Solo están las carpetas y la copia de `niif_observability`; no hay lógica todavía. Roadmap: `Tareas\T1-notificaciones\roadmap-notificaciones.md` (fase 3).

| | |
|---|---|
| Reemplaza a | `niif-dev-composite-notification-evaluator` |
| Código actual (referencia, solo lectura) | `Backend\Lamnbdas\niif-dev-composite-notification-evaluator\` |
| Repo de DevOps | `sc-datalake-niif-composite-notification-evaluator` |
| Entrada | La invocan las Step Functions de las mallas (`ResolverNotificacion`, `ResolverRegeneracion`) y además lee el stream de `niif-<amb>-composite-notification-status`. |
| Brechas que cierra | ARQ-27 (`--ENVIRONMENT` con el texto `'{env}'` literal), ARQ-32 (propagar el `ejecucion_id`) |

## Qué hace
- Decide si un caso es simple o compuesto (Rentas + Endosos, ARP + RVI) y, cuando el grupo está completo, lanza un solo job de certificación o notificación.
- Lleva el control del grupo (`READY_TO_NOTIFY`, `NOTIFICATION_IN_PROGRESS`, `NOTIFICATION_COMPLETED`).

No está en el diagrama (ARQ-21).

## Estructura
```
src/
├── lambda_function.py          punto de entrada que pide DevOps (pendiente): solo llama al handler de inbound
├── niif_observability/         copia de la librería común (no editar aquí sin copiarla a todos; ver verificar_copias_comunes.py)
└── composite_evaluator/
    ├── domain/                 reglas puras: estados, validaciones, errores con su clasificación
    ├── application/            casos de uso y puertos (Protocol)
    ├── adapters/inbound/       handler del evento (stream, API o programado)
    ├── adapters/outbound/      DynamoDB, S3, Glue, etc.
    └── config/                 lectura del entorno y raíz de composición
test/                           pruebas (test_lambda_function.py y el resto)
config/                         lambda.dev.json, lambda.qa.json, lambda.prd.json
eventos/                        eventos de ejemplo
scripts/                        package.sh: arma dist/niif-dev-composite-notification-evaluator.zip
```
Normas: `Docs\08-estandares-desarrollo\estandar-lambdas-python.md` (hexagonal, PEP 8 en inglés, logs en español con `ejecucion_id`, `cert_code` y `request_id`).
