# niif-dev-notification-certification-orchestrator

**Estado: esqueleto (2026-11-10).** Solo están las carpetas y la copia de `niif_observability`; no hay lógica todavía. Roadmap: `Tareas\T1-notificaciones\roadmap-notificaciones.md` (fase 3).

| | |
|---|---|
| Reemplaza a | `niif-dev-notification-certification-trigger`, `niif-dev-copy-s3-cert` y `niif-dev-move-s3-certification`, que se retiran (D1) |
| Código actual (referencia, solo lectura) | `Backend\Lamnbdas\niif-dev-notification-certification-trigger\`, `Backend\Lamnbdas\niif-dev-copy-s3-cert\`, `Backend\Lamnbdas\niif-dev-move-s3-certification\` |
| Repo de DevOps | `sc-datalake-niif-notification-certification-orchestrator` |
| Entrada | Stream de DynamoDB de `niif-<amb>-notification-certification` (un solo mapping en lugar de los 3 de hoy). Además, un disparo programado para el reprocesador. |
| Brechas que cierra | ARQ-23, ARQ-24, ARQ-25, ARQ-26, ARQ-30, SEG-12 |

## Qué hace
- `PENDING` (primer registro, sin `sendLock`): lanza el job de Glue `niif-<amb>-notification-certification` con `--EJECUCION_ID`.
- `CERTIFICATE`: copia el CSV a `niif-<amb>-transport/export/<carpeta>/` (luego el SFTP a FIS) y marca `TRANSPORT`.
- `NO_CERTIFICATE`: archiva en `NO-CERTIFICADOS`, marca `NO_TRANSPORT` y deja pendiente la programación de la malla (ARQ-23, pendiente de D2).
- Reprocesador: vuelve a lanzar los registros `PENDING` atascados, hasta un máximo de intentos, y si se agotan alerta a soporte (D5, parametrizado).

Recurso nuevo con nombre propio (decisión del usuario, 2026-11-10). Cambio en AWS: DevOps la crea con su mapping del stream deshabilitado, apaga los mappings de trigger, copy y move, y activa el nuevo; la vuelta atrás es el paso inverso. Usa `ReportBatchItemFailures`, cola de fallos y acciones idempotentes. El mapa de carpetas va en un solo lugar (hoy está duplicado en copy y move).

## Estructura
```
src/
├── lambda_function.py          punto de entrada que pide DevOps (pendiente): solo llama al handler de inbound
├── niif_observability/         copia de la librería común (no editar aquí sin copiarla a todos; ver verificar_copias_comunes.py)
└── certification_orchestrator/
    ├── domain/                 reglas puras: estados, validaciones, errores con su clasificación
    ├── application/            casos de uso y puertos (Protocol)
    ├── adapters/inbound/       handler del evento (stream, API o programado)
    ├── adapters/outbound/      DynamoDB, S3, Glue, etc.
    └── config/                 lectura del entorno y raíz de composición
test/                           pruebas (test_lambda_function.py y el resto)
config/                         lambda.dev.json, lambda.qa.json, lambda.prd.json
eventos/                        eventos de ejemplo
scripts/                        package.sh: arma dist/niif-dev-notification-certification-orchestrator.zip
```
Normas: `Docs\08-estandares-desarrollo\estandar-lambdas-python.md` (hexagonal, PEP 8 en inglés, logs en español con `ejecucion_id`, `cert_code` y `request_id`).
