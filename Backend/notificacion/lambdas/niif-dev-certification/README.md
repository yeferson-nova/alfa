# niif-dev-certification

**Estado: esqueleto (2026-11-10).** Solo están las carpetas y la copia de `niif_observability`; no hay lógica todavía. Roadmap: `Tareas\T1-notificaciones\roadmap-notificaciones.md` (fase 3).

| | |
|---|---|
| Reemplaza a | `niif-dev-certification` |
| Código actual (referencia, solo lectura) | `Backend\Lamnbdas\niif-dev-certification\` |
| Repo de DevOps | `sc-datalake-niif-certification` |
| Entrada | API Gateway `datalake-dev-niif-api-gtw`: `POST /certify` y `POST /not_certify`, con autorizador de Cognito. |
| Brechas que cierra | ARQ-33 (no hay log del resultado), SEG-12 (escribe en el log el evento y los claims con el correo) |

## Qué hace
- Valida que el usuario autenticado sea aprobador del caso y registra su voto en la tabla de auditoría (multiaprobador).
- Con las aprobaciones completas, cambia el estado a `CERTIFICATE` o `NO_CERTIFICATE`. El resto lo hace la orquestadora.
- Responde 403 (no aprobador), 409 (ya procesado), 404 o 200, y **deja un log del resultado** en cada caso.

Va separada de la orquestadora mientras se decide D7: el diagrama dibuja el API (66) llegando a la misma lambda de notificación. Quitar numpy y pandas del paquete.

## Estructura
```
src/
├── lambda_function.py          punto de entrada que pide DevOps (pendiente): solo llama al handler de inbound
├── niif_observability/         copia de la librería común (no editar aquí sin copiarla a todos; ver verificar_copias_comunes.py)
└── certification_decision/
    ├── domain/                 reglas puras: estados, validaciones, errores con su clasificación
    ├── application/            casos de uso y puertos (Protocol)
    ├── adapters/inbound/       handler del evento (stream, API o programado)
    ├── adapters/outbound/      DynamoDB, S3, Glue, etc.
    └── config/                 lectura del entorno y raíz de composición
test/                           pruebas (test_lambda_function.py y el resto)
config/                         lambda.dev.json, lambda.qa.json, lambda.prd.json
eventos/                        eventos de ejemplo
scripts/                        package.sh: arma dist/niif-dev-certification.zip
```
Normas: `Docs\08-estandares-desarrollo\estandar-lambdas-python.md` (hexagonal, PEP 8 en inglés, logs en español con `ejecucion_id`, `cert_code` y `request_id`).
