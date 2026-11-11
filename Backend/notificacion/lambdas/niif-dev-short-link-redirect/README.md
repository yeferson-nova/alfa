# niif-dev-short-link-redirect

**Estado: esqueleto (2026-11-10).** Solo están las carpetas y la copia de `niif_observability`; no hay lógica todavía. Roadmap: `Tareas\T1-notificaciones\roadmap-notificaciones.md` (fase 3).

| | |
|---|---|
| Reemplaza a | `niif-dev-short-link-redirect` |
| Código actual (referencia, solo lectura) | `Backend\Lamnbdas\niif-dev-short-link-redirect\` |
| Repo de DevOps | `sc-datalake-niif-short-link-redirect` |
| Entrada | API Gateway: `GET /r` y `GET /redirect`. |
| Brechas que cierra | ARQ-29, SEG-13 (token de 8 caracteres; el job nuevo ya genera 32) |

## Qué hace
- Busca el token en `niif-<amb>-short-links`, valida la vigencia y redirige a la acción (certificar o no certificar).
- Descarga del CSV: acción `DOWNLOAD` después del login, con una URL prefirmada de corta duración (ARQ-29, pendiente de D3).

El paquete desplegado hoy trae boto3 y botocore dentro; no hace falta, porque Lambda ya los incluye.

## Estructura
```
src/
├── lambda_function.py          punto de entrada que pide DevOps (pendiente): solo llama al handler de inbound
├── niif_observability/         copia de la librería común (no editar aquí sin copiarla a todos; ver verificar_copias_comunes.py)
└── short_link_redirect/
    ├── domain/                 reglas puras: estados, validaciones, errores con su clasificación
    ├── application/            casos de uso y puertos (Protocol)
    ├── adapters/inbound/       handler del evento (stream, API o programado)
    ├── adapters/outbound/      DynamoDB, S3, Glue, etc.
    └── config/                 lectura del entorno y raíz de composición
test/                           pruebas (test_lambda_function.py y el resto)
config/                         lambda.dev.json, lambda.qa.json, lambda.prd.json
eventos/                        eventos de ejemplo
scripts/                        package.sh: arma dist/niif-dev-short-link-redirect.zip
```
Normas: `Docs\08-estandares-desarrollo\estandar-lambdas-python.md` (hexagonal, PEP 8 en inglés, logs en español con `ejecucion_id`, `cert_code` y `request_id`).
