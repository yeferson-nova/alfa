# niif-dev-post-login

**Estado: esqueleto (2026-11-10).** Solo están las carpetas y la copia de `niif_observability`; no hay lógica todavía. Roadmap: `Tareas\T1-notificaciones\roadmap-notificaciones.md` (fase 3).

| | |
|---|---|
| Reemplaza a | `niif-post-login`. Pasa al estándar como `niif-<amb>-post-login` (decisión del usuario, 2026-11-10) |
| Código actual (referencia, solo lectura) | `Backend\Lamnbdas\niif-post-login\` |
| Repo de DevOps | `sc-datalake-niif-post-login` |
| Entrada | API Gateway: `GET /post_login` (vuelta de Cognito con Azure AD). |
| Brechas que cierra | SEG-10 (flujo implícito: pasar a código con PKCE), SEG-11 (`state` sin firmar) |

## Qué hace
- Recibe al usuario autenticado y llama a certificar o no certificar con el token.

El cambio de nombre lo aplica DevOps: crea `niif-<amb>-post-login`, apunta a ella la integración `GET /post_login` del API y retira `niif-post-login`. El callback de Cognito no cambia, porque apunta a la URL del API y no al nombre de la lambda.

## Estructura
```
src/
├── lambda_function.py          punto de entrada que pide DevOps (pendiente): solo llama al handler de inbound
├── niif_observability/         copia de la librería común (no editar aquí sin copiarla a todos; ver verificar_copias_comunes.py)
└── post_login/
    ├── domain/                 reglas puras: estados, validaciones, errores con su clasificación
    ├── application/            casos de uso y puertos (Protocol)
    ├── adapters/inbound/       handler del evento (stream, API o programado)
    ├── adapters/outbound/      DynamoDB, S3, Glue, etc.
    └── config/                 lectura del entorno y raíz de composición
test/                           pruebas (test_lambda_function.py y el resto)
config/                         lambda.dev.json, lambda.qa.json, lambda.prd.json
eventos/                        eventos de ejemplo
scripts/                        package.sh: arma dist/niif-dev-post-login.zip
```
Normas: `Docs\08-estandares-desarrollo\estandar-lambdas-python.md` (hexagonal, PEP 8 en inglés, logs en español con `ejecucion_id`, `cert_code` y `request_id`).
