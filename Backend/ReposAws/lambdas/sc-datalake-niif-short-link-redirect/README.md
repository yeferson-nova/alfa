# sc-datalake-niif-short-link-redirect

## Descripción

Lambda para redirección de short links de certificación NIIF17. Recibe un token por query string y redirige al URL asociado en DynamoDB.

## Endpoints

| Método | Path | Descripción |
|--------|------|-------------|
| GET | `/redirect?t={token}` | Redirección por token |
| GET | `/r?t={token}` | Redirección por token (path corto) |

## Respuestas

| Código | Descripción |
|--------|-------------|
| 302 | Redirección exitosa |
| 400 | Token no proporcionado |
| 404 | Link no encontrado |
| 410 | Link expirado |

## Variables de entorno

| Variable | Descripción |
|----------|-------------|
| `TABLE_NAME` | Nombre de la tabla DynamoDB de short links |
| `ENVIRONMENT` | Ambiente de ejecución (dev/qa/prd) |

## Ejecución de tests

```bash
pip install -r requirements-dev.txt
pytest tests
```

## Estructura

```
├── src/
│   └── short_link_redirect.py
├── tests/
│   └── test_lambda_function.py
├── Jenkinsfile
├── Dockerfile.tests
├── tox.ini
├── sonar-project.properties
├── requirements.txt
├── requirements-dev.txt
└── event.json
```
