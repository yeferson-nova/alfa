# Datalake send file via SFTP Lambda Function

Lambda function que envía archivos desde AWS S3 hacia un servidor SFTP,
conservando el nombre original del archivo y organizándolo bajo un directorio
base configurable. Se dispara a partir de eventos S3 (`aws:s3`) y transfiere
cada objeto por streaming (sin almacenarlo en disco).

## Estructura del Proyecto

```
src/
 └── lambda_function.py      # Handler principal (descarga de S3 y subida por SFTP)

tests/
 └── test_lambda_function.py # Pruebas unitarias (pytest + moto)

Dockerfile                   # Imagen de la Lambda (runtime)
Dockerfile.tests             # Imagen para ejecutar las pruebas
requirements.txt             # Dependencias de runtime
requirements-dev.txt         # Dependencias de runtime + testing
tox.ini                      # Configuración de pytest / coverage
sonar-project.properties     # Configuración de análisis SonarQube
Jenkinsfile                  # Pipeline CI/CD
```

## Variables de Entorno

| Variable | Descripción | Requerida | Default | Ejemplo |
|----------|-------------|:---------:|---------|---------|
| `SFTP_HOST` | Host del servidor SFTP | Sí | n/a | `sftp.example.com` |
| `SFTP_PORT` | Puerto del servidor SFTP | No | `22` | `22` |
| `SFTP_BASE_DIR` | Directorio base remoto donde se depositan los archivos | No | `/` | `/upload` |
| `CREDS_SECRET_ID` | ID del secreto en Secrets Manager con las credenciales SFTP | Sí | n/a | `prod/sftp/credentials` |
| `ENABLE_SSH_RSA_FALLBACK` | Deshabilita `rsa-sha2-*` para servidores legacy que sólo publican `ssh-rsa` | No | `true` | `true` |

## Estructura del Secreto (Secrets Manager)

```json
{
  "username": "sftp_user",
  "password": "sftp_password"
}
```

## Comportamiento

- Carga las credenciales desde Secrets Manager (`CREDS_SECRET_ID`).
- Verifica reachability TCP contra `SFTP_HOST:SFTP_PORT` antes de conectar.
- Abre la conexión SFTP vía `paramiko`, con reintento simple si falla por
  algoritmos deshabilitados.
- Por cada registro S3 del evento: descarga el objeto por streaming y lo sube
  al SFTP en `SFTP_BASE_DIR/<último_subdirectorio>/<archivo>`, creando el árbol
  de directorios remoto si no existe.
- Retorna un resumen con `successes`, `failures` y `elapsed_s`. Los errores en
  las etapas de `secrets` o `connect` se devuelven de forma controlada.

## Instalación de Dependencias

```bash
pip install -r requirements-dev.txt
```

## Ejecución de Pruebas

Las pruebas usan `pytest`, mockean S3 y Secrets Manager con `moto`, y simulan
la capa SFTP (no abren conexiones reales).

Localmente:

```bash
pytest tests
# o
make test
```

Con Docker (igual que el pipeline):

```bash
docker build -t send_via_sftp_lambda_tests -f Dockerfile.tests .
docker run -v ./reports:/app/reports --rm send_via_sftp_lambda_tests
```

Al ejecutarse se generan los reportes consumidos por SonarQube:

- `reports/junit_report.xml` (resultados de pruebas)
- `reports/coverage.xml` (cobertura)
- `reports/htmlcov/` (cobertura en HTML)

## Build y Deployment

### Construcción de imagen

```bash
docker build -t send_via_sftp_lambda:latest .
```

### Push a ECR

```bash
# Login a ECR
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com

# Tag
docker tag send_via_sftp_lambda:latest ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/send_via_sftp_lambda:latest

# Push
docker push ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/send_via_sftp_lambda:latest
```

## Comandos Make (opcional)

```bash
make runTest       # Ejecutar pruebas (pytest tests)
```
