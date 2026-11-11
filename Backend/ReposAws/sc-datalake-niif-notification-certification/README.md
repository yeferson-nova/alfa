# Lambda: Move S3 (Rejected Certifications)

Esta función Lambda gestiona los archivos que no fueron certificados. Reacciona a eventos de DynamoDB Streams y mueve los archivos en S3 a una subcarpeta de seguridad, actualizando el estado en la base de datos.

## Funcionalidad Principal

Cuando un aprobador rechaza una certificación (estado `NO_CERTIFICATE`), esta Lambda:

1. **Identificación**: Localiza todos los registros asociados al `certCode` en la tabla de notificaciones.
2. **Movimiento en S3**: Mueve los archivos hacia una subcarpeta `NO-CERTIFICADOS/`.
   - Origen: `s3://{REPORT_BUCKET}/{useCaseId}/{nameFile}`
   - Destino: `s3://{REPORT_BUCKET}/{useCaseId}/NO-CERTIFICADOS/{nameFile}`
3. **Limpieza**: Elimina el archivo original después de confirmar la copia exitosa.
4. **Actualización de Estado**: Cambia el estado del registro en DynamoDB a `NO_TRANSPORT`.

## Variables de Entorno

| Variable | Descripción |
|----------|-------------|
| `REPORT_BUCKET` | Bucket S3 donde se almacenan los reportes |
| `NOTIFICATIONS_TABLE` | Tabla DynamoDB de notificaciones de certificación |
| `ENVIRONMENT` | Ambiente de ejecución (dev, qa, prd) |

## Pruebas Unitarias

Se utiliza Pytest con Mocks para simular servicios AWS. Cobertura mínima requerida: **70%**.

```bash
# Ejecución local
pip install -r requirements-dev.txt
pytest tests -v --cov=src --cov-fail-under=70

# Ejecución con Docker
docker build -t move-s3-test -f Dockerfile.tests .
docker run --rm move-s3-test
```

## Despliegue y CI/CD

Esta Lambda se despliega como un **paquete ZIP en S3**.

1. **Empaquetado**: Jenkins toma todo el contenido de `src/` + dependencias de `requirements.txt` y genera un ZIP.
2. **Upload**: El ZIP se sube a `s3://niif-{env}-assets/lambdas/move-s3/{nombre}.zip`.
3. **Actualización**: Jenkins ejecuta `aws lambda update-function-code` para que la función tome el nuevo código.
4. **Calidad**: Antes del despliegue se ejecutan tests unitarios (cobertura ≥ 70%) y análisis SonarQube con Quality Gate.

### Pipeline (Jenkinsfile)

El pipeline usa la shared library `lambdaS3Pipeline` que ejecuta:

```
Unit Test → SonarQube → Package ZIP → Upload S3 → Update Lambda
```

## Estructura del Proyecto

```
├── src/
│   └── move_s3.py          # Código fuente de la Lambda
├── tests/
│   └── test_lambda_function.py
├── Jenkinsfile              # Pipeline CI/CD
├── Dockerfile.tests         # Contenedor para tests
├── requirements.txt         # Dependencias de producción
├── requirements-dev.txt     # Dependencias de desarrollo/testing
├── sonar-project.properties # Configuración SonarQube
└── tox.ini                  # Configuración pytest/cobertura
```

## Consideraciones de Idempotencia

La Lambda verifica existencia (`head_object`) antes de mover. Si un reintento del Stream reprocesa un archivo ya movido, detecta que existe en `NO-CERTIFICADOS/` y omite la operación.
