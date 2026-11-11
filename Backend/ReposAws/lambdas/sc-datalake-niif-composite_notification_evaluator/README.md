# 📨 Datalake Niif -  Certification Notification Lambda Function

## 📌 Descripción

Esta función **Lambda** Encargada solo de activar el job niif-{dev}-notificacion-certification solo cuando esta en estado PENDING en el status tabla niif-qa-notification-certification, si esta en estado PROCESS no debe lanzar la petición.

## 🧩 Funcionalidades

- Pendiente trigger de tabla  **DynamoDB**.
- valida estado de campo status = PENDING 
- Lanzar o disparar el job niif-{dev}-notificacion-certification
  - Pasar parámetros hacia el job

## ⚙️ Requisitos

### 🔐 Variables de entorno

| Variable                     | Descripción                                          |
|-----------------------------|------------------------------------------------------|
| `ENVIRONMENT`               | Nombre del entorno (e.g., `dev`, `prod`)            |
| `ASSETS_BUCKET`             | Bucket S3 donde están las plantillas HTML           |
| `REPORT_BUCKET`             | Bucket donde se almacenan los Excel exportados      |
| `EMAIL_DYNAMODB_TABLE`      | Tabla DynamoDB con la definición del correo         |
| `QUERY_DYNAMODB_TABLE`      | Tabla con queries SQL parametrizados                |
| `CERTIFICATION_DYNAMODB_TABLE` | Tabla que guarda los registros de certificación |

### 📦 Dependencias

- `pandas`
- `botocore`
- `Template` (de `string`)
- `zoneinfo` (Python 3.9+)

## 🧪 Pruebas unitarias

Para ejecutar las pruebas:

```bash
docker build -t notification_lambda_test -f Dockerfile.tests .
docker run -v ./reports:/app/reports notification_lambda_test
```

Se usa `pytest-cov` para generar cobertura compatible con SonarQube.

## 📥 Formato del evento de entrada

```json
{
  "useCaseId": "auxilioFunerario",
  "period": "202401",
  "emailType": "cert"
}
```

| Campo       | Requerido | Descripción                                              |
|-------------|-----------|----------------------------------------------------------|
| `useCaseId` | ✅         | Identificador funcional del caso de uso                 |
| `period`    | ✅         | Periodo de reporte en formato `yyyymm`                  |
| `emailType` | ✅         | Tipo de correo (`cert` para registrar certificación)    |

## 📤 Salida esperada

```json
{
  "statusCode": 200,
  "body": {
    "Status": "Enviado",
    "Subject": "Asunto del correo",
    "Destination": ["usuario@dominio.com"]
  }
}
```

## 🚨 Manejo de errores

- `400`: Parámetros faltantes.
- `404/400`: Problemas al consultar DynamoDB.
- `500+`: Errores inesperados en ejecución de queries o envío.

## 🚀 Publicación de imagen Docker para Lambda

Puedes construir y subir la imagen con los siguientes comandos:

```bash
# 1. Inicia sesión en ECR
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin <account_id>.dkr.ecr.us-east-1.amazonaws.com
# ej: aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 533267035849.dkr.ecr.us-east-1.amazonaws.com

# 2. Construye la imagen Docker
docker build -t notification_lambda .

# 3. Etiqueta la imagen con el repositorio de ECR
docker tag notification_lambda:latest 533267035849.dkr.ecr.us-east-1.amazonaws.com/datalake_dev_niif_notification:latest

# 4. Publica la imagen en ECR
docker push 533267035849.dkr.ecr.us-east-1.amazonaws.com/datalake_dev_notification:latest
```

Reemplaza `<account_id>` con tu ID de cuenta de AWS.

Para ejecutar esta Lambda desde imagen:

- Asegúrate de haber configurado correctamente el handler como `lambda_function.lambda_handler` en el `Dockerfile`.
- Establece las variables de entorno indicadas en la sección anterior.

## 🗂️ Estructura de tablas DynamoDB

Ver archivo: [dynamodb_structures.json](docs/dynamodb_structures.json)

## 📂 Archivos incluidos

- `dynamodb_structures.json` – Estructura esperada de las tablas DynamoDB.

---
