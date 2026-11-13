---
tipo: referencia
estado: vigente
actualizado: 2026-11-03
---
# Entorno local de AWS con LocalStack

Para probar en local mientras no hay acceso a AWS ni a Git ([[preferencias-usuario]]).

## Configuración (verificado en archivos, 2026-11-03)
- **Carpeta:** `alfa-repos\scripts\localstack\`: `docker-compose.yml`, `.env` (token, **nunca leerlo ni copiarlo a notas**), `.env.example` y `.gitignore` (excluye `.env`).
- **Contenedor:** `localstack-niif17`, imagen `localstack/localstack:latest` (edición Pro: el log carga los plugins `l.pro.*`). Endpoint `http://127.0.0.1:4566`; salud en `/_localstack/health`.
- **`SERVICES` habilitados (14):** s3, lambda, ses, sns, sqs, events (EventBridge), stepfunctions, dynamodb, apigateway, logs, iam, kms, secretsmanager, ssm.
- **Docker:** monta `/var/run/docker.sock`, necesario para ejecutar lambdas.
- **El token se pegó en el chat;** el usuario puede regenerarlo en app.localstack.cloud.

## Estado
- El usuario reportó que estaba corriendo, con los 14 servicios disponibles y **sin recursos creados ni invocados**.
- Al verificar (2026-11-03) el contenedor estaba **detenido**: `Exited (255)`; el log terminaba en "Ready." sin error visible. Se inicia con `docker compose up -d` desde `alfa-repos\scripts\localstack\`.
- **Comandos:** `docker logs localstack-niif17`, `docker compose down`, `docker compose up -d`.

## No habilitado o limitado
- **Glue, Athena, Lake Formation:** desactivados por la lista `SERVICES`. No se ha probado si la licencia los soporta.
- **sesv2:** desactivado; solo SES v1. Hay que verificar qué API usan las lambdas.
- **CloudWatch (métricas y alarmas):** no activo, solo Logs. Hace falta para probar alarma → SNS ([[monitoreo-grafana]]).
- **Firehose:** no habilitado. Hace falta para el plano de negocio (SNS → Firehose → S3).
- **Sin persistencia:** no hay `PERSISTENCE` ni volumen de datos, así que los recursos se pierden al reiniciar. Imagen `latest` sin versión fija.

## Qué se puede probar por tarea ([[tareas-aws]])
- **T1:**
  - correo con SES v1 (`SendEmail`, `SendTemplatedEmail`; LocalStack captura los correos sin enviarlos);
  - plantillas de `alfa-repos\s3\tpl-datalake-niif-email-template-s3\src\email-template\`;
  - SNS con suscriptores Lambda o SQS;
  - API Gateway + Lambda para Certificar / No certificar;
  - Step Functions con `waitForTaskToken`.
- **T2:** modelo único de lambdas con configuración en DynamoDB o SSM, secretos en Secrets Manager con KMS, logs en Logs.
- **T3:** Lambda de Validación sobre archivos en S3 y manifiesto ([[riesgos-glue-notificacion]]). Glue se simula subiendo su salida a S3.
- **Fuera de alcance local:** ServiceNow (se simula con un stub en API Gateway), Glue y Athena reales, alarmas de CloudWatch (mientras no se habiliten).
