---
tipo: repo
estado: vigente
actualizado: 2026-11-12
repo: iac-analytics-datalake-niif
categoria: iac
paso: "2 y 3 · Bodega NIIF 17 y Certificación Cifras (infraestructura base)"
malla: transversal
---
# iac-analytics-datalake-niif
> Terraform con Terragrunt que declara la infraestructura base del datalake NIIF 17: almacenamiento, tablas, secretos y Cognito, catálogo y jobs de Glue, API Gateway con WAF y las lambdas SFTP.

## Rol en el flujo
- Lo despliega Jenkins (`iacPipeline`, workspaces `dev`, `qa`, `prd`, credencial por ambiente). Los repos de código lo invocan como su pipeline de IaC (por ejemplo `get-files-sftp` apunta a su stack).
- Convive con `iac-analytics-datalake-niif-operations-manager`, que declara el gestor operativo (mallas, notificación y certificación); aquí queda lo base. Qué recurso es de cuál repo hay que verlo caso por caso (por confirmar): ambos declaran catálogo y jobs de Glue, crawlers y lambdas.

## Entradas y salidas
- Entra: parámetros por ambiente en `parameters.tf` de cada stack (`default` más `dev`, `qa`, `prd`) y variables comunes en `common/` (prefijo `datalake`, proyecto `niif`, etiquetas, estado remoto por ambiente). Lee por SSM la red, la seguridad y el almacenamiento de la cuenta.
- Sale: recursos AWS, parámetros SSM de versión de imágenes y salidas de Terraform.

## Recursos AWS que declara (un stack por carpeta en `stacks/`)
- `storage`: buckets de certificación, `ss`, `pcr`, `con`, `prm`, landing de Prophet y `niif-{env}-accountingtool`; en prd solo certificación y accountingtool.
- `database`: DynamoDB `datalake-{env}-use-case-emails/queries/certification/approvers`, tablas de artefactos, conceptos y notas, y `adm-*` (cargues, periodos, extracción, malla-ejecución); en prd la lista está vacía.
- `security`: secretos JDBC (varios Oracle) y de credenciales SFTP, y el user pool de Cognito con su dominio y cliente (callbacks a dos API Gateway).
- `analytics`: catálogo de Glue, conexiones, 13 crawlers, unos 28 jobs de Glue (bronce, plata, oro y certificación, con roles IAM por capa) y parámetros SSM.
- `application/api_gateway`: `datalake-{env}-niif-api-gtw` con WAF y rutas `/r`, `/redirect`, `/certify`, `/not_certify`, `/post_login`; definiciones OpenAPI por ambiente.
- `serverless`: repositorios ECR y parámetros de versión (notification, post-login, certification, send-via-sftp, get-files-sftp) y las lambdas `send_via_sftp` y `get_files_sftp`; `operations`: grupo de recursos.

## Código
- Módulos locales (`modules/`: API Gateway, Cognito, resource groups) y de la comunidad (DynamoDB, S3, Secrets Manager, IAM, ECR, SSM, Glue de cloudposse). `terragrunt.hcl` por stack, `.pre-commit-config.yaml`, Sonar, guía de operaciones. Pruebas Go del módulo de API Gateway; no hay pruebas propias.

## Estado
- En Alfa: `origin/quality`, commit `5577df3` (2026-11-12). Vigente. Qué parte está aplicada en cada cuenta: por confirmar.
- Código nuevo nuestro: ninguno. Naming `datalake-{env}-...` frente al `niif-{env}-...` del gobierno ([[est-gobierno-devops-niif]]).

## Brechas y relacionadas
- **ARQ-49** declara recursos parecidos a los del otro repo IaC y no se sabe cuál manda, en [[auditoria-repos-2026-11]].
- El Cognito del repo lista dos API de callback: el viejo sin WAF es el caso SEG-14, en [[inventario-lambdas]].
- Relacionado: [[iac-analytics-datalake-niif-operations-manager]], [[hu-devops]], [[estructura-repos-propuesta]], [[sc-datalake-niif-get-files-sftp]], [[sc-datalake-niif-send-via-sftp-lambda]]
