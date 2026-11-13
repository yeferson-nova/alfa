---
tipo: repo
estado: vigente
actualizado: 2026-11-12
repo: iac-analytics-datalake-niif-operations-manager
categoria: iac
paso: "2 y 3 · Bodega NIIF 17 y Certificación Cifras (gestor operativo)"
malla: transversal
---
# iac-analytics-datalake-niif-operations-manager
> Terraform con Terragrunt del "gestor operativo" NIIF 17: las lambdas configuradoras y su programador, las Step Functions de las mallas, los jobs de Glue, las tablas de configuración y los recursos de notificación y certificación.

## Rol en el flujo
- Lo despliega Jenkins (`iacPipelineNiif`, workspaces `dev`, `qa`, `prd`). Crea las funciones con un ZIP vacío de relleno; el código real lo sube cada repo `sc-datalake-niif-*` desde su propio pipeline.
- Completa a `iac-analytics-datalake-niif` (infraestructura base); el solapamiento de recursos entre ambos: por confirmar. Aquí usa el prefijo `niif-{env}-...`, que es el del gobierno ([[est-gobierno-devops-niif]]).

## Entradas y salidas
- Entra: `parameters.tf` por stack con valores `default`, `dev`, `qa`, `prd` y los interruptores `create_*`; lee por SSM la red, la seguridad, el almacenamiento, Oracle y API Gateway.
- Sale: funciones, máquinas de estado, tablas y roles; en `conf_manager` el programador de schedules para las configuradoras.

## Recursos AWS que declara (un stack por carpeta en `stacks/`)
- `serverless/lambdas/conf_*`: `niif-{env}-configuration-primas-arl/-primas-endosos/-primas-rentas/-auxilios-funerarios/-reserva-matematica-arp/-reserva-matematica-rvi` (handler `conf_<malla>.lambda_handler`, con `MALLA_PK`, `PRG_PK` y la Step Function), `niif-{env}-configuration-general` (dev, qa y prd) y `niif-{env}-configurador-mallas` (solo dev). Son las lambdas de las fichas [[sc-datalake-niif-primas-arl]], [[sc-datalake-niif-configuration-general]] y [[sc-datalake-niif-configurador-mallas]].
- `conf_manager`: `niif-{env}-manager-configuration`, disparada por el stream de `niif-{env}-prg-programaciones`; crea el grupo de EventBridge Scheduler `niif-{env}-programacion-configuradores` y su rol.
- `serverless` (evaluator, certification, copy, move, notification, short-link-redirect): lambdas de la cadena de certificación y parámetros SSM de versión.
- `app-integration/step_functions`: nueve máquinas `niif-{env}-<malla>` (auxilios, ARL, endosos, rentas, ARP, RVI, previsional, PCR Vida, gestor de archivos). `sns`: tema `niif-{env}-certification-email`.
- `analytics`: catálogo, seis crawlers y unos 21 jobs de Glue (extracción, validación técnica, remediación, SCD, certificación, notificación, silver por malla, gold-scd). `database`: nueve tablas (notificación-certificación, enlaces cortos, estado compuesto, estado de pipeline, auditoría de aprobación, prs, cnf, ejc, prg) en los tres ambientes. `storage`: bucket de certificación.

## Código
- Módulos locales (`modules/terraform-aws-glue` de cloudposse) y de la comunidad (lambda, step-functions, dynamodb, IAM). Las definiciones de las Step Functions están como JSON en `stacks/app-integration/step_functions/datafiles/`. Sin pruebas propias.

## Estado
- En Alfa: `origin/quality`, commit `c33f40a` (2026-11-12). Vigente. Desajustes por confirmar: el handler de `configurador-mallas` no lleva el prefijo del paquete y no pasa las variables que el código nuevo exige.
- `quality` frente a `main` (medido en GitHub el 2026-11-12): 27 commits solo en `quality`, 5 solo en `main`, 19 archivos distintos; las ramas divergen, 19 archivos distintos. Ver ARQ-50 en [[auditoria-repos-2026-11]].
- Código nuevo nuestro: ninguno.

## Brechas y relacionadas
- **ARQ-49** declara `configurador-mallas` solo en dev y sin las variables que exige el código nuevo, en [[auditoria-repos-2026-11]].
- Sin IDs propios en las notas. El drift entre las seis Step Functions de DEV está en [[inventario-lambdas]]; el índice de DynamoDB que espera la orquestadora está en [[orquestadora-notificaciones]].
- Relacionado: [[iac-analytics-datalake-niif]], [[repos-alfa-notificacion]], [[estructura-repos-propuesta]], [[hu-devops]]
