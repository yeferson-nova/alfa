---
tipo: repo
estado: vigente
actualizado: 2026-11-12
repo: cfg-datalake-niif-config-s3
categoria: s3
paso: "2 y 3 · Configuración de mallas, certificación y librerías de Glue"
malla: transversal
---
# cfg-datalake-niif-config-s3
> Repo de solo archivos: toda la configuración JSON de las mallas, los JSON de certificación y notificación, y las librerías de Glue. Jenkins lo sincroniza al bucket de assets.

## Rol en el flujo
- No ejecuta código. Es la fuente versionada de lo que leen los jobs de Glue y las lambdas (paso 2 y paso 3). Pipeline `s3StaticDeployPipeline`: `aws s3 sync` de `src/` a la raíz de `niif-{env}-assets`, con `--delete` (borrar aquí borra en S3). La rama develop va a DEV, quality a QA, main a PRD.
- Lo leen: extracción, validación, SCD, remediación, certificación, notificación y las lambdas configuradoras.

## Entradas y salidas
- Contiene por malla (`primas-rentas`, `primas-endosos`, `primas-arl`, `auxilios-funerarios`, `reserva-matematica-arp`, `reserva-matematica-rvi`, `pcr_vida`, `previsional`): configuración de extracción, validación técnica, SCD, remediación, plantillas versionadas y un `parametros_extraccion.json` inicial.
- JSON de caso de certificación (`primasArl.json`, `auxilioFunerario.json`, `reservaMatematicaRviArp.json`, `previsional.json`, etc.): consultas Athena, asunto, destinatarios, `templatePath` y enlaces por ambiente (`envConfig`).
- `configs/certification/json_global_notif_cert.json` (ambiente, buckets, tablas DynamoDB, Athena, URL de consola, estados) y `composite_groups.json` (grupos Rentas + Endosos y Reserva RVI + ARP).
- `glue-resources/`: `commons.zip`, wheelhouse (`openpyxl`, `pyxlsb`, `et_xmlfile`) y `niif17_campos_unica_vez.json`.

## Recursos AWS que toca
- Bucket `niif-{env}-assets`. Los JSON nombran las tablas `niif-{env}-notification-certification` y `niif-{env}-composite-notification-status`.

## Código
- Sin código Python. `Jenkinsfile` (despliegue por ambiente), README genérico de despliegue y `sonar-project.properties`. No se valida la sintaxis JSON en el pipeline.
- Diseño irregular: Primas ARL tiene ruta plana y también `configs/<tipo>/v1` y `v2`; hay carpetas de respaldos y de `versiones_post_alineacion`. Los `parametros_extraccion.json` son estado de runtime versionado en Git, por confirmar si el pipeline los pisa en cada ejecución.

## Estado
- En Alfa: `origin/quality`, commit 0a09b4d (2026-11-12). Desplegado: por confirmar por ambiente.
- `quality` frente a `main` (medido en GitHub el 2026-11-12): 6 commits solo en `quality`, 6 solo en `main`; ramas divergentes. Ver ARQ-50 en [[auditoria-repos-2026-11]].
- Código nuevo nuestro: ninguno en la rama; las correcciones de `templatePath` y del espacio de Auxilios se documentaron como cambios de copia ([[repos-alfa-notificacion]], R-01 a R-03).

## Brechas y relacionadas
- Enlace Certificar de Auxilios con espacio en el host de Cognito: [[inprod-enlaces-cognito-9411325]]. El `stringVars` apunta al API de DEV en QA (bloquea los botones): [[repos-alfa-notificacion]].
- Primas ARL con resumen vacío (`queries.summary` vacío): [[inprod-primas-arl-202606]].
- Relacionado: [[tpl-datalake-niif-email-template-s3]], [[familia-mallas-alfaprd]], [[repo-alfa-repos]]
