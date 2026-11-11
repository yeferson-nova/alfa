---
tipo: contexto
actualizado: 2026-11-11
---
# Repos de Alfa de la cadena de notificación (validados el 2026-11-11)

Copia de lectura en `Backend\ReposAws\` (ver su `LEEME.md`). `VERSIONES.md` salió vacío, así que **falta saber de qué rama es cada copia** (develop, quality o main). Despliegue: Jenkins con `analitica-library`. La rama `develop` va a DEV, `quality` a QA y `main` a PRD.

## Comparación con lo desplegado (hash sin `\r`)
| Repo | Contenido | Resultado |
|---|---|---|
| `data-datalake-niif-notification-certification-glue-job` | `src/jobs/notification_certification.py` | **Versión vieja con SNS** (`ffa8…`). Hay que cambiarla por la de SES del paquete (`1c6d…`) |
| `tpl-datalake-niif-email-template-s3` | `src/email-template/*.html` (6) | **Idénticas** a las del paquete |
| `sc-…-notification-certification` (trigger), `-composite_notification_evaluator`, `-certification`, `-copy-s3-certification`, `-mov-s3-certification`, `-short-link-redirect` | `src/*.py` | Idénticos al código desplegado en DEV (`EvidenciasAws\dev-codigo-actual\`). No cambian |
| `sc-datalake-niif-send-via-sftp-lambda` | `src/lambda_function.py` | Sin comparar (no está en la evidencia de DEV) |
| `cfg-datalake-niif-config-s3` | `src/**` (JSON de caso, global y `glue-resources`) | Ver hallazgos |

## Hallazgos
- **R-01, `templatePath`:** el repo tiene `email-templates/` (plural) en 7 JSON, `email_templates/` en Previsional y solo `email-template/` (singular) en Reserva Matemática. El repo de plantillas despliega en `email-template/`. Hay que corregir los JSON.
- **R-02, espacio en Auxilios:** `email.envConfig.prd.certificarLink` tiene `datalake- prd-niif`. Está en `auxilios-funerarios/auxilioFunerario.json` y en su copia de `configs/certification/`. Ver [[inprod-enlaces-cognito-9411325]].
- **R-03, `envConfig`:** cada JSON trae `envConfig.{dev,qa,prd}`, con el enlace propio de cada ambiente:
  - DEV usa el API `hfoy06vw40` y el cliente `36re…`;
  - QA, el API `d5qmm9fi64` y el cliente `48laf…`;
  - PRD, el API `jdwkua9re5` y el cliente `5mo3…`.
  - Ningún código lee `envConfig`, y `stringVars` trae `{env}`, que `Template` no reemplaza.
  - En S3 de prod, `stringVars` tiene el valor de prod, espacio incluido. **Hipótesis:** el pipeline (`s3StaticDeployPipeline`) copia `envConfig[<ambiente>]` en `stringVars` y reemplaza `{env}`. Hay que confirmarlo con DevOps.
  - Inconsistencias: en Previsional, `dev.nocertificarLink` tiene `dev}`; en `primas-arl/primasArl.json`, QA usa `{env}` en vez de `qa`; en Reserva Matemática, la llave de DEV se llama `{env}` en vez de `dev`.
- **R-04, bucket de plantillas:** el `Jenkinsfile` de `tpl-…` despliega en `alfa-niif-<env>-assets`, pero el job lee de `niif-<env>-assets` (lo usan el repo `cfg-` y el de Glue). Si es así, las plantillas no llegan a donde el job las busca. Confirmar con DevOps.
- **R-05, `--delete`:** el README de `cfg-`/`tpl-` dice que el pipeline sincroniza con `--delete`. `cfg-` despliega en la raíz de `niif-<env>-assets`. Si es literal, borraría lo que no está en el repo, como `email-template/` y los scripts de `glue-resources/notification/`. Confirmar con DevOps antes de subir.
- **R-06, repo de Glue:**
  - despliega `src/` en `glue-resources/notification/`, y el script queda en `src/jobs/`. Confirmar que la ruta final coincide con `ScriptLocation`;
  - el README es una copia del de Primas Endosos;
  - no hay carpeta `tests/` y el `tox.ini` tiene `fail_under=0`;
  - las 104 pruebas del script SES están en `Backend\Glue\niif-dev-notification-certification\tests\`.
- Los argumentos del job (`--SES_SENDER`) y los permisos no están en estos repos, sino en Terraform (pendiente).
