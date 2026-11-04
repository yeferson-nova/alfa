---
tipo: estandar
actualizado: 2026-11-03
---
# Manual de recetas en AWS Glue DataBrew

Fuente: `Docs\02-estandares-banco\aws\Manual de configuración para construcción de recetas en AWS GlueDataBrew.pdf`. Manual operativo (paso a paso de consola), sin código ni versión. Plataforma de "Nuevos Cargues", ver [[est-nuevos-cargues]].

## Convenciones que fija
- Acceso por IAM Identity Center (`ssoath-lz.awsapps.com`), cuenta `ALFA-DATA-PREP-DEV`, región **us-east-1 (N. Virginia)**.
- Bucket `dataprep-<amb>-agreementbroker-s3` con carpetas `Agreements/` (entrada por cartera), `mail_template/` (plantillas de correo), `OutPuts/` (salida por cartera), `Parameters/` (Divipola, tarifas).
- Nombres DataBrew: `Dataset<Nombre>`, `ProfileJob<Nombre>`, `Reglas<Nombre>` (reglas DQ), `Proyecto<Nombre>`, `Receta<Nombre>`, `RecipeJob<Nombre>`, rol IAM `dataprep<Nombre>`.
- Job de perfil con reglas DQ antes de aplicar la receta; muestreo recomendado 1121 filas.
- Salida: S3, CSV, coma, **sin compresión**, archivo único, reemplazo en cada ejecución; **CloudWatch Logs habilitado** obligatorio.
- Cada cambio de receta se publica con comentario (versión de receta).

## Brechas candidatas (propuesta, para la fase 3)
- **ARQ/SEG:** recursos e **IAM roles creados a mano desde consola**, contrario a IaC con Terraform y a identidades no humanas gobernadas ([[hu-devops]] HU-DO-09/11, [[hu-seguridad-acceso]] HU-SG-21).
- **COS:** salida CSV sin compresión; si Athena la consulta, escanea más (ver [[hu-seguridad-acceso]] HU-SG-11).
- **SEG:** el manual no menciona cifrado (KMS) del bucket ni de la salida.
- **ARQ:** nombres en CamelCase, distintos del patrón de recursos AWS `<prefijo>-<amb>-<proyecto>-<recurso>-<tipo>` de [[hu-devops]].
- **ARQ:** "reemplazar salida en cada ejecución" impide trazabilidad histórica ([[hu-trazabilidad]]).

## Relevancia
Si el paso 2 (Preparación) usa DataBrew, estas convenciones son el estado actual a contrastar. Por confirmar en [[preguntas-abiertas]].
