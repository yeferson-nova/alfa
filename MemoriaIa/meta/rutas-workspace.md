---
tipo: referencia
actualizado: 2026-11-03
---
# Rutas del workspace

Raíz: `C:\Users\rdpuser\Git\`. Mapa humano en `Docs\00-LEEME.md`.

## Backend\ (código y entorno local)
- `inventario\`: resto de la extracción por script de la cuenta AWS DEV, ya casi vacío: `identidad.json`, `_errores.log`, `logs\` (1 archivo vacío), `athena\`, `dynamodb\` e `iam\` (vacías) y `lambdas\codigo`/`config` por borrar (copiados). Incompleta; ver [[inventario-lambdas]].
- **Todo lo organizado por servicio** (trabajo en la raíz, el resto en `pruebas-*`): `Lamnbdas\` (+ `pruebas-lambda\`), `Glue\`, `Eventos\`, `StepFunctions\` (6 SF + `pruebas-stepfunctions\`), `SNS\` (3 tópicos: `ALFA-DEV-DATALAKE-Critical`/`Warning` y `niif-dev-certification-email`), `S3\` (8 buckets del pipeline). Cada uno con `LEEME.md`.
- `Lamnbdas\<nombre-lambda>\`: carpeta de trabajo de las 7 lambdas de la tarea con su `.py` (auxilios real; 5 copias de la `configurador` con `ORIGEN.md`; manager sin código, `PENDIENTE.md`).
- `Glue\<job>\`: 11 jobs de Glue de trabajo (los que invocan las 6 Step Functions) con su `.py` y `job.json`; `Glue\pruebas-glue\` con los otros 358 jobs y sus scripts. También `Glue\crawlers\` (3 de la SF) y `Glue\catalogo\` (3 bases `niif-dev` con sus tablas). Movido desde `inventario\glue\`, que quedó vacío. Ver `Glue\LEEME.md`.
- `Eventos\`: 4 reglas de EventBridge que tocan nuestras SF, jobs y lambdas (finalizador ARL y las 3 `ALFA-DEV-DATALAKE-*` hacia `PipelineObserver`); `Eventos\pruebas-eventos\` con las otras 18. Movido desde `inventario\eventos\` (ya no existe ahí). Ver `Eventos\LEEME.md`.
- `Lamnbdas\inventario-aws\`: `lambdas.md`, `step-functions-niif.md` y `spike-primas-rentas\` (lambda general por capas con pruebas). Movido aquí desde `Docs\05-arquitectura\`; ver [[inventario-lambdas]].
- `cloudshell-inventario\`: `inventario.sh` (solo lectura) y su zip para repetir la extracción en CloudShell.
- `localstack\`: entorno AWS local (docker compose), ver [[entorno-local-localstack]]. `.env` con token, excluido de Git; no leerlo.
- `htmlNew\`: plantillas compatibles con clientes de correo, el mockup UX y `newUx\` con el diseño nuevo.
- `reservas_matematicas_cert\`: 5 plantillas HTML del correo de certificación, ver [[correo-notificacion]].

## Tareas\ (para humanos, una carpeta por asignación)
- `TareasAsignadas.txt`: fuente original. Resumen IA en [[tareas-aws]].
- `00-LEEME.md`: contexto del proyecto, alcance, marco normativo y resumen de asignaciones.
- `T1-notificaciones\`, `T2-lambdas-gestor-configuracion\`, `T3-validaciones-reserva-matematica\`: `README.md` con problema, estado, estándares, propuesta y bloqueos; aquí van sus entregables.

## Docs\ (para humanos, referencia)
- `02-estandares-banco\`: entregables del banco, solo lectura, nombre original. Fichas en `MemoriaIa\estandares\` ([[est-etl]], [[est-datastage]], [[est-versionamiento-codigo]], [[est-codigo-seguro]], [[est-databrew]]).
  - `desarrollo\`: Estándar DataStage v6, Estándar ETL v1.
  - `versionamiento-seguro\`: Lineamiento versionamiento código seguro ALFALV2, Versionamiento de código V3.
  - `aws\`: Manual de recetas AWS Glue DataBrew.
- `03-historias-usuario\`: 6 xlsx de HUs técnicas (gobierno de ingestas, linaje, trazabilidad, DevOps/DataOps, remediación, seguridad y acceso). Fichas en `MemoriaIa\hu\` (prefijo `hu-`).
- `04-manuales\`: Manual de usuario Proyecto Nuevos Cargues V5, ver [[est-nuevos-cargues]].
- `05-arquitectura\`
  - `arquitectura-tobe-niif17.png`: arquitectura **objetivo (TO-BE)**, de alto nivel; nos competen los pasos 2 y 3. Ver [[arquitectura-niif17]].
  - `diagramas\`: `flujo-niif17-asis.drawio` (+ `.png`) es el diagrama **AS-IS** de punta a punta de los pasos 2 y 3, con iconos AWS y carriles por fase; `README.md` explica cómo leerlo.
  - `ejemplo-correo-certificacion.png`: ejemplo del correo, ver [[correo-notificacion]].
  - `aws_pasos_2_y_3.drawio`: parte AWS con tareas marcadas, ver [[diseno-propuesto]].
- `06-brechas\`: análisis de brechas (seguridad, costos, arquitectura, cumplimiento).
  - `seguridad.md`: ver [[brechas-seguridad]].
  - `monitoreo-continuo-grafana.md`: ver [[monitoreo-grafana]].
- `07-entregables\`: lo que se entrega al banco.

## MemoriaIa\ (para IA)
Subcarpetas: `meta\`, `contexto\`, `tareas\`, `estandares\`, `hu\`, `brechas\`. Los nombres de nota son únicos en toda la memoria, así que `[[nota]]` funciona sin importar la subcarpeta.
