---
tipo: auditoria
estado: vigente
actualizado: 2026-11-12
---
# Hallazgos del barrido de repos (2026-11-12)

> Salen de leer el código de los 35 repos al escribir sus fichas ([[00-mapa-repos]]). Verificados en el código por el orquestador (SFTP, cuenta fija, README, pruebas); el resto es lectura de los agentes. Todos quedan como **propuesta** hasta confirmar con DevOps. Sin humano equivalente en `Docs\06-brechas\` todavía.

## Seguridad
- **SEG-16** Las dos lambdas SFTP aceptan la clave del servidor sin verificarla (`AutoAddPolicy` en `lambda_function.py`, dos veces en cada una). Un servidor suplantado recibiría usuario y clave, o los archivos de cifras. Arreglo propuesto: cargar la clave de host conocida desde configuración y rechazar si no coincide. Repos: [[sc-datalake-niif-get-files-sftp]], [[sc-datalake-niif-send-via-sftp-lambda]]. Credenciales por usuario y clave: valorar llave o secreto rotado.

## Arquitectura
- **ARQ-45** `silver_vida_deudores.py` compara la cuenta de AWS contra un número fijo en el código (valor quemado de ambiente; incumple [[est-desarrollo-hexagonal]]). Repo: [[data-datalake-emisiones-vida-glue-job]].
- **ARQ-46** Los README de 6 de los 12 repos de Glue (Auxilios, notificación, ARL, Previsional, Rentas y `rmd-ext-estandar`) son copia del de Endosos, y los de 4 lambdas (certification, composite, copy-s3 y el trigger de notificación) son copia del de otra lambda; describen jobs o pruebas que no existen. Quien lea el README se equivoca. Las fichas de `repos\` son la descripción fiable.
- **ARQ-47** (por confirmar en otras ramas) En la rama `quality`, la única que se exportó, 10 de los 12 repos de Glue no tienen ningún archivo de prueba; solo `pcr-emisiones` y `emisiones-vida` traen un `conftest.py`, con paquetes vacíos. Falta revisar `develop` y `main`, donde puede haber pruebas. El estándar pide más de 80 % ([[est-codigo-seguro]]). Ver [[auditoria-pipeline-2026-11]] para la cobertura del pipeline.
- **ARQ-48** Jobs genéricos viven dentro de repos de una sola malla: `validacion_tecnica.py` y `scd.py` (usados por las 6 mallas) en [[data-datalake-niif-auxilios-funerarios-glue-job]], y la extracción y remediación genéricas en [[data-datalake-niif-rmd-ext-estandar-glue-job]]. Un cambio en Auxilios puede romper las demás mallas y el repo no lo dice.
- **ARQ-49** IaC ambigua: [[iac-analytics-datalake-niif]] y [[iac-analytics-datalake-niif-operations-manager]] declaran recursos parecidos y no se sabe cuál manda en cada cuenta. El segundo declara `configurador-mallas` solo en dev, con un handler sin el prefijo del paquete y sin las variables de idempotencia, catálogo y métricas que el código nuevo exige ([[sc-datalake-niif-configurador-mallas]]).

- **ARQ-50** `quality` (QA) y `main` (PRD) no tienen el mismo contenido en 12 de los 33 repos. Medido en GitHub el 2026-11-12 con `git rev-list --left-right --count origin/quality...origin/main` y `git diff --name-only origin/quality origin/main` (entre paréntesis: archivos distintos). Nuestra base de parches sale de `quality`. Por qué importa: PRD corre `main`, no `quality`.
  - **Mismo contenido** (21 repos, 0 archivos distintos): los commits de más que tiene `main` son merges de PR. `quality` sirve como base. Son los 6 jobs de Glue de certificación, previsional y las mallas de Rentas, Endosos y Reserva Matemática ARP y RVI, 14 lambdas y `params-s3-landing`.
  - **QA por delante de PRD** (4 repos, `main` no tiene nada que `quality` no tenga): [[data-datalake-emisiones-vida-glue-job]] (14), [[data-datalake-niif-pcr-emisiones-glue-job]] (14), [[sc-datalake-niif-configuration-general]] (23), [[sc-datalake-niif-get-files-sftp]] (13). Hay cambios en QA que no se han promovido.
  - **Divergen en 1 archivo** (4 repos): [[data-datalake-niif-auxilios-funerarios-glue-job]] (1), [[data-datalake-niif-notification-certification-glue-job]] (1), [[data-datalake-niif-primas-arl-glue-job]] (1), [[data-datalake-niif-rmd-ext-estandar-glue-job]] (1). Por identificar cuál archivo.
  - **Divergen en más archivos** (3 repos): [[iac-analytics-datalake-niif]] (3), [[iac-analytics-datalake-niif-operations-manager]] (19), [[cfg-datalake-niif-config-s3]] (12), [[tpl-datalake-niif-email-template-s3]] (7). Es el mayor riesgo: `operations-manager` tiene 27 commits solo en `quality` y 5 solo en `main`.
  - Para la base de las ramas feature de los 12 repos hay que decidir con Alfa si se parte de `quality` o de `main`, y a cuál va el PR.

## Otros hechos de las fichas (no son brecha, sirven para decidir)
- Orden real de la cadena (leído en código, no en el diagrama): la Step Function llama al evaluador compuesto y lanza el job de certificación; este escribe los PENDING; el stream de DynamoDB los lee el trigger y lanza el job de notificación; el correo trae el enlace a `short-link-redirect`, Cognito, `post-login` y `certification`; copy o move leen CERTIFICATE o NO_CERTIFICATE; el archivo en `export/` activa `send-via-sftp`. Detalle en [[sc-datalake-niif-notification-certification-orchestrator]].
- En la rama `feature/codigo-nuevo-notificacion`, certification, composite y short-link solo ganan un esqueleto hexagonal vacío y `niif_observability`; trigger, copy, move y sftp quedan idénticos a Alfa.
- `post-login` es solo un esqueleto (sin handler ni pruebas); el código real está en la lambda desplegada `niif-post-login`.
- Hay dos conteos distintos del paquete de notificación: la rama dice `notificacion_certificacion`, 537 pruebas; [[repos-alfa-notificacion]] habla de `certification_notifier`, 477 pruebas. Falta confirmar cuál es el vigente.

Relacionado: [[auditoria-pipeline-2026-11]], [[notificaciones-vs-arquitectura]], [[brechas-seguridad]].
