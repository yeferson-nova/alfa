---
tipo: referencia
actualizado: 2026-11-10
---
# Inventario de lambdas y Step Functions (código desplegado)

## Evidencia de uso 2026-11-10 (manda sobre lo de abajo)
Clasificación completa en `Docs\06-brechas\limpieza-dev-recursos.md`; datos en `EvidenciasAws\dev-uso-recursos\` (ignorada).
- **G3 ya no lee el stream:** los dos mappings de `programador-configuraciones` están `Disabled`. Se corrige el riesgo de doble ejecución de abajo.
- **G2 sigue activo:** stream de `niif-dev-programacion-cargues` `Enabled` (79 inv, 18 err) y finalizador 39/53 errores.
- **Dos API:** `datalake-dev-niif-api-gtw` (Terraform, con WAF; `/r`, `/post_login`, `/certify`, `/not_certify`) es el vigente. `Certificación API` (viejo, sin WAF) sigue con tráfico en `postLogin` (46): **SEG-14**.
- Vigentes de la cadena: `niif-dev-short-link-redirect`, `niif-post-login` (sin etiquetas de Terraform), `niif-dev-certification`, trigger, copy, move, composite, `datalake-dev-niif-send-via-sftp` (0 inv: **ARQ-30**, hipótesis).
- Nuevas sin documentar: `niif-dev-configuration-general` (2026-11-07, 82 inv / 27 err), `niif-dev-configurador-mallas` (Terraform, 0 inv), `niif-dev-configurador-pcr-vida`.
- Últimas invocaciones: G3 programador 2026-11-03, configuradores 2026-10-22; G2 programador 2026-09-15, iniciador 2026-09-18; **finalizador G2 sigue disparado por la regla de fin de la SF ARL** (2026-11-06, falla 39/53); `postLogin` viejo 2026-09-25. Cognito `datalake-dev-niif-api` tiene callback a los dos API (quitar el viejo antes de retirarlo). ARQ-30: el filtro `export/` coincide con donde escribe copy; falta ver si copy copió algo.
- **ARQ-30 confirmada:** copy falló todos los Certificar del 2026-08-19 al 2026-09-25 (falta índice `certCode`, error tragado); el 2026-10-09 copió 2 de ARL pero `export/` está vacío y `send-via-sftp` sin logs: Certificar → FIS nunca probado de punta a punta en DEV. Finalizador G2: `KeyError idAutomatizacion` antes de escribir (quitar la regla es seguro). `configuration-general`: configurador general de otro desarrollador (misma base que los `configuration-<malla>`, no nuestro T2), modo validación, ya cubre Satélites/PCR.
- DEV lo usan otras mallas en paralelo (PCR Vida, Generales, Satélites, Previsional, Emisiones): no se tocan.

## Alcance de trabajo (decidido 2026-11-03)
- **Lambdas de trabajo (7):** `niif-dev-configuration-{primas-rentas,primas-endosos,primas-arl,auxilios-funerarios,reserva-matematica-rvi,reserva-matematica-arp}` y `niif-dev-manager-configuration`.
- **Step Functions de trabajo (6):** `niif-dev-<malla>` de esas mallas.
- **Todo lo demás es pruebas / fuera de alcance:** movido sin modificar a `Backend\Lamnbdas\pruebas-lambda\` y `Backend\StepFunctions\pruebas-stepfunctions\`. Incluye `composite`, `certification` y el resto del paso 3 (recuperables de ahí).
- **Objetivo:** consolidar los 6 configuradores en **una lambda general estandarizada**. Solo difieren en `MALLA_PK`, `PRG_PK` y `STATE_MACHINE_ARN` (propuesta en `lambdas.md` §5).
- **Spike (2026-11-03):** `Backend\Lamnbdas\inventario-aws\spike-primas-rentas\`: Primas Rentas reescrita por capas (handler, servicio, constructores, periodo, puertos y adaptadores, catálogo de mallas), 37 pruebas, 98 % de cobertura del núcleo. Base: la `configurador` anterior, no el código desplegado. Faltan pruebas de adaptadores (LocalStack) y equivalencia con datos reales.
- **Corrección:** `primas-endosos` y `reserva-matematica-rvi` **sí existen**; su extracción salió vacía. Falta el código de manager, rentas, endosos, rvi y, parcial, arl y arp: repetir con `cloudshell-inventario\inventario.sh`.
- **Verificado 2026-11-03 (.py por lambda):** solo auxilios tiene su `.py` real (`conf_auxilios.py`). arl y arp tienen solo librerías; rentas, endosos, rvi y manager no tienen carpeta. Endosos y rvi tampoco tienen `config\*.json` (el script pidió `configuration-<malla>` sin el prefijo `niif-dev-`). Para las 5 de configuración hay copia de la `configurador` en su carpeta de `Backend\Lamnbdas\`; manager sin referencia.
- **Carpeta de trabajo:** `Backend\Lamnbdas\<nombre-lambda>\`, una por lambda de la tarea con su `.py` (copias; `inventario\` queda intacto como fuente).

Humano: `Backend\Lamnbdas\inventario-aws\lambdas.md` (movido desde `Docs\05-arquitectura\` el 2026-11-03) y `step-functions-niif.md`. Esta nota resume; el detalle y los hallazgos `LMB-xx` / `SF-xx` están allá.

- **Jobs de Glue de trabajo (2026-11-03):** 11, los que invocan las 6 SF (extracción, val-tecnica, remediación, scd y certification genéricos; 6 silver por malla). En `Backend\Glue\`; el resto en `Backend\Glue\pruebas-glue\`. `niif-dev-notification-certification` quedó en pruebas: validar si entra por T1.

- **Flujo integral (2026-11-03):** `Docs\05-arquitectura\flujo-integral-niif17.md` une programación → configurador → Step Function → 5 jobs de Glue → certificación → transporte, con las tablas DynamoDB y los 4 streams que mueven el proceso. Idea clave: los jobs se pasan el estado por un `parametros_extraccion.json` fijo por malla en S3. Falta el código de la cadena final (trigger de notificación, copy, move, SFTP).
- **Documentos separados (2026-11-03):** entender = `flujo-integral-niif17.md` (estado actual), `jobs-glue.md` (12 jobs y 3 crawlers) y `tareas-sobre-el-flujo.md` (T1, T2, T3 sobre el flujo), en `Docs\05-arquitectura\`. Corregir = `Docs\06-brechas\rendimiento-y-costos.md` (`RYC-01` a `RYC-17`) y `revision-pre-produccion.md` (`PP-01` a `PP-23`: 5 bloqueantes, 9 altos, 9 medios; veredicto: hoy no está listo para producción). Los `FLU-01` a `FLU-14` están en el anexo de `revision-pre-produccion.md`.

## Fuente
- `Backend\inventario\` (hoy `EvidenciasAws\dev-inventario-cuenta\`): extraído por script de la cuenta 533267035849 (us-east-1, DEV). **No hay repo Git: el código desplegado es la única fuente.**
- `lambdas\codigo\<nombre>\` (código) y `lambdas\config\<nombre>.{json,triggers,policy,aliases}.json`.
- `stepfunctions\` (73 ASL), `sns\`, `dynamodb\`, `iam\`, `s3\`, `logs\`, `athena\`.
- **Ya organizado y sacado de `inventario\` (2026-11-03):** jobs y scripts de Glue en `Backend\Glue\`, reglas EventBridge en `Backend\Eventos\`, ambos con su `pruebas-*`. Se verificó copia idéntica antes de borrar el original.
- **Incompleto:** el disco se llenó al descomprimir (`_errores.log`). Faltan 13 de 62 lambdas, entre ellas `notification-certification-trigger`, `copy-s3-cert`, `move-s3-certification`, `manager-configuration`, `short-link-redirect` (G4) y `send-via-sftp` (imagen ECR).
- `config\*.json` trae URLs prefirmadas con token de sesión (expiradas); no versionar.

## Lo esencial
- **4 generaciones paralelas** del gestor ([[tareas-aws]] T2), inferido por fechas:
  - G1: `datalake-dev-niif-*`, streams deshabilitados.
  - G2: `programador-cargues` → `configurador-extraccion` → `iniciador` → SF → `finalizador`.
  - G3: `programador-configuraciones` → `configurador-<malla>` ×6.
  - G4: Terraform, `manager-configuration` → `configuration-<malla>`, más la certificación.
- **G3 y G4 escuchan el mismo stream** (`niif-dev-prg-programaciones`): riesgo de doble ejecución.
- **SF vigente `niif-dev-<malla>`:** 40 estados, 6 copias con *drift*. Hace extracción → crawler Bronze → validación técnica → remediación → Silver → SCD Gold → lambda composite → Glue `niif-dev-certification`.
  - Solo invoca **una lambda**: `niif-dev-composite-notification-evaluator`.
  - Logs OFF en las 73 SF.
- **Paso 3 por eventos** después de la SF:
  - Glue certificación → tabla `niif-dev-notification-certification` PENDING → stream → trigger → Glue notificación (SNS `niif-dev-certification-email`).
  - Enlace corto `/r` → `POST /certify` (Cognito) → stream CERTIFICATE → copia a `niif-dev-transport` → SFTP FIS.
- **Calidad:**
  - 0 pruebas; todo en un archivo; handlers de hasta 252 líneas.
  - 7 configuradores con 86 % a 98 % de similitud.
  - Errores silenciosos: `'{env}'` sin f-string en composite; `'$period'` en CheckValidation.
  - Defaults a QA en `niif-dev-certification`.
  - Paquetes de 45 MB con pandas sin usar.
- **Observabilidad existente:** `ALFA-DEV-DATALAKE-PipelineObserver` publica EMF y avisa por SNS Critical/Warning cuando falla una SF. Es la base de [[monitoreo-grafana]] (1c).

## Respuestas que da el inventario (inferidas, confirmar)
- Certificar / No certificar **ya tienen endpoint**: API GW `hfoy06vw40` + `niif-dev-certification` con autorizador Cognito y multiaprobador ([[correo-notificacion]]).
- El correo sale por **SNS** (`niif-dev-certification-email`). Hay una regla temporal `niif-dev-ses-delivery-test` que sugiere pruebas con SES.
- Sin certificación no hay transporte (copy solo en CERTIFICATE), pero la SF termina igual en SUCCEEDED ([[riesgos-glue-notificacion]]).

Ver [[entorno-local-localstack]] para pruebas (Scheduler y Glue no están habilitados).
