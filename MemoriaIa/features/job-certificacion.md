---
tipo: feature
estado: vigente
actualizado: 2026-11-11
---
# Job de Glue de certificación (reescrito)

- **Código:** `alfa-repos\glue\data-datalake-niif-certification-glue-job\`. Tiene 412 pruebas y 97,6 % de cobertura; el punto de entrada es `src/niif_certification.py` y el paquete `certification_job`.
  - **No desplegado.**
  - El README único documenta los argumentos, las claves del JSON, las salidas, los módulos, los errores `CER-*` y los hallazgos CER-H1 a CER-H7.
- **Base:** el código desplegado más INC-3 (E1). El código viejo de referencia es `Backend\Glue\niif-dev-certification\`.
- **Argumentos:**
  - obligatorios: `--PERIOD`, `--CONFIG_BUCKET`, `--CONFIG_KEY` y `--GLOBAL_CONFIG_KEY`;
  - opcionales: `--REGENERACION_SIN_DATOS_NUEVOS`, `--EJECUCION_ID`, `--EJECUCIONES_ORIGEN` (separados por coma) y `--JOB_RUN_ID`;
  - los argumentos desconocidos se ignoran.
- **Escribe en DynamoDB:** `ejecucionId` y `ejecucionesOrigen` (E3), `pendingDate` al marcar `PENDING` (lo usa el índice de [[orquestadora-notificaciones]]), y `configBucket`/`configKey`/`globalConfigKey`.
- **Claves nuevas en `certification`:** `report_folders` (antes estaba quemado), `athena_poll_seconds`, `athena_max_wait_seconds`, `index_visibility_attempts` e `index_visibility_wait_seconds`. Además: `console_url.*` y `dynamodb.cert_code_index`.
- **Riesgo CER-H5:** el job lo comparten las Step Functions de generales, pcr-vida, satélites y previsional. Previsional usa otro JSON global. Antes de reemplazarlo hay que actualizar **todos** los JSON globales y probar con un job aparte.
- **Siguiente:** etapa 3, una prueba en DEV con un job aparte y Primas ARL, con confirmación. Antes: decidir CER-H1 a CER-H7.
- Ver [[roadmap-notificaciones]].
