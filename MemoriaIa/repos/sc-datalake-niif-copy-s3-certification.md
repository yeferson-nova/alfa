---
tipo: repo
estado: vigente
actualizado: 2026-11-12
repo: sc-datalake-niif-copy-s3-certification
categoria: lambda
paso: "3 · Certificación Cifras (hacia la Zona Transporte)"
malla: transversal
---
# sc-datalake-niif-copy-s3-certification
> Lambda que, cuando un `certCode` pasa a CERTIFICATE, copia sus CSV al bucket de transporte y marca los registros como TRANSPORT.

## Rol en el flujo
- La dispara el stream de DynamoDB de la tabla de notificaciones, después de la decisión Certificar de la lambda de certificación.
- Deja el archivo en `export/<carpeta>/` del bucket de transporte; ese objeto nuevo activa `send-via-sftp` hacia FIS.

## Entradas y salidas
- Entra: registro MODIFY con `status` nuevo CERTIFICATE y anterior distinto; usa `useCaseId` y `certCode`.
- Sale: copia en transporte y `status = TRANSPORT` en cada registro del `certCode`. Devuelve siempre éxito.

## Recursos AWS que toca
- Tabla `niif-{env}-notification-certification` (índice `certCode-index`, `update_item`).
- Bucket de reportes (`REPORT_BUCKET`, `niif-{env}-certification`) como origen y bucket de transporte (`TRANSPORT_BUCKET`, `niif-{env}-transport`) como destino. Ambos nombres vienen de variables de entorno.
- Función `niif-{env}-copy-s3-cert`.

## Código
- `src/copy_s3.py` (165 líneas): `lambda_handler`, `process_certcode` (recorre todos los registros del `certCode`), `niif_carpeta` (lista fija `NIIF_CARPETAS` de casos autorizados) y `niif_origen` (busca `<carpeta>/<archivo>` y luego `<useCaseId>/<archivo>`).
- Los errores se capturan por registro y se registran sin relanzar. El `update_item` no tiene condición.
- `tests/test_lambda_function.py`: 10 pruebas, umbral 70 %. El README de Alfa describe la lambda de move, no esta.

## Estado
- En Alfa: `origin/quality`, commit `8df3c16` (2026-10-08); desplegado.
- `quality` frente a `main` (medido en GitHub el 2026-11-12): 0 commits solo en `quality`, 2 solo en `main`; `main` tiene commits que `quality` no tiene (por revisar si son merges o cambios de PRD). Ver ARQ-50 en [[auditoria-repos-2026-11]].
- Código nuevo nuestro: ninguno en este repo. La rama `feature/codigo-nuevo-notificacion` lo deja igual; su reemplazo es la orquestadora (decisión D1), pendiente de despliegue.

## Brechas y relacionadas
- ARQ-24 (`NIIF_CARPETAS` duplicado en copy y move), ARQ-26 (traga excepciones y devuelve éxito), ARQ-30 (copy hacia SFTP: falló todos los Certificar del 2026-08-19 al 2026-09-25 por falta del índice; Certificar hacia FIS nunca probado de punta a punta en DEV), ARQ-41 y ARQ-43, en [[notificaciones-vs-arquitectura]] e [[inventario-lambdas]].
- Relacionado: [[sc-datalake-niif-notification-certification-orchestrator]], [[sc-datalake-niif-mov-s3-certification]], [[sc-datalake-niif-send-via-sftp-lambda]], [[orquestadora-notificaciones]].
