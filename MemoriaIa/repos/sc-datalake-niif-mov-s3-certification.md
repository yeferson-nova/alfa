---
tipo: repo
estado: vigente
actualizado: 2026-11-12
repo: sc-datalake-niif-mov-s3-certification
categoria: lambda
paso: "3 · Certificación Cifras (archivo de no certificados)"
malla: transversal
---
# sc-datalake-niif-mov-s3-certification
> Lambda que, cuando un `certCode` pasa a NO_CERTIFICATE, mueve sus CSV a la subcarpeta `NO-CERTIFICADOS` y marca los registros como NO_TRANSPORT.

## Rol en el flujo
- La dispara el stream de DynamoDB de la tabla de notificaciones, después de la decisión No certificar de la lambda de certificación.
- No entrega a ningún otro componente: el archivo queda en el bucket de reportes y no llega a transporte ni a FIS.

## Entradas y salidas
- Entra: registro MODIFY con `status` nuevo NO_CERTIFICATE y anterior distinto; usa `useCaseId` y `certCode`.
- Sale: CSV copiado a `<carpeta>/NO-CERTIFICADOS/<archivo>` y el original borrado; `status = NO_TRANSPORT`. Devuelve siempre éxito.

## Recursos AWS que toca
- Tabla `niif-{env}-notification-certification` (índice `certCode-index`, `update_item`).
- Bucket de reportes (`REPORT_BUCKET`, `niif-{env}-certification`): `head_object`, `copy_object` y `delete_object`.
- Función `niif-{env}-move-s3-certification`.

## Código
- `src/move_s3.py` (177 líneas): `process_no_certificate`, `move_to_no_certificados` (si el destino ya existe no copia; si el origen no existe solo avisa), `update_status_no_transport` y los mismos `niif_carpeta` y `niif_origen` de copy (lista fija duplicada).
- `object_exists` captura cualquier excepción, así que un `AccessDenied` se toma como "no existe". Los errores por registro se registran sin relanzar.
- `tests/test_lambda_function.py`: 10 pruebas, umbral 70 %. El README de Alfa se ajusta a esta lambda, pero indica una ruta de origen sin la carpeta oficial.

## Estado
- En Alfa: `origin/quality`, commit `7ff71d6` (2026-10-16); desplegado.
- `quality` frente a `main` (medido en GitHub el 2026-11-12): 0 commits solo en `quality`, 2 solo en `main`; `main` tiene commits que `quality` no tiene (por revisar si son merges o cambios de PRD). Ver ARQ-50 en [[auditoria-repos-2026-11]].
- Código nuevo nuestro: ninguno en este repo; la rama `feature/codigo-nuevo-notificacion` lo deja igual. Lo reemplaza la orquestadora (D1), que además es idempotente y trata `AccessDenied` como error; escribir la malla como pendiente en `prg-programaciones` espera la decisión D2.

## Brechas y relacionadas
- ARQ-23 (No certificar no deja la tarea pendiente; parcial en la orquestadora), ARQ-24, ARQ-26, ARQ-42 (no idempotente, `AccessDenied` como "no existe") y ARQ-43, en [[notificaciones-vs-arquitectura]].
- Relacionado: [[sc-datalake-niif-notification-certification-orchestrator]], [[sc-datalake-niif-copy-s3-certification]], [[orquestadora-notificaciones]], [[roadmap-notificaciones]].
