---
tipo: repo
estado: vigente
actualizado: 2026-11-12
repo: sc-datalake-niif-send-via-sftp-lambda
categoria: lambda
paso: "3 · Certificación Cifras (Lambda Transporte, hacia FIS)"
malla: transversal
---
# sc-datalake-niif-send-via-sftp-lambda
> Lambda en imagen de contenedor que sube por SFTP al servidor de FIS cada archivo nuevo del bucket de transporte, respetando la carpeta del caso.

## Rol en el flujo
- Paso 3, Zona Transporte. La activa una notificación de S3 (`aws:s3`) cuando copy deja un objeto en `export/` del bucket de transporte (el filtro exacto del disparador está en la infraestructura: por confirmar).
- Es el último eslabón de la cadena; entrega al servidor SFTP de FIS (paso 4 del diagrama).

## Entradas y salidas
- Entra: evento S3 con bucket y llave. Ignora llaves que terminan en `/`. Quita el prefijo `export` (variable `S3_ROOT_PREFIX`) y conserva el resto de carpetas bajo `SFTP_BASE_DIR`.
- Sale: archivo en el SFTP con su nombre original (`putfo` por streaming, crea las carpetas, verifica con `stat`). Devuelve conteo de éxitos y fallos; un fallo por archivo no detiene el lote.

## Recursos AWS que toca
- Bucket de transporte `niif-{env}-transport` (`get_object`).
- Secrets Manager: un secreto con usuario y clave del SFTP (`CREDS_SECRET_ID`). Variables `SFTP_HOST`, `SFTP_PORT`, `SFTP_BASE_DIR` y `ENABLE_SSH_RSA_FALLBACK`.
- Repositorio ECR `datalake_{env}_niif_send_via_sftp_lambda` y parámetro de versión en SSM (Jenkinsfile con `lambdaDockerPipeline`; IaC `serverless/lambdas/send_via_sftp`).

## Código
- `src/lambda_function.py` (251 líneas): `_connect_sftp` (paramiko, comprobación TCP previa, reintento sin los algoritmos desactivados), `_mkdirs_sftp` y `lambda_handler`. Usa `AutoAddPolicy`, es decir, no verifica la clave del servidor.
- `Dockerfile` sobre la imagen base de Lambda de Python 3.12; `tests/test_lambda_function.py` con moto, 11 pruebas; `tox.ini` con `fail_under = 0`, bajo el 70 % del estándar de DevOps.

## Estado
- En Alfa: `origin/quality`, commit `2c68017` (2026-10-08); en DEV figura inactiva (captura del 2026-11-11), nombre pendiente de decidir.
- `quality` frente a `main` (medido en GitHub el 2026-11-12): 0 commits solo en `quality`, 1 solo en `main`; `main` tiene commits que `quality` no tiene (por revisar si son merges o cambios de PRD). Ver ARQ-50 en [[auditoria-repos-2026-11]].
- Código nuevo nuestro: ninguno (rama `feature/codigo-nuevo-notificacion` idéntica). Entró al alcance como parte del flujo completo (paso 5 del roadmap, transporte a FIS).

## Brechas y relacionadas
- **SEG-16** acepta la clave del servidor SFTP sin verificarla (`AutoAddPolicy`), en [[auditoria-repos-2026-11]].
- ARQ-30: el tramo copy hacia SFTP nunca se probó de punta a punta en DEV, sin logs de esta lambda ([[inventario-lambdas]], [[notificaciones-vs-arquitectura]]).
- Observaciones propias, sin ID: clave del servidor no verificada y credenciales por usuario y clave.
- Relacionado: [[sc-datalake-niif-copy-s3-certification]], [[sc-datalake-niif-notification-certification-orchestrator]], [[roadmap-notificaciones]].
