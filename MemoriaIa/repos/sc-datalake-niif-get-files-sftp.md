---
tipo: repo
estado: vigente
actualizado: 2026-11-12
repo: sc-datalake-niif-get-files-sftp
categoria: lambda
paso: "4-5 · Herramientas actuariales (retorno desde Prophet/FIS), por confirmar"
malla: transversal
---
# sc-datalake-niif-get-files-sftp
> Lambda en imagen de contenedor que baja por SFTP los archivos de una carpeta remota y los copia a un bucket de S3 conservando las subcarpetas.

## Rol en el flujo
- La dispara un horario de EventBridge (tres corridas diarias definidas en la IaC, apagadas por defecto), no un evento S3. Es la inversa de [[sc-datalake-niif-send-via-sftp-lambda]], que sube de S3 a SFTP.
- Deja los archivos en el bucket `niif-{env}-accountingtool`; quién los consume después: por confirmar. No es de las mallas ALFAPRD ni aparece en las notas de la cadena de certificación.

## Entradas y salidas
- Entra: el evento no se usa (solo se registra); todo viene de variables de entorno (host, puerto, carpeta base, bucket, prefijo, omitir existentes, borrar tras descargar).
- Sale: resumen con éxitos, omitidos, fallos y segundos; los errores de secreto o conexión salen como `status error` con la etapa.

## Recursos AWS que toca
- Secrets Manager: secreto de credenciales SFTP (usuario y clave).
- S3: `niif-{env}-accountingtool` (head y upload por streaming).
- Función `niif-{env}-accountingtool` en VPC (subredes privadas), ECR `datalake_{env}_niif_get_files_sftp_lambda` y parámetro SSM con la versión de la imagen.

## Código
- `src/lambda_function.py` (243 líneas), un solo archivo: `_connect_sftp` (paramiko, reintento sin algoritmos RSA-SHA2), `_walk_remote` recursivo, `_s3_key_for`, `_object_exists`, `lambda_handler`.
- `Dockerfile` (base Lambda Python 3.12), `Dockerfile.tests`, `tox.ini`, Sonar. No es hexagonal.
- Pruebas: `tests/test_lambda_function.py`, 8 pruebas con moto y un SFTP simulado; umbral 70 %.
- Observado en el código (sin ID en las notas): la clave del servidor se acepta sin verificar (`AutoAddPolicy`) y el evento se escribe al log.

## Estado
- En Alfa: `origin/quality`, commit `e276080` (2026-10-16). Jenkins `lambdaDockerPipeline`: construye la imagen, la sube a ECR y actualiza el SSM de versión; la IaC (`iac-analytics-datalake-niif`, stack `serverless/lambdas/get_files_sftp`) crea la función. Despliegue real: por confirmar.
- Código nuevo nuestro: ninguno.

## Brechas y relacionadas
- **SEG-16** acepta la clave del servidor SFTP sin verificarla (`AutoAddPolicy`), en [[auditoria-repos-2026-11]].
- Sin IDs de brecha en las notas (el repo no aparece en ellas).
- Relacionado: [[sc-datalake-niif-send-via-sftp-lambda]], [[arquitectura-niif17]], [[iac-analytics-datalake-niif]]
