---
tipo: repo
estado: vigente
actualizado: 2026-11-12
repo: sc-datalake-niif-short-link-redirect
categoria: lambda
paso: "3 · Certificación Cifras (enlaces del correo)"
malla: transversal
---
# sc-datalake-niif-short-link-redirect
> Lambda detrás de `GET /r` que resuelve el token corto del correo y responde un 302 hacia la URL guardada (login de Cognito para Certificar / No certificar).

## Rol en el flujo
- La invoca API Gateway (con WAF) cuando el usuario pulsa un botón del correo. El job de notificación es quien genera el token y escribe la URL en la tabla de enlaces cortos.
- Entrega el navegador a Cognito (Azure AD); luego vuelve por `post-login`, que llama a la lambda de certificación.

## Entradas y salidas
- Entra: parámetro `t` o `token` de la consulta.
- Sale: 302 con `Location` y `Cache-Control: no-store`; 400 si falta el token o la URL; 404 si el token no existe. El código no valida vigencia.

## Recursos AWS que toca
- Tabla `niif-{env}-short-links` (`get_item` por `token`; la variable `SHORT_LINKS_TABLE` tiene por defecto el nombre de QA).
- API Gateway `datalake-{env}-niif-api-gtw` (ruta `/r`).
- Función `niif-{env}-short-link-redirect`.

## Código
- `src/short_link_redirect.py` (82 líneas): un `lambda_handler`; aplica `html.unescape` a la URL guardada. Imprime el evento completo y, en 400 y 404, devuelve el token (y el ítem completo cuando falta la URL).
- `tests/test_lambda_function.py`: 13 pruebas, umbral 70 %. El README de Alfa es el de otra lambda.

## Estado
- En Alfa: `origin/quality`, commit `dcaebd3` (2026-08-27); desplegado.
- Código nuevo nuestro: rama `feature/codigo-nuevo-notificacion`, solo esqueleto (`short_link_redirect/` hexagonal y `niif_observability`; el `.py` es idéntico). Meta de su README: validar vigencia, acción `DOWNLOAD` con URL prefirmada (D3) y token de 32 caracteres que ya genera el job nuevo.

## Brechas y relacionadas
- SEG-13 (token de 8 hex y `/r` público) y ARQ-29 (descarga del CSV por la consola de S3), en [[notificaciones-vs-arquitectura]]. ARQ-21 (enlaces cortos fuera del diagrama).
- Incidente: los 2 enlaces cortos de Auxilios guardados con el host roto en prod, en [[inprod-enlaces-cognito-9411325]].
- Relacionado: [[sc-datalake-niif-post-login]], [[sc-datalake-niif-certification]], [[job-notificacion-ses]], [[roadmap-notificaciones]].
