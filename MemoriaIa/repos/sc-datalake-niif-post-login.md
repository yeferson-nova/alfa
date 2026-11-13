---
tipo: repo
estado: nuevo
actualizado: 2026-11-12
repo: sc-datalake-niif-post-login
categoria: lambda
paso: "3 · Certificación Cifras (retorno de Cognito)"
malla: transversal
---
# sc-datalake-niif-post-login
> Lambda de retorno del login (`GET /post_login`): recibe al usuario autenticado por Cognito con Azure AD y llama a certificar o no certificar con su token. Hoy solo existe el esqueleto.

## Rol en el flujo
- La invoca API Gateway cuando Cognito redirige de vuelta, después de `short-link-redirect`.
- Entrega a la lambda de certificación (`/certify` o `/not_certify`).

## Entradas y salidas
- Entra (meta): la redirección de Cognito con el token y el `state` de la acción. El flujo actual usa el flujo implícito.
- Sale (meta): llamada a certificar o no certificar. Sin lógica en el repo; por confirmar el contrato exacto, que se tomará de la lambda desplegada `niif-post-login`.

## Recursos AWS que toca
- API Gateway `datalake-{env}-niif-api-gtw` (`GET /post_login`) y Cognito (cliente del API).
- Función nueva `niif-{env}-post-login`, que reemplaza a la desplegada `niif-post-login` (sin etiquetas de Terraform). DevOps apunta la integración y retira la anterior; el callback de Cognito no cambia.

## Código
- Solo en la rama `feature/codigo-nuevo-notificacion`, carpeta `lambdas`: README, carpetas vacías `src/post_login/{domain,application,adapters/{inbound,outbound},config}` (archivos `__init__.py`), copia de `src/niif_observability` (log estructurado y métricas) y marcadores `.gitkeep` en `config`, `eventos`, `scripts` y `test`.
- No hay `lambda_function.py`, Jenkinsfile, pruebas ni dependencias. Siguiente paso: el paso 6 del roadmap, después de decidir D7.

## Estado
- En Alfa: el repo no existe en `main` ni figura en `VERSIONES.md`.
- Código nuevo nuestro: esqueleto en `feature/codigo-nuevo-notificacion` (sin lógica).

## Brechas y relacionadas
- **Arquitectura:** en la propuesta se renombra `niif-{env}-post-login` y llama a certificar o no certificar. En la aprobada `postLogin()` solo se conecta con Cognito. Ver [[arquitectura-propuesta-niif17]].
- Cierra, cuando se escriba: SEG-10 (flujo implícito, pasar a código con PKCE) y SEG-11 (`state` sin firmar), en [[notificaciones-vs-arquitectura]].
- Incidente: el cliente de Cognito tenía un callback local en lugar de la URL real de `/post_login`, con 0 eventos en 14 días, en [[inprod-enlaces-cognito-9411325]]. SEG-14 (API viejo con tráfico en el post-login anterior) en [[inventario-lambdas]].
- Relacionado: [[sc-datalake-niif-short-link-redirect]], [[sc-datalake-niif-certification]], [[roadmap-notificaciones]].
