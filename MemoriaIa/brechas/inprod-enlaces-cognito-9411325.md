---
tipo: brecha
actualizado: 2026-11-11
---
# Incidente en prod: enlace de certificación no abre (caso 9411325)

**Estado:** diagnóstico con evidencia, **sin corrección entregada** (pausado el 2026-11-11, ver [[traspaso-sesion]]).

- **Síntoma:** el enlace "Certificar" del correo abre `https://datalake-%20prd-niif.auth.us-east-1.amazoncognito.com/...` y el navegador responde "no se puede obtener acceso a esta página" (DNS).
- **Script (solo lectura, CloudShell):** `Backend\cloudshell-inventario\evidencia-enlaces-cognito.sh` (`ENV=prd bash ...`; variables opcionales `DIAS_AUDITORIA`, `DIAS_LOGS`, `PROBAR_ENTRA=1`). No guarda tokens, correos ni JSON completos.
- **Evidencia:** `EvidenciasAws\evidencia-enlaces-cognito-prd\` (resumen en `00-resumen.txt`; consola Azure AD en `azure ad.txt`).

## Hechos comprobados (PASS en la evidencia del 2026-11-11)
- **Causa raíz:** un espacio dentro del host en el JSON de caso de **Auxilios Funerarios**, clave `email.stringVars.certificarLink`. Afecta a 2 archivos de `niif-prd-assets`: el de la raíz de la malla y `configs/certification/auxilioFunerario.json`. `nocertificarLink` está bien. Las otras 8 mallas o JSON están bien (son 18 enlaces en total).
- El host con espacio no resuelve en DNS. El correcto (`datalake-prd-niif`) resuelve y su dominio de Cognito está `ACTIVE`.
- El proveedor `AzureAD` (OIDC) responde con HTTP 200 desde AWS: Azure no es la causa.
- En DynamoDB, `niif-prd-short-links` tiene 2 enlaces cortos ya guardados con el host roto. Hay que corregirlos o regenerarlos.
- El cliente `d5qmm9fi64` / `48laf...` aparece solo en el respaldo `primas-arl/configs/certification/backups/primasArl_20260623_160752.json`; ese API ya no existe. Todos los JSON vigentes usan el API `jdwkua9re5` y el único cliente del pool (`datalake-prd-niif-api`).
- **Segundo bloqueo, más grave:** el único cliente de la aplicación tiene `CallbackURLs = ["https://localhost/post_login"]`, pero los enlaces mandan `redirect_uri=https://jdwkua9re5.../prd/post_login`.
  - Hipótesis fuerte: Cognito responde `redirect_mismatch` y el botón falla aunque se corrija el espacio.
  - La apoya que en 14 días no haya ni un evento `/post_login` en los logs del API.
  - Hay que pedir a DevOps (Terraform) que agregue el callback real y comprobar lo mismo en QA.
- El cliente usa el flujo `implicit` y el logout `https://localhost:/logout`. Es **propuesta** de brecha SEG.
- Las corridas del job de notificación en prod usan el JSON de la raíz de cada malla (por ejemplo `auxilios-funerarios/auxilioFunerario.json`). La copia de `configs/certification/` es un duplicado; Primas ARL la usó por última vez el 2026-10-05.
- En prod no hay JSON de caso de Rentas Vitalicias.
- El correo cita el bucket `niif-prd-assests`, que no existe (error de escritura; el bueno es `niif-prd-assets`).
- En los últimos 14 días no hubo eventos `/post_login` en los logs del API. El WAF está activo.

## Corrección propuesta (no ejecutada; prod la ejecuta DevOps por release manual)
1. Quitar el espacio en los 2 JSON de Auxilios y subirlos por el flujo de configuración (la fuente en Git).
2. Corregir o regenerar los 2 enlaces cortos de `niif-prd-short-links`.
3. Prevenir: validar el host de los enlaces al cargar la configuración (`read_global_config` o el validador de plantillas). Mejor aún, que el host no se escriba en cada JSON de caso: debe venir una sola vez del JSON global (regla de una sola fuente).
4. Reenviar el correo de Auxilios si el periodo sigue pendiente (depende de D4 en [[roadmap-notificaciones]]).

## Alerta de repositorio
- El commit `0c5931a` sacó `EvidenciasAws/` del `.gitignore` y subió a `main` la evidencia de prod de este caso. Esa evidencia incluye ids de Cognito, el client_id de Azure y el tenant.
- En la copia local, `.gitignore` vuelve a ignorarla, pero ese cambio **no está commiteado**. Que lo decida el usuario: commitear el `.gitignore` y `git rm --cached` de la carpeta (la historia la conserva igual; el repo es privado).
