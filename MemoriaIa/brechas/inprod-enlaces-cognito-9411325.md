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
- Hay dos combinaciones de cliente y `redirect_uri` en uso: el API `jdwkua9re5` y el API `d5qmm9fi64`. Está **por aclarar** cuál es el oficial (brecha de configuración duplicada).
- El cliente de la aplicación usa el flujo `implicit` y tiene como callback `https://localhost/post_login`. Es **propuesta** de brecha SEG: el flujo implicit no se recomienda y el callback de localhost no debería estar en prod.
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
