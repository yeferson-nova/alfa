---
tipo: repo
estado: vigente
actualizado: 2026-11-12
repo: tpl-datalake-niif-email-template-s3
categoria: s3
paso: "3 · Plantillas del correo de certificación"
malla: transversal
---
# tpl-datalake-niif-email-template-s3
> Plantillas HTML del correo de certificación (diseño nuevo, solo tablas y estilos en línea). Jenkins las despliega a la carpeta `email-template/` del bucket de assets.

## Rol en el flujo
- Paso 3. El job de notificación lee la plantilla indicada por `email.templatePath` en el JSON del caso y sustituye las variables con `string.Template`.
- Entrega el HTML final al envío por SES.

## Entradas y salidas
- Seis plantillas: `reservas_matematicas_cert`, `primas_arl_cert`, `auxilio_funerario_cert`, `primas_rentas_cert`, `rentas_vitalicias_cert` y `previsional`.
- Contrato de variables: `$useCaseName`, `$periodRange`, `$tableTotal`, `$tableSummary`, `$tableDetail`, `$certificarLink`, `$nocertificarLink` y `$avisoHtml` (recuadro de regeneración, sin datos o disponibilidad por miembro).
- Sale: HTML sin variables sobrantes; botones Certificar y No certificar compatibles con Outlook, Gmail y móvil (según su README, solo validación estática).

## Recursos AWS que toca
- Bucket `niif-{env}-assets`, ruta `email-template/` (`s3StaticDeployPipeline` con sync y `--delete`).

## Código
- Sin código: solo HTML, `Jenkinsfile` y `sonar-project.properties`. El README raíz es el genérico de despliegue.
- Rama nueva: `src/email-template/README.md` documenta el diseño, la compatibilidad, el contrato con el job y las mejoras de fase 2. Las plantillas de main ya coinciden con el diseño nuevo.

## Estado
- En Alfa: `origin/quality`, commit bd77b25 (2026-11-12). Desplegado: por confirmar.
- Código nuevo nuestro: rama `feature/codigo-nuevo-notificacion`, solo agrega el README de plantillas. Falta probar en clientes de correo reales.

## Brechas y relacionadas
- Los JSON de caso deben apuntar a `email-template/` (singular); corrección R-01 en [[repos-alfa-notificacion]]. Formato del correo: [[correo-notificacion]].
- Relacionado: [[cfg-datalake-niif-config-s3]], [[data-datalake-niif-notification-certification-glue-job]], [[job-notificacion-ses]], [[repo-alfa-repos]]
