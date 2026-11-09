---
tipo: referencia
actualizado: 2026-11-09
---
# Correo de notificación de certificación

Ejemplo en `Docs\05-arquitectura\ejemplo-correo-certificacion.png`. Es el objetivo de la tarea 1b ([[tareas-aws]]). El correo no lleva adjuntos, solo enlaces a datos y acciones.

- **De:** notificaciones@segurosalfa.com.co. **Para:** varios por caso de uso.
- **Asunto:** `NIIF 17 - Certificación <CasoUso> Periodo <AAAAMM>` (ejemplo: Primas ARL, 202502).
- **Cuerpo HTML:**
  - Saludo y texto con caso de uso y rango de fechas (`01/02/2025 - 28/02/2025`).
  - Tabla resumen: periodo, Total Registros, Valor Total Primas.
  - Enlace "Descargar CSV".
  - Botones/enlaces **Certificar** (verde) y **No certificar** (rojo).
  - Aviso de reportar incidencias por mesa de ayuda si no se certifica.
  - Firma "Administrador Datalake - Vicepresidencia de Tecnología e Información" y aviso de no responder.

## Plantillas compatibles (2026-11-03)
`Backend\htmlNew\` contiene las 5 plantillas reescritas para clientes de correo: tablas, estilos en línea, botones VML para Outlook y `@media` para móvil. Tienen los mismos placeholders (`$useCaseName`, `$periodRange`/`$perido_a`, `$tableTotal`, `$tableSummary`, `$tableDetail`, `$certificarLink`, `$nocertificarLink`). Detalle y pendientes en su `README.md`. Las tablas las inyecta el backend (clase `dataframe`, probablemente pandas); para máxima fidelidad deberían llegar con estilos en línea. Errata en reservas: `$perido_a - $perido_a`.

## Diseño nuevo, reemplazo directo (2026-11-03)
- `Backend\htmlNew\newUx\` contiene las mismas 5 plantillas con el diseño del mockup `Backend\htmlNew\mockup\propuesta-ux-notificaciones.html`.
- Mismos placeholders y mismo orden; las lambdas no cambian.
- El usuario pidió explícitamente que sean **las mismas plantillas** con el diseño nuevo, no un sistema de componentes nuevo.
- Fase 2 (requiere cambios en la lambda): tarjetas de cifras, validaciones, plazo, ID de ejecución y estados adicionales.

## Variables de la plantilla SES (propuesta)
`caso_uso`, `periodo`, `fecha_ini`, `fecha_fin`, `total_registros`, `valor_total`, `url_csv`, `url_certificar`, `url_no_certificar`.

## Implicaciones técnicas
- URL prefirmada de S3 para el CSV, con expiración.
- Endpoint (API Gateway + Lambda) para Certificar / No certificar, con token firmado de un solo uso. Registra decisión, usuario y fecha.
- Identidad del dominio verificada en SES (SPF/DKIM).

Envío por SES (1a) ya implementado en el job de notificación: [[job-notificacion-ses]]. Falta la plantilla HTML (1b).

Ver [[diseno-propuesto]] y [[preguntas-abiertas]].

## Estructura y uso actual (2026-11-09)
`Backend\htmlNew\` quedó organizado: `newUx\` (6 plantillas de producción, incluida `primas_rentas_cert.html`), `anterior\`, `mockup\` (4 estados y variaciones), `previews\` (6 escenarios por plantilla) y `herramientas\` (generador y validador contra el estándar de correo). Las plantillas se suben a `niif-dev-assets/email-templates/` y el job las lee con `email.templatePath` del JSON del caso. Detalle de cambios, marcadores (`$avisoHtml` incluido) y pruebas en [[job-notificacion-ses]] y en `htmlNew\README.md`.
