# T1. Ajuste de notificaciones AWS

**Estado:** en análisis. **Depende de:** T2 (formato de logs y taxonomía de errores comunes).

## Qué se pidió
- **1a.** Cambiar el servicio SNS por SES.
- **1b.** Presentar el correo de notificación en HTML.
- **1c.** Notificar al equipo de soporte cuando un proceso falla.
- **1d.** Revisar escenarios adicionales de notificación que hagan falta.

## El problema
El correo de certificación es la puerta de control antes de que las cifras pasen al cálculo actuarial. Hoy presenta tres problemas:

1. **Formato.** SNS solo envía texto plano, sin tabla de totales ni botones. El formato deseado (ejemplo en `Docs\05-arquitectura\ejemplo-correo-certificacion.png`) incluye:
   - asunto `NIIF 17 - Certificación <caso> Periodo <AAAAMM>`;
   - tabla con periodo, total de registros y valor total;
   - enlace "Descargar CSV";
   - botones Certificar y No certificar.
2. **Seguridad.** Los botones son enlaces que ejecutan una decisión de negocio. Sin protección:
   - cualquiera con el correo podría certificar;
   - un escáner antivirus que abre los enlaces podría certificar sin intervención humana;
   - no queda registro de quién decidió.
3. **Operación.** Si un proceso falla, nadie se entera hasta que alguien pregunta por las cifras.

## Estado actual (lo que sabemos)
- Hay cinco plantillas HTML casi idénticas, una por caso de uso, en `Backend\reservas_matematicas_cert\`.
- **Por confirmar** (sin acceso a AWS ni al código):
  - si hoy el correo sale por SNS en texto plano;
  - si los botones ya tienen un servicio que los atienda;
  - qué pasa en el flujo si nadie certifica.

## Qué exigen los estándares
| Tema | Requisito | Fuente |
|---|---|---|
| Canal | Las alarmas técnicas usan SNS. **SES no aparece en ningún estándar.** | HUs Gobierno de Ingestas y Remediación |
| Contenido | Ninguna notificación lleva datos personales. Cada una tiene código, severidad, destinatario, canal, acción esperada y plazo. | HU-RM-16 |
| Aprobaciones | Las decisiones de negocio se toman por API Gateway + Lambda con identidad federada (IAM Identity Center) y quedan auditadas. | HU-RM (decisión RM-D01), HU-SG-04 y HU-SG-18 |
| Fallos | Correo a responsables ante fallas; validación de archivo sin abortar; monitoreo de fallos en tiempo real. | Estándar DataStage 8.9-8.10, Estándar ETL 6.1 |
| Monitoreo | Logs estructurados con identificador de ejecución, catálogo cerrado de eventos, métricas y alarmas con destinatario. | HU-GI-05 a 07 |
| Precedente | Nuevos Cargues ya aprueba o niega cargues con un correo de cifras de control y un frontend en S3. | Manual Nuevos Cargues V5 |

## Propuesta
- **1a:** SES solo para el correo de negocio (certificación); las alarmas técnicas siguen en SNS, como dice el estándar. El dominio remitente (`notificaciones@segurosalfa.com.co`) se verifica con SPF, DKIM y DMARC.
- **1b:** una sola plantilla SES parametrizada en lugar de cinco HTML. Variables: `caso_uso`, `periodo`, `fecha_ini`, `fecha_fin`, `total_registros`, `valor_total`, `url_csv`, `url_certificar`, `url_no_certificar`. Todo valor se escapa antes de insertarlo.
- **Botones Certificar / No certificar:**
  - el enlace lleva a una página de confirmación, no ejecuta la acción;
  - la decisión se envía por POST a API Gateway + Lambda;
  - token firmado, de un solo uso y con expiración, y autenticación corporativa;
  - se registra usuario, fecha, IP y decisión;
  - el estado es atómico, para impedir que se decida dos veces.
- **Enlace del CSV:** URL prefirmada de S3 de corta duración, sobre un bucket privado cifrado con KMS.
- **1c:** por decisión previa, soporte se entera por un **monitoreo continuo** en lugar de correos sueltos: logs estructurados, métricas, tablero y alertas. La herramienta propuesta es Grafana; falta su aval (ver abajo). Detalle en `Docs\06-brechas\monitoreo-continuo-grafana.md`.
- **1d:** catálogo de notificaciones con el formato del estándar. Escenarios identificados:
  - certificación solicitada, exitosa y rechazada;
  - fallo en extracción, preparación, consolidación o transporte;
  - cifras con diferencias (sale de T3);
  - archivo vacío o ausente;
  - tiempo agotado;
  - enlace expirado;
  - certificación sin respuesta en el plazo.

## Avance
- **2026-11-03:** las 5 plantillas actuales se reescribieron con estándar de correo compatible con Outlook, Gmail y móvil, con el mismo diseño y los mismos placeholders. Están en `Backend\htmlNew\`; ver su `README.md`.
- **2026-11-03:** mockup de propuesta UX/CX en `Backend\htmlNew\propuesta-ux-notificaciones.html`. Una sola plantilla para todas las fuentes, con 6 estados (pendiente, con alertas, recordatorio, certificado, no certificado, fallo), validaciones de T3 en el correo, plazo y vencimiento visibles, y página de confirmación con inicio de sesión. Pendiente de validar con negocio.
- **2026-11-03:** las 5 plantillas actuales tienen ya el diseño nuevo en `Backend\htmlNew\newUx\`, como reemplazo directo con los mismos placeholders y sin cambios en las lambdas. Las mejoras que requieren datos nuevos (cifras con variación, validaciones, plazo, estados) quedan para una fase 2.

## Lo que se puede adelantar sin acceso
1. Plantilla SES única en HTML, lista para cargar (partir de `Backend\htmlNew\`).
2. Catálogo de notificaciones (código, evento, severidad, destinatario, canal, acción, plazo).
3. Diseño del servicio de certificación: API, token, página de confirmación y registro de decisiones.
4. Actualizar el diagrama `Docs\05-arquitectura\aws_pasos_2_y_3.drawio`.

## Bloqueos y preguntas
- ¿Arquitectura aprueba SES y Grafana, que no están en los estándares?
- ¿Dónde corre Grafana? ¿Existe OpenSearch o Elastic corporativo? ¿Qué canal usa soporte (correo, tickets, chat)?
- ¿El CSV descargable contiene datos personales? Define el nivel de protección del enlace.
- ¿Se puede reutilizar el mecanismo de aprobación de Nuevos Cargues?
- ¿Quiénes son los destinatarios por caso de uso?

## Propuesta de observabilidad y eventos (pendiente de refinar y validar)
Ver `propuesta-observabilidad-eventos.md` en esta carpeta. Organiza la solución en dos planos:
- **Técnico:** CloudWatch/EventBridge → SNS → ServiceNow. Responde a la 1c.
- **Negocio:** eventos → SNS → SES y lago → Power BI.

SNS se mantiene como columna vertebral, como dicen los estándares, y SES pasa a ser un suscriptor más (1a). Soporte usa ServiceNow (confirmado).

## Riesgos del flujo de datos
Ver `Tareas\riesgos-flujo-glue-notificacion.md`. En esta tarea aplican sobre todo: notificaciones duplicadas o parciales (3), inyección HTML (5), límite de 256 KB de SNS (6), periodo equivocado (8) y destinatarios sin control (9).

## Riesgos de seguridad relacionados
Ver `Docs\06-brechas\seguridad.md`, puntos 1 a 7, 10 y 12 a 13: enlaces sin protección, acción por GET, falta de autenticación, URL del CSV, SES sin SPF/DKIM, sin alertas de fallo y plantillas duplicadas.
