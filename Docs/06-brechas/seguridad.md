# Riesgos de seguridad y mejoras: pasos 2 y 3 de la arquitectura AWS (NIIF 17)

**Fecha:** 2026-10-30
**Alcance:** Bodega NIIF 17 (paso 2) y Certificación de Cifras (paso 3), con énfasis en el paso 3.
**Base del análisis:** el diagrama de arquitectura, la lista de tareas (`Tareas\TareasAsignadas.txt`) y las cinco plantillas de correo en `Backend\reservas_matematicas_cert\`.

> **Limitación:** todavía no se ha revisado el código de las lambdas. Los riesgos 8, 9 y 14 son hipótesis típicas de este tipo de solución y deben confirmarse contra el código antes de darlos por ciertos.

## 1. Resumen

El mayor riesgo está en el correo de certificación. Un clic en **Certificar** o **No certificar** representa una decisión con valor regulatorio y contable, y hoy no sabemos si esa acción está protegida. El segundo riesgo es de negocio: no hay validación de consistencia de la reserva matemática antes de pedir la certificación, ni alertas cuando un proceso falla.

## 2. Riesgos

### Alta severidad

| # | Riesgo | Qué puede pasar | Mejora |
|---|---|---|---|
| 1 | Los enlaces de Certificar y No certificar no están protegidos (por confirmar) | Cualquiera con el enlace puede certificar o rechazar cifras | Token firmado de un solo uso, ligado al usuario, periodo y caso de uso, con expiración |
| 2 | Las acciones se ejecutan al abrir el enlace (GET) | Los escáneres de correo y antivirus abren los enlaces y certifican sin intervención humana | El enlace abre una página de confirmación y la decisión se envía con POST |
| 3 | No hay autenticación ni registro de quién certificó | Sin trazabilidad. El correo puede reenviarse a terceros | Inicio de sesión (SSO) en el endpoint. Registrar usuario, fecha e IP |
| 5 | Descarga de datos por enlace prefirmado de S3 | Un enlace de larga duración y reenviado filtra datos de primas y reservas | Expiración corta, bucket privado, cifrado con KMS, registro de accesos |
| 7 | El correo puede falsificarse sin SPF, DKIM y DMARC | Phishing con botones de certificar | Verificar el dominio en SES y publicar DMARC estricto |
| 10 | No hay alertas cuando un proceso falla (tarea 1c) | Cifras desactualizadas sin que nadie lo note | Alarmas de CloudWatch, destinos de fallo y cola de errores, con aviso a soporte |
| 11 | No hay validación de consistencia antes de certificar (tarea 3) | Se puede certificar un dato incorrecto | Lambda de validación que bloquee el correo y avise a soporte |

### Media severidad

| # | Riesgo | Mejora |
|---|---|---|
| 4 | Se puede certificar y luego rechazar la misma cifra | Estado atómico en base de datos y bloqueo tras la primera decisión |
| 6 | Posible inyección de HTML o enlaces al armar el correo si los valores no se escapan | Escapar valores, validar URLs contra una lista permitida, o usar plantillas de SES |
| 8 | Permisos de las lambdas demasiado amplios (por verificar) | Un rol por lambda, con privilegio mínimo y sin comodines |
| 9 | Secretos y destinatarios en código o variables de entorno (por verificar) | Secrets Manager o Parameter Store |
| 14 | Modelos de programación mezclados en las lambdas del gestor de configuración (tarea 2) | Un único modelo con utilidades comunes |

### Baja severidad

| # | Riesgo | Mejora |
|---|---|---|
| 5b | El enlace a Redshift está comentado en las plantillas | Si se reactiva, no exponer endpoints de base de datos en el correo |
| 12 | Cifras de totales viajan por correo | Dejar en el correo solo totales agregados. El detalle va detrás de autenticación |
| 13 | Cinco plantillas casi idénticas | Una plantilla base con variables por caso de uso |

## 3. Orden recomendado

1. **Diseño del correo de certificación:** riesgos 1, 2, 3, 5 y 7.
2. **Migración a SES:** riesgos 6 y 13.
3. **Tareas 1c y 3:** riesgos 10 y 11.
4. **Tras revisar el código:** riesgos 8, 9 y 14.

## 4. Preguntas abiertas

- ¿Los enlaces de Certificar y No certificar ya tienen un endpoint, o hay que construirlo?
- ¿Qué requisitos de auditoría aplican a la certificación (quién, cuándo, evidencia)?
- ¿Qué pasa con el flujo si no se certifica o si no hay respuesta?
