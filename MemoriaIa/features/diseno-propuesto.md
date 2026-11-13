---
tipo: feature
estado: vigente
actualizado: 2026-11-03
---
# Diseño propuesto (no confirmado)

Diagrama: `Docs\05-arquitectura\aws_pasos_2_y_3.drawio`. Tareas en [[tareas-aws]].

- **Lambda de Validación (nueva, tarea 3):** en el paso 3, entre Consolidación y Transporte. Cada regla es un validador independiente, definido en el gestor de configuración (por eso la tarea 2 va antes). Si falla, bloquea la salida a S3 y dispara notificación a soporte.
- **Alertas de fallo (1c):** también para errores de Extracción, Preparación y Consolidación. Opciones: Step Functions con `Catch`, destinos `onFailure` de lambdas o alarmas de CloudWatch hacia la Lambda Notificación.
- **SES (1a, 1b):** plantilla HTML parametrizada por caso de uso, ver [[correo-notificacion]].
- **Escenarios adicionales (1d):** éxito, fallo, cifras con diferencias, archivo vacío o ausente, timeout, falla del transporte, "No certificar", enlace expirado o sin respuesta.
- **Componentes que faltan en el diagrama:** URL prefirmada de S3 para el CSV, y API Gateway + Lambda de certificación con token firmado.
- El resultado de la validación se incluye en el correo.

- **Contrato entre etapas (propuesta, 2026-11-03):** manifiesto inmutable por ejecución y Step Functions con espera de la decisión humana. Detalle en [[riesgos-glue-notificacion]].

Dudas: [[preguntas-abiertas]].
