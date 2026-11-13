---
tipo: hu
estado: vigente
actualizado: 2026-11-03
---
# HUs de Remediación y Enriquecimiento de Datos (HU-RM-01 a 21)

Fuente: `Docs\03-historias-usuario\04_HUs_Tecnicas_Remediacion_Datos_v6.xlsx` (v4.0 interna). Remediación en la capa Plata. **"Independiente en ejecución, compatible por diseño"** con los temas 01, 02 y 03: replica sus patrones en modo autónomo. MVP: dominio Clientes con la fuente FUE-001 (Banco de Bogotá).

## Patrones técnicos que confirma (útiles como referencia)
- **Configuración:** DynamoDB `gob_remediacion_configuracion` con tabla única (`SISTEMA#` / `OBJETO#` / `REGLA#`), versionada y aprobada por PR.
- **Orquestación:** Step Functions `sfn_remediacion` con un solo disparo por lote (S3 → EventBridge o EventBridge Scheduler) y deduplicación.
- **Operación y aprobaciones (RM-D01):** **API Gateway + Lambda con autenticación por IAM Identity Center**, para aceptar o rechazar propuestas, interruptor de emergencia y ejecución bajo demanda.
- **Observabilidad:** métricas incrustadas en el log (espacio `DataLake/Remediacion`), Logs Insights, **alarmas con SNS**, tableros de CloudWatch y CloudTrail.
- **Calidad:** Glue Data Quality; compuerta antes de Oro (severidad alta detiene, media publica parcial y envía a cuarentena).
- **Retención (RM-D09):** Landing **7 años** (evidencia); Bronce mínimo 2 años.
- **Seguridad (HU-RM-19):** base legal y finalidad registradas por fuente (Ley 1581); enmascaramiento; derechos del titular; nada de datos personales reales fuera de producción.

## Catálogo de notificaciones (HU-RM-16)
- Cada notificación tiene código `NTF-REM-xx`, evento, severidad, destinatario, canal (correo, ticket, chat, resumen diario), acción esperada y tiempo de respuesta.
- **"Ninguna contiene datos personales."**

## Relevancia ([[tareas-aws]])
- **Tarea 1 (notificaciones):** modelo a copiar para el catálogo de escenarios 1d. Cada notificación del paso 3 debería tener código, severidad, destinatario, canal, acción y plazo.
- **Endpoint de certificación** ([[correo-notificacion]]): RM-D01 es el patrón corporativo (API Gateway + Lambda + IAM Identity Center). Respalda las brechas #1 a #3 de [[brechas-seguridad]].
- **SNS frente a SES:** el estándar usa SNS para alarmas operativas. La tarea 1a (SNS → SES) aplica al **correo de negocio**; las alarmas técnicas seguirían en SNS (propuesta, ver [[preguntas-abiertas]]).
- **Datos personales en el correo:** la regla "sin datos personales" aplica al correo de certificación. Los totales parecen agregados, sin datos personales; el CSV enlazado sí podría contener datos personales (brecha SEG candidata).
