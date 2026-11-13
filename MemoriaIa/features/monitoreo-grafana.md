---
tipo: feature
estado: vigente
actualizado: 2026-11-10
---
# Monitoreo continuo a Grafana (en iteración)

Humano: `Docs\06-brechas\monitoreo-continuo-grafana.md`.

## Decisiones del usuario
- **Cambio de alcance (2026-11-10, manda sobre lo de abajo):**
  - Grafana **ya no es tarea nuestra**. Entregamos el ciclo de observabilidad completo en logs JSON, métricas EMF y alarmas de CloudWatch, listo para conectar un Grafana si se requiere.
  - Las alertas técnicas salen por el **mismo SES** que el correo de negocio, al grupo de soporte (pendiente: cuál es ese grupo).
  - Lo que falte en la arquitectura se le sugiere al arquitecto (ARQ-31 en [[notificaciones-vs-arquitectura]]).
- Soporte **no** se entera por email: la tarea 1c ([[tareas-aws]]) pasa a ser un ciclo de monitoreo continuo con logs hacia Grafana.
- Alcance: toda la solución NIIF 17, **solo del lado AWS**.
- Grafana: **no definido** (dónde corre, quién lo administra).
- Origen de logs: sin decidir. El usuario preguntó si aplica Elastic: sí, Grafana consulta Elasticsearch/OpenSearch.
- Canal de alerta a soporte: **ServiceNow** (confirmado por el usuario, 2026-11-03). La forma de integrarlo está en iteración: Event Management, AWS Service Management Connector o Lambda con la API.

## Propuesta de dos planos (PENDIENTE DE REFINAR Y VALIDAR, 2026-11-03)
Humano: `Tareas\T1-notificaciones\propuesta-observabilidad-eventos.md`. Esta propuesta reemplazaría a la de "solo Grafana" cuando se valide.
- **Principio:** SNS no crea eventos en CloudWatch; CloudWatch y EventBridge publican en SNS, que reparte a los suscriptores.
- **Plano técnico:** métricas y logs JSON (con `ejecucion_id`) + cambios de estado nativos en EventBridge (Glue, Step Functions, ECS) → alarmas → SNS `alertas-tecnicas` → ServiceNow y correo de respaldo. Visor: CloudWatch Dashboards o Grafana.
- **Plano de negocio:** eventos de dominio (`certificacion_solicitada`, `certificada`, `rechazada`, `validacion_con_alertas`, `proceso_fallido`) → SNS `eventos-negocio` → Lambda SES (correo 1a/1b) + Firehose → S3 Parquet → Athena → Power BI.
- **Concilia la tarea 1a con el estándar:** SNS sigue siendo la columna vertebral; SES es un suscriptor más.
- **ServiceNow:** A Event Management (ITOM), B AWS Service Management Connector (vía OpsCenter, sin código), C Lambda + API de tablas. Recomendación provisional: B, luego A, C como último recurso. Reglas: deduplicación por alarma y recurso, cierre automático al volver a OK, severidad de HU-GI-06 → impacto y urgencia, sin datos personales. Opcional: ticket automático al No certificar.
- **Costos:** nunca `ejecucion_id` como dimensión de métrica; nivel de log por ambiente (D-13); Parquet particionado + workgroup con tope (HU-SG-11).
- **Por validar:** SNS+Firehose o EventBridge; Dashboards o Grafana; módulos de ServiceNow, conector, grupo de asignación, salida a internet; ticket al No certificar; dónde se guarda la evidencia de certificación (7 años); aval de SES y Grafana.

## Opciones de almacenamiento de logs (sin decidir)
- A. CloudWatch Logs directo (recomendada para empezar).
- B. CloudWatch -> OpenSearch -> Grafana.
- C. Elastic corporativo, si existe.

## Independiente de la opción
- Logs JSON estructurados con campos comunes (id de correlación, caso de uso, periodo, paso, componente, estado, error). Se define en el modelo único de lambdas (tarea 2).
- Métricas además de logs (errores y duración de lambdas, estado de jobs, conteo de registros).
- Ciclo: emitir, recolectar, almacenar, visualizar, alertar, atender, revisar.

Preguntas abiertas en [[preguntas-abiertas]]. Relacionado: [[diseno-propuesto]].
