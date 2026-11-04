# Propuesta: observabilidad y eventos (plano técnico y plano de negocio)

> **PROPUESTA PENDIENTE DE REFINAR Y VALIDAR.** No es una decisión tomada. Surge de la iteración del 2026-11-03 sobre la tarea 1c (notificar a soporte cuando un proceso falla) y su relación con la 1a (SNS → SES). Antes de construir hay que cerrar los puntos de la sección "Pendiente de validar".

## Idea central
**SNS no crea eventos en CloudWatch; es al revés.** CloudWatch y EventBridge detectan lo que pasa y lo publican en SNS, que lo reparte a varios suscriptores a la vez. Con ese principio, la solución se organiza en dos planos que comparten infraestructura:

- **Plano técnico:** ¿la solución está funcionando? Sirve a soporte.
- **Plano de negocio:** ¿qué pasó en el proceso? Sirve al negocio y a la gerencia.

```
PLANO TÉCNICO (¿funciona?)                         PLANO DE NEGOCIO (¿qué pasó?)
Lambda · Step Functions · Glue · Fargate           Lambdas del flujo publican eventos de dominio:
   │ métricas automáticas + logs JSON                certificacion_solicitada, certificada,
   │ (con ejecucion_id)                              rechazada, validacion_con_alertas, proceso_fallido
   ▼                                                    │
CloudWatch (métricas, logs, alarmas)                    ▼
EventBridge (cambios de estado nativos:            SNS  tema "eventos-negocio"  ──┬─► Lambda → SES (correo de certificación, tareas 1a/1b)
  Glue job FAILED, Step Functions FAILED,                                         ├─► Firehose → S3 lago (Parquet) → Glue Catalog → Athena → Power BI / Grafana
  tarea ECS/Fargate detenida)                                                      └─► (futuro) otros consumidores sin tocar el emisor
   │
   ▼
SNS  tema "alertas-tecnicas" ──┬─► ServiceNow (ticket automático)
                               └─► correo de soporte (respaldo)
Visualización: CloudWatch Dashboards o Grafana (fuente de datos CloudWatch)
```

## Qué resuelve
- **Soporte se entera solo:** alarma → SNS → ticket en ServiceNow. Responde a la tarea 1c.
- **Escala sin desarrollo:** lambdas, Fargate o Glue que se integren después entran al mismo esquema, porque sus métricas y cambios de estado ya llegan solos a CloudWatch y EventBridge.
- **Resuelve el choque de la tarea 1a con el estándar:** SNS sigue siendo la columna vertebral de eventos, como dicen las HUs. SES es solo un suscriptor, el que envía el correo de negocio.
- **Responde preguntas de negocio** (tiempo de certificación, tasa de rechazo, fuentes que más fallan) en Power BI, desde los mismos eventos y sin leer logs técnicos.

## Soporte en ServiceNow (confirmado que la mesa de ayuda usa ServiceNow)

| Opción | Flujo | Ventajas | Requiere |
|---|---|---|---|
| **A. ServiceNow Event Management (ITOM)** | Alarma → SNS → Lambda → API de eventos (`em_event`) | ServiceNow deduplica, correlaciona y crea el incidente con sus propias reglas | Licencia ITOM y reglas configuradas por el equipo de ServiceNow |
| **B. AWS Service Management Connector** | Alarma → OpsItem de Systems Manager OpsCenter → el conector lo sincroniza como incidente | Sin código propio; soportado por AWS; también trae Security Hub y AWS Health | Que el equipo de ServiceNow instale la aplicación; puede que ya exista |
| **C. Lambda propia** | Alarma → SNS → Lambda → API de tablas (`/api/now/table/incident`) | Control total del ticket | Código propio, OAuth en Secrets Manager, salida a internet (NAT o proxy) |

Se descarta **SNS → correo → regla de correo entrante en ServiceNow**: es frágil, no deduplica y no cierra el ticket solo.

**Recomendación provisional:** B si el conector ya existe; si no, A si hay licencia ITOM; C solo como último recurso.

**Reglas de diseño, sea cual sea la opción:**
- **Un ticket por alarma y recurso**, con llave de deduplicación (ej. `niif17-prd-consolidacion-lambda-Errors`). Las repeticiones actualizan el mismo ticket.
- **Cierre automático** cuando la alarma vuelve a OK.
- **Severidad mapeada:** la severidad de la taxonomía de errores (HU-GI-06) se traduce a impacto y urgencia (ej. alta → P2, media → P3). Lo que no requiere acción va solo al tablero.
- **Ticket accionable:** caso de uso, periodo, etapa, código de error, `ejecucion_id`, enlace a logs y grupo de asignación. Sin datos personales.
- **Correo a soporte como respaldo**, por si ServiceNow no está disponible.
- **Opcional:** crear el ticket automáticamente cuando el negocio marca **No certificar**, con el motivo que escribió.

## Respaldo en los estándares

| Pieza | ¿Respaldada? |
|---|---|
| CloudWatch + CloudTrail, catálogo de métricas y alarmas con destinatario | Sí: HU-GI-07 |
| Alarmas por SNS | Sí: HU-RM-16 y observabilidad de remediación |
| EventBridge | Sí: remediación (HU-RM-20) |
| Taxonomía de errores con severidad y acción | Sí: HU-GI-06 |
| Power BI como consumo | Sí: HU-SG-12, por la vista certificada y con identidad propagada, no directo a S3 |
| Grafana | **No aparece.** Si se usa, solo como visor sobre CloudWatch o Athena |
| Eventos de negocio en el lago | Encaja con las tablas `aud_*` (HU-TR-01); el formato se decide con Arquitectura |

## Riesgos de costo a fijar desde el diseño
- **No usar `ejecucion_id` como dimensión de una métrica:** cada valor crea una métrica nueva que se cobra cada mes. El ID va en el log.
- **La ingesta de CloudWatch Logs es el costo principal:** controlar el nivel de detalle por ambiente (decisión D-13 del estándar).
- **Eventos de negocio en Parquet, particionados por fecha** y consultados con un grupo de trabajo de Athena con tope de escaneo (HU-SG-11).

## Pendiente de validar
1. **Plano de negocio:** ¿SNS + Firehose o EventBridge? Recomendación: EventBridge para los cambios de estado de servicios AWS, que llegan ahí de forma nativa, y SNS para entregar alarmas y repartir eventos de negocio.
2. **Visualización técnica:** ¿CloudWatch Dashboards (incluido y en el estándar) o Grafana? ¿Ya existe Grafana en la compañía?
3. **ServiceNow:** ¿tienen ITOM Event Management o solo ITSM? ¿Está instalado el AWS Service Management Connector? ¿Cuál es el grupo de asignación de Datalake? ¿Las lambdas tienen salida a internet?
4. **Ticket automático** al marcar No certificar: ¿se quiere?
5. **Decisiones de certificación como evidencia** (retención de 7 años): ¿solo en el lago o también en una tabla de auditoría inmutable?
6. **Aval de Arquitectura** para SES y Grafana, que no están en los estándares.

Relacionado: `Tareas\riesgos-flujo-glue-notificacion.md` (cómo llegan los datos a la notificación) y `Docs\06-brechas\monitoreo-continuo-grafana.md` (versión anterior del monitoreo, a reemplazar cuando esta propuesta se valide).
