---
tipo: feature
estado: vigente
actualizado: 2026-11-19
---
# Roadmap de observabilidad (Glue, Step Functions y lambdas)

> Plan por fases para seguir una corrida de punta a punta con un solo `ejecucion_id`, medir la duración de cada parte y dejar logs y métricas listos para que otro equipo arme tableros. Propuesta sin aprobar; no se tocó código.

Humano: `Docs\06-brechas\roadmap-observabilidad.md` (diagnóstico por repo, contrato de log v1, preguntas que responde, fases y decisiones). Antes: [[monitoreo-grafana]]. Alertas futuras: propuesta de dos planos en esa misma nota.

## Hechos verificados en código (`main` de `alfa-repos`, 2026-11-19)
- Ninguno de los 47 `startJobRun` de las 8 Step Functions pasa `$$.Execution.Name`; el `$$.Execution.Id` solo va a `ejc-ejecuciones` y `pipeline-estado` (ARQ-32 sigue).
- Los configuradores ya crean el ID raíz y el registro `EJC#`: son el punto de arranque.
- Los 12 repos de Glue registran texto libre, sin ID ni duración estructurada (ARQ-53).
- `niif_observability` (rama `feature/codigo-nuevo-notificacion`, 7 copias) da el MDC (`bind`) y EMF, pero no mide duración (ARQ-54).
- IaC: Glue con logs continuos y métricas en los 47 jobs; Step Functions sin logs; lambdas sin `tracing_mode` ni retención; sin alarmas ni reglas de fallo (ARQ-51, ARQ-52, COS-10).

## Decisiones de diseño (propuesta)
- Contrato v1 = nombres que ya usa el código (`marcaTiempo`, `nivel`, `evento`, `mensaje`, `funcion`, `ejecucion_id`, `request_id`, `periodo_negocio`, `use_case_id`) + `esquema`, `ambiente`, `malla`, `etapa`, `paso`, `duracion_ms`, `resultado`, conteos, `suma_control`, `config_version` y campos de error.
- Eventos comunes: `EJECUCION_INICIADA`, `COMPONENTE_INICIO/FIN`, `PASO_INICIO/FIN`, `CUADRE_RESULTADO`, `ERROR_CLASIFICADO` + los de la cadena de certificación.
- **Tres niveles = logs, métricas y trazas (decisión del usuario, 2026-11-19).** Trazas: X-Ray en lambdas, Step Functions y API; en Glue (sin X-Ray), traza por logs con `ejecucion_id` y tramos `PASO_INICIO`/`PASO_FIN`.
- Duración por job sin código: logs `ALL` de la Step Function sin datos de ejecución. El código mide solo los pasos internos.
- Destino: CloudWatch Logs + copia a S3 Parquet para Athena; Grafana u otro se conecta después.
- **Alcance (usuario, 2026-11-19):** no hacemos tableros ni elegimos herramienta. Llegamos hasta dejar logs, métricas y trazas publicados con su guía (fase 5).

## Impacto en infraestructura (2026-11-19, verificado en el drawio)
- El diagrama solo trae CloudWatch y Modelo Trazabilidad (DynamoDB) en la `Página-1`, sin conexiones; la `Página-2` no dibuja nada (ARQ-31). El flujo de datos no cambia.
- Sin servicios nuevos: código, configuración de CloudWatch, Step Functions, Glue y API, y el historial por paso en el Modelo Trazabilidad (propuesta: `ejc-ejecuciones`).
- La propuesta ([[arquitectura-propuesta-niif17]]) tampoco dibuja observabilidad: su nota de la `Página-2` solo promete logs JSON con `ejecucion_id` y `cert_code` en la cadena de certificación. Ya se dibujó ahí (2026-11-19): elementos 79 a 84 en las hojas `Página-1` y `Página-2`.
- Grupos (evaluador compuesto): una certificación viene de varias corridas, así que logs y trazabilidad llevan `ejecucionesOrigen`.
- Fuera del diagrama, y piden aval del arquitecto: X-Ray; opcionales Firehose → S3 → Athena (solo si hay retención larga) y EventBridge + SNS (alertas). Detalle: documento humano §7.1.

## Fases
0 contrato → 1 ID de punta a punta → 4 IaC (DevOps) → 2a Glue genéricos → 3 lambdas → 2b Silver por malla → 2c resto de Glue → 5 publicar para tableros → 6 alertas. Detalle y repos por fase en el documento humano §7.

Brechas: [[auditoria-observabilidad-2026-11]]. Estándar de logs: [[est-desarrollo-hexagonal]] §5.
