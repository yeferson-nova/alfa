---
tipo: feature
estado: vigente
actualizado: 2026-11-26
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
- **Decisión del usuario (2026-11-25), ID de punta a punta en PCR:** el ID lo **genera la Step Function** (`$$.Execution.Name`); ese cambio es de la IaC (DevOps). **Nosotros lo recibimos de ahí en adelante** y lo pasamos en cada paso: configuradora, extracción, validación, remediación, Silver, SCD y certificación, hasta **certificar o no certificar y el cierre**. Es el mismo ID en todo el flujo y no se regenera en ningún paso.
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

## Primer job con el contrato v1 (2026-11-26)
La extracción de PCR Emisiones ya lo implementa en su repo (logs, trazas por paso, errores clasificados y EMF); detalle en [[modularizacion-extraccion]]. Es la referencia para los demás jobs de Glue.

## Fases
0 contrato → 1 ID de punta a punta → 4 IaC (DevOps) → 2a Glue genéricos → 3 lambdas → 2b Silver por malla → 2c resto de Glue → 5 publicar para tableros → 6 alertas. Detalle y repos por fase en el documento humano §7.

## Retención según las HU del banco (verificado 2026-11-19)
- **Decisión del usuario (2026-11-19):** retención de logs configurable por ambiente en la IaC; PRD empieza en **180 días** (DEV y QA: 30, propuesta).
- HU-GI-07: logs de CloudWatch con retención por ambiente ("corta en desarrollo, la necesaria en producción"), sin número; D-13 (Operación de Datos). No buscar el resultado de negocio en CloudWatch.
- HU-GI-05: el registro de ejecución por fase es la evidencia: **7 años**, exportado a S3 antes de vencer, sin valores de negocio. RM-D09 aplica lo mismo a las bitácoras de remediación ([[hu-remediacion]], [[hu-gobierno-ingestas]]).
- Consecuencia: los 7 años van en el Modelo Trazabilidad (`ejc-ejecuciones`), no en los logs. La copia de logs a S3 no es por cumplimiento.

## Verificación pendiente
- Consola (interno, `EvidenciasAws\dev-observabilidad\`, nunca en el informe): tandas 1 y 2 corrieron en `us-east-2` y no valen (lección: fijar la región). **Tanda 3 (us-east-1, 2026-11-19), puntos 0 a 6:**
  - Glue: `/aws-glue/jobs/{logs-v2,output,error}` sin retención; mensajes por corrida en `logs-v2`, flujos `<JobRunId>-driver` y `-1..n`.
  - Lambdas NIIF: todas `PassThrough` (sin X-Ray), formato `Text`, grupos sin retención. Hay dos generaciones (`datalake-dev-niif-*` y `niif-dev-*`).
  - Step Functions NIIF (24): todas con logs `OFF`; las 9 de IaC con X-Ray `true`; **0 con `EJECUCION_ID`** (ARQ-32 confirmado en lo desplegado). Hay 10 `orquestador-niif17-*` viejas.
  - Glue: los jobs de las mallas (`niif-dev-*`, repo `operations-manager`) sin `--enable-observability-metrics`; los `datalake-dev-niif-{extraccion,remediacion,scd,val-tecnica}` no están en ninguno de los dos repos IaC (ARQ-49).
  - **Tanda 4 (us-east-1):** cada corrida de Glue escribe en `logs-v2` (Spark), `output` y `error` (el `logging` de Python va a `error`; por confirmar con una muestra). Reglas de cuenta `ALFA-DEV-DATALAKE-GlueJobEvents` y `-SfnEvents` + `niif-dev-finalizar-ejecucion-primas-arl`. 187 alarmas, muchas de Application Insights `niif-dev` (Step Functions solo de ARL, Rentas y Emisiones; nada de Glue). SNS: solo los de certificación. API `dev` con logs de acceso y X-Ray. Sin EMF ni filtros de suscripción. `ejc-ejecuciones`: TTL `DISABLED`, PITR `ENABLED`, 0 exportaciones.
  - **Decisión de diseño:** la fase 6 parte de las reglas y de Application Insights que ya existen, no crea otras; hay que hablar con su dueño (prefijo `ALFA-DEV-DATALAKE`).
  - **Tanda 5 (us-east-1):** las reglas `ALFA-DEV-DATALAKE-*` van a `ALFA-DEV-DATALAKE-PipelineObserver` ("Fin de Glue Jobs y avisos de ejecución prolongada"); `finalizar-ejecucion-primas-arl` va a `niif-dev-finalizador-ejecucion`. Alarmas de Application Insights con acción: **0**; OpsCenter apagado, sin SNS. El flujo `error` de Glue mezcla Spark con el `logging` del código; `output` tiene 20 eventos. Logs de acceso del API en JSON con `requestId` y `xrayTraceId`.
  - **Error mío corregido:** `PipelineObserver` y los SNS `ALFA-DEV-DATALAKE-Critical`/`-Warning` ya estaban en [[inventario-lambdas]] y en `Docs\05-arquitectura\flujo-integral-niif17.md` §F; mi consulta de SNS filtró por `niif` y no los vio. Antes de concluir "no existe", buscar en la memoria y en Docs.
  - **Decisión:** la fase 6 reutiliza `PipelineObserver` y los SNS de plataforma; no se crea un SNS nuevo.
  - **Dueño (2026-11-19):** stack `stack-cf-observability`, sin etiquetas; creado y actualizado (15 y 20 de octubre de 2026) por un solo usuario. Cada SNS tiene una suscripción por correo. Ver [[preguntas-abiertas]].
- Código (2026-11-19): en la rama feature el job de notificación arranca por `job_notificacion.py` y el empaquetado solo lleva los paquetes nuevos, así que no registra correos. Pero el script viejo `src/jobs/notification_certification.py` (con correos en el log) sigue en el repo: borrarlo al integrar. La orquestadora pasa `--EJECUCION_ID` y registra `FLOW_ID_MISSING` si falta; en la certificación el argumento es opcional.

- **2026-11-20:** la fase 2a empieza por la extracción, dentro de [[modularizacion-extraccion]]: la duración de lectura de hoy engaña (ARQ-58) y el log por tramo trae filas, unidades y densidad.

Brechas: [[auditoria-observabilidad-2026-11]]. Estándar de logs: [[est-desarrollo-hexagonal]] §5.
