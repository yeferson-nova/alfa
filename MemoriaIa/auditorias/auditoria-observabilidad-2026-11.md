---
tipo: auditoria
estado: vigente
actualizado: 2026-11-19
---
# Auditoría de observabilidad (2026-11-19)

> Brechas de logs, trazas, métricas y alarmas encontradas al revisar con código los 28 repos de `main` y la IaC. Humano: `Docs\06-brechas\roadmap-observabilidad.md` §8.

- **ARQ-51** Avisos de falla solo de plataforma y parciales: `ALFA-DEV-DATALAKE-PipelineObserver` (CloudFormation) avisa por SNS `ALFA-DEV-DATALAKE-Critical`/`-Warning` (correo) cuando falla una Step Function; de Glue y crawlers solo publica métricas. Sin aviso fuera de las Step Functions (configuradores, job de notificación, lambdas de certificación, SFTP, rebotes). Application Insights `niif-dev` tiene alarmas **sin acción** (0 de N avisan). Detalle previo en `Docs\05-arquitectura\flujo-integral-niif17.md` §F.
- **ARQ-52** Step Functions sin logs (DEV: 24 en `OFF`); lambdas sin X-Ray (DEV: todas `PassThrough`); Glue de las mallas sin `--enable-observability-metrics`. El API **sí** tiene logs de acceso y X-Ray en DEV.
- **ARQ-53** Los 12 repos de Glue registran texto libre, sin `ejecucion_id`, `JOB_RUN_ID`, duración ni conteos estructurados.
- **ARQ-54** Contrato de log sin unificar (skill con `ts`/`periodo`, código con `marcaTiempo`/`periodo_negocio`); la librería no trae `malla`, `etapa`, `paso`, `duracion_ms`, `resultado`, `config_version`.
- **ARQ-55** `pipeline-estado` (llave `amb#malla#periodo`) se sobrescribe: no guarda historial por paso.
- **ARQ-56** `except: pass` fuera de lo ya auditado (ARQ-26): SFTP enviar (5) y traer (4), `primas-previsional`, `pcr-emisiones`, `configuration-general`.
- **SEG-17** Job de notificación en `main` registra correos de destinatarios y enlaces; `short-link-redirect` registra el evento HTTP completo y `token -> URL`.
- **COS-10** Sin retención declarada en los grupos de logs. **Confirmado en DEV (us-east-1, 2026-11-19):** grupos de Glue y de todas las lambdas NIIF sin retención.

Ya conocidas que cierra el roadmap: ARQ-12, ARQ-26, ARQ-31, ARQ-32, ARQ-33, SEG-07, SEG-12, SEG-15. Plan: [[roadmap-observabilidad]].
