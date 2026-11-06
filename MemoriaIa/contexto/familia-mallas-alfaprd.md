---
tipo: contexto
actualizado: 2026-11-06
---
# Familia de mallas ALFAPRD (incluye Primas ARL) y su plan

Humano: `Docs\05-arquitectura\jobs-glue.md` (los 12 jobs). Relacionado: [[arquitectura-niif17]], [[inprod-primas-arl-202606]], [[auditoria-pipeline-2026-11]], [[inventario-lambdas]].

## Qué es la familia
- **6 mallas estándar** con origen Oracle **ALFAPRD** (esquemas arpprod/general/prv): Primas Rentas, Primas Endosos, **Primas ARL**, Auxilios Funerarios, Reserva Matemática ARP, Reserva Matemática RVI.
- Comparten los **mismos jobs genéricos** (extracción, validación técnica, remediación, SCD, certificación, notificación); solo cambian el JSON de configuración y el job Silver (`niif-dev-silver-<malla>` ×6). Por eso un defecto en un job genérico afecta a las 6 ([[inprod-primas-arl-202606]], sección 4).
- Step Function por malla: `niif-<amb>-<malla>`; configuración en `s3://niif-<amb>-assets/<malla>/` (ej. `primas-arl/`).
- **Primas ARL:** ARL = Administradora de Riesgos Laborales; reporte de primas de ese ramo, certificado y transportado a FIS. Es la malla del incidente INC-5. El `job.json` del extractor optimizado apunta por defecto a `config_extraccion_primas_arl.json`.
- **No pertenecen a la familia:** los flujos PCR (PCR Tradicional, PCR Vida, Vida Deudores) vienen de **ACSELE**, otra familia, hoy con jobs de prueba en `Backend\Glue\pruebas-glue\`. Significado de PCR = "Pasivo por Cobertura Restante" es **inferido**, sin documento funcional (propuesta; ver [[preguntas-abiertas]]).

## Jobs: arquitectura vs desplegado (2026-11-06)
- Diagrama TOBE: **9 jobs** (Extracción y Consolidación en Bronce; Validación, Remediación, Transformación y Consolidación en Plata; Transformación y Consolidación en Oro; Notificación). Conteo por etiquetas del drawio, sin revisar flechas.
- Desplegado: **12 jobs** (5 genéricos + 6 Silver + 1 correo). Consolidación de Bronce no existe como job (va dentro de la extracción); la de Plata y las de Oro tampoco están separadas; `certification` no aparece como "Job" en el diagrama.

## Plan acordado (2026-11-06, propuesta, nada ejecutado)
1. **Subir el fix pendiente** de la extracción (INC-5, los 3 bloques de exclusión; ver [[inprod-primas-arl-202606]] sección 3c). El repo tiene un cambio local sin commit en `extraccion_estandar_optimized.py` (+18/−13: `combinar_lotes_respaldados` movida debajo de `resolver_llaves_reales` y mensajes). Entregar el archivo completo y congelado (commit o hash).
2. **Auditoría de la familia de jobs** (los 12): partir de [[auditoria-pipeline-2026-11]] y `Docs\06-brechas\rendimiento-y-costos.md` (RYC-01..17); revisar si las otras 5 mallas comparten el riesgo de INC-5/INC-2 y contrastar contra los 9 jobs del diagrama. Brechas con ID `SEG`/`COS`/`ARQ`/`CUM`.
