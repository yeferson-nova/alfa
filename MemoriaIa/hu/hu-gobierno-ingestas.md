---
tipo: hu
actualizado: 2026-11-03
---
# HUs Tema 01: Gobierno de Ingestas (HU-GI-01 a 12)

Fuente: `Docs\03-historias-usuario\01_HUs_Tecnicas_Gobierno_Ingestas.xlsx` (v3.0, ago-2026). **Estándar corporativo obligatorio** para toda iniciativa que ingiera, transforme o exponga datos en el Data Lake.

## Principios
- **La ingesta se declara como metadata, no se programa.** Modelo de gobierno en **DynamoDB** como fuente única de verdad. Los jobs no tienen tablas, rutas, predicados ni credenciales en el código.
- **"El motor no decide, obedece":** si falta metadata, falla con error clasificado; no hay valores por defecto implícitos.
- **Identificador de ejecución único** generado una vez y propagado por todas las fases, logs, métricas, auditoría y linaje.
- **Tres fuentes de verdad:** DynamoDB (estado y evidencia), CloudWatch (métricas y logs), CloudTrail (acceso).
- **Cobertura total:** al cierre no queda ninguna ingesta fuera del estándar (HU-GI-12 regulariza lo existente).

## Capas
Landing (copia fiel) → Bronce (Delta Lake + Snappy en S3, Glue Data Catalog) → Plata → Oro → Platino (Redshift o Athena). Cuarentena junto a la capa del rechazo.
Evolución de esquema: columna nueva o reordenada continúa; columna eliminada, tipo incompatible o cambio de llave **detiene**.

## Catálogos clave (hojas del libro)
- **Taxonomía de errores:** dominios como configuración, conectividad, origen, calidad, escritura, permisos, recursos, orquestación, plataforma y trazabilidad. Cada uno con severidad, si es recuperable y la acción y destinatario (ej. permisos: detener y notificar a Seguridad, sin reintento).
- **Catálogo cerrado de eventos:** `CORRIDA_INICIADA`, `LECTURA_*`, `ESCRITURA_*`, `REGLA_EVALUADA`, etc.
- **Métricas y alarmas:** duración frente a SLA, **entrada frente a salida ≠ 0**, rechazados sobre umbral, cambios de configuración fuera del pipeline, entre otras.
- **Decisiones abiertas:** D-01 a D-17. Relevantes: D-05 retención por tabla, D-13 nivel de log en producción, D-15 destinatarios y plazos por severidad.

## Relevancia para nuestras tareas ([[tareas-aws]])
- **Tarea 2 (modelo único de lambdas):** el estándar define el modelo. Lambdas dirigidas por metadata del gestor de configuración (DynamoDB), sin valores quemados, con el `ejecucion_id` propagado y errores según la taxonomía.
- **Tarea 1c ([[monitoreo-grafana]]):** los catálogos de eventos, métricas y alarmas y la taxonomía de errores ya definen qué emitir. El estándar habla de CloudWatch y CloudTrail, **no de Grafana**.
- **Tarea 3:** la métrica "entrada frente a salida" y `REGLA_EVALUADA` encajan con el validador.
