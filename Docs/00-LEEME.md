# Documentación del proyecto NIIF 17 (AWS)

Esta carpeta reúne la documentación para personas: tareas asignadas, estándares entregados por el banco, historias de usuario, arquitectura y análisis de brechas. El alcance actual son los pasos 2 (Bodega NIIF 17) y 3 (Certificación de cifras) de la arquitectura, ambos en AWS.

Las tareas asignadas, con el contexto de cada problema, están en una carpeta aparte: `C:\Users\rdpuser\Git\Tareas\` (empezar por `00-LEEME.md`).

## Cómo está organizada

| Carpeta | Contenido | ¿Se edita? |
|---|---|---|
| `02-estandares-banco\` | Estándares y lineamientos entregados por el banco, con su nombre original: `desarrollo\` (DataStage v6, ETL v1), `versionamiento-seguro\` (ALFALV2, V3) y `aws\` (Glue DataBrew). | No; solo lectura. |
| `03-historias-usuario\` | Seis libros Excel de HUs técnicas: gobierno de ingestas, linaje, trazabilidad y auditoría, DevOps/DataOps, remediación de datos, y seguridad y acceso a datos. | No; solo lectura. |
| `04-manuales\` | Manual de usuario del Proyecto Nuevos Cargues V5. | No; solo lectura. |
| `05-arquitectura\` | Diagrama de la arquitectura actual (`arquitectura-tobe-niif17.png`), diagrama editable de los pasos 2 y 3 (`aws_pasos_2_y_3.drawio`) y ejemplo del correo de certificación (`ejemplo-correo-certificacion.png`). Cuatro documentos para entender el proceso: `flujo-integral-niif17.md` (cómo funciona hoy, de punta a punta), `jobs-glue.md` (cada job y crawler por dentro) y `tareas-sobre-el-flujo.md` (cómo se conectan T1, T2 y T3 con el flujo). El inventario de las 7 lambdas de trabajo y de las 6 Step Functions `niif-dev-<malla>` está junto al código, en `Backend\Lamnbdas\inventario-aws\`. | El `.drawio`. |
| `06-brechas\` | Análisis propios de brechas frente a los estándares: seguridad (`seguridad.md`) monitoreo (`monitoreo-continuo-grafana.md`), problemas actuales de rendimiento y costos (`rendimiento-y-costos.md`) y la lista priorizada de lo que hay que corregir antes de producción (`revision-pre-produccion.md`). Aquí se agregarán costos (Athena), arquitectura, cumplimiento y la matriz consolidada. | Sí. |
| `07-entregables\` | Documentos listos para entregar al banco. | Sí, con versión en el nombre. |
| `08-estandares-desarrollo\` | Estándar propio del equipo para el código Python (`estandar-lambdas-python.md`): arquitectura hexagonal, nombres, código limpio, errores, logs, idempotencia y pruebas. Implementación de referencia: `Backend\Lamnbdas\inventario-aws\t2 unificadas\sc-niif-configurador-mallas\`. | Sí, con versión. |

## Convenciones
- Los archivos entregados por el banco conservan su nombre original para mantener la trazabilidad.
- Los documentos que creamos usan nombres en minúscula separados por guiones.
- Cada brecha lleva un identificador único: `SEG-xx` (seguridad), `COS-xx` (costos), `ARQ-xx` (arquitectura) y `CUM-xx` (cumplimiento).

## Relación con la memoria de IA
`C:\Users\rdpuser\Git\MemoriaIa\` contiene resúmenes cortos y enlazados de estos mismos documentos para el asistente de IA. Los resúmenes apuntan a los archivos de esta carpeta y no los reemplazan.
