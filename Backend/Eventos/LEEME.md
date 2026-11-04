# Reglas de EventBridge (bus `default`)

Movidas el 2026-11-03 desde `Backend\inventario\eventos\` (cuenta 533267035849, us-east-1, DEV). Se verificó que la copia fuera idéntica antes de quitarlas del inventario. Cada regla son dos archivos: `.regla.json` (patrón o cron) y `.targets.json` (destino).

## Reglas que tocan nuestras lambdas, Step Functions y jobs (4)

| Regla | Qué captura | Destino | Para qué nos sirve |
|---|---|---|---|
| `niif-dev-finalizar-ejecucion-primas-arl` | Fin de la Step Function `niif-dev-primas-arl` (éxito, fallo, timeout, abortada) | `niif-dev-finalizador-ejecucion` | **Riesgo (SF-04):** esa lambda es de la generación anterior y espera campos del iniciador; con ejecuciones del configurador falla con `KeyError`. La Step Function ya registra el estado: queda duplicado. |
| `ALFA-DEV-DATALAKE-SfnEvents` | Fin de **cualquier** Step Function | `PipelineObserver` | Ya cubre nuestras 6 Step Functions. Base de la T1. |
| `ALFA-DEV-DATALAKE-GlueJobEvents` | Estado de **cualquier** job de Glue y alertas de retraso | `PipelineObserver` | Cubre nuestros 11 jobs (`Backend\Glue\`). |
| `ALFA-DEV-DATALAKE-GlueCrawlerEvents` | Fin de **cualquier** crawler | `PipelineObserver` | Cubre los crawlers Bronze, Silver y Gold de la Step Function. |

Las tres `ALFA-DEV-DATALAKE-*` son genéricas de la cuenta, no solo nuestras. `PipelineObserver` está en `Backend\Lamnbdas\pruebas-lambda\`.

## Lo que no existe aquí
- Ninguna regla dispara las Step Functions: las inicia el configurador (`ejecutar:true`) y los schedules viven en EventBridge Scheduler.
- La cadena de certificación va por streams de DynamoDB, no por estas reglas.

## `pruebas-eventos\` (18 reglas, 36 archivos)
- **Cargues de otros proyectos:** `clientes-*`, `datalake-dev-bronce-sf-scheduler`, `datalake-dev-smb-sf-scheduler` (días 10 a 12 de cada mes). Pueden competir por Glue y Athena con nuestro pipeline esos días.
- **Plataforma:** Control Tower, GuardDuty, Security Hub, CloudWatch Application Insights, notificaciones de usuario.
- `niif-dev-ses-delivery-test`: eventos de entrega de SES (rebotes, quejas, rechazos) a un log group. Parece prueba temporal, pero es lo único que muestra si un correo de certificación rebotó; **validar si sigue en uso** (T1).
