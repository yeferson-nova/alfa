---
tipo: contexto
actualizado: 2026-11-03
---
# Arquitectura de solución NIIF 17 (TOBE)

Fuente: `Docs\05-arquitectura\arquitectura-tobe-niif17.png`. Según el usuario, es la arquitectura actual; nos competen los pasos 2 y 3.

**Alcance actual: solo pasos 2 y 3 (AWS), en especial el 3, donde van los cambios.** Ver [[tareas-aws]].

| # | Componente | Detalle |
|---|---|---|
| 1 | Fuentes | Acsel-E, Sipren, Arpis, Point, Sipren+, Conciso, Uniclass, fuente no estructurada (primas previsional) |
| 2 | **Bodega NIIF 17 (AWS)** | Extracción -> Preparación -> Consolidación |
| 3 | **Certificación Cifras (AWS)** | Lambda Notificación -> Email -> Lambda Transporte -> S3 Salida |
| 4 | Herramientas Actuariales, extracción | FIS Data Conversion System |
| 5 | Herramientas Actuariales, motores de cálculo | FIS, Prophet |
| 6 | Herramientas Actuariales Subledger | MS, Accounting Tool |
| 7 | Herramientas Actuariales Reportería | MS |
| 8 | Subledger contable | IBM DataStage (bloques 1 a 4) |
| 9 | ERP | SAP |
| 12-14 | En análisis | Alfa Planning, Componente Actuarial, Sevinpro |

Flujo principal: 1 -> 2 -> 3 -> 4 -> 5 -> 6 -> 7. Los resultados de cálculo también vuelven al Subledger (8), que envía asientos contables al ERP (9).

Relacionado: [[diseno-propuesto]], [[correo-notificacion]].
