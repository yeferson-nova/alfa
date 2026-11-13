---
tipo: contexto
estado: vigente
actualizado: 2026-11-12
---
# Arquitectura de solución NIIF 17 (aprobada)

> Resumen de `Docs\05-arquitectura\diagramas\NIIF17_ModeloSolucion_V1.0.drawio`, la **arquitectura aprobada**. Validado el 2026-11-12 leyendo el XML de las 8 hojas (etiquetas, íconos y flechas). Lo que difiera en el código o en otras notas es una brecha, no un error del diagrama. Nos competen los pasos 2 y 3; el 3 es donde van los cambios ([[tareas-aws]]).

## Pasos (hoja `Página-10`)
| # | Componente del diagrama | Contenido |
|---|---|---|
| 1 | Fuentes | Acsel-E, Sipren, Point, Arpis, Sipren+, Conciso, Uniclass y la fuente **semiestructurada** de Previsional |
| 2 | **Bodega NIIF17** (AWS) | Extracción y Cargue → Calidad → Preparación y Consolidación (por dentro: Extracción → Transformación → Cargue) |
| 3 | **Certificación Cifras** (AWS) | Notificación y Transporte → Email y Salida (detalle en la hoja `Página-2`) |
| 4 | Herramientas Actuariales, Componentes de Extracción | Client ETL Tool y Data Conversion System |
| 5 | Herramientas Actuariales, Motores de Cálculo | Prophet (FIS aparece como destino del SFTP en `Página-2`) |
| 6 | Herramienta Contable | AccountingTool |
| 7 | Herramienta Reportería | ReportingService |
| 9 | Subledger Contable | Recibe los asientos contables |
| 10, 11, 12 | Otros Componentes | Alfa Planning (10), Componente Actuarial (11) y Sevinpro (12) |

El **ERP** está dibujado con el número 7, repetido con la Reportería, y no hay paso 8. Por confirmar con el arquitecto si el ERP debe ser el 8. El diagrama no nombra DataStage ni SAP: "DataStage" viene de los estándares del banco ([[est-datastage]]) y "SAP" de las notas del proyecto.

## Flujo (flechas de `Página-10`)
- Principal: 1 Fuentes → 2 Bodega → 3 Certificación Cifras → 4 → 5 → 6 → 7.
- Contable: 6 Herramienta Contable → 9 Subledger Contable → ERP.
- Las Fuentes también alimentan **directo** al Subledger Contable, sin pasar por la Bodega.

## Paso 3 en detalle (hoja `Página-2`)
- **Lambda Notificación** es el centro: la dispara el modelo de notificaciones (DynamoDB) y el API Gateway; lanza el **Job de Notificación** (Glue); envía el **Mensaje por SES** al usuario de Outlook; escribe en la Zona Certificación y en la Zona Transporte Salida (S3) y actualiza el modelo de configuración de extracción.
- El Job de Notificación lee de **Athena** sobre el catálogo de la Zona Oro y escribe en la Zona Certificación.
- Usuario Outlook → **WAF** → **API Gateway** → lambda `postLogin()` y **Cognito** (identidad interna, federada con el Active Directory externo); el API también llega a la Zona Certificación y a la Lambda Notificación.
- Transporte: Zona Transporte Salida → **Lambda SFTP** → Transit Gateway → firewalls → FIS Single Platform (SFTP In y Out).
- El modelo de notificaciones también alimenta la Malla de Ejecución (Step Functions); el modelo de configuración alimenta al Programador (EventBridge Scheduler).

## Medallón (hojas `Página-1` y `Página-6`)
- Zonas S3: Aterrizaje, Bronce, Plata, Oro y Recursos, cada una con su catálogo Glue; rastreadores en Bronce, Plata y Oro; extracción JDBC desde el core (Acsel-E, Sipren, Arpis).
- Jobs de Glue dibujados: Extracción (Bronce); Consolidación (Bronce); Validación, Remediación y Consolidación (Plata); Transformación; Consolidación (Oro); y Notificación (hoja 2). Ver [[familia-mallas-alfaprd]].
- Modelo de programación (`Página-3`): DynamoDB → DynamoDB Stream → Lambda de automatización → EventBridge Pipe → EventBridge.
- `Página-1` dibuja CloudWatch y un `ModeloTrazabilidad` (DynamoDB), **sin conectarlos** a ningún componente.

## Inconsistencias dentro del diagrama (para el arquitecto)
1. Numeración: el ERP repite el 7 y falta el 8.
2. El job de Transformación está en Plata en la hoja 1 y en Oro en la hoja 6.
3. CloudWatch y `ModeloTrazabilidad` están sin flechas; la hoja 2 no dibuja observabilidad ([[notificaciones-vs-arquitectura]], ARQ-31).
4. Las hojas 1, 2 y 9 traen direcciones IP internas: no copiarlas a notas ni al repo.

Mapa de hojas y flujo AS-IS: skill `niif17-alfa`, `references\arquitectura-y-flujo.md`. Nuestra segunda arquitectura, con las lambdas que faltan en la hoja `Página-2`, está en `NIIF17_ModeloSolucion_V1.0-propuesta.drawio` (el original no se toca): ver [[arquitectura-propuesta-niif17]]. Al tocar esas piezas hay que tener presentes las dos.

Relacionado: [[arquitectura-propuesta-niif17]], [[diseno-propuesto]], [[correo-notificacion]].
