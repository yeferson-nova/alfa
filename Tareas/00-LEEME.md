# Tareas asignadas: contexto del proyecto

Esta carpeta reúne el trabajo asignado sobre la plataforma AWS de NIIF 17. Aquí se explica qué problema resuelve cada asignación, en qué estado se encuentra, qué exigen los estándares del banco y qué falta para avanzar. La documentación de referencia (estándares, HUs, arquitectura, brechas) está en `Docs\`.

| Archivo o carpeta | Contenido |
|---|---|
| `TareasAsignadas.txt` | Lista original de tareas. No se edita. |
| `T1-notificaciones\` | Cambio de SNS a SES, correo HTML, alertas de fallo y escenarios de notificación. |
| `T2-lambdas-gestor-configuracion\` | Unificar el modelo de programación de las lambdas. |
| `T3-validaciones-reserva-matematica\` | Validaciones de consistencia de los datos procesados. |
| `T1-notificaciones\propuesta-observabilidad-eventos.md` | Propuesta pendiente de validar: plano técnico (alertas a ServiceNow) y plano de negocio (eventos a SES y Power BI). |
| `riesgos-flujo-glue-notificacion.md` | Análisis transversal (T1, T2 y T3): riesgos del paso de datos de Glue a la Lambda de Notificación y patrón propuesto de manifiesto + Step Functions. |

Cada carpeta tiene un `README.md` con el problema, el estado actual, los requisitos de los estándares, la propuesta, lo que se puede adelantar hoy y lo que bloquea. Los diseños y entregables de cada tarea se guardan en su propia carpeta.

## El proyecto en una página

**Entidad:** Seguros Alfa S.A. y Seguros de Vida Alfa S.A., vigiladas por la Superintendencia Financiera de Colombia (SFC).

**Solución:** plataforma de datos para la norma contable NIIF 17 (contratos de seguro). Toma datos de los sistemas fuente (Acsel-e, Sipren, Arpis y otros), los consolida en AWS, los entrega a herramientas actuariales (FIS, Prophet) y termina en el subledger contable (DataStage) y en SAP.

**Nuestro alcance:** solo los dos pasos que corren en AWS (diagrama en `Docs\05-arquitectura\`).

| Paso | Qué hace | Componentes |
|---|---|---|
| 2. Bodega NIIF 17 | Recibe los datos fuente y los deja listos para cálculo. | Extracción, Preparación, Consolidación |
| 3. Certificación de cifras | Un responsable de negocio revisa los totales de cada caso de uso y certifica antes de que los datos sigan hacia las herramientas actuariales. | Lambda de Notificación, correo, Lambda de Transporte, S3 de salida |

**Cómo funciona hoy la certificación:** por cada caso de uso y periodo, se envía un correo con:
- la tabla de totales (registros y valor);
- un enlace para descargar el CSV;
- dos botones, **Certificar** y **No certificar**.

Hay cinco casos de uso con plantilla propia: reservas matemáticas, primas ARL, previsional, rentas vitalicias y auxilio funerario (`Backend\reservas_matematicas_cert\`). El ejemplo del correo está en `Docs\05-arquitectura\ejemplo-correo-certificacion.png`.

## Marco que deben cumplir las soluciones
- **Lineamientos internos de Alfa:**
  - estándar ETL;
  - versionamiento en GitHub;
  - código seguro con SonarQube: sin errores nuevos y cobertura mayor a 80 % en código nuevo;
  - CI/CD con Jenkins;
  - infraestructura como código con Terraform.
- **Estándares transversales del lago de datos (HUs técnicas):**
  - gobierno por metadata en DynamoDB;
  - trazabilidad con compuerta de calidad y cuadre entre capas;
  - seguridad de acceso con IAM Identity Center, KMS y topes de costo en Athena.
- **Normas externas que citan esos documentos:**
  - Ley 1581 de 2012 y Decreto 1377 de 2013 (datos personales);
  - Circular Básica Contable y Financiera y Circular Básica Jurídica de la SFC (trazabilidad de cifras y seguridad de la información);
  - retención de evidencia de 7 años.

Cada solución se revisa en tres frentes: **seguridad**, **costos** (en especial Athena) y **arquitectura**.

## Resumen de las asignaciones

| # | Asignación | Problema de fondo | Depende de |
|---|---|---|---|
| T1 | Notificaciones | El correo de certificación y los avisos de fallo no cumplen el formato esperado ni los controles de seguridad, y soporte no se entera cuando un proceso falla. | T2 (logs y errores comunes) |
| T2 | Lambdas del gestor de configuración | Conviven varios modelos de programación. Eso dificulta mantener, probar y monitorear, y no se alinea con el estándar de componentes dirigidos por metadata. | Nada; es la base |
| T3 | Validaciones de reserva matemática | Hoy se certifican y envían cifras sin comprobar que sean consistentes. Un error llega al cálculo actuarial y al cierre contable. | T2 y la definición funcional (en curso) |

**Orden recomendado:** T2 → T1 (SES y HTML) → T1 (alertas y escenarios) → T3.

## Hallazgos que afectan a todas las tareas
1. **SES y Grafana no aparecen en ningún estándar entregado.** Los estándares usan SNS para alarmas y CloudWatch con CloudTrail para monitoreo. Hay que confirmar con Arquitectura antes de construir.
2. **Hay patrones ya aprobados en la compañía que conviene reutilizar:**
   - aprobación por correo con cifras de control (Proyecto Nuevos Cargues);
   - API de aprobaciones con API Gateway, Lambda e IAM Identity Center (HUs de remediación).
3. **Faltan documentos que los estándares citan:** lineamiento de CI/CD V1, lineamiento de IaC con Terraform GTC-GDD-004 y Modelo de Seguridad de Datos.

## Restricciones actuales
- **Sin acceso a AWS ni al repositorio Git.** Todo lo de esta carpeta se basa en el diagrama, las plantillas HTML, el ejemplo del correo y los estándares. Lo que no se ha podido verificar se marca como **por confirmar**.
- **Se puede adelantar:** diseños, especificaciones, catálogos de reglas y notificaciones, plantillas, y el análisis de brechas.
- **Requiere acceso:** confirmar el estado real del código y de los recursos, implementar y desplegar.
