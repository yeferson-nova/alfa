---
tipo: pendiente
actualizado: 2026-11-03
---
# Preguntas abiertas

## Surgidas de los estándares (fase 2)
- **SES y Grafana no aparecen en ningún estándar entregado.** Los estándares usan SNS + CloudWatch + CloudTrail. ¿SES y Grafana son decisiones ya aprobadas por Arquitectura o hay que justificarlas? ([[hu-gobierno-ingestas]], [[hu-remediacion]])
- ¿SNS → SES aplica solo al correo de negocio (certificación) y las alarmas técnicas siguen en SNS? (propuesta: sí)
- ¿Podemos obtener los lineamientos citados pero no entregados: CI/CD V1, IaC Terraform GTC-GDD-004, Modelo de Seguridad de Datos y los documentos técnicos 01, 02 y 03? ([[marco-normativo]])
- ¿Los temas transversales 01 a 04 (HUs) ya aplican a NIIF 17 o solo a Desmonte DWH, FUD y Radar 360? Dicen "toda iniciativa nueva", pero ninguno menciona NIIF 17.
- ¿El paso 2 (Preparación) usa Glue DataBrew como Nuevos Cargues? ([[est-databrew]])
- ¿El flujo de aprobación de Nuevos Cargues (frontend `dataprep-stg-approval-frontend`) se puede reutilizar para Certificar / No certificar? ([[est-nuevos-cargues]])
- ¿Trabajamos como proveedor externo? Si es así, aplica el reporte SonarQube inicial y final ([[est-codigo-seguro]] 5.2).
- ¿Cuál es el ticket Jira del proyecto? Es obligatorio en ramas y commits ([[est-versionamiento-codigo]]).
- ¿El CSV de certificación contiene datos personales? Define cifrado, expiración y si viola "notificaciones sin datos personales" ([[hu-remediacion]]).

## Paso 3
- El inventario sugiere respuestas a tres preguntas: endpoint de certificación, canal del correo y qué pasa sin certificación. Ver [[inventario-lambdas]]. Falta confirmarlas con el equipo.
- ¿Cuál generación del gestor es la oficial: G3 (`configurador-*`) o G4 (`configuration-*`, Terraform)? ¿G1 y G2 se pueden retirar?
- Definición funcional de las validaciones de reserva matemática. ([[tareas-aws]], tarea 3)

## Monitoreo ([[monitoreo-grafana]])
- ¿Dónde corre Grafana y quién lo administra? (no definido)
- ¿Existe un Elastic u OpenSearch corporativo?
- ¿Retención mínima exigida para los logs?
- ¿El alcance incluye otros servicios AWS además de los pasos 2 y 3?
- Soporte usa **ServiceNow** (resuelto). Falta saber: ¿tienen ITOM Event Management o solo ITSM? ¿Ya está instalado el AWS Service Management Connector? ¿Cuál es el grupo de asignación? ¿Se permite salida a internet desde las lambdas (NAT o proxy)?

Al resolver una, moverla a la nota correspondiente y borrarla de aquí.
