---
tipo: pendiente
actualizado: 2026-11-09
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

## Estructura de repos y despliegue (para DevOps) - sin enviar ni responder (2026-11-05)
Contexto: nosotros somos Desarrollo. DevOps tiene Jenkins y Terraform; ellos crean el repo, la IaC y el despliegue. Nos entregan un repo por lambda o job. Ver [[est-versionamiento-codigo]] y [[hu-devops]].
- **Las 3 que deciden (prioridad):**
  1. ¿Jenkins asume un repo = un artefacto = una versión, o soporta varios jobs en un repo con construcción por carpeta y tags por componente (`extraccion-v1.2.0`)?
  2. ¿Qué recibe Terraform de nosotros: el artefacto, un `job.<amb>.json` con parámetros, o ambos?
  3. ¿Cómo se hace el rollback: se lo pedimos a DevOps o lanzamos un job de Jenkins con el tag anterior?
- **Repos:** ¿podemos pedir uno por job y por lambda, o hay límite o aprobación? ¿Quién los crea y con qué nombre?
- **Plantilla:** ¿hay `pipeline.yaml` y `Jenkinsfile` de ejemplo (una lambda y un job de Glue)? ¿Qué archivos y nombres exige el pipeline (`config/`, `pyproject.toml`, `sonar-project.properties`)? ¿Versión de Python y herramienta de cobertura?
- **Artefactos y promoción:** ¿dónde publica Jenkins el artefacto de Glue (Nexus, S3 o ambos) y con qué ruta? ¿Cómo se promueve el mismo artefacto de DEV a QA y PRD y quién aprueba?
- **Recursos existentes:** los Glue y lambdas creados a mano, ¿se importan a Terraform o se recrean? Afecta nombres.
- **Versiones:** ¿cuántas conserva Nexus? (lo visto: lo de PRD más las últimas 10).
- **Documento para enviar:** `Docs\07-entregables\preguntas-devops-repos-y-despliegue.md` (18 preguntas). **Los ambientes son cuentas de AWS distintas** (confirmado por el usuario 2026-11-05; solo conocemos la de DEV, 533267035849). El documento ya corrige 4 preguntas por esto y la 17: los `config_*.json` los **genera la lambda configuradora** desde DynamoDB (`cnf-mallas`, `prs-estructuras`), no se editan a mano.
- **Configuración y seguridad:** ¿dónde viven los parámetros por ambiente (repo, Terraform o Parameter Store)? ¿Cómo se manejan los secretos y qué rol asume Jenkins en cada cuenta y cómo se promueve el artefacto entre cuentas? Los `config_*.json` de `niif-dev-assets` hoy se editan a mano en S3: ¿los despliega el pipeline desde un repo?
- **Documentos:** CI/CD V1 e IaC GTC-GDD-004 (ya listados arriba).

Al resolver una, moverla a la nota correspondiente y borrarla de aquí.

## Incidente en prod Primas ARL 202606 ([[inprod-primas-arl-202606]]) - 2026-11-05
- ¿El job `niif-prd-extraccion-estandar-optimized` desplegado es la misma versión que el código de DEV del repo? ¿QA ya conserva `lotes_segmento` con los lotes de respaldo? (hipótesis INC-5)
- ¿De qué lote(s) vienen los 1.203.789 registros excluidos? (el campo `lotes_respaldo_exclusion` está vacío en prod)
- ¿Gold de prod tiene versionado de S3 o respaldo anterior al 2 nov? ¿Quién autoriza restaurarlo?
- ¿Por qué entraron registros de 202610 con `WHERE PERIODO <= '202606'`? ¿`PERIODO` y `FEC_PROCESO_POINT` difieren?
- ¿Por qué el 91 % de Gold (1.100.009) tiene `per_proceso_point` nulo? ¿Es esperado?
- ¿El SCD debe apagar por ausencia en cargas incrementales, o la extracción debe entregar siempre la foto completa? (decisión de diseño)
- ~~¿Qué se hace con la evidencia de prod subida al repo público?~~ El repo pasó a privado el 2026-11-09. Abierto: si lo que estuvo expuesto antes (correos, ids de Cognito y de cuenta) requiere más acciones (clones o forks, rotar identificadores) y si se reescribe el historial.
- ¿Por qué la notificación sale aunque el resumen y los totales estén vacíos? ¿Se acepta bloquear el envío en ese caso?
- **Parcialmente resuelta (2026-11-10):** Víctor (transcripción L276-277) dijo que No certificar debe dejar la tarea **pendiente** en el modelo de configuración (tabla `niif-dev-prg-programaciones`) y que negocio avisa **a mano** al operador; no hace falta alerta. Hoy solo archiva y marca `NO_TRANSPORT`. Abierto: ¿escribir `PENDIENTE` en `scheduleEstado`/`scheduleExpression`/`periodoNegocio` (el manager lo rechaza con error en el log) o un estado nuevo como `RECHAZADO`? ¿Quién limpia `PROGRAMADO` cuando el `at()` termina? Ver [[notificaciones-vs-arquitectura]].
- ¿`--ENVIRONMENT: '{env}'` literal en `composite_notification_evaluator.py` es un error? ¿El job de notificación usa ese argumento?

## Para el daily del 2026-11-11 (enlace de descarga del CSV, ARQ-29)
- **Hallazgo en QA (2026-11-10):** el enlace "Archivos de detalle" del correo abre la consola de S3 vía portal SSO; el usuario debe pulsar Download y necesita rol en la cuenta. Detalle en [[notificaciones-vs-arquitectura]] (ARQ-29, SEG-13).
- **Pregunta de negocio (bloqueante para decidir):** ¿los aprobadores reales de Primas ARL (y de las demás mallas) tienen acceso a la consola de AWS? Si no, el enlace no les sirve hoy y sube la prioridad de la opción A (descarga por API tras login Cognito + URL prefirmada de 1 a 5 min).
- **Decisión para el equipo/Víctor:** (A) descarga mediada por API Gateway, o (B) dejar la consola solo para usuarios con acceso y documentarlo como limitación. Víctor (L270-271) y el diagrama esperaban descarga por API.
- **Si es A, necesita DevOps:** ruta nueva en API Gateway y lambda; el job de certificación guardaría bucket/key en lugar de la URL de consola.
- **Otros para el daily:** restaurar el JSON real `primas-arl/primasArl.json` en QA (destinatarios de prueba); DevOps: `--SES_SENDER` por defecto, `templatePath` singular en todos los JSON de caso, reenvío de `politica-iam-ses.json` y `SHA256.txt` actualizados; pendientes de pruebas QA 2, 4, 5, 6 y 7.

## Limpieza de DEV (2026-11-10, ver `Docs\06-brechas\limpieza-dev-recursos.md`)
- ¿Quién mantiene `niif-dev-configuration-general` (configurador general en modo validación, ya cubre Satélites/PCR)? ¿Reemplaza a las 6 `configuration-<malla>` y a nuestro T2?
- ¿De quién es `niif-dev-configurador-mallas` (Terraform, 0 invocaciones)? Nuestro T2 se desplegaría como `datalake-<amb>-niif-configurador-mallas-lambda`.
- ARQ-30: ¿quién borró los 2 archivos de ARL de `niif-dev-transport/export/` (2026-10-09)? ¿Hay ciclo de vida en el bucket? ¿Cuándo se configuró la notificación de S3 hacia `send-via-sftp`?
- ¿Se puede quitar el callback del API viejo (`53x06brwqh`) del cliente de Cognito `datalake-dev-niif-api`? Lo hace DevOps/Terraform (SEG-14).
