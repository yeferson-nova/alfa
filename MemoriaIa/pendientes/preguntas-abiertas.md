---
tipo: pendiente
estado: vigente
actualizado: 2026-11-23
---
# Preguntas abiertas

## Surgidas de los estándares (fase 2)
- ~~¿SES y Grafana están aprobados por Arquitectura?~~ **Resuelta (2026-11-10):** SES está en el diagrama de arquitectura (icono "Mensaje", paso 53). **Grafana sale de nuestro alcance:** dejamos el ciclo de observabilidad completo en logs y métricas, listo para conectar un Grafana si se requiere. Lo que falte en el diagrama se le sugiere al arquitecto (ver [[notificaciones-vs-arquitectura]], ARQ-31).
- ~~¿Las alarmas técnicas siguen en SNS?~~ **Resuelta (2026-11-10): no.** Todo correo, de negocio y de alerta técnica, sale por el mismo SES. **Abierto:** cuál es el grupo de soporte que recibe las alertas técnicas.
- **Parcialmente resuelta (2026-11-10):** llegó `Docs\02-estandares-banco\GobiernoVersionameinto-DevOps.docx` ([[est-gobierno-devops-niif]]). Ese documento remite a `LINEAMIENTO PARA EL VERSIONAMIENTO DE CODIGO V2.pdf` y `LINEAMIENTO PARA IAC CON TERRAFORM V1.pdf`, que siguen sin entregarse. Siguen faltando también el Modelo de Seguridad de Datos y los documentos técnicos 01 a 03.
- ~~¿Las HU transversales 01 a 04 aplican a NIIF 17?~~ **Resuelta (2026-11-10): por ahora quedan fuera.** El foco es normalizar la arquitectura de notificaciones. Las fichas `hu-*` quedan como referencia.
- ¿El paso 2 (Preparación) usa Glue DataBrew como Nuevos Cargues? ([[est-databrew]])
- ~~¿Se reutiliza el frontend de aprobación de Nuevos Cargues?~~ **Resuelta (2026-11-10): no.** El componente 3 del diagrama define su propio camino: WAF → API Gateway → `postLogin` → Cognito ↔ Azure AD → Lambda Notificación / Zona Certificación.
- ¿Trabajamos como proveedor externo? Si es así, aplica el reporte SonarQube inicial y final ([[est-codigo-seguro]] 5.2).
- ¿Cuál es el ticket Jira del proyecto? Es obligatorio en ramas y commits ([[est-versionamiento-codigo]]).
- ~~¿El CSV de certificación tiene datos personales?~~ **Resuelta (2026-11-10, usuario): no.** Los de DEV y QA son datos de ejemplo. Matiz: el CSV de Primas trae `NID_NUMERO_IDENTIFICACION_TERCERO` y `NOM_TERCERO`; en la muestra son NIT de empresas, pero en prod podría haber personas naturales. Revisarlo antes de producción.

## PCR Emisiones 202606 ([[inprod-pcr-emisiones-202606]], 2026-11-23)
- ~~(Negocio, Javier González) ¿Contra cuál cifra se valida: el RRC sin contabilidad o el `E1-A1`?~~ Resuelta: la cifra oficial es el RRC (decisión del usuario).
- (Negocio, piloto Pymes, 2026-11-25) ¿Quién mantiene el catálogo de productos: el área, o se arma desde ACSELE con su aprobación? ¿Quién aprueba el mapa de conceptos? Ver [[productos-genericos-emisiones]].
- (Negocio) "Lo emitido en junio" ¿es por mes de producción (como el resumen) o por fecha de la operación? El RRC trae 726 filas de meses anteriores dentro del corte de junio.
- (Negocio) ¿Qué conceptos de ACSELE forman cada columna del Excel (prima emitida, comisión intermediario y su IVA, comisión recaudo y su IVA)? El mapeo actual es propuesta.
- (Negocio) ¿De qué campo sale el "Tomador" del RRC en cada producto? En 52897 y 55059 es el pagador maestro (entidad del acuerdo); en Hogar (54959) y Mascotas (54979) no coincide con ningún rol del Silver aunque la prima por póliza cuadra.
- (Negocio) En 52897 (Deudores), ¿el tomador del reporte es el del acuerdo (entidad) y no el de la póliza? En 14 acuerdos el RRC pone toda la prima en el tomador del acuerdo y el Silver la reparte entre 8.812 tomadores de póliza (misma suma).
- (DBA/DevOps) ¿La Oracle ACSSOP (DEV y QA) tiene enmascarados los nombres de personas naturales? Misma póliza y prima que el RRC con otro nombre en Hogar y Mascotas; las entidades sí cuadran (2026-11-24).
- (DevOps) ¿La Oracle ACSELE de QA es copia de PRD? ¿De qué fecha? ¿PRD corre la rama `main` o `quality` de los repos de Emisiones?

## Extracción (2026-11-20, para Julio en Oracle; no son para Víctor, ver [[modularizacion-extraccion]])
- Scripts listos para entregar, uno por base (solo diccionario): `Tareas/Modularizacion/Extraccion/alfaprd/validaciones/scripts/consultas-oracle-alfaprd.sql` (40 tablas de los 6 JSON; flashback) y `Tareas/Modularizacion/PCR/01-emisiones/validaciones/scripts/consultas-oracle-acsele.sql` (93 tablas de M1-M4; `em_*`; flashback). Resultados, en CSV, a `EvidenciasAws/dev-extraccion/`. **Primera corrida de Acsel-e (2026-11-25):** se corrió con un usuario personal, no con el de servicio de Glue, en la base COREPROD (memoria dice Emisiones → ACSSOP: confirmar si es la misma). Faltan los resultados de las consultas 1 (tipos e índices) y 2 (tamaños). Las 4 y 5 (em_*) y la 3a (flashback) dieron 0 filas para ese usuario; la 3b dio ORA-00942. El usuario de servicio no se usa: su clave la custodia otro equipo y no se saca de AWS. Las consultas 1 y 2 sirven con el usuario personal si la columna `nota` no dice "NO VISIBLE"; las de permisos (4, 5, 3a) se piden al DBA con la variante 3d y el nombre del usuario de servicio (solo el nombre, de la conexión de Glue), más `undo_retention`.
- ¿Qué tipo de dato tiene cada columna de join (`ALL_TAB_COLUMNS`) y cuáles tienen índice (`ALL_IND_COLUMNS`)? Define si la tabla va por rangos o por `IN`.
- ¿El usuario de servicio puede leer `ALL_TABLES.NUM_ROWS` y `LAST_ANALYZED`? ¿Qué tan recientes son las estadísticas?
- ¿Hay permiso de `FLASHBACK` y suficiente `UNDO_RETENTION` para leer todos los tramos en la misma foto (`AS OF SCN`)?
- ¿En qué instancia están `acsele_alfa_bi.em_*` y se pueden leer para la prueba de equivalencia?
- (DevOps, preguntar el lunes 2026-11-23) ¿De qué repo salen las 13 políticas del rol del proceso de extracción (9 en línea: `...-gestor-configuracion` y `pcr-vida-*`; 4 administradas: `satelites-assets`, `validacion-oracle-emisiones`, `extraccion-estandar-emisiones-s3`, `glue-read-acsele-connections`)? No están en la `quality` (2026-11-20) de ninguno de los 33 repos que tenemos; puede ser un repo de DevOps que no tenemos. Ojo: el rol lo comparten 4 componentes y acumula permisos (mínimo privilegio).
- (Equipo o DevOps, lunes 2026-11-23) ¿Quién reescribió `data-datalake-niif-pcr-emisiones-glue-job` en `quality` el 2026-11-20 (commit `e1a13ac`)? Sigue cambiando: hay que coordinar antes de EXT-06.
- (DevOps o arquitecto) ¿Las tablas vacías `datalake-{env}-adm-extraccion-costo`, `-parametro` y `-ejecucion` tienen un uso previsto o se pueden retirar?

## Paso 3
- El inventario sugiere respuestas a tres preguntas: endpoint de certificación, canal del correo y qué pasa sin certificación. Ver [[inventario-lambdas]]. Falta confirmarlas con el equipo.
- ¿Cuál generación del gestor es la oficial: G3 (`configurador-*`) o G4 (`configuration-*`, Terraform)? ¿G1 y G2 se pueden retirar?
- Definición funcional de las validaciones de reserva matemática. ([[tareas-aws]], tarea 3)

## Propuesta Página-2: comentarios de Víctor (2026-11-19, ver [[arquitectura-propuesta-niif17]])
- ¿Qué activa la lambda de movimiento en el "modelo de segregación de eventos"? La frase de Víctor quedó cortada ("se activa cuando...").
- ¿"Frena la notificación" significa cortar en postLogin (no llamar a Certificación) o detener todo el flujo de notificación del caso? Hoy el corte (403) está en `niif-dev-certification`.
- ¿El Modelo de Identidades se monta sobre `use-case-approvers` (existe en IaC y en DEV, sin uso en el código) o es una tabla nueva? Propuesta: reutilizarla y extenderla con los enlaces.

## Monitoreo ([[monitoreo-grafana]], decisiones de la fase 0 en [[roadmap-observabilidad]] §9)
- ~~¿Qué son los "tres niveles"?~~ **Resuelta (2026-11-19):** logs, métricas y trazas.
- ¿Instrumentación mínima en los Silver por malla o esperar a reescribirlos?
- ¿Quién administra `ALFA-DEV-DATALAKE-PipelineObserver` y los SNS `ALFA-DEV-DATALAKE-Critical`/`-Warning`? **Parcial (2026-11-19):** stack `stack-cf-observability` ("Plataforma de Observabilidad Data Platform"), creado el 2026-10-15 y actualizado el 2026-10-20 por un mismo usuario (correo no guardado aquí), sin etiquetas; cada SNS con una sola suscripción por correo. Falta confirmar si es de nuestro equipo o de plataforma. La fase 6 de [[roadmap-observabilidad]] los reutiliza.
- ~~¿Quién arma los tableros?~~ **Resuelta (2026-11-19):** fuera de nuestro alcance; entregamos los orígenes listos y documentados para cualquier herramienta.
- ¿Dónde corre Grafana y quién lo administra? (no definido)
- ¿Existe un Elastic u OpenSearch corporativo?
- ~~¿Retención mínima exigida para los logs?~~ **Resuelta (2026-11-19):** configurable por ambiente, PRD 180 días; los 7 años aplican al registro de ejecución (HU-GI-05).
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
- ¿Gold de prod tiene versionado de S3 o respaldo anterior al 2 nov? ¿Quién autoriza restaurarlo? Versionado: **sí**, Enabled en los tres ambientes y sin Lifecycle (DevOps, 2026-11-18). Sigue abierto quién autoriza restaurar.
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

## Flujo de las mallas por periodo y SCD (Slowly Changing Dimension, dimensión de cambio lento) (2026-11-12, ver [[inprod-primas-arl-202606]] 3e)
- **Preguntar al equipo/dueños del flujo:** en cada malla, ¿cada corrida trae la **foto acumulada** (todos los periodos hasta el actual, como el `WHERE PERIODO <= X` de ARL en prod) o **solo el periodo cargado** (Auxilios Funerarios filtra por fechas en Silver)?
- ~~¿Al cargar un periodo nuevo, los registros de periodos anteriores deben seguir activos en Gold?~~ **Resuelta (2026-11-17, decisión del usuario): en Gold hay un solo periodo activo; al cargar otro, los anteriores quedan inactivos (`ACT_REG = 0`, histórico).** Consecuencia: el SCD sí debe cerrar el periodo anterior y lo que se ajusta es el **freno**, que debe medir el 20 % solo sobre los activos del mismo periodo. 
- **Regla del SCD por periodo y las 12 dudas abiertas del fix (2026-11-17):** ver [[fix-scd-freno-periodo]], secciones "Regla del SCD por periodo" y "Dudas abiertas del fix".
- ¿Por qué el `WHERE` de ARL en QA es `PERIODO = '202509'` y en prod `<= '202606'`? ¿Cuál es el oficial? **Respondida 2026-11-19:** el oficial es `=` (decisión del usuario) y el `where` real sale de DynamoDB `cnf-mallas` (la lambda sobrescribe el JSON), ver [[fix-scd-freno-periodo]].
- Auxilios Funerarios en prod: ¿Gold ya tiene cerrados los periodos anteriores? (consulta de solo lectura por `per_envio_tesoreria`). ¿Se detectó antes algún reclamo?
- Primas Rentas y Endosos, Reserva Matemática ARP y RVI: ¿tienen el mismo patrón (solo el periodo en Silver)?

## Dudas que dejó el barrido de repos (2026-11-12, ver [[auditoria-repos-2026-11]])
- ¿Cuál configurador manda: las 6 `configuration-<malla>`, la general o el T2 ([[sc-datalake-niif-configurador-mallas]])? Las fichas de las 6 por malla son casi iguales porque el código casi no cambia.
- ¿Qué repo de IaC manda en cada cuenta, [[iac-analytics-datalake-niif]] o [[iac-analytics-datalake-niif-operations-manager]]? ¿Qué está aplicado?
- ¿Cuál es el filtro del disparador S3 de [[sc-datalake-niif-send-via-sftp-lambda]]? En el código solo se ve que procesa eventos `aws:s3` y quita el prefijo `export`.
- ¿De quién es el bucket `accountingtool` y quién lo consume (retorno desde Prophet/FIS, [[sc-datalake-niif-get-files-sftp]])? El paso del diagrama de esa lambda está inferido.
- El README nuevo de [[sc-datalake-niif-composite_notification_evaluator]] dice que también lee el stream de su tabla; el código de Alfa ignora los `Records`. ¿Es meta o ya existe?
- ¿La integración de [[sc-datalake-niif-post-login]] con certification usa el flujo implícito o un `state`? No está en el repo nuevo.
- ¿`bronze_primas_arl.py` o `extraccion_estandar_emisiones.py` (PCR) están en uso en algún job desplegado? Parecen versiones previas; la de PCR no trae el fix de INC-5.
- ¿Los `parametros_extraccion.json` versionados en [[cfg-datalake-niif-config-s3]] son estado de runtime que el pipeline pisa en cada corrida? ¿El Excel de ejemplo de [[cfg-datalake-niif-params-s3-landing]] trae datos reales?
- Previsional y PCR Vida no encajan en el enum de malla de la plantilla de ficha; quedaron como `transversal`. ¿Se agregan como mallas propias?
- **Desfase quality/main (ARQ-50, [[auditoria-repos-2026-11]]):** 21 repos tienen el mismo contenido en las dos ramas (solo merges); 12 no. Falta ver **qué** cambia en los 4 repos que divergen en 1 archivo y en `iac-analytics-datalake-niif`, `iac-analytics-datalake-niif-operations-manager`, `cfg-datalake-niif-config-s3` y `tpl-datalake-niif-email-template-s3` (`git diff --stat origin/quality origin/main`). **Aplazado:** la rama base de cada repo se confirma al trabajar en él (Alfa sigue en construcción).

## Para el arquitecto: inconsistencias del diagrama aprobado (2026-11-12, ver [[arquitectura-niif17]])
- En `Página-10` el ERP lleva el número 7 (igual que la Reportería) y no hay paso 8. ¿El ERP es el 8?
- El job de Transformación está en la zona Plata en `Página-1` y en la zona Oro en `Página-6`. ¿Cuál es? Cambia el conteo de jobs del diagrama (8 o 9, [[familia-mallas-alfaprd]]).
- CloudWatch y `ModeloTrazabilidad` aparecen en `Página-1` sin conexiones, y `Página-2` no dibuja observabilidad (ARQ-31).
- Las Fuentes alimentan directo al Subledger Contable, además de la Bodega. ¿Es una carga independiente de la certificación?

## Primas Rentas ([[inprod-primas-rentas-202606]])
- ~~¿Gold de Primas Rentas debe guardar solo las rentas del periodo o el acumulado?~~ **Resuelta (2026-11-13, Victor en la reunión): solo las del periodo, 401 para 202606 (de 400 a 600 por mes).** Encaja con `VLR_PRIMA_UNICA`, que se cobra una vez, al expedir la renta. Lo que queda es técnico y nuestro: el SCD por periodo ([[pendientes]]).
- ~~¿La llave del SCD de Primas Rentas incluye el periodo?~~ **Resuelta en el repo (2026-11-13): no, es solo `NID_RENTA`.** Falta confirmar el JSON desplegado.
- ~~¿La versión del job Silver con el join sin "Ajuste" está en prod?~~ **Resuelta (2026-11-13): sí, el script de prod es idéntico al del repo.** No se revisan los periodos certificados ni los conteos en Athena: la simulación de Victor y el código bastan (decisión del usuario, 2026-11-13).
- **Víctor (2026-11-25):** la forma paralela "por esa misma conexión", ¿permite varias sesiones JDBC simultáneas del mismo usuario de servicio? ¿Cuántas como máximo? Hoy el proceso de Emisiones abre hasta 3 a la vez (`:764`), y 4 por tabla en 2 tablas de PCR Vida (`partitionColumn`). Define si se puede leer una tabla en particiones o correr los 3 casos a la vez ([[modularizacion-extraccion]]).
- **DevOps / DEV (2026-11-25):** ¿quién cargó los ítems de Emisiones en `cnf-mallas` y de dónde? No están en ningún repo. ¿La lambda `niif-dev-configuration-general` desplegada es el código de `quality`? Con 25 tablas en PCR Vida, `quality` (que exige 27) fallaría, y la corrida del 24 nov pasó ([[modularizacion-extraccion]]).
