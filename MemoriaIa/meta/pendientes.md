---
tipo: pendiente
actualizado: 2026-11-11
---
# Pendientes

> **PAUSA DE LA MIGRACIÓN (2026-11-11, decisión del usuario).** Se detiene la migración de la cadena de notificación y certificación. No se avanza con el roadmap ni se lanza nada en DEV hasta que el usuario lo reanude. El estado congelado y la forma de retomar están en [[traspaso-sesion]] §Pausa.

## Organización (plan por fases)
- [x] Fase 1: reorganizar carpetas, corregir rutas e índice (2026-11-03).
- [x] Borrar `MemoriaIa\CLAUDE.md` duplicado (lo hizo el usuario, 2026-11-03).
- [x] Fase 2: 6 fichas en `estandares\`, 6 en `hu\` y [[marco-normativo]] extraído de los documentos (2026-11-03). Las brechas candidatas quedaron marcadas como "propuesta" dentro de cada ficha.
- [ ] Fase 3: matriz de brechas con IDs `SEG`/`COS`/`ARQ`/`CUM` en `Docs\06-brechas\matriz-brechas.md`. Incluye costos de Athena.
- [ ] Fase 4: trabajo técnico de [[tareas-aws]] contrastado contra la matriz.

## Técnicos
- [ ] Actualizar `Docs\05-arquitectura\aws_pasos_2_y_3.drawio` con API Gateway, Lambda de certificación y URL prefirmada de S3 ([[diseno-propuesto]]).
- [x] Revisar el código de las lambdas para identificar los modelos que conviven: son 4 generaciones paralelas ([[inventario-lambdas]], 2026-11-03).
- [ ] **Roadmap (2026-11-03):** `Tareas\roadmap-cambios.md`. Orden validado: fase 0 recolección → T2-A (un código en las 6 lambdas) → T2-B (una lambda + manager envía `malla`) ; T1a+b (SES con HTML) en paralelo, pedir ya trámites SES (DNS, sandbox, seguridad). Propuesta, por validar con el equipo.
- [ ] **T2 lambda general hecha (2026-11-03), no desplegada:** `Backend\Lamnbdas\inventario-aws\t2 unificadas\sc-niif-configurador-mallas\` (hexagonal, 174 pruebas, 99 % cobertura). Falta: confirmar catálogo de 5 mallas `POR_CONFIRMAR`, prueba de equivalencia de los 4 JSON por malla, tabla de idempotencia y rol en IaC.
- [ ] **T1a SES hecho (2026-11-04), no desplegado:** `Backend\notificacion\jobs-glue\niif-dev-notification-certification\` (hexagonal, 451 pruebas, 99,9 % cobertura). Falta: identidad y *sandbox* de SES, rol con `ses:SendEmail`, confirmar que nadie lee `snsMessageId`, subir artefactos y desplegar por IaC. Ver [[job-notificacion-ses]].
- [ ] **T1a+T1b, entrega sobre el job desplegado (2026-11-09), detalle en [[job-notificacion-ses]]:** decisión del usuario: se modifica `notification_certification.py` (cambio mínimo, por auditoría) y `niif-dev-notification-certification` hexagonal queda para después. Hecho: código SES con envío parcial, HTML con `templatePath`, avisos por tono, 104 pruebas, 6 plantillas en `htmlNew\newUx\` con vistas previas y validador, JSON de caso unificados a `email-templates/`, plantillas y script en S3 DEV, `--SES_SENDER` en el job. Primera corrida en DEV SUCCEEDED con dos fallos de permisos (SES sobre cada destinatario en sandbox y lectura de `email-templates/*`); política `niif-dev-notification-ses-templates` creada por CLI, sin IaC. Falta: prueba final en una dirección de prueba (`pruebas/primasArl-prueba.json`), ver el correo en Outlook, Gmail y móvil, probar un caso con resumen y uno compuesto, redactar la solicitud a DevOps para QA y PRD (política, `--SES_SENDER`, script con hash, plantillas, `templatePath`), retirar `TemporarySesTestAccess` y SNS, borrar `email-template/` (singular), DKIM y salida del sandbox, commit de los archivos nuevos. El script desplegado no es idempotente: probar solo con `certCode` en SENT, nunca en TRANSPORT. Permisos del rol (SES por destinatario en sandbox, lectura de `email-templates/*` y de `pruebas/*`): ver la sección IAM de [[job-notificacion-ses]]; la política se creó por CLI y DevOps debe llevarla a IaC. Primera corrida completa con HTML en DEV: SUCCEEDED (2026-11-09); falta confirmar la llegada a la bandeja.
- [ ] **T1a SES, validación en DEV (2026-11-09), detalle en [[job-notificacion-ses]]:** abiertos sandbox (cuota 200), DKIM y MAIL FROM en FAILED, falta el conjunto `niif-dev-notificaciones`, política IAM de `ses:SendEmail` para el remitente real, subir librería y `job_notificacion.py` a S3, y pedir a DNS los 3 CNAME de DKIM y los registros de MAIL FROM (SPF y DMARC ya existen; DMARC está en `p=reject`, riesgo de rechazo si no alinean). Segunda corrida hecha el 2026-11-09, ver la tabla en [[job-notificacion-ses]]. El script de prueba SES de S3 ya no existe (404). 1b: el job aún arma solo texto.
- [ ] Correr `Backend\cloudshell-inventario\recolectar-faltantes.sh` en CloudShell (solo lectura; reemplaza repetir la extracción) y analizar `niif-faltantes.zip`. Guía: `LEEME-recolectar-faltantes.md`.
- [ ] Confirmar con el equipo cuál generación es la oficial (G3 o G4) y desactivar el programador sobrante (riesgo de doble ejecución).
- [ ] Asignar IDs `SEG`/`COS`/`ARQ` a los hallazgos `LMB-xx` y `SF-xx` en la matriz de la fase 3.
- [ ] Pedir la línea base SonarQube del repositorio de las lambdas antes de modificarlas ([[est-codigo-seguro]]).
- [ ] **Estructura de repos (2026-11-05), en iteración, sin crear nada.** Diseño, decisiones y plantilla de DevOps en [[estructura-repos-propuesta]]. Falta bajar los CSV de tablas por malla (el zip de CloudShell no abrió).
  - Enviar a DevOps las preguntas de la sección "Estructura de repos y despliegue" de [[preguntas-abiertas]]. **Sin respuesta por ahora; seguimos avanzando con supuestos provisionales (propuesta).**
  - Supuestos: IaC y despliegue son de DevOps (Jenkins y Terraform); la organización de GitHub no es nuestra y nos dan el repo; nosotros entregamos código, artefacto versionado y `config/` con parámetros.
  - Criterio de nombres, **a decidir**: repos por acción (`data-niif-extraccion`, `-validacion-tecnica`, `-remediacion`, `-preparacion-mallas`, `-consolidacion-historica`, `-certificacion`, `-notificacion`), no por capa del medallón. Alternativa más habitual en la industria: un repo `data-niif-pipeline` con carpeta por etapa. Decide la respuesta de DevOps sobre tags por componente. Las lambdas van en repos `sc-niif-*` propios.
  - Mientras tanto se puede avanzar con: el diseño interno de cada repo (hexagonal, `src/ tests/ config/`), la librería común, y el orden de migración. La capa (Bronze, Silver, Gold) se declara en el README del repo, no en su nombre.
  - **Sobran por un error mío** (copias, sin usar): `Backend\Glue\estructura-nueva\` y `Backend\reposEstandar\` (README vacíos). Decidir si se borran.
  - Hallazgos de integridad de Glue (Gold SCD sobrescrito, sin compuerta de calidad, `dropDuplicates` sin registro, concurrencia sobre el mismo JSON): se tratan **después** de cerrar la estructura, como parte de la migración.
- [ ] **Diagnóstico de resiliencia y escalabilidad (2026-11-05):** `Docs\07-entregables\diagnostico-resiliencia-escalabilidad.md` (18 problemas con solución, para Analítica). Falta correr `Backend\cloudshell-inventario\comparar-arquitectura.sh` en CloudShell DEV y PRD (solo lectura; probado solo con `aws` simulado) y pasar sus `FALTA`/`REVISAR` a IDs `SEG`/`COS`/`ARQ`. Hallazgo: la plantilla corporativa `Glue\data-datalake-niif-primas-rentas-glue-job` trae cobertura en 0 % (contradice > 80 %) y su README no coincide con el código.
- [ ] **Roadmap (2026-11-05, reescrito):** `Tareas\roadmap-normalizacion.md`, 4 frentes en paralelo (incidente en prod, entrega de tareas, auditoría, migración al estándar) con tiempos en días hábiles +30 % por lo desconocido. Camino largo ~17 semanas (~13 sin colchón), supuesto de 3 personas. Propuesta por validar. Complementa `Tareas\roadmap-cambios.md` y [[estructura-repos-propuesta]].
- [ ] Resolver [[preguntas-abiertas]].

## Incidente en prod: Primas ARL 202606 (2026-11-05), ver [[inprod-primas-arl-202606]]
- [ ] **Plan de cambios del incidente (2026-11-05), detalle en [[inprod-primas-arl-202606]] secciones 3b y 3c.** INC-5 confirmada con diff contra el script de prod (`errores\extraccion_estandar_optimized.py`): faltan 3 bloques de la exclusión. Paquete 1 = **fix de extracción** (script del repo, 3 bloques juntos; el único obligatorio). Seguros opcionales: freno de `scd.py` (borrador sin entregar, 18 pruebas; **antes hay que hacer opcional `UMBRAL_APAGADO_PCT`**, la Step Function no lo pasa), correo con totales vacíos y alarma de fallo del SCD. `Tareas\paquete-release-inprod\LEEME.md` **no existe**: hay que crear el paquete de release (instrucciones, prueba en QA, reversa). Pendiente confirmar si QA tiene la versión nueva de extracción y decidir si se conserva el borrador de `scd.py`.
- [x] ~~Inmediato: no volver a correr Primas ARL ni certificar 202606~~: la recuperación de 202606 ya la hizo el equipo (2026-11-05). Queda solo el fix preventivo.
- [x] ~~Versionado y restauración de Gold~~: hecho por el equipo (2026-11-05).
- [x] Comparar el script de prod con el repo: hecho 2026-11-05, INC-5 confirmada (3 bloques faltantes). [ ] Falta confirmar si QA tiene la versión nueva (comparar su script igual).
- [ ] Ver las carpetas `lote_cargue_id=` de los 5 segmentos de 202606 en `niif-prd-assets\segmentos\` (qué corridas respaldan los 1.203.789).
- [ ] Verificar por qué entraron endosos de 202610 con `WHERE PERIODO <= '202606'` (consulta del paso 6 de la sección 7 de `errores.md`).
- [ ] Agregar `queries.summary` al JSON de prod `primas-arl/primasArl.json` (copiar de QA) y reenviar la notificación cuando Gold esté corregido.
- [x] Corrección de diseño aplicada y **validada en DEV 2026-11-06** (extracción con lotes de respaldo + freno del SCD con umbral opcional); detalle en [[inprod-primas-arl-202606]] 3d. [ ] Falta: línea en blanco PEP 8 en el repo, limpiar archivos de prueba `_prueba_freno` en S3 DEV, avisar al equipo (el `scd.py` de DEV es compartido por todas las mallas), congelar con hash y liberar a QA → PRD (respaldar antes el script de cada ambiente).
- [ ] **Pasó a la tarea de notificaciones (T1), decisión 2026-11-06, no va en el release 1:** detectar "sin datos" contando filas sin el encabezado y no enviar el correo con total y detalle vacíos. Diseño en [[inprod-primas-arl-202606]] 3d. Depende de la respuesta del líder (¿certificar en cero?). Futuro: alarma de fallo hacia soporte.
- [ ] Revisar si las otras 5 mallas comparten el riesgo (mismos jobs genéricos de extracción y SCD).
- [ ] **Plan acordado 2026-11-06 (propuesta):** (1) subir el fix de extracción (ya commiteado: `c0682a5`, `f48f8b8`; entregar archivo completo congelado) y (2) auditar la familia de los 12 jobs de las 6 mallas ALFAPRD contra los 9 jobs del diagrama. Detalle en [[familia-mallas-alfaprd]].
- [ ] Analizar los 1.100.009 registros (91 %) con `per_proceso_point` nulo en `scd_primas_arl`.
- [x] El repo pasó a **privado** el 2026-11-09 (resuelve la decisión sobre la evidencia de prod subida al repo público). [ ] Queda evaluar si lo que estuvo expuesto durante el periodo público (correos, ids de Cognito y de cuenta) pide acciones adicionales: revisar clones o forks y valorar rotar identificadores. Seguir sin subir credenciales.
- [ ] Ejecutar `replicar-fallo-dev.sh` en CloudShell de DEV (hoy solo probado con `aws` simulado) y asignar IDs `ARQ`/`CUM` a INC-1…INC-6 en la matriz de la fase 3.
- [ ] **Notificaciones, verificación del flujo en DEV (2026-11-10), detalle en [[job-notificacion-ses]]:** ejecutar bloque de login/API Gateway/Cognito, ImageUri y logs de `send-via-sftp`, `scheduler list`; confirmar con Víctor el comportamiento de No certificar; paquete de QA (script, plantillas, `templatePath`, política IAM, `--SES_SENDER` por defecto en el job, no quitar `SNS_TOPIC_ARN` de trigger/evaluador, plan de pruebas en QA).
- [ ] **Paquete de release 2 armado (2026-11-10):** `Tareas\paquete-release-notificaciones\` (script con hash, 6 plantillas, plantilla de política IAM, plan de pruebas en QA y reversa). Faltan por completar con QA: nombre del job y ruta del script (`get-jobs`), bucket de assets, identidad SES (QA tiene SES, dominio con DKIM OK; falta confirmar sandbox), que callback/autorizador/`REDIRECT_BASE_URL` apunten a QA. Destinatarios de QA: personas reales del equipo; avisar antes de certificar. Ver [[job-notificacion-ses]].
- [ ] **Notificaciones vs. arquitectura (2026-11-10):** decidir con el equipo ARQ-22 (quién envía) y ARQ-23 (No certificar y reprogramación); luego corregir ARQ-26/27 y SEG-12, desplegar `niif-dev-notification-certification` hexagonal, unificar lambdas, SEG-10/11 con DevOps y actualizar el diagrama. Detalle en [[notificaciones-vs-arquitectura]].
- [ ] **Plan para unificar la cadena de notificación/certificación (propuesta 2026-11-10, fases 0 a 6):** detalle y estimaciones en [[notificaciones-vs-arquitectura]]. Siguiente: reunión de decisiones (fase 1) y fase 2 (correcciones de bajo riesgo), que no depende de ellas.

- [ ] **Daily 2026-11-11:** llevar las preguntas de ARQ-29 (aprobadores con acceso a consola AWS; opción A vs B) y los pendientes de QA. Ver sección "Para el daily del 2026-11-11" en [[preguntas-abiertas]].

- [ ] **Limpieza de DEV y alineación (iniciado 2026-11-10):** paso 1 = evidencia de uso con `Backend\cloudshell-inventario\evidencia-uso-dev.sh` (solo lectura: invocaciones 90 d, mappings, reglas, schedules, API GW + WAF, S3, Step Functions, Glue, streams). Sin evidencia no se clasifica nada. Foto previa (2026-11-03, 65 lambdas, incompleta) en `EvidenciasAws\dev-inventario-lambdas\`. No tocar recursos de plataforma ni de otros equipos (CloudGuard, Control Tower, Quick Setup, smb/clientes, contingency, accountingtool, PipelineObserver). Dudas a validar: cuál `short-link-redirect` y cuál `post-login` están conectados al API; G3 (`configurador-*`, `programador-configuraciones`) y G4 leen el mismo stream. Borrar solo con dueño confirmado y por Terraform si el recurso es de IaC. Ver [[inventario-lambdas]] y [[notificaciones-vs-arquitectura]].

- [ ] **Optimización de Athena y Gold: pasó al roadmap como fase 7 (2026-11-10, decisión del usuario); antes estaba en el backlog:** COS-07 Gold sin particiones (cada consulta lee todo el historial y el SCD); COS-08 sin workgroup propio ni tope de escaneo (`primary`); COS-09 `CAST` de la columna de periodo, que impide el filtrado por estadísticas y la poda. También quedan para después ARQ-34, ARQ-36 y ARQ-37 (montos de las consultas). Detalle en `Docs\06-brechas\consultas-athena-notificacion.md`. Antes de cambiar: medir `DataScannedInBytes` de las consultas actuales.

- [ ] **Roadmap de la cadena de notificación (propuesta 2026-11-10, sin aprobar). Alcance (2026-11-11): flujo completo de `Página-2`, 6 lambdas + 1 job, incluida la Lambda SFTP:** `Tareas\T1-notificaciones\roadmap-notificaciones.md`.
  - **Fases:** (1) cerrar el job nuevo (envío real en DEV, N-04, N-08, paquete para DevOps); (2) ID de punta a punta (ARQ-32); (3) reescribir las 7 lambdas (copy y move primero); (4) transporte a FIS (ARQ-30); (5) alertas y métricas; (6) limpieza de DEV y diagrama. En paralelo, el `summary` de ARL.
  - **Estimación:** 25 a 34 días propios.
  - **Decidido 2026-11-10:** D1 = una sola lambda orquestadora, `niif-<amb>-notification-certification-orchestrator` (recurso nuevo), que reemplaza a trigger, copy y move. D5 = los destinatarios de alertas quedan parametrizados (por configuración) hasta que se defina el grupo de soporte. D6 = nombres en el estándar `niif-<amb>-*`: post-login pasa a `niif-<amb>-post-login`; las demás conservan su nombre actual, que ya cumple.
  - **En espera de respuesta:** D2 (No certificar en `prg-programaciones`), D3 (descarga del CSV, ARQ-29) y D4 (reenvío sobre `SENT`).
  - **Agregado 2026-11-10 (resiliencia y Athena):**
    - 1.5: cancelar la consulta en un timeout y marcar `NTF-PLA-005` como recuperable.
    - 2.5: la certificación calcula resumen y totales sobre el mismo CSV y los guarda en `resumen-<certCode>.json`; la notificación deja de consultar Athena (cierra el riesgo de timeout y ARQ-34).
    - 3.1: reprocesador de registros `PENDING` atascados dentro de la orquestadora.
    - Fase 7: medir, workgroup con tope de escaneo, filtros sin `CAST`, regla de redondeo y Gold particionado (con el dueño del paso 2).
    - Total: 35 a 47 días.
    - Athena no tiene modo stream: la resiliencia viene de no consultar en la notificación, cancelar, reintentar y reprocesar.
  - **Prueba en DEV del job nuevo (2026-11-10):** `niif-dev-notification-certification-prueba-v2`, código en `s3://niif-dev-assets/pruebas/notificacion-v2/` (el rol solo lee archivos exactos más `pruebas/*`, `wheelhouse/*` y `email-templates/*`). Resultado: `SUCCEEDED`, log JSON completo, sin envío. Ese zip no trae aún el arreglo de `NTF_FIN_JOB`. Borrar el job al cerrar la fase 1.

- [ ] **Paso 3 (ID único), etapa 1 hecha (2026-11-11):**
  - Ninguna Step Function pasa el ID al job de certificación; solo al evaluador compuesto (`sourceExecutionId` = ARN), y este lanza la certificación sin ID.
  - El job lee 5 argumentos e ignora `LOTE_CARGUE_ID`. El ID debe ir en el `put_item` (`PROCESSED`), antes del `PENDING`.
  - Los argumentos por defecto del job están quemados (`PERIOD=202411`, `CONFIG_KEY` de Auxilios).
  - **Decisiones:** E1 = versión desplegada + INC-3. El paquete inprod solo trae extracción y SCD, y ambos ya están desplegados en DEV, idénticos (comprobado el 2026-11-11). E2 = el configurador genera el nombre `<malla>-<periodo>-<uuid v7>` y se propaga con `$$.Execution.Name`, sin agregar nada después. E3 = en el compuesto, `ejecucionId` + `ejecucionesOrigen`.
  - **Siguiente:** etapa 2 (código en local), solo con orden explícita.

- [ ] **Cadena de notificación (2026-11-11), estado en [[traspaso-sesion]]:**
  - Orquestadora: código listo (commit `a0e1ac0`). Falta lo de DevOps (índice `status-pendingDate-index`, mapping, Scheduler y permisos), el correo de `alert_recipients`, las claves en el JSON global de previsional y verificar `pendingDate` en el job viejo. Ver [[orquestadora-notificaciones]].
  - Certificación: código listo. Falta la etapa 3 en DEV y decidir CER-H1 a CER-H7. Ver [[job-certificacion]].
  - Notificación: en DEV, quitar los 10 argumentos viejos y subir el bloque `notification` al JSON global de S3 antes de volver a correrlo. Ver [[job-notificacion-ses]].
  - Siguiente paso del roadmap: el Paso 5 (transporte a FIS / Lambda SFTP).

- [ ] **Incidente en prod, caso 9411325 (2026-11-11):** el enlace de certificación de Auxilios Funerarios tiene un espacio en el host. Diagnóstico hecho; la corrección no se ha entregado. Ver [[inprod-enlaces-cognito-9411325]].
- [ ] **Repo:** decidir si se commitea el `.gitignore` local (vuelve a ignorar `EvidenciasAws/`) y si se saca del índice la evidencia de prod subida en `0c5931a`.
- [ ] **Repo:** `.claude` está en `.gitignore`, así que la skill `niif17-alfa` **no viaja** con el repo. Decidir si se versiona (por ejemplo, con la excepción `!.claude/skills/`).
