---
tipo: pendiente
actualizado: 2026-11-05
---
# Pendientes

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
- [ ] **T1a SES hecho (2026-11-04), no desplegado:** `Backend\Glue\reposNew\niif-dev-notification-certification\` (hexagonal, 451 pruebas, 99,9 % cobertura). Falta: identidad y *sandbox* de SES, rol con `ses:SendEmail`, confirmar que nadie lee `snsMessageId`, subir artefactos y desplegar por IaC. Ver [[job-notificacion-ses]].
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
- [ ] Decidir y aplicar la corrección de diseño: extracción que conserve lotes de respaldo, o SCD que no apague por ausencia en cargas incrementales. Probar primero en DEV con `errores\replicar-fallo-dev.sh`.
- [ ] Detectar "sin datos" contando filas sin el encabezado del CSV y no enviar el correo de certificación con resumen y totales vacíos.
- [ ] Revisar si las otras 5 mallas comparten el riesgo (mismos jobs genéricos de extracción y SCD).
- [ ] Analizar los 1.100.009 registros (91 %) con `per_proceso_point` nulo en `scd_primas_arl`.
- [ ] **Decidir qué hacer con la evidencia de prod ya subida al repo público** (correos, ids de Cognito y de cuenta): redactar, reescribir historial o repo privado. No subir más sin redactar.
- [ ] Ejecutar `replicar-fallo-dev.sh` en CloudShell de DEV (hoy solo probado con `aws` simulado) y asignar IDs `ARQ`/`CUM` a INC-1…INC-6 en la matriz de la fase 3.
