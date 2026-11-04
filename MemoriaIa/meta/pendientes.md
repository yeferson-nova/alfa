---
tipo: pendiente
actualizado: 2026-11-03
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
- [ ] Correr `Backend\cloudshell-inventario\recolectar-faltantes.sh` en CloudShell (solo lectura; reemplaza repetir la extracción) y analizar `niif-faltantes.zip`. Guía: `LEEME-recolectar-faltantes.md`.
- [ ] Confirmar con el equipo cuál generación es la oficial (G3 o G4) y desactivar el programador sobrante (riesgo de doble ejecución).
- [ ] Asignar IDs `SEG`/`COS`/`ARQ` a los hallazgos `LMB-xx` y `SF-xx` en la matriz de la fase 3.
- [ ] Pedir la línea base SonarQube del repositorio de las lambdas antes de modificarlas ([[est-codigo-seguro]]).
- [ ] Resolver [[preguntas-abiertas]].
