---
name: niif17-alfa
description: Lineamientos del proyecto NIIF 17 de Seguros Alfa en AWS (pasos 2 y 3 del modelo de solución). Usar SIEMPRE que se diseñe, escriba, revise, refactorice o analice código o arquitectura de los jobs de Glue del medallón (extracción Oracle → Bronce → validación → remediación → Silver → Gold SCD → certificación por Athena), de las lambdas o jobs de notificación y certificación (SES/SNS, enlaces Certificar / No certificar, transporte a FIS), de las Step Functions `niif-<amb>-<malla>`, o cuando se busquen brechas de seguridad, costos (Athena) o arquitectura. También al actualizar el estado del proyecto en MemoriaIa, al revisar un PR, al analizar un incidente de las mallas (Primas Rentas, Endosos, ARL, Auxilios Funerarios, Reserva Matemática ARP y RVI) o al tocar el diagrama NIIF17_ModeloSolucion. No contiene código: contiene reglas de calidad, código seguro, patrones de diseño y cómo llevar la memoria.
---

# NIIF 17 Alfa: lineamientos de arquitectura y calidad

Proyecto de **Seguros Alfa S.A. y Seguros de Vida Alfa S.A.** (aseguradora regulada por la SFC, Colombia). Todo el stack es AWS, región us-east-1, una cuenta por ambiente (DEV, QA, PRD).
Las cifras que produce este sistema se **certifican** por personas del negocio, viajan a los motores actuariales (FIS, Prophet) y terminan en el **cierre contable**. Un error aquí no es un bug técnico: es una cifra regulatoria equivocada. Ese es el criterio para decidir cualquier duda: **ante la duda, se detiene y se avisa; nunca se publica algo parcial ni se certifica en silencio**.

## Alcance del equipo
- **Nuestro:** el sistema de **jobs de Glue** (medallón de los pasos 2 y 3) y las **lambdas y jobs de notificación y certificación** del paso 3.
- **Contexto, no nuestro por ahora:** programación (manager y configuradores), motores actuariales (pasos 4-7), Subledger DataStage (8), SAP (9). Se leen para entender contratos, no se rediseñan sin que el usuario lo pida.
- Tareas asignadas: T1 notificaciones (SES, HTML, monitoreo, escenarios), T2 lambda general de configuración, T3 validaciones de reserva matemática. Estado en la memoria (`[[tareas-aws]]`, `[[pendientes]]`).

## Antes de empezar cualquier trabajo (obligatorio)
1. Leer el índice de `CLAUDE.md` (raíz del repo) y abrir **solo** las notas de `MemoriaIa\` que el tema requiera. Protocolo completo en [references/memoria-ia.md](references/memoria-ia.md).
2. Ubicar la pieza en la arquitectura: [references/arquitectura-y-flujo.md](references/arquitectura-y-flujo.md). Si el tema es de arquitectura general, la fuente de verdad es `Docs\05-arquitectura\diagramas\NIIF17_ModeloSolucion_V1.0.drawio` (varias hojas; mapa de hojas en esa referencia). El diagrama se actualiza; si contradice a la memoria, gana el diagrama y se corrige la nota.
3. Revisar las brechas ya conocidas del componente antes de proponer (`Docs\06-brechas\revision-pre-produccion.md`, `rendimiento-y-costos.md`, `seguridad.md`). No redescubrir lo ya documentado: citarlo por su ID.

## Qué referencia cargar según la tarea
| Si la tarea es... | Leer |
|---|---|
| Diseñar o revisar un job de Glue, una capa del medallón, SCD, crawlers, Athena | [references/lineamientos-glue-medallon.md](references/lineamientos-glue-medallon.md) |
| Cálculos de montos y valores, rendimiento de Spark, costo de cómputo (Glue) o de consultas (Athena) | [references/montos-rendimiento-costos.md](references/montos-rendimiento-costos.md) |
| Diseñar o revisar una lambda o job de notificación / certificación / transporte | [references/lineamientos-lambdas-notificacion.md](references/lineamientos-lambdas-notificacion.md) |
| Cualquier código: seguridad, secretos, IAM, datos personales, correo, endpoints | [references/seguridad-y-cumplimiento.md](references/seguridad-y-cumplimiento.md) |
| Decidir un diseño: cifras, cierres, aprobaciones, reprocesos | [references/patrones-seguros.md](references/patrones-seguros.md) |
| Revisar un PR, un entregable o un incidente | [references/checklist-revision.md](references/checklist-revision.md) |
| Registrar avances, dudas, decisiones o estado | [references/memoria-ia.md](references/memoria-ia.md) |

## Principios que no se negocian
1. **El motor no decide, obedece.** Tablas, rutas, buckets, ARN, consultas, destinatarios, umbrales y periodos vienen de metadata versionada (DynamoDB, `config/`, argumentos del job). Si falta algo, se falla con un error clasificado. Nada quemado; ningún valor por defecto que apunte a otro ambiente o a otra malla.
2. **Arquitectura hexagonal en todo Python** (lambdas y jobs de Glue): dominio puro en el centro, AWS en los adaptadores, una raíz de composición. Estándar propio: `Docs\08-estandares-desarrollo\estandar-lambdas-python.md` (`[[est-desarrollo-hexagonal]]`). Si choca con un documento del banco, manda el del banco.
3. **Compuerta antes de escribir:** ninguna capa se escribe ni se notifica sin validar antes y sin cuadrar `entra = sale + cuarentena` (HU-TR-02, HU-TR-04). Una regla bloqueante detiene el flujo.
4. **Un `ejecucion_id` de punta a punta**, generado una vez y propagado a la Step Function, rutas S3, DynamoDB, logs, métricas y correo.
5. **Idempotencia:** el mismo evento dos veces no repite efectos (correo, escritura, transporte). Reprocesar un periodo da el mismo resultado.
6. **Nunca se silencia un error.** Se clasifica con la taxonomía, se registra en JSON y se relanza.
7. **Montos exactos y costo consciente en toda ETL:** importes en DECIMAL con precisión y redondeo declarados, calculados una vez y cuadrados entre capas (filas y valores). Rendimiento y costo medidos: DPU-horas de Glue, timeouts reales, Athena con workgroup y tope de escaneo, columnas explícitas (nunca `SELECT *`), particiones. Medir antes de optimizar. Todo cambio de ETL declara su impacto en montos, tiempo, cómputo y consultas.
8. **Sin datos personales** en logs, correos, repositorio ni ambientes no productivos (Ley 1581). **El repo es público**: nada de evidencia de prod, correos, IDs de cuentas, IPs ni tokens.
9. **Todo por IaC y pipeline** (Terraform/Terragrunt, Jenkins, build once deploy many). Nada creado a mano en consola. DevOps despliega; nosotros entregamos código, artefacto versionado y `config/`.
10. **Validar con evidencia, no suponer.** Cada afirmación sobre el comportamiento real se comprueba (código, consulta, script PASS/FAIL). Lo no comprobado se marca **hipótesis** o **propuesta**.

## Forma de trabajar con el usuario
- Español, conciso, palabras simples cuando lo pida. Plan antes de trabajo pesado.
- No hay acceso directo a AWS: dar pasos de consola y comandos de CloudShell **de solo lectura** para prod; el usuario trae la evidencia.
- Documentar en dos lugares: `Docs\` (humanos, prosa completa) y `MemoriaIa\` (IA, notas cortas que apuntan a Docs). No duplicar.
- Toda brecha nueva lleva ID `SEG-xx`, `COS-xx`, `ARQ-xx` o `CUM-xx`, igual en la nota y en `Docs\06-brechas\`.
- Esta skill no lleva código. Los ejemplos de referencia viven en el repo:
  - Job de Glue hexagonal: `Backend\Glue\reposNew\niif-dev-notification-certification\`.
  - Lambda hexagonal: `Backend\Lamnbdas\inventario-aws\t2 unificadas\sc-niif-configurador-mallas\`.

## Mantener esta skill
Si cambia un lineamiento (por decisión del usuario, un documento nuevo del banco o el diagrama), actualizar la referencia afectada y la nota de memoria correspondiente en el mismo cambio. Las reglas de aquí resumen; el detalle vive en `Docs\` y `MemoriaIa\`.
