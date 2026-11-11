---
name: niif17-alfa
description: Lineamientos del proyecto NIIF 17 de Seguros Alfa en AWS (pasos 2 y 3 del modelo de solución). Usar SIEMPRE que se diseñe, escriba, revise, refactorice o analice código o arquitectura de los jobs de Glue del medallón (extracción Oracle → Bronce → validación → remediación → Silver → Gold SCD → certificación por Athena), de las lambdas o jobs de notificación y certificación (SES/SNS, enlaces Certificar / No certificar, transporte a FIS), de las Step Functions `niif-<amb>-<malla>`, o cuando se busquen brechas de seguridad, costos (Athena) o arquitectura. También al actualizar el estado del proyecto en MemoriaIa, al revisar un PR, al analizar un incidente de las mallas (Primas Rentas, Endosos, ARL, Auxilios Funerarios, Reserva Matemática ARP y RVI) o al tocar el diagrama NIIF17_ModeloSolucion. No contiene código: contiene reglas de calidad, código seguro, patrones de diseño y cómo llevar la memoria.
---

# NIIF 17 Alfa: lineamientos de arquitectura y calidad

Proyecto de **Seguros Alfa S.A. y Seguros de Vida Alfa S.A.** (aseguradora regulada por la SFC, Colombia). Todo el stack es AWS, región us-east-1, una cuenta por ambiente (DEV, QA, PRD).
Las cifras que produce este sistema se **certifican** por personas del negocio, viajan a los motores actuariales (FIS, Prophet) y terminan en el **cierre contable**. Un error aquí no es un bug técnico: es una cifra regulatoria equivocada. Ese es el criterio para decidir cualquier duda: **ante la duda, se detiene y se avisa; nunca se publica algo parcial ni se certifica en silencio**.

## Alcance del equipo
- **Nuestro:** el sistema de **jobs de Glue** (medallón de los pasos 2 y 3) y las **lambdas y jobs de notificación y certificación** del paso 3.
- **Foco actual (2026-11-10):** solo la cadena de notificación, en 5 partes: (1) lambdas, (2) plantillas HTML, (3) job de notificación por SES, (4) enlaces de aprobar y rechazar, (5) observabilidad completa en logs por paso para ubicar la falla. Los costos y montos de Athena (COS-07 a COS-09, ARQ-34, ARQ-36 y ARQ-37) quedan en el backlog. Detalle en `[[notificaciones-vs-arquitectura]]`.
- **Fuera por ahora (decisión 2026-11-10):** Grafana (dejamos logs, métricas y alarmas listos para conectarlo) y las HU transversales 01 a 04 (quedan como referencia). El foco es normalizar la arquitectura de notificaciones.
- **Contexto, no nuestro por ahora:** programación (manager y configuradores), motores actuariales (pasos 4-7), Subledger DataStage (8), SAP (9). Se leen para entender contratos, no se rediseñan sin que el usuario lo pida.
- Tareas asignadas y estado (2026-11-10):
  - **T1a/T1b**, correo por SES con plantilla HTML: probado en QA de punta a punta (el correo llega a la bandeja). Sigue pendiente lo de DevOps: `--SES_SENDER` por defecto y `templatePath` en singular. El enlace de descarga abre la consola de S3 (ARQ-29, decisión pendiente). **T1c y T1d** sin empezar.
  - **T2**, lambda general de configuración hexagonal: hecha y sin desplegar. Ojo: en DEV ya existe `niif-dev-configuration-general`, de otro desarrollador y sobre la base del código desplegado; hay que aclarar cuál queda.
  - **T3**, validaciones de reserva matemática: pendiente.
  - En curso: alinear la cadena de notificación al diagrama y a la calidad de código, y limpiar DEV. Estado en `[[pendientes]]`, `[[notificaciones-vs-arquitectura]]` e `[[inventario-lambdas]]`.

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
2. **Arquitectura hexagonal en todo Python** (lambdas y jobs de Glue), con **PEP 8** (sangría de 4 espacios; identificadores en inglés: `snake_case`, `PascalCase`, `UPPER_SNAKE_CASE`; los contratos externos en español se conservan como constantes): dominio puro en el centro, AWS en los adaptadores, una raíz de composición. Estándar propio: `Docs\08-estandares-desarrollo\estandar-lambdas-python.md` (`[[est-desarrollo-hexagonal]]`). Si choca con un documento del banco, manda el del banco, en especial el **gobierno DevOps NIIF 17** (`[[est-gobierno-devops-niif]]`): nombres `niif-{env}-*`, repos `sc-datalake-niif-*` y `data-datalake-niif-*` (los crea y versiona DevOps), `/src/lambda_function.py` como handler y el resto modular en `src/`, pruebas en `/test/`, cobertura mínima de 70 % en el pipeline (apuntamos al 80 %). **DevOps hace la infraestructura y los commits; nosotros entregamos el código en un zip con el nombre del recurso** (ej. `niif-dev-composite-notification-evaluator.zip`).
3. **Compuerta antes de escribir:** ninguna capa se escribe ni se notifica sin validar antes y sin cuadrar `entra = sale + cuarentena` (HU-TR-02, HU-TR-04). Una regla bloqueante detiene el flujo.
4. **Un `ejecucion_id` de punta a punta**, generado una vez (es el nombre de la ejecución de la Step Function, que crea el configurador) y propagado a cada job (`--EJECUCION_ID`), rutas S3, DynamoDB (junto al `certCode`), logs, métricas y correo. **Hoy no existe (ARQ-32):** los jobs no lo reciben y la certificación arranca un `certCode` nuevo sin enlazarlo. Debe llegar al 100 % por diseño: una sola fuente (`$$.Execution.Name` en todas las tareas), el ID guardado dentro del dato desde la certificación, una prueba automática sobre las definiciones de las Step Functions y ninguna entrada lateral. Los logs van en español y cada línea lleva `ejecucion_id`, `request_id` y, desde la certificación, `cert_code`.
5. **Idempotencia:** el mismo evento dos veces no repite efectos (correo, escritura, transporte). Reprocesar un periodo da el mismo resultado.
6. **Nunca se silencia un error.** Se clasifica con la taxonomía, se registra en JSON y se relanza.
7. **Montos exactos y costo consciente en toda ETL:** importes en DECIMAL con precisión y redondeo declarados, calculados una vez y cuadrados entre capas (filas y valores). Rendimiento y costo medidos: DPU-horas de Glue, timeouts reales, Athena con workgroup y tope de escaneo, columnas explícitas (nunca `SELECT *`), particiones. Medir antes de optimizar. Todo cambio de ETL declara su impacto en montos, tiempo, cómputo y consultas.
8. **Sin datos personales** en logs, correos, repositorio ni ambientes no productivos (Ley 1581). El repo es **privado desde el 2026-11-09** (antes fue público y lo expuesto sigue en el historial): igual, nada de evidencia, correos, IDs de cuenta, IPs ni tokens en archivos versionados. La evidencia sacada de AWS va en `EvidenciasAws\<ambiente>-<tema>\`, carpeta ignorada por Git.
9. **Todo por IaC y pipeline** (Terraform/Terragrunt, Jenkins, build once deploy many). Nada creado a mano en consola. DevOps despliega y hace los commits; nosotros entregamos el código y la `config/` en un zip con el nombre del recurso.
10. **Validar con evidencia, no suponer.** Cada afirmación sobre el comportamiento real se comprueba (código, consulta, script PASS/FAIL). Lo no comprobado se marca **hipótesis** o **propuesta**.

## Forma de trabajar con el usuario
- Español, conciso, palabras simples cuando lo pida. Plan antes de trabajo pesado.
- No hay acceso directo a AWS desde aquí. El usuario (Desarrollo) solo entra a **DEV** y corre ahí los comandos de CloudShell. **QA y PRD los opera DevOps**: para esos ambientes se entregan paquetes y comandos de solo lectura, y la evidencia vuelve por el usuario.
- Los comandos de CloudShell empiezan con `export AWS_PAGER=""` y no imprimen tokens, URL firmadas ni correos.

## Estado real de AWS (no suponerlo por el nombre)
- **DEV es compartido.** Otras mallas y equipos (PCR Vida, Generales, Satélites, Previsional, Emisiones, clientes y SMB) y la plataforma (CloudGuard, Control Tower) tienen recursos y corridas activas ahí. Solo tocamos lo de nuestro alcance.
- Qué se usa se decide con `Backend\cloudshell-inventario\evidencia-uso-dev.sh` (invocaciones de 90 días, disparadores, API y WAF, Step Functions, streams), nunca por el nombre. La clasificación vigente está en `Docs\06-brechas\limpieza-dev-recursos.md`.
- Lo que tiene etiquetas de Terraform (`terraform-aws-modules`, `ManagedBy`) se cambia o se retira por Terraform/DevOps, no desde la consola. No se borra nada sin un dueño confirmado.
- La cadena vigente del paso 3 en DEV es: el API `datalake-dev-niif-api-gtw` (con WAF), `niif-dev-short-link-redirect`, `niif-post-login`, `niif-dev-certification`, trigger, copy, move, el evaluador compuesto y `datalake-dev-niif-send-via-sftp`. El API `Certificación API` es viejo, no tiene WAF y su callback sigue en Cognito (SEG-14).
- **Certificar → FIS no se ha probado de punta a punta en DEV** (ARQ-30). No dar el transporte por funcionando.
- Documentar en dos lugares: `Docs\` (humanos, prosa completa) y `MemoriaIa\` (IA, notas cortas que apuntan a Docs). No duplicar.
- Toda brecha nueva lleva ID `SEG-xx`, `COS-xx`, `ARQ-xx` o `CUM-xx`, igual en la nota y en `Docs\06-brechas\`.
- Esta skill no lleva código. Los ejemplos de referencia viven en el repo:
  - Job de Glue hexagonal: `Backend\notificacion\niif-dev-notification-certification\`.
  - Lambda hexagonal: `Backend\Lamnbdas\inventario-aws\t2 unificadas\sc-niif-configurador-mallas\` (desplegaría como `datalake-<amb>-niif-configurador-mallas-lambda`).

## Mantener esta skill
Si cambia un lineamiento (por decisión del usuario, un documento nuevo del banco o el diagrama), actualizar la referencia afectada y la nota de memoria correspondiente en el mismo cambio. Las reglas de aquí resumen; el detalle vive en `Docs\` y `MemoriaIa\`.
