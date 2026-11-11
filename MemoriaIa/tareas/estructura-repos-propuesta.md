---
tipo: diseno
actualizado: 2026-11-05
---
# Estructura de repos y de archivos (propuesta, en iteración)

**Estado:** propuesta. Todavía no se crea ningún repo ni se migra código. Lo marcado "decidido" lo dijo el usuario; lo demás es propuesta por validar. Preguntas para DevOps en [[preguntas-abiertas]]; trabajo por hacer en [[pendientes]].

## Decidido por el usuario
- **Lambdas:** un repo por lambda (`sc-niif-<nombre>`), con su arquitectura adentro ([[est-desarrollo-hexagonal]]).
- **Lo de dentro de Git es nuestro** (Desarrollo). DevOps pone el repo, Jenkins y Terraform, y despliega. No analizar ni suponer lo de DevOps ([[hu-devops]]).
- **Los ambientes son cuentas AWS distintas.** Solo conocemos DEV (533267035849).
- **Nada de ambientes quemados en el código.** Ambiente y nombres de recurso son variables.
  - Lambdas: variables de entorno. Glue: argumentos del job (`getResolvedOptions`).
  - Se declaran en `config/<amb>.json` de cada repo. Secretos: solo el nombre. Aprobadores y destinatarios son datos de negocio, no código.
  - Sin valores por defecto entre ambientes. Una prueba (`test_calidad_codigo.py`) busca literales `-dev-`, `-qa-`, `-prd-`, cuentas y ARN.
- **Repos nombrados por acción, no por capa del medallón.** La capa (Bronze, Silver, Gold) va en el README y en etiquetas.
- **Motor y casos de uso separados:** el motor es genérico y funciona por configuración; el caso de uso es la configuración y el Silver de cada malla.

## Estructura de una lambda
`src/<paquete>/{dominio, aplicacion, adaptadores/{entrada,salida}, configuracion}`, `tests/`, `eventos/`, `config/lambda.<amb>.json`, `scripts/empaquetar.sh`, `pyproject.toml`, `sonar-project.properties`, `CHANGELOG.md`, `README.md`. Referencia: `Backend\Lamnbdas\inventario-aws\t2 unificadas\sc-niif-configurador-mallas\`.

## Glue: propuesta A (recomendada, falta validar con DevOps)
- **Repo `data-niif-pipeline`:** `src/niif_pipeline/{comun, extraccion, validacion, remediacion, preparacion/mallas/<malla>, consolidacion, certificacion}`, `scripts/` con un punto de entrada delgado por job, `config/`, `contratos/parametros_extraccion.schema.json`, `tests/`, `docs/`. Un artefacto por job.
- **Repos aparte:**
  - `data-niif-notificacion` (job ya reescrito, contrato propio; es un job de Glue, no una lambda, lo dispara la lambda `niif-dev-notification-certification-trigger`). Ver [[job-notificacion-ses]].
  - `data-niif-config-mallas` (tipo `data`): `mallas/<malla>/{cnf.json, prs.json, plantilla de remediación}`, `esquemas/`, `tests/`.
  - `lib-niif-comunes` (tipo `lib`): **opcional**, solo si lambdas y jobs comparten código; si no, basta `comun/`.
- **Alternativas:** B (motor en un repo y un repo por malla solo con su Silver y su config, como la plantilla de DevOps) y C (repo completo por malla; descartada, duplica el código genérico). Nombres de preparación y SCD a decidir: `preparacion-mallas` y `consolidacion-historica`.

## Plantilla que dio DevOps (vista el 2026-11-05)
`Backend\Glue\data-datalake-niif-primas-rentas-glue-job\`: **un repo por caso de uso (malla)**.
- Contenido: `src/jobs/silver-primas-rentas.py`, `Jenkinsfile`, `tox.ini`, `sonar-project.properties`, `Dockerfile` vacío.
- Jenkinsfile: `@Library('analitica-library')` y `glueJobsClientesPipeline`; por rama (`develop`, `quality`, `main`) declara credencial `aws-datalake-<amb>`, bucket `niif-<amb>-assets` y ruta `glue-resources/primas-rentas`. La IaC está en `iac-analytics-datalake-niif-operations-manager`.
- Defectos: el README es de Primas Endosos; la cobertura se omite (`src/**.py`, `fail_under = 0`); `tox` usa py38 y py310 pero Sonar dice 3.12; `tox.init` vacío; la ruta del artefacto no lleva versión.
- **Contradice la propuesta A.** Decidir con DevOps si Jenkins soporta varios jobs por repo.
- Se subió primero como repo incrustado (carpeta con `.git` interno), lo que dejaba la carpeta vacía; ya está corregido.

## Lo validado con datos (2026-11-05)
- **Catálogo:** 69 tablas Bronze, 94 Silver, 9 Gold. Silver duplica 67 tablas de Bronce, hay una tabla `lote_cargue_id_*` por lote (crecen) y tablas basura del crawler. Reserva matemática llega a 440 y 511 columnas. Detalle en `Backend\Glue\LEEME.md`.
- **Tablas fuente por malla** (de `cnf-mallas` y `prs-estructuras` en DynamoDB, 40 distintas): Endosos 4, Auxilios 7, Rentas 7, ARL 8, Reserva ARP 18, Reserva RVI 20. De las 40: 24 de una sola malla, 10 de 2, 5 de 3 y 1 de 5; **ninguna en las 6**.
- **Los `config_*.json` de S3 no se editan a mano:** los genera la lambda configuradora desde DynamoDB. La fuente a versionar son los registros CNF y PRS, más las plantillas de remediación.
- Falta: `tablas-compartidas.csv`, `tablas-por-malla.csv`, `cnf-mallas.json` y `prs-estructuras.json`. El zip bajado de CloudShell no abrió en Windows.

## Pendientes de esta nota
- Carpetas sobrantes creadas por error: `Backend\Glue\estructura-nueva\` y `Backend\reposEstandar\`. Decidir si se borran.
- Hallazgos de integridad de Glue (Gold SCD, sin compuerta de calidad, concurrencia): se tratan después de cerrar esta estructura.

- **Ajuste obligatorio (2026-11-10):** los nombres siguen [[est-gobierno-devops-niif]]: repos `sc-datalake-niif-*` y `data-datalake-niif-*` (no `sc-niif-*` ni `data-niif-*`); lambdas y jobs `niif-{env}-*`; `src/lambda_function.py` como handler (el resto del código, modular, también dentro de `src/`) y la carpeta `test/` en singular. **Entregamos un zip con el nombre de la lambda (ej. `niif-dev-composite-notification-evaluator.zip`) y DevOps hace el commit.** Revisar esta propuesta contra eso.

## Cadena de notificación (decidido 2026-11-10)
- **`Backend\notificacion\` = solo código nuevo.** Una carpeta por componente con el nombre del recurso AWS (= nombre del zip). Dentro: `src/lambda_function.py` (o el script del job) + `src/<package>/{domain,application,adapters/{inbound,outbound},config}`, `test/`, `config/`, `eventos/`, `scripts/package.sh`, README, `pyproject.toml`, `tox.ini`, `sonar-project.properties`. Código común en `_shared/`, copiado en cada zip (propuesta; falta confirmar capas con DevOps).
- **El código actual no se mezcla:** sigue en `Backend\Lamnbdas\<nombre>\`, `Backend\Glue\<job>\`, `Backend\htmlNew\` y `Backend\S3\niif-dev-assets\` (decisión del usuario).
- Pendiente: el nombre `niif-post-login` (no sigue `niif-{env}-*`). La lambda `niif-dev-certification` empaqueta numpy y pandas sin usarlos.
