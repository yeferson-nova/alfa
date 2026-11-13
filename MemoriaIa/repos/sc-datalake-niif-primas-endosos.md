---
tipo: repo
estado: vigente
actualizado: 2026-11-12
repo: sc-datalake-niif-primas-endosos
categoria: lambda
paso: "2 · Bodega NIIF 17 (configura y arranca la malla)"
malla: primas-endosos
---
# sc-datalake-niif-primas-endosos
> Lambda configuradora de la malla Primas Endosos: reconstruye los JSON de configuración desde DynamoDB, los valida contra las plantillas de S3, los publica y puede iniciar la Step Function.

## Rol en el flujo
- La invoca EventBridge Scheduler (el `manager-configuration` crea los schedules desde el stream de `niif-{env}-prg-programaciones`); no la dispara un stream. Es la `configuration-<malla>` de [[inventario-lambdas]].
- Entrega a la Step Function `niif-{env}-primas-endosos`, que corre los jobs de Glue de la malla (`data-datalake-niif-primas-endosos-glue-job`).
- Duplicidad: hay una lambda por malla (esta), una general (`sc-datalake-niif-configuration-general`, que embebe este mismo módulo) y el configurador T2 de la rama feature (`sc-datalake-niif-configurador-mallas`), pensado para reemplazar a las seis por malla. Cuál queda: por confirmar ([[preguntas-abiertas]]).

## Entradas y salidas
- Entra: evento con `publicar`, `ejecutar`, `periodoNegocio` (YYYYMM, opcional), `modoEjecucion` y `origen`. El periodo real sale de la programación (PRG); si el evento trae otro, falla. `ejecutar` exige `publicar`.
- Sale: resumen (estado, periodo, comparaciones, publicados, ARN de la ejecución). Escribe los JSON activos en S3 y un registro de ejecución en DynamoDB.

## Recursos AWS que toca
- DynamoDB: `niif-{env}-cnf-mallas` (lee 7 registros y persiste el periodo), `niif-{env}-prs-estructuras`, `niif-{env}-prg-programaciones`, `niif-{env}-ejc-ejecuciones` (escribe).
- S3: lee plantillas y escribe los archivos activos; las URIs vienen de CNF (bucket `niif-{env}-assets`, por confirmar).
- Step Functions `niif-{env}-primas-endosos`. Función `niif-{env}-configuration-primas-endosos`.

## Código
- `src/conf_endosos.py` (~890 líneas, un solo archivo): `lambda_handler`, `consultar_cnf`, `construir_extraccion/validacion/scd/remediacion`, `comparar`, `aplicar_periodo_runtime`, `_evento_desde_prg`. Exige 7 registros CNF y 2 ciclos.
- Variables: `CNF_TABLE`, `PRS_TABLE`, `PRG_TABLE`, `EJC_TABLE`, `MALLA_PK`, `PRG_PK`, `STATE_MACHINE_ARN`. Casi idéntica a las otras cinco; cambian constantes y conteos.
- No es hexagonal; el handler es `conf_endosos.lambda_handler` (no `lambda_function.py` ni `test/`, que pide [[est-gobierno-devops-niif]]). Pruebas: `tests/test_lambda_function.py`, unas 36 con mocks, umbral 70 %.

## Estado
- En Alfa: `origin/quality`, commit `616de5e` (2026-10-20). Jenkins `lambdaS3Pipeline` sube un ZIP a `niif-{env}-lambda-artifacts` y actualiza la función en dev, qa y prd; la IaC la crea en `iac-analytics-datalake-niif-operations-manager`. Si está desplegada: por confirmar.
- `quality` frente a `main` (medido en GitHub el 2026-11-12): 0 commits solo en `quality`, 2 solo en `main`, 0 archivos distintos; mismo contenido que `main` (los commits de diferencia son merges); la base `quality` sirve. Ver ARQ-50 en [[auditoria-repos-2026-11]].
- Código nuevo nuestro: ninguno aquí; el reemplazo es la rama `feature/T2-configurador-mallas` de `sc-datalake-niif-configurador-mallas`.

## Brechas y relacionadas
- Sin brechas con ID propias en las notas; aplica el diagnóstico de calidad de los configuradores en [[inventario-lambdas]] (copias con 86 % a 98 % de similitud, sin pruebas en lo desplegado entonces).
- Relacionado: [[inventario-lambdas]], [[familia-mallas-alfaprd]], [[data-datalake-niif-primas-endosos-glue-job]]
