---
tipo: repo
estado: vigente
actualizado: 2026-11-12
repo: sc-datalake-niif-conf-rsm-rvi
categoria: lambda
paso: "2 · Bodega NIIF 17 (configura y arranca la malla)"
malla: reserva-matematica-rvi
---
# sc-datalake-niif-conf-rsm-rvi
> Lambda configuradora de la malla Reserva Matemática RVI: reconstruye los JSON de configuración desde DynamoDB, los valida contra las plantillas de S3, los publica y puede iniciar la Step Function.

## Rol en el flujo
- La invoca EventBridge Scheduler (el `manager-configuration` crea los schedules desde el stream de `niif-{env}-prg-programaciones`); no la dispara un stream. Es la `configuration-<malla>` de [[inventario-lambdas]].
- Entrega a la Step Function `niif-{env}-reserva-matematica-rvi`, que corre los jobs de Glue de la malla (job de Glue de reserva RVI; ficha por escribir).
- Duplicidad: hay una lambda por malla (esta), una general (`sc-datalake-niif-configuration-general`, que embebe este mismo módulo) y el configurador T2 de la rama feature (`sc-datalake-niif-configurador-mallas`), pensado para reemplazar a las seis por malla. Cuál queda: por confirmar ([[preguntas-abiertas]]).

## Entradas y salidas
- Entra: evento con `publicar`, `ejecutar`, `periodoNegocio` (YYYYMM, opcional), `modoEjecucion` y `origen`. El periodo real sale de la programación (PRG); si el evento trae otro, falla. `ejecutar` exige `publicar`.
- Sale: resumen (estado, periodo, comparaciones, publicados, ARN de la ejecución). Escribe los JSON activos en S3 y un registro de ejecución en DynamoDB.

## Recursos AWS que toca
- DynamoDB: `niif-{env}-cnf-mallas` (lee 9 registros y persiste el periodo), `niif-{env}-prs-estructuras`, `niif-{env}-prg-programaciones`, `niif-{env}-ejc-ejecuciones` (escribe).
- S3: lee plantillas y escribe los archivos activos; las URIs vienen de CNF (bucket `niif-{env}-assets`, por confirmar).
- Step Functions `niif-{env}-reserva-matematica-rvi`. Función `niif-{env}-configuration-reserva-matematica-rvi`.

## Código
- `src/conf_res_mat_rvi.py` (~890 líneas, un solo archivo): `lambda_handler`, `consultar_cnf`, `construir_extraccion/validacion/scd/remediacion`, `comparar`, `aplicar_periodo_runtime`, `_evento_desde_prg`. Exige 9 registros CNF y 4 ciclos. Particularidad: emite `where` y `dependencia` solo si existen (igual que ARP).
- Variables: `CNF_TABLE`, `PRS_TABLE`, `PRG_TABLE`, `EJC_TABLE`, `MALLA_PK`, `PRG_PK`, `STATE_MACHINE_ARN`. Casi idéntica a las otras cinco; cambian constantes y conteos.
- No es hexagonal; el handler es `conf_res_mat_rvi.lambda_handler` (no `lambda_function.py` ni `test/`, que pide [[est-gobierno-devops-niif]]). Pruebas: `tests/test_lambda_function.py`, unas 37 con mocks, umbral 70 %.

## Estado
- En Alfa: `origin/quality`, commit `4f7f778` (2026-10-20). Jenkins `lambdaS3Pipeline` sube un ZIP a `niif-{env}-lambda-artifacts` y actualiza la función en dev, qa y prd; la IaC la crea en `iac-analytics-datalake-niif-operations-manager`. Si está desplegada: por confirmar.
- Código nuevo nuestro: ninguno aquí; el reemplazo es la rama `feature/T2-configurador-mallas` de `sc-datalake-niif-configurador-mallas`.

## Brechas y relacionadas
- Sin brechas con ID propias en las notas. Su job de Glue (`data-datalake-niif-reserva-matematica-rvi-glue-job`) aún no tiene ficha.
- Relacionado: [[inventario-lambdas]], [[familia-mallas-alfaprd]]
