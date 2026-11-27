---
tipo: repo
estado: vigente
actualizado: 2026-11-26
repo: sc-datalake-niif-manager-configuration
categoria: lambda
paso: "2 · Bodega NIIF 17 (programa las configuradoras)"
malla: transversal
---
# sc-datalake-niif-manager-configuration
> Lambda que crea o actualiza en EventBridge Scheduler la programación de cada malla para invocar su configuradora. El código de `quality` pide una variable que la IaC de `quality` no define.

## Rol en el flujo
- La dispara el stream de `niif-{env}-prg-programaciones` (registros `activo = true` y `scheduleEstado = PENDIENTE`); también admite evento directo con `codigoMalla` y `accion` (`VALIDAR` o `SINCRONIZAR`).
- Cada schedule invoca la configuradora de la malla ([[sc-datalake-niif-primas-arl]] y las demás; ver [[inventario-lambdas]]).

## Entradas y salidas
- Entra: item `PROGRAMACION#{codigoMalla}` / `METADATA` con `scheduleName`, `scheduleExpression`, `scheduleTimezone`, `periodoNegocio`, `scheduleState` (por defecto `DISABLED`) y `modoEjecucion`.
- Sale: schedule con `publicar` y `ejecutar` en `true`, `periodoNegocio`, `modoEjecucion` y `origen = PROGRAMACION_GESTOR` (2 reintentos, 1 h); marca el item `PROGRAMADO`. No envía ID de ejecución ([[modularizacion-extraccion]]). Las expresiones `at(...)` se borran al terminar.

## Recursos AWS que toca
- DynamoDB (`GetItem`, `UpdateItem`) y EventBridge Scheduler, grupo `niif-{env}-programacion-configuradores`. La declara `conf_manager` en [[iac-analytics-datalake-niif-operations-manager]].
- Variables que lee: `AWS_REGION`, `ACCOUNT_ID`, `PRG_TABLE`, `SCHEDULER_ROLE_ARN`, `SCHEDULE_GROUP`, `CONFIGURADORES_JSON`.

## Código
- `src/scheduler_configurators.py` (~545 líneas, un solo archivo). Pruebas en `tests/test_lambda_function.py`, umbral 70 %; el README cita un `tests/conftest.py` que no existe. No es hexagonal.

## Estado
- En Alfa: `origin/quality`, commit `4c2e249` (2026-10-09), traído el 2026-11-26. Si es lo desplegado y qué tiene `develop`: por confirmar.

## Brechas y relacionadas
- **Desajuste (propuesta, sin ID):** el código lee `CONFIGURADORES_JSON` al importar (`:30`), pero `conf_manager/parameters.tf` (IaC `quality` `77de48c`) define `CONFIGURADOR_GENERAL` y `CODIGOS_MALLA`; con ambos tal cual, la función fallaría al arrancar. Su mapa local apunta `EMISIONES` a `configuration-emisiones`, que no está declarada en la IaC.
- **Lote del stream (propuesta):** un registro inválido lanza error y afecta todo el lote (2 reintentos, `bisect`, sin destino de fallo).
- Relacionado: [[inventario-lambdas]], [[sc-datalake-niif-configuration-general]], [[cambios-cnf-mallas]]
