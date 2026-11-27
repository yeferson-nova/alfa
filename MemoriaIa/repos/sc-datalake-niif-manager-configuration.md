---
tipo: repo
estado: vigente
actualizado: 2026-11-27
repo: sc-datalake-niif-manager-configuration
categoria: lambda
paso: "2 · Bodega NIIF 17 (programa las configuradoras)"
malla: transversal
---
# sc-datalake-niif-manager-configuration
> Lambda que crea o actualiza en EventBridge Scheduler la programación de cada malla para invocar su configuradora. Código igual en `develop`, `quality` y `main` (2026-11-27).

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
- En Alfa (traído el 2026-11-27): `develop` `ad80f7c`, `quality` `4c2e249`, `main` `49dbffc`, con el mismo contenido. Si es lo desplegado: por confirmar.

## Brechas y relacionadas
- **Variable `CONFIGURADORES_JSON` (resuelto):** la IaC `quality` `77de48c` (2026-11-18) no la definía; desde `6baf99a` (2026-11-25), y en `develop` y `main`, `conf_manager/parameters.tf:42` sí. El código no lee `CONFIGURADOR_GENERAL` ni `CODIGOS_MALLA`.
- **EMISIONES (propuesta, sin ID):** el mapa apunta a `configuration-emisiones`; confirmar que esa lambda exista en cada ambiente.
- **Lote del stream (propuesta):** un registro inválido lanza error y afecta todo el lote (2 reintentos, `bisect`, sin destino de fallo).
- Relacionado: [[inventario-lambdas]], [[sc-datalake-niif-configuration-general]], [[cambios-cnf-mallas]]
