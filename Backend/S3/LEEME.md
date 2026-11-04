# Buckets S3

Movidos el 2026-11-03 desde `Backend\inventario\s3\`. Por bucket hay `.notificaciones.json` (eventos hacia lambdas, SNS o SQS) y, si existe, `.versionado.json`.

## Buckets del pipeline (8)
| Bucket | Qué guarda (por las rutas de CNF y las Step Functions) |
|---|---|
| `niif-dev-assets` | JSON de configuración de cada malla, plantillas y scripts de Glue |
| `alfa-datalake-dev-bronce` / `-silver` / `-gold` | Capas de datos del pipeline |
| `alfa-datalake-dev-niif-certification` | Certificación |
| `alfa-datalake-dev-niif-transport` | Archivos certificados listos para transporte (SFTP a FIS) |
| `niif-dev-certification` | Certificación (por confirmar si se usa o es la versión nueva) |
| `niif-dev-lambda-artifacts` | Paquetes de las lambdas |

## `pruebas-s3\` (64 archivos)
Buckets de otros proyectos, de pruebas, de la plataforma (CloudFormation, Athena, SSM) y los demás `niif-dev-*` (`landing`, `accountingtool`). La asignación a trabajo o pruebas se hizo por nombre; **está por validar** con el equipo.
