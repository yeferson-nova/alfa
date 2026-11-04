# Tópicos SNS

Movidos el 2026-11-03 desde `Backend\inventario\sns\`. Cada tópico tiene `.attrs.json` (política y atributos) y `.subs.json` (suscriptores; contiene correos corporativos: no publicar).

## Tópicos de trabajo (3)
| Tópico | Uso |
|---|---|
| `niif-dev-certification-email` | Sale el correo de certificación |
| `ALFA-DEV-DATALAKE-Critical` | Alertas críticas de `PipelineObserver` (base de la T1) |
| `ALFA-DEV-DATALAKE-Warning` | Alertas de advertencia de `PipelineObserver` |

## `pruebas-sns\`
`datalake-dev-niif-certification-email` (versión anterior), Control Tower, CloudGuard y `GFT-Notifications-Budgets`, más `topics.json` (lista completa) y `sqs-colas.json`.
