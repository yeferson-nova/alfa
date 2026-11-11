# Evidencias de AWS

Salidas de los scripts de CloudShell (solo lectura) y capturas sacadas de las cuentas de AWS. **Nada de esta carpeta se versiona** salvo este archivo: trae ARN con número de cuenta, correos y datos de ejecución.

## Convención
- Una carpeta por extracción: `<ambiente>-<tema>\` (ambiente: `dev`, `qa`, `prd`), sin fecha en el nombre. La fecha va en esta tabla.
- Dentro, la salida tal como sale del script, sin editar. Si hace falta un resumen, va en `Docs\` (humanos) o `MemoriaIa\` (IA), citando la carpeta.
- Los scripts que generan la evidencia viven en `Backend\cloudshell-inventario\` (o junto al tema, como `errores\`).
- La evidencia de producción no se comparte fuera del equipo ni se sube a servicios externos.

## Contenido
| Carpeta | Script | Qué tiene |
|---|---|---|
| `dev-uso-recursos\` (2026-11-10) | `Backend\cloudshell-inventario\evidencia-uso-dev.sh` | Uso real (90 días) de lambdas, Glue, Step Functions, streams, API Gateway + WAF. Análisis en `Docs-brechas\limpieza-dev-recursos.md` |
| `dev-inventario-cuenta\` (2026-11-03) | `Backend\cloudshell-inventario\inventario.sh` | Extracción completa de DEV (lambdas, config, IAM, S3, SNS, logs). Antes en `Backend\inventario-archivo\` |
| `dev-inventario-lambdas\` (2026-11-03) | `inventario.sh` (fase 1) | Lista de las 65 lambdas con runtime y fechas. Antes en `Backend\Lamnbdas\inventario-aws\extraccion\` |
| `prd-primas-arl-202606\subida-equipo\` (2026-11-05) | `erroresecolectar-evidencia-prd.sh` | Evidencia de prod del incidente Primas ARL 202606 que subió el equipo (antes en `errores\evidencia-primas-arl-202606\`, versionada) |
| `prd-primas-arl-202606ecolectada\` (2026-11-05) | `erroresecolectar-evidencia-prd.sh` | El mismo paquete tal como salió de CloudShell, con `LOG.txt` y el `.tar.gz` (antes en `errores\Evidencia\`) |
