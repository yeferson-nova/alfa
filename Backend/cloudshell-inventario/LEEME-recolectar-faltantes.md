# Recolectar lo que falta (CloudShell)

Script: `recolectar-faltantes.sh`. **Solo lectura**: describe, lista y descarga; no crea, modifica ni borra nada en AWS. Trae lo que falta para el roadmap (`Tareas\roadmap-cambios.md`, sección 7).

## Pasos

1. Abrir AWS CloudShell en la cuenta `533267035849`, región **us-east-1**.
2. **Actions → Upload file** → `recolectar-faltantes.sh`.
3. Ejecutar:
   ```bash
   bash recolectar-faltantes.sh
   ```
   Tarda unos minutos (descarga 18 lambdas y consulta Logs Insights). Para repetir solo una parte:
   ```bash
   bash recolectar-faltantes.sh lambdas ses
   ```
   Secciones: `lambdas s3 ses dynamo scheduler iam glue sfn athena logs buckets metricas`.
4. Revisar en pantalla el **resumen rápido** (también queda en `_resumen.txt`).
5. **Actions → Download file** → `/home/cloudshell-user/niif-faltantes.zip`.
6. Descomprimir en `alfa\Backend\cloudshell-inventario\resultados\<fecha>\` y avisar para analizarlo.

## Qué trae

| Carpeta | Contenido |
|---|---|
| `lambdas\codigo\` | Solo el código propio de cada lambda (sin pandas, numpy ni boto3, que fueron las que llenaron el disco la vez anterior). Las lambdas que son imagen de contenedor quedan como `*.IMAGEN.txt` |
| `lambdas\config\` | Configuración, triggers, permisos, etiquetas, alias y destinos. **Sin la URL prefirmada** del código |
| `s3\` | JSON y plantillas de configuración de `niif-dev-assets` (menores de 1 MB) |
| `ses\` | Estado de la cuenta (sandbox, cuota), identidades y conjuntos de configuración |
| `dynamo\` | Definición de las tablas; registros de `cnf-mallas`, `prg-programaciones`, `pipeline-estado`, `ejc-ejecuciones` y grupos compuestos. De `notification-certification` solo un conteo por estado |
| `scheduler\` | *Schedules* de los configuradores y a qué función apuntan |
| `iam\` | Políticas de los roles de lambdas, jobs, Step Functions, crawler y Scheduler |
| `glue\`, `sfn\` | Definición vigente y últimas ejecuciones (tiempos, DPU, errores) |
| `athena\` | Bytes escaneados de las últimas 300 consultas del grupo `primary` |
| `metricas\` | Costo y duración de los últimos 30 días según `PipelineObserver` |
| `buckets\`, `logs\` | Existencia, cifrado, ciclo de vida, notificaciones y retención |

## Cuidado
- El zip trae **correos corporativos** (destinatarios en los JSON) y variables de entorno: no subirlo a un repositorio ni compartirlo fuera del equipo.
- Si alguna llamada no tiene permiso, el script sigue y la anota en `_errores.log`.
- Si CloudShell se queda sin espacio, borrar `~/niif-faltantes` y repetir por secciones.
