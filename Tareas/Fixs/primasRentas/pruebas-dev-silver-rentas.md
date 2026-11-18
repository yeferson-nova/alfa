# Pruebas en DEV del fix de Primas Rentas

Guía para probar en DEV el fix del job `niif-dev-silver-primas-rentas` (rama `fix/primas-rentas-periodo-expedicion` de `alfa-repos`, commit `172b17c`). El fix deja en Silver solo las rentas expedidas en el periodo, igual que el SQL de arquitectura V2.1 (401 en lugar de 75.181 en 202606).

Script: `probar-silver-rentas-dev.sh` (esta carpeta). Solo corre en DEV.

## Antes de empezar

1. Entrar a la consola de AWS de DEV y abrir CloudShell (región us-east-1).
2. Subir dos archivos con **Actions > Upload file**:
   - `probar-silver-rentas-dev.sh` (esta carpeta).
   - `silver-primas-rentas.py` de `alfa-repos\glue\data-datalake-niif-primas-rentas-glue-job\src\jobs\`, con la rama `fix/primas-rentas-periodo-expedicion` activa.
3. Comprobar que están los dos:

```bash
ls -l probar-silver-rentas-dev.sh silver-primas-rentas.py
grep -c 'col("r.FEC_EXPEDICION").between(' silver-primas-rentas.py
```

El `grep` debe dar `1`.

## Paso 1. Revisar el estado actual

```bash
bash probar-silver-rentas-dev.sh script
bash probar-silver-rentas-dev.sh foto
```

| Qué mirar | Qué debe salir |
|---|---|
| `El script desplegado ... el fix` | `NO trae el fix` (todavía no se sube) |
| `periodo_negocio` | El periodo de la última corrida de Rentas en DEV |
| `estado_validacion` | `SUCCESS` o `FAILED` |
| `estado_remediacion`, `estado_disponibilizacion_silver` | `SUCCESS` |
| `total_registros_silver_preparada` | Total con el job viejo (anotarlo) |
| Gold activos por periodo | Activos y expedidas del periodo (anotarlos) |

Si `parametros_extraccion.json` no existe o los estados no están en `SUCCESS`, primero hay que correr la malla de Rentas en DEV; el job de Silver no arranca sin eso.

## Paso 2. Respaldar, subir el fix y correr Silver

```bash
bash probar-silver-rentas-dev.sh todo
```

Hace, en orden: revisa el script, toma la foto, respalda (script, parámetros, Silver preparada, Gold y archivo de control), sube el script nuevo y corre el job de Silver. Si el respaldo falla, se detiene.

**Anotar la ruta del respaldo** que imprime (`Respaldo en s3://niif-dev-assets/primas-rentas/respaldos_fix_rentas/<fecha>/`); se usa para volver atrás.

| Qué mirar | Qué debe salir |
|---|---|
| `El script desplegado TRAE el fix.` | Después de subir |
| `Estado:` | `SUCCEEDED` |
| `total_registros_silver_preparada` | Menor que el del paso 1: solo las rentas expedidas en el periodo |

## Paso 3. Llevar a Gold

```bash
bash probar-silver-rentas-dev.sh scd
```

- `estado_gold_scd: SUCCESS`: seguir al paso 4.
- `motivo_gold_scd: SCD_APAGADO_MASIVO ...`: es lo esperado si Gold tenía las rentas que sobran del mismo periodo. Revisar que el número que se apagaría sea activos del paso 1 menos el total del paso 2, y liberar el freno una sola vez:

```bash
PERMITIR=true bash probar-silver-rentas-dev.sh scd
```

## Paso 4. Actualizar el catálogo y verificar

```bash
bash probar-silver-rentas-dev.sh crawler
bash probar-silver-rentas-dev.sh foto
```

| Qué mirar | Qué debe salir |
|---|---|
| Gold activos del periodo | Igual a `total_registros_silver_preparada` del paso 2 |
| Expedidas en el periodo | Igual a los activos (todas las activas se expidieron en el periodo) |
| Llaves duplicadas activas | `0` |
| `control_periodo_scd` | El periodo probado |

Guardar la salida de los pasos 1 a 4 en `EvidenciasAws\dev-pruebas-fix-rentas\` (fuera de Git).

## Volver atrás

```bash
bash probar-silver-rentas-dev.sh restaurar s3://niif-dev-assets/primas-rentas/respaldos_fix_rentas/<fecha>
bash probar-silver-rentas-dev.sh crawler
bash probar-silver-rentas-dev.sh script
```

Restaura el script viejo, `parametros_extraccion.json`, Silver preparada, Gold y el archivo de control. `script` debe decir `NO trae el fix`.

## Comandos del script

| Comando | Qué hace |
|---|---|
| `script` | Muestra el script desplegado y si trae el fix |
| `foto` | Parámetros, archivo de control y Gold en Athena |
| `respaldo` | Copia script, parámetros, Silver, Gold y control a `respaldos_fix_rentas/<fecha>/` |
| `subir` | Sube `silver-primas-rentas.py` (solo si trae el fix) |
| `silver` | Corre `niif-dev-silver-primas-rentas` |
| `scd` | Corre `niif-dev-scd`; con `PERMITIR=true` libera el freno |
| `crawler` | Corre `niif-dev-crawler-gold` |
| `restaurar <ruta>` | Vuelve todo al respaldo |
| `todo` | `script`, `foto`, `respaldo`, `subir` y `silver` |

Variables opcionales: `SCRIPT_LOCAL` (ruta del `.py`, por defecto `./silver-primas-rentas.py`), `WG` (workgroup de Athena, por defecto `primary`), `OUT` (salida de Athena si el workgroup no la tiene).
