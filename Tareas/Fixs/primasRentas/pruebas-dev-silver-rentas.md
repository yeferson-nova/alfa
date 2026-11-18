# Pruebas en DEV del fix de Primas Rentas

Guía para probar en DEV el fix del job `niif-dev-silver-primas-rentas` (rama `fix/primas-rentas-periodo-expedicion` de `alfa-repos`, commit `172b17c`). El fix deja en Silver solo las rentas expedidas en el periodo, igual que el SQL de arquitectura V2.1 (401 en lugar de 75.181 en 202606).

El script del job **no se sube a mano**: llega a DEV por PR a `develop` y Jenkins lo despliega en `s3://niif-dev-assets/glue-resources/primas-rentas/`. El script de pruebas `probar-silver-rentas-dev.sh` (esta carpeta) solo comprueba el despliegue, respalda los datos, corre los jobs y verifica. Solo corre en DEV.

## Resultado de la ejecución del 2026-11-18

Log completo: `log de consola.txt` (esta carpeta).

| Paso | Resultado |
|---|---|
| 0. Despliegue | Hecho. `script` dice `El script desplegado TRAE el fix.` (`glue-resources/primas-rentas/silver-primas-rentas.py`) |
| 1. Estado inicial | Última corrida de Rentas en DEV del 2026-07-01, periodo `202605`, lote `1782948057`. Todos los estados en `SUCCESS`. Silver preparada 74.201 con el job viejo. El SCD reportó 74.201 insertados en Gold, pero Athena no devuelve filas en `scd_primas_rentas`. Sin archivo de control. 0 llaves duplicadas |
| 2. Respaldo | Hecho en `s3://niif-dev-assets/primas-rentas/respaldos_fix_rentas/20261118T161348Z/` (sin archivo de control, no existía) |
| 2. Silver | **FAILED** antes de procesar: `AnalysisException: Path does not exist: s3://alfa-datalake-dev-silver/niif/ALFAPRD/general/alfaprd_general_anp_movimientos_pago` |
| 3 y 4 | No se ejecutaron |

**Lectura:** el fallo no es del fix. El job no encontró la Silver base de la corrida de julio (la carpeta de `anp_movimientos_pago` ya no existe en DEV), así que falló al leer, antes de escribir: Silver preparada, Gold y `parametros_extraccion.json` quedaron como estaban (el JSON sigue mostrando 74.201 de la corrida anterior). No hace falta restaurar.

**Para seguir:** correr la malla completa de Rentas en DEV con el fix ya desplegado. Regenera la Silver base y corre Silver, SCD y crawler.

| Recurso | Nombre en DEV |
|---|---|
| Step Function | `niif-dev-primas-rentas` |
| Lambda que la arranca | `niif-dev-configuration-primas-rentas` (lee el periodo de la programación `PROGRAMACION#PRIMAS_RENTAS`, publica `config_extraccion_primas_rentas.json` y arranca la Step Function) |

Forma normal, por la lambda (toma el periodo programado):

```bash
export AWS_PAGER=""
aws lambda invoke --function-name niif-dev-configuration-primas-rentas --payload '{}' \
  --cli-binary-format raw-in-base64-out /tmp/respuesta.json && cat /tmp/respuesta.json
```

Seguir la ejecución:

```bash
SF_ARN=$(aws stepfunctions list-state-machines --query "stateMachines[?name=='niif-dev-primas-rentas'].stateMachineArn" --output text)
aws stepfunctions list-executions --state-machine-arn "$SF_ARN" --max-items 3 \
  --query 'executions[].[name,status,startDate]' --output table
```

Cuando termine en `SUCCEEDED`:

```bash
bash probar-silver-rentas-dev.sh foto
```

Si el SCD frena por el apagado de las rentas que sobran, correr `PERMITIR=true bash probar-silver-rentas-dev.sh scd`, luego `crawler` y `foto`. Esperado: en el periodo, activos de Gold iguales a `total_registros_silver_preparada`, todos expedidos en el periodo, y 0 duplicados.

### Segunda corrida: malla completa (2026-11-18, 14:28 a 15:33 hora Colombia)

Antes, la ejecución `279cd303` había fallado en la extracción: el rol `niif-dev-extraccion-estandar-optimized` no tenía `glue:GetConnection` sobre `datalake-dev-alfaprd-dev-connection`. El IaC lo declara, pero lo aplicado en DEV no lo tenía. Corregido el permiso, se lanzó la ejecución `ad0b75f2` de `niif-dev-primas-rentas` desde la consola.

| Paso de la Step Function | Duración | Resultado |
|---|---|---|
| ExtraccionEstandar | 48 min 48 s | OK |
| CrawlerBronze | 4 min 26 s | OK |
| ValidacionTecnica | 1 min 57 s | OK (`estado_validacion: FAILED` en el JSON, no bloqueante: siguió a remediación) |
| Remediacion | 3 min 07 s | OK |
| SilverJob (con el fix) | 2 min 56 s | OK |
| CrawlerSilver | 1 min 01 s | OK |
| GoldSCD | 2 min 11 s | OK, sin freno |
| CrawlerGold | 41 s | OK |
| SuccessEsperandoComposite | | **FAILED**: `$.ejcPK` no existe porque se lanzó desde la consola con entrada `{}`. No afecta los datos; lanzada por la lambda no pasa |

Foto después de la corrida:

| Qué mirar | Antes (2026-07-01, job viejo) | Después (fix) |
|---|---|---|
| `periodo_negocio` | `202605` | `202605` |
| `lote_cargue_id` | `1782948057` | `1795030139` |
| `total_registros_silver_preparada` | 74.201 | **770** |
| Gold activos del periodo | sin filas en Athena | 770 |
| Expedidas en el periodo | | 770 (todas) |
| Llaves duplicadas activas | 0 | 0 |
| Archivo de control del SCD | no existía | `periodo_negocio: 202605` |
| `metricas_gold_scd` | | 770 insertados nuevos, 0 apagados (Gold estaba vacío) |

**Lectura:** el fix hace lo esperado en DEV: Silver deja solo las rentas expedidas en el periodo y una fila por renta, y Gold queda igual a Silver.

### Tercera corrida: cuadre con el SQL en 202606 (2026-11-18, 15:48 a 16:26 hora Colombia)

La base NIIF 17 de DEV tiene los mismos datos de PRD en 202606, así que se corrió ese periodo para comparar con el SQL de arquitectura V2.1 (401). La lambda toma el periodo de PRG (`niif-dev-prg-programaciones`, `PROGRAMACION#PRIMAS_RENTAS`), que estaba en `202512`: se cambió a `202606` con condición sobre el valor anterior y se lanzó la lambda con `{"publicar":true,"ejecutar":true}`. Ejecución `rentas-202606-auto-20261118T204806Z-7b63db5f`: **SUCCEEDED** en 38 min 23 s (por la lambda, el registro final en `niif-dev-ejc-ejecuciones` funciona).

| Qué mirar | Resultado |
|---|---|
| `periodo_negocio` | `202606` |
| `total_registros_silver_preparada` | **401, igual al SQL V2.1** (PRD con el job anterior: 75.181) |
| Gold activos | solo `202606`: 401, todas expedidas en el periodo |
| Llaves duplicadas activas | 0 |
| `metricas_gold_scd` | 401 insertados, 770 de 202605 apagados, sin freno |
| `control_periodo_scd` | `cambio_de_periodo: true`, anterior `202605` |

**Conclusión:** el fix cuadra con el SQL de arquitectura y el SCD deja un solo periodo activo. Listo para el PR `develop` → `quality`.

Pendientes de DEV:
- Devolver PRG a `202512` (`update-item` con `:p` = `202512` y condición `:v` = `202606`).
- `estado_validacion: FAILED` en las dos corridas (no bloqueante, siguió a remediación); no lo causa el fix, revisar qué regla falla.
- La lambda desplegada lee PRG y exige `publicar` y `ejecutar` en `true`; lanzada desde la consola con `{}` la Step Function usa el último JSON publicado y termina en `FAILED` en `SuccessEsperandoComposite` por falta de `$.ejcPK`.

## Paso 0. Despliegue por PR (antes de las pruebas)

1. Push de la rama `fix/primas-rentas-periodo-expedicion` al repo `data-datalake-niif-primas-rentas-glue-job`.
2. PR a `develop`, revisión y merge.
3. Esperar que el pipeline de Jenkins termine en verde.

## Antes de empezar

1. Entrar a la consola de AWS de DEV y abrir CloudShell (región us-east-1).
2. Subir `probar-silver-rentas-dev.sh` con **Actions > Upload file**.

## Paso 1. Revisar el despliegue y el estado actual

```bash
bash probar-silver-rentas-dev.sh script
bash probar-silver-rentas-dev.sh foto
```

| Qué mirar | Qué debe salir |
|---|---|
| `El script desplegado ... el fix` | `TRAE el fix`. Si dice `NO trae el fix`, Jenkins no ha desplegado: volver al paso 0 |
| `periodo_negocio` | El periodo de la última corrida de Rentas en DEV |
| `estado_validacion` | `SUCCESS` o `FAILED` |
| `estado_remediacion`, `estado_disponibilizacion_silver` | `SUCCESS` |
| `total_registros_silver_preparada` | Total de la última corrida, con el job viejo (anotarlo) |
| Gold activos por periodo | Activos y expedidas del periodo (anotarlos) |

Si `parametros_extraccion.json` no existe o los estados no están en `SUCCESS`, primero hay que correr la malla de Rentas en DEV; el job de Silver no arranca sin eso.

## Paso 2. Respaldar y correr Silver

```bash
bash probar-silver-rentas-dev.sh todo
```

Hace, en orden: comprueba que el script desplegado trae el fix (si no, se detiene), toma la foto, respalda los datos (parámetros, Silver preparada, Gold y archivo de control) y corre el job de Silver. Si el respaldo falla, se detiene.

**Anotar la ruta del respaldo** que imprime (`Respaldo en s3://niif-dev-assets/primas-rentas/respaldos_fix_rentas/<fecha>/`); se usa para volver atrás.

| Qué mirar | Qué debe salir |
|---|---|
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

Datos:

```bash
bash probar-silver-rentas-dev.sh restaurar s3://niif-dev-assets/primas-rentas/respaldos_fix_rentas/<fecha>
bash probar-silver-rentas-dev.sh crawler
```

Restaura `parametros_extraccion.json`, Silver preparada, Gold y el archivo de control.

Código: el script del job se revierte por PR a `develop` (revert del merge) y Jenkins vuelve a desplegar. No se cambia a mano en S3.

## Comandos del script

| Comando | Qué hace |
|---|---|
| `script` | Muestra el script desplegado y si trae el fix |
| `foto` | Parámetros, archivo de control y Gold en Athena |
| `respaldo` | Copia parámetros, Silver, Gold y control a `respaldos_fix_rentas/<fecha>/` |
| `silver` | Corre `niif-dev-silver-primas-rentas` |
| `scd` | Corre `niif-dev-scd`; con `PERMITIR=true` libera el freno |
| `crawler` | Corre `niif-dev-crawler-gold` |
| `restaurar <ruta>` | Vuelve los datos al respaldo |
| `todo` | `script`, `foto`, `respaldo` y `silver` |

Variables opcionales: `WG` (workgroup de Athena, por defecto `primary`), `OUT` (salida de Athena si el workgroup no la tiene).
