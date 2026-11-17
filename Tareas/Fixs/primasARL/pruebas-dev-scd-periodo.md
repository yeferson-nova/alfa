# Pruebas en DEV: fix de Primas ARL completo

Guía para probar en DEV, con Primas ARL, los tres cambios:

| Cambio | Dónde | Qué hace |
|---|---|---|
| Lotes de respaldo (fix inprod) | `extraccion_estandar_optimized.py` | Al excluir llaves ya extraídas, Silver lee los lotes de respaldo más el actual |
| Freno (fix inprod) | `scd.py` | Si se fuera a apagar más del 20 % de Gold, falla antes de escribir |
| SCD-01 (hoy) | `scd.py` | Si Gold existe y no se puede leer, falla sin tocarlo |
| Periodo activo (hoy) | `scd.py` | Un solo periodo activo; cambio de periodo sin freno; merge en el mismo periodo |

Se usa `probar-scd-periodo-dev.sh` en CloudShell de DEV:

```bash
export AMB=dev
export JOB_SCD=niif-dev-scd
```

## 0. Antes de empezar
| Paso | Comando | Qué debe dar |
|---|---|---|
| 0.1 Script del SCD | `bash probar-scd-periodo-dev.sh script` | "trae la regla de periodo" |
| 0.2 Script de extracción | `aws glue get-job --job-name niif-dev-extraccion-estandar-optimized --query Job.Command.ScriptLocation` y buscar `combinar_lotes_respaldados` en ese archivo | La función existe |
| 0.3 Filtro de ARL | `config_extraccion_primas_arl.json` en DEV | `PERIODO = 'P'`. Con `<=` todos los periodos quedan activos y la parte de periodo no sirve |
| 0.4 Foto inicial | `bash probar-scd-periodo-dev.sh foto > foto-0.txt` | Control "no existe"; anotar activos de Gold |
| 0.5 Respaldo | `bash probar-scd-periodo-dev.sh respaldo` | Anotar la ruta del respaldo |

## 1. Escenarios, en este orden
P es un periodo distinto del que hoy está activo en Gold; P+1 es el siguiente.

| # | Prueba | Cómo | Qué debe dar |
|---|---|---|---|
| 1 | SCD-01, Gold ilegible | `gold-ilegible poner`, luego `scd`, luego `gold-ilegible quitar` | Job `FAILED` con `SCD_GOLD_ILEGIBLE`; `foto` igual a la inicial |
| 2 | Freno, primera corrida sin control | Step Function `niif-dev-primas-arl` con P | SCD `FAILED` con `SCD_APAGADO_MASIVO`; Gold igual; control no existe |
| 3 | Freno liberado | `PERMITIR=true bash probar-scd-periodo-dev.sh scd` | `SUCCEEDED`; control con P; solo P con `act_reg = 1` |
| 4 | Lotes de respaldo y mismo periodo | Step Function con P otra vez | `lotes_segmento` con 2 o más lotes (o solo los de respaldo si Oracle no trae nada nuevo); `cambio_de_periodo: false`; apagados 0; activos iguales o más |
| 5 | Freno en el mismo periodo | `bash probar-scd-periodo-dev.sh freno-mismo-periodo` | `FAILED` con `SCD_APAGADO_MASIVO`; Gold igual; al final restaura `parametros_extraccion.json` y borra la entrada parcial |
| 6 | Cambio de periodo | Step Function con P+1 | `SUCCEEDED` sin permiso; `cambio_de_periodo: true`; P con `act_reg = 0`, P+1 activo |
| 7 | Volver a un periodo ejecutado | Step Function con P | `SUCCEEDED`; `total_reactivados_desde_historico` mayor que 0, con `vrs_reg` 2; P+1 inactivo |

Después de cada paso: `bash probar-scd-periodo-dev.sh foto > foto-<n>.txt`.

Notas:
- El paso 5 necesita que Silver preparada tenga 2 o más archivos parquet; si tiene uno, el script lo avisa y no hace nada. La entrada parcial va en `_prueba_freno/`, que Spark no lee en la ruta normal.
- En el paso 4, la parte "datos nuevos más respaldo" solo se ve si Oracle trae endosos nuevos de P entre corridas. Si no, ese caso queda probado en local (`pruebas\`, cadena ARL).
- Si en el paso 3 aparece `AccessDenied` sobre `niif/_control_scd/`, DEV no tiene ese permiso: Gold ya quedó escrito. Restaurar y pedir el permiso a DevOps.

## 2. Al terminar
- Dejar DEV como estaba: `bash probar-scd-periodo-dev.sh restaurar <ruta del respaldo>` (restaura Gold y borra el control).
- Guardar las fotos en `EvidenciasAws\` (fuera de Git).
