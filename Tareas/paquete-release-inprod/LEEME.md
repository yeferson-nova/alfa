# Paquete de release 1: fix de Primas ARL (extracción + freno del SCD)

Para: Carolina (sube al git de Alfa y despliega a QA).
Estado: probado en DEV el 2026-11-06. Pendiente de QA y PRD.

## 1. Qué es y por qué
En PRD, la malla de Primas ARL del periodo 202606 certificó con resumen y totales vacíos:
- La extracción incremental dejó la capa Silver con 209 registros en vez de ~1,2 millones.
- El job SCD (Gold) apagó por ausencia 1.203.789 registros.
- Las consultas de certificación quedaron en 0 filas.

Este paquete corrige las dos primeras causas. **No incluye** el correo con totales vacíos (eso va en la tarea de notificaciones).

## 2. Qué se entrega (2 archivos, sin infraestructura nueva)
| Archivo (carpeta del job) | Job de Glue | Qué cambia |
|---|---|---|
| `niif-dev-extraccion-estandar-optimized\extraccion_estandar_optimized.py` | `niif-<amb>-extraccion-estandar-optimized` | Cuando excluye llaves ya extraídas, conserva los lotes de respaldo más el actual (`lotes_segmento`). Así Silver lee todo y el SCD no apaga nada. |
| `niif-dev-scd\scd.py` | `niif-<amb>-scd` | Freno de apagado masivo: si el SCD fuera a apagar más del 20 % de los activos de Gold, el job **falla antes de escribir** y Gold queda intacto. El umbral es opcional. |

Hash SHA256 de los archivos entregados (también en `SHA256.txt`):
```
7b26b52891c370c002ea5bedde27db05b17d1076107091f308cde902f724e190  niif-dev-extraccion-estandar-optimized/extraccion_estandar_optimized.py
52a89a987dbcbff721ea589a4cd3eb2dea5de5cc27c4caf512d97db65640a72e  niif-dev-scd/scd.py
```
**Verifica los hashes** (desde esta carpeta, en PowerShell). Cada línea debe decir `OK`; si dice `DIFIERE`, el archivo cambió y no se despliega hasta aclararlo:
```powershell
Get-Content SHA256.txt | ForEach-Object {
  $h, $f = $_ -split ' \*', 2
  $real = (Get-FileHash $f -Algorithm SHA256).Hash.ToLower()
  '{0}  {1}' -f $(if ($real -eq $h) { 'OK     ' } else { 'DIFIERE' }), $f
}
```
Regla: si cualquier archivo cambia, se recalcula `SHA256.txt` en el mismo commit. Los archivos usan finales de línea CRLF; si Git los convierte en tu máquina y el hash difiere, compara el contenido con `fc.exe` contra una copia recibida directamente.

No cambia: `job.json`, la Step Function, permisos IAM, argumentos del job, configuraciones `config_*.json`. No hay argumentos nuevos obligatorios.

## 3. Importante antes de desplegar
1. **`scd.py` lo usan todas las mallas** (Primas Rentas, Endosos, ARL, Auxilios Funerarios, Reserva Matemática ARP y RVI). Desde el despliegue, cualquier corrida de esas mallas que apague más del 20 % de Gold se detendrá con el error `SCD_APAGADO_MASIVO`. Es el freno funcionando, no una falla nueva. Avisa a quien use QA.
2. **Confirmar si QA ya tiene la versión nueva de la extracción.** No sabemos si QA tiene el fix. Compara (paso 4.2) antes de reemplazar.
3. Los nombres de job y las rutas del script pueden variar por ambiente. Compruébalos con `get-job` (paso 4.1). En DEV: extracción en `s3://niif-dev-assets/glue-resources/estandar/extraccion_estandar_optimized.py` y SCD en `s3://niif-dev-assets/glue-resources/auxilios-funerarios/scd.py`.

## 4. Pasos en QA
Sustituye `AMB` por el ambiente (qa). Todos los nombres de abajo son los de DEV con el ambiente cambiado: confirma que existen.

**4.1 Averiguar de dónde lee cada job su script**
```powershell
aws glue get-job --job-name niif-AMB-extraccion-estandar-optimized --query "Job.Command.ScriptLocation" --output text
aws glue get-job --job-name niif-AMB-scd --query "Job.Command.ScriptLocation" --output text
```

**4.2 Respaldar y comparar lo que hay hoy** (con las rutas del paso anterior)
```powershell
aws s3 cp RUTA_EXTRACCION_QA extraccion.qa.original.py
aws s3 cp RUTA_SCD_QA scd.qa.original.py
fc.exe extraccion.qa.original.py niif-dev-extraccion-estandar-optimized\extraccion_estandar_optimized.py
fc.exe scd.qa.original.py niif-dev-scd\scd.py
```
Conserva `*.qa.original.py`: son la reversa. Si el de extracción ya es igual al entregado, no lo subas.

**4.3 Subir** (solo si hay diferencias esperadas)
```powershell
aws s3 cp niif-dev-extraccion-estandar-optimized\extraccion_estandar_optimized.py RUTA_EXTRACCION_QA
aws s3 cp niif-dev-scd\scd.py RUTA_SCD_QA
```

**4.3b Verificar lo desplegado** (el hash del script en QA debe ser igual al de `SHA256.txt`)
```powershell
aws s3 cp RUTA_EXTRACCION_QA extraccion.qa.desplegado.py
aws s3 cp RUTA_SCD_QA scd.qa.desplegado.py
Get-FileHash extraccion.qa.desplegado.py, scd.qa.desplegado.py -Algorithm SHA256 | Format-Table Hash, Path
```

**4.4 Probar en QA** (corrida normal de Primas ARL; luego una incremental si hay Gold previo)
Mira `s3://niif-AMB-assets/primas-arl/parametros_extraccion.json`:
- Cuando hubo exclusión de llaves: `segmentacion_horizontal.lotes_segmento` tiene **dos o más lotes** (antes tenía solo el actual).
- `estado_gold_scd` = `SUCCESS`.
- `metricas_gold_scd.total_registros_apagados_por_ausencia` = `0` o muy bajo.
- Los activos de Gold no colapsan: antes y después de la corrida, el recuento de `act_reg = 1` en `scd_primas_arl` es igual o mayor.

Con `jq` (en PowerShell usa `--arg`, las comillas internas se pierden):
```powershell
aws s3 cp s3://niif-AMB-assets/primas-arl/parametros_extraccion.json params.json
jq --arg k "total_registros_apagados_por_ausencia" '.. | objects | select(has($k)) | {($k): .[$k]}' params.json
jq '[.estado_gold_scd, .motivo_gold_scd]' params.json
```

## 5. Si el freno salta (error `SCD_APAGADO_MASIVO`)
- Gold **no se modifica**. El job deja `estado_gold_scd = FAILED` y el motivo en `parametros_extraccion.json`.
- No lo saltes sin entender por qué. Sube más del 20 % de apagados cuando la entrada de Silver está incompleta.
- Si hay una razón de negocio legítima (por ejemplo, una baja masiva real), se puede autorizar una sola vez ejecutando el job con `--PERMITIR_APAGADO_MASIVO true`. Hazlo solo con aprobación del equipo.
- Se puede cambiar el umbral por malla poniendo `umbral_apagado_pct` en el JSON `config_scd_<malla>.json`, o con el argumento `--UMBRAL_APAGADO_PCT`. Si no se define ninguno, se usa 20.

## 6. Reversa
Sube de vuelta `extraccion.qa.original.py` y `scd.qa.original.py` a sus rutas. No hay cambios en datos, permisos ni infraestructura.

## 7. Pruebas unitarias (24: 6 + 18)
Cada job trae su carpeta `tests/` (necesita `pytest`). Desde la carpeta de cada job:
```powershell
cd niif-dev-extraccion-estandar-optimized
python -m pytest tests -q
cd ..\niif-dev-scd
python -m pytest tests -q
```
Resultado esperado: `6 passed` (combinación de lotes) y `18 passed` (freno del SCD). Son requisito del estándar (cobertura > 80 % del código nuevo).

## 8. Evidencia en DEV (2026-11-06)
- **Extracción:** carga base de 2.045 registros y luego incremental (54 nuevos, 2.045 excluidos). `lotes_segmento` con 2 lotes. Gold quedó en **2.099** activos y **0** apagados por ausencia.
- **SCD sin el argumento de umbral:** el job arranca y termina en `SUCCESS` (2.099 → 2.099).
- **Freno:** con una entrada parcial (100 de 2.099) el job falló: `SCD_APAGADO_MASIVO: se apagarian 1999 de 2099 registros activos de Gold (95.24% > umbral 20.0%). Gold no se modifico.` y Gold quedó intacto.

## 9. Pendientes fuera de este paquete
- Correo de certificación con totales vacíos: tarea de notificaciones (T1).
- Alarma por ejecución fallida hacia soporte: trabajo futuro del monitoreo.
- Liberación a PRD: mismos pasos que QA, respaldando antes lo que haya en PRD.
