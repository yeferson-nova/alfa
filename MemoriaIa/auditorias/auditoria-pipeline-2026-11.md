---
tipo: auditoria
estado: vigente
actualizado: 2026-11-05
---
# Auditoría del pipeline de datos y de las lambdas (2026-11-05)

Dos documentos humanos, validados contra el código (archivo y línea): `Docs\06-brechas\auditoria-pipeline-datos-vs-arquitectura.md` (jobs de Glue, Step Functions, crawlers, contraste con el diagrama) y `Docs\06-brechas\auditoria-lambdas.md`. Excluye la lambda unificada del spike. Complementa [[brechas-seguridad]] y [[riesgos-glue-notificacion]]; los PP-xx de `revision-pre-produccion.md` se citan, no se repiten.

## Críticos (verificados en código)
- **ARQ-01** `scd.py:312-347`: si falla la lectura de Gold, `except Exception` asume «primera ejecución» y sobrescribe Gold (se pierde el historial).
- **ARQ-02** `scd.py:495-500`: Gold se reescribe en su propia ruta, sin atomicidad ni versión previa confirmada.
- **ARQ-03** La validación nunca bloquea (`detiene_flujo=False` fijo) y la remediación no corrige ni aparta; `PENDIENTE_ANALITICA` no lo lee nadie.
- **ARQ-04** Vacío/parcial llega a certificar: 4 de 6 Silver sin guarda de 0 filas; SCD vacío → `SKIPPED`; extracción con 0 filas → regeneración. La Step Function no decide sobre `estado_*`.
- **ARQ-05** Silver: inner joins sin cuadre y `dropDuplicates` por llave sin orden (monto arbitrario).
- **ARQ-06** El freno de SCD exige `UMBRAL_APAGADO_PCT`, que ni `job.json` ni las 6 SFN pasan; `estructura-nueva\…\scd.py` no tiene el freno.
- **CUM-01** El repo versiona evidencia PRD, correos reales y el ID de cuenta (330 archivos). Visibilidad no verificada.
- **SEG-05** `EnvioSftpFis`: `AutoAddPolicy`, contraseña, `ssh-rsa` de respaldo. **ARQ-15** el SFTP devuelve éxito con fallos, sin DLQ ni archivo temporal.

## Evidencia de la cuenta DEV (`EvidenciasAws\aud-datos-cuenta\`, 2026-11-05)
- **Confirmado:** las 6 mallas con validación `FAILED` (11-100 errores) y Gold `SUCCESS`; el SCD desplegado no tiene el freno; solo ARL tiene regla de cierre de ejecución; 11 de 12 scripts desplegados = repo (solo difiere `scd`).
- **Corregido:** Gold, Silver, Bronce y assets tienen versionado, KMS y bloqueo público (ARQ-02 baja a alta); los roles de jobs están acotados por malla (excepción: `sns:*` sobre `*` en notificación); `programador-configuraciones` está Disabled; DEV es barato (máx. 1,8 DPU-h, 0,38 GB en Athena).
- **Nuevo:** la redirección desplegada no valida expiración y filtra el registro en un 400; reintentos del Scheduler + nombre de ejecución no determinista = ejecuciones duplicadas; llaves de SCD con corte (ARP/RVI) apagarían el corte anterior cada mes (por confirmar con Athena).
- **Abierto:** configs de caso de certificación (consultas, aprobadores), reglas de WAF, código de los triggers activos y de `send-via-sftp`, consulta de Athena por `per_corte`, visibilidad del repo.

## Contraste con el diagrama
Faltan como job «Consolidación Bronce» y «Consolidación Plata»; Validación y Remediación existen pero no cumplen su función; lambdas sin VPC (el diagrama las ubica en subred privada); trazabilidad parcial (logs SFN apagados, cierre de ejecución solo para ARL).

## Pendiente de evidencia
Configs de `niif-dev-assets` (`config_scd_*`, `config_val_tec_*`), consultas de certificación, políticas IAM, versionado de Gold, que el script desplegado de SCD sea el del repo, código del manager y de `send-via-sftp`.

Relacionado: [[pendientes]], [[inprod-primas-arl-202606]].
