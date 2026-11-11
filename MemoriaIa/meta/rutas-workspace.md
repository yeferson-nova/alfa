---
tipo: referencia
actualizado: 2026-11-09
---
# Rutas del workspace

Raíz: `C:\Users\rdpuser\Git\`. Mapa humano en `Docs\00-LEEME.md`.

## Backend\ (código y entorno local)
- `inventario\` / `inventario-archivo\`: ya no están en `Backend\`; la extracción de DEV se movió a `EvidenciasAws\dev-inventario-cuenta\` (2026-11-10).
- **Todo lo organizado por servicio** (trabajo en la raíz, el resto en `pruebas-*`): `Lamnbdas\` (+ `pruebas-lambda\`), `Glue\`, `Eventos\`, `StepFunctions\` (6 SF + `pruebas-stepfunctions\`), `SNS\` (3 tópicos: `ALFA-DEV-DATALAKE-Critical`/`Warning` y `niif-dev-certification-email`), `S3\` (8 buckets del pipeline). Cada uno con `LEEME.md`.
- `Lamnbdas\<nombre-lambda>\`: las 7 lambdas de configuración con su `.py` **desplegado real** (2026-11-10: `conf_<malla>.py` y `scheduler_configurators.py` del manager, con `ORIGEN.md`; se quitaron las copias del configurador viejo).
- `Glue\<job>\`: 11 jobs de Glue del pipeline con su `.py` y `job.json` (remediación y validación técnica actualizados al desplegado el 2026-11-10; `niif-dev-certification` tiene un arreglo local **sin desplegar**: `contar_filas_datos`, commit 0020179); `Glue\pruebas-glue\` con los otros 358.
- `Eventos\`: 4 reglas de EventBridge que tocan nuestras SF, jobs y lambdas (finalizador ARL y las 3 `ALFA-DEV-DATALAKE-*` hacia `PipelineObserver`); `Eventos\pruebas-eventos\` con las otras 18. Movido desde `inventario\eventos\` (ya no existe ahí). Ver `Eventos\LEEME.md`.
- **`notificacion\` (2026-11-10): solo el código nuevo** de la cadena de notificación, una carpeta por componente con el nombre del recurso AWS (el job `niif-dev-notification-certification`, hexagonal, 513 pruebas, con su copia de la librería común en `src\niif_observability\`; `verificar_copias_comunes.py` compara las copias entre paquetes; `email-template\` con las 6 plantillas HTML de producción, mismo nombre que en S3). El código actual sigue en `Lamnbdas\<nombre>\` (las 7 lambdas de la cadena, con `ORIGEN.md`), `Glue\niif-dev-notification-certification\` (monolito desplegado con SES), `htmlNew\` (herramientas y diseños anteriores de las plantillas) y `S3\niif-dev-assets\` (JSON de caso, con marcadores). Mapa en `notificacion\LEEME.md`.
- `Lamnbdas\inventario-aws\`: `lambdas.md`, `step-functions-niif.md` y `spike-primas-rentas\` (lambda general por capas con pruebas). Movido aquí desde `Docs\05-arquitectura\`; ver [[inventario-lambdas]].
- `cloudshell-inventario\`: `inventario.sh` (solo lectura) y su zip para repetir la extracción en CloudShell; `evidencia-uso-dev.sh` (solo lectura: uso real de lambdas, Glue, SF, streams, API GW + WAF en 90 días) (salida en `EvidenciasAws\`).
- `localstack\`: entorno AWS local (docker compose), ver [[entorno-local-localstack]]. `.env` con token, excluido de Git; no leerlo.
- `reservas_matematicas_cert\`: 5 plantillas HTML del correo de certificación, ver [[correo-notificacion]].

## Tareas\ (para humanos, una carpeta por asignación)
- `TareasAsignadas.txt`: fuente original. Resumen IA en [[tareas-aws]].
- `00-LEEME.md`: contexto del proyecto, alcance, marco normativo y resumen de asignaciones.
- `T1-notificaciones\`, `T2-lambdas-gestor-configuracion\`, `T3-validaciones-reserva-matematica\`: `README.md` con problema, estado, estándares, propuesta y bloqueos; aquí van sus entregables.

## Docs\ (para humanos, referencia)
- `02-estandares-banco\`: entregables del banco, solo lectura, nombre original. Fichas en `MemoriaIa\estandares\` ([[est-etl]], [[est-datastage]], [[est-versionamiento-codigo]], [[est-codigo-seguro]], [[est-databrew]]).
  - `desarrollo\`: Estándar DataStage v6, Estándar ETL v1.
  - `versionamiento-seguro\`: Lineamiento versionamiento código seguro ALFALV2, Versionamiento de código V3.
  - `aws\`: Manual de recetas AWS Glue DataBrew.
- `03-historias-usuario\`: 6 xlsx de HUs técnicas (gobierno de ingestas, linaje, trazabilidad, DevOps/DataOps, remediación, seguridad y acceso). Fichas en `MemoriaIa\hu\` (prefijo `hu-`).
- `04-manuales\`: Manual de usuario Proyecto Nuevos Cargues V5, ver [[est-nuevos-cargues]].
- `05-arquitectura\`
  - `arquitectura-tobe-niif17.png`: arquitectura **objetivo (TO-BE)**, de alto nivel; nos competen los pasos 2 y 3. Ver [[arquitectura-niif17]].
  - `diagramas\`: `flujo-niif17-asis.drawio` (+ `.png`) es el diagrama **AS-IS** de punta a punta de los pasos 2 y 3, con iconos AWS y carriles por fase; `recorrido-dato-niif17.drawio` (+ `.html` animado y `.png`) muestra cómo viaja y se transforma el dato en 18 etapas; `README.md` explica cómo leerlos.
  - `ejemplo-correo-certificacion.png`: ejemplo del correo, ver [[correo-notificacion]].
  - `aws_pasos_2_y_3.drawio`: parte AWS con tareas marcadas, ver [[diseno-propuesto]].
- `06-brechas\`: análisis de brechas (seguridad, costos, arquitectura, cumplimiento).
  - `seguridad.md`: ver [[brechas-seguridad]].
  - `monitoreo-continuo-grafana.md`: ver [[monitoreo-grafana]].
- `07-entregables\`: lo que se entrega al banco.

## MemoriaIa\ (para IA)
Subcarpetas: `meta\`, `contexto\`, `tareas\`, `estandares\`, `hu\`, `brechas\`. Los nombres de nota son únicos en toda la memoria, así que `[[nota]]` funciona sin importar la subcarpeta.

## EvidenciasAws\ (salidas de AWS, ignorada salvo `LEEME.md`)
- Una carpeta por extracción: `<ambiente>-<tema>\` (sin fecha, por preferencia del usuario). Convención en `EvidenciasAws\LEEME.md`. Movidas aquí el 2026-11-10 y sacadas de Git.
- `dev-uso-recursos\` (salida de `evidencia-uso-dev.sh`; análisis en `Docs\06-brechas\limpieza-dev-recursos.md`), `dev-inventario-cuenta\` (antes `Backend\inventario-archivo\`), `dev-inventario-lambdas\` (antes `Backend\Lamnbdas\inventario-aws\extraccion\`), `prd-primas-arl-202606\subida-equipo\` y `\recolectada\` (antes `errores\evidencia-primas-arl-202606\` y `errores\Evidencia\`).
- Los scripts de `errores\` reciben la carpeta como argumento: pasar la ruta nueva.

## errores\ (incidente en prod Primas ARL 202606, 2026-11-05)
Detalle del incidente en [[inprod-primas-arl-202606]].
- Documentos: `errores.md`, `analisis-flujo-primas-arl-202606.md`, `replicar-fallo-en-dev.md`.
- Scripts: `recolectar-evidencia-prd.sh` (CloudShell prod, solo lectura), `replicar-fallo-dev.sh` (solo DEV), `validar-analisis.py`, `validar-athena.py`, `simular-limpieza-segmentos.py`, `ver-logs-evidencia.py`.
- Evidencia: movida a `EvidenciasAws\prd-primas-arl-202606\` (ver arriba).
- En la raíz del repo: `.gitignore` (ignora `EvidenciasAws/*` salvo el `LEEME.md`) y `.gitattributes` (`*.sh` con LF).
