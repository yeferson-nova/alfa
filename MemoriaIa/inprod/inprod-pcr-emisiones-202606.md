---
tipo: inprod
estado: vigente
actualizado: 2026-11-23
---
# Incidente: PCR Emisiones 202606, cifras de PRD distintas a las de QA

> Negocio (correos del 10, 13 y 18 de nov, reenviados el 2026-11-23) pide que la extracción de junio 2026 cuadre con dos Excel; se reporta que PRD trae cifras distintas a QA. **No se sabe si PRD y QA tienen los mismos datos de origen: no se asume que el problema es nuestro.** Estado 2026-11-23: Excel validados con código; diagnóstico por capas listo para correr en QA y PRD (DevOps) y en Oracle (DBA). Sin evidencia de AWS todavía.

Tarea humana y scripts: `Tareas\Fixs\PcrEmisiones\` (`README.md`, carpeta `diagnostico\`). Relacionado: [[data-datalake-niif-pcr-emisiones-glue-job]], [[data-datalake-emisiones-vida-glue-job]], [[modularizacion-extraccion]], [[auditoria-extraccion-2026-11]] (ARQ-61 a ARQ-65), [[auditoria-repos-2026-11]] (ARQ-50).

## Referencia (validado con `validar_referencia.py`)
- Generales: el resumen = detalle RRC con `Mes Produccion = 202606` (PASS al centavo). 4.478 filas; 726 son de meses anteriores dentro del corte de junio y no entran.
- El Excel no es una sola verdad: frente a `E1-A1`, Generales IVA comisión recaudo difiere 63.748.417,52; Vida comisión 4.053,80 (VidaAp). Vida y Satélites difieren en IVA de comisión. Acordar con negocio contra cuál se valida ([[preguntas-abiertas]]).

## Flujo real (código `quality` 2026-11-20)
- Configuración por ambiente: PRG `PROGRAMACION#EMISIONES` (`niif-{env}-prg-programaciones`: periodo, fechas, status, productos, compañías, lote, id) + `MALLA#GENERALES|SATELITES|PCR_VIDA` en `cnf-mallas` (SQL) → `conf_emisiones.py` publica `s3://niif-{env}-assets/<caso>/config_extraccion_<caso>.json`.
- Step Function `niif-{env}-emisiones`: configuradora general → `extraccion-estandar-emisiones` (los 3 casos) → por caso val-tecnica, remediacion, `silver-<caso>` (`S3_SLV_PREP_PATH=s3://alfa-datalake-{env}-silver/niif/preparacion_emisiones/<caso>`) → SCD.
- Montos: los Silver suman `thirdpartymovementpolicy.ammount` por listas de conceptos, agrupados por (operación, póliza).

## Hipótesis H1-H10 (detalle en el README de la tarea)
H1 fuente Oracle distinta · H2 código distinto (ARQ-50, ARQ-62) · H3 PRG/CNF distintos · H4 rutas quemadas (ARQ-61) · H5 checkpoint reutilizado con lote fijo (ARQ-63) · H6 grano del Silver (ARQ-64) · H7 "emitido en junio" = mes de producción frente a `TIME_STAMP` · H8 columnas: Silver Generales no produce IVA comisión intermediario; mapeo **propuesta** · H9 filtros fijos en el Silver (ARQ-65) · H10 falta `--RUTA_CONFIG_SCD` (ARQ-65).

## Diagnóstico entregado
- `diagnostico_pcr_emisiones.py` (+ `.sh`): solo lectura, por ambiente; huella del código, Step Function y corridas, PRG/CNF, JSON publicados, runtime, lotes y checkpoints en S3, Athena (Silver por producto, Silver por operación para H6, Bronce por producto y concepto) y genera las SQL de Oracle con la configuración del propio ambiente. Alertas automáticas.
- `conciliacion-oracle-acsele.sql`: Q1-Q6 fijas para QA y PRD (frescura, estados, compañía por producto, operaciones, movimientos por concepto, por día).
- `comparar_ambientes.py`: veredicto por capa (Oracle → código → orquestación → configuración → extracción → Bronce → Silver) y Silver frente al Excel. Probado con evidencias sintéticas (detecta cada capa) y con evidencias iguales (todo IGUAL).
- No probado contra AWS real (no hay acceso desde aquí).

## Cumplimiento
- Los `.msg` y el RRC (hoja `Data` con nombres de tomadores) quedaron versionados en `alfa` (commit `ee37aa7`, ya en `origin/main`). Reincidencia de CUM-01: decisión del usuario sobre sacarlos ([[pendientes]]).
