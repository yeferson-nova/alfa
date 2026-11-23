---
tipo: inprod
estado: vigente
actualizado: 2026-11-23
---
# Incidente: PCR Emisiones 202606, cifras de PRD distintas a las de QA

> Negocio (correos del 10, 13 y 18 de nov, reenviados el 2026-11-23) pide que la extracción de junio 2026 cuadre con dos Excel; se reporta que PRD trae cifras distintas a QA. **No se sabe si PRD y QA tienen los mismos datos de origen: no se asume que el problema es nuestro.** Estado 2026-11-23 (tarde): corrido en DEV y QA + Q1-Q4 en Oracle QA. **QA extrae fiel a su Oracle** (18 conteos iguales al registro); DEV=QA en código, Step Function y PRG. Falta PRD, Q5-Q6 y repetir la sección de datos (montos) por errores del script.

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
- `diag_emisiones_v2.py` (+ `.sh`): solo lectura, por ambiente; huella del código, Step Function y corridas, PRG/CNF, JSON publicados, runtime, lotes y checkpoints en S3, Athena (Silver por producto, Silver por operación para H6, Bronce por producto y concepto) y genera las SQL de Oracle con la configuración del propio ambiente. Alertas automáticas.
- `conciliacion-oracle-acsele.sql`: Q1-Q6 fijas para QA y PRD (frescura, estados, compañía por producto, operaciones, movimientos por concepto, por día).
- `comparar_ambientes.py`: veredicto por capa (Oracle → código → orquestación → configuración → extracción → Bronce → Silver) y Silver frente al Excel. Probado con evidencias sintéticas (detecta cada capa) y con evidencias iguales (todo IGUAL).
- No probado contra AWS real (no hay acceso desde aquí).

## Resultados DEV y QA (2026-11-23; evidencia en `Tareas\Fixs\PcrEmisiones\ResultadosAWS\`, commit `c949d0e`)
- DEV y QA usan la **misma Oracle** (conexión `datalake-{env}-acsele-sop-connection`, servicio ACSSOP); Q1: datos hasta el 18/11/2026.
- QA: contextoperation 3.928.778 = Q2; 48897 3.703.183, 52897 203.161, 54959 95, 55059 314, 54899 10.776/10.568, 54939 6.908, 54919 560, 55060 60, 55119 111 = Q4. Extracción fiel en filas.
- 13 tablas no existen en ACSSOP (`ORA-00942`, `SKIPPED_NOT_AVAILABLE`): mascotas, decenal, `urproteccionfinanciera`, petlover, exequias tradicional, protección salud/vida (atributos, no montos). La fuente de QA no tiene la estructura de producción.
- Código (7 jobs + lambda) y Step Function **iguales** DEV/QA; PRG con los mismos filtros. La SF desplegada **sí** pasa `--RUTA_CONFIG_SCD` → H10 descartada en DEV/QA (la IaC `77de48cb` estaba atrasada); la parte de ARQ-65 sobre ese argumento queda solo para confirmar en PRD.
- DEV vs QA: misma Oracle, conteos distintos (DEV 4.006.114 operaciones, extraído 19-21 nov; QA 3.928.778, el 22): la base cambió entre fechas (*hipótesis: recarga; confirmar con DBA*). DEV tiene corridas manuales de otra persona (complementos Pymes, uniones de lotes): **no sirve de línea base**.
- Conteo del Silver igual en DEV y QA (203.689 / 21.695 / 3.703.183) pese a Bronce distinto: sin explicar.
- `silver_satelites.py:32` `PRODUCTOS` es una constante sin uso (el filtro real es compañía `50.0` + nombres de producto): no es brecha.
- **Errores del diagnóstico (corregidos):** Silver sumaba `idproducto` (AMBIGUOUS_NAME) y en Satélites tomó `controles/`; Bronce tomó cargas completas viejas sin filtro (15 GB/consulta, CSV idénticos DEV=QA, inválidos). Ahora usa la tabla de la ruta exacta del runtime o no consulta. Repetir con `SECCIONES=configuracion,datos`.

## Cumplimiento
- Los `.msg` y el RRC (hoja `Data` con nombres de tomadores) quedaron versionados en `alfa` (commit `ee37aa7`, ya en `origin/main`). Reincidencia de CUM-01: decisión del usuario sobre sacarlos ([[pendientes]]).
