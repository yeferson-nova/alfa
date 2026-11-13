---
tipo: estandar
estado: vigente
actualizado: 2026-11-03
---
# Estándar de desarrollo ETL (GTC-GDD-010 v1, feb-2024)

Fuente: `Docs\02-estandares-banco\desarrollo\Estandar de desarrollo ETL v1.pdf` (8 págs.). Seguros Alfa, Gestión de la Demanda de TI. Genérico, aplica a cualquier ETL (incluye Glue/Lambda de los pasos 2 y 3).

## Reglas verificables
- **Nombres:** tablas `TMP_` / `STG_`; procesos `ETL_<NOMBRE>`; archivos `IN_` / `OUT_` + fecha `YYYYMMDD` (ej. `OUT_SUMMARY_20240201.txt`).
- **Extracción:** sin `SELECT *`; solo columnas y filas necesarias; incremental; horarios de baja carga; conexiones seguras y cifrado de datos sensibles.
- **Transformación:** reglas de validación y limpieza documentadas; manejo de errores; alertas de error; sumas de comprobación para integridad; componentes modulares y reutilizables.
- **Carga:** en lote; control de duplicados (PK/unicidad); particionamiento; **mecanismo de rollback**; registrar estadísticas (tiempo, registros, errores).
- **Logging (6.1):** cada ejecución con inicio, fin, duración y errores; monitoreo de fallos en tiempo real.
- **Calidad (6.2):** verificar integridad antes y después de cada proceso; **comparar volumen extraído vs. cargado**.
- **Versionamiento (7):** Git según [[est-versionamiento-codigo]]; historial de cambios de datos en tablas de auditoría.

## Relevancia para nuestras tareas
- Tarea 3 (validaciones de reserva matemática, [[tareas-aws]]): 6.2 exige cuadre de volúmenes y verificación de integridad; base mínima del validador.
- Tarea 1c ([[monitoreo-grafana]]): 6.1 exige monitoreo de fallos en tiempo real.
- Nombres de archivos de salida del paso 3 (CSV de certificación): revisar contra `OUT_<nombre>_YYYYMMDD` (posible brecha ARQ, propuesta).
