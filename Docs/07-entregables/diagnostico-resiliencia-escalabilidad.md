# Diagnóstico de resiliencia y escalabilidad: Glue y notificación NIIF 17

**De:** equipo de Desarrollo, dueño del pipeline, de la tarea y del incidente en prod. **Para:** equipo de Analítica, que recibe el pipeline terminado. **Fecha:** 2026-11-05. **Alcance:** jobs de Glue del medallón y lambdas/jobs de notificación y certificación.
**Responsabilidad:** los 18 problemas de abajo son nuestros y los corregimos nosotros antes de entregar. Analítica recibe un sistema ya resiliente y escalable, con pruebas, IaC y documentación; no hereda estas correcciones.
**Base:** código del repositorio (12 scripts de Glue, 6 configuradores, manager sin código), revisión de DEV, el incidente de prod (Primas ARL 202606) y el diagrama `NIIF17_ModeloSolucion_V1.0.drawio`. Lo marcado **(por confirmar)** se comprueba con `Backend\cloudshell-inventario\comparar-arquitectura.sh`.

## 1. Qué hay y qué falta del código

| Pieza | Estado |
|---|---|
| 12 scripts de Glue (≈ 10 000 líneas) y 6 configuradores casi idénticos (≈ 4 800 líneas) | Existen, **sin una sola prueba**, con funciones copiadas entre jobs |
| `manager-configuration`, trigger de notificación, `copy-s3-cert`, `move-s3-certification`, SFTP | **Sin código** en el repo (solo configuración) |
| Job de notificación con SES y lambda general de configuración | Reescritos con arquitectura hexagonal y pruebas (451 y 174), **sin desplegar** |
| Plantilla corporativa de repo Glue (`data-datalake-niif-primas-rentas-glue-job`) | Trae Jenkinsfile (`glueJobsClientesPipeline`), credenciales por ambiente y ruta `niif-<amb>-assets/glue-resources/<caso>`. Pero su cobertura está en 0 % (`fail_under = 0`, `src` excluido) y el README describe otro caso: **no cumple el > 80 % exigido** |
| Diagrama vs. lo desplegado | El diagrama muestra EventBridge Pipe, WAF, zona de Aterrizaje, Entra ID y Transit Gateway hacia FIS. En el código solo se ve stream → lambda → Scheduler, sin WAF ni Landing **(por confirmar)** |

## 2. Problemas y cómo resolverlos

### A. Cifras (lo más grave)
| # | Problema | Solución |
|---|---|---|
| 1 | **El SCD apaga por ausencia** con carga incremental: en prod Gold quedó con 209 activos de 1,2 M (INC-2). | Modo de carga explícito en metadata; el cierre por ausencia solo con foto completa. Compuerta: si la entrada es una fracción anómala del Gold activo, **detener y alertar**. Gold versionado para poder revertir |
| 2 | **La validación nunca detiene el flujo** y no hay cuadre (PP-01). | Compuerta HU-TR-02: regla bloqueante detiene; cuadre `entra = sale + cuarentena` en filas **y en sumas de importes**, con alarma si ≠ 0 |
| 3 | **El correo sale con totales vacíos**: el CSV siempre trae encabezado (INC-3). | Contar filas reales; si resumen, totales o detalle están vacíos, no enviar y avisar a soporte |
| 4 | Montos sin tipo fijado (riesgo de `float` y de redondeo repetido). | DECIMAL con precisión declarada, un solo redondeo, total calculado una vez y transportado en el manifiesto |

### B. Resiliencia
| # | Problema | Solución |
|---|---|---|
| 5 | El estado entre jobs es **un archivo fijo por malla**: dos ejecuciones se pisan (FLU-02). | **Manifiesto inmutable por `ejecucion_id`**; cada etapa lee el anterior y escribe el suyo |
| 6 | **Streams con reintentos infinitos y sin destino de fallo**; un registro malo bloquea el shard (PP-09). | Reintentos y edad máxima limitados, división de lote, cola de fallos con alarma. Consumidores idempotentes |
| 7 | **Dos programadores sobre el mismo stream** (PP-03): doble ejecución. | Dejar uno solo (decisión G3 o G4) |
| 8 | Step Functions sin logs ni `TimeoutSeconds`; fallos fuera de ellas sin aviso (PP-05). | Logs y X-Ray activos, timeouts por tarea, `Catch` hacia notificación de fallo; alarmas de Glue, lambdas y DLQ hacia SNS → ServiceNow |
| 9 | Certificar y transportar pueden ser versiones distintas del dato. | Aprobar un manifiesto con checksum; el transporte lo verifica. Espera con `waitForTaskToken` |

### C. Escalabilidad
| # | Problema | Solución |
|---|---|---|
| 10 | **6 copias de lambda y 11 jobs con código repetido**; un arreglo se hace 6 veces. | Una lambda general (T2) y una librería común versionada; jobs por caso con config, no por copia |
| 11 | `remediacion` admite 1 ejecución y los 3 crawlers recorren toda la capa en cada corrida: las mallas se bloquean entre sí (RYC-02/04). | Subir la concurrencia, registrar solo la partición nueva o un crawler por malla |
| 12 | Extracción de Oracle con una conexión por tabla y 94 tablas en Silver (una por lote). | Lectura paralela por columna si la medición lo justifica; una tabla por objeto particionada por lote |
| 13 | Una malla nueva hoy exige copiar código y rutas. | Alta de malla solo por metadata (`cnf-mallas`) + IaC; sin tocar código |

### D. Seguridad y costo
| # | Problema | Solución |
|---|---|---|
| 14 | **Athena sin workgroup, sin tope ni cifrado** (PP-06). | Workgroup propio con `BytesScannedCutoffPerQuery`, resultados cifrados y alerta de presupuesto; columnas explícitas |
| 15 | Timeouts de 48 h (≈ 84 USD por job colgado) y logs sin retención (PP-14, RYC-13). | Timeout = 2-3 × p95 real; retención de 30 días; Spark UI solo para diagnóstico |
| 16 | Correo por SNS en texto plano; enlaces Certificar/No certificar por GET; lambda con valores por defecto de QA (PP-08, PP-10). | SES con HTML escapado; página de confirmación + POST con token de un uso e identidad federada; sin defaults entre ambientes |
| 17 | Lake Formation, KMS por dominio, WAF y CloudTrail **(por confirmar)**. | Ejecutar el script; cerrar lo que salga `FALTA` o `REVISAR` |

### E. Gobierno del código
| # | Problema | Solución |
|---|---|---|
| 18 | Sin pruebas, sin línea base de SonarQube, recursos creados a mano. | Repos desde la plantilla corporativa **corrigiendo** `fail_under ≥ 80`; hexagonal; todo por Terraform y Jenkins (build once, deploy many) |

## 3. Orden propuesto (a cargo de Desarrollo)
1. **Contener (ya):** no re-ejecutar Primas ARL; restaurar Gold; compuerta contra SCD parcial y contra correo vacío (1, 3).
2. **Base resiliente:** manifiesto por ejecución, streams con destino de fallo, un solo programador, logs y alarmas, workgroup de Athena (5-9, 14, 15).
3. **Escalar:** lambda general, librería común, alta de mallas por metadata, concurrencia y catálogo (10-13) con pruebas > 80 % y despliegue por pipeline (18).

## 4. Cómo comprobar lo "por confirmar"
En CloudShell de cada cuenta (solo lectura): `bash comparar-arquitectura.sh` (DEV) y `AMB=prd bash comparar-arquitectura.sh` (PRD). Entrega `resumen.tsv` con `OK`, `FALTA` (el diagrama lo muestra y no existe), `REVISAR` (existe con riesgo) y `SIN-ACCESO`. No subir el zip al repositorio (es público).
