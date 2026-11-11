# Jobs de Glue de la tarea

Movidos el 2026-11-03 desde `Backend\inventario\glue\` (cuenta 533267035849, us-east-1, DEV). Se verificó que la copia fuera idéntica antes de quitar `jobs.json` y `scripts\` del inventario.

## Jobs de trabajo (11)
Son los que invocan las 6 Step Functions `niif-dev-<malla>`. Cada carpeta tiene el `.py` del job y su `job.json` (definición: rol, workers, argumentos, conexiones).

| Carpeta | Etapa | Script |
|---|---|---|
| `niif-dev-extraccion-estandar-optimized` | Extracción (conexión `datalake-dev-alfaprd-dev-connection`) | `extraccion_estandar_optimized.py` |
| `niif-dev-val-tecnica` | Validación técnica | `validacion_tecnica.py` |
| `niif-dev-remediacion-estandar` | Remediación | `remediacion_estandar.py` |
| `niif-dev-silver-primas-rentas` | Silver | `silver-primas-rentas.py` |
| `niif-dev-silver-primas-endosos` | Silver | `silver_primas_endosos.py` |
| `niif-dev-silver-primas-arl` | Silver | `silver_primas_arl.py` |
| `niif-dev-silver-auxilios-funerarios` | Silver | `silver_aux_funerarios.py` |
| `niif-dev-silver-reserva-matematica-arp` | Silver | `silver_reserva_matematica_arp.py` |
| `niif-dev-silver-reserva-matematica-rvi` | Silver | `silver_reserva_matematica_rvi.py` |
| `niif-dev-scd` | Gold (SCD) | `scd.py` |
| `niif-dev-certification` | Certificación | `niif_certification.py` |
| `niif-dev-notification-certification` | **Envío del correo (T1)**: arma el mensaje y lo publica por SNS. Lo dispara el trigger del stream `PENDING`, no la Step Function. Copiado de `pruebas-glue\` el 2026-11-03 | `notification_certification.py` |

Extracción, validación, remediación y SCD son **genéricos**: los comparten las 6 mallas. Solo Silver es uno por malla.

## Crawlers y catálogo de datos
- `crawlers\`: los 3 de la Step Function, `niif-dev-crawler-bronze`, `-silver` y `-gold`.
- `catalogo\`: las 3 bases `niif-dev-{bronze,silver,gold}-catalog-db` (`databases-niif-dev.json`) y sus tablas, extraídas en CloudShell el 2026-11-04 (`tablas-niif-dev-*.json`).

| Capa | Tablas | Qué hay |
|---|---|---|
| Bronze | 69 | Copia de las fuentes: 27 de `ACSELE`, 12 de `ALFAPRD\arpprod`, 17 de `ALFAPRD\general`, 8 de `ALFAPRD\prv` y 2 de control de PCR Vida. |
| Silver | 94 | Las mismas 66 de las fuentes, más por malla: `silver_preparado_<malla>`, `validacion_tecnica_<malla>`, tablas `lote_cargue_id_*` de remediación (una por malla) y `tmp_preparacion_*`. |
| Gold | 9 | `scd_<malla>` de las 6 mallas, más `scd_pcr_vida`, `niif_prima_previsional` y `reserva_matematica_cargue_unico`. |

- **Reserva matemática es mucho más ancha:** `silver_preparado` y `scd` de ARP y RVI tienen unas 440 y 458 columnas, y las `tmp_preparacion_*` hasta 511. Primas y auxilios tienen entre 14 y 27.
- **Tablas que sobran:** el crawler de Silver crea una tabla por cada partición de `lote_cargue_id` y de `ejecucion_preparacion`, y deja copias `tmp_` e históricas en el catálogo. Es ruido en Athena; hay que ver si el crawler debe apuntar a rutas más específicas (hallazgo por validar).

## `pruebas-glue\`
Los otros 358 jobs (`jobs.json`) y sus scripts (`scripts\`, 267 archivos). Incluye los `datalake-dev-niif-*` anteriores, `niif-dev-pcr-*`, `Prueba_*` y diagnósticos.
- `niif-dev-pcr-vida-remediacion` y `niif-dev-pcr-vida-val-tecnica` usan el mismo script que los jobs de trabajo (`remediacion_estandar.py`, `validacion_tecnica.py`). Ese `.py` solo está en las carpetas de trabajo.

## Fuera de este alcance (pendiente de validar)
- `niif-dev-notification-certification` ya está en la carpeta de trabajo (es el punto de envío del correo, T1); su copia sigue también en `pruebas-glue\`.
- También en `pruebas-glue\`: `crawlers.json` (los otros 87), `databases.json` (las otras 48 bases), `tablas\` (35 archivos de otras bases), `triggers.json` y `workflows.json` (solo el workflow de prueba `ORQUESTADOR_PRIMAS_ARL_MANUEL_MACHADO`).
- `Backend\inventario\glue\` quedó vacío.

## Nota
Los scripts son los que Glue tenía guardados en la extracción; no hay repositorio Git, así que la cuenta es la única fuente.

## Contraste con lo desplegado (2026-11-10)
Comparado con `EvidenciasAws\dev-codigo-actual\` (sin contar diferencias de espacios ni de fin de línea):
- **Iguales:** extracción, SCD y los 6 silver.
- **Actualizados al desplegado:** `niif-dev-remediacion-estandar` y `niif-dev-val-tecnica`. En DEV ya leen Parquet de forma recursiva y saltan los `_DONE.json`.
- **`niif-dev-certification`:** la copia local tiene un arreglo **que no está desplegado** (`contar_filas_datos`, commit 0020179). Antes de entregarlo, compararlo otra vez con lo desplegado.
- `niif-dev-notification-certification`: igual al desplegado (el monolito con SES). Su versión nueva (hexagonal) está en `Backend\notificacion\jobs-glue\niif-dev-notification-certification\`.
