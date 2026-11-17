# Evidencia: SCD con periodo activo

Escenarios con PySpark que prueban la regla de periodo del `scd.py` (rama `bugfix/scd-freno-periodo` de `alfa-repos`). No van al repo; en el repo quedan las pruebas sin Spark (`tests/test_control_periodo.py`).

| Escenario | Resultado |
|---|---|
| Periodo nuevo | El periodo anterior queda inactivo; el freno no actúa |
| Mismo periodo otra vez | Merge: 1 cambio (versión 2), 1 nuevo, 1 ausente inactivo, 8 sin cambio |
| Mismo periodo con pérdida masiva | Actúa el freno; Gold no cambia |
| Volver a un periodo ya ejecutado | Los que vuelven entran activos con la versión siguiente; el otro periodo queda inactivo |
| Primera corrida sin archivo de control | Actúa el freno; con `PERMITIR_APAGADO_MASIVO=true` pasa y guarda el periodo; el siguiente cambio de periodo pasa solo |

Resultados completos en `esperados\`.

## Cómo repetirlo
1. Copiar `tests\` de la rama `feature/modular-rmd-ext-estandar` y `src\jobs\` de `bugfix/scd-freno-periodo` a una carpeta de trabajo.
2. En `tests\soporte\escenarios_scd.py`, agregar a `correr_scd` el parámetro `periodo` (va como `periodo_negocio` en `parametros_extraccion.json`). En `tests\soporte\entorno.py`, hacer que `get_object` del S3 simulado lance `ClientError` `NoSuchKey` si el objeto no existe.
3. Copiar `test_scd_periodo.py` a `tests\caracterizacion\` y `esperados\*.json` a `tests\caracterizacion\esperados\`.
4. Correr `python -m pytest tests\caracterizacion\test_scd_periodo.py -o addopts=""` con el entorno `C:\Users\rdpuser\.venvs\niif-glue`.
