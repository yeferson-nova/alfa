# Spike: configurador de Primas Rentas con estructura corregida

**Qué es:** la lambda `niif-dev-configuration-primas-rentas` reescrita por capas, como prueba de concepto de la lambda general estandarizada (`..\lambdas.md`, sección 5). **Propuesta, no desplegada.**
**Fecha:** 2026-11-03 · **Python:** 3.12+ · **Pruebas:** 37, todas pasan.

> **Base del spike:** el código real de `configuration-primas-rentas` no está extraído (ver `..\lambdas.md`, sección 2). Se partió de su antecesora `niif-dev-configurador-primas-rentas` (`Backend\inventario\lambdas\codigo\referencia-configurador\`, 767 líneas, un archivo). La versión desplegada añade, según lo medido en auxilios, `_evento_desde_prg` y `_resolver_where_periodo`; **no están en este spike**. Hay que traerlas cuando se extraiga el código real (`servicio.py` y `periodo.py` son sus lugares naturales).

---

## 1. El problema que corrige

| Problema del original | Evidencia | Cómo queda |
|---|---|---|
| **Un solo archivo de 767 líneas** con 5 responsabilidades mezcladas (leer entorno, reglas de negocio, DynamoDB, S3, Step Functions). | `lambda_function.py` | 18 archivos de 1 a 113 líneas; cada uno con una sola razón para cambiar. |
| **Handler de 232 líneas.** | `lambda_handler` (líneas 398-630) | `handler.py` de 13 líneas; la lógica está en `servicio.py`, dividida en métodos de hasta ~35 líneas. |
| **Imposible probar sin AWS.** boto3 se crea al importar; `os.environ[...]` se lee al importar. | Líneas 14-32 | Los puertos (`puertos.py`) se inyectan. Las pruebas usan dobles en memoria y no necesitan boto3 ni red. |
| **Todo lo de la malla está quemado:** `"primas_rentas"`, `"EJC#PRIMAS_RENTAS#"`, `"rentas-"`, `9` registros, `4` ciclos, `CICLO#001..004`, metadatos `"primas-rentas"`. Por eso hay 6 copias. | Líneas 17-20, 133, 173, 335, 423-433, 530-546 | Un catálogo de datos (`mallas.py`). Agregar una malla es agregar una entrada, no copiar 767 líneas. |
| **Imports dentro de funciones** (`import os`, `import boto3` repetidos). | Líneas 517-519, 634-636, 693-694, 715-719 | Todos al inicio del módulo; boto3 solo en `adaptadores\` y `composicion.py`. |
| **Código muerto y valores falsos:** `comparaciones = []` nunca se llena, `comparar()` no se llama, `"estructurasPRSValidadas": 7` es constante. | Líneas 351-361, 481, 616 | Eliminados. |
| **Validación del periodo duplicada** en el handler y en `_preparar_periodo_cnf`; además `aplicar_periodo_runtime(nuevo → nuevo)` no hace nada. | Líneas 451-478 y 633-689 | Una sola validación (`periodo.validar_formato`) y una sola aplicación. |
| **Revisa los registros requeridos después de usarlos:** si falta `EXTRACCION`, el error es un `KeyError` sin contexto. | Líneas 421-442 | `validaciones.validar_registros_cnf` corre primero y dice qué registro falta. |
| **Sin validar variables de entorno** (KeyError al importar). | Líneas 14-16 | `config.Ajustes.desde_entorno` valida todas y las nombra en el error. |
| **Log de texto** con `json.dumps` suelto. | Línea 627 | `logs.registrar`: una línea JSON por evento. |
| **Sin pruebas.** | — | 37 pruebas; el núcleo (todo menos adaptadores) tiene **98 % de cobertura**. |

---

## 2. Estructura

```
spike-primas-rentas/
  pyproject.toml
  README.md
  src/configurador/
    handler.py          13 líneas  Entrada de Lambda: evento -> servicio -> respuesta
    composicion.py      26         Arma el servicio con los adaptadores reales (único lugar con boto3 del flujo)
    servicio.py         97         Caso de uso: validar -> publicar -> ejecutar
    constructores.py   113         Reglas de negocio: arma los 4 JSON (extracción, validación, remediación, SCD)
    periodo.py          84         Reglas de periodo (puras)
    validaciones.py     34         Valida la configuración leída y los JSON generados
    mallas.py           44         Catálogo de mallas (lo que antes se copiaba)
    modelos.py          66         Contrato de entrada (Evento) y de salida (Resultado)
    config.py           43         Variables de entorno validadas
    puertos.py          34         Interfaces hacia AWS (Protocol)
    json_utils.py       14         Decimal -> int/float
    logs.py             13         Log JSON
    errores.py           5         ConfiguracionError
    adaptadores/                   Implementaciones con boto3
      dynamo.py         85         CNF (consulta y transacción), PRS, registro de ejecuciones
      s3.py             39         Leer y escribir JSON
      stepfunctions.py  14         Iniciar la ejecución
  tests/                           486 líneas, sin AWS
    fakes.py  conftest.py  test_periodo.py  test_constructores.py
    test_validaciones.py  test_servicio.py  test_handler_y_config.py
```

Dependencias (siempre hacia adentro):
```
handler -> composicion -> adaptadores (boto3)
   |            |
   v            v
servicio -> constructores, periodo, validaciones -> puertos (interfaces)
```
Ninguna regla de negocio importa `boto3`.

### Dónde quedó cada función del original

| Original (`lambda_function.py`) | Ahora |
|---|---|
| `lambda_handler` | `handler.lambda_handler` + `servicio.Configurador.ejecutar` |
| `decimal_default`, `normalizar` | `json_utils` |
| `leer_json_s3`, `escribir_json_s3` | `adaptadores/s3.AlmacenS3` |
| `consultar_cnf` | `adaptadores/dynamo.DynamoCnf.consultar` + `validaciones.validar_registros_cnf` |
| `obtener_estructura_prs` | `adaptadores/dynamo.DynamoPrs.obtener` |
| `construir_ciclos`, `construir_extraccion`, `construir_validacion`, `construir_scd`, `construir_remediacion` | `constructores` |
| `aplicar_periodo_runtime`, `_preparar_periodo_cnf` | `periodo` |
| `_validar_generados_cnf` | `validaciones.validar_generados` |
| `_persistir_periodo_cnf` | `adaptadores/dynamo.DynamoCnf.persistir_periodo` |
| `start_execution` y `put_item` (dentro del handler) | `adaptadores/stepfunctions.OrquestadorStep` y `DynamoEjecuciones`, orquestados por `servicio._iniciar_ejecucion` |
| `comparar`, `comparaciones` | Eliminados (código muerto) |

---

## 3. Cambios de comportamiento (revisar antes de adoptar)

El resto es equivalente al original. Estas son las únicas diferencias:

1. **Respuesta sin dos campos:** se quitan `comparaciones` (siempre `[]`) y `estructurasPRSValidadas` (siempre `7`). Confirmar que nadie los lea.
2. **Orden de validación:** el chequeo de registros CNF requeridos va **antes** de preparar el periodo. Mismo resultado en el caso válido; error más claro en el inválido.
3. **Variables de entorno:** `MALLA_PK` pasa a ser **obligatoria** (antes tenía valor por defecto `MALLA#PRIMAS_RENTAS`). Es el comportamiento deseado: no operar con valores por defecto de otro ambiente.
4. **`periodoNegocio: null` en el evento** sigue siendo error, como en el original.
5. **Se acepta `malla` en el evento** (opcional). Si no llega, se usa la del entorno. Es el primer paso hacia la lambda general.

---

## 4. Cómo correr las pruebas

```bash
cd spike-primas-rentas
python -m venv .venv
.venv\Scripts\activate                      # Linux/CloudShell: source .venv/bin/activate
pip install pytest pytest-cov
pytest --cov=configurador --cov-report=term-missing
```
Resultado del 2026-11-03: `37 passed`. Cobertura total 71 %: **98 % en el núcleo** y 0 % en `adaptadores\` y `composicion.py` (ver sección 5).

---

## 5. Lo que este spike NO hace

- **Los adaptadores no se probaron contra AWS.** `adaptadores\dynamo.py`, `s3.py` y `stepfunctions.py` están escritos pero sin pruebas. Se necesita probarlos con LocalStack (`Backend\localstack\`: DynamoDB, S3 y Step Functions están; Scheduler no) o con `moto`. Sin eso la cobertura global (71 %) no llega al 80 % del lineamiento de código seguro (`Docs\02-estandares-banco\`).
- **No está comparado contra el código desplegado:** solo contra su antecesora. Falta la prueba de equivalencia: mismo `cnf` de entrada, mismos 4 JSON de salida (propuesta: tomar el `cnf` real de `niif-dev-cnf-mallas` DEV y comparar los JSON publicados en `s3://niif-dev-assets/primas-rentas/`).
- **No incluye `_evento_desde_prg` ni `_resolver_where_periodo`** (ver el aviso de arriba).
- **El regex `FEC_APROBACION_PENSION`** se conservó, pero el original lo comenta como "identificado en ARP". **Pregunta abierta:** ¿aplica a Rentas?
- **No hay IaC ni empaquetado.** Falta el módulo de Terraform y el paquete liviano (sin numpy ni pandas, que el original no usa).

---

## 6. Camino a la lambda general

1. Pasar este spike a un repositorio Git (hoy no hay) con CI: `pytest --cov` y SonarQube, según los lineamientos del banco.
2. Escribir pruebas de los adaptadores con LocalStack y la prueba de equivalencia con datos reales.
3. Agregar al catálogo `mallas.py` las otras 5 mallas (auxilios, ARL, endosos, ARP, RVI). Si las diferencias entre mallas son solo datos, el código no cambia. Si alguna tiene lógica propia (por ejemplo, `fecDesde`/`fecHasta` en auxilios), va como una estrategia pequeña por malla, no como una copia.
4. Largo plazo: leer el catálogo de `cnf-mallas` en lugar de `mallas.py`.
5. Un solo `STATE_MACHINE_ARN` derivado de la malla (`niif-dev-<slug>`) y un solo contrato de entrada con la Step Function (`..\step-functions-niif.md`, SF-01).
6. Apuntar todos los *schedules* de `niif-dev-manager-configuration` a la lambda general, con la malla en el `Input`; retirar las 6 copias.
