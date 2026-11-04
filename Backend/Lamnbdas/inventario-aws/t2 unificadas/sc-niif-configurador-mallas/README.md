# sc-niif-configurador-mallas

Lambda **general y estandarizada** que reemplaza a las seis `niif-dev-configuration-<malla>` (primas rentas, primas endosos, primas ARL, auxilios funerarios, reserva matemática RVI y ARP). Valida la configuración de una malla, publica sus cuatro JSON (extracción, validación, remediación y SCD) y arranca su Step Function `niif-<amb>-<malla>`. Tarea T2 del roadmap (`Tareas\roadmap-cambios.md`).

- **Nombre del repositorio:** `sc-niif-configurador-mallas`, según el lineamiento de versionamiento V3 §5.2 (`<tipo>-<proyecto>-<complemento>`, tipo `sc` = serverless).
- **Nombre sugerido en AWS** (HU-DO, convención de nombres): función `datalake-<amb>-niif-configurador-mallas-lambda`, rol `datalake-<amb>-niif-configurador-mallas-role`.
- **Estándar de código:** `Docs\08-estandares-desarrollo\estandar-lambdas-python.md` (arquitectura hexagonal).
- **Estado:** listo para pruebas en DEV. **No está desplegado.** Ver "Antes de producción".

---

## Arquitectura (hexagonal: puertos y adaptadores)

```
src/configurador_mallas/
├── dominio/             Reglas puras. No importa AWS ni otras capas.
│   ├── modelos.py         Evento, Resultado, MallaSpec (contratos)
│   ├── errores.py         Taxonomía de errores (HU-GI-06)
│   ├── catalogo.py        Valida el catálogo de mallas
│   ├── periodo.py         Traslado del periodo a filtros y segmentos
│   ├── constructores.py   Los cuatro JSON de la malla
│   ├── validaciones.py    Validación de CNF y de lo generado
│   └── serializacion.py
├── aplicacion/          Casos de uso y puertos. Solo depende del dominio.
│   ├── configurar_malla.py  Caso de uso
│   ├── idempotencia.py      Control por clave
│   └── puertos.py           Interfaces de salida + catálogo de eventos y métricas
├── adaptadores/
│   ├── entrada/lambda_handler.py   Traduce el evento de Lambda al caso de uso
│   └── salida/                     dynamo.py, s3.py, stepfunctions.py, cloudwatch.py, errores_aws.py
└── configuracion/       Entorno (ajustes.py) y raíz de composición (composicion.py)
```

La regla de dependencias (`dominio ← aplicacion ← adaptadores`, y solo `configuracion` conecta todo) la hace cumplir `tests/test_arquitectura.py`: si alguien importa boto3 en el dominio, la prueba falla.

**Handler:** `configurador_mallas.adaptadores.entrada.lambda_handler.lambda_handler`

### Flujo de una invocación
1. Lee el catálogo de la malla y valida el evento (booleanos estrictos, periodo `YYYYMM`, modo permitido).
2. **Idempotencia:** toma la clave (`idempotencyKey` o el `aws_request_id`). Si ya terminó, devuelve el resultado guardado sin repetir nada.
3. Lee el periodo de la programación (`prg-programaciones`); si el evento trae otro, falla.
4. Lee `cnf-mallas`, aplica el periodo, construye y valida los cuatro JSON.
5. `publicar`: actualiza el periodo en CNF (escritura condicional) y escribe los JSON en S3.
6. `ejecutar`: arranca la Step Function con un **nombre determinista** y registra la ejecución en `ejc-ejecuciones` (solo si no existe).
7. Marca la clave como completada y emite logs y métricas.

---

## Contrato

### Evento de entrada
| Campo | Tipo | Obligatorio | Descripción |
|---|---|---|---|
| `malla` | texto | Sí (o variable `MALLA_PK`) | Código de la malla, con o sin `MALLA#`: `PRIMAS_RENTAS`, ... |
| `publicar` | booleano | No (`false`) | Escribe CNF y los JSON en S3 |
| `ejecutar` | booleano | No (igual a `publicar`) | Arranca la Step Function; exige `publicar=true` |
| `periodoNegocio` | texto `YYYYMM` | No | Solo confirma: debe coincidir con la programación |
| `modoEjecucion` | texto | No (`valoresPorDefecto` del catálogo: `NORMAL`) | Debe estar en `modosEjecucion` del catálogo |
| `origen` | texto | No (`valoresPorDefecto` del catálogo: `CONFIGURADOR`) | Quién invoca (`PROGRAMACION_GESTOR`, ...) |
| `idempotencyKey` | texto (1-128) | No (usa `aws_request_id`) | En Scheduler: `<aws.scheduler.execution-id>` |

Ejemplos en `eventos/`.

### Respuesta
`estado`, `malla`, `publicar`, `ejecutar`, `periodoNegocio`, `registrosCNF`, `estructurasPRSValidadas` (calculado: estructuras de origen distintas más la Gold), `ejecucionId`, `publicados[]`, `executionName`, `executionArn`, `ejcPK`, `ejcSK`, `reutilizado`.

### Entrada que recibe la Step Function
`ejcPK`, `ejcSK`, `malla`, `periodoNegocio`, `modoEjecucion`, `origen` y `ejecucionId`. Según `entradaStepFunction` del catálogo: `rutaConfigExtraccion` (todas menos las reservas matemáticas) y `fecDesde`/`fecHasta` (auxilios funerarios).

---

## Configuración (sin valores quemados)

### Variables de entorno
| Variable | Obligatoria | Ejemplo DEV |
|---|---|---|
| `CNF_TABLE` | Sí | `niif-dev-cnf-mallas` |
| `PRS_TABLE` | Sí | `niif-dev-prs-estructuras` |
| `PRG_TABLE` | Sí | `niif-dev-prg-programaciones` |
| `EJC_TABLE` | Sí | `niif-dev-ejc-ejecuciones` |
| `IDEMPOTENCIA_TABLE` | Sí | Tabla nueva sugerida: `datalake-dev-niif-configurador-idempotencia-dynamodb` (PK/SK texto, TTL en `expiraEn`) |
| `IDEMPOTENCIA_TTL_SEGUNDOS` | Sí | `604800` (7 días) |
| `CATALOGO_MALLAS_URI` | Sí | `s3://niif-dev-assets/configs/configurador/catalogo-mallas.dev.json` |
| `METRICAS_NAMESPACE` | Sí | `NIIF17/Configurador` |
| `MALLA_PK` | Solo en el paso T2-A | `MALLA#PRIMAS_RENTAS` |
| `NIVEL_LOG` | No (`INFO`) | `DEBUG`, `INFO`, `WARNING`, `ERROR` |

`AWS_REGION` la pone Lambda; la cuenta y la partición se toman del ARN de la función. Si falta una variable obligatoria, la función falla con `CFG-CNF-001` y la lista de las que faltan.

### Catálogo de mallas
`config/catalogo-mallas.<amb>.json`: todo lo que antes estaba quemado en cada copia. Sumar una malla = agregar una entrada. Se lee y valida una vez por contenedor; si falta un campo, falla con `CFG-CNF-002` (no hay valores implícitos).

| Campo | Qué define | Diferencia entre mallas |
|---|---|---|
| `modosEjecucion`, `valoresPorDefecto` | Modos permitidos; `modoEjecucion` y `origen` cuando el evento no los trae | Iguales para todas |
| `nombreCaso`, `prefijoEjecucion`, `stateMachineName`, `programacion`, `totalCiclos` | Identidad de la malla | Una por malla |
| `remediacion.estadoSinReglas` / `estadoConReglas` | Estado de definición exigido. Con `estadoConReglas`, depende de si `REMEDIACION` trae `reglasTransformativas` | `estadoConReglas` solo en RVI y ARP |
| `periodo.columnasFecha`, `operadoresFecha`, `columnaPeriodo` | Qué límites de los `where` se ajustan al periodo | Iguales hoy (regla de auxilios) |
| `periodo.ajustarSegmentacion`, `patronIdentificadorSegmento` | Si se ajusta y valida el periodo de la segmentación horizontal | Iguales hoy |
| `tablaExtraccion.camposOpcionales`, `omitirNulos` | Campos de la estructura que pasan a cada tabla del JSON de extracción | RVI y ARP agregan `reutilizacion_compartida` y omiten nulos |
| `entradaStepFunction.incluirRutaConfigExtraccion`, `incluirRangoFechas` | Campos opcionales de la entrada de la Step Function | Sin ruta: RVI y ARP. Con rango: auxilios |

> Auxilios funerarios sale del código desplegado. **Las otras cinco salen de la versión anterior** (`Backend\Lamnbdas\niif-dev-configuration-<malla>`) y están marcadas `POR_CONFIRMAR`. Sus reglas de periodo se igualaron a las de auxilios porque la versión desplegada agrega la lectura de PRG y el ajuste de filtros (ver `ORIGEN.md` de cada una); hay que confirmarlas con el código real (script `recolectar-faltantes.sh`).

---

## Idempotencia

| Riesgo | Cómo se evita |
|---|---|
| Lambda o Scheduler reintentan la misma invocación | Registro por clave en `IDEMPOTENCIA_TABLE` con escritura condicional: si ya terminó, se devuelve el resultado guardado |
| Dos invocaciones con la misma clave a la vez | La segunda recibe `CFG-CON-001` (recuperable); el bloqueo vence con el tiempo restante de la Lambda |
| Reintento después de una falla a medias | Cada paso es idempotente: el periodo ya aplicado no genera cambios en CNF, los JSON son iguales, el nombre de la ejecución es el mismo (Step Functions no crea otra con el mismo nombre y entrada) y el registro EJC es condicional |
| Cambio concurrente de CNF | Escritura condicional sobre el valor anterior: `CFG-CON-002` |

**No cubre:** dos invocaciones con **claves distintas** para la misma malla y periodo (por ejemplo, dos programadores sobre la misma tabla, PP-03). Eso se corrige dejando un solo programador.

---

## Errores y logs en CloudWatch

Cada error sale en una línea JSON con `evento: CORRIDA_FALLIDA`, el `ejecucion_id`, la malla y el bloque `error` (taxonomía HU-GI-06), y suma la métrica `CorridaFallida` por dominio. Luego se relanza: Lambda lo cuenta en `Errors` y aplica reintentos y destino de fallo.

| Código | Dominio | Recuperable | Causa |
|---|---|---|---|
| `CFG-ENT-001` a `005` | ENTRADA | No | Evento inválido; periodo mal formado (002); periodo distinto a la programación (003); sin malla (004); modo no permitido (005) |
| `CFG-CNF-001` a `010` | CONFIGURACION | No | Entorno (001); catálogo (002); malla fuera del catálogo (003); registros CNF (004); estructura PRS (005); programación (006); remediación (007); JSON generado (008); recurso S3/AWS no encontrado (009); nombre de caso (010) |
| `CFG-CON-001`, `002` | CONCURRENCIA | Sí | Clave en curso; CNF modificada |
| `CFG-PER-001` | PERMISOS | No | Acceso denegado (destinatario: Seguridad de la información) |
| `CFG-PLT-000`, `001` | PLATAFORMA | Sí | No clasificado; límites o indisponibilidad de AWS |
| `CFG-ORQ-001` | ORQUESTACION | No | Ya existe una ejecución con ese nombre y otra entrada |

**Eventos** (catálogo cerrado): `CORRIDA_INICIADA`, `IDEMPOTENCIA_REUTILIZADA`, `CONFIGURACION_LEIDA`, `CONFIGURACION_PUBLICADA`, `EJECUCION_INICIADA`, `EJECUCION_YA_REGISTRADA`, `CORRIDA_FINALIZADA`, `CORRIDA_FALLIDA`.
**Métricas EMF** (dimensiones `Funcion`, `Malla`): `CorridaExitosa`, `CorridaFallida` (+`Dominio`), `CorridaReutilizada`, `DuracionMs`.

Consulta en Logs Insights:
```
fields @timestamp, evento, malla, ejecucion_id, error.codigo, error.mensaje
| filter evento = "CORRIDA_FALLIDA" | sort @timestamp desc
```

---

## Pruebas y calidad

```bash
pip install -r requirements-dev.txt
pytest --cov --cov-report=xml:coverage.xml --junitxml=reports/pytest.xml
```
- 245 pruebas: dominio, caso de uso con dobles en memoria, adaptadores con `botocore.stub.Stubber` (sin AWS), handler, la regla de arquitectura y la **equivalencia con las seis lambdas actuales**.
- `tests/test_equivalencia_lambdas_actuales.py` ejecuta el código de cada `niif-dev-configuration-<malla>` con AWS simulado y esta lambda con los mismos datos, y compara los 4 JSON y sus metadatos, la transacción sobre CNF, la entrada de la Step Function, el registro EJC y la respuesta. Toma las lambdas de `LAMBDAS_ACTUALES_DIR` (por defecto, la carpeta del repositorio) y se omite si no están.
- Cobertura de ramas: **99 %** (mínimo configurado 85 %; el lineamiento exige más de 80 % en código nuevo).
- SonarQube: `sonar-project.properties` toma `coverage.xml` y `reports/pytest.xml`.

---

## Despliegue

1. `bash scripts/empaquetar.sh` → `dist/sc-niif-configurador-mallas-<versión>.zip` (solo el código; boto3 viene en el runtime).
2. Configuración de la función (en el IaC del proyecto, no a mano): Python 3.12, 512 MB (igual que las seis actuales), 300 s, el handler indicado arriba, las variables de entorno, **formato de log `Text`** (las líneas ya son JSON y EMF las necesita sin envolver), invocación asíncrona con 2 reintentos y destino de fallo (cola SQS).
3. Subir el catálogo del ambiente a la ruta de `CATALOGO_MALLAS_URI`.

### Permisos mínimos del rol
| Acción | Recurso |
|---|---|
| `dynamodb:Query`, `dynamodb:GetItem`, `dynamodb:UpdateItem` | `CNF_TABLE` |
| `dynamodb:GetItem` | `PRS_TABLE`, `PRG_TABLE` |
| `dynamodb:PutItem` | `EJC_TABLE` |
| `dynamodb:PutItem`, `dynamodb:GetItem`, `dynamodb:UpdateItem` | `IDEMPOTENCIA_TABLE` |
| `s3:GetObject` | El catálogo y las plantillas de remediación |
| `s3:PutObject` | Las rutas `archivoActivoUri` de las 6 mallas |
| `states:StartExecution` | Las 6 máquinas `niif-<amb>-<malla>` |
| `kms:Decrypt`, `kms:GenerateDataKey` | Claves de las tablas y del bucket (en DEV: `a8eabea1-...` y `c417247b-...`) |
| Logs | Política administrada `AWSLambdaBasicExecutionRole` |

### Migración (roadmap T2)
- **T2-A:** desplegar este artefacto en las 6 funciones actuales, cada una con `MALLA_PK`. No cambia el manager ni los *schedules*. Malla por malla, con alias para volver atrás.
- **T2-B:** una sola función; el manager envía `"malla"` y `"idempotencyKey": "<aws.scheduler.execution-id>"` en el `Input` del *schedule*; retirar las 6 funciones.

---

## Diferencias con las lambdas actuales (intencionales)
La auditoría completa está en `..\AUDITORIA-configurador-mallas.md`.
1. `publicar` y `ejecutar` deben ser booleanos; antes `"false"` (texto) se tomaba como verdadero.
2. El nombre de la ejecución es `<prefijo>-<periodo>-auto-<ejecucionId>` (determinista) en vez de llevar fecha y hora.
3. La Step Function, el registro EJC y los metadatos de S3 llevan además `ejecucionId`.
4. `estructurasPRSValidadas` se calcula (antes era un número fijo en cada copia); `comparaciones` se retira (siempre salía vacía); se agregan `ejecucionId` y `reutilizado`.
5. El registro en `ejc-ejecuciones` no sobrescribe uno existente.
6. `modoEjecucion` solo acepta los valores de `modosEjecucion` del catálogo (antes, cualquier texto).
7. Idempotencia por clave (antes, un reintento repetía la publicación y arrancaba otra ejecución).

## Antes de producción
- [ ] Confirmar el catálogo de las 5 mallas `POR_CONFIRMAR` con el código real.
- [ ] Prueba de equivalencia por malla: los 4 JSON generados deben ser iguales a los que publica hoy la lambda actual para el mismo periodo.
- [ ] Crear la tabla de idempotencia y el rol en el IaC (Terragrunt) del proyecto.
- [ ] Dejar un solo programador sobre `prg-programaciones` (PP-03).
- [ ] Repositorio Git con pipeline y línea base de SonarQube (lineamiento de código seguro).
