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
| `modoEjecucion` | texto | No (`NORMAL`) | Debe estar en `modosEjecucion` del catálogo |
| `origen` | texto | No (`INVOCACION_DIRECTA`) | Quién invoca (`PROGRAMACION_GESTOR`, ...) |
| `idempotencyKey` | texto (1-128) | No (usa `aws_request_id`) | En Scheduler: `<aws.scheduler.execution-id>` |

Ejemplos en `eventos/`.

### Respuesta
`estado`, `malla`, `publicar`, `ejecutar`, `periodoNegocio`, `registrosCNF`, `ejecucionId`, `publicados[]`, `executionName`, `executionArn`, `ejcPK`, `ejcSK`, `reutilizado`.

### Entrada que recibe la Step Function
`ejcPK`, `ejcSK`, `malla`, `periodoNegocio`, `modoEjecucion`, `rutaConfigExtraccion`, `origen`, `ejecucionId`, y `fecDesde`/`fecHasta` si el catálogo lo indica (auxilios funerarios).

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
`config/catalogo-mallas.<amb>.json`: todo lo que antes estaba quemado en cada copia (ciclos, prefijo de ejecución, Step Function, llave de programación, estados de remediación permitidos, columnas de fecha, patrón de segmentos y si se envía el rango de fechas). Sumar una malla = agregar una entrada. Se lee una vez por contenedor.

> Auxilios funerarios sale del código desplegado. **Las otras cinco salen de la versión anterior** (`configurador-<malla>`) y están marcadas `POR_CONFIRMAR`: hay que confirmarlas con el código real (script `recolectar-faltantes.sh`).

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
- 174 pruebas: dominio, caso de uso con dobles en memoria, adaptadores con `botocore.stub.Stubber` (sin AWS), handler y la regla de arquitectura.
- Cobertura de ramas: **99 %** (mínimo configurado 85 %; el lineamiento exige más de 80 % en código nuevo).
- SonarQube: `sonar-project.properties` toma `coverage.xml` y `reports/pytest.xml`.

---

## Despliegue

1. `bash scripts/empaquetar.sh` → `dist/sc-niif-configurador-mallas-<versión>.zip` (solo el código; boto3 viene en el runtime).
2. Configuración de la función (en el IaC del proyecto, no a mano): Python 3.12, 256 MB, 300 s, el handler indicado arriba, las variables de entorno, **formato de log `Text`** (las líneas ya son JSON y EMF las necesita sin envolver), invocación asíncrona con 2 reintentos y destino de fallo (cola SQS).
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

## Diferencias con las lambdas actuales (validar)
1. `publicar` y `ejecutar` deben ser booleanos; antes `"false"` (texto) se tomaba como verdadero.
2. El nombre de la ejecución es `<prefijo>-<periodo>-auto-<ejecucionId>` (determinista) en vez de llevar fecha y hora.
3. La Step Function recibe además `ejecucionId`; `fecDesde`/`fecHasta` solo donde el catálogo lo indica.
4. La respuesta ya no trae `estructurasPRSValidadas` ni `comparaciones` (estaban fijos o vacíos); agrega `ejecucionId` y `reutilizado`.
5. El registro en `ejc-ejecuciones` no sobrescribe uno existente.

## Antes de producción
- [ ] Confirmar el catálogo de las 5 mallas `POR_CONFIRMAR` con el código real.
- [ ] Prueba de equivalencia por malla: los 4 JSON generados deben ser iguales a los que publica hoy la lambda actual para el mismo periodo.
- [ ] Crear la tabla de idempotencia y el rol en el IaC (Terragrunt) del proyecto.
- [ ] Dejar un solo programador sobre `prg-programaciones` (PP-03).
- [ ] Repositorio Git con pipeline y línea base de SonarQube (lineamiento de código seguro).
