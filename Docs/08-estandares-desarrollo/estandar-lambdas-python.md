# Estándar de desarrollo: lambdas y componentes Python del proyecto NIIF 17

**Versión:** 1.0 (2026-11-03). **Aplica a:** todo código Python nuevo o refactorizado del proyecto (lambdas de AWS y, con la adaptación de la sección 9, jobs de Glue).
**Implementación de referencia:** `Backend\Lamnbdas\inventario-aws\t2 unificadas\sc-niif-configurador-mallas\`.

Es un estándar propio del equipo. Concreta, para el código, lo que ya exigen los documentos del banco: versionamiento V3 (nombres, ramas, "sin código quemado"), código seguro con SonarQube (cobertura mayor a 80 %), HU de Gobierno de Ingestas (metadata, taxonomía de errores, `ejecucion_id`, logs y métricas) y HU de DevOps (convención de nombres AWS, IaC y "build once, deploy many"). Si algo de aquí choca con un documento del banco, manda el del banco.

---

## 1. Nombres y estilo de código

El gobierno DevOps NIIF 17 (`Docs\02-estandares-banco\GobiernoVersionameinto-DevOps.docx`) fija los nombres de recursos y repositorios. Los repositorios los crea y versiona DevOps.

| Elemento | Regla | Ejemplo |
|---|---|---|
| Recurso AWS (lambda o job de Glue) | `niif-{env}-{nombre}` en kebab-case | `niif-dev-configurador-mallas` |
| Repositorio (lo crea DevOps) | Lambda: `sc-datalake-niif-{nombre}`. Job: `data-datalake-niif-{nombre}` | `sc-datalake-niif-configurador-mallas` |
| Entrega a DevOps | Zip con el nombre del recurso; DevOps hace el commit | `niif-dev-configurador-mallas.zip` |
| Paquete Python | snake_case, en inglés | `mesh_configurator` |

**Estilo de código: PEP 8, el estándar de Python.**
- Sangría de **4 espacios**, sin tabuladores. Líneas de 100 caracteres como máximo (se valida con `ruff` o `flake8`).
- **Todos los identificadores en inglés:** módulos, paquetes, funciones, métodos, variables, parámetros, clases y constantes. No se mezclan idiomas.
- `snake_case` para módulos, funciones, métodos y variables; `PascalCase` para clases; `UPPER_SNAKE_CASE` para constantes. Ejemplos: `build_period()`, `MeshSpec`, `MAX_RETRIES`.
- Los **contratos externos** conservan su nombre original aunque estén en español: campos de DynamoDB (`periodoNegocio`, `scheduleEstado`), llaves de JSON, nombres de recursos AWS y valores de estado (`PENDIENTE`). Se declaran como constantes con nombre en inglés (`PERIOD_FIELD = "periodoNegocio"`).
- Imports en orden: estándar, terceros y propios, en bloques separados. Anotaciones de tipo en toda función pública.
- Los mensajes de log van en español (decidido el 2026-11-10, ver sección 5). **Propuesta (sin confirmar):** docstrings y comentarios también en español.

---

## 2. Estructura del proyecto: arquitectura hexagonal

Se usa **arquitectura hexagonal (puertos y adaptadores)**: la lógica de negocio queda en el centro, sin depender de AWS, y todo lo externo entra o sale por interfaces. Es el patrón más usado en la industria para servicios que deben probarse sin la nube y cambiar de infraestructura sin tocar reglas de negocio.

```
<repositorio>/
├── README.md                    Propósito, contrato, variables, permisos, despliegue
├── pyproject.toml               Dependencias, pytest y cobertura mínima
├── requirements-dev.txt
├── sonar-project.properties
├── config/                      Metadata por ambiente (p. ej. catalogo-<x>.<amb>.json)
├── eventos/                     Eventos de ejemplo para invocar la lambda
├── scripts/empaquetar.sh        Genera el artefacto
├── src/
│   ├── lambda_function.py       Punto de entrada que exige DevOps: solo llama al handler del paquete
│   └── <package>/
│       ├── domain/              Reglas de negocio puras
│       ├── application/         Casos de uso y puertos
│       ├── adapters/
│       │   ├── inbound/         Handler de Lambda (y otros disparadores)
│       │   └── outbound/        DynamoDB, S3, Step Functions, CloudWatch...
│       └── config/              Lectura del entorno y raíz de composición
└── test/                        Pruebas (test_lambda_function.py y las demás), dobles en memoria y prueba de arquitectura
```

### Qué va en cada capa

| Capa | Contiene | No puede |
|---|---|---|
| `domain` | Modelos (contratos de entrada y salida), reglas puras, validaciones, errores con su clasificación | Importar `boto3`, `botocore` ni otra capa. Leer el entorno. Escribir logs |
| `application` | Casos de uso (orquestan el dominio y los puertos), idempotencia, puertos (`Protocol`) y el catálogo de eventos y métricas | Importar adaptadores, configuración o `boto3` |
| `adapters/inbound` | El handler: traduce el evento al caso de uso y la respuesta al formato de salida; registra y relanza errores | Importar adaptadores de salida o `boto3` (los pide a la configuración) |
| `adapters/outbound` | Implementación de cada puerto con `boto3`; traducción de errores de AWS a la taxonomía | Contener reglas de negocio |
| `config` | Lectura y validación de variables de entorno; **raíz de composición** (único lugar que conecta puertos con adaptadores) | Contener reglas de negocio |

**Regla de dependencias:** `domain ← application ← adapters`; solo `config` conoce a todos. Se verifica con una **prueba de arquitectura** (`test/test_architecture.py`) que analiza los imports y falla si una capa importa lo que no debe. Esa prueba es obligatoria en todo repositorio.

---

## 3. Código limpio y SonarQube

1. **Sin valores quemados.** Nombres de tablas, buckets, rutas, ARN, cuentas, regiones y cualquier dato que cambie por ambiente o por caso de uso van en variables de entorno o en metadata versionada (`config/`, DynamoDB). La cuenta y la región se leen del contexto de ejecución. Lo único fijo en el código son los **contratos** (prefijos de llave, nombres de campos), declarados como constantes con nombre.
2. **El motor no decide, obedece** (HU-GI): la configuración se valida al arrancar y, si falta algo, se falla con un error clasificado. No hay valores por defecto de negocio implícitos.
3. **Una responsabilidad por función y por módulo.** Funciones cortas (como guía, menos de 40 líneas y complejidad cognitiva menor a 15, el umbral de SonarQube). Sin código muerto ni comentarios que repiten el código; los comentarios explican el **porqué**.
4. **Validar la entrada en el borde:** el evento se convierte en un modelo tipado (`dataclass`) con validaciones estrictas (por ejemplo, booleanos que sean `bool`, no texto).
5. **Inmutabilidad por defecto:** `@dataclass(frozen=True)` para modelos y especificaciones; las funciones puras no modifican sus argumentos (copian).
6. **Sin dependencias innecesarias.** `boto3` viene en el runtime de Lambda y no se empaqueta. Cada librería nueva necesita justificación (paquetes de 45 MB con pandas sin usar son el antiejemplo actual).
7. **Sin secretos en código ni en logs.** Credenciales en Secrets Manager (HU-DO-14). No se registran datos personales (Ley 1581).
8. **Quality Gate:** cero issues bloqueantes o críticos nuevos y cobertura de código nuevo mayor a 80 % frente a la línea base (lineamiento de código seguro). El repositorio incluye `sonar-project.properties` con la ruta de `coverage.xml`.

---

## 4. Errores

- Todos los errores propios heredan de una clase base con **clasificación** (HU-GI-06): `code`, `domain`, `severity`, `recoverable`, `action`, `recipient`.
- Dominios mínimos: `ENTRADA`, `CONFIGURACION`, `CONCURRENCIA`, `PERMISOS`, `PLATAFORMA`, `ORQUESTACION`. Códigos con el formato `<COMPONENTE>-<DOM>-<NNN>` (p. ej. `CFG-CNF-004`), documentados en el README.
- Los adaptadores de salida **traducen** los errores de AWS (`AccessDenied` → permisos, `Throttling` → plataforma recuperable, `ConditionalCheckFailed` → concurrencia, `NotFound` → configuración).
- El handler registra el error y **lo relanza**, para que Lambda lo cuente en `Errors` y aplique reintentos y destino de fallo. Nunca se silencia un error.

---

## 5. Logs y métricas (CloudWatch)

- **Mensajes de log en español** (decisión del usuario, 2026-11-10). Las llaves del JSON siguen esta sección.
- **Toda línea de log debe permitir rastrear la solicitud.** Campos obligatorios: `ejecucion_id` (ID de punta a punta), `request_id` (`aws_request_id` en lambda o `JOB_RUN_ID` en Glue) y, desde la certificación, `cert_code`. Además, el contexto de negocio: `malla` o `use_case_id` y `periodo_negocio`. Un log sin `ejecucion_id` es un defecto.
- **Una línea JSON por evento**, con `marcaTiempo`, `nivel`, `evento`, `funcion`, `request_id`, `ejecucion_id` y el contexto del negocio (por ejemplo, `malla` y `periodo_negocio`).
- **Catálogo cerrado de eventos** (enum en `application/ports.py`): no se registran textos libres para hechos del negocio.
- **`ejecucion_id`** se genera una sola vez por corrida: es el nombre de la ejecución de la Step Function, que crea el configurador. Se propaga a cada job de Glue como `--EJECUCION_ID`, a los registros de DynamoDB (también junto al `certCode`), a los metadatos de S3, al correo y a la cadena de certificar y transportar (HU-GI-05). **Debe llegar al 100 %, por diseño y no por suerte:**
  1. **Una sola fuente:** la Step Function inyecta `$$.Execution.Name` en **todas** sus tareas (jobs, lambdas, registros de DynamoDB). Ese valor existe en cualquier ejecución, también si se arranca a mano. Las 6 definiciones salen de una sola plantilla, no de copias.
  2. **Viaja dentro del dato:** desde la certificación, el ID se escribe en el registro de DynamoDB. Así cada evento del stream (trigger, copy, move) y cada enlace lo llevan sin depender de que alguien lo pase.
  3. **Una prueba automática lo comprueba antes de entregar:** un script recorre las definiciones de las Step Functions y falla si alguna tarea no lleva `--EJECUCION_ID`. Las pruebas de cada componente verifican que el ID entra, sale y aparece en el log.
  4. **No hay entrada lateral:** los jobs y las lambdas del flujo solo se arrancan desde la Step Function o desde el registro; para reprocesar también se usa la Step Function.
  5. Como red de seguridad, si aun así faltara el ID, el componente emite una alarma clasificada. Esto debe ser imposible si se cumplen los cuatro puntos anteriores. Hoy no se cumple: ver ARQ-32 en `Docs-brechas
otificaciones-vs-arquitectura.md`.
- **Métricas en formato EMF**, sin llamadas extra a la API: como mínimo, éxito, fallo (por dominio) y duración.
- Nivel de log por variable (`NIVEL_LOG`, `INFO` por defecto). Formato de log de Lambda: `Text`, porque las líneas ya son JSON.

---

## 6. Idempotencia y concurrencia

Toda lambda que escriba o dispare procesos debe ser idempotente:
1. **Clave de idempotencia:** del evento (en Scheduler, `<aws.scheduler.execution-id>`) o el `aws_request_id`, que Lambda conserva en sus reintentos asíncronos.
2. **Registro por clave** en DynamoDB con escritura condicional (`attribute_not_exists` o fallido o bloqueo vencido); si ya terminó, se devuelve el resultado guardado.
3. **Cada efecto, idempotente por sí mismo:** escrituras condicionales (`ConditionExpression`), nombres de ejecución deterministas en Step Functions, contenido determinista en S3 (`sort_keys`).
4. **Concurrencia optimista** al actualizar configuración: condición sobre el valor anterior.

---

## 7. Pruebas

| Tipo | Qué cubre | Herramienta |
|---|---|---|
| Unitarias del dominio | Reglas puras, con casos límite | `pytest` |
| Caso de uso | Flujos completos con **dobles en memoria** de los puertos (`test/fakes.py`) | `pytest` |
| Adaptadores | Llamadas exactas a AWS y traducción de errores, **sin conectarse a AWS** | `botocore.stub.Stubber` |
| Handler | Respuesta, logs JSON y métricas, manejo de errores | `pytest` + `capsys` |
| Arquitectura | Regla de dependencias entre capas | `ast` (incluida en el repositorio) |
| Integración (opcional) | Adaptadores contra servicios emulados | LocalStack (`Backend\localstack\`) |

- Comando oficial de DevOps: `pytest test -v --cov=src --cov-fail-under=70` (el pipeline rechaza por debajo de 70 %). Nuestra meta es 80 % o más, por Sonar.
- Herramientas del documento de DevOps: Python 3.12.0, pytest 7.4.4, pytest-mock, moto y tox 4.14.1. `Stubber` y los dobles en memoria también valen.
- Nombres de prueba en inglés que describen el comportamiento (`test_same_event_twice_does_not_repeat_effects`).
- Datos de prueba ficticios, nunca datos reales ni de personas.

---

## 8. Empaquetado y despliegue

- **Entrega:** un zip con el nombre del recurso (`niif-<amb>-<nombre>.zip`) con `src/`, `test/`, `config/` y el README. DevOps lo versiona en su repositorio y lo despliega; nosotros no hacemos commits.
- **Build once, deploy many** (HU-DO): el mismo artefacto pasa por DEV, QA y PROD; lo que cambia es la configuración (variables y `config/<x>.<amb>.json`).
- Función, rol, permisos mínimos, tablas, invocación asíncrona (reintentos y destino de fallo) y alarmas se declaran en el IaC del proyecto (Terraform/Terragrunt). No se crean a mano en la consola.
- El README documenta el handler, las variables, los permisos mínimos y el plan de migración o de vuelta atrás.

---

## 9. Jobs de Glue (adaptación)

Los mismos principios, con una diferencia: Spark es infraestructura. Se separan las **transformaciones puras** (funciones que reciben y devuelven DataFrames, probadas con un Spark local) de la **lectura y escritura** (catálogo, S3, JDBC) y del **script principal**, que solo lee los argumentos, arma las piezas y llama a la transformación. Las rutas y los parámetros vienen de los argumentos del job y de la metadata, no del código.

---

## 10. Lista de verificación del Pull Request

- [ ] Los nombres de recursos y del zip siguen la sección 1.
- [ ] PEP 8: sangría de 4 espacios, identificadores en inglés, `snake_case` / `PascalCase` / `UPPER_SNAKE_CASE`; el linter pasa sin errores.
- [ ] La estructura es la de la sección 2 y la prueba de arquitectura pasa.
- [ ] No hay valores quemados ni secretos; la configuración se valida al arrancar.
- [ ] Los errores usan la taxonomía; ninguno se silencia.
- [ ] Logs JSON con `ejecucion_id` y eventos del catálogo; métricas EMF.
- [ ] Las escrituras y los disparos son idempotentes.
- [ ] Cobertura de código nuevo mayor a 80 % y Quality Gate de SonarQube en verde frente a la línea base.
- [ ] El README está actualizado (contrato, variables, permisos, despliegue).
- [ ] Commit `<tipo>-<TICKET>-<asunto>` y rama `feature/<TICKET>` (V3).
