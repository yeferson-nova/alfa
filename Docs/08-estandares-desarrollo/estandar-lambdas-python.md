# Estándar de desarrollo: lambdas y componentes Python del proyecto NIIF 17

**Versión:** 1.0 (2026-11-03). **Aplica a:** todo código Python nuevo o refactorizado del proyecto (lambdas de AWS y, con la adaptación de la sección 9, jobs de Glue).
**Implementación de referencia:** `Backend\Lamnbdas\inventario-aws\t2 unificadas\sc-niif-configurador-mallas\`.

Es un estándar propio del equipo. Concreta, para el código, lo que ya exigen los documentos del banco: versionamiento V3 (nombres, ramas, "sin código quemado"), código seguro con SonarQube (cobertura mayor a 80 %), HU de Gobierno de Ingestas (metadata, taxonomía de errores, `ejecucion_id`, logs y métricas) y HU de DevOps (convención de nombres AWS, IaC y "build once, deploy many"). Si algo de aquí choca con un documento del banco, manda el del banco.

---

## 1. Nombres

| Elemento | Regla | Ejemplo |
|---|---|---|
| Repositorio o carpeta del componente | `<tipo>-<proyecto>-<complemento>` en kebab-case (V3 §5.2). Lambdas: tipo `sc` | `sc-niif-configurador-mallas` |
| Paquete Python | snake_case, el complemento del repositorio | `configurador_mallas` |
| Recurso AWS | `<prefijo>-<amb>-<proyecto>-<recurso>-<tipo>`, máximo 64 caracteres (HU-DO) | `datalake-dev-niif-configurador-mallas-lambda` |
| Rol IAM | Igual que el recurso, con `-role` | `datalake-dev-niif-configurador-mallas-role` |
| Artefacto | `<repositorio>-<X.Y.Z>.zip` (semver) | `sc-niif-configurador-mallas-1.0.0.zip` |
| Módulos, funciones y variables | snake_case; clases en PascalCase; constantes en MAYÚSCULAS. Idioma: español, sin mezclar | `preparar_periodo`, `MallaSpec` |

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
├── src/<paquete>/
│   ├── dominio/                 Reglas de negocio puras
│   ├── aplicacion/              Casos de uso y puertos
│   ├── adaptadores/
│   │   ├── entrada/             Handler de Lambda (y otros disparadores)
│   │   └── salida/              DynamoDB, S3, Step Functions, CloudWatch...
│   └── configuracion/           Lectura del entorno y raíz de composición
└── tests/                       Pruebas unitarias, dobles en memoria y prueba de arquitectura
```

### Qué va en cada capa

| Capa | Contiene | No puede |
|---|---|---|
| `dominio` | Modelos (contratos de entrada y salida), reglas puras, validaciones, errores con su clasificación | Importar `boto3`, `botocore` ni otra capa. Leer el entorno. Escribir logs |
| `aplicacion` | Casos de uso (orquestan el dominio y los puertos), idempotencia, puertos (`Protocol`) y el catálogo de eventos y métricas | Importar adaptadores, configuración o `boto3` |
| `adaptadores/entrada` | El handler: traduce el evento al caso de uso y la respuesta al formato de salida; registra y relanza errores | Importar adaptadores de salida o `boto3` (los pide a la configuración) |
| `adaptadores/salida` | Implementación de cada puerto con `boto3`; traducción de errores de AWS a la taxonomía | Contener reglas de negocio |
| `configuracion` | Lectura y validación de variables de entorno; **raíz de composición** (único lugar que conecta puertos con adaptadores) | Contener reglas de negocio |

**Regla de dependencias:** `dominio ← aplicacion ← adaptadores`; solo `configuracion` conoce a todos. Se verifica con una **prueba de arquitectura** (`tests/test_arquitectura.py`) que analiza los imports y falla si una capa importa lo que no debe. Esa prueba es obligatoria en todo repositorio.

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

- Todos los errores propios heredan de una clase base con **clasificación** (HU-GI-06): `codigo`, `dominio`, `severidad`, `recuperable`, `accion`, `destinatario`.
- Dominios mínimos: `ENTRADA`, `CONFIGURACION`, `CONCURRENCIA`, `PERMISOS`, `PLATAFORMA`, `ORQUESTACION`. Códigos con el formato `<COMPONENTE>-<DOM>-<NNN>` (p. ej. `CFG-CNF-004`), documentados en el README.
- Los adaptadores de salida **traducen** los errores de AWS (`AccessDenied` → permisos, `Throttling` → plataforma recuperable, `ConditionalCheckFailed` → concurrencia, `NotFound` → configuración).
- El handler registra el error y **lo relanza**, para que Lambda lo cuente en `Errors` y aplique reintentos y destino de fallo. Nunca se silencia un error.

---

## 5. Logs y métricas (CloudWatch)

- **Una línea JSON por evento**, con `marcaTiempo`, `nivel`, `evento`, `funcion`, `request_id`, `ejecucion_id` y el contexto del negocio (por ejemplo, `malla` y `periodo_negocio`).
- **Catálogo cerrado de eventos** (enum en `aplicacion/puertos.py`): no se registran textos libres para hechos del negocio.
- **`ejecucion_id`** generado una vez por corrida y propagado a la Step Function, a los metadatos de S3 y a los registros de DynamoDB (HU-GI-05).
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
| Caso de uso | Flujos completos con **dobles en memoria** de los puertos (`tests/fakes.py`) | `pytest` |
| Adaptadores | Llamadas exactas a AWS y traducción de errores, **sin conectarse a AWS** | `botocore.stub.Stubber` |
| Handler | Respuesta, logs JSON y métricas, manejo de errores | `pytest` + `capsys` |
| Arquitectura | Regla de dependencias entre capas | `ast` (incluida en el repositorio) |
| Integración (opcional) | Adaptadores contra servicios emulados | LocalStack (`Backend\localstack\`) |

- Cobertura mínima configurada en `pyproject.toml` (`fail_under`), nunca por debajo de 80 %.
- Nombres de prueba que describen el comportamiento (`test_mismo_evento_dos_veces_no_repite_efectos`).
- Datos de prueba ficticios, nunca datos reales ni de personas.

---

## 8. Empaquetado y despliegue

- `scripts/empaquetar.sh` genera un único artefacto con solo el código del paquete.
- **Build once, deploy many** (HU-DO): el mismo artefacto pasa por DEV, QA y PROD; lo que cambia es la configuración (variables y `config/<x>.<amb>.json`).
- Función, rol, permisos mínimos, tablas, invocación asíncrona (reintentos y destino de fallo) y alarmas se declaran en el IaC del proyecto (Terraform/Terragrunt). No se crean a mano en la consola.
- El README documenta el handler, las variables, los permisos mínimos y el plan de migración o de vuelta atrás.

---

## 9. Jobs de Glue (adaptación)

Los mismos principios, con una diferencia: Spark es infraestructura. Se separan las **transformaciones puras** (funciones que reciben y devuelven DataFrames, probadas con un Spark local) de la **lectura y escritura** (catálogo, S3, JDBC) y del **script principal**, que solo lee los argumentos, arma las piezas y llama a la transformación. Las rutas y los parámetros vienen de los argumentos del job y de la metadata, no del código.

---

## 10. Lista de verificación del Pull Request

- [ ] El nombre del repositorio, del paquete y de los recursos sigue la sección 1.
- [ ] La estructura es la de la sección 2 y la prueba de arquitectura pasa.
- [ ] No hay valores quemados ni secretos; la configuración se valida al arrancar.
- [ ] Los errores usan la taxonomía; ninguno se silencia.
- [ ] Logs JSON con `ejecucion_id` y eventos del catálogo; métricas EMF.
- [ ] Las escrituras y los disparos son idempotentes.
- [ ] Cobertura de código nuevo mayor a 80 % y Quality Gate de SonarQube en verde frente a la línea base.
- [ ] El README está actualizado (contrato, variables, permisos, despliegue).
- [ ] Commit `<tipo>-<TICKET>-<asunto>` y rama `feature/<TICKET>` (V3).
