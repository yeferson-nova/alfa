---
tipo: estandar
estado: vigente
actualizado: 2026-11-11
---
# Estándar propio de desarrollo Python (hexagonal)

Fuente: `Docs\08-estandares-desarrollo\estandar-lambdas-python.md` (decidido por el usuario el 2026-11-03). Referencia: `Backend\Lamnbdas\inventario-aws\t2 unificadas\sc-niif-configurador-mallas\`.

## Reglas verificables
- **Estructura hexagonal obligatoria:** `src/<paquete>/{dominio, aplicacion, adaptadores/{entrada,salida}, configuracion}`. Dependencias `dominio ← aplicacion ← adaptadores`; solo `configuracion` conecta todo. `tests/test_arquitectura.py` lo verifica.
- **Nombres:** repo `sc-<proyecto>-<complemento>` ([[est-versionamiento-codigo]]); AWS `<prefijo>-<amb>-<proyecto>-<recurso>-<tipo>` ([[hu-devops]]).
- **Sin valores quemados:** entorno validado al arrancar y metadata versionada en `config/`. Cuenta y región desde el ARN de la función.
- **Errores** con taxonomía `codigo/dominio/severidad/recuperable/accion/destinatario` ([[hu-gobierno-ingestas]]); se registran y se relanzan.
- **Logs** JSON con `ejecucion_id`, catálogo cerrado de eventos y métricas EMF.
- **Idempotencia:** clave (`idempotencyKey` o `aws_request_id`), registro condicional en DynamoDB y efectos idempotentes.
- **Pruebas:** dobles en memoria, `Stubber` para adaptadores, cobertura mayor a 80 % ([[est-codigo-seguro]]), `sonar-project.properties`.
- **Despliegue:** artefacto único y IaC; formato de log de Lambda `Text`.
- Glue: transformaciones puras separadas de la lectura y escritura (sección 9).

## Relevancia
- Aplica a T2 (lambda general) y a todo código nuevo de T1 y T3 ([[tareas-aws]]).

- **Con el gobierno DevOps NIIF 17 (2026-11-10) no hay choque:** DevOps se encarga de la infraestructura y nosotros del código. Seguimos con lambdas modulares en hexagonal; `/src/lambda_function.py` es solo el handler y las pruebas van en `/test/`. Entregamos un zip con el nombre de la lambda y DevOps hace el commit. Ver [[est-gobierno-devops-niif]].
- **Estilo de código (decisión del usuario, 2026-11-10):** PEP 8, con sangría de 4 espacios y **todos los identificadores en inglés** (`snake_case`, `PascalCase`, `UPPER_SNAKE_CASE`). Las capas pasan a llamarse `domain`, `application`, `adapters/{inbound,outbound}` y `config`; `src/lambda_function.py` es el handler y las pruebas van en `test/`. Los contratos externos en español (campos de DynamoDB, llaves de JSON, estados) no cambian y se declaran como constantes en inglés. **Mensajes de log en español** (decidido) y cada log con `ejecucion_id` + `request_id` (+ `cert_code` desde certificación) para rastrear la solicitud ([[notificaciones-vs-arquitectura]] ARQ-32). Sin confirmar: idioma de docstrings y comentarios (propuesta: español). El código actual (T2 y `niif-dev-notification-certification` hexagonal) usa nombres en español y hay que migrarlo antes de entregarlo.
- **Configuración por ambiente (decisión del usuario, 2026-11-11):** JSON en S3 por ambiente, con la fuente en Git y desplegado por el pipeline de DevOps. Argumentos solo para lo que cambia por ejecución y para los apuntadores. Sin valores por defecto y una sola fuente por valor. Los tags no son configuración. Detalle en la skill (`references/configuracion-por-ambiente.md`) y brechas del job en [[job-notificacion-ses]].
