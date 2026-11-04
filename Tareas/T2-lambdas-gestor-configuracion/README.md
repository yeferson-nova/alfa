# T2. Unificar el modelo de programación de las lambdas del gestor de configuración

**Estado:** en análisis, bloqueado para implementación (sin acceso al código). **Es la base de T1 y T3.**

## Qué se pidió
Arreglar las lambdas del gestor de configuración para que usen un solo modelo de programación.

## El problema
El gestor de configuración define cómo se comporta cada proceso de los pasos 2 y 3 (qué casos de uso existen, a quién se notifica, qué se transporta). Sus lambdas están escritas con modelos distintos. "Modelo" puede referirse a la estructura del código, el manejo de errores, la forma de leer la configuración, el logging o el empaquetado; no sabemos cuáles difieren. La mezcla trae cuatro consecuencias:
- cada lambda se mantiene y se prueba de forma distinta;
- los errores y logs no tienen un formato común, así que no se puede montar el monitoreo de T1 (1c);
- añadir una validación (T3) o un caso de uso implica tocar código distinto en cada lambda;
- es más probable que haya valores quemados, secretos en código o permisos amplios.

## Estado actual
**Actualizado 2026-11-03:** ya tenemos el código desplegado, extraído de la cuenta a `Backend\inventario\`. El alcance de trabajo quedó en 7 lambdas (6 `configuration-<malla>` + `manager-configuration`) y 6 Step Functions; lo demás está apartado como pruebas. El inventario está en `Backend\Lamnbdas\inventario-aws\lambdas.md`, el flujo de orquestación en `step-functions-niif.md` y el spike de la lambda general en `spike-primas-rentas\`, todos en esa carpeta. Lo principal:
- Todo es Python. Conviven **cuatro generaciones** del gestor, no solo estilos de código distintos:
  - G1: `datalake-dev-niif-*`;
  - G2: programador de cargues → configurador de extracción → iniciador → finalizador;
  - G3: `configurador-<malla>` ×6;
  - G4: `configuration-<malla>`, desplegada con Terraform.
- G3 y G4 leen la misma tabla de programaciones y pueden iniciar dos veces la misma Step Function.
- Cada lambda es un solo archivo, sin pruebas. Los 7 configuradores son copias con un 86 % a 98 % de similitud.
- La extracción quedó incompleta (disco lleno): faltan 13 lambdas, varias de G4.

## Qué exigen los estándares (el "modelo único" ya está definido en buena parte)
| Aspecto | Requisito | Fuente |
|---|---|---|
| Configuración | El comportamiento se declara como metadata (DynamoDB), no se programa. Nada de tablas, rutas, credenciales ni valores quemados en el código. | HU-GI-01 a 04, Versionamiento V3 §6, DataStage 8.6 |
| Comportamiento | "El motor no decide, obedece": si falta configuración, falla con error clasificado, sin valores por defecto implícitos. | HU-GI (criterios) |
| Trazabilidad | Un identificador de ejecución único que viaja por todas las fases y logs. | HU-GI-05, HU-TR-01 |
| Errores | Taxonomía corporativa: dominio, severidad, recuperable o no, acción y destinatario. | HU-GI-06 |
| Logs y métricas | Logs estructurados, catálogo cerrado de eventos, métricas en CloudWatch y acceso auditado en CloudTrail. | HU-GI-07 |
| Secretos | Ninguna credencial en texto plano; se usa Secrets Manager con KMS. | HU-DO-14 |
| Permisos | Cada lambda es una identidad no humana con rol propio de mínimo privilegio, dueño y recertificación. | HU-SG-21 |
| Código | Repositorio `sc-` o `data-<proyecto>-<complemento>`, Git Flow, commits `<tipo>-<TICKET>-<asunto>`. | Versionamiento V3, HU-DO-02 a 04 |
| Calidad | SonarQube sin errores nuevos frente a la línea base y cobertura mayor a 80 % en código nuevo; la evidencia va al Comité de Cambios. | Lineamiento de código seguro |
| Despliegue | Infraestructura como código (Terraform) y pipeline Jenkins; el mismo artefacto se promueve DEV → QA → PROD. | HU-DO-06 a 11 |
| Nombres AWS | `<prefijo>-<dev\|qa\|stg\|prd>-<proyecto>-<recurso>-<tipo>`, por ejemplo `datalake-prd-niif-...-lambda`. | HU-DO (convención de nombres) |

## Propuesta
Escribir primero la **especificación del modelo único**. Cuando haya acceso, se compara con el código real y se migra lambda por lambda. La especificación cubre:
1. Estructura del proyecto y plantilla de *handler*: entrada, validación de configuración, lógica y salida.
2. Contrato de lectura de configuración: qué entidades y atributos, y qué pasa si faltan.
3. Formato de log JSON común: `ejecucion_id`, caso de uso, periodo, paso, componente, estado y código de error.
4. Manejo de errores con la taxonomía corporativa; reintentos solo en los errores recuperables.
5. Secretos y permisos: Secrets Manager y un rol por lambda.
6. Pruebas unitarias mínimas para cumplir la cobertura.
7. Despliegue con Terraform y pipeline.

## Riesgos del flujo de datos
Ver `Tareas\riesgos-flujo-glue-notificacion.md`. El modelo único debería incluir el contrato de manifiesto entre etapas, la orquestación con Step Functions y el `ejecucion_id` de punta a punta (riesgos 3, 4, 7, 10, 11 y 12).

## Lo que se puede adelantar sin acceso
- La especificación completa y un *handler* de referencia con sus pruebas.
- La lista de verificación para revisar cada lambda cuando llegue el código.

## Bloqueos y preguntas
- Acceso al repositorio (código de las lambdas) y a la cuenta AWS (configuración real, roles, nombres).
- Línea base de SonarQube del repositorio, obligatoria antes de modificar.
- Ticket Jira del proyecto, obligatorio en ramas y commits.
- Lineamientos de CI/CD V1 e IaC con Terraform GTC-GDD-004 (citados, no entregados).
- ¿El gestor de configuración ya usa DynamoDB o guarda la configuración en otro lugar?
