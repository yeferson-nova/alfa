# Preguntas para DevOps: repositorios, despliegue y ambientes

**Fecha:** 2026-11-05 · **De:** equipo de Desarrollo (proyecto NIIF 17) · **Para:** equipo de DevOps
**Estado:** sin enviar, sin respuesta.

## Contexto
Somos el equipo de Desarrollo. DevOps administra Jenkins y Terraform, crea los repositorios y despliega. Nosotros entregamos el código, el artefacto versionado y los parámetros por ambiente. Hoy no existe ningún repositorio Git de este proyecto: el código desplegado en AWS es la única fuente.

Vamos a migrar a repositorios con el estándar de versionamiento V3 (`<tipo>-<proyecto>-<complemento>`) y arquitectura hexagonal. Necesitamos confirmar lo siguiente antes de fijar la estructura.

**Supuesto de esta lista:** DEV, QA y PRD son **cuentas de AWS distintas** (hoy conocemos solo la de DEV, `533267035849`). Las preguntas de las secciones 4 y 6 parten de eso. Si algún ambiente comparte cuenta con otro, avísenos.

**Qué vamos a entregar** (propuesta):
- Un repositorio por lambda (`sc-niif-<nombre>`), con su arquitectura adentro.
- Los jobs de Glue en un repositorio de pipeline, con una carpeta por etapa y un artefacto por job. Aparte, el job de notificación, que ya está en su propio repositorio.
- Una librería común y un repositorio de metadatos por malla.

---

## 1. Las tres preguntas que deciden la estructura
| # | Pregunta | Qué decidimos con la respuesta |
|---|---|---|
| 1 | ¿Su pipeline de Jenkins asume **un repo = un artefacto = una versión**, o soporta varios componentes en un mismo repo, con construcción por carpeta y tags por componente (por ejemplo `extraccion-v1.2.0`)? | Un repo para todos los jobs de Glue, o uno por job. |
| 2 | ¿Qué recibe Terraform de nosotros: el artefacto, un archivo de parámetros por ambiente (`job.<amb>.json`), o ambos? | Qué incluye cada repo en `config/`. |
| 3 | ¿Cómo se hace un **rollback**: se lo pedimos a ustedes, o hay un job de Jenkins que lanzamos con el tag anterior? | Cómo versionamos y cuánto debemos conservar. |

## 2. Repositorios
4. ¿Podemos pedir un repositorio por cada lambda y job, o hay límite o proceso de aprobación?
5. ¿Quién crea los repositorios y con qué nombre exacto? Nosotros solo conocemos el formato de V3.

## 3. Qué debe llevar cada repositorio
6. ¿Existe una **plantilla** de `pipeline.yaml` y de `Jenkinsfile`, o un repositorio de ejemplo que ya funcione para una lambda y para un job de Glue?
7. ¿Qué archivos y carpetas exige el pipeline, y con qué nombres exactos (`config/`, `pyproject.toml`, `sonar-project.properties`)?
8. ¿Qué versión de Python usan las pruebas del pipeline y qué herramienta de cobertura?

## 4. Artefactos, despliegue y ambientes
9. ¿Dónde publica Jenkins el artefacto de Glue (Nexus, S3 o ambos) y con qué ruta y nombre?
10. ¿Cómo se promueve el **mismo artefacto** de DEV a QA y a PRD **entre cuentas distintas** (publicación a un bucket por cuenta, replicación, o un repositorio central), y quién aprueba cada paso?
11. Los jobs y lambdas que ya existen en AWS (creados a mano), ¿se **importan a Terraform** o se recrean con el despliegue nuevo? Afecta los nombres de los recursos.
12. ¿Qué **rol asume Jenkins en cada cuenta** para desplegar, y los nombres de recurso llevan el ambiente (`niif-<dev|qa|prd>-…`) aunque cada ambiente tenga su cuenta? Nos importa para no tener nombres fijos en el código.
13. ¿Quién crea los **roles de ejecución** de lambdas y jobs? Nosotros podemos declarar los permisos mínimos que necesita cada uno.

## 5. Versiones y rollback
14. ¿Cuántas versiones conserva el repositorio de artefactos? (Lo que vimos: lo desplegado en PRD más las últimas 10. ¿Es correcto?)

## 6. Configuración y seguridad
15. ¿Dónde viven los parámetros de cada ambiente (rutas S3, tablas, ARN, id de la cuenta): en nuestro `config/<amb>.json`, en las variables de Terraform o en Parameter Store de cada cuenta? No podemos dejar valores en el código.
16. ¿Cómo se manejan los **secretos** (Oracle, SFTP) y cómo los consumen las lambdas y los jobs?
17. Los `config_*.json` de cada malla en `s3://niif-dev-assets/` **los genera la lambda configuradora** a partir de los registros de DynamoDB (`niif-dev-cnf-mallas` y `niif-dev-prs-estructuras`). Hoy esos registros existen solo en DEV y sin versión. Queremos versionarlos en un repositorio. ¿El pipeline puede cargarlos a DynamoDB de cada cuenta al desplegar, y quién crea las tablas y los buckets en QA y PRD?

## 7. Documentos pendientes
18. ¿Pueden compartirnos los lineamientos **CI/CD V1** e **IaC con Terraform GTC-GDD-004**? Las historias de usuario los citan y no nos los entregaron.

---

## Prioridad si solo hay tiempo para pocas
Las preguntas **1, 2 y 3**, y después la **11** y la **17**.

## Qué haremos mientras no haya respuesta
Seguiremos con el diseño interno de cada repositorio (arquitectura hexagonal, `src/`, `tests/`, `config/`), la librería común y el orden de migración. No crearemos repositorios ni cambiaremos nada en AWS hasta recibir estas respuestas.
