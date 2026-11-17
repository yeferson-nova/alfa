# Estándar de Pull Requests (PR) del equipo

Propuesta del equipo (2026-11-17), pendiente de aprobación del líder técnico y de DevOps. Se apoya en el lineamiento de versionamiento V3 del banco (secciones 5.3 a 5.8, ver `Docs\02-estandares-banco\versionamiento-seguro\`) y no lo reemplaza: donde la V3 define una regla, aquí se cita. Los valores que cada repo o equipo puede ajustar están en la tabla de parámetros (sección 2); el resto del documento no se edita.

## 1. Qué es un PR y para qué sirve
Un Pull Request es la solicitud de llevar los commits de una rama a otra (por ejemplo, de `bugfix/...` a `develop`). Es el único punto donde el cambio se revisa antes de entrar. Sirve para tres cosas:
1. **Que otra persona lo lea** antes de que llegue al ambiente.
2. **Que el pipeline lo pruebe** (pruebas, SonarQube, análisis de seguridad) con el resultado a la vista.
3. **Dejar el registro:** qué cambió, por qué, cómo se probó y cómo se revierte.

Un PR cuenta una sola historia: un arreglo o una función. Si el cambio mezcla temas, se parte en dos PR.

## 2. Parámetros (lo único que se ajusta)
Cambiar un valor aquí cambia la regla para todos. Si un repo necesita otro valor, se anota en la columna "Excepciones" con el nombre del repo y el motivo.

| Parámetro | Valor por defecto | Excepciones |
|---|---|---|
| Rama base de un feature, bugfix o chore | `develop` | Por confirmar repo a repo (ARQ-50: `quality` frente a `main`) |
| Ramas protegidas | `main`, `quality`, `develop` (V3 5.3.4) | |
| Promoción entre ambientes | `develop` a `quality` (QA) a `main` (PRD), cada paso con su PR | |
| Rama base de un hotfix | `main`, y luego se lleva el mismo cambio a `quality` y `develop` | |
| Revisores mínimos | 2 (V3 5.3.4) | |
| Tamaño máximo recomendado | 400 líneas cambiadas (sin contar pruebas ni archivos generados) | Más de eso, partirlo o justificarlo en la descripción |
| Cobertura del código nuevo | más de 80 % y Quality Gate en verde | |
| Formato del título | `<tipo>-<TICKET>-<asunto>` (V3 5.8) | Mientras no haya ticket Jira se usa `<tipo>(<alcance>): <asunto>` |
| Formato de la rama | `feature/`, `bugfix/`, `hotfix/` más `<TICKET>`, `release/X.Y.Z`, `docs/`, `chore/` (V3 5.5-5.6) | |
| Tipos permitidos | `feat`, `fix`, `hotfix`, `refactor`, `perf`, `test`, `docs`, `ci`, `chore`, `style`, `revert` (V3 5.8) | |
| Autor de los commits | usuario y correo de la organización (V3 5.8) | |

## 3. Reglas
**Antes de abrir el PR**
- La rama sale de la rama base de la tabla. Se verifica con `git log --oneline <base>..HEAD`: solo deben aparecer los commits del cambio.
- Los commits siguen el formato de la tabla y llevan el usuario de la organización (V3 5.8).
- Las pruebas pasan en local y no hay secretos, datos personales ni valores quemados.

**Al abrirlo**
- **Base:** la de la tabla. GitHub propone `main` por defecto: **hay que cambiarla a mano** cuando corresponde otra.
- **Título:** formato de la tabla, en minúsculas y en presente, máximo 72 caracteres. Ejemplo: `fix-NIIF-123-un solo periodo activo en gold`.
- **Descripción:** se llena la plantilla (`plantillas\pull_request_template.md`). Lo que no aplica se marca "no aplica" con una frase; no se borra la sección.
- **Marcarlo como borrador (Draft)** si todavía no está listo para revisión.

**Durante la revisión**
- El autor responde cada comentario y no resuelve el hilo del revisor: lo cierra quien lo abrió.
- El líder técnico valida buenas prácticas y ausencia de código quemado (V3 6).
- Un cambio nuevo después de la aprobación invalida la aprobación y se revisa otra vez.

**Al fusionar**
- Solo con los revisores mínimos y el pipeline en verde (V3 5.3.6). No se fusiona con comentarios sin responder.
- Se usa *squash* para un feature o bugfix de un solo tema; *merge commit* para promociones entre ambientes (`develop` a `quality` a `main`), para conservar el historial.
- Se borra la rama del cambio después de fusionar.

## 4. Quién hace qué
| Rol | Responsabilidad |
|---|---|
| Autor | Un cambio pequeño y de un solo tema; descripción completa; responde la revisión |
| Revisor | Lee el código y la descripción; comprueba la lista de verificación; aprueba o pide cambios con motivo |
| Líder técnico | Último control de calidad y de código quemado; aprueba las promociones a `quality` y `main` |
| DevOps | Mantiene ramas protegidas, pipeline y despliegue; el equipo no sube nada a mano a los ambientes |

## 5. Lista de verificación por tipo de repo
Va dentro de la plantilla. El autor marca la que aplica a su repo; el revisor la comprueba. Detalle de cada punto en `estandar-lambdas-python.md` (sección 10) y en la skill del proyecto.

- **Todos:** nombres de repo, rama y commit según V3; sin secretos ni datos personales; README actualizado; pipeline en verde.
- **Jobs de Glue:** columnas explícitas y filtro por periodo; compuerta de calidad antes de escribir; cuadre `entra = sale + cuarentena`; importes en DECIMAL; modo de carga explícito; impacto en tiempo y costo (DPU-horas, bytes de Athena) declarado.
- **Lambdas:** estructura hexagonal con prueba de arquitectura; handler sin lógica; errores con taxonomía; idempotencia; permisos mínimos.
- **Cualquier código nuevo:** parámetros que pueden cambiar están en configuración (no en código); reintentos y timeouts acotados; un evento de log por paso con `ejecucion_id`.
- **IaC:** `plan` adjunto, sin recursos creados a mano, etiquetas de costos y gobierno.
- **Plantillas de correo y S3:** HTML escapado, sin datos reales, vista previa adjunta.

## 6. Cómo adoptarlo
1. Copiar `plantillas\pull_request_template.md` a `.github\pull_request_template.md` en cada repo (por DevOps o con PR propio).
2. Pedir a DevOps que la rama base y los revisores mínimos de la tabla estén en la protección de ramas.
3. Cuando cambie un parámetro, editar solo la sección 2 y avisar al equipo.
