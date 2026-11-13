---
tipo: contexto
estado: vigente
actualizado: 2026-11-12
---
# Repo `alfa-repos`: dónde vive ahora el código

Este PC no ve la red de Alfa y el PC corporativo no ve este. Se pasan los cambios por un repo privado de la cuenta personal: `github.com/yeferson-nova/alfa-repos`, clonado en `C:\Users\rdpuser\Git\alfa-repos\`. **Todo el código (viejo y nuevo) vive allá; `alfa` quedó solo con lineamientos, documentación, memoria y evidencia.** Comandos: `COMANDOS-ALFA-REPOS.md` en la raíz de `alfa`; reglas y ciclo: `README.md` de `alfa-repos`.

## Cómo viaja
- **Corporativo → aquí:** `scripts\exportar.ps1` copia la foto de `quality` (si no existe, `main`; si está vacía, `develop`) de cada repo de Alfa a `lambdas\`, `glue\`, `s3\`, `iac\`, y anota el commit en `VERSIONES.md`. La clave SSH se pide una vez (ssh-agent).
- **Aquí → corporativo:** se trabaja en una rama `feature/...` desde `main`; `scripts\generar-parches.ps1` convierte los commits en `.patch` (uno por commit y repo) y `scripts\aplicar-parches.ps1`, en el corporativo, los aplica con `git am --3way` en la rama feature de cada repo. Los commits llegan con su autor, fecha y mensaje.
- `main` solo recibe `sync:`, `parches:` y herramientas (`scripts\`). Nunca se trabaja directo en `main`.

## Dónde quedó cada cosa (2026-11-12)
| Antes en `alfa` | Ahora |
|---|---|
| `Backend\ReposAws\` (copia de lectura) | Raíz de `alfa-repos` (misma estructura por tipo) |
| `Backend\notificacion\` (código nuevo) | Rama `feature/codigo-nuevo-notificacion`, dentro de cada repo: job de notificación y de certificación, lambdas (certificación, evaluador, short-link, **orquestadora** y **post-login**, estas dos son repos nuevos), plantillas en `s3\tpl-...-email-template-s3\src\email-template` |
| `Backend\Lamnbdas\inventario-aws\t2 unificadas\sc-niif-configurador-mallas` | Rama `feature/T2-configurador-mallas`, en `lambdas\sc-datalake-niif-configurador-mallas` |
| `Backend\Glue`, `Backend\Lamnbdas\niif-*`, `pruebas-lambda`, `spike-primas-rentas`, `reposEstandar` | Borrados (copias del código desplegado y propuestas descartadas; siguen en el historial de Git de `alfa`) |
| `Backend\localstack` | `alfa-repos\scripts\localstack` |
| `verificar_copias_comunes.py` | `alfa-repos\scripts\` (comprueba que `niif_observability` sea igual en todos los paquetes) |
| `Backend\Eventos`, `SNS`, `StepFunctions`, `S3`, `cloudshell-inventario`, `Glue\catalogo` y `crawlers` | `EvidenciasAws\backend\` (fuera de Git, `.gitignore`) |
| `Backend\notificacion\LEEME.md` | `Tareas\T1-notificaciones\estructura-codigo-nuevo-anterior.md` (describe la estructura de antes) |

Siguen en `Backend\` solo `htmlNew\` (maquetas, previews y dos herramientas de plantillas), `reservas_matematicas_cert\` (HTML) y `Lamnbdas\inventario-aws\` (documentos).

## Pendientes de esta migración
- **Entrada de los jobs de Glue.** Los repos `data-...-notification-certification-glue-job` y `data-...-certification-glue-job` conservan su script viejo en `src\jobs\`, y el pipeline de Jenkins toma los jobs de esa carpeta. El código nuevo tiene su entrada en `src\` (`job_notificacion.py`, `niif_certification.py`). Mientras convivan, las pruebas de calidad del repo fallan porque revisan también el script viejo. Decidir con DevOps: borrar el viejo y poner la entrada nueva en `src\jobs\`.
- **Repos nuevos sin crear en Alfa:** `sc-datalake-niif-notification-certification-orchestrator` y `sc-datalake-niif-post-login`. `aplicar-parches.ps1` los salta con aviso; se entregan por zip a DevOps ([[est-gobierno-devops-niif]]).
- Las rutas `Backend\...` de otras notas son históricas; ver la tabla de arriba. Código que sigue fuera de `Backend\`: scripts de `errores\` y paquetes de `Tareas\paquete-release-*`.

Relacionado: [[rutas-workspace]], [[traspaso-sesion]], [[estructura-repos-propuesta]], [[repos-alfa-notificacion]].
