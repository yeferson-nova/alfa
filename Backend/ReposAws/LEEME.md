# Repos de Alfa (copia de lectura)

Copia sin `.git` de los repos de Alfa que toca la cadena de notificación. Sirve para leerlos y compararlos desde aquí, porque en esta máquina no se pueden clonar. Los clones de verdad viven en `_clones\` (ignorada por Git). Los cambios se hacen en esos clones o se entregan a DevOps.

## Carpetas
| Carpeta | Prefijo del repo | Contenido |
|---|---|---|
| `lambdas\` | `sc-` | Código de las lambdas |
| `glue\` | `data-` | Código de los jobs de Glue |
| `s3\` | `cfg-`, `tpl-` | Lo que se sube a los buckets: JSON de configuración y plantillas HTML |
| `otros\` | otro prefijo | Lo que no encaje en las anteriores |

`VERSIONES.md` dice la rama y el commit que se copió de cada repo.

## Cómo actualizar (en la máquina que sí tiene acceso)
1. Los clones (con `.git`) viven en `Backend\ReposAws\_clones\`, que está en el `.gitignore` de la raíz y no se sube.
2. Para actualizar, corre `git pull` dentro de cada clon de `_clones\`.
3. Desde la raíz de `alfa`: `powershell -ExecutionPolicy Bypass -File Backend\ReposAws\sincronizar-repos.ps1`
4. Revisar `git status`, hacer commit y push.

El script hace lo siguiente:
- Quita los enlaces a repos (modo `160000`) que se subieron por error.
- Copia cada repo sin `.git` a su carpeta según el prefijo.
- Escribe `VERSIONES.md`.
- Se detiene si encuentra un clon con `.git` fuera de `_clones\`.

## Pendientes (fuera de estos repos)
- La lambda de `/post_login`: confirmar si existe y cómo se llama.
- Terraform (de DevOps): el rol y los argumentos del job, Cognito y el API. Aunque sea de solo lectura.
- `sc-datalake-niif-certification` (los botones Certificar y No certificar): no está entre los repos copiados.
