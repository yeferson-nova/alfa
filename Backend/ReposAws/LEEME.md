# Repos de Alfa (copia de lectura)

Copia sin `.git` de los repos de Alfa que toca la cadena de notificación. Sirve para leerlos y compararlos desde aquí, porque en esta máquina no se pueden clonar. Los clones de verdad viven fuera de `alfa`. Los cambios se hacen en esos clones o se entregan a DevOps.

## Carpetas
| Carpeta | Prefijo del repo | Contenido |
|---|---|---|
| `lambdas\` | `sc-` | Código de las lambdas |
| `glue\` | `data-` | Código de los jobs de Glue |
| `s3\` | `cfg-`, `tpl-` | Lo que se sube a los buckets: JSON de configuración y plantillas HTML |
| `otros\` | otro prefijo | Lo que no encaje en las anteriores |

`VERSIONES.md` dice la rama y el commit que se copió de cada repo.

## Cómo actualizar (en la máquina que sí tiene acceso)
1. Clonar o actualizar (`git pull`) los repos **fuera** de `alfa`, por ejemplo en `C:\Users\<usuario>\Git\ReposAws`.
2. Desde `alfa`: `powershell -ExecutionPolicy Bypass -File Backend\ReposAws\sincronizar-repos.ps1 -Origen C:\Users\<usuario>\Git\ReposAws`
3. Revisar `git status`, hacer commit y push.

El script hace lo siguiente:
- Quita los enlaces a repos (modo `160000`) que se subieron por error.
- Copia cada repo sin `.git` a su carpeta según el prefijo.
- Escribe `VERSIONES.md`.
- Se detiene si encuentra un clon con `.git` dentro de `alfa`.

## Pendientes (fuera de estos repos)
- La lambda de `/post_login`: confirmar si existe y cómo se llama.
- Terraform (de DevOps): el rol y los argumentos del job, Cognito y el API. Aunque sea de solo lectura.
- `sc-datalake-niif-certification` (los botones Certificar y No certificar): no está entre los repos copiados.
