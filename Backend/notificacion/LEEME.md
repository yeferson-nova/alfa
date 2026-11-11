# Cadena de notificación y certificación: código nuevo

Esta carpeta guarda **solo el código nuevo** que se entregará a DevOps. El código desplegado hoy sigue en su lugar de siempre:

| Código actual (desplegado, solo lectura) | Dónde está |
|---|---|
| Lambdas de la cadena: trigger, evaluador compuesto, copy, move, `niif-dev-certification`, `niif-dev-short-link-redirect`, `niif-post-login` | `Backend\Lamnbdas\<nombre>\` (cada una con `ORIGEN.md`) |
| Job de notificación desplegado (monolito con SES) | `Backend\Glue\niif-dev-notification-certification\` |
| Plantillas HTML del correo | `Backend\htmlNew\` (`newUx\` = producción) |
| JSON de cada caso con sus consultas de Athena (correos, cuenta y portal SSO cambiados por marcadores) | `Backend\S3\niif-dev-assets\` (misma ruta relativa que en el bucket) |

Todo lo desplegado se comparó el 2026-11-10 con lo que hay en AWS (`EvidenciasAws\dev-codigo-actual\`).

## Estructura del código nuevo
Una carpeta por componente, con el **nombre del recurso en AWS**. Ese mismo nombre lleva el zip que se entrega (`niif-<amb>-<nombre>.zip`). DevOps crea el repositorio, hace el commit y despliega con Terraform/Terragrunt.

```
Backend/notificacion/
├── LEEME.md
├── _shared/                    código común (estados, mapa de carpetas, log JSON con ejecucion_id); el empaquetado lo copia en cada zip
└── <nombre-del-recurso>/
    ├── src/
    │   ├── lambda_function.py  punto de entrada que pide DevOps (en el job: el script de Glue)
    │   └── <package>/          domain/ application/ adapters/{inbound,outbound}/ config/  (PEP 8, en inglés)
    ├── test/                   test_lambda_function.py y el resto de pruebas
    ├── config/                 <tipo>.dev.json, .qa.json, .prd.json
    ├── eventos/                eventos de ejemplo
    ├── scripts/package.sh      arma dist/<nombre-del-recurso>.zip
    └── README.md  pyproject.toml  tox.ini  sonar-project.properties
```
Normas: `Docs\08-estandares-desarrollo\estandar-lambdas-python.md`. Los logs van en español y llevan `ejecucion_id`, `cert_code` y `request_id`.

## Estado
| Componente | Código nuevo |
|---|---|
| `niif-dev-notification-certification\` (job de Glue) | Hexagonal, con 451 pruebas que pasan. **Falta ajustarlo al estándar:** paquete en inglés, `test\` en singular, punto de entrada en `src\`, `tox.ini`, `config\` de qa y prd, `package.sh` |
| `_shared\` y las 7 lambdas | Sin empezar |
