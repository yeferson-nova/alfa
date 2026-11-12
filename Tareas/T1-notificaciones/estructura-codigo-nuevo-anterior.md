# Cadena de notificación y certificación: código nuevo

Esta carpeta guarda **solo el código nuevo** que se entregará a DevOps. El código desplegado hoy sigue en su lugar de siempre:

| Código actual (desplegado, solo lectura) | Dónde está |
|---|---|
| Lambdas de la cadena: trigger, evaluador compuesto, copy, move, `niif-dev-certification`, `niif-dev-short-link-redirect`, `niif-post-login` | `Backend\Lamnbdas\<nombre>\` (cada una con `ORIGEN.md`) |
| Job de notificación desplegado (monolito con SES) | `Backend\Glue\niif-dev-notification-certification\` |
| Herramientas de las plantillas (generador de *previews*, validador) y diseños anteriores | `Backend\htmlNew\` |
| JSON de cada caso con sus consultas de Athena (correos, cuenta y portal SSO cambiados por marcadores) | `Backend\S3\niif-dev-assets\` (misma ruta relativa que en el bucket) |

Todo lo desplegado se comparó el 2026-11-10 con lo que hay en AWS (`EvidenciasAws\dev-codigo-actual\`).

## Estructura del código nuevo
Una carpeta por componente, con el **nombre del recurso en AWS**. Ese mismo nombre lleva el zip que se entrega (`niif-<amb>-<nombre>.zip`). DevOps crea el repositorio, hace el commit y despliega con Terraform/Terragrunt.

```
Backend/notificacion/
├── LEEME.md
├── verificar_copias_comunes.py comprueba que la copia de niif_observability sea igual en todos los paquetes
├── email-template/             las 6 plantillas HTML del correo (= carpeta email-template/ en S3)
├── lambdas/                    una carpeta por lambda (repo sc-datalake-niif-<nombre>); nombres en el estándar niif-<amb>-*
│   └── <nombre-del-recurso>/
└── jobs-glue/                  una carpeta por job de Glue (repo data-datalake-niif-<nombre>)
    └── niif-dev-notification-certification/

Dentro de cada <nombre-del-recurso>/ (lambda o job):
    ├── src/
    │   ├── lambda_function.py  punto de entrada que pide DevOps (en el job: el script de Glue)
    │   ├── niif_observability/ COPIA de la librería común (log JSON con trazabilidad); igual en todos los paquetes
    │   └── <package>/          domain/ application/ adapters/{inbound,outbound}/ config/  (PEP 8, en inglés)
    ├── test/                   test_lambda_function.py y el resto de pruebas
    ├── config/                 <tipo>.dev.json, .qa.json, .prd.json
    ├── eventos/                eventos de ejemplo
    ├── scripts/package.sh      arma dist/<nombre-del-recurso>.zip
    └── README.md  pyproject.toml  tox.ini  sonar-project.properties
```
Normas: `Docs\08-estandares-desarrollo\estandar-lambdas-python.md`. Los logs van en español y llevan `ejecucion_id`, `cert_code` y `request_id`.

## Estado (2026-11-10)
| Componente | Código nuevo |
|---|---|
| `niif_observability` (librería común) | Hecho: log JSON con trazabilidad en todas las líneas y EMF para lambdas, en inglés y PEP 8. **Cada paquete lleva su copia** en `src\niif_observability\` (se duplica en Git porque la entrega es por carpeta). Hoy hay 6 copias iguales: la del job y las de los 5 esqueletos de lambdas. Al cambiarla, copiarla a todos y correr `python verificar_copias_comunes.py` |
| `jobs-glue\niif-dev-notification-certification\` | Hexagonal, 515 pruebas, 99 % de cobertura. Probado en Glue de DEV (job de prueba `-prueba-v2`, sin envío). Ya hace lo mismo que el desplegado (HTML, envío parcial) y tiene la trazabilidad completa. **Falta:** pasarlo a inglés y PEP 8, `test\` en singular, punto de entrada en `src\`, `tox.ini` y `config\` de qa y prd (N-08). No desplegado |
| `email-template\` | Las 6 plantillas HTML del correo (T1b). Mismo nombre de carpeta que en S3 (`s3://niif-<amb>-assets/email-template/`); son idénticas a las del paquete que está en QA. Se validan con `Backend\htmlNew\herramientas\validar_plantillas.py` |
| `lambdas\` | **Esqueletos creados (2026-11-10), sin lógica todavía.** Cada uno tiene carpetas, `__init__.py`, la copia de `niif_observability` y un README con qué reemplaza, su entrada y sus brechas:<br>• `niif-dev-notification-certification-orchestrator` (nueva: reemplaza a trigger, copy y move; D1)<br>• `niif-dev-composite-notification-evaluator`<br>• `niif-dev-certification` (certificar/rechazar; separada mientras se decide D7)<br>• `niif-dev-short-link-redirect`<br>• `niif-dev-post-login` (antes `niif-post-login`; pasa al estándar) |
