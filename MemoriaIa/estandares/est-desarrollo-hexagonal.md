---
tipo: estandar
actualizado: 2026-11-03
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
