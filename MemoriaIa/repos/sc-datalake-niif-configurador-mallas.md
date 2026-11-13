---
tipo: repo
estado: nuevo
actualizado: 2026-11-12
repo: sc-datalake-niif-configurador-mallas
categoria: lambda
paso: "2 · Bodega NIIF 17 (configura y arranca la malla)"
malla: transversal
---
# sc-datalake-niif-configurador-mallas
> Reescritura hexagonal (tarea T2) de una sola lambda que configura las mallas desde un catálogo JSON y reemplazaría a las `configuration-<malla>`. En Alfa el repo solo tiene un README.

## Rol en el flujo
- Misma etapa que las `configuration-<malla>`: lee CNF y PRS, valida, publica la configuración y puede iniciar la Step Function. La malla llega en el evento (`malla`) o por la variable `MALLA_PK` si se despliega una función por malla.
- Duplicidad: pensado para reemplazar a las seis por malla; la `configuration-general` de otro desarrollador cubre lo mismo con módulos copiados. Cuál queda: por confirmar ([[preguntas-abiertas]]).

## Entradas y salidas
- Entra: `malla`, `publicar`, `ejecutar`, `periodoNegocio` (solo confirma el de la programación), `modoEjecucion`, `origen`, `idempotencyKey`.
- Sale: resultado con periodo, publicados, `ejecucion_id` y ARN de la ejecución; con la misma clave devuelve el resultado anterior. Errores clasificados `CFG-*`.

## Recursos AWS que toca
- DynamoDB: `niif-{env}-cnf-mallas`, `prs-estructuras`, `prg-programaciones`, `ejc-ejecuciones` y una tabla de idempotencia nueva (no está en la IaC revisada).
- S3 (catálogo `config/catalogo-mallas.<env>.json`, plantillas, archivos activos), Step Functions `niif-{env}-<malla>`, CloudWatch (métricas y logs JSON).

## Código
- Rama `feature/T2-configurador-mallas` (commit `0c14b08`), paquete `src/configurador_mallas/`: `dominio` (catálogo, constructores, periodo, validaciones), `aplicacion` (caso de uso `ConfiguradorMallas`, puertos, idempotencia), `adaptadores/entrada` (handler), `adaptadores/salida` (dynamo, s3, stepfunctions, cloudwatch) y `configuracion` (ajustes, composición).
- Sin valores de negocio quemados: las variables de entorno son obligatorias y las reglas por malla van en el catálogo. Solo Auxilios está CONFIRMADO; las otras cinco, POR_CONFIRMAR.
- `tests/` con dobles en memoria, prueba de arquitectura y de equivalencia con las lambdas actuales; las notas citan 174 pruebas y 99 % (2026-11-03). `scripts/empaquetar.sh` arma el ZIP; cobertura mínima 85 %.

## Estado
- `quality` frente a `main` (medido en GitHub el 2026-11-12): 0 commits solo en `quality`, 0 solo en `main`; ramas iguales. Ver ARQ-50 en [[auditoria-repos-2026-11]].
- En Alfa (`origin/quality`, commit `3918928`, 2026-11-04): solo un README de una línea. La IaC ya declara la función en dev (`niif-{env}-configurador-mallas`; 0 invocaciones en DEV según las notas).
- Desajustes con la IaC (por confirmar): el handler declarado no lleva el prefijo del paquete y no pasa las variables de idempotencia, catálogo ni métricas.

## Brechas y relacionadas
- Sin IDs de brecha propios; resuelve el problema de copias descrito en [[inventario-lambdas]].
- Relacionado: [[est-desarrollo-hexagonal]], [[est-gobierno-devops-niif]], [[sc-datalake-niif-configuration-general]], [[pendientes]]
