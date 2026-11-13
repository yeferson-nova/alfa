---
tipo: repo
estado: vigente
actualizado: 2026-11-12
repo: sc-datalake-niif-configuration-general
categoria: lambda
paso: "2 · Bodega NIIF 17 (configura y arranca la malla)"
malla: transversal
---
# sc-datalake-niif-configuration-general
> Lambda "configuradora general": recibe el código de una malla, lee su periodo de la programación y delega en el módulo de esa malla, que reconstruye, valida y publica la configuración y puede iniciar la Step Function.

## Rol en el flujo
- Misma etapa que las `configuration-<malla>` (invocación directa, por EventBridge Scheduler o a mano), pero con una sola función para nueve mallas. La IaC la crea en dev, qa y prd.
- Entrega a la Step Function `niif-{env}-<malla>` que fija el registro de casos. Las notas la describen como la lambda de otro desarrollador, ya activa en DEV en modo validación ([[inventario-lambdas]]).
- Duplicidad: reemplaza en la práctica a las seis `sc-datalake-niif-primas-*` y `conf-*` (embebe sus mismos módulos) y suma Satélites, Generales y PCR Vida. El configurador T2 (`sc-datalake-niif-configurador-mallas`) busca el mismo objetivo con otra arquitectura. Cuál prevalece: por confirmar ([[preguntas-abiertas]]).

## Entradas y salidas
- Entra: evento con `codigoMalla` (mayúsculas, debe estar en el registro) más los campos habituales (`publicar`, `ejecutar`, `modoEjecucion`, `origen`).
- Sale: envoltura con `codigoMalla`, `periodoNegocio` (siempre el de la programación, origen "PRG"), módulo delegado y el resultado del módulo.

## Recursos AWS que toca
- DynamoDB: `niif-{env}-cnf-mallas`, `prs-estructuras`, `prg-programaciones`, `ejc-ejecuciones` (nombres por defecto derivados del ambiente).
- S3 (plantillas y archivos activos de cada malla), Step Functions `niif-{env}-<malla>`, STS para resolver cuenta y región.
- Función `niif-{env}-configuration-general`; el ambiente se deduce del nombre de la función (dev, qa o prod).

## Código
- `src/lambda_function.py` (361 líneas): valida el código, fija variables de entorno de la malla, lee el periodo PRG y delega con `importlib`.
- `src/case_registry.py`: registro de nueve mallas (módulo, claves de malla y programación, ARN de la Step Function); reemplaza los marcadores de ambiente, región y cuenta.
- `src/conf_*.py` (arl, auxilios, endosos, rentas, res_mat_arp, res_mat_rvi, generales, satelites, pcr_vida): copias de los configuradores por malla, de 890 a 1300 líneas.
- Pruebas: `tests/test_lambda_function.py` y `tests/test_conf_modulos.py` (~165 con mocks, umbral 70 %). El README es una plantilla sin completar y nombra un archivo fuente que no existe.

## Estado
- En Alfa: `origin/quality`, commit `9c669bf` (2026-11-07). Jenkins `lambdaS3Pipeline` (ZIP a `niif-{env}-lambda-artifacts`); handler `lambda_function.lambda_handler`. Activa en DEV según [[inventario-lambdas]].
- `quality` frente a `main` (medido en GitHub el 2026-11-12): 21 commits solo en `quality`, 0 solo en `main`, 23 archivos distintos; `quality` va 23 archivos por delante de `main`: lo de QA no se ha promovido a PRD. Ver ARQ-50 en [[auditoria-repos-2026-11]].
- Código nuevo nuestro: ninguno.

## Brechas y relacionadas
- Sin IDs de brecha propios en las notas. Riesgo por diseño: nueve módulos duplicados dentro de una sola lambda (similitud descrita en [[inventario-lambdas]]).
- Relacionado: [[familia-mallas-alfaprd]], [[sc-datalake-niif-primas-arl]], [[sc-datalake-niif-primas-rentas]]
