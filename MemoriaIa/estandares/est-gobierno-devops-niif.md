---
tipo: estandar
estado: vigente
actualizado: 2026-11-10
---
# Gobierno y versionamiento DevOps NIIF 17 (v1.0, 2026-11-09)

Fuente: `Docs\02-estandares-banco\GobiernoVersionameinto-DevOps.docx` (Dirección de Arquitectura / DevOps). **Es el único lineamiento escrito para NIIF 17.** Si choca con [[est-desarrollo-hexagonal]] o con [[estructura-repos-propuesta]], manda este.
- Job de Glue en AWS: `niif-{env}-{nombre}`. Repo: `data-datalake-niif-{nombre}`. Se inicializa con `new-glue-job.sh` (DevOps, SharePoint) y el código va en `/src/`.
- Lambda en AWS: `niif-{env}-{nombre}`. Repo: `sc-datalake-niif-{nombre}`. Se inicializa con `new_lambda.sh`.
- **La infraestructura solo soporta un archivo principal por lambda: `/src/lambda_function.py`.**
- Pruebas obligatorias en `/test/test_lambda_function.py`.
- Flujo: DevOps crea el repo → rama `feature/{nombre}` → commit `feat(niif): <descripción>` → push → PR.
- Herramientas: Python 3.12.0, pytest 7.4.4, pytest-mock, **moto**, tox 4.14.1 y Docker (opcional).
- Comando oficial de pruebas: `pytest test -v --cov=src --cov-fail-under=70`. Si la cobertura baja de **70 %**, el pipeline rechaza el artefacto.
- Remite a `LINEAMIENTO PARA EL VERSIONAMIENTO DE CODIGO V2.pdf` y `LINEAMIENTO PARA IAC CON TERRAFORM V1.pdf`, que no se han entregado (SharePoint de DevOps).

## Cómo lo aplicamos (decisión del usuario, 2026-11-10)
- **DevOps es dueño de la infraestructura y nosotros del código.** La arquitectura del código es nuestra: las lambdas van **modulares, en hexagonal**, para que el código se pueda mantener. `/src/lambda_function.py` es solo el punto de entrada (el handler) e importa el resto de módulos que viven en `src/`. No hay choque.
- **Nosotros no hacemos commits.** Entregamos una **carpeta comprimida con el nombre de la lambda** (ejemplo: `niif-dev-composite-notification-evaluator.zip`) y DevOps la versiona en su repo (`sc-datalake-niif-*` o `data-datalake-niif-*`) y la despliega. El formato de commit no nos aplica.
- Dentro del zip va `src/` (con `lambda_function.py` y los módulos), `test/` (con `test_lambda_function.py`, que se puede acompañar de más pruebas) y el README.
- Antes de entregar: `pytest test -v --cov=src --cov-fail-under=70` en verde. Apuntamos al 80 % que pide Sonar ([[est-codigo-seguro]]).
- Nombres en AWS: `niif-{env}-{nombre}`. Nuestra T2 queda como `niif-<amb>-configurador-mallas`, no `datalake-<amb>-niif-configurador-mallas-lambda`.
- En las pruebas se puede usar moto, como dice el documento; nuestros dobles en memoria y el `Stubber` también valen.
