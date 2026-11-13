---
tipo: repo
estado: vigente
actualizado: 2026-11-12
repo: cfg-datalake-niif-params-s3-landing
categoria: s3
paso: "2 · Landing de archivos de carga (Previsional)"
malla: transversal
---
# cfg-datalake-niif-params-s3-landing
> Esqueleto de carpetas del bucket landing para las cargas por archivo: crea las rutas IN, OUT, ERROR y METADATA de cada caso.

## Rol en el flujo
- Prepara el bucket `niif-{env}-landing` (no el de assets) para las fuentes no estructuradas del paso 2. Jenkins hace `aws s3 sync` de `src/` con `s3StaticDeployPipeline`, con `--delete`.
- Lo consume el gestor de archivos de `data-datalake-niif-primas-previsional-glue-job`, que lee de IN, mueve inválidos a ERROR y válidos a OUT.

## Entradas y salidas
- Contiene dos casos: `NIIF17_PRIMAS_PREVISIONAL` (IN con un Excel de cesión de ejemplo, OUT con subcarpeta del archivo, ERROR y METADATA) y `NIIF17_CAMPOS_UNICA_VEZ` (solo carpetas vacías con `.keep`).
- Sale al bucket landing solo estructura y ese archivo de ejemplo.

## Recursos AWS que toca
- Bucket `niif-{env}-landing` (DEV, QA y PRD con el mismo contenido).

## Código
- Sin código. `Jenkinsfile`, `README.md` (genérico de despliegue, habla de buckets `alfa-datalake-*-assets` que no son los reales) y `sonar-project.properties`.
- El archivo `.xlsx` de IN se despliega a todos los ambientes, incluido PRD; por confirmar si trae datos reales de pólizas (no se abrió).

## Estado
- En Alfa: `origin/quality`, commit c46b14f (2026-09-29). Desplegado: por confirmar.
- Código nuevo nuestro: ninguno.

## Brechas y relacionadas
- Por confirmar: el Excel de ejemplo se despliega a todos los ambientes y no se abrió; verificar que no traiga datos reales antes de dejarlo en un repo.
- Relacionado: [[data-datalake-niif-primas-previsional-glue-job]], [[cfg-datalake-niif-config-s3]], [[repo-alfa-repos]]
