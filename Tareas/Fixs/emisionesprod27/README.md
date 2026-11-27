# Fix: despliegue de Emisiones en PRD (27 nov 2026)

## Caso
La malla de Emisiones (PCR Vida, Generales y Satélites) falla en PRD después del paso a `main` del 26 de noviembre. En DEV y QA funciona. Todavía no se conoce la causa: falta la evidencia de PRD.

Componentes involucrados:
- Programador de configuradoras: `sc-datalake-niif-manager-configuration`, lambda `niif-<amb>-manager-configuration`. Crea el schedule de EventBridge que invoca la configuradora.
- Configuradora general: `sc-datalake-niif-configuration-general`, lambda `niif-<amb>-configuration-general`, módulo `conf_emisiones.py`.
- Step Function `niif-<amb>-emisiones`: `iac-analytics-datalake-niif-operations-manager`, archivo `stacks/app-integration/step_functions/datafiles/niif-snf-emisiones.json`.
- Jobs de Glue de la malla. Entre ellos, `niif-<amb>-remediacion-estandar`, del repo `data-datalake-niif-rmd-ext-estandar-glue-job`.

## Código revisado (rama `main`, 27 nov)
En los 7 repos de la malla, `quality` y `main` tienen el mismo código. Diferencias frente a la versión anterior:

| Pieza | Cambio |
|---|---|
| Step Function | La validación técnica llama a `niif-<amb>-val-tecnica-emisiones` (antes `val-tecnica`). Silver Generales recibe `--RUTA_CONFIG_SCD`. |
| Programador (IaC) | `CONFIGURADORES_JSON` se arma con el mapa `configuradores`, donde `EMISIONES` apunta a `niif-<amb>-configuration-emisiones`. El rol del scheduler ahora también puede invocar `configuration-general`. |
| `remediacion_estandar.py` | La ruta Silver se arma por caso (`caso_uso=<caso>`) y se acepta el estado `COMPLETE`. |

## Hechos comprobados en DEV (27 nov, 18:18 UTC)
- Hay dos lambdas activas sobre el stream de `prg-programaciones`: `manager-configuration` y `programador-configuraciones`. Cada una tiene su propio mapa de configuradores.
- Las dos intentaron crear el mismo schedule:
  - `manager-configuration` lo creó apuntando a `niif-dev-configuration-emisiones`;
  - `programador-configuraciones` intentó crearlo apuntando a `configuration-general` y recibió `ConflictException`.
- La lambda `niif-dev-configuration-emisiones` **no existe**.

## Puntos por confirmar en PRD (no son causa todavía)
1. **Ambiente `prd`.** `conf_emisiones.py` (líneas 26-35) solo acepta `dev`, `qa` y `prod`, y en PRD la lambda se llama `niif-prd-configuration-general`. Si este es el fallo, el log de la configuradora muestra `Ambiente no soportado en AWS_LAMBDA_FUNCTION_NAME: prd`. Solo afecta a Emisiones; los demás módulos no validan el ambiente.
2. **Destino del schedule.** Hay que ver a qué lambda apunta en PRD y si esa lambda existe.
3. **Job `val-tecnica-emisiones`.** Hay que ver si existe en PRD.
4. **Script de remediación.** Hay que ver si en PRD está la versión de `main`.

## Cómo se diagnostica
Script de solo lectura: `scripts/diag-despliegue-emisiones.sh`. Se corre en CloudShell, `us-east-1`:

```bash
bash diag-despliegue-emisiones.sh prd 7
bash diag-despliegue-emisiones.sh dev 7
```

- El segundo valor son los días hacia atrás que revisa.
- La corrida de `dev` sirve para comparar con PRD.
- Cada corrida deja `diag-despliegue-emisiones-<amb>-<fecha>.txt`, con cuentas y correos enmascarados. Ese archivo se guarda en `salidas/`.

El script revisa:
- las lambdas y sus mapas;
- quién lee el stream;
- la programación de Emisiones y sus schedules;
- CloudTrail: quién programó, quién arrancó y qué se desplegó;
- los errores de la configuradora;
- la Step Function y el estado exacto en que falló;
- los jobs de Glue: si existen, su script y sus últimas corridas;
- las métricas del scheduler.

## Estado
- [x] Código de `main`, `quality` y `develop` revisado.
- [x] Evidencia de DEV del programador.
- [ ] Salida del script en PRD (DevOps).
- [ ] Salida del script en DEV.
- [ ] Causa confirmada y propuesta de arreglo, con sus archivos de configuración.
