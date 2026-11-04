# Desplegar y probar el spike de Primas Rentas en AWS (DEV)

Paso a paso para subir `spike-primas-rentas` como una **lambda nueva** (`niif-dev-configuration-general-spike`) y probarla sin afectar las lambdas existentes. Cuenta `533267035849`, `us-east-1`. Script: `desplegar-cloudshell.sh` (en esta carpeta).

> Los valores marcados **(inferido)** salen del inventario de la cuenta, sin verificarlos en vivo. Confírmalos antes de ejecutar.

## 0. Antes de empezar

| Qué | Detalle |
|---|---|
| Permisos | Tu usuario necesita crear roles IAM y lambdas. Si el rol de CloudShell no puede, pide a quien administre la cuenta que cree el rol o corra el paso 2. |
| Pruebas locales | Antes de subir: `pip install -e ".[dev]"` y `pytest` en esta carpeta (37 pruebas, no usan AWS). |
| Qué NO hace | No se conecta al stream de `niif-dev-prg-programaciones` (ya lo escuchan G3 y G4: riesgo de doble ejecución), no usa Scheduler y no reemplaza nada. |

## 1. Subir el código a CloudShell

1. Comprime esta carpeta (`spike-primas-rentas`) en un `.zip`, sin `__pycache__`.
2. En CloudShell (consola AWS, región `us-east-1`): **Actions → Upload file** y sube el zip.
3. Descomprime y entra:
   ```bash
   unzip spike-primas-rentas.zip -d spike && cd spike/spike-primas-rentas
   ```
   (si el zip no trae la carpeta raíz, entra donde esté `desplegar-cloudshell.sh`).

## 2. Revisar los valores (inferidos)

El script usa estos valores por defecto. Cámbialos con variables de entorno si no coinciden:

| Variable | Valor por defecto | Cómo confirmarlo |
|---|---|---|
| `CNF_TABLE` | `niif-dev-cnf-mallas` | `aws dynamodb list-tables` |
| `PRS_TABLE` | `niif-dev-prs-estructuras` | ídem |
| `EJC_TABLE` | `niif-dev-ejc-ejecuciones` | ídem |
| `STATE_MACHINE` | `niif-dev-primas-rentas` | `aws stepfunctions list-state-machines` |
| `MALLA_PK` | `MALLA#PRIMAS_RENTAS` | Variable de entorno de la lambda actual `niif-dev-configuration-primas-rentas` |
| `BUCKET_PARAMS` | `niif-dev-assets` | Rutas `s3://...` que usa la Step Function; las rutas reales están en el registro `archivoActivoUri` de CNF |
| `KMS_KEYS` | Las dos claves verificadas el 2026-11-03: `...key/c417247b-...` (bucket `niif-dev-assets`) y `...key/a8eabea1-...` (tabla `niif-dev-cnf-mallas`) | `aws s3api get-bucket-encryption --bucket niif-dev-assets` y `aws dynamodb describe-table --table-name <tabla> --query Table.SSEDescription`. Repetir para PRS y EJC; si usan otra clave, agregarla separada por coma |
| `BUCKETS_EXTRA` | vacío | Otro bucket si CNF apunta a uno distinto (separados por coma) |

Ejemplo: `BUCKETS_EXTRA=otro-bucket bash desplegar-cloudshell.sh`.

## 3. Desplegar

```bash
bash desplegar-cloudshell.sh
```

Hace, en orden, y se puede repetir (actualiza si ya existe):
1. Empaqueta `src\` en un zip (solo código propio; `boto3` ya viene en el runtime de Lambda).
2. Crea el rol `niif-dev-configuration-general-spike-role` con **mínimo privilegio**: lectura de CNF y PRS, actualización de CNF (periodo), alta en EJC, lectura/escritura en el bucket de parámetros y `states:StartExecution` **solo** sobre la Step Function de Primas Rentas.
3. Crea la lambda (Python 3.12, 256 MB, 300 s, handler `configurador.handler.lambda_handler`) con sus variables de entorno, etiquetas y log group con retención de 14 días.

## 4. Probar, de menor a mayor riesgo

**Prueba 1. Validación (segura).** No escribe en S3 ni DynamoDB y no inicia la Step Function. Solo lee CNF y PRS y valida.
```bash
bash desplegar-cloudshell.sh probar                 # periodo por defecto
PERIODO=202610 bash desplegar-cloudshell.sh probar  # periodo AAAAMM
```
Esperado: `estado: VALIDADO`, `registrosCNF: 9` y `publicados: []`. Un error de `ConfiguracionError` indica qué registro CNF o variable falta.

**Prueba 2. Publicar (modifica DEV).** Escribe los 4 JSON de parámetros en S3 y actualiza el periodo en CNF. Esto **cambia la configuración que usa la Step Function real**: avisa al equipo y usa un periodo acordado.
```bash
aws lambda invoke --function-name niif-dev-configuration-general-spike \
  --cli-binary-format raw-in-base64-out \
  --payload '{"publicar":true,"periodoNegocio":"202610"}' /tmp/salida.json && cat /tmp/salida.json
```

**Prueba 3. Ejecutar (inicia el pipeline real).** `ejecutar:true` exige `publicar:true` e inicia la Step Function (Glue, crawlers y notificación de certificación). Hazla solo con visto bueno del equipo y confirmando que el correo no llegue a destinatarios reales.
```bash
aws lambda invoke --function-name niif-dev-configuration-general-spike \
  --cli-binary-format raw-in-base64-out \
  --payload '{"publicar":true,"ejecutar":true,"periodoNegocio":"202610","origen":"SPIKE"}' /tmp/salida.json && cat /tmp/salida.json
```

## 5. Ver logs y verificar

```bash
aws logs tail /aws/lambda/niif-dev-configuration-general-spike --since 15m --follow
```
Los logs son JSON, una línea por evento (`inicio`, `fin`, errores). Si hubo ejecución, el resultado trae `executionArn`: `aws stepfunctions describe-execution --execution-arn <arn>`.

## 6. Si falla

| Síntoma | Causa probable |
|---|---|
| `Variables de entorno faltantes` | Falta alguna variable; el spike no usa valores por defecto a propósito |
| `AccessDenied` en DynamoDB o S3 | Nombre de tabla o bucket distinto del inferido (paso 2), o clave KMS: añadir `kms:Decrypt` y `kms:GenerateDataKey` sobre la clave a la política del rol |
| `Registros CNF faltantes` | La malla no tiene sus 9 registros en `cnf-mallas` |
| `Malla desconocida` | El spike solo trae `PRIMAS_RENTAS` en `mallas.py` |
| `ResourceNotFound` al crear | La región no es `us-east-1` |

## 7. Limpiar

```bash
bash desplegar-cloudshell.sh eliminar   # pide escribir SI; borra lambda, rol y log group del spike
```
No borra los JSON de S3 ni el periodo en CNF si hiciste la prueba 2 o 3.

## Pendiente

- Los adaptadores (DynamoDB, S3, Step Functions) no tienen pruebas: probarlos en LocalStack antes de usar este despliegue como referencia.
- El script crea recursos a mano; para producción deben pasar a Terraform y al pipeline (HU-DO).
