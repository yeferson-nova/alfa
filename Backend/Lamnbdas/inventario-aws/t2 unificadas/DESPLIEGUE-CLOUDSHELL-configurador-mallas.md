# Desplegar y probar `sc-niif-configurador-mallas` desde AWS CloudShell

Guía paso a paso para subir la lambda a **DEV** con comandos de CloudShell, probarla con la CLI y, al final, probarla desde la consola web (solo ejecutar la lambda).

> Los valores marcados con `<...>` o definidos en el paso 1 son los que debes ajustar. Los nombres de recursos salen del `README.md` y del catálogo `config/catalogo-mallas.dev.json`. Si tu IaC (Terragrunt) ya crea el rol o la tabla, salta esos pasos.

---

## 0. Antes de empezar

- Entra a la consola de AWS en la **cuenta y región de DEV** y abre CloudShell (icono `>_` de la barra superior). La región que ves arriba a la derecha es la que usarán los comandos.
- Tu usuario debe poder crear funciones Lambda, roles IAM y tablas DynamoDB, y escribir en el bucket de assets. Si un comando responde `AccessDenied`, pide el permiso a Seguridad de la información.
- Las tablas `niif-dev-cnf-mallas`, `niif-dev-prs-estructuras`, `niif-dev-prg-programaciones` y `niif-dev-ejc-ejecuciones` y las máquinas `niif-dev-<malla>` **ya deben existir**. Esta guía no las crea.

---

## 1. Subir el código a CloudShell

**Opción A: subir un zip del proyecto (la más simple)**

1. En tu PC, comprime la carpeta `sc-niif-configurador-mallas` (sin `dist/` ni `__pycache__`).
2. En CloudShell: menú **Actions → Upload file** y elige el zip.
3. Descomprime:
   ```bash
   cd ~
   unzip -o sc-niif-configurador-mallas.zip -d sc-niif-configurador-mallas
   cd sc-niif-configurador-mallas
   ls    # debe verse README.md, src/, scripts/, config/, eventos/
   ```
   Si el zip trae una carpeta intermedia, entra a ella con `cd` hasta ver `src/` y `scripts/`.

**Opción B: clonar el repositorio**
```bash
cd ~
git clone https://github.com/yeferson-nova/alfa.git
cd "alfa/Backend/Lamnbdas/inventario-aws/t2 unificadas/sc-niif-configurador-mallas"
```
Los comandos de esta guía se ejecutan siempre desde la carpeta `sc-niif-configurador-mallas`, dentro de la raíz `t2 unificadas`.
(Si el repo es privado, CloudShell pedirá un token de acceso personal como contraseña.)

---

## 2. Definir las variables de la sesión

CloudShell pierde las variables si se cierra la sesión; si eso pasa, vuelve a ejecutar este bloque.

```bash
export AMB=dev
export REGION=$(aws configure get region || echo "$AWS_REGION")
export CUENTA=$(aws sts get-caller-identity --query Account --output text)

export FN=datalake-${AMB}-niif-configurador-mallas-lambda
export ROL=datalake-${AMB}-niif-configurador-mallas-role
export TABLA_IDEM=datalake-${AMB}-niif-configurador-idempotencia-dynamodb
export BUCKET_ASSETS=niif-${AMB}-assets

echo "Cuenta=$CUENTA Region=$REGION Funcion=$FN"
```

Revisa que la cuenta y la región impresas sean las de DEV antes de seguir.

---

## 3. Empaquetar la lambda

```bash
bash scripts/empaquetar.sh
ls -lh dist/
```

Resultado esperado:
```
Artefacto: .../dist/sc-niif-configurador-mallas-1.0.0.zip
Handler:   configurador_mallas.adaptadores.entrada.lambda_handler.lambda_handler
```

Define el zip para los pasos siguientes:
```bash
export ZIP=$(ls dist/sc-niif-configurador-mallas-*.zip | tail -1)
echo "$ZIP"
```

(Opcional) Verifica el contenido: `unzip -l "$ZIP" | head -30`. Debe empezar con `configurador_mallas/`, sin carpetas `src/`.

---

## 4. Subir el catálogo de mallas a S3

La lambda lee el catálogo desde `CATALOGO_MALLAS_URI`.

```bash
aws s3 cp config/catalogo-mallas.dev.json \
  s3://${BUCKET_ASSETS}/configs/configurador/catalogo-mallas.dev.json

aws s3 ls s3://${BUCKET_ASSETS}/configs/configurador/
```

> Cinco mallas están en estado `POR_CONFIRMAR` en el catálogo. Para probar con `PRIMAS_RENTAS` funciona, pero confirma los valores contra el código real antes de producción.

---

## 5. Crear la tabla de idempotencia

Solo si no existe todavía (PK y SK de tipo texto, TTL en `expiraEn`):

```bash
aws dynamodb describe-table --table-name $TABLA_IDEM --query Table.TableStatus 2>/dev/null \
|| {
  aws dynamodb create-table \
    --table-name $TABLA_IDEM \
    --attribute-definitions AttributeName=PK,AttributeType=S AttributeName=SK,AttributeType=S \
    --key-schema AttributeName=PK,KeyType=HASH AttributeName=SK,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST
  aws dynamodb wait table-exists --table-name $TABLA_IDEM
  aws dynamodb update-time-to-live --table-name $TABLA_IDEM \
    --time-to-live-specification Enabled=true,AttributeName=expiraEn
}
```

> Si las tablas de tu proyecto usan clave KMS propia, créala con `--sse-specification Enabled=true,SSEType=KMS,KMSMasterKeyId=<arn-clave>`.

---

## 6. Crear el rol de ejecución

### 6.1 Rol y política de confianza
```bash
cat > /tmp/confianza.json <<'EOF'
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Service": "lambda.amazonaws.com"},
    "Action": "sts:AssumeRole"
  }]
}
EOF

aws iam create-role --role-name $ROL \
  --assume-role-policy-document file:///tmp/confianza.json

aws iam attach-role-policy --role-name $ROL \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
```

### 6.2 Política con los permisos mínimos
Reemplaza los `<...>` por los valores reales (ARN de las claves KMS y rutas de `archivoActivoUri`).

```bash
cat > /tmp/permisos.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {"Effect": "Allow",
     "Action": ["dynamodb:Query", "dynamodb:GetItem", "dynamodb:UpdateItem"],
     "Resource": "arn:aws:dynamodb:${REGION}:${CUENTA}:table/niif-${AMB}-cnf-mallas"},
    {"Effect": "Allow",
     "Action": "dynamodb:GetItem",
     "Resource": [
       "arn:aws:dynamodb:${REGION}:${CUENTA}:table/niif-${AMB}-prs-estructuras",
       "arn:aws:dynamodb:${REGION}:${CUENTA}:table/niif-${AMB}-prg-programaciones"]},
    {"Effect": "Allow",
     "Action": "dynamodb:PutItem",
     "Resource": "arn:aws:dynamodb:${REGION}:${CUENTA}:table/niif-${AMB}-ejc-ejecuciones"},
    {"Effect": "Allow",
     "Action": ["dynamodb:PutItem", "dynamodb:GetItem", "dynamodb:UpdateItem"],
     "Resource": "arn:aws:dynamodb:${REGION}:${CUENTA}:table/${TABLA_IDEM}"},
    {"Effect": "Allow",
     "Action": "s3:GetObject",
     "Resource": "arn:aws:s3:::${BUCKET_ASSETS}/*"},
    {"Effect": "Allow",
     "Action": "s3:PutObject",
     "Resource": "arn:aws:s3:::<bucket-de-archivoActivoUri>/<prefijo>/*"},
    {"Effect": "Allow",
     "Action": "states:StartExecution",
     "Resource": "arn:aws:states:${REGION}:${CUENTA}:stateMachine:niif-${AMB}-*"},
    {"Effect": "Allow",
     "Action": ["kms:Decrypt", "kms:GenerateDataKey"],
     "Resource": ["<arn-clave-kms-tablas>", "<arn-clave-kms-bucket>"]}
  ]
}
EOF

aws iam put-role-policy --role-name $ROL \
  --policy-name permisos-configurador-mallas \
  --policy-document file:///tmp/permisos.json
```

> El heredoc va **sin comillas** (`<<EOF`) para que se expandan `${REGION}`, `${CUENTA}`, etc. Los `<...>` quedan literales: edítalos antes con `nano /tmp/permisos.json` si los ejecutaste ya.

Espera unos segundos a que IAM propague el rol y guarda su ARN:
```bash
sleep 15
export ROL_ARN=$(aws iam get-role --role-name $ROL --query Role.Arn --output text)
echo "$ROL_ARN"
```

---

## 7. Variables de entorno de la función

```bash
cat > /tmp/entorno.json <<EOF
{
  "Variables": {
    "CNF_TABLE": "niif-${AMB}-cnf-mallas",
    "PRS_TABLE": "niif-${AMB}-prs-estructuras",
    "PRG_TABLE": "niif-${AMB}-prg-programaciones",
    "EJC_TABLE": "niif-${AMB}-ejc-ejecuciones",
    "IDEMPOTENCIA_TABLE": "${TABLA_IDEM}",
    "IDEMPOTENCIA_TTL_SEGUNDOS": "604800",
    "CATALOGO_MALLAS_URI": "s3://${BUCKET_ASSETS}/configs/configurador/catalogo-mallas.${AMB}.json",
    "METRICAS_NAMESPACE": "NIIF17/Configurador",
    "NIVEL_LOG": "INFO"
  }
}
EOF
```

`MALLA_PK` solo se pone en el paso T2-A (una función por malla). Para la función general, la malla llega en el evento.

---

## 8. Crear la función

```bash
aws lambda create-function \
  --function-name $FN \
  --runtime python3.12 \
  --handler configurador_mallas.adaptadores.entrada.lambda_handler.lambda_handler \
  --role $ROL_ARN \
  --zip-file fileb://$ZIP \
  --memory-size 256 \
  --timeout 300 \
  --environment file:///tmp/entorno.json \
  --logging-config LogFormat=Text \
  --description "Configurador general de mallas NIIF 17 (T2)"

aws lambda wait function-active-v2 --function-name $FN
aws lambda get-function-configuration --function-name $FN \
  --query '{Estado:State,Handler:Handler,Runtime:Runtime,Timeout:Timeout}'
```

- `LogFormat=Text` es obligatorio: las líneas ya son JSON y las métricas EMF necesitan salir sin envoltura.
- Si el rol recién creado da `The role defined for the function cannot be assumed by Lambda`, espera 10 segundos y repite.

### Reintentos de la invocación asíncrona
```bash
aws lambda put-function-event-invoke-config \
  --function-name $FN \
  --maximum-retry-attempts 2
```
El destino de fallo (cola SQS) lo define el IaC del proyecto.

---

## 9. Probar con la CLI

Todas las pruebas usan los eventos de la carpeta `eventos/`. El flag `--cli-binary-format raw-in-base64-out` es necesario con AWS CLI v2 para enviar JSON.

### 9.1 Validar sin publicar ni ejecutar (la prueba segura)
```bash
cat eventos/validar.json

aws lambda invoke \
  --function-name $FN \
  --cli-binary-format raw-in-base64-out \
  --payload file://eventos/validar.json \
  --log-type Tail \
  --query 'LogResult' --output text \
  respuesta.json | base64 -d

echo; cat respuesta.json
```

Esperado: `StatusCode 200`, sin `FunctionError`, y en `respuesta.json` el `estado`, la `malla` `PRIMAS_RENTAS`, `publicar: false` y `ejecutar: false`. No escribe en CNF ni S3 ni arranca la máquina.

### 9.2 Repetir la misma invocación con clave de idempotencia
```bash
echo '{"malla":"PRIMAS_RENTAS","publicar":false,"idempotencyKey":"prueba-001"}' > /tmp/idem.json

for i in 1 2; do
  aws lambda invoke --function-name $FN --cli-binary-format raw-in-base64-out \
    --payload file:///tmp/idem.json /tmp/idem-$i.json > /dev/null
  cat /tmp/idem-$i.json; echo
done
```
La segunda respuesta debe traer `"reutilizado": true`.

### 9.3 Publicar sin ejecutar (escribe CNF y S3)
> **Ojo:** esta prueba modifica el periodo en `cnf-mallas` y escribe los 4 JSON en S3. Hazla solo en DEV y cuando el periodo coincida con la programación.

```bash
echo '{"malla":"PRIMAS_RENTAS","publicar":true,"ejecutar":false,"origen":"INVOCACION_DIRECTA"}' > /tmp/publicar.json

aws lambda invoke --function-name $FN --cli-binary-format raw-in-base64-out \
  --payload file:///tmp/publicar.json --log-type Tail \
  --query LogResult --output text /tmp/publicar-resp.json | base64 -d
cat /tmp/publicar-resp.json
```
Revisa `publicados[]` y comprueba los archivos con `aws s3 ls <ruta-de-publicados>`.

### 9.4 Publicar y ejecutar (arranca la Step Function)
```bash
aws lambda invoke --function-name $FN --cli-binary-format raw-in-base64-out \
  --payload file://eventos/programado-scheduler.json \
  --log-type Tail --query LogResult --output text /tmp/ejecutar-resp.json | base64 -d
cat /tmp/ejecutar-resp.json
```
> El `idempotencyKey` de ese archivo es el texto literal `<aws.scheduler.execution-id>`. Cámbialo por un valor único, por ejemplo `prueba-$(date +%s)`, para no chocar con otra invocación manual. Ajusta también `periodoNegocio` al de la programación.

Con la respuesta, confirma la ejecución:
```bash
aws stepfunctions describe-execution \
  --execution-arn "$(python3 -c "import json;print(json.load(open('/tmp/ejecutar-resp.json'))['executionArn'])")" \
  --query '{Estado:status,Inicio:startDate}'
```

### 9.5 Ver los logs
```bash
# Últimos 10 minutos, en vivo
aws logs tail /aws/lambda/$FN --since 10m --follow

# Solo los errores
aws logs filter-log-events --log-group-name /aws/lambda/$FN \
  --filter-pattern '"CORRIDA_FALLIDA"' --max-items 10 \
  --query 'events[].message' --output text
```

---

## 10. Actualizar el código después de un cambio

```bash
bash scripts/empaquetar.sh
export ZIP=$(ls dist/sc-niif-configurador-mallas-*.zip | tail -1)

aws lambda update-function-code --function-name $FN --zip-file fileb://$ZIP
aws lambda wait function-updated-v2 --function-name $FN
```

Para cambiar solo las variables de entorno:
```bash
aws lambda update-function-configuration --function-name $FN \
  --environment file:///tmp/entorno.json
aws lambda wait function-updated-v2 --function-name $FN
```

Para volver atrás al migrar en T2-A, publica versiones y usa alias:
```bash
aws lambda publish-version --function-name $FN
aws lambda create-alias --function-name $FN --name dev --function-version <n>
```

---

## 11. Correr las pruebas unitarias en CloudShell (opcional)

No necesitan AWS (usan dobles y `Stubber`).
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
pytest --cov
```
Debe terminar con 174 pruebas pasando y cobertura de ramas mayor a 85 %.

---

## 12. Si algo falla

| Síntoma | Causa probable | Qué hacer |
|---|---|---|
| `CFG-CNF-001` y lista de variables | Falta una variable de entorno | Revisa `/tmp/entorno.json` y aplica el paso 10 (variables) |
| `CFG-CNF-002` | No se lee el catálogo | Revisa `CATALOGO_MALLAS_URI` y que el rol tenga `s3:GetObject` |
| `CFG-CNF-003` | La malla no está en el catálogo | Usa uno de los códigos del catálogo, por ejemplo `PRIMAS_RENTAS` |
| `CFG-ENT-003` | El `periodoNegocio` del evento no coincide con la programación | Quita el campo del evento o ponlo igual al de `prg-programaciones` |
| `CFG-ENT-004` | El evento no trae `malla` y no hay `MALLA_PK` | Agrega `"malla": "..."` al evento |
| `CFG-PER-001` o `AccessDenied` | Falta un permiso en el rol o en la clave KMS | Revisa la política del paso 6.2 y la política de la clave |
| `CFG-CON-001` | Hay otra invocación en curso con la misma clave | Espera o usa otra `idempotencyKey` |
| `CFG-CON-002` | Alguien cambió CNF durante la corrida | Reintenta |
| `CFG-ORQ-001` | Ya existe una ejecución con ese nombre y otra entrada | Revisa Step Functions; no repitas con otra entrada |
| `Task timed out` | Más de 300 s | Revisa los logs; sube el timeout si es necesario |
| `ResourceConflictException` al actualizar | La función aún se está actualizando | Espera con `aws lambda wait function-updated-v2` |

El detalle de la taxonomía de errores está en el `README.md`, sección "Errores y logs en CloudWatch".

---

## 13. Probarla desde la consola web (solo ejecutar la lambda)

Sirve para una prueba rápida, sin CLI, cuando la función ya está desplegada.

1. Abre la consola de AWS en la región de DEV y entra a **Lambda → Functions**.
2. Haz clic en `datalake-dev-niif-configurador-mallas-lambda`.
3. Ve a la pestaña **Test** (sección *Test event*).
4. Elige **Create new event**:
   - **Event name:** `validar-primas-rentas`
   - **Event sharing settings:** *Private*
   - **Event JSON:** pega el contenido de `eventos/validar.json`:
     ```json
     {"malla": "PRIMAS_RENTAS", "publicar": false, "origen": "INVOCACION_DIRECTA"}
     ```
5. Pulsa **Save** y luego **Test**.
6. Revisa el panel **Execution results**:
   - **Status: Succeeded** y en *Response* el `estado`, la `malla` y `reutilizado`.
   - En *Function Logs* aparecen los eventos `CORRIDA_INICIADA`, `CONFIGURACION_LEIDA` y `CORRIDA_FINALIZADA`.
   - Si sale **Failed**, el *Response* trae el código `CFG-...`; búscalo en la tabla del paso 12.
7. Para ver más logs: pestaña **Monitor → View CloudWatch logs**, abre el último *log stream*.

Para probar otros casos, crea más eventos de prueba con el mismo procedimiento:

| Evento | JSON | Qué hace |
|---|---|---|
| Validar | `{"malla":"PRIMAS_RENTAS","publicar":false}` | Solo valida, no escribe |
| Publicar | `{"malla":"PRIMAS_RENTAS","publicar":true,"ejecutar":false}` | Escribe CNF y los 4 JSON en S3 |
| Publicar y ejecutar | `{"malla":"PRIMAS_RENTAS","publicar":true,"ejecutar":true,"origen":"PROGRAMACION_GESTOR","idempotencyKey":"web-001"}` | Además arranca la Step Function |

> Una prueba con `publicar: true` modifica datos reales de DEV. Cambia `idempotencyKey` en cada prueba nueva; si repites la misma clave, la lambda devuelve el resultado guardado (`reutilizado: true`) sin volver a ejecutar.

Para confirmar que la ejecución arrancó, abre **Step Functions → State machines → `niif-dev-primas-rentas`** y mira la última ejecución.
