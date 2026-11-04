# Cómo replicar en DEV el fallo de Primas ARL (prueba controlada) y validar los correos

Complementa `analisis-flujo-primas-arl-202606.md`. Aquí se explica **cómo reproducir el fallo en DEV**, **a quién le llegan hoy los correos** y **cómo agregar tu correo en DEV**.

- Script: `replicar-fallo-dev.sh` (bash + AWS CLI + jq, para **AWS CloudShell de la cuenta DEV**).
- El script **se niega a correr** si la cuenta no es la de DEV (`533267035849`) y aborta explícitamente en PROD (`992382496548`).

## 1. Qué se replica

**El fallo:** una segunda corrida del mismo periodo extrae solo los registros nuevos (extracción incremental), Silver queda con esos pocos y el SCD de Gold **apaga todo lo que no vino** (`ACT_REG = 0`). Después de eso Gold solo tiene activos los registros nuevos, y los totales y el detalle del correo salen vacíos o incompletos.

**Qué NO garantiza la prueba:** que el correo salga idéntico al de prod. Si los registros "nuevos" de DEV pertenecen o no al periodo certificado depende de los datos de DEV. Lo que se comprueba con aserciones es el mecanismo (métricas del SCD y conteo de activos en Gold).

## 2. Hechos del código que condicionan la prueba (verificados)

| Hecho | Dónde | Consecuencia |
|---|---|---|
| La Step Function **no usa** `rutaConfigExtraccion` de su entrada: tiene fija `--RUTA_CONFIG_EXTRACCION = s3://niif-dev-assets/primas-arl/config_extraccion_primas_arl.json` | Definición `niif-dev-primas-arl` (paso `ExtraccionEstandar`) | Para cambiar el `WHERE` hay que **modificar temporalmente ese archivo** (el script lo respalda y lo restaura) |
| Los parámetros se leen de `primas-arl/parametros_extraccion.json` (fijo) | Paso `LeerParametrosExtraccion` | Ahí se leen las métricas del SCD |
| El correo usa el JSON `primas-arl/primasArl.json` (fijo en `ResolverNotificacion`) | Definición de la Step Function | Para no escribir a otras personas durante la prueba, el script **aísla** ese JSON (destinatarios = tu correo) y luego lo restaura |
| La exclusión de llaves (`EXCLUIR_EXTRAIDOS`) usa un segmento por **periodo** (`niif17_<periodo>_alfaprd_arpprod_par_endosos`) y solo excluye si ya existe la carpeta de llaves | `extraccion_estandar_optimized.py` líneas 617-775 | La prueba exige un periodo **sin segmento previo** en DEV (si no, la primera corrida ya excluye) |
| El SCD hace `full join` entre la entrada y Gold activo y marca `ACT_REG = 0` a lo que no viene | `scd.py` líneas 340-367 | Es el mecanismo del fallo |
| Los correos salen por SNS: un mensaje por destinatario, con el atributo `recipient`; solo lo recibe la suscripción confirmada cuyo `FilterPolicy` coincide | `notification_certification.py` líneas 78-156 y 598-612 | Ver sección 3 |

## 3. A quién le llegan hoy los correos

### Cómo se decide
1. El JSON del caso (`primas-arl/primasArl.json`) trae `email.destinations` (y `approvers.users`, que son quienes aprueban).
2. El job de notificación recorre `email.destinations` y publica **un mensaje por destinatario** en el topic SNS `niif-dev-certification-email`, con el atributo `recipient = <correo>`.
3. Antes de enviar, el job pone a cada suscripción de correo **confirmada** un `FilterPolicy` `{"recipient": ["<su correo>"]}`.
4. Resultado: **recibe el correo quien esté en `destinations` Y tenga una suscripción SNS confirmada.** Estar solo en una de las dos listas no basta.

### Comando
```bash
bash replicar-fallo-dev.sh destinatarios
```
Imprime, comparando ambas listas:

| Sección | Significado |
|---|---|
| Llegan HOY | Está en `destinations` y tiene suscripción confirmada |
| NO llegan | Está en `destinations` pero sin suscripción confirmada |
| Suscripción PENDIENTE | La persona no dio clic en el correo de confirmación de AWS |
| Suscritos pero NO reciben | Tienen suscripción pero no están en `destinations` |
| FilterPolicy | Debe ser `{"recipient":[su correo]}`; "SIN FILTRO" recibiría los mensajes de todos |
| Correos ENVIADOS hoy | Registros `SENT` de hoy (hora Colombia) en DynamoDB con su `emailDestination` real |

## 4. Cómo agregar tu correo en DEV

```bash
# 1. Agrega tu correo a destinations (y a aprobadores con --aprobador) y crea la suscripción SNS
bash replicar-fallo-dev.sh agregar-email tu@correo.com            # o: ... tu@correo.com --aprobador

# 2. En tu bandeja llega un correo de AWS: dar clic en "Confirm subscription"

# 3. Fija el filtro de tu suscripción (el job también lo hace antes de cada envío)
bash replicar-fallo-dev.sh filtro tu@correo.com

# 4. Verifica
bash replicar-fallo-dev.sh destinatarios
```

Qué hace el paso 1: respalda el JSON original (local y copia en S3 `primas-arl/respaldo-prueba/`), agrega tu correo sin duplicar, valida que el JSON resultante sea válido, **muestra el diff**, pide confirmación y lo sube.

**Equivalente manual** (sin el script): editar `s3://niif-dev-assets/primas-arl/primasArl.json` agregando tu correo en `email.destinations`, y en SNS → topic `niif-dev-certification-email` → *Create subscription* (protocolo Email, tu correo) y confirmar el mensaje.

**Si solo quieres recibir tú los correos de prueba** (y que no le lleguen a otras personas): `bash replicar-fallo-dev.sh aislar tu@correo.com` deja `destinations` y `approvers` = tu correo, y `bash replicar-fallo-dev.sh restaurar` lo devuelve a como estaba.

## 5. La prueba controlada (flujo completo)

### Idea
Se usa el **mismo periodo** en dos corridas, sin tocar Oracle:

```
Corrida 1 "base":        WHERE par_endosos = PERIODO <= P  AND NID_ENDOSO <= CORTE
                         → Gold queda con N activos (la foto "completa" de la prueba)
Corrida 2 "incremental": WHERE original (sin el corte)
                         → los endosos > CORTE aparecen como "nuevos"
                         → EXCLUIR_EXTRAIDOS excluye los N ya extraídos
                         → Silver preparada = solo los k nuevos
                         → el SCD apaga los N y deja k activos     ← el fallo
```

### Antes de empezar (checklist)
- [ ] Estás en CloudShell de la cuenta **DEV**, región `us-east-1`.
- [ ] Tienes un **periodo sin segmento previo** en DEV (el script lo comprueba) o usas `FORZAR=1` bajo tu responsabilidad.
- [ ] DEV tiene conexión a Oracle de DEV y datos de `par_endosos` (la extracción la usa).
- [ ] Nadie más está corriendo/probando Primas ARL en DEV en este momento. La lambda `configuration-primas-arl` o la programación del gestor **pueden sobrescribir** `config_extraccion_primas_arl.json` si se ejecutan a la vez.
- [ ] Tu correo está suscrito y **confirmado** en el topic (sección 4).
- [ ] Opcional pero recomendado: `bash replicar-fallo-dev.sh respaldar-gold` (copia Gold DEV de Primas ARL; la prueba lo modifica).

### Ejecución
```bash
bash replicar-fallo-dev.sh prechequeo                     # cuenta, archivos, segmento previo, estado de Gold
bash replicar-fallo-dev.sh respaldar-gold                 # recomendado
MI_EMAIL=tu@correo.com PERIODO=202606 bash replicar-fallo-dev.sh ejecutar-prueba
```
Variables: `MI_EMAIL` (obligatoria), `PERIODO` (por defecto 202606), `CORTE_NID_ENDOSO` (si no se da, se calcula con el percentil 98 de `NID_ENDOSO` en Bronce DEV), `YES=1` (sin confirmaciones), `FORZAR=1`.

### Qué hace `ejecutar-prueba`, en orden
1. Verifica que la cuenta sea DEV y que el periodo no tenga segmento previo.
2. Respalda `primasArl.json` y `config_extraccion_primas_arl.json` (local y en S3).
3. Calcula el corte de `NID_ENDOSO`, muestra el resumen de lo que hará y **pide confirmación**.
4. **Aísla** el caso: los correos de la prueba llegan solo a `MI_EMAIL`.
5. Corrida **base**: sube la config con el `WHERE` limitado, crea el registro en `niif-dev-ejc-ejecuciones`, inicia `niif-dev-primas-arl` y espera (~25 min). Mide los activos de Gold.
6. Restaura la config original. Corrida **incremental**: igual, espera y mide los activos de Gold.
7. Ejecuta `validar` e imprime PASS / FAIL.
8. Al salir (aunque falle) **restaura** el JSON del caso y la config de extracción originales (`trap`).

Duración total aproximada: 50 a 60 minutos (la ejecución de prod tardó ~24 min).

### Resultado esperado (si el fallo se replica)
`bash replicar-fallo-dev.sh validar` debe dar todo en PASS:

| # | Verificación | Valor esperado |
|---|---|---|
| 1 | Gold SCD en `SUCCESS` | `SUCCESS` |
| 2 | Se excluyeron llaves ya extraídas | `registros_excluidos_por_segmento > 0` |
| 3 | Apagados por ausencia = excluidos por segmento | Mismo número (en prod: 1.203.789 y 1.203.789) |
| 4 | Ninguna fila previa coincide con la entrada | `sin_cambio = 0`, `cerradas = 0` |
| 5 | Entrada del SCD = insertados | Igual (en prod: 209 y 209) |
| 6 | Todo lo activo tras la base fue apagado | `apagados = activos_base` |
| 7 | Tras la incremental solo quedan activos los nuevos | `activos_final = entrada` |

Termina con: `FALLO REPLICADO: el SCD apaga todo lo que no viene en una corrida incremental.` Si alguna falla, imprime `FAIL` con los valores reales y `No se replicó por completo`.

### El correo de la prueba
Llega a `MI_EMAIL` con asunto *"NIIF 17 - Certificación Primas ARL Periodo <periodo>"*. Se espera que **Totales** cuente solo los registros nuevos del periodo (o diga "No hay datos" si los nuevos no son de ese periodo) y que **Resumen** diga "No hay resumen configurado" si el JSON de DEV tampoco tiene `queries.summary`. Compruébalo con `destinatarios` ("Correos ENVIADOS hoy").

## 6. Volver a dejar DEV como estaba

```bash
bash replicar-fallo-dev.sh restaurar        # JSON del caso y config de extracción originales
bash replicar-fallo-dev.sh restaurar-gold   # solo si hiciste respaldar-gold (sincroniza con --delete, pide confirmación)
```
Los respaldos viven en `replica-fallo-dev/respaldo/` (CloudShell) y en `s3://niif-dev-assets/primas-arl/respaldo-prueba/`. Además, `segmentos/niif17_<periodo>_…` quedará creado en DEV y `parametros_extraccion.json` tendrá los datos de la última corrida.

## 7. Seguridad y límites

- **Solo DEV.** Aborta si la cuenta es prod o distinta de DEV.
- **Modifica DEV:** Gold (`scd_primas_arl`), `config_extraccion_primas_arl.json` y `primasArl.json` (temporalmente), segmentos, EJC, y envía correos reales (solo a `MI_EMAIL`). Cada cambio tiene respaldo y `restaurar`.
- **Sin pruebas contra AWS real.** El script se probó con un `aws` simulado: sintaxis, destinatarios, agregar correo, validación (casos PASS y FAIL) y la modificación del `WHERE`. La primera corrida real en DEV es la prueba final.
- **Supuestos por confirmar en la primera corrida:** nombres de las tablas y bases de DEV (`niif-dev-gold-catalog-db.scd_primas_arl`, `niif-dev-bronze-catalog-db.alfaprd_arpprod_par_endosos`, tomados del catálogo del repo), el topic `niif-dev-certification-email` (tomado de la documentación) y que Bronce DEV tenga datos para calcular el corte (si no, usar `CORTE_NID_ENDOSO`).
- Los argumentos de ejemplo de `reposNew/.../argumentos-job.dev.json` usan `configs/certification/primas-arl/primasArl.json`, distinto del `primas-arl/primasArl.json` que usa la Step Function. Para esta prueba vale el de la Step Function.
- Si las pruebas dejan Gold DEV inconsistente: `restaurar-gold` (si hubo respaldo) o volver a correr el flujo completo del periodo.

## 8. Qué sigue después de replicarlo

Con el fallo replicado en DEV se puede **probar la corrección** antes de tocar prod: por ejemplo, que el SCD no apague por ausencia en cargas incrementales, o que la extracción no excluya llaves cuando alimenta un SCD de foto completa. Repetir `ejecutar-prueba` con el cambio desplegado en DEV y comprobar que `validar` ahora **falla** (es decir, que `activos_final` conserva los N + k registros).
