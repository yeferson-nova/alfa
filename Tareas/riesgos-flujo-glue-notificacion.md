# Riesgos en el paso de datos de Glue a la Lambda de Notificación

**Estado:** análisis en iteración (2026-11-03). Sin acceso al código ni a AWS, se basa en el diagrama (`Docs\05-arquitectura\aws_pasos_2_y_3.drawio`), las plantillas HTML actuales y los estándares del banco. Lo que depende de la implementación real es **hipótesis por confirmar**.

**Afecta a:** T1 (notificaciones), T2 (modelo de lambdas) y T3 (validaciones de reserva matemática).

## El problema de fondo
En el paso 2, Glue consolida los datos. En el paso 3, la Lambda de Notificación arma el correo con los totales, la Lambda de Transporte envía los datos a S3 de salida y un usuario certifica en medio. Las plantillas sugieren que la Lambda genera las tablas con pandas (clase `dataframe`) y publica un enlace al detalle.

**No hay un contrato explícito entre lo que Glue produce, lo que se notifica, lo que se certifica y lo que se transporta.** Cada etapa puede ver una versión distinta de los datos, y nada lo detecta.

## Riesgos altos

| # | Riesgo | Escenario concreto | Tipo |
|---|---|---|---|
| 1 | **Se certifica una versión y se transporta otra** | Glue se vuelve a ejecutar y sobrescribe el mismo prefijo de S3 mientras el correo espera respuesta. El usuario certifica las cifras del primer correo y Transporte envía las nuevas. El manual de DataBrew indica "reemplazar la salida en cada ejecución". | Arquitectura / Cumplimiento |
| 2 | **Los totales se calculan dos veces, con motores distintos** | Glue (Spark) suma y la Lambda (pandas, `float64`) vuelve a sumar. Con valores de billones y decimales, el redondeo acumulado hace que el correo no cuadre con el archivo. La Circular Básica Contable exige que la cifra publicada sea demostrable. | Arquitectura / Cumplimiento |
| 3 | **Notificación de datos parciales o repetida** | Si la Lambda se dispara por un evento de S3, Spark escribe muchos archivos y se envían varios correos, o uno antes de terminar la escritura. Lambda y SNS entregan "al menos una vez": puede haber correos duplicados con tokens distintos. | Arquitectura |
| 4 | **Fallos silenciosos** | Si Glue falla, nada ejecuta la notificación de error a menos que un orquestador capture el fallo. Peor: el job termina "con éxito" pero con 0 filas y se envía a certificar un correo con totales en cero. | Arquitectura |
| 5 | **Inyección en el HTML** | `$tableDetail` contiene enlaces, así que probablemente se genera sin escapar el contenido. Cualquier texto que venga de la fuente se inserta como HTML en un correo con botones de acción. | Seguridad |

## Riesgos medios

| # | Riesgo | Escenario concreto | Tipo |
|---|---|---|---|
| 6 | **Límites de tamaño** | SNS (canal actual) rechaza mensajes de más de 256 KB: una tabla grande hace fallar el envío. Gmail recorta los correos de más de 102 KB y oculta los botones. | Arquitectura |
| 7 | **La Lambda lee el dataset completo** | Si calcula los totales leyendo los datos consolidados, choca con los límites de memoria y de 15 minutos y gasta más. Si consulta con Athena sin particiones, escanea de más. | Costos |
| 8 | **El periodo sale de la fecha, no de los datos** | La plantilla de reservas usa `$perido_a - $perido_a`. Si el periodo se deduce del calendario y no de los datos, el correo puede mostrar uno equivocado. | Arquitectura |
| 9 | **Destinatarios sin control** | Un error en el gestor de configuración envía cifras financieras a un destinatario equivocado o externo. No hay lista de dominios permitidos. | Seguridad |
| 10 | **La Lambda se salta Lake Formation** | Lee S3 directamente con permisos amplios, sin pasar por los permisos del catálogo (hallazgos A-01 y A-03 del estándar de seguridad). | Seguridad |
| 11 | **Contrato de esquema implícito** | Glue renombra una columna y el correo sale con columnas vacías o equivocadas sin que nada falle. El estándar exige detenerse ante cambios incompatibles. | Arquitectura |
| 12 | **Sin hilo de trazabilidad** | No hay un identificador de ejecución que una la ejecución de Glue, el correo, la decisión y el transporte. No se puede probar qué datos se certificaron. | Cumplimiento |

## Propuesta: manifiesto inmutable + Step Functions

```
Glue (Consolidación) ── escribe datos en  s3://.../caso=<x>/periodo=<AAAAMM>/ejecucion=<id>/   (nunca sobrescribe)
        │                + manifiesto.json: ejecucion_id, caso, periodo (de los datos), rutas,
        │                  conteos, sumas en DECIMAL (como texto), checksum, esquema y versión
        ▼
Step Functions ── Glue .sync con Catch → error clasificado → alertas técnicas (SNS → ServiceNow)
        │
        ├─ Lambda Validación: lee el manifiesto y los datos, aplica las reglas (T3) y añade el resultado al manifiesto
        ├─ Lambda Notificación: lee SOLO el manifiesto (pocos KB). No recalcula nada; escapa todo; destinatarios con lista permitida
        ├─ Espera la decisión humana (waitForTaskToken): el token del correo es el de la tarea; caduca con el plazo
        └─ Lambda Transporte: verifica el checksum del manifiesto certificado y mueve exactamente esa versión
```

| Riesgo | Cómo lo mitiga la propuesta |
|---|---|
| 1 | Rutas por ejecución que nunca se sobrescriben, más checksum verificado antes de transportar |
| 2 | Los totales se calculan una sola vez, en Glue, con decimales exactos; la Lambda solo los muestra |
| 3 | Un solo disparo por ejecución desde el orquestador; el token de la tarea evita decisiones dobles |
| 4 | El orquestador captura los fallos de Glue; la validación detecta los datos vacíos |
| 5 y 9 | La Lambda escapa todo el contenido y valida los destinatarios contra una lista permitida |
| 6 y 7 | La Lambda lee un manifiesto de pocos KB, no el dataset |
| 8 y 11 | El manifiesto declara periodo, esquema y versión, tomados de los datos |
| 10 | La Lambda de Notificación ya no necesita leer los datos: rol mínimo |
| 12 | El `ejecucion_id` viaja de punta a punta |

La espera de la decisión humana con token de Step Functions es el mismo mecanismo de "pendiente de aprobación" que ya usa el Proyecto Nuevos Cargues (manual V5). El patrón se alinea con los estándares de trazabilidad (HU-TR-01 y HU-TR-04) y de gobierno de ingestas (HU-GI-05).

## Para validar cuando haya acceso
1. ¿Hoy hay un orquestador (Step Functions) o las Lambdas se disparan por eventos de S3?
2. ¿La Lambda de Notificación calcula los totales o los recibe ya calculados de Glue?
3. ¿Glue escribe en una ruta por ejecución o sobrescribe el mismo prefijo?
4. ¿Qué datos lleva el CSV de detalle y cómo se genera su enlace?
5. ¿Las tablas se generan con `to_html(escape=False)`?

Cuando se confirmen, estos riesgos pasan a la matriz de brechas (`Docs\06-brechas\`) con IDs `ARQ`, `SEG`, `COS` y `CUM`.
