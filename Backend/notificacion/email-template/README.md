# Plantillas de certificación con el diseño nuevo (newUx)

Las mismas cinco plantillas de `Backend\reservas_matematicas_cert\`, con el diseño del mockup `..\mockup\propuesta-ux-notificaciones.html`. **Son un reemplazo directo:** mismos nombres de archivo, mismos placeholders y mismo orden de contenido, así que las lambdas actuales las llenan sin cambios.

| Archivo | Secciones |
|---|---|
| `reservas_matematicas_cert.html` | Total General, Archivos de detalle |
| `primas_arl_cert.html` | Total General, Archivos de detalle |
| `auxilio_funerario_cert.html` | Total General, Total Resumen (Auxilios Funerarios por Origen de Renta), Archivos de detalle |
| `rentas_vitalicias_cert.html` | Total General, Total Resumen (Rentas Vitalicias), Archivos de detalle |
| `previsional.html` | Total, El total de registros cargados es, detalle |

## Qué cambia frente a la versión actual
- **Encabezado de marca:** "seguros alfa" con la etiqueta *Datalake · NIIF 17*.
- **Banda de estado:** "Acción requerida · Certificar o no certificar las cifras", con un glifo `!` en un círculo (sin emoji).
- **Título directo:** "Certifique las cifras de `$useCaseName`".
- **Tablas en secciones con título:** la tabla del Total General se resalta con letra más grande y en negrita.
- **Botones diferenciados:** **Certificar** sólido en verde y **No certificar** con contorno rojo, sin emojis.
- **Aviso de incidencias** en un recuadro informativo.
- **Ficha** con caso de uso, periodo y soporte.
- **Pie** con firma y aviso de no responder.

## Compatibilidad con clientes de correo (Outlook de escritorio, Outlook web y móvil, Gmail web y app)
- **Estructura:** solo tablas, con estilos en línea y atributos `bgcolor` en los fondos. No usa `flex`, `grid`, `float` ni `position`.
- **Outlook de escritorio:**
  - ajuste de DPI;
  - fuente Arial forzada;
  - tabla fantasma de 680 px;
  - botones en VML con esquinas redondeadas, en los que todo el botón es clicable.
- **Botones en el resto de clientes:**
  - cada uno va en un bloque `inline-block` que pasa a la línea siguiente si no cabe, incluso en clientes que eliminan `<style>`;
  - en móvil, la consulta `@media` los pone a ancho completo.
- **Tablas anchas:** se desplazan en horizontal dentro de su sección.
- **Otros ajustes:** modo claro declarado, texto de vista previa oculto y sin detección automática de teléfonos ni fechas en iOS.

## Contrato con el job de notificación
`$useCaseName`, `$periodRange`, `$tableTotal`, `$tableSummary`, `$tableDetail`, `$certificarLink`, `$nocertificarLink` y `$avisoHtml`. Ver el contrato completo y las reglas de validación en `..\README.md`.

- **Repeticiones nuevas:** `$useCaseName` aparece también en el título, en la vista previa y en la ficha, y el periodo también en la ficha. Los enlaces de los botones aparecen dos veces: versión Outlook y versión resto. Con `string.Template` no hace falta ningún cambio.
- **Tablas inyectadas:** el job las genera con estilos en línea en cada celda, así que no dependen del `<style>` (que algunos clientes eliminan). Las clases `dataframe` y `.tbl-wrap` del `<style>` quedan como respaldo.

## Validación hecha (estática)
- **Placeholders:** los mismos que en las originales, ninguno dentro de `<style>` y las tablas sin duplicar.
- **Estructura:** etiquetas balanceadas y sin CSS ni etiquetas incompatibles.
- **Sustitución de prueba:** con `string.Template.substitute`, como la lambda, sin variables sobrantes.
- **Tamaño:** unos 15 KB, lejos del límite de 102 KB en que Gmail recorta.

**Falta:** probar en clientes reales (Outlook de escritorio, Gmail web y app, iOS Mail), con Litmus, Email on Acid o envíos de prueba.

## Mejoras del mockup que requieren cambios en la lambda (fase 2)
- Tarjetas de cifras con variación frente al periodo anterior.
- Validaciones de consistencia (tarea T3).
- Fecha límite y vencimiento de enlaces.
- ID de ejecución.
- Estados adicionales: recordatorio, certificado, no certificado y fallo.
- Página de confirmación con inicio de sesión para Certificar y No certificar.

## Cambios del 2026-11-09 (migración de SNS a SES)
- **`$avisoHtml`:** nuevo marcador, una sola vez, justo encima de la sección del total. El job coloca ahí el recuadro de avisos (regeneración, sin datos, disponibilidad por miembro). Sin el marcador, el job envía solo texto cuando hay avisos.
- **`reservas_matematicas_cert.html`:** el periodo pasa de `$perido_a - $perido_a` a `$periodRange`. El job nuevo no entrega `$perido_a`, así que sin este cambio el correo mostraría el marcador literal.
- **Bloques comentados eliminados:** el enlace de Redshift (`$redshiftLink`) y la sección comentada de resumen. Los comentarios HTML llegan al destinatario y, con `$tableSummary` dentro de un comentario, se insertaba una tabla completa que el cliente nunca muestra.
- **`table-layout:fixed`** en las dos tablas contenedoras: sin él, una tabla ancha ensanchaba todo el correo en pantallas de 390 px y se cortaban el título y los botones.
- **Banda:** se cambió el texto "Pendiente de certificación" por "Certificar o no certificar las cifras" (dice qué hacer) y el emoji de reloj de arena por un glifo, según la regla del mockup (un estado distinto solo si cambia la acción).
- **Avisos por tono:** el job pone un recuadro por aviso: azul para "regenerado" y ámbar para "sin datos" y "disponibilidad por miembro".
- Se validaron con `..\herramientas\validar_plantillas.py` y se revisaron visualmente a 390 px y a escritorio.
