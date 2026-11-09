# Plantillas de correo de certificación NIIF 17

Plantillas HTML para clientes de correo (Outlook de escritorio, Outlook web y móvil, Gmail web y app, Apple Mail) que llena el job `niif-dev-notification-certification` y envía por SES.

## Mapa de carpetas

| Carpeta | Qué contiene | Se usa |
|---|---|---|
| `newUx\` | **Las 5 plantillas de producción**, con el diseño nuevo. Son las que se suben a `s3://niif-dev-assets/email-templates/`. Ver su `README.md` | Sí |
| `anterior\` | Las 5 plantillas con el diseño anterior (compatibles con correo, sin el marcador `$avisoHtml`) | No. Referencia histórica |
| `mockup\` | `propuesta-ux-notificaciones.html`: mockup de propuesta UX/CX (no es una plantilla). Se abre en el navegador | No. Propuesta |
| `previews\` | Correos de ejemplo ya llenos: 3 variantes por plantilla (15 archivos) y un `index.html`. Datos inventados | Para revisar |
| `herramientas\` | `generar_previews.py` y `validar_plantillas.py` | Para trabajar |

Las 5 plantillas, una por caso de uso: `primas_arl_cert`, `auxilio_funerario_cert`, `previsional`, `rentas_vitalicias_cert`, `reservas_matematicas_cert`.

## Cómo trabajar

1. **Editar** una plantilla en `newUx\`.
2. **Regenerar las vistas previas:** `python herramientas/generar_previews.py`. Usa las mismas funciones del job (`build_html_body`, `result_to_html`, `details_to_html`, `notices_to_html`), así que lo que se ve es lo que sale por SES. No necesita AWS.
3. **Validar:** `python herramientas/validar_plantillas.py`. Revisa `newUx\`, `anterior\` y `previews\` contra el estándar de HTML para correo. Devuelve código 1 si hay algún ERROR. Las pruebas del job (`Backend\Glue\niif-dev-notification-certification\tests\`) ejecutan la misma validación.
4. **Mirar** `previews\index.html` en el navegador. Para ver el ancho móvil real hay que usar las herramientas de desarrollador del navegador con un ancho de 390 px (una ventana de escritorio reducida suele tener un mínimo de ~500 px).
5. **Subir** las plantillas de `newUx\` a `s3://niif-dev-assets/email-templates/` y probar con un envío real a una dirección verificada.

Las vistas previas son un apoyo. No reemplazan la prueba en clientes reales (Outlook de escritorio, Gmail web y app, iOS Mail).

## Variantes de cada vista previa
| Archivo | Qué muestra |
|---|---|
| `01-normal.html` | Correo con total, resumen y archivos de detalle |
| `02-con-aviso.html` | Sin datos más aviso de regeneración (recuadro de avisos, tablas vacías, sin archivos) |
| `03-estres.html` | 12 columnas, valores largos, caracteres especiales (`& < > ñ á`), nombres de archivo muy largos y avisos largos |

## Estándar que valida `validar_plantillas.py`
| Regla | Nivel |
|---|---|
| `DOCTYPE`, `lang`, `meta charset`, `meta viewport`, `<title>` | Error |
| Maquetación solo con tablas, todas con `role="presentation"` | Error |
| Sin `<script>`, `<iframe>`, `<form>`, `<svg>`, `<button>`, `javascript:` ni eventos (`onclick`) | Error |
| Sin CSS de maquetación moderna (`flex`, `grid`, `float`, `position`, `var()`, `calc()`, `vw/vh`, `@import`, `@font-face`, animaciones) | Error |
| Tablas de datos con estilos en línea en cada celda (Gmail sin `<style>`) | Error |
| `table-layout:fixed` en las tablas contenedoras (si no, una tabla ancha ensancha todo el correo en móvil) | Error |
| `@media` para móvil | Error |
| Tabla fantasma y botones VML para Outlook de escritorio (comentarios condicionales `[if mso]`) | Error |
| Enlaces con `href` y esquema `https:`, `mailto:` o `tel:` | Error |
| Tamaño menor a 102 KB (Gmail recorta desde ahí) | Error; aviso desde 90 KB |
| Marcadores: solo los que entiende el job; los obligatorios presentes; `$avisoHtml` una vez | Error |
| Correos de ejemplo sin ningún marcador sin reemplazar | Error |
| Contraste de color texto/fondo menor a 4.5:1, `box-shadow`, `background-image`, `overflow-x` | Aviso |
| Ancho máximo mayor a 700 px, vista previa oculta, `color-scheme` | Aviso |

## Contrato con el job
El job llena estos marcadores (con `string.Template.safe_substitute`):

| Marcador | Contenido |
|---|---|
| `$useCaseName`, `$periodRange` y las demás variables del caso | Texto, escapado con `html.escape` |
| `$tableTotal`, `$tableSummary` | Tablas de las consultas de Athena (`queries.total` y `queries.summary`), con estilos en línea |
| `$tableDetail` | Enlaces a los archivos de detalle (enlaces cortos) |
| `$certificarLink`, `$nocertificarLink` | Enlaces cortos de los botones |
| `$avisoHtml` | Recuadro con los avisos (regeneración, sin datos, disponibilidad por miembro). Vacío si no hay avisos |

Si una plantilla no tiene `$avisoHtml` y hay avisos, el job envía **solo texto** y lo deja en el log, para no ocultar el aviso. Si la plantilla no existe en S3, también envía solo texto.

## Pendiente
- Probar en clientes reales: Outlook de escritorio, Outlook web y móvil, Gmail web y app, iOS Mail, modo oscuro.
- Decidir la seguridad de los botones Certificar y No certificar (hoy son enlaces cortos directos).
