# Plantillas de correo de certificación (compatibles con clientes de correo)

Versión compatible de las plantillas de `Backend\reservas_matematicas_cert\`, con el mismo diseño, los mismos textos y **los mismos placeholders**. Pueden reemplazar a las originales sin cambiar el código que las llena.

| Archivo | Caso de uso |
|---|---|
| `reservas_matematicas_cert.html` | Reservas matemáticas |
| `primas_arl_cert.html` | Primas ARL |
| `auxilio_funerario_cert.html` | Auxilio funerario |
| `rentas_vitalicias_cert.html` | Rentas vitalicias |
| `previsional.html` | Previsional |

**`newUx\`** contiene las mismas cinco plantillas con el diseño nuevo del mockup. También son un reemplazo directo (mismos placeholders); ver su `README.md`.

**`propuesta-ux-notificaciones.html`** es un mockup de propuesta UX/CX, no una plantilla: un sistema de notificaciones reutilizable para cualquier fuente de Seguros Alfa. Se abre en el navegador y permite elegir fuente y estado para ver el correo en escritorio y en móvil, junto con los principios, los componentes y el contrato de datos.

## Estándar aplicado
- **Estructura:**
  - doctype XHTML 1.0 Transitional;
  - maquetación solo con tablas (`role="presentation"`), sin `flex`, `grid`, `float` ni `position`;
  - estilos en línea en cada elemento.
- **Outlook de escritorio (motor Word):**
  - ajuste de DPI (`PixelsPerInch`);
  - fuente Arial forzada;
  - "tabla fantasma" de 760 px que contiene el ancho;
  - botones en VML (`v:roundrect`) con esquinas redondeadas, en los que todo el botón es clicable.
- **Gmail, Apple Mail, Outlook web y móvil:**
  - botones `<a>` con relleno, borde y radio;
  - contenedor fluido con `max-width`;
  - consulta `@media` que en pantallas de menos de 600 px apila los botones a lo ancho;
  - las tablas de datos con muchas columnas se desplazan en horizontal.
- **Otros ajustes:**
  - modo claro declarado (`color-scheme`);
  - texto de vista previa oculto;
  - sin detección automática de teléfonos ni fechas en iOS;
  - emojis de los botones como entidades HTML (`&#9989;`, `&#10060;`);
  - el `<q>` original se cambió por comillas tipográficas en cursiva.

## Contrato con el backend
Placeholders sin cambios: `$useCaseName`, `$periodRange` (o `$perido_a`), `$tableTotal`, `$tableSummary`, `$tableDetail`, `$certificarLink`, `$nocertificarLink` y `$redshiftLink` (este último está comentado, como en el original).

- **Tablas inyectadas:** el diseño de las tablas (bordes #999, encabezado #eee, filas alternas) se aplica desde el `<style>` con las clases `dataframe` y `.tbl-wrap`. Algunos clientes eliminan `<style>`: la app de Gmail con cuentas que no son de Google y algunos webmails corporativos. En esos clientes la tabla conserva solo el atributo `border="1"`.
- **Recomendación:** para máxima fidelidad, el backend debería generar las tablas con estilos en línea. Por ejemplo, con pandas `Styler` o pasando el HTML por un *inliner* de CSS. Las filas alternas no se ven en Outlook en ningún caso.

## Pendientes detectados
- `reservas_matematicas_cert.html` muestra el periodo como `$perido_a - $perido_a`: la misma variable dos veces y con errata. Las demás usan `$periodRange`. Se mantuvo igual para no romper el código; conviene corregirlo junto con el backend.
- Se agregó un punto antes de "A continuación" en el texto de introducción; en el original faltaba.
- Falta probar en clientes reales (Outlook de escritorio, Gmail web y app, iOS Mail). La validación hecha es estática: estructura, placeholders, CSS no compatible y sustitución de prueba.
