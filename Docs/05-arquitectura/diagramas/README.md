# Diagramas de arquitectura

| Archivo | Qué muestra | Editable |
|---|---|---|
| `flujo-niif17-asis.drawio` | **AS-IS:** el flujo de los pasos 2 y 3 tal como está desplegado hoy en DEV, de punta a punta, con iconos de AWS | Sí, en draw.io (diagrams.net) |
| `flujo-niif17-asis.png` | Vista del mismo diagrama para leerlo sin abrir draw.io | No |
| `recorrido-dato-niif17.drawio` | **Recorrido del dato, animado:** cómo sale el dato de Oracle, por dónde pasa (lambdas, Step Function, jobs de Glue, S3) y cómo se transforma en cada etapa, hasta la entrega a FIS | Sí, en draw.io |
| `recorrido-dato-niif17.html` | El mismo recorrido para abrir en el navegador con las flechas en movimiento (necesita internet para cargar el visor de draw.io) | No |
| `recorrido-dato-niif17.png` | Vista fija del recorrido | No |
| `..\arquitectura-tobe-niif17.png` y `..\aws_pasos_2_y_3.drawio` | **TO-BE:** la arquitectura objetivo, de alto nivel | Sí, el `.drawio` |

## Cómo leer el diagrama AS-IS
Cada carril (segmento) es una fase y sigue la numeración de `..\flujo-integral-niif17.md`:

| Carril | Fase | Pasos |
|---|---|---|
| A | Programación (DynamoDB → manager → EventBridge Scheduler) | 1-4 |
| B | Configuración (6 lambdas `configuration-<malla>`, las que T2 consolida en una) | 5-9 |
| C-D | Step Function `niif-dev-<malla>` y jobs de Glue | |
| E | Certificación y notificación | 10-14 |
| F | Aprobación y transporte | 15-16 |
| G | Observabilidad y errores | |

- Flecha negra continua: flujo principal.
- Flecha gris discontinua: lectura, estado o paso **inferido o no verificable** (por ejemplo, el manager y el envío SFTP no tienen código en el repositorio).
- Flecha y notas rojas: fallos y riesgos.
- Los iconos con "(sin código)" son componentes de los que solo hay configuración.

## Cómo leer el recorrido del dato
Cuatro carriles, de arriba abajo, con las etapas numeradas del 1 al 18:

| Carril | Etapas | Qué se ve |
|---|---|---|
| 1. Control | 1-4 | Programación → manager → Scheduler → configurador (las lambdas que preparan la ejecución) |
| 2. El dato | 5-11 | La Step Function (barra superior) orquesta los jobs de Glue: Oracle → Bronce → validación → remediación → Silver → Gold → catálogo y Athena. Cada recuadro amarillo dice qué le pasa al dato en esa etapa |
| 3. Del dato al negocio | 12-18 | Evaluador → certificación (CSV) → correo → aprobación → copia a transporte → SFTP a FIS |
| 4. Estado y vigilancia | | Cómo se anota el estado y cómo se avisa si algo falla |

Flecha naranja animada = el dato; azul animada = control y orden de ejecución; roja = falla; gris punteada = consulta o paso no verificable. En draw.io, las flechas se mueven al abrir el archivo (propiedad "Flow Animation" de las aristas).

## Fuente y validación
Se construyó con las definiciones de las Step Functions, el código de los jobs de Glue y de las lambdas, y las reglas de EventBridge extraídas el 2026-11-03 (ver `..\flujo-integral-niif17.md`). Si el desplegado cambia, el diagrama y ese documento deben actualizarse juntos.

## Editar
Abrir el `.drawio` en <https://app.diagrams.net> o en draw.io Desktop. Los iconos son del conjunto oficial AWS Architecture 2023 incluido en draw.io (formas `mxgraph.aws4.*`). Tras editarlo, exportar de nuevo el PNG con el mismo nombre.
