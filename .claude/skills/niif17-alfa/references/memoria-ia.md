# MemoriaIa: cómo usar la memoria de agentes

`MemoriaIa\` es la memoria compartida de los agentes de IA del proyecto: qué tenemos, qué falta, qué se decidió y en qué estado está cada cosa. El estándar e índice completos están en `CLAUDE.md` (raíz del repo); esto es el protocolo de uso.

## Al iniciar o al perder contexto
1. Leer el índice de `CLAUDE.md`.
2. Abrir solo las notas del tema. Mínimo útil para retomar: `[[pendientes]]` (qué falta), `[[preguntas-abiertas]]` (qué está bloqueado), `[[preferencias-usuario]]` (cómo trabajar), y la nota de la tarea en curso.
3. Si una nota nombra un archivo, una función o un recurso, verificar que sigue existiendo antes de recomendarlo.

## Dónde va cada cosa
| Subcarpeta | Contenido | `tipo` |
|---|---|---|
| `meta\` | rutas, preferencias, preguntas abiertas, pendientes | referencia, preferencia, pendiente |
| `contexto\` | arquitectura, inventario, entorno local, marco normativo | contexto, referencia |
| `tareas\` | diseño y estado de T1, T2, T3 y sus piezas | tarea, diseno |
| `estandares\` | fichas de lineamientos del banco y el estándar propio | estandar |
| `hu\` | fichas de las HU transversales | hu |
| `brechas\` | brechas, riesgos e incidentes | brecha, referencia |

## Reglas de escritura
- Una nota = un tema, kebab-case, nombre único en toda la memoria; enlaces `[[nombre]]` sin extensión ni carpeta.
- Frontmatter con `tipo` y `actualizado: AAAA-MM-DD` (actualizar la fecha al editar).
- Actualizar la nota existente antes de crear otra; si es nueva, agregarla al índice de `CLAUDE.md`.
- La nota resume y apunta al documento humano en `Docs\` o `Tareas\`; no copia su contenido.
- Fechas absolutas, nunca "ayer" o "la semana pasada".
- Marcar lo no confirmado como **propuesta** o **hipótesis**.
- Nada sensible (repo público): sin tokens, correos de personas, IDs de cuenta, IPs.

## Estados que se registran
- **Trabajo:** `[[pendientes]]` con casillas `- [ ]` / `- [x]`, fecha y ruta del entregable. Un componente pasa por: propuesto → diseñado → implementado en local (pruebas y cobertura) → desplegado en DEV → validado → QA → PRD. Escribir siempre en cuál está y qué falta para el siguiente.
- **Dudas y bloqueos:** `[[preguntas-abiertas]]`, agrupadas por tema y destinatario (negocio, DevOps, Arquitectura). Al resolverse, mover la respuesta a la nota del tema y borrar la pregunta.
- **Decisiones:** en la nota del tema, con fecha y quién decidió.
- **Brechas:** ID `SEG/COS/ARQ/CUM-xx`, igual en `Docs\06-brechas\`. Los IDs locales (`PP`, `RYC`, `FLU`, `LMB`, `SF`, `INC`) se mapean en la matriz de la fase 3.
- **Preferencias del usuario:** en `[[preferencias-usuario]]`, solo si las pide como regla permanente.

## Al cerrar un trabajo
1. Actualizar la nota del tema y `[[pendientes]]`.
2. Si cambió una ruta, actualizar `[[rutas-workspace]]`.
3. Si cambió un lineamiento, actualizar también la referencia de esta skill.
4. Si cambió la arquitectura, revisar el mapa de hojas en `arquitectura-y-flujo.md`.
