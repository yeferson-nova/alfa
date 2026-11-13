---
tipo: referencia
estado: vigente
actualizado: 2026-11-12
---
# Plantilla de la ficha de repo

> Una ficha por repo de Alfa, en `MemoriaIa/repos/<nombre-del-repo>.md`. Nombre de archivo = nombre exacto del repo, para enlazar con `[[nombre-del-repo]]`. Entre 20 y 40 líneas. Español. Lo no confirmado en el código se marca "propuesta" o "por confirmar".

## Reglas
- Describir lo que hace el código **leyéndolo**, no suponiendo por el nombre.
- Sin valores sensibles: nada de ID de cuenta, ARN completos, correos, secretos ni URL con datos. Los recursos van con `{env}` (ej. `niif-{env}-assets`).
- Sin bloques de código. Solo nombres de archivos, funciones y recursos.
- Enlazar con `[[nota]]` solo notas que existan en `MemoriaIa` (verificar con `grep -rl`). Los IDs de brecha (`SEG-xx`, `COS-xx`, `ARQ-xx`, `CUM-xx`) solo si aparecen en las notas; citar el ID y la nota donde está.
- No inventar el paso del diagrama: tomarlo de `[[arquitectura-niif17]]`.
- En repos de IaC la sección se llama `## Recursos AWS que declara` (el validador acepta cualquier título que empiece con `## Recursos AWS`).

## Plantilla (copiar tal cual)
```
---
tipo: repo
estado: vigente | nuevo | obsoleto
actualizado: 2026-11-12
repo: <nombre-del-repo>
categoria: lambda | glue | s3 | iac
paso: <paso o capa del diagrama, ej. "3 · Gold">
malla: <primas-rentas | primas-endosos | primas-arl | auxilios-funerarios | reserva-matematica-arp | reserva-matematica-rvi | transversal>
---
# <nombre-del-repo>
> Una línea: qué hace.

## Rol en el flujo
- Capa o paso y qué lo dispara (trigger, evento, Step Function, programación).
- A quién le entrega (siguiente componente).

## Entradas y salidas
- Entra: ...
- Sale: ...

## Recursos AWS que toca
- Lista con nombres `niif-{env}-...` (buckets, tablas DynamoDB, topics, funciones, jobs).

## Código
- Archivos clave y qué hace cada uno (5 a 8 líneas).
- Arquitectura (hexagonal, script único, etc.) y cómo se prueba.

## Estado
- En Alfa: rama exportada y si está desplegado (ver `VERSIONES.md` de `alfa-repos`).
- Código nuevo nuestro: rama `feature/...` de `alfa-repos` o "ninguno".

## Brechas y relacionadas
- Brechas con ID y nota; features e inprods que lo tocan.
- Relacionado: [[...]]
```
