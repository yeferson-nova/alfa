# Monitoreo continuo de la solución NIIF 17 en Grafana (borrador en iteración)

**Fecha:** 2026-10-30
**Estado:** borrador. Faltan decisiones, ver sección 4.

## 1. Qué se busca
Soporte no recibirá correos cuando un proceso falle. En su lugar, la solución NIIF 17 debe cumplir un **ciclo de monitoreo continuo**: sus logs llegan a Grafana y desde ahí se vigila todo. El alcance es toda la solución NIIF 17, **solo lo que corre en AWS**.

Esto reemplaza lo que antes se planteaba en la tarea 1c (notificar a soporte por correo). El correo de certificación para usuarios de negocio (tareas 1a y 1b) no cambia.

## 2. Opciones para almacenar los logs

| Opción | Cómo funciona | A favor | En contra |
|---|---|---|---|
| A. CloudWatch Logs directo | Grafana consulta CloudWatch | Simple, nativo de lambdas y Glue | Búsqueda de texto limitada, retención y costo de consultas |
| B. CloudWatch → OpenSearch | Los logs se reenvían y Grafana consulta OpenSearch | Búsqueda potente, retención larga | Más piezas, costo y operación |
| C. Elastic corporativo | Los logs van al Elastic de la compañía | Reutiliza lo existente | Depende de otro equipo, duplica alertas con Kibana |

**Recomendación inicial:** la opción A, y evolucionar a B o C solo si hay requisito de retención o de búsqueda entre servicios.

## 3. Lo que aplica con cualquier opción
- Logs en JSON con campos comunes: id de correlación, caso de uso, periodo, paso (2 o 3), componente, estado y error. Se define junto con el modelo único de lambdas (tarea 2).
- Métricas además de logs: errores y duración de lambdas, estado de los jobs de la bodega, conteo de registros por periodo.
- Ciclo: emitir → recolectar → almacenar → visualizar → alertar → atender → revisar.

## 4. Pendiente de definir
- Dónde corre Grafana y quién lo administra (no definido).
- Si existe un Elastic u OpenSearch corporativo.
- Retención mínima exigida para los logs.
- Si el alcance incluye otros servicios AWS además de los pasos 2 y 3.
- Cómo se entera soporte de una falla: canal o herramienta de tickets (no definido).
