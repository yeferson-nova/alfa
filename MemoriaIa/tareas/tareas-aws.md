---
tipo: tarea
actualizado: 2026-11-03
---
# Tareas del plan de trabajo AWS

Fuente: `Tareas\TareasAsignadas.txt`. Versión humana con contexto completo: `Tareas\00-LEEME.md` y un `README.md` por tarea (T1, T2, T3). Los entregables de cada tarea van en su carpeta. Contexto en [[arquitectura-niif17]].

1. **Ajuste de notificaciones AWS** (paso 3)
   - 1a. Cambio del servicio SNS a SES.
   - 1b. Correo de notificación en HTML, ver [[correo-notificacion]].
   - 1c. Soporte NO se entera por email: pasa a ser un ciclo de monitoreo continuo con logs a Grafana, ver [[monitoreo-grafana]] (en iteración).
   - 1d. Revisar escenarios adicionales de notificación que hagan falta.
2. **Lambdas del gestor de configuración:** unificar el modelo de programación (debe ser uno solo). Transversal a los pasos 2 y 3.
3. **Reserva matemática:** validaciones para detectar errores de consistencia de la data procesada (definición funcional en proceso). Es el cambio principal del paso 3.

## Orden sugerido
2 (base) -> 1a/1b -> 1c/1d -> 3 (al cerrar la definición funcional; mientras tanto, esqueleto del validador).

Diseño asociado: [[diseno-propuesto]]. Dudas: [[preguntas-abiertas]].
