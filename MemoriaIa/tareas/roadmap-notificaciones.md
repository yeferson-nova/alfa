---
tipo: tarea
actualizado: 2026-11-11
---
# Roadmap de la cadena de notificación (contexto para IA)

Humano (control, con ✅ y ⬜): `Tareas\T1-notificaciones\roadmap-notificaciones.md`. Esta nota resume; no duplicar el detalle.

- **Alcance:** el flujo completo de la hoja `Página-2`: 6 lambdas y 1 job de Glue, del Modelo Notificaciones hasta FIS, incluida la Lambda SFTP. Fuera: el resto del paso 2.
- **Orden (pasos):** 1 cerrar el job de notificación · 2 medir Athena (hecho en DEV) · 3 ID único (etapa 1 hecha) · 4 orquestadora · 5 transporte a FIS · 6 resto de lambdas (6.1 a 6.4) · 7 mejoras de Athena · 8 alertas y métricas · 9 limpieza y diagrama. En paralelo: Gold particionado y las decisiones pendientes.
- **Componentes y carpetas (código nuevo en `Backend\notificacion\`):**
  - `jobs-glue\niif-dev-notification-certification` (515 pruebas, probado en Glue de DEV sin envío);
  - `lambdas\`, con 5 esqueletos: `niif-dev-notification-certification-orchestrator` (reemplaza a trigger, copy y move), `niif-dev-composite-notification-evaluator`, `niif-dev-certification`, `niif-dev-short-link-redirect` y `niif-dev-post-login`;
  - `email-template\`.
  - La Lambda SFTP `datalake-dev-niif-send-via-sftp` está inactiva en DEV y su nombre está por decidir.
  - El código actual está en `Backend\Lamnbdas\` y `Backend\Glue\`.
- **Decisiones tomadas:**
  - D1 una sola orquestadora;
  - D5 alertas por SES, parametrizadas (grupo de soporte pendiente);
  - D6 nombres en el estándar `niif-<amb>-*`;
  - E1 certificación = lo desplegado + INC-3;
  - E2 el ID = el nombre de la ejecución `<malla>-<periodo>-<uuid v7>`, generado una sola vez por el configurador y propagado con `$$.Execution.Name`;
  - E3 en el compuesto se guardan `ejecucionId` y `ejecucionesOrigen`.
- **Decisiones pendientes:** D2 (No certificar), D3 (descarga del CSV), D4 (reenvío sobre `SENT`), D7 (si el API va a la orquestadora o a la lambda de certificación) y el nombre de la Lambda SFTP.
- **Hallazgos de la medición (DEV, 2026-11-11):**
  - Las consultas tardan 0,8-2,3 s y Athena casi no cuesta.
  - Los jobs gastan casi todo su tiempo en el arranque de Spark (propuesta: Python shell).
  - COS-07 y COS-08 confirmados.
  - El Gold de 5 mallas se vació en DEV por pruebas: probar con ARL.
- **Diagrama:** la propuesta está en `NIIF17_ModeloSolucion_V1.0-propuesta.drawio`, hoja `Página-2` (72-78 y una nota); el original no se toca.
- **Forma de trabajo:** no cambiar código sin una orden explícita ([[preferencias-usuario]]); cada paso en AWS va con confirmación. Ver [[notificaciones-vs-arquitectura]], [[job-notificacion-ses]] y [[pendientes]].
