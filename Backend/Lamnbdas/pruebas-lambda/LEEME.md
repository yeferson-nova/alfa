# Pruebas / fuera de alcance

Lambdas del inventario que **no son de trabajo** para el proyecto NIIF 17 (pruebas, versiones anteriores, datalake de clientes, utilidades de la plataforma).
Se movieron aquí el 2026-11-03 sin modificarlas. Para restaurarlas, moverlas de vuelta a `..\codigo\` y `..\config\`.

Las lambdas **de trabajo** quedan en `..\codigo\` y `..\config\`: las 6 `niif-dev-configuration-<malla>` y `niif-dev-manager-configuration`.

**2026-11-10:** `niif-dev-certification` y `niif-dev-short-link-redirect` pasaron a ser de trabajo (cadena de notificación). Su código desplegado está ahora en `Backend\Lamnbdas\<nombre>\` y se quitaron sus copias de `codigo\` (la de certificación era idéntica; la de redirección solo traía librerías). Su `config\` se conserva aquí.
