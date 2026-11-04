# Cómo se conectan las tareas asignadas con el flujo actual

**Tareas** (`Tareas\TareasAsignadas.txt` y la lista de lambdas de la T2):
1. Ajuste de notificaciones: (a) de SNS a SES, (b) correo en HTML, (c) aviso a soporte cuando un proceso falla, (d) revisar escenarios adicionales.
2. Unificar las lambdas del gestor de configuración en **una sola lambda general estandarizada** (las 6 `niif-dev-configuration-<malla>` y `niif-dev-manager-configuration`).
3. Validaciones de consistencia sobre los datos procesados de reserva matemática (la definición funcional está en curso).

**Fuente:** `flujo-integral-niif17.md` (cómo funciona hoy) y `jobs-glue.md`. Aquí no se repite el flujo: se indica dónde actúa cada tarea, qué ya se sabe, qué falta y un orden de trabajo. Lo propuesto está marcado como **propuesta** y se valida con el equipo antes de cambiar nada. Los problemas actuales están en `Docs\06-brechas\revision-pre-produccion.md`.

---

## T1. Notificaciones

### Dónde actúa en el flujo
Todo ocurre en la **cadena de certificación**: el job `niif-dev-notification-certification` (se activa cuando una fila de `notification-certification` pasa a `PENDING`) arma y envía el correo; `PipelineObserver` avisa de las fallas de la Step Function.

### T1a. De SNS a SES
- **Hoy:** el job publica con `sns.publish` un mensaje por destinatario en el tópico `niif-dev-certification-email`, y antes de cada envío reconfigura el `FilterPolicy` de las suscripciones para que cada correo llegue solo a su destinatario.
- **Propuesta:** reemplazar ese bloque por un envío directo con SES (`SendEmail` con cuerpo HTML y texto), uno por destinatario. El `FilterPolicy` y el tópico dejan de ser necesarios para el correo. El código ya guarda el `MessageId` y marca `SENT` solo tras el envío, así que ese manejo se conserva con el identificador de SES.
- **Hace falta:** dominio o dirección verificada en SES, saber si la cuenta está en *sandbox* (en ese caso solo envía a direcciones verificadas), permiso `ses:SendEmail` para el rol `niif-dev-jobs-notification-certification`, y un conjunto de configuración con eventos de rebote y queja (el seguimiento de entrega que SNS no da). Los destinatarios reales están en los JSON de cada caso en S3, que no se han visto.
- **Riesgo:** SNS se sigue usando en otros avisos (alertas técnicas); no se retira.

### T1b. Correo en HTML
- **Hoy:** el cuerpo es una sola cadena de texto armada en el código (con resumen, totales, archivos y los enlaces Certificar y No certificar).
- **Ya hecho:** hay 5 plantillas HTML en `Backend\htmlNew\` (versión compatible con clientes de correo y versión en el diseño nuevo), con los mismos campos.
- **Propuesta:** el job rellena la plantilla del caso con los datos que ya calcula (nombre del caso, resumen, totales, archivos, enlaces), agrega siempre una versión en texto, y cubre los avisos `[REGENERADO]` y "sin datos por periodo" dentro del HTML. Las tablas de resumen y totales salen de Athena y hay que convertirlas a tablas HTML.
- **Hace falta:** decidir dónde viven las plantillas (S3 junto a la configuración del caso) y probar en los clientes de correo reales del banco.

### T1c. Aviso a soporte cuando un proceso falla
- **Hoy:** solo avisa (por SNS) cuando **falla una Step Function** (`PipelineObserver`).
- **No avisa:** el configurador, el manager, el job de notificación, las lambdas de transporte, el SFTP, ni un fallo de job que la Step Function absorba.
- **Propuesta:** extender el observador y las reglas de EventBridge a los jobs `niif-dev-*` que fallen (ya llegan los eventos), agregar alarmas a las lambdas de la cadena y colas de fallos (DLQ) a los streams, y definir el destino del aviso (correo de soporte; ServiceNow según la propuesta `Tareas\T1-notificaciones\propuesta-observabilidad-eventos.md`, pendiente de validar).
- **Hace falta:** quién recibe el aviso y con qué severidad.

### T1d. Escenarios adicionales (propuesta)
| Escenario | Dónde se detecta | Hoy |
|---|---|---|
| La validación técnica encontró hallazgos | `estado_validacion = FAILED` en el archivo de estado | Nadie se entera; el correo no lo menciona |
| Un grupo compuesto espera a una malla que no llegó | `composite-notification-status` (`READY_TO_NOTIFY` incompleto) | Nadie se entera |
| El aprobador no responde | Filas en `PENDING` o `PENDING_APPROVERS` durante más de N horas | Nadie se entera |
| El correo rebota o se queja | Eventos de SES | Solo una regla de prueba lo escribe a un log |
| Falla el transporte a FIS | `copy-s3-cert` o el SFTP | Sin aviso |
| La programación no se creó o el *schedule* falló | `scheduleEstado` en `prg-programaciones` | Sin aviso |
| Un job o crawler tarda mucho más de lo normal | Eventos de Glue (`NotifyDelayAfter`) | Solo métrica |
| Ejecución sin datos nuevos | Ya se avisa con `[REGENERADO]` | Cubierto |

---

## T2. Una sola lambda general (y el manager)

### Dónde actúa en el flujo
En los pasos A y B: el manager crea el *schedule* y el configurador de la malla valida, publica los 4 JSON y arranca la Step Function.

### Qué ya se sabe
- Las 6 `configuration-<malla>` **son la misma lambda**: solo cambian tres variables de entorno.

| Malla | `MALLA_PK` | Step Function |
|---|---|---|
| Primas Rentas | `MALLA#PRIMAS_RENTAS` | `niif-dev-primas-rentas` |
| Primas Endosos | `MALLA#PRIMAS_ENDOSOS` | `niif-dev-primas-endosos` |
| Primas ARL | `MALLA#PRIMAS_ARL` | `niif-dev-primas-arl` |
| Auxilios Funerarios | `MALLA#AUXILIOS_FUNERARIOS` | `niif-dev-auxilios-funerarios` |
| Reserva Matemática ARP | `MALLA#RESERVA_MATEMATICA_ARP` | `niif-dev-reserva-matematica-arp` |
| Reserva Matemática RVI | `MALLA#RESERVA_MATEMATICA_RVI` | `niif-dev-reserva-matematica-rvi` |

  (Rentas está confirmado; los demás siguen el mismo patrón, por confirmar.)
- **El manager ya está parametrizado:** `CONFIGURADORES_JSON` asigna cada malla a su lambda. Con una lambda general, ese mapa se reduce a un solo destino y el código del manager casi no cambia.
- **Contrato de entrada:** `{publicar, ejecutar, periodoNegocio, modoEjecucion, origen}`, que el Scheduler envía al configurador (detalle en el flujo, paso A.4).
- **Spike hecho:** `Backend\Lamnbdas\inventario-aws\spike-primas-rentas\` reescribe Primas Rentas por capas (37 pruebas, 98 % de cobertura del núcleo) y trae un script de despliegue para CloudShell. Falta probar los adaptadores con LocalStack y comparar contra datos reales.

### Propuesta, en orden
1. **Recuperar el código real** de las 5 configuradoras que faltan y del manager (hoy hay copias de la versión anterior y nada del manager).
2. **Confirmar con ese código** que las 6 solo se diferencian en la configuración y en las dos funciones que leen la programación y el periodo.
3. **Una lambda general** que reciba la malla (evento o variable) y lea lo específico de `cnf-mallas`, con un único contrato de entrada y de respuesta.
4. **Pruebas** del núcleo y de los adaptadores (LocalStack), y repositorio con el pipeline (ver PP-02 de la revisión).
5. **Migrar** el manager a un solo destino, probar en paralelo y retirar las 6 lambdas y la generación anterior (`configurador-<malla>` y `programador-configuraciones`).

### Dependencias y riesgos
- Primero hay que **dejar un solo programador** escuchando `prg-programaciones` (hoy hay dos activos); si no, probar el cambio puede iniciar dos veces el proceso.
- Decidir cuál generación del gestor es la oficial.
- El `ejecucion_id` en la ruta del archivo de estado es la mejora que más conviene aprovechar mientras se unifica (PP-04).

---

## T3. Validaciones de consistencia en reserva matemática

### Dónde actúa en el flujo
Los datos de reserva matemática pasan por Silver (`silver-reserva-matematica-arp` y `-rvi`, que calculan las salidas de 440 y 458 columnas) y por Gold antes de certificarse y transportarse a FIS. La validación actual (`niif-dev-val-tecnica`) corre **antes** de esos cálculos y revisa solo columnas sueltas, sin bloquear.

### Qué ya se sabe
- El job de validación **ya se configura por JSON**: las reglas se declaran por dataset y columna. Eso permite sumar reglas sin cambiar código, que es lo que pide el estándar.
- La validación nunca detiene el flujo (`validacion_tecnica_detiene_flujo` siempre `False`) y su resultado no llega al correo.
- No hay hoy reglas de duplicados, rangos, cuadre de volúmenes ni coherencia entre campos.

### Propuesta de diseño (a validar)
- **Ampliar el catálogo de reglas del job de validación** con las del README de la T3: duplicados sobre la llave, rangos, coherencia del periodo y **cuadre** (entrada = salida + rechazados; la suma de valores de Silver coincide con Gold).
- **Nueva etapa de validación sobre el resultado** (después de Gold, antes de la certificación), porque las inconsistencias de reserva matemática aparecen en las salidas calculadas, no en las tablas de origen. El diseño original del proyecto la ubicaba como una lambda entre Consolidación y Transporte; con la arquitectura real, un job o consulta de Athena con la misma configuración por JSON encaja mejor. **Propuesta**, por decidir.
- **Comportamiento:** una regla de severidad bloqueante detiene el flujo y avisa (T1c); las demás quedan registradas y se resumen en el correo de certificación, para que quien certifica vea más que los totales.
- **Registro:** resultados de las reglas y registros rechazados en las tablas del estándar (`aud_resultado_reglas_calidad`, `cuarentena_<capa>_<objeto>`).

### Qué se puede avanzar sin esperar al negocio
- La especificación de la regla (contrato, resultado, severidad, qué pasa al fallar) y las **reglas técnicas** del catálogo (estructura, calidad y conciliación).
- Pruebas de equivalencia de los Silver de reserva matemática contra el SQL original (PP-07), que además dan datos de control para las reglas.

### Qué falta
- La **definición funcional** de las reglas de negocio (variación máxima, rangos por producto, relaciones entre campos actuariales) y los umbrales de rechazo.
- Si las reglas aplican solo a reserva matemática o también a los otros cuatro casos.
- Si, ante una falla, el flujo se detiene o sigue al paso 4.

---

## Orden de trabajo sugerido

| Orden | Tarea | Por qué en ese lugar |
|---|---|---|
| 1 | Recuperar el código faltante y los JSON de S3; dejar un solo programador | Todo lo demás depende de ver el código real y de no ejecutar dos veces |
| 2 | **T2** (lambda general) | Es la base de T1 y T3 (los avisos y las reglas se configuran por ahí) |
| 3 | **T1b y T1a** (HTML y SES) | Se pueden hacer en el job de notificación sin esperar a T3 |
| 4 | **T1c y T1d** | Se apoyan en los logs y alarmas que se agregan en T2 |
| 5 | **T3** | Necesita la definición funcional; mientras tanto se avanzan las reglas técnicas |
