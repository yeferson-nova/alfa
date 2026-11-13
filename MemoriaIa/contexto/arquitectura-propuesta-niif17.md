---
tipo: contexto
estado: vigente
actualizado: 2026-11-12
---
# Arquitectura propuesta (segunda arquitectura, nuestra)

> `Docs\05-arquitectura\diagramas\NIIF17_ModeloSolucion_V1.0-propuesta.drawio` es **nuestra** propuesta, hecha el 2026-11-11 sobre la aprobada ([[arquitectura-niif17]]). Solo cambia la hoja `Página-2` (paso 3); las otras 7 hojas son idénticas. **Regla del usuario (2026-11-12): al tocar las piezas donde las dos difieren, tener en cuenta ambas arquitecturas.** La aprobada manda; la propuesta es lo que pedimos agregar (ARQ-21) y está sujeta a decisión.

## Qué agrega la propuesta (hoja `Página-2`)
Se compara contra el XML de las dos hojas: 24 elementos nuevos, ninguno borrado ni modificado. Numeración nueva 72 a 78.

| Pieza | Aprobada (V1.0) | Propuesta | Repo |
|---|---|---|---|
| **Lambda (Certificación)** (72-73) | No existe. El API llega a la Lambda (Notificación) | Recibe `POST /certify` y `/not_certify` del API; valida que el usuario sea aprobador, registra el voto y escribe `CERTIFICATE` o `NO_CERTIFICATE` en el Modelo Notificaciones | [[sc-datalake-niif-certification]] |
| **Lambda (Enlace corto)** (74-75) y **Modelo Enlaces cortos** | No existen | `GET /r` resuelve el token del correo en una tabla nueva y redirige al login. Los enlaces los crea el Job Notificación | [[sc-datalake-niif-short-link-redirect]] |
| **Lambda (Evaluador compuesto)** (76-78) y **Modelo Compuesto** | No existen | La llama la Malla de Ejecución; espera a todos los casos de un grupo (Rentas con Endosos, ARP con RVI) y libera una sola certificación; escribe el Modelo Compuesto y el Modelo Notificaciones | [[sc-datalake-niif-composite_notification_evaluator]] |
| **Lambda (Notificación)** | Una sola lambda: recibe del API, lanza el Job, envía el Mensaje por SES y escribe en las zonas | Pasa a ser la **orquestadora**: reemplaza al trigger, a copy y a move; según el estado lanza el Job, copia a transporte o archiva los no certificados, y reprocesa los atascados | [[sc-datalake-niif-notification-certification-orchestrator]], [[sc-datalake-niif-notification-certification]], [[sc-datalake-niif-copy-s3-certification]], [[sc-datalake-niif-mov-s3-certification]] |
| **Quién envía el correo** | El diagrama dibuja Lambda (Notificación) → Mensaje (SES) | El texto de la nota dice que lo envía el **Job Notificación**, no la lambda (ARQ-22). La flecha original sigue dibujada | [[data-datalake-niif-notification-certification-glue-job]] |
| **postLogin()** | `postLogin()` → Cognito | Se renombra `niif-{env}-post-login`; recibe al usuario autenticado y llama a certificar o no certificar | [[sc-datalake-niif-post-login]] |
| **Entrada del API (66)** | API → Lambda (Notificación) y Zona Certificación | Se agregan API → Lambda (Certificación) y API → Lambda (Enlace corto); **pendiente D7**: la propuesta quiere que la pieza expuesta a internet no tenga permisos de Glue ni de transporte | [[sc-datalake-niif-certification]] |

Sin diferencia: Lambda (SFTP) → Transit Gateway → FIS, y todo el medallón, la programación y los pasos 1 a 12 ([[sc-datalake-niif-send-via-sftp-lambda]]).

## Cómo usar las dos
1. Antes de diseñar o cambiar una de las piezas de la tabla, abrir esta nota y la `Página-2` de ambos diagramas.
2. Decir en la respuesta o en el PR cuál de las dos arquitecturas sigue el cambio y dónde se aparta de la aprobada.
3. No dar por aprobado lo que solo está en la propuesta: lo de esta tabla se marca "propuesta" hasta que el arquitecto lo apruebe. D7 y ARQ-22 siguen abiertas ([[notificaciones-vs-arquitectura]], [[roadmap-notificaciones]]).
4. Si la propuesta se aprueba, pasa a ser la V1.0: actualizar [[arquitectura-niif17]] y la skill.
