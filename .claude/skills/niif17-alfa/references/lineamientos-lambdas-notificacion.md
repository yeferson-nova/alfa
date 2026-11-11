# Lineamientos para lambdas y jobs de notificación, certificación y transporte

Base: `Docs\08-estandares-desarrollo\estandar-lambdas-python.md` (leerlo si se va a escribir código), `[[job-notificacion-ses]]`, `[[correo-notificacion]]`, `[[monitoreo-grafana]]`, `[[hu-remediacion]]` (catálogo de notificaciones), `[[brechas-seguridad]]`.

## 1. Estructura y nombres
- **Manda el gobierno DevOps NIIF 17** (`[[est-gobierno-devops-niif]]`): la lambda se llama `niif-{env}-{nombre}` y el job `niif-{env}-{nombre}`; los repos son `sc-datalake-niif-{nombre}` (lambda) y `data-datalake-niif-{nombre}` (job). `/src/lambda_function.py` es el handler y el resto del código va modular en `src/`. Las pruebas van en `/test/` (con `test_lambda_function.py`; cobertura de 70 % o más para el pipeline y 80 % o más para Sonar). Se entrega un zip con el nombre de la lambda y DevOps hace el commit.
- **Estilo PEP 8:** sangría de 4 espacios; todos los identificadores en inglés (`snake_case` para funciones y variables, `PascalCase` para clases, `UPPER_SNAKE_CASE` para constantes). Los contratos externos en español (campos de DynamoDB, llaves de JSON, estados) se conservan y se declaran como constantes con nombre en inglés.
- Capas: `domain` (modelos, reglas, errores con clasificación; sin `boto3`, sin entorno, sin logs) ← `application` (casos de uso, puertos `Protocol`, catálogo de eventos) ← `adapters` (`inbound` = handler; `outbound` = AWS). `config` es la única raíz de composición. `test/test_architecture.py` lo verifica.
- Entrada validada en el borde a un `dataclass(frozen=True)`; el handler no tiene lógica.
- `boto3` no se empaqueta; cada dependencia se justifica.

## 2. Errores, logs y métricas
- Errores con `code`, `domain`, `severity`, `recoverable`, `action`, `recipient`; códigos `<COMP>-<DOM>-<NNN>` documentados en el README (ej. `NTF-CNF-001`).
- Los adaptadores traducen errores de AWS (`AccessDenied` → permisos, `Throttling` → plataforma recuperable, `ConditionalCheckFailed` → concurrencia).
- El handler registra y **relanza**. Toda invocación asíncrona y todo stream tienen reintentos limitados, división de lote y **destino de fallo** (hoy varios streams reintentan sin fin: no repetir). Lección real: `copy-s3-cert` tragó durante semanas un error de índice y dejó certificaciones en `CERTIFICATE` sin alarma (ARQ-26, ARQ-30).
- Logs JSON de una línea con `ejecucion_id`, `certCode`, `useCaseId`, `periodo`; métricas EMF de éxito, fallo por dominio y duración.

## 3. Idempotencia y concurrencia
- Clave de idempotencia (del evento o `aws_request_id`) registrada en DynamoDB con escritura condicional.
- Un correo por `certCode` y destinatario: si ya está `SENT`, no se reenvía. Las transiciones de estado son condicionales sobre el estado anterior (`PENDING → SENT`, `PENDING_APPROVERS → CERTIFICATE`), para que dos clics o dos aprobadores simultáneos no generen doble decisión.
- Streams de DynamoDB entregan "al menos una vez": todo consumidor debe tolerar duplicados y desorden.

## 4. Correo de negocio (T1a, T1b)
- **SES para todo correo: el de negocio y el de alertas técnicas** (decisión del 2026-11-10; el grupo de soporte que las recibe está pendiente). El diagrama dibuja SES (icono "Mensaje"). Implementado en el job y probado en QA el 2026-11-10.
- Lo aprendido en QA: el rol necesita `ses:SendEmail` sobre la identidad **y** sobre el conjunto de configuración; `--SES_SENDER` debe ser argumento por defecto del job; las plantillas viven en `<bucket-assets>/email-template/` (en singular). Detalle en `[[job-notificacion-ses]]`.
- Identidad del dominio verificada (SPF, DKIM, DMARC), salida de *sandbox*, conjunto de configuración con rebotes y quejas hacia una alarma.
- Plantillas HTML compatibles con clientes de correo (`Backend\notificacion\email-template\`), con los mismos *placeholders*. Todo valor insertado se **escapa**; nada de `to_html(escape=False)`.
- Asunto `NIIF 17 - Certificación <CasoUso> Periodo <AAAAMM>`; marca `[REGENERADO]` cuando aplique.
- **No enviar** si el resumen o los totales están vacíos o el CSV no tiene filas reales: emitir evento de fallo de negocio y alerta técnica (lección del incidente Primas ARL 202606).
- Destinatarios desde metadata del caso, contra lista permitida; nunca desde el evento.
- Sin datos personales en el cuerpo; solo agregados y enlaces.

## 5. Enlaces Certificar / No certificar y descarga
- El clic del correo abre una **página de confirmación**; la decisión se envía por **POST** autenticado (los escáneres de correo siguen GET).
- Token firmado, de un solo uso, con expiración; identidad federada (Cognito / IAM Identity Center con el directorio corporativo).
- Se registra quién, cuándo, desde dónde y sobre qué versión de los datos (checksum del manifiesto). Multiaprobador: `approvers.required` desde metadata, cada voto auditado.
- CSV por URL prefirmada de corta duración, bucket privado con KMS; no saltarse Lake Formation. **Hoy no es así:** el enlace abre la consola de S3 por el portal SSO y exige un rol en la cuenta (ARQ-29). Propuesta: acción `DOWNLOAD` tras el login de Cognito, que devuelve una URL prefirmada de 1 a 5 minutos.
- Ningún valor por defecto apunta a otro ambiente (hoy la lambda de certificación cae a `niif-qa-*` si falta una variable: PP-10).

## 6. Orquestación del paso 3 (dirección objetivo, propuesta)
- Step Functions con Glue `.sync` y `Catch` hacia notificación de fallo.
- Espera de la decisión humana con `waitForTaskToken`: el token del correo es el de la tarea; la ejecución no termina `SUCCEEDED` antes de certificar.
- Transporte verifica el checksum del manifiesto antes de copiar a FIS; lo certificado es exactamente lo transportado.

## 7. Catálogo de notificaciones (T1d)
Cada notificación tiene código `NTF-xx`, evento, severidad, destinatario, canal (correo, ServiceNow, chat), acción esperada y plazo. Escenarios mínimos: éxito para certificar, proceso fallido, validación con alertas, cifras con diferencias, sin datos, timeout, falla de transporte, No certificar, enlace expirado, sin respuesta en plazo, rebote del correo. Soporte se entera por alarmas de CloudWatch que llegan por correo a través de SES al grupo de soporte (grupo pendiente). **Grafana no es nuestro alcance:** se dejan logs JSON, métricas EMF y alarmas listos para que alguien conecte un Grafana. Lo que falte en el diagrama se le sugiere al arquitecto (ARQ-31).

## 8. Pruebas
Dominio y casos de uso con dobles en memoria; adaptadores SES, DynamoDB, Athena, S3 con `Stubber`; handler con `capsys`; escenarios: mismo evento dos veces, estado ya `SENT`, dos aprobadores a la vez, token vencido o reusado, totales vacíos, destinatario fuera de lista, error de SES recuperable y no recuperable. Integración opcional en LocalStack (`[[entorno-local-localstack]]`, solo SES v1).
