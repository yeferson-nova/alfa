---
tipo: referencia
estado: vigente
actualizado: 2026-11-12
---
# Mapa de repos de Alfa

> Una línea por repo, con enlace a su ficha. El código vive en el repo `alfa-repos` (ver [[repo-alfa-repos]]); la plantilla de ficha es [[plantilla-ficha-repo]]. Flujo general en [[arquitectura-niif17]].

Estado: **vigente** = existe en Alfa; **nuevo** = repo que todavía no existe en Alfa (solo en una rama feature de `alfa-repos`).

## Jobs de Glue (medallón)

| Repo | Paso / capa | Malla | Estado | Qué hace |
|---|---|---|---|---|
| [[data-datalake-emisiones-vida-glue-job]] | 2 · Silver (preparación) | transversal | vigente | Silver de PCR Vida (Vida Deudores): arma las emisiones de vida con módulos SQL de Spark sobre las tablas extraídas de ACSELE. Versión de prueba. |
| [[data-datalake-niif-auxilios-funerarios-glue-job]] | 2 · Validación técnica, Silver y Gold SCD | auxilios-funerarios | vigente | Silver de Auxilios Funerarios y, además, los dos jobs genéricos de toda la familia: validación técnica y Gold SCD. |
| [[data-datalake-niif-certification-glue-job]] | 3 · Certificación de cifras (genera archivos) | transversal | vigente | Job de Glue que genera los CSV de certificación de una malla y periodo con Athena y deja los registros del `certCode` en PENDING, lo que dispara la notificación. |
| [[data-datalake-niif-notification-certification-glue-job]] | 3 · Notificación por correo (Lambda Notificación → Email) | transversal | vigente | Job de Glue que arma y envía por SES el correo de certificación (resumen, totales, detalle y enlaces Certificar / No certificar). |
| [[data-datalake-niif-pcr-emisiones-glue-job]] | 2 · Bronce (extracción) | transversal | vigente | Extracción Oracle a Bronce de la familia PCR (Vida y emisiones, origen ACSELE): variante ampliada del extractor estándar con lectura relacional por dependencias. |
| [[data-datalake-niif-primas-arl-glue-job]] | 2 · Bronce (extracción) y Silver (preparación) | primas-arl | vigente | Dos jobs propios de Primas ARL: un Bronce específico (extracción simple de 8 tablas Oracle) y el Silver preparado. |
| [[data-datalake-niif-primas-endosos-glue-job]] | 2 · Silver (preparación) | primas-endosos | vigente | Job Silver de la malla Primas Endosos: arma la Silver preparada de endosos de rentas. Es la única capa propia de la malla en este repo. |
| [[data-datalake-niif-primas-previsional-glue-job]] | 2 · Bronce (gestor de archivos) y Gold SCD | transversal | vigente | Caso Primas Previsional: carga un Excel de cesión desde landing a Bronce con validaciones y luego versiona en Gold con SCD. No viene de Oracle. |
| [[data-datalake-niif-primas-rentas-glue-job]] | 2 · Silver (preparación) | primas-rentas | vigente | Job Silver de la malla Primas Rentas: arma la Silver preparada a partir de la Silver base. Es la única capa propia de la malla en este repo. |
| [[data-datalake-niif-reserva-matematica-arp-glue-job]] | 2 · Silver (preparación) | reserva-matematica-arp | vigente | Job Silver de Reserva Matemática ARP: prepara pensionados y beneficiarios con la lógica actuarial de la reserva, por periodo de corte. |
| [[data-datalake-niif-reserva-matematica-rvi-glue-job]] | 2 · Silver (preparación) | reserva-matematica-rvi | vigente | Job Silver de Reserva Matemática RVI (rentas vitalicias): prepara pensiones, movimientos de pago y beneficiarios por periodo de corte. |
| [[data-datalake-niif-rmd-ext-estandar-glue-job]] | 2 · Bronce (extracción) y remediación | transversal | vigente | Jobs genéricos de extracción Oracle a Bronce y de remediación (disponibilización Bronce a Silver). Los usan las 6 mallas ALFAPRD con distinto JSON. |

## Lambdas

| Repo | Paso / capa | Malla | Estado | Qué hace |
|---|---|---|---|---|
| [[sc-datalake-niif-certification]] | 3 · Certificación Cifras (enlace Certificar / No certificar) | transversal | vigente | Lambda del API de aprobación: recibe `POST /certify` y `POST /not_certify`, valida al aprobador y registra la decisión (CERTIFICATE o NO_CERTIFICATE) en la tabla de notificaciones. |
| [[sc-datalake-niif-composite_notification_evaluator]] | 3 · Certificación Cifras (compuerta antes de la certificación) | transversal | vigente | Lambda que decide si un caso se notifica solo o espera a su grupo (Rentas + Endosos, ARP + RVI) y, cuando el grupo está completo, deja que se lance un solo job de certificación. |
| [[sc-datalake-niif-conf-auxilios-funerarios]] | 2 · Bodega NIIF 17 (configura y arranca la malla) | auxilios-funerarios | vigente | Lambda configuradora de la malla Auxilios Funerarios: reconstruye los JSON de configuración desde DynamoDB, los valida contra las plantillas de S3, los publica y puede iniciar la Step Function. |
| [[sc-datalake-niif-conf-rsm-arp]] | 2 · Bodega NIIF 17 (configura y arranca la malla) | reserva-matematica-arp | vigente | Lambda configuradora de la malla Reserva Matemática ARP: reconstruye los JSON de configuración desde DynamoDB, los valida contra las plantillas de S3, los publica y puede iniciar la Step Function. |
| [[sc-datalake-niif-conf-rsm-rvi]] | 2 · Bodega NIIF 17 (configura y arranca la malla) | reserva-matematica-rvi | vigente | Lambda configuradora de la malla Reserva Matemática RVI: reconstruye los JSON de configuración desde DynamoDB, los valida contra las plantillas de S3, los publica y puede iniciar la Step Function. |
| [[sc-datalake-niif-configurador-mallas]] | 2 · Bodega NIIF 17 (configura y arranca la malla) | transversal | nuevo | Reescritura hexagonal (tarea T2) de una sola lambda que configura las mallas desde un catálogo JSON y reemplazaría a las `configuration-<malla>`. En Alfa el repo solo tiene un README. |
| [[sc-datalake-niif-configuration-general]] | 2 · Bodega NIIF 17 (configura y arranca la malla) | transversal | vigente | Lambda "configuradora general": recibe el código de una malla, lee su periodo de la programación y delega en el módulo de esa malla, que reconstruye, valida y publica la configuración y puede iniciar la Step Function. |
| [[sc-datalake-niif-copy-s3-certification]] | 3 · Certificación Cifras (hacia la Zona Transporte) | transversal | vigente | Lambda que, cuando un `certCode` pasa a CERTIFICATE, copia sus CSV al bucket de transporte y marca los registros como TRANSPORT. |
| [[sc-datalake-niif-get-files-sftp]] | 4-5 · Herramientas actuariales (retorno desde Prophet/FIS), por confirmar | transversal | vigente | Lambda en imagen de contenedor que baja por SFTP los archivos de una carpeta remota y los copia a un bucket de S3 conservando las subcarpetas. |
| [[sc-datalake-niif-mov-s3-certification]] | 3 · Certificación Cifras (archivo de no certificados) | transversal | vigente | Lambda que, cuando un `certCode` pasa a NO_CERTIFICATE, mueve sus CSV a la subcarpeta `NO-CERTIFICADOS` y marca los registros como NO_TRANSPORT. |
| [[sc-datalake-niif-notification-certification-orchestrator]] | 3 · Certificación Cifras (orquestación de la cadena de notificación) | transversal | nuevo | Lambda orquestadora que reemplaza a trigger, copy y move: según el cambio de estado en la tabla de notificaciones lanza el job de notificación, copia a transporte o archiva los no certificados; más un reprocesador de PENDING atascados. |
| [[sc-datalake-niif-notification-certification]] | 3 · Certificación Cifras (Lambda Notificación, trigger) | transversal | vigente | Lambda trigger: al ver un registro PENDING en la tabla de notificaciones lanza el job de Glue de notificación (el que envía el correo). |
| [[sc-datalake-niif-post-login]] | 3 · Certificación Cifras (retorno de Cognito) | transversal | nuevo | Lambda de retorno del login (`GET /post_login`): recibe al usuario autenticado por Cognito con Azure AD y llama a certificar o no certificar con su token. Hoy solo existe el esqueleto. |
| [[sc-datalake-niif-primas-arl]] | 2 · Bodega NIIF 17 (configura y arranca la malla) | primas-arl | vigente | Lambda configuradora de la malla Primas ARL: reconstruye los JSON de configuración desde DynamoDB, los valida contra las plantillas de S3, los publica y puede iniciar la Step Function. |
| [[sc-datalake-niif-primas-endosos]] | 2 · Bodega NIIF 17 (configura y arranca la malla) | primas-endosos | vigente | Lambda configuradora de la malla Primas Endosos: reconstruye los JSON de configuración desde DynamoDB, los valida contra las plantillas de S3, los publica y puede iniciar la Step Function. |
| [[sc-datalake-niif-primas-rentas]] | 2 · Bodega NIIF 17 (configura y arranca la malla) | primas-rentas | vigente | Lambda configuradora de la malla Primas Rentas: reconstruye los JSON de configuración desde DynamoDB, los valida contra las plantillas de S3, los publica y puede iniciar la Step Function. |
| [[sc-datalake-niif-send-via-sftp-lambda]] | 3 · Certificación Cifras (Lambda Transporte, hacia FIS) | transversal | vigente | Lambda en imagen de contenedor que sube por SFTP al servidor de FIS cada archivo nuevo del bucket de transporte, respetando la carpeta del caso. |
| [[sc-datalake-niif-short-link-redirect]] | 3 · Certificación Cifras (enlaces del correo) | transversal | vigente | Lambda detrás de `GET /r` que resuelve el token corto del correo y responde un 302 hacia la URL guardada (login de Cognito para Certificar / No certificar). |

## Contenido de buckets S3 (config y plantillas)

| Repo | Paso / capa | Malla | Estado | Qué hace |
|---|---|---|---|---|
| [[cfg-datalake-niif-config-s3]] | 2 y 3 · Configuración de mallas, certificación y librerías de Glue | transversal | vigente | Repo de solo archivos: toda la configuración JSON de las mallas, los JSON de certificación y notificación, y las librerías de Glue. Jenkins lo sincroniza al bucket de assets. |
| [[cfg-datalake-niif-params-s3-landing]] | 2 · Landing de archivos de carga (Previsional) | transversal | vigente | Esqueleto de carpetas del bucket landing para las cargas por archivo: crea las rutas IN, OUT, ERROR y METADATA de cada caso. |
| [[tpl-datalake-niif-email-template-s3]] | 3 · Plantillas del correo de certificación | transversal | vigente | Plantillas HTML del correo de certificación (diseño nuevo, solo tablas y estilos en línea). Jenkins las despliega a la carpeta `email-template/` del bucket de assets. |

## Infraestructura (Terraform)

| Repo | Paso / capa | Malla | Estado | Qué hace |
|---|---|---|---|---|
| [[iac-analytics-datalake-niif-operations-manager]] | 2 y 3 · Bodega NIIF 17 y Certificación Cifras (gestor operativo) | transversal | vigente | Terraform con Terragrunt del "gestor operativo" NIIF 17: las lambdas configuradoras y su programador, las Step Functions de las mallas, los jobs de Glue, las tablas de configuración y los recursos de notificación y certificación. |
| [[iac-analytics-datalake-niif]] | 2 y 3 · Bodega NIIF 17 y Certificación Cifras (infraestructura base) | transversal | vigente | Terraform con Terragrunt que declara la infraestructura base del datalake NIIF 17: almacenamiento, tablas, secretos y Cognito, catálogo y jobs de Glue, API Gateway con WAF y las lambdas SFTP. |
