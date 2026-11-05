# Arquitectura y flujo de datos NIIF 17

## Fuentes de verdad (en orden)
1. `Docs\05-arquitectura\diagramas\NIIF17_ModeloSolucion_V1.0.drawio`: **arquitectura general objetivo**, casi completa y se sigue actualizando. Pesa unos 3 MB: no leerlo completo, extraer solo la hoja necesaria (las hojas pueden venir comprimidas en base64+deflate).
2. `Docs\05-arquitectura\flujo-integral-niif17.md`: cómo funciona **hoy** en DEV (AS-IS), de punta a punta.
3. `Docs\05-arquitectura\jobs-glue.md`: cada job por dentro. `tareas-sobre-el-flujo.md`: dónde actúa T1, T2 y T3.
4. Diagramas derivados propios: `diagramas\flujo-niif17-asis.drawio` y `recorrido-dato-niif17.drawio`.

Si el AS-IS y el diagrama general difieren, la diferencia es una **brecha o un trabajo pendiente**, no un error del diagrama. Registrarla.

## Mapa de hojas del modelo de solución (contenido observado 2026-11-05)
| Hoja | Qué muestra |
|---|---|
| Página-10 | Vista de negocio de punta a punta: fuentes (Acsel-E, Sipren, Arpis, Point, Sipren+, Conciso, Uniclass, previsional semiestructurada) → Bodega NIIF17 (extracción, calidad, preparación, consolidación) → Certificación de cifras (notificación, email, transporte, salida) → herramientas actuariales (FIS, Prophet), contable (Subledger, Accounting Tool), ERP y reportería |
| Página-1 | Red e infraestructura: datacenter, ambientes de las fuentes Oracle, conectividad hacia AWS, VPC y subred privada, zonas del lago (aterrizaje, bronce, plata, oro), jobs, rastreadores y catálogos por zona, modelo de configuración de extracción y de notificaciones. **Contiene IPs internas: nunca copiarlas a notas, skill ni repo** |
| Página-2 | Paso 3 en detalle: modelo de notificaciones, job y lambda de notificación, catálogo oro, Athena, zona de certificación, API Gateway + WAF + Cognito (identidad interna) + Entra ID / Active Directory (identidad externa), usuario Outlook, zona de transporte de salida, lambda SFTP y FIS por Transit Gateway |
| Página-3 | Modelo de programación: DynamoDB Stream (detector de cambios) → Lambda (automatización) → EventBridge Pipe (enrutador) → EventBridge (programación de ejecución) |
| Página-5 y su copia | Servicios transversales por capas (identidad, acceso, persistencia, procesamiento, orquestación) aplicados al modelo de programación |
| Página-6 | Capas de servicio del lago: extracción JDBC desde el core, zonas de aterrizaje, bronce, plata, oro y de recursos, orquestación de la malla |
| Página-9 | Capas de servicio del paso 3: notificación, certificación, integración HTTP, seguridad perimetral, identidad interna y externa, transporte SFTP a FIS Single Platform (Folder IN) |

Al cambiar el diagrama, actualizar esta tabla y la nota `[[arquitectura-niif17]]`.

## Pasos del modelo (nos competen 2 y 3)
1 Fuentes → **2 Bodega NIIF 17 (AWS)** → **3 Certificación de cifras (AWS)** → 4-7 herramientas actuariales (FIS, Prophet, Subledger actuarial, reportería) → 8 Subledger contable (DataStage) → 9 ERP (SAP).

## Medallón (estándar HU-GI) y cómo se mapea hoy
| Capa | Regla del estándar | Hoy en NIIF |
|---|---|---|
| Landing / Aterrizaje | Copia fiel de la fuente, evidencia 7 años | No se ve como capa separada en el AS-IS (brecha a confirmar) |
| Bronce | Delta + Snappy en S3, catálogo Glue, particionado por lote | Parquet por `lote_cargue_id` (`alfa-datalake-<amb>-bronce/niif/...`) |
| Plata | Datos validados y remediados; cuarentena junto a la capa | Silver base por lote + `silver_preparado_<malla>`; errores de validación en `remediacion/<malla>/` |
| Oro | Producto de datos consolidado | `scd_<malla>` (dimensión de cambio lento) |
| Platino / consumo | Athena o Redshift | Athena sobre Gold para certificar |

Evolución de esquema: columna nueva o reordenada continúa; columna eliminada, tipo incompatible o cambio de llave **detiene**.

## Flujo de una malla (AS-IS resumido)
1. **Programación:** fila en `prg-programaciones` → stream → manager → EventBridge Scheduler.
2. **Configuración:** lambda configuradora lee `cnf-mallas` y `prs-estructuras`, publica 4 JSON en `niif-<amb>-assets/<malla>/` y arranca la Step Function.
3. **Step Function `niif-<amb>-<malla>`:** extracción (Oracle → Bronce) → ¿hay datos nuevos? → crawler bronce → validación técnica → remediación → Silver de la malla → crawler silver → SCD Gold → crawler gold → evaluador de notificación (simple o compuesta) → job de certificación.
4. **Certificación:** Athena ejecuta `summary`, `total`, `detail` sobre Gold, genera CSV y `certCode`, escribe filas `PROCESSED → PENDING` en `notification-certification`.
5. **Notificación:** stream `PENDING` → trigger → job de notificación arma tablas, enlaces cortos y envía el correo (SNS hoy; SES en T1a).
6. **Aprobación:** enlace → API Gateway + Cognito → lambda de certificación (multiaprobador, auditoría) → `CERTIFICATE` / `NO_CERTIFICATE`.
7. **Transporte:** stream → copia al bucket de transporte → SFTP a FIS.

**Contrato entre jobs hoy:** un archivo de estado fijo por malla (`parametros_extraccion.json`) que cada job lee y reescribe. Riesgo conocido (FLU-02): dos ejecuciones se pisan. Dirección objetivo: **manifiesto inmutable por `ejecucion_id`** (ver `[[riesgos-glue-notificacion]]`).

Estados de certificación: `PROCESSED → PENDING → PENDING_APPROVERS → CERTIFICATE | NO_CERTIFICATE → TRANSPORT | NO_TRANSPORT`.

## Mallas
Primas Rentas y Primas Endosos (correo compartido), Primas ARL, Auxilios Funerarios (usa `fecDesde`/`fecHasta`), Reserva Matemática ARP y RVI (correo compartido; Silver de 440-458 columnas con lógica actuarial). Jobs genéricos: extracción, validación técnica, remediación, SCD, certificación, notificación. Un job Silver por malla.

## Lo que aún no se ve (no inventarlo)
Código del manager, de la cadena final (trigger, copy, move, SFTP), JSON de configuración en S3, una ejecución real con tiempos y costos. Si una respuesta depende de eso, decirlo y pedir la evidencia.
