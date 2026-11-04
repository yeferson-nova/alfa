---
tipo: brecha
actualizado: 2026-11-03
---
# Riesgos del paso de datos Glue → Lambda de Notificación (propuesta, en iteración)

Humano: `Tareas\riesgos-flujo-glue-notificacion.md`. Todo es **hipótesis** (sin código ni AWS). Afecta a T1, T2 y T3 ([[tareas-aws]]).

**Raíz:** no hay contrato entre lo que Glue produce, lo que se notifica, lo que se certifica y lo que se transporta.

## Altos
1. Se certifica una versión y se transporta otra: Glue sobrescribe el prefijo mientras el correo espera respuesta.
2. Totales calculados dos veces (Spark y pandas `float64`): redondeo y cifra no demostrable (CBCF SFC).
3. Disparo por evento de S3: correos múltiples, parciales o duplicados (entrega "al menos una vez").
4. Fallos silenciosos: Glue falla sin captura, o termina "OK" con 0 filas y se certifican ceros.
5. Inyección HTML: `$tableDetail` con enlaces, probablemente `to_html(escape=False)`.

## Medios
6. Límite de 256 KB de SNS y recorte de Gmail a 102 KB.
7. La Lambda lee el dataset completo (costo, timeout, escaneo en Athena).
8. Periodo deducido de la fecha (errata `$perido_a`).
9. Destinatarios sin lista permitida.
10. La Lambda se salta Lake Formation (A-01/A-03 de [[hu-seguridad-acceso]]).
11. Esquema implícito: un cambio de columna rompe el correo sin error.
12. Sin `ejecucion_id` de punta a punta.

## Mitigación propuesta
- **Manifiesto inmutable** por ejecución: ruta `ejecucion=<id>`, conteos, sumas DECIMAL como texto, checksum, esquema, versión y periodo de los datos.
- **Step Functions:** Glue `.sync` + Catch → SNS → ServiceNow.
- **Validación (T3)** añade su resultado al manifiesto.
- **Notificación** lee solo el manifiesto.
- **Espera `waitForTaskToken`:** el token del correo es el de la tarea.
- **Transporte** verifica el checksum.

Precedente: la aprobación por Step Functions de [[est-nuevos-cargues]]. Alineado con [[hu-trazabilidad]] y [[hu-gobierno-ingestas]].

Por validar: ¿hay orquestador?, ¿quién calcula los totales?, ¿ruta por ejecución o sobrescritura?, ¿contenido del CSV?, ¿`escape=False`? Relacionado: [[diseno-propuesto]], [[brechas-seguridad]], [[monitoreo-grafana]].
