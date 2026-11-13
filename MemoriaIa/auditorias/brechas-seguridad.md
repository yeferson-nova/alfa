---
tipo: auditoria
estado: vigente
actualizado: 2026-11-03
---
# Brechas de seguridad: vulnerabilidades y mejoras (pasos 2 y 3)

Versión para humanos: `Docs\06-brechas\seguridad.md` (con tablas completas). La numeración pasará a IDs `SEG-xx` en la matriz de brechas (fase 3). Esta nota es el resumen para IA.

Basado en diagrama, [[tareas-aws]] y las plantillas de `Backend\reservas_matematicas_cert\`. **El código de las lambdas no se ha revisado**: 8, 9 y 14 son hipótesis.

## Alta
1. Enlaces Certificar / No certificar sin protección confirmada: token firmado, de un solo uso, con expiración.
2. Acción por GET: escáneres de correo pueden certificar; usar página de confirmación + POST.
3. Sin autenticación ni trazabilidad: SSO, registrar usuario, fecha e IP.
5. URL prefirmada de S3 para el CSV: expiración corta, bucket privado, KMS.
7. SES sin SPF / DKIM / DMARC: verificar dominio.
10. Sin alertas de fallo: tarea 1c.
11. Sin validación de consistencia: tarea 3.

## Media
4. Decisión doble: estado atómico. 6. Inyección en HTML: escapar o plantillas SES. 8. IAM amplio. 9. Secretos en código. 14. Modelos de programación mezclados (tarea 2).

## Baja
5b. Enlace Redshift comentado. 12. Totales por correo. 13. Cinco plantillas casi idénticas: unificar.

Relacionado: [[correo-notificacion]], [[diseno-propuesto]], [[pendientes]].
