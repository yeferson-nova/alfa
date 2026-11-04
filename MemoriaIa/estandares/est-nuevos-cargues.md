---
tipo: estandar
actualizado: 2026-11-03
---
# Manual de usuario Proyecto Nuevos Cargues (V5, oct-2024)

Fuente: `Docs\04-manuales\Proyecto Nuevos Cargues ManualUsuario V5..pdf` (46 págs., confidencial). Manual de operación, no estándar. Útil como **precedente de un flujo AWS con aprobación por correo**, muy parecido a nuestro paso 3.

## Qué describe
- Clientes (bancos y gaseras) envían archivos de acuerdos/pólizas. Operaciones los sube a mano a S3 `dataprep-stg-agreementbroker-s3/Agreements/<entidad>/`.
- Se ejecuta la Step Function `data-process-sf`: recetas DataBrew ([[est-databrew]]) → archivo de salida → transporte **MFT** → Dataproc → Acsel-e (cuentas de cobro, reaseguros).
- **Aprobación con cifras de control:** llega un correo a Operaciones con los totales calculados frente a los enviados por la entidad, con botones **Aprobar / Negar**. Desde la v4 hay un frontend en S3 (`dataprep-stg-approval-frontend`) con la pantalla "Cifras de Control" y un correo informativo sin botones. Al aprobar, el control pasa a Dataproc.
- Acceso: usuario en Directorio Activo + MFA, IAM Identity Center, región us-east-1.
- Recomendaciones operativas: carpeta vacía antes de cargar; Excel solo primera hoja; encabezados exactos sin espacios; valores asegurados no negativos; extensiones en minúscula; sin caracteres especiales en dirección.

## Relevancia para nuestras tareas
- El patrón **correo con cifras + Certificar/No certificar** del paso 3 ([[correo-notificacion]]) ya existe en la compañía. Revisar cómo resolvieron el endpoint de aprobación (frontend S3) antes de diseñar el nuestro ([[diseno-propuesto]]). Por confirmar si hay código reutilizable.
- Las recomendaciones (valores no negativos, encabezados, archivo vacío) son candidatas a reglas del validador de la tarea 3.

## Brechas candidatas (propuesta)
- **SEG/ARQ:** carga manual por consola y borrado manual de archivos fallidos (sin trazabilidad ni control de cambios).
- **SEG:** el manual no documenta cómo se protege la acción Aprobar/Negar (token, autenticación).
