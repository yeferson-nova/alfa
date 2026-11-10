# Origen del código

`copy_s3.py` es el código **desplegado en DEV** de la lambda `niif-dev-copy-s3-cert`, descargado de CloudShell el 2026-11-10 (última modificación en AWS: 2026-10-08).

- Se omitió `six.py` (librería empaquetada) y las demás dependencias; la lambda las incluye en su zip.
- Solo lectura para análisis: no se ha modificado. Variables de entorno (solo nombres) en `MemoriaIa`.
