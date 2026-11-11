# Origen del código

`lambda_function.py` es el código **desplegado en DEV** de la lambda `niif-post-login` (handler `lambda_function.lambda_handler`), descargado con `Backend\cloudshell-inventario\codigo-actual-dev.sh` el 2026-11-10 (última modificación en AWS: 2026-10-09).

- No incluye las librerías empaquetadas (lista en `EvidenciasAws\dev-codigo-actual\lambdas\niif-post-login\librerias.txt`). `configuracion-aws.json`: runtime, handler y **solo los nombres** de las variables de entorno.
- Solo lectura. El código nuevo de este componente va en `Backend\notificacion\niif-post-login\`.
