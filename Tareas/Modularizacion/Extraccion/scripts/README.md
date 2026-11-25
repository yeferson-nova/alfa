# Scripts de la extracción

Una carpeta por flujo, en el orden en que se atacan. Todos son de **solo lectura**: no ejecutan jobs ni cambian datos. Lo que generan es evidencia y se guarda en `EvidenciasAws\dev-extraccion\`, que está fuera de Git.

## 01-pcr-emisiones (primero)
Proceso de extracción de Emisiones (Acsel-e): PCR Vida, Generales y Satélites.

| Script | Dónde corre | Qué hace |
|---|---|---|
| `medir-tiempos-dev.sh` | CloudShell de DEV (`bash medir-tiempos-dev.sh`) | Lee las corridas que ya existen: el historial del job y de la Step Function, el último `parametros_extraccion.json` de cada caso y los tiempos y filas por tabla de los últimos 5 lotes (de los checkpoints de Bronce) |
| `medir-uso-glue-dev.sh` | CloudShell de DEV (`bash medir-uso-glue-dev.sh 3`) | De las últimas corridas exitosas: ejecutores y CPU en CloudWatch, y etapas de Spark (cuántas tareas y cuánto tardan) leídas de los event logs que ya existen |
| `consultas-oracle-acsele.sql` | Oracle Acsel-e (servicio ACSSOP). Las consultas 1 y 2 con cualquier usuario que vea las tablas; los permisos (3d) los corre el DBA | Consultas de diccionario: tipos e índices de las llaves y estadísticas de tamaño |

## 02-alfaprd (después)
Proceso de extracción estándar: las 6 mallas de ALFAPRD.

| Script | Dónde corre | Qué hace |
|---|---|---|
| `consultas-oracle-alfaprd.sql` | Oracle ALFAPRD. Las consultas 1 y 2 con cualquier usuario que vea las tablas; los permisos (3d) los corre el DBA | Consultas de diccionario de las 40 tablas de los 6 JSON |
