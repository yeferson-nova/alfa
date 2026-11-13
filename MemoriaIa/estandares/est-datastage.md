---
tipo: estandar
estado: vigente
actualizado: 2026-11-03
---
# Estándar de desarrollo DataStage (v6, jul-2025)

Fuente: `Docs\02-estandares-banco\desarrollo\Estandar de desarrollo DataStage v6.pdf` (25 págs.). Aplica al Subledger contable (paso 9 de [[arquitectura-niif17]]), **fuera de nuestro alcance directo**. Se resume porque varias reglas son de aplicación general en Analítica.

## Reglas específicas de DataStage (solo referencia)
- Jobs `JOB_<EXT|DUP|CDC|CLE|TRF|LOD>_<destino>_[nn]`; secuencias `SEQ_`; datasets `DS_<proceso>_<nombre>.ds`; prefijos por stage (`LKP_`, `JN_`, `TRSF_`, `LNK_`...).
- Directorios `PRY_<PROYECTO>[_PRU|_DES]` con 01 Extracciones ... 05 Secuencias.
- Variables `PRM_PATH_*`, `PRM_DB_DNS|USR|PWD_*`, `PRM_MAIL_TO|FROM|SERVER`; locales `VAL_`.
- SAP BO: `CNX_`, `CIF_`, `UNV_`, `RP_`, `DSB_`.

## Reglas generalizables (aplican por analogía a AWS)
- **Arquitectura (8.4):** todo desarrollo requiere documento de diseño aprobado por Arquitectura; **fuentes que no sean tabla (archivos planos, Excel) deben ser autorizadas por el Comité de Revisión de Arquitectura**.
- **Parametrización (8.6, 9.3):** nada "quemado"; conexiones y rutas por variables de ambiente; homologaciones en tabla (`TBL_HOMOLOGACION`); nulos con valores por defecto (`TBL_NULIDAD`: texto "-", número 0, fecha 01/01/1900, código -1).
- **Reprocesos (8.3):** parámetros fecha inicio/fin; borrar el rango antes de recargar (idempotencia).
- **Depuración (8.7):** limpiar temporales aunque la ejecución falle.
- **Warnings (8.8):** ejecución sin warnings; abortar tras 30.
- **Validación de archivos (8.9):** existencia, estructura (columnas, separador, formato, extensión) y no vacío; si no cumple, **notificar por correo sin abortar**.
- **Notificación (8.10):** correo ante fallas.
- **Auditoría (9.2):** esquema de auditoría obligatorio con control de ejecuciones y de rechazo.
- Contraseñas según políticas de Seguros Alfa.

## Relevancia
- Tarea 3: 8.9 da los escenarios mínimos de validación (archivo ausente, vacío, estructura inválida), ver [[diseno-propuesto]] (1d).
- Tarea 1c: 8.10 y 9.2 respaldan notificar fallas y registrar ejecuciones.
