-- Conciliación de la fuente ACSELE (Oracle) para Emisiones, periodo 202606.
--
-- Objetivo: saber si la Oracle de QA y la de PRD tienen los mismos datos de junio de 2026,
-- SIN pasar por los jobs de Glue. Correr exactamente igual en las dos bases (solo lectura)
-- y exportar cada resultado a CSV con el nombre indicado en cada bloque:
--     oracle_<amb>_Q1.csv ... oracle_<amb>_Q6.csv
--
-- Ventana: mismas reglas que el configurador de Emisiones (desde el 1 del mes, hasta el 1 del
-- mes siguiente sin incluirlo, STATUS = 2). Si el PRG de algún ambiente usa otra ventana o
-- otro status (ver configuracion/prg_programacion_emisiones.json del diagnóstico), ajustar
-- aquí las dos fechas y el status y anotarlo.
--
-- No devuelve datos de personas: solo conteos y sumas por producto, concepto y estado.

-- Q1. Frescura de la base: hasta qué fecha llegan las operaciones.
--     Si QA es una copia de PRD, aquí se ve la fecha de la copia.
SELECT MAX(C.TIME_STAMP) AS ULTIMA_OPERACION,
       COUNT(*)          AS OPERACIONES_TOTALES
FROM ACSELE_ALFA.CONTEXTOPERATION C;

-- Q2. Operaciones de la ventana por estado.
SELECT C.STATUS, COUNT(*) AS OPERACIONES
FROM ACSELE_ALFA.CONTEXTOPERATION C
WHERE C.TIME_STAMP >= TO_DATE('2026-06-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
  AND C.TIME_STAMP <  TO_DATE('2026-07-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
GROUP BY C.STATUS
ORDER BY C.STATUS;

-- Q3. Compañía registrada por producto (los Silver filtran por el texto exacto de la compañía).
SELECT PP.STATIC AS PRODUCTID, PP.COMPANIAINPUT, PP.COMPANIAVALUE, COUNT(*) AS FILAS
FROM ACSELE_ALFA.PREPRODUCT PP
WHERE PP.STATIC IN (48897, 52897, 54899, 54919, 54939, 54959, 54979, 54999,
                    55039, 55059, 55060, 55099, 55119, 55139)
GROUP BY PP.STATIC, PP.COMPANIAINPUT, PP.COMPANIAVALUE
ORDER BY 1, 2;

-- Q4. Operaciones y pólizas emitidas en la ventana, por producto.
SELECT AP.PRODUCTID,
       COUNT(DISTINCT C.ID)                 AS OPERACIONES,
       COUNT(DISTINCT AP.AGREGATEDPOLICYID) AS POLIZAS
FROM ACSELE_ALFA.CONTEXTOPERATION C
JOIN ACSELE_ALFA.POLICYDCO P        ON P.OPERATIONPK = C.ID
JOIN ACSELE_ALFA.AGREGATEDPOLICY AP ON AP.AGREGATEDPOLICYID = P.AGREGATEDOBJECTID
WHERE C.STATUS = 2
  AND C.TIME_STAMP >= TO_DATE('2026-06-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
  AND C.TIME_STAMP <  TO_DATE('2026-07-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
  AND AP.PRODUCTID IN (48897, 52897, 54899, 54919, 54939, 54959, 54979, 54999,
                       55039, 55059, 55060, 55099, 55119, 55139)
GROUP BY AP.PRODUCTID
ORDER BY AP.PRODUCTID;

-- Q5. Movimientos (de donde salen prima, comisiones, costo de recaudo e IVA) por producto y concepto.
--     Misma llave que usan los Silver: OPERATIONPK de la operación y POLICYID de la póliza.
SELECT TO_CHAR(AP.PRODUCTID) AS PRODUCTID,
       UPPER(T.CONCEPT)      AS CONCEPTO,
       COUNT(*)              AS FILAS,
       SUM(T.AMMOUNT)        AS TOTAL
FROM ACSELE_ALFA.THIRDPARTYMOVEMENTPOLICY T
JOIN ACSELE_ALFA.CONTEXTOPERATION C ON C.ID = T.OPERATIONPK
JOIN (SELECT DISTINCT AGREGATEDPOLICYID, PRODUCTID FROM ACSELE_ALFA.AGREGATEDPOLICY) AP
     ON AP.AGREGATEDPOLICYID = T.POLICYID
WHERE C.STATUS = 2
  AND C.TIME_STAMP >= TO_DATE('2026-06-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
  AND C.TIME_STAMP <  TO_DATE('2026-07-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
  AND T.AMMOUNT <> 0
  AND AP.PRODUCTID IN (48897, 52897, 54899, 54919, 54939, 54959, 54979, 54999,
                       55039, 55059, 55060, 55099, 55119, 55139)
GROUP BY AP.PRODUCTID, UPPER(T.CONCEPT)
ORDER BY 1, 2;

-- Q6. Movimientos de operaciones de junio por día de la operación (detecta cargas tardías o
--     días faltantes en una de las dos bases).
SELECT TO_CHAR(C.TIME_STAMP, 'YYYY-MM-DD') AS DIA,
       COUNT(*)                           AS MOVIMIENTOS,
       SUM(T.AMMOUNT)                     AS TOTAL
FROM ACSELE_ALFA.THIRDPARTYMOVEMENTPOLICY T
JOIN ACSELE_ALFA.CONTEXTOPERATION C ON C.ID = T.OPERATIONPK
WHERE C.STATUS = 2
  AND C.TIME_STAMP >= TO_DATE('2026-06-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
  AND C.TIME_STAMP <  TO_DATE('2026-07-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
  AND T.AMMOUNT <> 0
GROUP BY TO_CHAR(C.TIME_STAMP, 'YYYY-MM-DD')
ORDER BY 1;
