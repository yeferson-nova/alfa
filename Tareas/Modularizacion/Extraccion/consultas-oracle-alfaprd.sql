-- =====================================================================
-- Consultas de diccionario para Julio (Oracle): ALFAPRD (esquemas ARPPROD, GENERAL y PRV).
-- Extracción NIIF 17, opción D (lectura por rangos de llave). Generado el 2026-11-20.
-- Origen de la lista de tablas: los 6 config_extraccion_*.json de cfg-datalake-niif-config-s3 (alfa-repos main 3d5f623): 40 tablas, 53 columnas de llave.
--
-- SOLO LECTURA. Todo consulta el diccionario (ALL_*): no lee datos de negocio.
-- La única excepción es la prueba 3c, comentada, opcional y de una sola fila.
--
-- IMPORTANTE:
--  1. Correr con el USUARIO DE SERVICIO de la conexión de Glue. Las vistas ALL_* y los
--     privilegios dependen del usuario. Si se corren con otro usuario, usar el bloque 3d (DBA).
--  2. Base: la de ALFAPRD (en DEV, el servicio ALPRODAC). Anotar en cuál corrió (consulta 0).
--  3. Exportar cada resultado a CSV con el número de la consulta en el nombre (ej. 1.csv).
-- =====================================================================

-- 0. Dónde y con qué usuario se corre
SELECT sys_context('USERENV', 'DB_NAME')      AS base,
       sys_context('USERENV', 'SERVICE_NAME') AS servicio,
       sys_context('USERENV', 'SESSION_USER') AS usuario
  FROM dual;

-- 1. Tipo de dato e índices de cada columna de llave (define rangos o IN).
--    Para rangos: la llave debe ser numérica o fecha, idealmente primera columna (pos=1) de un índice.
WITH llaves (owner, table_name, column_name) AS (
            SELECT 'ARPPROD', 'GAR_TIPOS_ENDOSOS_ARP', 'COD_TIPO_ENDOSO_ARP' FROM dual
  UNION ALL SELECT 'ARPPROD', 'PAR_AFILIACIONES', 'COD_TIPO_IDENTIFICACION' FROM dual
  UNION ALL SELECT 'ARPPROD', 'PAR_AFILIACIONES', 'NID_AFILIACION' FROM dual
  UNION ALL SELECT 'ARPPROD', 'PAR_AFILIACIONES', 'NID_NUMERO_IDENTIFICACION' FROM dual
  UNION ALL SELECT 'ARPPROD', 'PAR_ENDOSOS', 'COD_SUCURSAL_ALFA' FROM dual
  UNION ALL SELECT 'ARPPROD', 'PAR_ENDOSOS', 'COD_TIPO_ENDOSO' FROM dual
  UNION ALL SELECT 'ARPPROD', 'PAR_ENDOSOS', 'COD_TIPO_ENDOSO_ARP' FROM dual
  UNION ALL SELECT 'ARPPROD', 'PAR_ENDOSOS', 'MODULO' FROM dual
  UNION ALL SELECT 'ARPPROD', 'PAR_ENDOSOS', 'NID_ENDOSO' FROM dual
  UNION ALL SELECT 'ARPPROD', 'PAR_ENDOSOS', 'NID_POLIZA' FROM dual
  UNION ALL SELECT 'ARPPROD', 'PAR_POLIZAS', 'COD_SUCURSAL_ALFA' FROM dual
  UNION ALL SELECT 'ARPPROD', 'PAR_POLIZAS', 'MODULO' FROM dual
  UNION ALL SELECT 'ARPPROD', 'PAR_POLIZAS', 'NID_POLIZA' FROM dual
  UNION ALL SELECT 'ARPPROD', 'SAR_DATOS_INVALIDEZ_MUERTE', 'NID_AUDITORIA_MEDICA' FROM dual
  UNION ALL SELECT 'ARPPROD', 'SAR_DATOS_INVALIDEZ_MUERTE', 'NID_SINIESTRO' FROM dual
  UNION ALL SELECT 'GENERAL', 'ACU_PERSONAS_JURIDICAS', 'COD_TIPO_IDENTIFICACION' FROM dual
  UNION ALL SELECT 'GENERAL', 'ACU_PERSONAS_JURIDICAS', 'NID_CONSECUTIVO' FROM dual
  UNION ALL SELECT 'GENERAL', 'ACU_PERSONAS_JURIDICAS', 'NID_NUMERO_IDENTIFICACION' FROM dual
  UNION ALL SELECT 'GENERAL', 'ACU_PERSONAS_NATURALES', 'COD_TIPO_IDENTIFICACION' FROM dual
  UNION ALL SELECT 'GENERAL', 'ACU_PERSONAS_NATURALES', 'NID_NUMERO_IDENTIFICACION' FROM dual
  UNION ALL SELECT 'GENERAL', 'ANP_DETALLE_SOLICITUDES_GIRO', 'NID_DETALLE_SOLICITUD_GIRO' FROM dual
  UNION ALL SELECT 'GENERAL', 'ANP_DETALLE_SOLICITUDES_GIRO', 'NID_PERIODO_SOLICITUD_GIRO' FROM dual
  UNION ALL SELECT 'GENERAL', 'ANP_DISTRIBUCION_VIG_PENSION', 'COD_TIPO_IDENTIFICACION' FROM dual
  UNION ALL SELECT 'GENERAL', 'ANP_DISTRIBUCION_VIG_PENSION', 'NID_DISTRIBUCION_VIGENCIA' FROM dual
  UNION ALL SELECT 'GENERAL', 'ANP_DISTRIBUCION_VIG_PENSION', 'NID_NUMERO_IDENTIFICACION' FROM dual
  UNION ALL SELECT 'GENERAL', 'ANP_DISTRIBUCION_VIG_PENSION', 'NID_PENSION' FROM dual
  UNION ALL SELECT 'GENERAL', 'ANP_MOVIMIENTOS_PAGO', 'COD_CONCEPTO' FROM dual
  UNION ALL SELECT 'GENERAL', 'ANP_MOVIMIENTOS_PAGO', 'COD_TIPO_IDENTIFICACION' FROM dual
  UNION ALL SELECT 'GENERAL', 'ANP_MOVIMIENTOS_PAGO', 'NID_DISTRIBUCION_VIGENCIA' FROM dual
  UNION ALL SELECT 'GENERAL', 'ANP_MOVIMIENTOS_PAGO', 'NID_NUMERO_IDENTIFICACION' FROM dual
  UNION ALL SELECT 'GENERAL', 'ANP_MOVIMIENTOS_PAGO', 'PERIODO_PAGO' FROM dual
  UNION ALL SELECT 'GENERAL', 'ANP_PENSIONADO_NOVEDAD', 'COD_CONCEPTO' FROM dual
  UNION ALL SELECT 'GENERAL', 'ANP_PENSIONADO_NOVEDAD', 'COD_TIPO_IDENTIFICACION' FROM dual
  UNION ALL SELECT 'GENERAL', 'ANP_PENSIONADO_NOVEDAD', 'FEC_INICIAL' FROM dual
  UNION ALL SELECT 'GENERAL', 'ANP_PENSIONADO_NOVEDAD', 'NID_DISTRIBUCION_VIGENCIA' FROM dual
  UNION ALL SELECT 'GENERAL', 'ANP_PENSIONADO_NOVEDAD', 'NID_NUMERO_IDENTIFICACION' FROM dual
  UNION ALL SELECT 'GENERAL', 'ANP_PENSIONES', 'NID_PENSION' FROM dual
  UNION ALL SELECT 'GENERAL', 'ANP_PENSIONES', 'NID_PROCEDENCIA' FROM dual
  UNION ALL SELECT 'GENERAL', 'ANP_PERIODO_SOLICITUDES_GIRO', 'NID_PERIODO_SOLICITUD_GIRO' FROM dual
  UNION ALL SELECT 'GENERAL', 'CGR_CONCEPTOS', 'COD_CONCEPTO' FROM dual
  UNION ALL SELECT 'GENERAL', 'CGR_ORIGENES', 'COD_ORIGEN' FROM dual
  UNION ALL SELECT 'GENERAL', 'CGR_SUCURSALES_ALFA', 'COD_SUCURSAL_ALFA' FROM dual
  UNION ALL SELECT 'GENERAL', 'CGR_TIPOS_ENDOSOS', 'COD_TIPO_ENDOSO' FROM dual
  UNION ALL SELECT 'PRV', 'PRV_ENDOSOS_POINT', 'COD_TIPO_ENDOSO' FROM dual
  UNION ALL SELECT 'PRV', 'PRV_ENDOSOS_POINT', 'MODULO' FROM dual
  UNION ALL SELECT 'PRV', 'PRV_ENDOSOS_POINT', 'NID_ENDOSO_POINT' FROM dual
  UNION ALL SELECT 'PRV', 'PRV_ENDOSOS_POINT', 'NID_POLIZA_POINT' FROM dual
  UNION ALL SELECT 'PRV', 'PRV_ENDOSOS_POINT', 'NID_RENTAS' FROM dual
  UNION ALL SELECT 'PRV', 'PRV_RENTAS', 'COD_ORIGEN' FROM dual
  UNION ALL SELECT 'PRV', 'PRV_RENTAS', 'NID_PROCEDENCIA' FROM dual
  UNION ALL SELECT 'PRV', 'PRV_RENTAS', 'NID_RENTAS' FROM dual
  UNION ALL SELECT 'PRV', 'PRV_SALDOS_MATEMATICA', 'NID_RENTAS' FROM dual
  UNION ALL SELECT 'PRV', 'PRV_SALDOS_MATEMATICA', 'PERIODO' FROM dual
)
SELECT l.owner, l.table_name, l.column_name,
       c.data_type, c.data_length, c.data_precision, c.data_scale, c.nullable,
       (SELECT LISTAGG(ic.index_name || ' pos=' || ic.column_position
                       || CASE i.uniqueness WHEN 'UNIQUE' THEN ' UNICO' END, '; ')
                 WITHIN GROUP (ORDER BY ic.column_position, ic.index_name)
          FROM all_ind_columns ic
          JOIN all_indexes i ON i.owner = ic.index_owner AND i.index_name = ic.index_name
         WHERE ic.table_owner = l.owner
           AND ic.table_name  = l.table_name
           AND ic.column_name = l.column_name) AS indices,
       CASE WHEN c.column_name IS NULL THEN 'NO VISIBLE PARA ESTE USUARIO' END AS nota
  FROM llaves l
  LEFT JOIN all_tab_columns c
    ON c.owner = l.owner AND c.table_name = l.table_name AND c.column_name = l.column_name
 ORDER BY l.owner, l.table_name, l.column_name;

-- 2. Tamaño de cada tabla según las estadísticas (define cuáles son "chicas").
--    unidades_estimadas = num_rows x num_columnas (modelo de costo de Víctor).
WITH tablas (owner, table_name) AS (
            SELECT 'ARPPROD', 'GAR_TIPOS_ENDOSOS_ARP' FROM dual
  UNION ALL SELECT 'ARPPROD', 'HIST_DATOS_INVALIDEZ_MUERTE' FROM dual
  UNION ALL SELECT 'ARPPROD', 'HIST_SAR_PCL_PENSION' FROM dual
  UNION ALL SELECT 'ARPPROD', 'IND_DATOS_RESERVAS_SINIESTROS' FROM dual
  UNION ALL SELECT 'ARPPROD', 'PAR_AFILIACIONES' FROM dual
  UNION ALL SELECT 'ARPPROD', 'PAR_ENDOSOS' FROM dual
  UNION ALL SELECT 'ARPPROD', 'PAR_POLIZAS' FROM dual
  UNION ALL SELECT 'ARPPROD', 'PAR_TRABAJADORES' FROM dual
  UNION ALL SELECT 'ARPPROD', 'SAR_AUDITORIA_MEDICA' FROM dual
  UNION ALL SELECT 'ARPPROD', 'SAR_BENEFICIARIOS_SINIESTROS' FROM dual
  UNION ALL SELECT 'ARPPROD', 'SAR_CALCULOS_ACTUARIALES' FROM dual
  UNION ALL SELECT 'ARPPROD', 'SAR_DATOS_INVALIDEZ_MUERTE' FROM dual
  UNION ALL SELECT 'ARPPROD', 'SAR_RESERVA_PCJ_AMORTIZACION' FROM dual
  UNION ALL SELECT 'ARPPROD', 'SAR_SINIESTROS' FROM dual
  UNION ALL SELECT 'GENERAL', 'ACU_PERSONAS_JURIDICAS' FROM dual
  UNION ALL SELECT 'GENERAL', 'ACU_PERSONAS_NATURALES' FROM dual
  UNION ALL SELECT 'GENERAL', 'ANP_DETALLE_SOLICITUDES_GIRO' FROM dual
  UNION ALL SELECT 'GENERAL', 'ANP_DISTRIBUCION_VIG_PENSION' FROM dual
  UNION ALL SELECT 'GENERAL', 'ANP_IPC' FROM dual
  UNION ALL SELECT 'GENERAL', 'ANP_MOVIMIENTOS_PAGO' FROM dual
  UNION ALL SELECT 'GENERAL', 'ANP_PENSIONADO_NOVEDAD' FROM dual
  UNION ALL SELECT 'GENERAL', 'ANP_PENSIONES' FROM dual
  UNION ALL SELECT 'GENERAL', 'ANP_PERIODO_SOLICITUDES_GIRO' FROM dual
  UNION ALL SELECT 'GENERAL', 'CGR_CONCEPTOS' FROM dual
  UNION ALL SELECT 'GENERAL', 'CGR_EQUIVALENCIA_TIPOS_ID' FROM dual
  UNION ALL SELECT 'GENERAL', 'CGR_ESTADOS_PERSONAS' FROM dual
  UNION ALL SELECT 'GENERAL', 'CGR_ORIGENES' FROM dual
  UNION ALL SELECT 'GENERAL', 'CGR_PARENTESCOS' FROM dual
  UNION ALL SELECT 'GENERAL', 'CGR_SUCURSALES_ALFA' FROM dual
  UNION ALL SELECT 'GENERAL', 'CGR_TIPOS_ENDOSOS' FROM dual
  UNION ALL SELECT 'GENERAL', 'PGR_SALARIOS_MINIMOS' FROM dual
  UNION ALL SELECT 'GENERAL', 'RVA_RESERVA_MATEMATICA_TRANSIC' FROM dual
  UNION ALL SELECT 'PRV', 'CRV_CALCULOS_2' FROM dual
  UNION ALL SELECT 'PRV', 'PRV_BENEFICIARIOS' FROM dual
  UNION ALL SELECT 'PRV', 'PRV_ENDOSOS_POINT' FROM dual
  UNION ALL SELECT 'PRV', 'PRV_RENTAS' FROM dual
  UNION ALL SELECT 'PRV', 'PRV_RENTAS_APROBADAS_OBP' FROM dual
  UNION ALL SELECT 'PRV', 'PRV_RENTAS_NOTAS' FROM dual
  UNION ALL SELECT 'PRV', 'PRV_SALDOS_MATEMATICA' FROM dual
  UNION ALL SELECT 'PRV', 'SPR_SINIESTROS' FROM dual
)
SELECT x.owner, x.table_name,
       t.num_rows, t.blocks, t.avg_row_len, t.last_analyzed, t.partitioned,
       (SELECT COUNT(*) FROM all_tab_columns c
         WHERE c.owner = x.owner AND c.table_name = x.table_name) AS num_columnas,
       t.num_rows * (SELECT COUNT(*) FROM all_tab_columns c
                      WHERE c.owner = x.owner AND c.table_name = x.table_name) AS unidades_estimadas,
       CASE WHEN t.table_name IS NULL THEN 'NO VISIBLE PARA ESTE USUARIO' END AS nota
  FROM tablas x
  LEFT JOIN all_tables t ON t.owner = x.owner AND t.table_name = x.table_name
 ORDER BY unidades_estimadas DESC NULLS LAST;

-- =====================================================================
-- 3. Foto consistente entre tramos (AS OF SCN)
-- =====================================================================

-- 3a. Privilegios de flashback de la sesión (directos o por rol).
SELECT privilege FROM session_privs WHERE privilege LIKE '%FLASHBACK%';
SELECT owner, table_name, privilege FROM user_tab_privs WHERE privilege = 'FLASHBACK';
SELECT role, owner, table_name, privilege FROM role_tab_privs WHERE privilege = 'FLASHBACK';

-- 3b. Retención de UNDO. Si da ORA-00942 (sin acceso a V$PARAMETER), pedir el valor al DBA.
SELECT name, value FROM v$parameter WHERE name IN ('undo_management', 'undo_retention');

-- 3c. OPCIONAL, solo con aval de Julio: prueba mínima de lectura AS OF (una fila).
--     Si da ORA-01031, no hay permiso. Si da ORA-01555, el UNDO no alcanza para esa antigüedad.
-- SELECT TIMESTAMP_TO_SCN(SYSTIMESTAMP) AS scn_actual FROM dual;
-- SELECT 1 FROM ARPPROD.PAR_ENDOSOS AS OF TIMESTAMP (SYSTIMESTAMP - INTERVAL '60' MINUTE) WHERE ROWNUM = 1;

-- 3d. Variante DBA (si no se corre con el usuario de servicio).
--     Reemplazar :usuario_servicio por el usuario de la conexión de Glue.
-- SELECT grantee, privilege FROM dba_sys_privs WHERE grantee = :usuario_servicio AND privilege LIKE '%FLASHBACK%';
-- SELECT grantee, owner, table_name, privilege FROM dba_tab_privs WHERE grantee = :usuario_servicio AND privilege IN ('FLASHBACK', 'SELECT', 'READ') AND owner IN ('ARPPROD', 'GENERAL', 'PRV');
-- SELECT grantee, granted_role FROM dba_role_privs WHERE grantee = :usuario_servicio;
