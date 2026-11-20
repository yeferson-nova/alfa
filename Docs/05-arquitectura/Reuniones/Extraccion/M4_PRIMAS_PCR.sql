/* ========================================================================================== */
/* M4 PRIMAS PCR - MALLA 4 DEL PROCESO PCR  -  VERSION 1.5 - 2026-11-09                       */
/*                                                                                            */
/* Tres salidas de la Zona Oro: pcr_detalle (base PCR con la homologacion PCR Point -         */
/* Acsel-e, ORIGEN = Acsele; el 13Y queda por fuera), pcr_totales (resumen de primas y        */
/* comisiones de la historia de usuario) y pcr_comisiones (comisionistas de recaudo e         */
/* intermediacion en una sola salida).                                                        */
/* DEPENDENCIA: corre despues de las mallas 1, 2 y 3 y REUTILIZA su extraccion; casi no lee   */
/* Acsel-e:                                                                                   */
/*  - REUTILIZA de M3: REASEGUROS (Oro), partidas openitem con los tipos de documento de los  */
/* scripts de comisionistas (rs_11), sus valores de comision e IVA (rs_uaa), participantes de */
/* la poliza (rs_ppt), porcentajes del intermediario (rs_pin) y parametros (rs_cfg_1,         */
/* rs_cfg_4).                                                                                 */
/*  - REUTILIZA de M1: EMISIONES (Oro) para el tomador de la poliza matriz.                   */
/*  - EXTRAE NUEVO solo: stts_role (descripcion del rol del comisionista), participacion      */
/* (porcentaje del recaudador) y el DELTA de terceros que cobran comision y no son            */
/* participantes de la poliza (stte_thirdparty, prethirdparty, anti-join contra rs_ppt).      */
/* Reglas de negocio: PRIMAALFA <> 0 en el BRUTO (su DETALLE de reaseguradores se conserva);  */
/* UNIDAD_DE_CUENTA vacio; ramo = ramo contable; fechas DD/MM/AAAA; MESPRO AAAAMM;            */
/* TIPO_COASEGURO D / A / C; ID_COM_RECAUDO sin digito de verificacion.                       */
/* Reglas: pivote -> dependientes; inner antes de left; cero subqueries; sin KEEP / OVER /    */
/* DENSE_RANK; sin funciones PL/SQL; esquema en todas las tablas; lectura parallel(3) /       */
/* escritura parallel 2.                                                                      */
/* PRUEBA EN ORACLE - VERSION POR TABLAS (recomendada): paso a paso, con indices y conteo por */
/* tabla. Parametros (periodo, corte, companias, productos, numeros): se cambian SOLO en      */
/* 00_PARAMETROS/PCR_PARAMETROS_v1.5.sql. Conceptos: salen de stst_systemproperty (malla 3).  */
/* ========================================================================================== */


/* ========================================================================================== */
/* PARAMETRO - Ramos contables (hoja RAMOSCONTABLES de la homologacion PCR)                   */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.pcr_ramos;
commit;
*/

create table acsele_alfa_bi.pcr_ramos
tablespace tsdat_acsele_alfa_rep
as
select 1 as ramo, 'AUTOS' as desc_ramo from sys.dual
union all
select 3 as ramo, 'EQUIPO ELECTRONICO' as desc_ramo from sys.dual
union all
select 4 as ramo, 'CUMPLIMIENTO' as desc_ramo from sys.dual
union all
select 5 as ramo, 'INCENDIO' as desc_ramo from sys.dual
union all
select 6 as ramo, 'LUCRO' as desc_ramo from sys.dual
union all
select 7 as ramo, 'MANEJO' as desc_ramo from sys.dual
union all
select 8 as ramo, 'ROTURA MAQUINARIA' as desc_ramo from sys.dual
union all
select 9 as ramo, 'CASCO' as desc_ramo from sys.dual
union all
select 10 as ramo, 'RESPONSABILIDAD' as desc_ramo from sys.dual
union all
select 12 as ramo, 'SUSTRACCION' as desc_ramo from sys.dual
union all
select 13 as ramo, 'TERREMOTO' as desc_ramo from sys.dual
union all
select 14 as ramo, 'TODO RIESGO' as desc_ramo from sys.dual
union all
select 15 as ramo, 'TRANSPORTE' as desc_ramo from sys.dual
union all
select 16 as ramo, 'VIDRIOS' as desc_ramo from sys.dual
union all
select 24 as ramo, 'DESEMPLEO' as desc_ramo from sys.dual
union all
select 25 as ramo, 'HOGAR' as desc_ramo from sys.dual
union all
select 44 as ramo, 'JUDICIAL' as desc_ramo from sys.dual
union all
select 95 as ramo, 'MONTAJE' as desc_ramo from sys.dual
union all
select 96 as ramo, 'EQUIPO Y MAQUINARIA' as desc_ramo from sys.dual
union all
select 18 as ramo, 'ACCIDENTES PERSONALES' as desc_ramo from sys.dual
union all
select 20 as ramo, 'VIDA GRUPO NO CONTRIBUTIVO' as desc_ramo from sys.dual
union all
select 22 as ramo, 'DEUDORES' as desc_ramo from sys.dual
union all
select 30 as ramo, 'EXEQUIAS' as desc_ramo from sys.dual
union all
select 31 as ramo, 'PREVISIONALES' as desc_ramo from sys.dual
union all
select 34 as ramo, 'SALUD' as desc_ramo from sys.dual
union all
select 40 as ramo, 'VIDA INDIVIDUAL' as desc_ramo from sys.dual
union all
select 50 as ramo, 'RIESGOS PROFESIONALES' as desc_ramo from sys.dual
union all
select 51 as ramo, 'RENTAS VITALICIAS' as desc_ramo from sys.dual
union all
select 52 as ramo, 'RENTAS TASA VARIABLE' as desc_ramo from sys.dual
union all
select 70 as ramo, 'VIDA GRUPO CONTRIBUTIVO' as desc_ramo from sys.dual
;
commit;

create index acsele_alfa_bi.pcr_ramos_idx_1
on acsele_alfa_bi.pcr_ramos (ramo);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.pcr_ramos
;


/* ========================================================================================== */
/* PASO 1 - PIVOTE: brutos de REASEGUROS con PRIMAALFA <> 0                                   */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.pcr_1;
commit;
*/

create table acsele_alfa_bi.pcr_1
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  r.cor_id
	, r.riskunit_id
	, r.rekind_id
	, r.companycode
from acsele_alfa_bi.reaseguros r
where r.tipo_registro = 'BRUTO'
and r.alfapremium <> 0
group by r.cor_id
	, r.riskunit_id
	, r.rekind_id
	, r.companycode
;
commit;

create index acsele_alfa_bi.pcr_1_idx_1
on acsele_alfa_bi.pcr_1 (cor_id, riskunit_id, rekind_id);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.pcr_1
;


/* ========================================================================================== */
/* PASO 2 - Base PCR: brutos del pivote y el detalle de reaseguradores de esos brutos         */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.pcr_2;
commit;
*/

create table acsele_alfa_bi.pcr_2
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  r.*
from acsele_alfa_bi.pcr_1 k
inner join acsele_alfa_bi.reaseguros r
on r.cor_id = k.cor_id
and r.riskunit_id = k.riskunit_id
and r.rekind_id = k.rekind_id
;
commit;

create index acsele_alfa_bi.pcr_2_idx_1
on acsele_alfa_bi.pcr_2 (cor_id);

create index acsele_alfa_bi.pcr_2_idx_2
on acsele_alfa_bi.pcr_2 (poliza_matriz);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.pcr_2
;


/* ========================================================================================== */
/* PASO 3 - Polizas matriz de la base                                                         */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.pcr_3;
commit;
*/

create table acsele_alfa_bi.pcr_3
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  b.poliza_matriz
from acsele_alfa_bi.pcr_2 b
where b.poliza_matriz is not null
group by b.poliza_matriz
;
commit;

create index acsele_alfa_bi.pcr_3_idx_1
on acsele_alfa_bi.pcr_3 (poliza_matriz);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.pcr_3
;


/* ========================================================================================== */
/* PASO 4A - Tomador de la poliza matriz desde EMISIONES (Oro malla 1): menor identificacion  */
/* por poliza matriz                                                                          */
/*                                                                                            */
/* La homologacion pide pegarlo a partir del acuerdo. EMISIONES solo trae acuerdos con        */
/* emision en el periodo: los vigentes de meses anteriores sin emision en el mes quedan       */
/* vacios (punto a validar).                                                                  */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.pcr_4a;
commit;
*/

create table acsele_alfa_bi.pcr_4a
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  pm.poliza_matriz
	, min(e.identificacion_tomador)		as id_tomador_poliza_matriz
from acsele_alfa_bi.pcr_3 pm
inner join acsele_alfa_bi.emisiones e
on to_char(e.numero_poliza_matriz) = to_char(pm.poliza_matriz)
where e.identificacion_tomador is not null
group by pm.poliza_matriz
;
commit;

create index acsele_alfa_bi.pcr_4a_idx_1
on acsele_alfa_bi.pcr_4a (poliza_matriz, id_tomador_poliza_matriz);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.pcr_4a
;


/* ========================================================================================== */
/* PASO 4 - Nombre del tomador de la poliza matriz                                            */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.pcr_4;
commit;
*/

create table acsele_alfa_bi.pcr_4
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  a.poliza_matriz
	, a.id_tomador_poliza_matriz
	, min(e.tomador)		as tomador_poliza_matriz
from acsele_alfa_bi.pcr_4a a
inner join acsele_alfa_bi.emisiones e
on to_char(e.numero_poliza_matriz) = to_char(a.poliza_matriz)
and e.identificacion_tomador = a.id_tomador_poliza_matriz
group by a.poliza_matriz
	, a.id_tomador_poliza_matriz
;
commit;

create index acsele_alfa_bi.pcr_4_idx_1
on acsele_alfa_bi.pcr_4 (poliza_matriz);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.pcr_4
;


/* ========================================================================================== */
/* PASO 5 - Operaciones de la base PCR (pivote de comisiones)                                 */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.pcr_5;
commit;
*/

create table acsele_alfa_bi.pcr_5
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  k.cor_id			as operationpk
from acsele_alfa_bi.pcr_1 k
group by k.cor_id
;
commit;

create index acsele_alfa_bi.pcr_5_idx_1
on acsele_alfa_bi.pcr_5 (operationpk);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.pcr_5
;


/* ========================================================================================== */
/* PASO 6A - Terceros que cobran comision en las partidas de la base PCR (REUTILIZA M3 rs_11, */
/* rs_cfg_4, rs_cfg_1)                                                                        */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.pcr_ter;
commit;
*/

create table acsele_alfa_bi.pcr_ter
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  oi.thirdpartyid
from acsele_alfa_bi.pcr_5 op
inner join acsele_alfa_bi.rs_11 oi
on oi.operationpk = op.operationpk
left join acsele_alfa_bi.rs_cfg_4 c4
on c4.dty_id = oi.dty_id
left join acsele_alfa_bi.rs_cfg_1 c1
on c1.dty_id = oi.dty_id
and c1.concepto = 'IMP_RECAUDO'
where (c4.dty_id is not null or c1.dty_id is not null)
group by oi.thirdpartyid
;
commit;

create index acsele_alfa_bi.pcr_ter_idx_1
on acsele_alfa_bi.pcr_ter (thirdpartyid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.pcr_ter
;


/* ========================================================================================== */
/* PASO 6B - DOCUMENTO Y NOMBRE DEL COMISIONISTA: REUTILIZA M3 (rs_ppt, participantes de la   */
/* poliza) + DELTA solo de los terceros que no son participantes (stte_thirdparty,            */
/* prethirdparty, anti-join)                                                                  */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.pcr_doc;
commit;
*/

create table acsele_alfa_bi.pcr_doc
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  p.thirdpartyid
	, min(p.tipodocumento)		as tipo_doc_comisionista
	, min(p.documento)			as documento
	, min(p.nombre)				as nombre_comisionista
from acsele_alfa_bi.pcr_ter t
inner join acsele_alfa_bi.rs_ppt p
on p.thirdpartyid = t.thirdpartyid
group by p.thirdpartyid
union all
select /*+ parallel(3) */
	  t.thirdpartyid
	, min(ptp.tipodocumentoinput)
	, min(ptp.documentoidentidadinput)
	, min(ptp.nombrepredeterminadoinput)
from acsele_alfa_bi.pcr_ter t
inner join acsele_alfa.stte_thirdparty tp
on tp.tpt_id = t.thirdpartyid
inner join acsele_alfa.prethirdparty ptp
on ptp.pk = tp.iddco
left join acsele_alfa_bi.rs_ppt m
on m.thirdpartyid = t.thirdpartyid
where m.thirdpartyid is null
group by t.thirdpartyid
;
commit;

create index acsele_alfa_bi.pcr_doc_idx_1
on acsele_alfa_bi.pcr_doc (thirdpartyid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.pcr_doc
;


/* ========================================================================================== */
/* PASO 6C - EXTRAE NUEVO: descripcion del rol (stts_role) de las partidas de comision        */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.pcr_rol;
commit;
*/

create table acsele_alfa_bi.pcr_rol
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  rl.rol_id
	, rl.rol_description
from acsele_alfa_bi.pcr_5 op
inner join acsele_alfa_bi.rs_11 oi
on oi.operationpk = op.operationpk
inner join acsele_alfa.stts_role rl
on rl.rol_id = oi.rol_id
group by rl.rol_id
	, rl.rol_description
;
commit;

create index acsele_alfa_bi.pcr_rol_idx_1
on acsele_alfa_bi.pcr_rol (rol_id);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.pcr_rol
;


/* ========================================================================================== */
/* PASO 6 - Comisionistas de RECAUDO por operacion (REUTILIZA rs_11; tipos de documento de    */
/* RRC.commissionCollection* e impuesto RRC.taxCommissionCollection*)                         */
/*                                                                                            */
/* Comision con el signo del RRC (dty_isdebit = 0 suma). Documento sin digito de              */
/* verificacion: se corta en el guion si lo trae.                                             */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.pcr_6;
commit;
*/

create table acsele_alfa_bi.pcr_6
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  oi.operationpk
	, oi.thirdpartyid
	, oi.rol_id
	, rl.rol_description
	, d.tipo_doc_comisionista
	, (case when instr(d.documento, '-') > 0 then substr(d.documento, 1, instr(d.documento, '-') - 1) else d.documento end)		as id_comisionista
	, d.nombre_comisionista
	, sum(case when c4.dty_id is not null then (case when oi.dty_isdebit = 0 then oi.amount else oi.amount * (-1) end) else 0 end)		as comision
	, sum(case when c1.dty_id is not null then (case when oi.dty_isdebit = 0 then oi.amount else oi.amount * (-1) end) else 0 end)		as iva_comision
from acsele_alfa_bi.pcr_5 op
inner join acsele_alfa_bi.rs_11 oi
on oi.operationpk = op.operationpk
inner join acsele_alfa_bi.pcr_doc d
on d.thirdpartyid = oi.thirdpartyid
inner join acsele_alfa_bi.pcr_rol rl
on rl.rol_id = oi.rol_id
left join acsele_alfa_bi.rs_cfg_4 c4
on c4.dty_id = oi.dty_id
and c4.concepto = 'COM_RECAUDO'
left join acsele_alfa_bi.rs_cfg_1 c1
on c1.dty_id = oi.dty_id
and c1.concepto = 'IMP_RECAUDO'
where (c4.dty_id is not null or c1.dty_id is not null)
group by oi.operationpk
	, oi.thirdpartyid
	, oi.rol_id
	, rl.rol_description
	, d.tipo_doc_comisionista
	, (case when instr(d.documento, '-') > 0 then substr(d.documento, 1, instr(d.documento, '-') - 1) else d.documento end)
	, d.nombre_comisionista
;
commit;

create index acsele_alfa_bi.pcr_6_idx_1
on acsele_alfa_bi.pcr_6 (operationpk, thirdpartyid, rol_id);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.pcr_6
;


/* ========================================================================================== */
/* PASO 7 - Comisionistas de INTERMEDIACION por operacion (REUTILIZA rs_11 con los tipos de   */
/* documento de RRC.commisionIntermediary y rs_uaa con su comision e IVA)                     */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.pcr_7;
commit;
*/

create table acsele_alfa_bi.pcr_7
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  oi.operationpk
	, oi.thirdpartyid
	, oi.rol_id
	, rl.rol_description
	, d.tipo_doc_comisionista
	, (case when instr(d.documento, '-') > 0 then substr(d.documento, 1, instr(d.documento, '-') - 1) else d.documento end)		as id_comisionista
	, d.nombre_comisionista
	, sum(case when u.simbolo = 'Comision' then u.value else 0 end)		as comision
	, sum(case when u.simbolo = 'IvaComision' then u.value else 0 end)	as iva_comision
from acsele_alfa_bi.pcr_5 op
inner join acsele_alfa_bi.rs_11 oi
on oi.operationpk = op.operationpk
inner join acsele_alfa_bi.rs_cfg_4 c4
on c4.dty_id = oi.dty_id
inner join acsele_alfa_bi.pcr_doc d
on d.thirdpartyid = oi.thirdpartyid
inner join acsele_alfa_bi.pcr_rol rl
on rl.rol_id = oi.rol_id
left join acsele_alfa_bi.rs_uaa u
on u.openitemid = oi.openitemid
where c4.concepto = 'COM_INTERMEDIACION'
group by oi.operationpk
	, oi.thirdpartyid
	, oi.rol_id
	, rl.rol_description
	, d.tipo_doc_comisionista
	, (case when instr(d.documento, '-') > 0 then substr(d.documento, 1, instr(d.documento, '-') - 1) else d.documento end)
	, d.nombre_comisionista
;
commit;

create index acsele_alfa_bi.pcr_7_idx_1
on acsele_alfa_bi.pcr_7 (operationpk, thirdpartyid, rol_id);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.pcr_7
;


/* ========================================================================================== */
/* PASO 8 - Porcentajes por tercero y rol: REUTILIZA M3 (rs_ppt, rs_pin) + EXTRAE NUEVO       */
/* participacion (porcentaje del recaudador)                                                  */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.pcr_8;
commit;
*/

create table acsele_alfa_bi.pcr_8
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  p.operationpk
	, p.thirdpartyid
	, p.rol_id
	, to_number(max(par.porcentajeinput))				as porc_participacion_recaudo
	, to_number(max(pi.porcentajeparticipacinput))		as porc_part_intermediario
	, to_number(max(pi.porcentajecomisioninput))		as porc_comision_intermediario
from acsele_alfa_bi.pcr_5 op
inner join acsele_alfa_bi.rs_ppt p
on p.operationpk = op.operationpk
left join acsele_alfa_bi.rs_pin pi
on pi.dcoid = p.dcoid
left join acsele_alfa.participacion par
on par.pk = p.dcoid
group by p.operationpk
	, p.thirdpartyid
	, p.rol_id
;
commit;

create index acsele_alfa_bi.pcr_8_idx_1
on acsele_alfa_bi.pcr_8 (operationpk, thirdpartyid, rol_id);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.pcr_8
;


/* ========================================================================================== */
/* PASO 9A - Comisionista de recaudo de cada operacion para la base PCR: el de menor          */
/* documento con comision distinta de cero                                                    */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.pcr_9a;
commit;
*/

create table acsele_alfa_bi.pcr_9a
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  c.operationpk
	, min(c.id_comisionista)		as id_com_recaudo
from acsele_alfa_bi.pcr_6 c
where c.comision <> 0
group by c.operationpk
;
commit;

create index acsele_alfa_bi.pcr_9a_idx_1
on acsele_alfa_bi.pcr_9a (operationpk, id_com_recaudo);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.pcr_9a
;


/* ========================================================================================== */
/* PASO 9 - Nombre del comisionista de recaudo                                                */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.pcr_9;
commit;
*/

create table acsele_alfa_bi.pcr_9
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  a.operationpk
	, a.id_com_recaudo
	, min(c.nombre_comisionista)		as nombre_com_recaudo
from acsele_alfa_bi.pcr_9a a
inner join acsele_alfa_bi.pcr_6 c
on c.operationpk = a.operationpk
and c.id_comisionista = a.id_com_recaudo
group by a.operationpk
	, a.id_com_recaudo
;
commit;

create index acsele_alfa_bi.pcr_9_idx_1
on acsele_alfa_bi.pcr_9 (operationpk);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.pcr_9
;


/* ========================================================================================== */
/* PASO 10 - RESULTADO ORO 1: PCR_DETALLE (base PCR con la homologacion PCR Point - Acsel-e,  */
/* ORIGEN = Acsele)                                                                           */
/*                                                                                            */
/* Columnas en el orden de la hoja ResultadoEsperado (59 campos) + 6 columnas de trazabilidad */
/* al final.                                                                                  */
/* COD_COBERTURA / DESC_COBERTURA: ramo de reaseguro, como lo agrupa hoy el boton de Acsel-e  */
/* (la historia de usuario lo deja por confirmar con Consis).                                 */
/* GASTOS_EXP_PORCENTAJE y GASTOS_EXP: vacios; la homologacion pide que sean parametro de     */
/* Prophet administrado por Actuaria.                                                         */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.pcr_detalle;
commit;
*/

create table acsele_alfa_bi.pcr_detalle
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  'Acsele'		as origen
	, (case b.companycode when '0.0' then 'Seguros Alfa S.A.' when '50.0' then 'Seguros de Vida Alfa S.A.' else b.nom_compania end)		as compania
	, b.accountingbranchcod		as ramo
	, rm.desc_ramo		as desc_ramo
	, b.pro_id		as producto
	, b.desc_producto		as desc_producto
	, to_number(replace(b.cod_ramo_tecnico, '.0', ''))		as ramo_tecnico
	, b.desc_ramo_tecnico		as desc_ramo_tecnico
	, b.poliza_matriz		as poliza_matriz
	, b.policynumber		as poliza
	, b.branchcode		as sucursal
	, cast(null as varchar2(10))		as modulo
	, b.riskunitnum		as unidad_riesgo
	, b.groupreinsurancecod		as cod_cobertura
	, b.groupreinsurancedesc		as desc_cobertura
	, b.endorsementnum		as endoso
	, to_char(b.endorsementdate, 'dd/mm/yyyy')		as fecha_endoso
	, to_number(to_char(b.endorsementdate, 'yyyymm'))		as mespro
	, b.businessunit		as unidad_negocio
	, to_char(b.policyinitialdate, 'dd/mm/yyyy')		as fecha_inicio_poliza_matriz
	, to_char(b.policyfinishdate, 'dd/mm/yyyy')		as fecha_fin_poliza_matriz
	, to_char(b.coveragestartdate, 'dd/mm/yyyy')		as fecha_inicio_cobertura
	, to_char(b.coverageenddate, 'dd/mm/yyyy')		as fecha_fin_cobertura
	, tm.id_tomador_poliza_matriz		as id_tomador_poliza_matriz
	, tm.tomador_poliza_matriz		as tomador_poliza_matriz
	, b.tpt_id		as id_tomador_poliza
	, b.third_desc		as tomador_poliza
	, (case b.coinsurancetype when 1 then 'D' when 2 then 'A' when 3 then 'C' end)		as tipo_coaseguro
	, b.percentageparticalfa		as porcpart_alfa
	, b.paymentfrequency		as frecuencia_pago
	, b.issuedpremium		as prima_emitida
	, (b.issuedpremium - b.alfapremium)		as primaced_coaseguro
	, b.alfapremium		as primaalfa
	, b.intercommipercentage		as porc_com_intermediacion
	, b.brokeragecommission		as comision_intermediacion
	, b.taxbrokeragecommission		as iva_comision_intermediacion
	, (case when b.tipo_registro = 'BRUTO' and b.commissioncollection <> 0 then rec.id_com_recaudo end)		as id_com_recaudo
	, (case when b.tipo_registro = 'BRUTO' and b.commissioncollection <> 0 then rec.nombre_com_recaudo end)		as nombre_com_recaudo
	, b.collectcommipercentage		as porccomrecaudo
	, b.commissioncollection		as comision_recaudo
	, b.taxcommissioncollection		as iva_comision_recaudo
	, b.reinsurercode		as codreaseg
	, b.reinsurerdesc		as nomreaseg
	, b.reinsurerclass		as clase_reasegurador
	, b.cededpremiumcttoautom		as primacedaut
	, b.cededpremcttoautominte		as primacedaut_int
	, b.cededpremcttoautomouter		as primacedaut_ext
	, b.facultativecededpremium		as primacedfac
	, b.intefacultcededpremium		as primacedfac_int
	, b.outerfacultcededpremium		as primacedfac_ext
	, b.automaticreinscommi		as comisionreasegaut
	, b.inteautomaticreinscommi		as comisionreasegaut_int
	, b.outerautomaticreinscommi		as comisionreasegaut_ext
	, b.facultativereinscommi		as comisionreasegfac
	, b.intefacultareinscommi		as comisionreasegfac_int
	, b.outerfacultareinscommi		as comisionreasegfac_ext
	, cast(null as number)		as gastos_exp_porcentaje
	, cast(null as number)		as gastos_exp
	, cast(null as number)		as unidad_de_cuenta
	, b.tipo_registro		as tipo_registro
	, b.universo		as universo
	, b.companycode		as companycode
	, b.cor_id		as cor_id
	, b.riskunit_id		as riskunit_id
	, b.rekind_id		as rekind_id
from acsele_alfa_bi.pcr_2 b
left join acsele_alfa_bi.pcr_ramos rm
on rm.ramo = b.accountingbranchcod
left join acsele_alfa_bi.pcr_4 tm
on tm.poliza_matriz = b.poliza_matriz
left join acsele_alfa_bi.pcr_9 rec
on rec.operationpk = b.cor_id
;
commit;

create index acsele_alfa_bi.pcr_detalle_idx_1
on acsele_alfa_bi.pcr_detalle (mespro, compania, ramo);

create index acsele_alfa_bi.pcr_detalle_idx_2
on acsele_alfa_bi.pcr_detalle (poliza_matriz);

create index acsele_alfa_bi.pcr_detalle_idx_3
on acsele_alfa_bi.pcr_detalle (poliza);

create index acsele_alfa_bi.pcr_detalle_idx_4
on acsele_alfa_bi.pcr_detalle (cor_id);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.pcr_detalle
;


/* ========================================================================================== */
/* PASO 11 - RESULTADO ORO 2: PCR_TOTALES (resumen de primas y comisiones para el calculo de  */
/* la reserva)                                                                                */
/*                                                                                            */
/* Historia de usuario: Prima = PRIMAALFA; Prima cedida = PRIMACEDAUT + PRIMACEDFAC; Comision */
/* reaseguro = COMISIONREASEGAUT + COMISIONREASEGFAC; Comision intermediacion y Comision      */
/* recaudo: Generales el valor, Vida el valor + su IVA.                                       */
/* Grano: compania, ramo, origen (historia de usuario) + unidad de negocio y MESPRO (consulta */
/* de validacion de Actuaria). Para la vista de la historia de usuario se suma sin las dos    */
/* ultimas.                                                                                   */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.pcr_totales;
commit;
*/

create table acsele_alfa_bi.pcr_totales
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  d.origen
	, d.compania
	, d.companycode
	, d.ramo
	, d.desc_ramo
	, d.unidad_negocio
	, d.mespro
	, count(*)																				as registros
	, sum(d.primaalfa)																		as prima
	, sum(d.primacedaut + d.primacedfac)													as prima_cedida
	, sum(d.comisionreasegaut + d.comisionreasegfac)										as comision_reaseguro
	, sum(case when d.companycode = '50.0' then d.comision_intermediacion + d.iva_comision_intermediacion else d.comision_intermediacion end)	as comision_intermediacion
	, sum(case when d.companycode = '50.0' then d.comision_recaudo + d.iva_comision_recaudo else d.comision_recaudo end)						as comision_recaudo
	, sum(d.comision_intermediacion)														as com_intermediacion_sin_iva
	, sum(d.iva_comision_intermediacion)													as iva_comision_intermediacion
	, sum(d.comision_recaudo)																as comision_recaudo_sin_iva
	, sum(d.iva_comision_recaudo)															as iva_comision_recaudo
	, sum(d.prima_emitida)																	as prima_emitida
from acsele_alfa_bi.pcr_detalle d
group by d.origen
	, d.compania
	, d.companycode
	, d.ramo
	, d.desc_ramo
	, d.unidad_negocio
	, d.mespro
;
commit;

create index acsele_alfa_bi.pcr_totales_idx_1
on acsele_alfa_bi.pcr_totales (mespro, compania, ramo);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.pcr_totales
;


/* ========================================================================================== */
/* PASO 12 - Datos de la operacion para comisiones (un registro por operacion, desde los      */
/* brutos de PCR_DETALLE)                                                                     */
/*                                                                                            */
/* Si una operacion tiene brutos de varios ramos contables se toma el menor (punto a          */
/* validar).                                                                                  */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.pcr_10;
commit;
*/

create table acsele_alfa_bi.pcr_10
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  d.cor_id
	, min(d.origen)						as origen
	, min(d.compania)					as compania
	, min(d.ramo)						as ramo
	, min(d.producto)					as producto
	, min(d.desc_producto)				as desc_producto
	, min(d.poliza_matriz)				as poliza_matriz
	, min(d.poliza)						as poliza
	, min(d.sucursal)					as sucursal
	, min(d.mespro)						as mespro
	, min(d.unidad_negocio)				as unidad_negocio
	, min(d.fecha_inicio_cobertura)		as fecha_inicio_cobertura
	, min(d.fecha_fin_cobertura)		as fecha_fin_cobertura
	, min(d.id_tomador_poliza_matriz)	as id_tomador_poliza_matriz
	, min(d.tomador_poliza_matriz)		as tomador_poliza_matriz
	, min(d.frecuencia_pago)			as frecuencia_pago
from acsele_alfa_bi.pcr_detalle d
where d.tipo_registro = 'BRUTO'
group by d.cor_id
;
commit;

create index acsele_alfa_bi.pcr_10_idx_1
on acsele_alfa_bi.pcr_10 (cor_id);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.pcr_10
;


/* ========================================================================================== */
/* PASO 13 - RESULTADO ORO 3: PCR_COMISIONES (detalle de comisionistas de recaudo e           */
/* intermediacion en una sola salida)                                                         */
/*                                                                                            */
/* Columnas de la hoja Base Intermediarios Caso Nuevo, con TIPO_COMISION (RECAUDO /           */
/* INTERMEDIACION) y los datos del comisionista. Es el detalle que soporta las columnas de    */
/* comision de PCR_TOTALES.                                                                   */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.pcr_comisiones;
commit;
*/

create table acsele_alfa_bi.pcr_comisiones
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  'RECAUDO'			as tipo_comision
	, o.origen
	, o.compania
	, o.ramo
	, rm.desc_ramo
	, o.producto
	, o.desc_producto
	, o.poliza_matriz
	, o.poliza
	, o.sucursal
	, o.mespro
	, o.unidad_negocio
	, o.fecha_inicio_cobertura
	, o.fecha_fin_cobertura
	, o.id_tomador_poliza_matriz
	, o.tomador_poliza_matriz
	, o.frecuencia_pago
	, o.cor_id						as endoso
	, c.tipo_doc_comisionista
	, c.id_comisionista
	, c.nombre_comisionista
	, c.rol_description				as rol_comisionista
	, p.porc_participacion_recaudo		as porc_participacion
	, cast(null as number)		as porc_comision
	, c.comision
	, c.iva_comision
from acsele_alfa_bi.pcr_6 c
inner join acsele_alfa_bi.pcr_10 o
on o.cor_id = c.operationpk
left join acsele_alfa_bi.pcr_ramos rm
on rm.ramo = o.ramo
left join acsele_alfa_bi.pcr_8 p
on p.operationpk = c.operationpk
and p.thirdpartyid = c.thirdpartyid
and p.rol_id = c.rol_id
union all
select /*+ parallel(3) */
	  'INTERMEDIACION'			as tipo_comision
	, o.origen
	, o.compania
	, o.ramo
	, rm.desc_ramo
	, o.producto
	, o.desc_producto
	, o.poliza_matriz
	, o.poliza
	, o.sucursal
	, o.mespro
	, o.unidad_negocio
	, o.fecha_inicio_cobertura
	, o.fecha_fin_cobertura
	, o.id_tomador_poliza_matriz
	, o.tomador_poliza_matriz
	, o.frecuencia_pago
	, o.cor_id						as endoso
	, c.tipo_doc_comisionista
	, c.id_comisionista
	, c.nombre_comisionista
	, c.rol_description				as rol_comisionista
	, p.porc_part_intermediario		as porc_participacion
	, p.porc_comision_intermediario		as porc_comision
	, c.comision
	, c.iva_comision
from acsele_alfa_bi.pcr_7 c
inner join acsele_alfa_bi.pcr_10 o
on o.cor_id = c.operationpk
left join acsele_alfa_bi.pcr_ramos rm
on rm.ramo = o.ramo
left join acsele_alfa_bi.pcr_8 p
on p.operationpk = c.operationpk
and p.thirdpartyid = c.thirdpartyid
and p.rol_id = c.rol_id
;
commit;

create index acsele_alfa_bi.pcr_comisiones_idx_1
on acsele_alfa_bi.pcr_comisiones (endoso);

create index acsele_alfa_bi.pcr_comisiones_idx_2
on acsele_alfa_bi.pcr_comisiones (mespro, compania, ramo);

create index acsele_alfa_bi.pcr_comisiones_idx_3
on acsele_alfa_bi.pcr_comisiones (id_comisionista);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.pcr_comisiones
;


/* ========================================================================================== */
/* VALIDACIONES                                                                               */
/* ========================================================================================== */

/* 1. Validacion de Actuaria (Estrategia Valida capa oro): primas y comisiones del mes de corte deben dar lo contable */
select /*+ parallel(3) */
	  t.compania
	, t.ramo
	, t.origen
	, t.unidad_negocio
	, t.mespro
	, sum(t.prima)								as primaalfa
	, sum(t.prima_cedida)						as prima_cedida
	, sum(t.com_intermediacion_sin_iva)	as comision_intermediacion
	, sum(t.iva_comision_intermediacion)		as iva_comision_intermediacion
	, sum(t.comision_recaudo_sin_iva)			as comision_recaudo
	, sum(t.iva_comision_recaudo)				as iva_comision_recaudo
	, sum(t.comision_reaseguro)					as comision_reaseguro
from acsele_alfa_bi.pcr_totales t
inner join acsele_alfa_bi.cfg_pcr_periodo per
on per.mespro = t.mespro
group by t.compania
	, t.ramo
	, t.origen
	, t.unidad_negocio
	, t.mespro
order by t.compania
	, t.ramo
	, t.origen
	, t.unidad_negocio
;

/* 2. Vista de la historia de usuario: por compania, ramo y origen */
select /*+ parallel(3) */
	  t.compania
	, t.ramo
	, t.desc_ramo
	, t.origen
	, sum(t.prima)						as prima
	, sum(t.prima_cedida)				as prima_cedida
	, sum(t.comision_reaseguro)			as comision_reaseguro
	, sum(t.comision_intermediacion)	as comision_intermediacion
	, sum(t.comision_recaudo)			as comision_recaudo
from acsele_alfa_bi.pcr_totales t
group by t.compania
	, t.ramo
	, t.desc_ramo
	, t.origen
order by t.compania
	, t.ramo
;

/* 3. Cuadre de comisiones: el detalle de comisionistas contra los totales del mismo MESPRO (diferencia esperada cero o explicada) */
select /*+ parallel(3) */
	  c.tipo_comision
	, c.compania
	, c.mespro
	, sum(c.comision)			as comision_detalle
	, sum(c.iva_comision)		as iva_detalle
from acsele_alfa_bi.pcr_comisiones c
group by c.tipo_comision
	, c.compania
	, c.mespro
order by c.tipo_comision
	, c.compania
	, c.mespro
;

/* 4. Cuadre del mes contra las mallas 1 y 2: prima emitida y prima cedida de PCR (MESPRO del corte, cfg_pcr_periodo) vs EMISIONES y PRIMAS_CEDIDAS */
select /*+ parallel(3) */
	  'PCR'								as fuente
	, d.companycode						as compania
	, sum(d.prima_emitida)				as prima_emitida
	, sum(d.primacedaut + d.primacedfac)	as prima_cedida
from acsele_alfa_bi.pcr_detalle d
inner join acsele_alfa_bi.cfg_pcr_periodo per
on per.mespro = d.mespro
group by d.companycode
union all
select /*+ parallel(3) */
	  'M1 EMISIONES'
	, e.codigo_compania
	, sum(e.prima)
	, 0
from acsele_alfa_bi.emisiones e
group by e.codigo_compania
union all
select /*+ parallel(3) */
	  'M2 PRIMAS_CEDIDAS'
	, pc.cod_compania
	, 0
	, sum(pc.prima_cedida)
from acsele_alfa_bi.primas_cedidas pc
group by pc.cod_compania
;

/* 5. Reglas de la historia de usuario (deben dar cero) */
select /*+ parallel(3) */
	  sum(case when d.tipo_registro = 'BRUTO' and d.primaalfa = 0 then 1 else 0 end)											as bruto_primaalfa_cero
	, sum(case when d.comision_recaudo <> 0 and (d.id_com_recaudo is null or d.nombre_com_recaudo is null) then 1 else 0 end)	as recaudo_sin_comisionista
	, sum(case when d.desc_ramo is null then 1 else 0 end)																	as ramo_sin_descripcion
	, sum(case when d.codreaseg = 0 and d.tipo_registro = 'DETALLE' then 1 else 0 end)										as detalle_sin_codigo
from acsele_alfa_bi.pcr_detalle d
;
