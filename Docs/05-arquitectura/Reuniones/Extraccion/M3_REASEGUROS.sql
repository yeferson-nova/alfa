/* ========================================================================================== */
/* M3 REASEGUROS - MALLA 3 DEL PROCESO PCR  -  VERSION 1.5 - 2026-11-09                       */
/*                                                                                            */
/* Salida: acsele_alfa_bi.reaseguros (Zona Oro de la malla 3). Replica la caida de datos del  */
/* boton de reserva de riesgo en curso (EXT_RISKRESERVEALFA, CONSULTAS_RRC_v3): registros     */
/* BRUTO (totales por operacion, unidad de riesgo y ramo de reaseguro) y DETALLE (por         */
/* reasegurador).                                                                             */
/* DEPENDENCIA: corre despues de las mallas 1 y 2. HEREDA el universo de la malla 1 a traves  */
/* de la malla 2 (MES = emitidas del periodo + VIGENTE = polizas no mensuales vigentes al     */
/* corte) y REUTILIZA su extraccion:                                                          */
/*  - de M2: operaciones y poliza (pc_1, pc_2), unidades de riesgo (pc_1r), acuerdo y         */
/* versiones (pc_opacu, pc_acu, pc_10), toda la cadena de reaseguro (pc_3, pc_6, pc_7d,       */
/* pc_7p, pc_7r, pc_9, pc_11) y coberturas (pc_8);                                            */
/*  - de M1: participantes de la poliza (em_tpb) y movimientos (em_tmb) de todo el universo.  */
/*  - ZONA PLATA: stst_systemproperty (copia de la original) y stst_systemproperty_OPT (una   */
/* fila por propiedad y elemento de su lista) para que las consultas de conceptos sean por    */
/* igualdad.                                                                                  */
/*  - EXTRAE NUEVO: ri_reinsurancegroup / stri_reinsurancekind, financialplandco,             */
/* preriskunits, personajuridica, participacionintermediarios, openitem, uaadetail /          */
/* property, tdcalificacionriesgo y parametros de stst_systemproperty.                        */
/*  - PUBLICA para M4: partidas (openitem) con los tipos de documento del RRC y de los        */
/* scripts de comisionistas, su uaadetail, los participantes de la poliza y los porcentajes   */
/* de los intermediarios.                                                                     */
/* Reglas: pivote -> dependientes; inner antes de left; cero subqueries; sin KEEP / OVER /    */
/* DENSE_RANK; sin funciones PL/SQL; esquema en todas las tablas; lectura parallel(3) /       */
/* escritura parallel 2.                                                                      */
/* PRUEBA EN ORACLE - VERSION POR TABLAS (recomendada): paso a paso, con indices y conteo por */
/* tabla. Parametros (periodo, corte, companias, productos, numeros): se cambian SOLO en      */
/* 00_PARAMETROS/PCR_PARAMETROS_v1.5.sql. Conceptos: salen de stst_systemproperty (malla 3).  */
/* ========================================================================================== */


/* ========================================================================================== */
/* PLATA 1 - COPIA de stst_systemproperty (tabla de propiedades de Acsel-e, tal cual)         */
/*                                                                                            */
/* Zona Plata: copia de la original. Es la fuente unica de las listas de conceptos (tipos de  */
/* documento, roles, conceptos de movimientos).                                               */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.stst_systemproperty;
commit;
*/

create table acsele_alfa_bi.stst_systemproperty
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  syp.*
from acsele_alfa.stst_systemproperty syp
;
commit;

create index acsele_alfa_bi.stst_systemproperty_idx_1
on acsele_alfa_bi.stst_systemproperty (syp_property);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.stst_systemproperty
;


/* ========================================================================================== */
/* PLATA 2 - OPT: stst_systemproperty OPTIMIZADA para consultas, un registro por propiedad y  */
/* elemento de su lista                                                                       */
/*                                                                                            */
/* Traduce el WITH DESCRIPTIONS ... TRIM(REGEXP_SUBSTR(DESCRIPTION, '[^,]+', 1, LEVEL)) ...   */
/* CONNECT BY LEVEL <= REGEXP_COUNT(DESCRIPTION, ',') + 1 del RRC y de los scripts de         */
/* comisionistas.                                                                             */
/* Se resuelve una sola vez con la tabla de numeros cfg_pcr_numeros (sin CONNECT BY ni        */
/* subconsulta); despues cada concepto se consulta con un cruce de igualdad (elemento =       */
/* dty_description / rol_description), que usa indice.                                        */
/* posicion = orden del elemento en la lista; se agrupa para no repetir un elemento dentro de */
/* la misma propiedad.                                                                        */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.stst_systemproperty_opt;
commit;
*/

create table acsele_alfa_bi.stst_systemproperty_opt
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  syp.syp_property
	, trim(regexp_substr(syp.syp_value, '[^,]+', 1, n.n))		as elemento
	, min(n.n)													as posicion
from acsele_alfa_bi.stst_systemproperty syp
inner join acsele_alfa_bi.cfg_pcr_numeros n
on n.n <= regexp_count(syp.syp_value, ',') + 1
where syp.syp_value is not null
and trim(regexp_substr(syp.syp_value, '[^,]+', 1, n.n)) is not null
group by syp.syp_property
	, trim(regexp_substr(syp.syp_value, '[^,]+', 1, n.n))
;
commit;

create index acsele_alfa_bi.stst_systemproperty_opt_idx_1
on acsele_alfa_bi.stst_systemproperty_opt (syp_property, elemento);

create index acsele_alfa_bi.stst_systemproperty_opt_idx_2
on acsele_alfa_bi.stst_systemproperty_opt (elemento);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.stst_systemproperty_opt
;


/* ========================================================================================== */
/* PARAMETRO 1 - Tipos de documento por concepto y ramo (IVA, comision de recaudo, prima      */
/* asegurada, impuesto de recaudo, comision de intermediacion), desde stst_systemproperty_opt */
/*                                                                                            */
/* ramo_key: codigo de ramo de reaseguro al que aplica la propiedad (05, 13, 10, 12, 01, 24)  */
/* o DEF para el resto (ELSE del CASE del RRC).                                               */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.rs_cfg_1;
commit;
*/

create table acsele_alfa_bi.rs_cfg_1
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  syp.syp_property
	, (case syp.syp_property
		when 'RRC.tax.fireDocType' then 'IVA'
		when 'RRC.tax.earthquakeDocType' then 'IVA'
		when 'RRC.tax.liabilityDocType' then 'IVA'
		when 'RRC.tax.subtractionDocType' then 'IVA'
		when 'RRC.tax.assistsDocType' then 'IVA'
		when 'RRC.tax.unemploymentDocType' then 'IVA'
		when 'RRC.ivaDocType' then 'IVA'
		when 'RRC.commissionCollection.fireDocType' then 'RECAUDO'
		when 'RRC.commissionCollection.earthquakeDocType' then 'RECAUDO'
		when 'RRC.commissionCollection.liabilityDocType' then 'RECAUDO'
		when 'RRC.commissionCollection.subtractionDocType' then 'RECAUDO'
		when 'RRC.commissionCollection.assistsDocType' then 'RECAUDO'
		when 'RRC.commissionCollection.unemploymentDocType' then 'RECAUDO'
		when 'RRC.commissionCollectionDocType' then 'RECAUDO'
		when 'RRC.issuedPremium.fireDocType' then 'PRIMA_ASEGURADA'
		when 'RRC.issuedPremium.earthquakeDocType' then 'PRIMA_ASEGURADA'
		when 'RRC.issuedPremium.liabilityDocType' then 'PRIMA_ASEGURADA'
		when 'RRC.issuedPremium.subtractionDocType' then 'PRIMA_ASEGURADA'
		when 'RRC.issuedPremium.assistsDocType' then 'PRIMA_ASEGURADA'
		when 'RRC.issuedPremium.unemploymentDocType' then 'PRIMA_ASEGURADA'
		when 'RRC.issuedPremiumDocType' then 'PRIMA_ASEGURADA'
		when 'RRC.taxCommissionCollection.fireDocType' then 'IMP_RECAUDO'
		when 'RRC.taxCommissionCollection.earthquakeDocType' then 'IMP_RECAUDO'
		when 'RRC.taxCommissionCollection.liabilityDocType' then 'IMP_RECAUDO'
		when 'RRC.taxCommissionCollection.subtractionDocType' then 'IMP_RECAUDO'
		when 'RRC.taxCommissionCollection.assistsDocType' then 'IMP_RECAUDO'
		when 'RRC.taxCommissionCollection.unemploymentDocType' then 'IMP_RECAUDO'
		when 'RRC.taxCommissionCollectionDocType' then 'IMP_RECAUDO'
		when 'RRC.commisionIntermediary' then 'INTERMEDIACION'
	  end)						as concepto
	, (case syp.syp_property
		when 'RRC.tax.fireDocType' then '05'
		when 'RRC.tax.earthquakeDocType' then '13'
		when 'RRC.tax.liabilityDocType' then '10'
		when 'RRC.tax.subtractionDocType' then '12'
		when 'RRC.tax.assistsDocType' then '01'
		when 'RRC.tax.unemploymentDocType' then '24'
		when 'RRC.ivaDocType' then 'DEF'
		when 'RRC.commissionCollection.fireDocType' then '05'
		when 'RRC.commissionCollection.earthquakeDocType' then '13'
		when 'RRC.commissionCollection.liabilityDocType' then '10'
		when 'RRC.commissionCollection.subtractionDocType' then '12'
		when 'RRC.commissionCollection.assistsDocType' then '01'
		when 'RRC.commissionCollection.unemploymentDocType' then '24'
		when 'RRC.commissionCollectionDocType' then 'DEF'
		when 'RRC.issuedPremium.fireDocType' then '05'
		when 'RRC.issuedPremium.earthquakeDocType' then '13'
		when 'RRC.issuedPremium.liabilityDocType' then '10'
		when 'RRC.issuedPremium.subtractionDocType' then '12'
		when 'RRC.issuedPremium.assistsDocType' then '01'
		when 'RRC.issuedPremium.unemploymentDocType' then '24'
		when 'RRC.issuedPremiumDocType' then 'DEF'
		when 'RRC.taxCommissionCollection.fireDocType' then '05'
		when 'RRC.taxCommissionCollection.earthquakeDocType' then '13'
		when 'RRC.taxCommissionCollection.liabilityDocType' then '10'
		when 'RRC.taxCommissionCollection.subtractionDocType' then '12'
		when 'RRC.taxCommissionCollection.assistsDocType' then '01'
		when 'RRC.taxCommissionCollection.unemploymentDocType' then '24'
		when 'RRC.taxCommissionCollectionDocType' then 'DEF'
		when 'RRC.commisionIntermediary' then 'ALL'
	  end)						as ramo_key
	, dt.dty_id
	, dt.dty_description
	, dt.dty_isdebit
from acsele_alfa_bi.stst_systemproperty_opt syp
inner join acsele_alfa.stca_doctype dt
on dt.dty_description = syp.elemento
where syp.syp_property in (
	  'RRC.tax.fireDocType',
	  'RRC.tax.earthquakeDocType',
	  'RRC.tax.liabilityDocType',
	  'RRC.tax.subtractionDocType',
	  'RRC.tax.assistsDocType',
	  'RRC.tax.unemploymentDocType',
	  'RRC.ivaDocType',
	  'RRC.commissionCollection.fireDocType',
	  'RRC.commissionCollection.earthquakeDocType',
	  'RRC.commissionCollection.liabilityDocType',
	  'RRC.commissionCollection.subtractionDocType',
	  'RRC.commissionCollection.assistsDocType',
	  'RRC.commissionCollection.unemploymentDocType',
	  'RRC.commissionCollectionDocType',
	  'RRC.issuedPremium.fireDocType',
	  'RRC.issuedPremium.earthquakeDocType',
	  'RRC.issuedPremium.liabilityDocType',
	  'RRC.issuedPremium.subtractionDocType',
	  'RRC.issuedPremium.assistsDocType',
	  'RRC.issuedPremium.unemploymentDocType',
	  'RRC.issuedPremiumDocType',
	  'RRC.taxCommissionCollection.fireDocType',
	  'RRC.taxCommissionCollection.earthquakeDocType',
	  'RRC.taxCommissionCollection.liabilityDocType',
	  'RRC.taxCommissionCollection.subtractionDocType',
	  'RRC.taxCommissionCollection.assistsDocType',
	  'RRC.taxCommissionCollection.unemploymentDocType',
	  'RRC.taxCommissionCollectionDocType',
	  'RRC.commisionIntermediary')
group by syp.syp_property
	, dt.dty_id
	, dt.dty_description
	, dt.dty_isdebit
;
commit;

create index acsele_alfa_bi.rs_cfg_1_idx_1
on acsele_alfa_bi.rs_cfg_1 (dty_id);

create index acsele_alfa_bi.rs_cfg_1_idx_2
on acsele_alfa_bi.rs_cfg_1 (concepto, ramo_key);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.rs_cfg_1
;


/* ========================================================================================== */
/* PARAMETRO 2 - Roles de cliente (propiedad clientRoles) para el pagador de la poliza, desde */
/* stst_systemproperty_opt                                                                    */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.rs_cfg_2;
commit;
*/

create table acsele_alfa_bi.rs_cfg_2
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  rl.rol_id
	, rl.rol_description
from acsele_alfa_bi.stst_systemproperty_opt syp
inner join acsele_alfa.stts_role rl
on rl.rol_description = syp.elemento
where syp.syp_property = 'clientRoles'
group by rl.rol_id
	, rl.rol_description
;
commit;

create index acsele_alfa_bi.rs_cfg_2_idx_1
on acsele_alfa_bi.rs_cfg_2 (rol_id);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.rs_cfg_2
;


/* ========================================================================================== */
/* PARAMETRO 3 - Conceptos de thirdpartymovementpolicy: prima de la operacion y porcentaje de */
/* gastos de expedicion, desde stst_systemproperty_opt                                        */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.rs_cfg_3;
commit;
*/

create table acsele_alfa_bi.rs_cfg_3
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  syp.elemento					as concept
	, (case
		when syp.syp_property in ('RRC.PremiumConceptName', 'RRC.PremiumDevConceptName') then 'PRIMA_OPERACION'
		else 'GASTOS_EXPEDICION'
	  end)							as tipo
from acsele_alfa_bi.stst_systemproperty_opt syp
where syp.syp_property in ('RRC.PremiumConceptName', 'RRC.PremiumDevConceptName', 'RRC.ExpensesCommiConceptName')
group by syp.elemento
	, syp.syp_property
;
commit;

create index acsele_alfa_bi.rs_cfg_3_idx_1
on acsele_alfa_bi.rs_cfg_3 (concept);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.rs_cfg_3
;


/* ========================================================================================== */
/* PARAMETRO 4 - Tipos de documento de los comisionistas (recaudo e intermediacion) para M4:  */
/* los mismos de las propiedades RRC.commissionCollection* y RRC.commisionIntermediary        */
/*                                                                                            */
/* Reemplaza las listas escritas a mano de los scripts de comisionistas: son los mismos       */
/* conceptos, tomados de la tabla de propiedades (rs_cfg_1).                                  */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.rs_cfg_4;
commit;
*/

create table acsele_alfa_bi.rs_cfg_4
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  (case when c.concepto = 'INTERMEDIACION' then 'COM_INTERMEDIACION' else 'COM_RECAUDO' end)		as concepto
	, c.dty_id
	, c.dty_description
from acsele_alfa_bi.rs_cfg_1 c
where c.concepto in ('RECAUDO', 'INTERMEDIACION')
group by (case when c.concepto = 'INTERMEDIACION' then 'COM_INTERMEDIACION' else 'COM_RECAUDO' end)
	, c.dty_id
	, c.dty_description
;
commit;

create index acsele_alfa_bi.rs_cfg_4_idx_1
on acsele_alfa_bi.rs_cfg_4 (dty_id);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.rs_cfg_4
;


/* ========================================================================================== */
/* PARAMETRO 1D - Tipos de documento distintos a extraer de openitem                          */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.rs_cfg_1d;
commit;
*/

create table acsele_alfa_bi.rs_cfg_1d
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  c.dty_id
from acsele_alfa_bi.rs_cfg_1 c
group by c.dty_id
;
commit;

create index acsele_alfa_bi.rs_cfg_1d_idx_1
on acsele_alfa_bi.rs_cfg_1d (dty_id);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.rs_cfg_1d
;


/* ========================================================================================== */
/* PASO 2 - REUTILIZA M2 (pc_1, pc_2): operaciones y poliza del universo MES + VIGENTE        */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.rs_op;
commit;
*/

create table acsele_alfa_bi.rs_op
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  p.operationpk
	, p.dcoid
	, p.agregatedobjectid
	, p.time_stamp
	, p.policy_begindate
	, p.policy_enddate
	, p.productid
	, p.prplc_static
	, p.numeropoliza				as policy_number
	, p.nom_sucursal				as office
	, p.des_tipo_poliza			as policytype_name
	, p.cod_tipo_poliza			as policytype_code
	, p.des_frecuencia_pago		as paymentfrequency_name
	, pre.cod_producto
	, pre.nom_producto			as desc_producto
	, pre.cod_ramo_tecnico
	, pre.nom_ramo_tecnico		as desc_ramo_tecnico
	, pre.nom_compania
	, p.universo
from acsele_alfa_bi.pc_1 p
inner join acsele_alfa_bi.pc_2 pre
on pre.pk = p.pro_dcoid
;
commit;

create index acsele_alfa_bi.rs_op_idx_1
on acsele_alfa_bi.rs_op (operationpk);

create index acsele_alfa_bi.rs_op_idx_2
on acsele_alfa_bi.rs_op (universo);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.rs_op
;


/* ========================================================================================== */
/* PASO 3A - REUTILIZA M2 (pc_3): operacion de reaseguro, un registro por operationid         */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.rs_ro;
commit;
*/

create table acsele_alfa_bi.rs_ro
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  pc3.cor_id
	, pc3.operationid
	, pc3.agregatedriskunitid
	, pc3.policyevent
	, pc3.totalpremium
	, min(pc3.universo)		as universo
from acsele_alfa_bi.pc_3 pc3
group by pc3.cor_id
	, pc3.operationid
	, pc3.agregatedriskunitid
	, pc3.policyevent
	, pc3.totalpremium
;
commit;

create index acsele_alfa_bi.rs_ro_idx_1
on acsele_alfa_bi.rs_ro (cor_id);

create index acsele_alfa_bi.rs_ro_idx_2
on acsele_alfa_bi.rs_ro (operationid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.rs_ro
;


/* ========================================================================================== */
/* PASO 3C - EXTRAE NUEVO: grupo y ramo de reaseguro (ri_reinsurancegroup,                    */
/* stri_reinsurancekind) de los grupos que trajo M2 (pc_6)                                    */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.rs_grp;
commit;
*/

create table acsele_alfa_bi.rs_grp
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  rg.reinsurancegroupid		as groupid
	, rg.rekind_id
	, rg.rig_companycode
	, rg.code					as reinsurancegroup_code
	, rg.description			as reinsurancegroup_description
	, rk.rek_code
	, rk.rek_description
from acsele_alfa_bi.pc_6 c
inner join acsele_alfa.ri_reinsurancegroup rg
on rg.reinsurancegroupid = c.groupid
inner join acsele_alfa.stri_reinsurancekind rk
on rk.rekind_id = rg.rekind_id
group by rg.reinsurancegroupid
	, rg.rekind_id
	, rg.rig_companycode
	, rg.code
	, rg.description
	, rk.rek_code
	, rk.rek_description
;
commit;

create index acsele_alfa_bi.rs_grp_idx_1
on acsele_alfa_bi.rs_grp (groupid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.rs_grp
;


/* ========================================================================================== */
/* PASO 3 - UNIVERSO: operaciones con totalpremium <> 0 y grupo de reaseguro de la compania   */
/* (los EXISTS del RRC), con su marca MES / VIGENTE                                           */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.rs_3;
commit;
*/

create table acsele_alfa_bi.rs_3
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  r.cor_id					as operationpk
	, g.rig_companycode			as companycode
	, min(r.universo)			as universo
from acsele_alfa_bi.rs_ro r
inner join acsele_alfa_bi.pc_6 c
on c.operationid = r.operationid
inner join acsele_alfa_bi.rs_grp g
on g.groupid = c.groupid
inner join acsele_alfa_bi.cfg_pcr_compania cc
on cc.companiavalue = g.rig_companycode
where r.totalpremium <> 0
group by r.cor_id
	, g.rig_companycode
;
commit;

create index acsele_alfa_bi.rs_3_idx_1
on acsele_alfa_bi.rs_3 (operationpk);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.rs_3
;


/* ========================================================================================== */
/* PASO 3O - Operaciones distintas del universo, con su marca                                 */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.rs_3o;
commit;
*/

create table acsele_alfa_bi.rs_3o
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  u.operationpk
	, min(u.universo)		as universo
from acsele_alfa_bi.rs_3 u
group by u.operationpk
;
commit;

create index acsele_alfa_bi.rs_3o_idx_1
on acsele_alfa_bi.rs_3o (operationpk);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.rs_3o
;


/* ========================================================================================== */
/* PASO 4 - Base de reaseguro del universo a nivel de componente (REUTILIZA M2 pc_6 + rs_ro,  */
/* rs_grp)                                                                                    */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.rs_4;
commit;
*/

create table acsele_alfa_bi.rs_4
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  ro.cor_id
	, ro.operationid
	, ro.agregatedriskunitid
	, ro.policyevent
	, ro.totalpremium
	, c.reinsurancegroupinfoid
	, c.premiumamount
	, c.reinopercomponentid
	, c.contracttype
	, c.roc_totalpremium
	, g.rekind_id
	, g.rig_companycode
	, g.reinsurancegroup_code
	, g.reinsurancegroup_description
	, g.rek_code
	, g.rek_description
	, ro.universo
from acsele_alfa_bi.rs_3 u
inner join acsele_alfa_bi.rs_ro ro
on ro.cor_id = u.operationpk
inner join acsele_alfa_bi.pc_6 c
on c.operationid = ro.operationid
inner join acsele_alfa_bi.rs_grp g
on g.groupid = c.groupid
and g.rig_companycode = u.companycode
;
commit;

create index acsele_alfa_bi.rs_4_idx_1
on acsele_alfa_bi.rs_4 (cor_id, agregatedriskunitid, rekind_id);

create index acsele_alfa_bi.rs_4_idx_2
on acsele_alfa_bi.rs_4 (reinopercomponentid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.rs_4
;


/* ========================================================================================== */
/* PASO 4G - Grupos de reaseguro (un registro por reinsurancegroupinfo) con al menos un       */
/* componente distinto de Retention, de operaciones distintas de Automatico                   */
/*                                                                                            */
/* Evita que la prima del grupo se sume una vez por cada componente al calcular GROUPPREMIUM. */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.rs_4g;
commit;
*/

create table acsele_alfa_bi.rs_4g
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  b.cor_id
	, b.agregatedriskunitid
	, b.rekind_id
	, b.rek_code
	, b.rek_description
	, b.rig_companycode
	, b.totalpremium
	, b.reinsurancegroupinfoid
	, b.premiumamount
	, b.reinsurancegroup_code
	, b.reinsurancegroup_description
from acsele_alfa_bi.rs_4 b
where b.contracttype <> 'Retention'
and b.policyevent <> 'Automatico'
and b.totalpremium <> 0
group by b.cor_id
	, b.agregatedriskunitid
	, b.rekind_id
	, b.rek_code
	, b.rek_description
	, b.rig_companycode
	, b.totalpremium
	, b.reinsurancegroupinfoid
	, b.premiumamount
	, b.reinsurancegroup_code
	, b.reinsurancegroup_description
;
commit;

create index acsele_alfa_bi.rs_4g_idx_1
on acsele_alfa_bi.rs_4g (cor_id);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.rs_4g
;


/* ========================================================================================== */
/* PASO 5 - LLAVE DEL BRUTO: operacion, unidad de riesgo y ramo de reaseguro, con prima del   */
/* ramo y porcentaje del ramo (GROUP_PERCENTAGE)                                              */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.rs_5;
commit;
*/

create table acsele_alfa_bi.rs_5
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  g.cor_id
	, g.agregatedriskunitid
	, g.rekind_id
	, g.rek_code
	, g.rek_description
	, g.rig_companycode
	, g.totalpremium
	, sum(g.premiumamount)										as grouppremium
	, abs((sum(g.premiumamount) * 100) / g.totalpremium)		as group_percentage
	, min(g.reinsurancegroup_code)								as reinsurancegroup_code
	, min(g.reinsurancegroup_description)						as reinsurancegroup_description
from acsele_alfa_bi.rs_4g g
group by g.cor_id
	, g.agregatedriskunitid
	, g.rekind_id
	, g.rek_code
	, g.rek_description
	, g.rig_companycode
	, g.totalpremium
;
commit;

create index acsele_alfa_bi.rs_5_idx_1
on acsele_alfa_bi.rs_5 (cor_id, agregatedriskunitid, rekind_id);

create index acsele_alfa_bi.rs_5_idx_2
on acsele_alfa_bi.rs_5 (cor_id);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.rs_5
;


/* ========================================================================================== */
/* PASO 6 - Retencion por ramo (componentes Retention)                                        */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.rs_6;
commit;
*/

create table acsele_alfa_bi.rs_6
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  b.cor_id
	, b.agregatedriskunitid
	, b.rekind_id
	, sum(b.roc_totalpremium)		as retained_premium
from acsele_alfa_bi.rs_4 b
where b.contracttype = 'Retention'
group by b.cor_id
	, b.agregatedriskunitid
	, b.rekind_id
;
commit;

create index acsele_alfa_bi.rs_6_idx_1
on acsele_alfa_bi.rs_6 (cor_id, agregatedriskunitid, rekind_id);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.rs_6
;


/* ========================================================================================== */
/* PASO 7A - REUTILIZA M2 (pc_opacu, pc_acu): acuerdo y poliza matriz por operacion           */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.rs_acu;
commit;
*/

create table acsele_alfa_bi.rs_acu
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  a.operationpk
	, acu.ast_id
	, acu.ast_numeroacu
	, acu.ast_nombreacu
from acsele_alfa_bi.pc_opacu a
inner join acsele_alfa_bi.rs_3o u
on u.operationpk = a.operationpk
inner join acsele_alfa_bi.pc_acu acu
on acu.ast_id = a.ast_id
group by a.operationpk
	, acu.ast_id
	, acu.ast_numeroacu
	, acu.ast_nombreacu
;
commit;

create index acsele_alfa_bi.rs_acu_idx_1
on acsele_alfa_bi.rs_acu (operationpk);

create index acsele_alfa_bi.rs_acu_idx_2
on acsele_alfa_bi.rs_acu (ast_id);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.rs_acu
;


/* ========================================================================================== */
/* PASO 7 - Informacion de la poliza (REUTILIZA rs_op) + moneda (EXTRAE NUEVO                 */
/* financialplandco) + poliza matriz (rs_acu)                                                 */
/*                                                                                            */
/* Un registro por operacion (minimo de cada atributo).                                       */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.rs_7;
commit;
*/

create table acsele_alfa_bi.rs_7
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  o.operationpk
	, min(o.dcoid)		as dcoid
	, min(o.agregatedobjectid)		as agregatedobjectid
	, min(o.time_stamp)		as time_stamp
	, min(o.policy_begindate)		as policy_begindate
	, min(o.policy_enddate)		as policy_enddate
	, min(o.productid)		as productid
	, min(o.prplc_static)		as prplc_static
	, min(o.policy_number)		as policy_number
	, min(o.office)		as office
	, min(o.policytype_name)		as policytype_name
	, min(o.policytype_code)		as policytype_code
	, min(o.paymentfrequency_name)		as paymentfrequency_name
	, min(o.cod_producto)		as cod_producto
	, min(o.desc_producto)		as desc_producto
	, min(o.cod_ramo_tecnico)		as cod_ramo_tecnico
	, min(o.desc_ramo_tecnico)		as desc_ramo_tecnico
	, min(o.nom_compania)		as nom_compania
	, min(o.time_stamp)				as endorsement_date
	, min(fp.ccy_id)				as currency_id
	, min(acu.ast_id)				as cod_acuerdo
	, min(acu.ast_numeroacu)		as poliza_matriz
	, min(acu.ast_nombreacu)		as des_acuerdo
from acsele_alfa_bi.rs_3o u
inner join acsele_alfa_bi.rs_op o
on o.operationpk = u.operationpk
inner join acsele_alfa.financialplandco fp
on fp.agregatedparentid = o.agregatedobjectid
and fp.operationpk = o.operationpk
left join acsele_alfa_bi.rs_acu acu
on acu.operationpk = o.operationpk
group by o.operationpk
;
commit;

create index acsele_alfa_bi.rs_7_idx_1
on acsele_alfa_bi.rs_7 (operationpk);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.rs_7
;


/* ========================================================================================== */
/* PASO 8A - REUTILIZA M1 (em_tpb): participantes de la poliza del universo                   */
/*                                                                                            */
/* Publica para M4 (porcentajes de participacion y datos del comisionista).                   */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.rs_ppt;
commit;
*/

create table acsele_alfa_bi.rs_ppt
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  t.plcdco_operationpk						as operationpk
	, t.stpplcprtcptn_thirdpartyid				as thirdpartyid
	, t.stpplcprtcptn_rol_id					as rol_id
	, t.stpplcprtcptndco_dcoid					as dcoid
	, t.prthrdprty_pk							as prethirdparty_pk
	, t.prthrdprty_tipodocumentoinput			as tipodocumento
	, t.prthrdprty_documentoidentidadinput		as documento
	, t.prthrdprty_nombrepredeterminadoinput	as nombre
from acsele_alfa_bi.em_tpb t
inner join acsele_alfa_bi.rs_3o u
on u.operationpk = t.plcdco_operationpk
group by t.plcdco_operationpk
	, t.stpplcprtcptn_thirdpartyid
	, t.stpplcprtcptn_rol_id
	, t.stpplcprtcptndco_dcoid
	, t.prthrdprty_pk
	, t.prthrdprty_tipodocumentoinput
	, t.prthrdprty_documentoidentidadinput
	, t.prthrdprty_nombrepredeterminadoinput
;
commit;

create index acsele_alfa_bi.rs_ppt_idx_1
on acsele_alfa_bi.rs_ppt (operationpk, thirdpartyid, rol_id);

create index acsele_alfa_bi.rs_ppt_idx_2
on acsele_alfa_bi.rs_ppt (dcoid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.rs_ppt
;


/* ========================================================================================== */
/* PASO 8 - Pagadores de la poliza (roles de cliente, rs_cfg_2) y grupo empresarial (EXTRAE   */
/* NUEVO personajuridica)                                                                     */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.rs_8;
commit;
*/

create table acsele_alfa_bi.rs_8
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  p.operationpk
	, p.thirdpartyid					as thirdparty_id
	, p.nombre							as thirdparty_name
	, pj.grupoempresarialrelacinput		as grupo_empresarial
from acsele_alfa_bi.rs_ppt p
inner join acsele_alfa_bi.rs_cfg_2 rl
on rl.rol_id = p.rol_id
left join acsele_alfa.personajuridica pj
on pj.pk = p.prethirdparty_pk
group by p.operationpk
	, p.thirdpartyid
	, p.nombre
	, pj.grupoempresarialrelacinput
;
commit;

create index acsele_alfa_bi.rs_8_idx_1
on acsele_alfa_bi.rs_8 (operationpk, thirdparty_id);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.rs_8
;


/* ========================================================================================== */
/* PASO 9A - Un pagador por operacion: el de menor tpt_id                                     */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.rs_9a;
commit;
*/

create table acsele_alfa_bi.rs_9a
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  p.operationpk
	, min(p.thirdparty_id)		as thirdparty_id
from acsele_alfa_bi.rs_8 p
group by p.operationpk
;
commit;

create index acsele_alfa_bi.rs_9a_idx_1
on acsele_alfa_bi.rs_9a (operationpk, thirdparty_id);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.rs_9a
;


/* ========================================================================================== */
/* PASO 9 - Pagador elegido con su nombre y grupo empresarial                                 */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.rs_9;
commit;
*/

create table acsele_alfa_bi.rs_9
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  a.operationpk
	, a.thirdparty_id
	, min(p.thirdparty_name)		as thirdparty_name
	, min(p.grupo_empresarial)		as grupo_empresarial
from acsele_alfa_bi.rs_9a a
inner join acsele_alfa_bi.rs_8 p
on p.operationpk = a.operationpk
and p.thirdparty_id = a.thirdparty_id
group by a.operationpk
	, a.thirdparty_id
;
commit;

create index acsele_alfa_bi.rs_9_idx_1
on acsele_alfa_bi.rs_9 (operationpk);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.rs_9
;


/* ========================================================================================== */
/* PASO 10A - EXTRAE NUEVO: porcentajes del intermediario (participacionintermediarios) de    */
/* los participantes del universo                                                             */
/*                                                                                            */
/* Publica para M4.                                                                           */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.rs_pin;
commit;
*/

create table acsele_alfa_bi.rs_pin
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  pi.pk								as dcoid
	, pi.porcentajecomisioninput
	, pi.porcentajeparticipacinput
from acsele_alfa_bi.rs_ppt p
inner join acsele_alfa.participacionintermediarios pi
on pi.pk = p.dcoid
group by pi.pk
	, pi.porcentajecomisioninput
	, pi.porcentajeparticipacinput
;
commit;

create index acsele_alfa_bi.rs_pin_idx_1
on acsele_alfa_bi.rs_pin (dcoid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.rs_pin
;


/* ========================================================================================== */
/* PASO 10 - Porcentaje de comision del intermediario por operacion (suma de los              */
/* intermediarios)                                                                            */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.rs_10;
commit;
*/

create table acsele_alfa_bi.rs_10
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  p.operationpk
	, sum(pi.porcentajecomisioninput)		as inter_commission_pct
from acsele_alfa_bi.rs_ppt p
inner join acsele_alfa_bi.rs_pin pi
on pi.dcoid = p.dcoid
group by p.operationpk
;
commit;

create index acsele_alfa_bi.rs_10_idx_1
on acsele_alfa_bi.rs_10 (operationpk);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.rs_10
;


/* ========================================================================================== */
/* PASO 11 - EXTRAE NUEVO: partidas (openitem) del universo con los tipos de documento del    */
/* RRC y de los scripts de comisionistas                                                      */
/*                                                                                            */
/* Publica para M4: el tercero, el rol y la descripcion del tipo de documento.                */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.rs_11;
commit;
*/

create table acsele_alfa_bi.rs_11
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  oi.operationpk
	, oi.openitemid
	, oi.dty_id
	, dt.dty_description
	, dt.dty_isdebit
	, oi.amount
	, oi.thirdpartyid
	, oi.rol_id
from acsele_alfa_bi.rs_3o u
inner join acsele_alfa.openitem oi
on oi.operationpk = u.operationpk
inner join acsele_alfa_bi.rs_cfg_1d d
on d.dty_id = oi.dty_id
inner join acsele_alfa.stca_doctype dt
on dt.dty_id = oi.dty_id
where oi.opm_substatus <> 'notApplied'
;
commit;

create index acsele_alfa_bi.rs_11_idx_1
on acsele_alfa_bi.rs_11 (operationpk);

create index acsele_alfa_bi.rs_11_idx_2
on acsele_alfa_bi.rs_11 (openitemid);

create index acsele_alfa_bi.rs_11_idx_3
on acsele_alfa_bi.rs_11 (dty_id);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.rs_11
;


/* ========================================================================================== */
/* PASO 12 - Conceptos por operacion y ramo: IVA, comision de recaudo, prima asegurada e      */
/* impuesto de recaudo (signo segun dty_isdebit, como el RRC)                                 */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.rs_12;
commit;
*/

create table acsele_alfa_bi.rs_12
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  oi.operationpk
	, c.ramo_key
	, sum(case when c.concepto = 'IVA' then (case when oi.dty_isdebit = 1 then oi.amount else oi.amount * (-1) end) else 0 end)				as iva
	, sum(case when c.concepto = 'RECAUDO' then (case when oi.dty_isdebit = 0 then oi.amount else oi.amount * (-1) end) else 0 end)			as commissioncollection
	, sum(case when c.concepto = 'PRIMA_ASEGURADA' then (case when oi.dty_isdebit = 1 then oi.amount else oi.amount * (-1) end) else 0 end)	as issuedpremium
	, sum(case when c.concepto = 'IMP_RECAUDO' then (case when oi.dty_isdebit = 0 then oi.amount else oi.amount * (-1) end) else 0 end)		as taxcommissioncollection
from acsele_alfa_bi.rs_11 oi
inner join acsele_alfa_bi.rs_cfg_1 c
on c.dty_id = oi.dty_id
where c.concepto in ('IVA', 'RECAUDO', 'PRIMA_ASEGURADA', 'IMP_RECAUDO')
group by oi.operationpk
	, c.ramo_key
;
commit;

create index acsele_alfa_bi.rs_12_idx_1
on acsele_alfa_bi.rs_12 (operationpk, ramo_key);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.rs_12
;


/* ========================================================================================== */
/* PASO 13A - EXTRAE NUEVO: valores de comision e IVA de las partidas de intermediacion       */
/* (uaadetail, property)                                                                      */
/*                                                                                            */
/* Tipos de documento de intermediacion del RRC (rs_cfg_1) y del script (rs_cfg_4). Publica   */
/* para M4.                                                                                   */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.rs_uaa;
commit;
*/

create table acsele_alfa_bi.rs_uaa
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  oi.openitemid
	, pr.simbolo
	, sum(det.value)		as value
from acsele_alfa_bi.rs_11 oi
inner join acsele_alfa.uaadetail det
on det.openitemid = oi.openitemid
inner join acsele_alfa.property pr
on pr.propertyid = det.property
left join acsele_alfa_bi.rs_cfg_1 c1
on c1.dty_id = oi.dty_id
and c1.concepto = 'INTERMEDIACION'
left join acsele_alfa_bi.rs_cfg_4 c4
on c4.dty_id = oi.dty_id
and c4.concepto = 'COM_INTERMEDIACION'
where pr.simbolo in ('Comision', 'IvaComision')
and (c1.dty_id is not null or c4.dty_id is not null)
group by oi.openitemid
	, pr.simbolo
;
commit;

create index acsele_alfa_bi.rs_uaa_idx_1
on acsele_alfa_bi.rs_uaa (openitemid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.rs_uaa
;


/* ========================================================================================== */
/* PASO 13 - Comision de intermediacion y su impuesto por operacion (tipos de documento del   */
/* RRC)                                                                                       */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.rs_13;
commit;
*/

create table acsele_alfa_bi.rs_13
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  oi.operationpk
	, sum(case when u.simbolo = 'Comision' then u.value else 0 end)		as commision_intermediary
	, sum(case when u.simbolo = 'IvaComision' then u.value else 0 end)	as taxcommision_intermediary
from acsele_alfa_bi.rs_11 oi
inner join acsele_alfa_bi.rs_cfg_1 c
on c.dty_id = oi.dty_id
inner join acsele_alfa_bi.rs_uaa u
on u.openitemid = oi.openitemid
where c.concepto = 'INTERMEDIACION'
group by oi.operationpk
;
commit;

create index acsele_alfa_bi.rs_13_idx_1
on acsele_alfa_bi.rs_13 (operationpk);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.rs_13
;


/* ========================================================================================== */
/* PASO 14A - REUTILIZA M1 (em_tmb): movimientos de la poliza (prima de la operacion y gastos */
/* de expedicion)                                                                             */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.rs_tpm;
commit;
*/

create table acsele_alfa_bi.rs_tpm
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  t.thrdprtmvmntplc_operationpk		as operationpk
	, t.thrdprtmvmntplc_concept			as concept
	, t.thrdprtmvmntplc_ammount			as ammount
from acsele_alfa_bi.em_tmb t
inner join acsele_alfa_bi.rs_3o u
on u.operationpk = t.thrdprtmvmntplc_operationpk
inner join acsele_alfa_bi.rs_cfg_3 c
on c.concept = t.thrdprtmvmntplc_concept
;
commit;

create index acsele_alfa_bi.rs_tpm_idx_1
on acsele_alfa_bi.rs_tpm (operationpk);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.rs_tpm
;


/* ========================================================================================== */
/* PASO 14 - Prima de la operacion y porcentaje de gastos de expedicion                       */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.rs_14;
commit;
*/

create table acsele_alfa_bi.rs_14
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  t.operationpk
	, sum(case when c.tipo = 'PRIMA_OPERACION' then t.ammount else 0 end)		as operation_premium
	, max(case when c.tipo = 'GASTOS_EXPEDICION' then t.ammount end)			as expenses_commission_percentage
from acsele_alfa_bi.rs_tpm t
inner join acsele_alfa_bi.rs_cfg_3 c
on c.concept = t.concept
group by t.operationpk
;
commit;

create index acsele_alfa_bi.rs_14_idx_1
on acsele_alfa_bi.rs_14 (operationpk);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.rs_14
;


/* ========================================================================================== */
/* PASO 15 - Unidad de riesgo de cada bruto (REUTILIZA M2 pc_1r + EXTRAE NUEVO preriskunits)  */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.rs_15;
commit;
*/

create table acsele_alfa_bi.rs_15
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  k.cor_id
	, k.agregatedriskunitid
	, min(pru.numunidriesgoinput)		as riskunit_number
	, min(ur.initialdate)				as riskunit_begindate
	, max(ur.finishdate)				as riskunit_enddate
from acsele_alfa_bi.rs_5 k
inner join acsele_alfa_bi.pc_1r ur
on ur.agregatedobjectid = k.agregatedriskunitid
and ur.operationpk = k.cor_id
inner join acsele_alfa.preriskunits pru
on pru.pk = ur.dcoid
group by k.cor_id
	, k.agregatedriskunitid
;
commit;

create index acsele_alfa_bi.rs_15_idx_1
on acsele_alfa_bi.rs_15 (cor_id, agregatedriskunitid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.rs_15
;


/* ========================================================================================== */
/* PASO 16 - Unidad de negocio vigente a la fecha fin de la unidad de riesgo (REUTILIZA M2    */
/* pc_10, rs_acu)                                                                             */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.rs_16;
commit;
*/

create table acsele_alfa_bi.rs_16
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  ur.cor_id
	, ur.agregatedriskunitid
	, min(v.des_unidad_negocio)		as businessunit_name
from acsele_alfa_bi.rs_15 ur
inner join acsele_alfa_bi.rs_acu a
on a.operationpk = ur.cor_id
inner join acsele_alfa_bi.pc_10 v
on v.ast_id = a.ast_id
and ur.riskunit_enddate between v.fe_inicio_acuerdo and v.fe_fin_acuerdo
group by ur.cor_id
	, ur.agregatedriskunitid
;
commit;

create index acsele_alfa_bi.rs_16_idx_1
on acsele_alfa_bi.rs_16 (cor_id, agregatedriskunitid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.rs_16
;


/* ========================================================================================== */
/* PASO 17 - REUTILIZA M2 (pc_8): ramo contable de las coberturas de la operacion             */
/*                                                                                            */
/* Si la operacion tiene un solo ramo contable se usa ese en ACCOUNTINGBRANCHCOD; si tiene    */
/* varios, la regla del RRC.                                                                  */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.rs_17;
commit;
*/

create table acsele_alfa_bi.rs_17
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  c.operationpk
	, min(c.cod_ramo_contable)		as ramo_contable_min
	, max(c.cod_ramo_contable)		as ramo_contable_max
from acsele_alfa_bi.pc_8 c
inner join acsele_alfa_bi.rs_3o u
on u.operationpk = c.operationpk
group by c.operationpk
;
commit;

create index acsele_alfa_bi.rs_17_idx_1
on acsele_alfa_bi.rs_17 (operationpk);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.rs_17
;


/* ========================================================================================== */
/* PASO 18 - Prima por reasegurador por bruto y tipo de contrato (REUTILIZA M2 pc_7d)         */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.rs_18;
commit;
*/

create table acsele_alfa_bi.rs_18
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  k.cor_id
	, k.agregatedriskunitid
	, k.rekind_id
	, b.contracttype
	, d.thirdpartyid
	, d.thirdpartyname
	, d.participationid
	, sum(d.rod_proratedpremium)		as detail_premium
from acsele_alfa_bi.rs_5 k
inner join acsele_alfa_bi.rs_4 b
on b.cor_id = k.cor_id
and b.agregatedriskunitid = k.agregatedriskunitid
and b.rekind_id = k.rekind_id
inner join acsele_alfa_bi.pc_7d d
on d.reinopercomponentid = b.reinopercomponentid
where b.contracttype <> 'Retention'
group by k.cor_id
	, k.agregatedriskunitid
	, k.rekind_id
	, b.contracttype
	, d.thirdpartyid
	, d.thirdpartyname
	, d.participationid
;
commit;

create index acsele_alfa_bi.rs_18_idx_1
on acsele_alfa_bi.rs_18 (cor_id, agregatedriskunitid, rekind_id);

create index acsele_alfa_bi.rs_18_idx_2
on acsele_alfa_bi.rs_18 (thirdpartyid);

create index acsele_alfa_bi.rs_18_idx_3
on acsele_alfa_bi.rs_18 (participationid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.rs_18
;


/* ========================================================================================== */
/* PASO 19 - Comision del reasegurador por bruto, tipo de contrato y tercero (REUTILIZA M2    */
/* pc_7d, pc_9: ComPriCedida / ComPriCedidaInd)                                               */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.rs_19;
commit;
*/

create table acsele_alfa_bi.rs_19
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  k.cor_id
	, k.agregatedriskunitid
	, k.rekind_id
	, b.contracttype
	, d.thirdpartyid
	, sum(p.ropp_value)		as reinsurance_commission
from acsele_alfa_bi.rs_5 k
inner join acsele_alfa_bi.rs_4 b
on b.cor_id = k.cor_id
and b.agregatedriskunitid = k.agregatedriskunitid
and b.rekind_id = k.rekind_id
inner join acsele_alfa_bi.pc_7d d
on d.reinopercomponentid = b.reinopercomponentid
inner join acsele_alfa_bi.pc_9 p
on p.rod_id = d.reinsuranceoperationdetailid
where b.contracttype <> 'Retention'
and p.ropp_name in ('ComPriCedida', 'ComPriCedidaInd')
group by k.cor_id
	, k.agregatedriskunitid
	, k.rekind_id
	, b.contracttype
	, d.thirdpartyid
;
commit;

create index acsele_alfa_bi.rs_19_idx_1
on acsele_alfa_bi.rs_19 (cor_id, agregatedriskunitid, rekind_id, contracttype, thirdpartyid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.rs_19
;


/* ========================================================================================== */
/* PASO 20 - Clase y codigo del reasegurador (REUTILIZA M2 pc_7r, todos los roles)            */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.rs_20;
commit;
*/

create table acsele_alfa_bi.rs_20
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  d.thirdpartyid
	, min(r.clasereaseguroinput)			as clase_reaseguro
	, min(r.codigoreaseguradorinput)		as codigo_reasegurador
from acsele_alfa_bi.rs_18 d
inner join acsele_alfa_bi.pc_7r r
on r.thirdpartyid = d.thirdpartyid
group by d.thirdpartyid
;
commit;

create index acsele_alfa_bi.rs_20_idx_1
on acsele_alfa_bi.rs_20 (thirdpartyid);

create index acsele_alfa_bi.rs_20_idx_2
on acsele_alfa_bi.rs_20 (codigo_reasegurador);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.rs_20
;


/* ========================================================================================== */
/* PASO 21 - Participacion e identificacion del reasegurador (REUTILIZA M2 pc_7p, pc_11)      */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.rs_21;
commit;
*/

create table acsele_alfa_bi.rs_21
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  par.participationid
	, min(par.percentage)					as reinsurer_part_pct
	, min(pte.num_documento_tercero)		as reinsurer_documentoidentidad
from acsele_alfa_bi.rs_18 d
inner join acsele_alfa_bi.pc_7p par
on par.participationid = d.participationid
left join acsele_alfa_bi.pc_11 pte
on pte.static = par.par_thirdpartyid
group by par.participationid
;
commit;

create index acsele_alfa_bi.rs_21_idx_1
on acsele_alfa_bi.rs_21 (participationid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.rs_21
;


/* ========================================================================================== */
/* PASO 22 - Fecha de la calificacion de riesgo vigente al corte por reasegurador             */
/* (tdcalificacionriesgo)                                                                     */
/*                                                                                            */
/* Reemplaza el ORDER BY fecha DESC del RRC: se toma la ultima fecha menor o igual a la fecha */
/* fin.                                                                                       */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.rs_22;
commit;
*/

create table acsele_alfa_bi.rs_22
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  r.codigo_reasegurador
	, max(to_date(td.fechainput, 'dd-mm-yyyy'))		as fecha_calificacion
from acsele_alfa_bi.rs_20 r
cross join acsele_alfa_bi.cfg_pcr_periodo per
inner join acsele_alfa.tdcalificacionriesgo td
on td.codigoreaseguradorinput = r.codigo_reasegurador
where to_date(td.fechainput, 'dd-mm-yyyy') <= per.fecha_fin
group by r.codigo_reasegurador
;
commit;

create index acsele_alfa_bi.rs_22_idx_1
on acsele_alfa_bi.rs_22 (codigo_reasegurador);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.rs_22
;


/* ========================================================================================== */
/* PASO 23 - Calificacion vigente del reasegurador                                            */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.rs_23;
commit;
*/

create table acsele_alfa_bi.rs_23
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  f.codigo_reasegurador
	, min(td.calificadorariesgoinput)		as calificadorariesgo
	, min(td.calificacionriesgoinput)		as calificacionriesgo
	, to_number(min(td.porcentajecalifinput))		as porcentajecalificacion
from acsele_alfa_bi.rs_22 f
inner join acsele_alfa.tdcalificacionriesgo td
on td.codigoreaseguradorinput = f.codigo_reasegurador
and to_date(td.fechainput, 'dd-mm-yyyy') = f.fecha_calificacion
group by f.codigo_reasegurador
;
commit;

create index acsele_alfa_bi.rs_23_idx_1
on acsele_alfa_bi.rs_23 (codigo_reasegurador);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.rs_23
;


/* ========================================================================================== */
/* PASO 24 - DATOS COMUNES DEL BRUTO: un registro por operacion, unidad de riesgo y ramo de   */
/* reaseguro, con los datos comunes de EXT_RISKRESERVEALFA, el FRN y los conceptos del ramo   */
/*                                                                                            */
/* ACCOUNTINGBRANCHCOD: ramo contable de la cobertura cuando es unico en la operacion; si no, */
/* regla del RRC (ramo 22 y tipo de poliza 2.0 -> 40). accountingbranchcod_rrc conserva el    */
/* valor del RRC para comparar.                                                               */
/* FRNCMONTH: el RRC lo define en 0.5 sin condicion; aqui aplica solo a frecuencia Mensual    */
/* (si no, FRNC siempre seria 0.5). Punto a validar con Consis.                               */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.rs_24;
commit;
*/

create table acsele_alfa_bi.rs_24
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  u.universo		as universo
	, per.courtmonth		as courtmonth
	, per.courtyear		as courtyear
	, 0		as courtstate
	, pol.currency_id		as ccy_id
	, k.cor_id		as cor_id
	, k.rig_companycode		as companycode
	, pol.nom_compania		as nom_compania
	, pol.office		as branchcode
	, pol.policy_number		as policynumber
	, 0		as policyrenewnum
	, k.agregatedriskunitid		as riskunit_id
	, ur.riskunit_number		as riskunitnum
	, 0		as coveragenum
	, cast(null as varchar2(170))		as coveragedesc
	, k.cor_id		as endorsementnum
	, ur.riskunit_begindate		as coveragestartdate
	, ur.riskunit_enddate		as coverageenddate
	, pol.endorsement_date		as endorsementdate
	, pag.thirdparty_id		as tpt_id
	, pag.thirdparty_name		as third_desc
	, pag.grupo_empresarial		as economicsector
	, pol.productid		as pro_id
	, pol.cod_producto		as cod_producto
	, pol.desc_producto		as desc_producto
	, pol.cod_ramo_tecnico		as cod_ramo_tecnico
	, pol.desc_ramo_tecnico		as desc_ramo_tecnico
	, pol.cod_acuerdo		as cod_acuerdo
	, pol.poliza_matriz		as poliza_matriz
	, pol.des_acuerdo		as des_acuerdo
	, 1		as coinsurancetype
	, (case when ur.riskunit_enddate > per.fecha_fin then 1 else 0 end)		as validpolicyincator
	, (ur.riskunit_enddate - ur.riskunit_begindate)		as validitydays
	, pol.endorsement_date		as systemincomedate
	, k.reinsurancegroup_code		as branchreinsurancecod
	, k.reinsurancegroup_description		as branchreinsurancedesc
	, un.businessunit_name		as businessunit
	, k.rekind_id		as rekind_id
	, k.rek_code		as groupreinsurancecod
	, k.rek_description		as groupreinsurancedesc
	, (case when k.rek_code = 22 and pol.policytype_code = '2.0' then 40 else k.rek_code end)		as accountingbranchcod_rrc
	, rc.ramo_contable_min		as ramo_contable_min
	, rc.ramo_contable_max		as ramo_contable_max
	, (case when rc.ramo_contable_min = rc.ramo_contable_max then rc.ramo_contable_min when k.rek_code = 22 and pol.policytype_code = '2.0' then 40 else k.rek_code end)		as accountingbranchcod
	, pol.policytype_code		as policytype_code
	, pol.paymentfrequency_name		as paymentfrequency
	, pol.policy_begindate		as policyinitialdate
	, pol.policy_enddate		as policyfinishdate
	, per.fecha_fin		as courtdate
	, cast(null as number)		as cov_id
	, sysdate		as processenddate
	, per.courtyear		as yearrated
	, (case
		when pol.paymentfrequency_name = 'Mensual' then 0
		when (per.fecha_fin - ur.riskunit_begindate) > 0 then (ur.riskunit_enddate - per.fecha_fin) / nullif(ur.riskunit_enddate - ur.riskunit_begindate, 0)
		else 1
	  end)		as frncnomonth
	, (case when pol.paymentfrequency_name = 'Mensual' then 0.5 else 0 end)		as frncmonth
	, (case when (case when pol.paymentfrequency_name = 'Mensual' then 0.5 else 0 end) > 0 then (case when pol.paymentfrequency_name = 'Mensual' then 0.5 else 0 end) else (case
		when pol.paymentfrequency_name = 'Mensual' then 0
		when (per.fecha_fin - ur.riskunit_begindate) > 0 then (ur.riskunit_enddate - per.fecha_fin) / nullif(ur.riskunit_enddate - ur.riskunit_begindate, 0)
		else 1
	  end) end)		as frnc
	, 0		as accepcoinscommipercent
	, k.totalpremium		as totalpremium
	, k.grouppremium		as grouppremium
	, k.group_percentage		as group_percentage
	, mov.operation_premium		as operation_premium
	, pin.inter_commission_pct		as intercommipercentage
	, mov.expenses_commission_percentage		as expensescommipercentage
	, nvl(itm.commision_intermediary, 0)		as commision_intermediary
	, nvl(itm.taxcommision_intermediary, 0)		as taxbrokeragecommission
	, nvl(con.iva, 0)		as iva
	, nvl(con.commissioncollection, 0)		as commissioncollection
	, nvl(con.issuedpremium, 0)		as issuedpremium
	, nvl(con.taxcommissioncollection, 0)		as taxcommissioncollection
	, nvl(ret.retained_premium, 0)		as retainedpremium
from acsele_alfa_bi.rs_5 k
cross join acsele_alfa_bi.cfg_pcr_periodo per
inner join acsele_alfa_bi.rs_3 u
on u.operationpk = k.cor_id
and u.companycode = k.rig_companycode
inner join acsele_alfa_bi.rs_7 pol
on pol.operationpk = k.cor_id
inner join acsele_alfa_bi.rs_15 ur
on ur.cor_id = k.cor_id
and ur.agregatedriskunitid = k.agregatedriskunitid
left join acsele_alfa_bi.rs_9 pag
on pag.operationpk = k.cor_id
left join acsele_alfa_bi.rs_10 pin
on pin.operationpk = k.cor_id
left join acsele_alfa_bi.rs_12 con
on con.operationpk = k.cor_id
and con.ramo_key = (case when lpad(to_char(k.rek_code), 2, '0') in ('05','13','10','12','01','24') then lpad(to_char(k.rek_code), 2, '0') else 'DEF' end)
left join acsele_alfa_bi.rs_13 itm
on itm.operationpk = k.cor_id
left join acsele_alfa_bi.rs_14 mov
on mov.operationpk = k.cor_id
left join acsele_alfa_bi.rs_6 ret
on ret.cor_id = k.cor_id
and ret.agregatedriskunitid = k.agregatedriskunitid
and ret.rekind_id = k.rekind_id
left join acsele_alfa_bi.rs_16 un
on un.cor_id = k.cor_id
and un.agregatedriskunitid = k.agregatedriskunitid
left join acsele_alfa_bi.rs_17 rc
on rc.operationpk = k.cor_id
;
commit;

create index acsele_alfa_bi.rs_24_idx_1
on acsele_alfa_bi.rs_24 (cor_id, riskunit_id, rekind_id);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.rs_24
;


/* ========================================================================================== */
/* PASO 25 - DETALLE: un registro por reasegurador dentro de cada bruto y tipo de contrato,   */
/* con su comision, rol, participacion y calificacion                                         */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.rs_25;
commit;
*/

create table acsele_alfa_bi.rs_25
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  c.universo
	, c.courtmonth
	, c.courtyear
	, c.courtstate
	, c.ccy_id
	, c.cor_id
	, c.companycode
	, c.nom_compania
	, c.branchcode
	, c.policynumber
	, c.policyrenewnum
	, c.riskunit_id
	, c.riskunitnum
	, c.coveragenum
	, c.coveragedesc
	, c.endorsementnum
	, c.coveragestartdate
	, c.coverageenddate
	, c.endorsementdate
	, c.tpt_id
	, c.third_desc
	, c.economicsector
	, c.pro_id
	, c.cod_producto
	, c.desc_producto
	, c.cod_ramo_tecnico
	, c.desc_ramo_tecnico
	, c.cod_acuerdo
	, c.poliza_matriz
	, c.des_acuerdo
	, c.coinsurancetype
	, c.validpolicyincator
	, c.validitydays
	, c.systemincomedate
	, c.branchreinsurancecod
	, c.branchreinsurancedesc
	, c.businessunit
	, c.rekind_id
	, c.groupreinsurancecod
	, c.groupreinsurancedesc
	, c.accountingbranchcod_rrc
	, c.ramo_contable_min
	, c.ramo_contable_max
	, c.accountingbranchcod
	, c.policytype_code
	, c.paymentfrequency
	, c.policyinitialdate
	, c.policyfinishdate
	, c.courtdate
	, c.cov_id
	, c.processenddate
	, c.yearrated
	, c.frncnomonth
	, c.frncmonth
	, c.frnc
	, c.accepcoinscommipercent
	, c.totalpremium
	, c.grouppremium
	, c.group_percentage
	, c.operation_premium
	, c.intercommipercentage
	, c.expensescommipercentage
	, c.commision_intermediary
	, c.taxbrokeragecommission
	, c.iva
	, c.commissioncollection
	, c.issuedpremium
	, c.taxcommissioncollection
	, c.retainedpremium
	, d.contracttype		as contracttype
	, d.thirdpartyid		as thirdpartyid
	, d.thirdpartyname		as thirdpartyname
	, d.participationid		as participationid
	, d.detail_premium		as detail_premium
	, nvl(dc.reinsurance_commission, 0)		as reinsurance_commission
	, rol.clase_reaseguro		as clase_reaseguro
	, rol.codigo_reasegurador		as codigo_reasegurador
	, inf.reinsurer_part_pct		as reinsurer_part_pct
	, inf.reinsurer_documentoidentidad		as reinsurer_documentoidentidad
	, cal.calificadorariesgo		as calificadorariesgo
	, cal.calificacionriesgo		as calificacionriesgo
	, cal.porcentajecalificacion		as porcentajecalificacion
from acsele_alfa_bi.rs_24 c
inner join acsele_alfa_bi.rs_18 d
on d.cor_id = c.cor_id
and d.agregatedriskunitid = c.riskunit_id
and d.rekind_id = c.rekind_id
left join acsele_alfa_bi.rs_19 dc
on dc.cor_id = d.cor_id
and dc.agregatedriskunitid = d.agregatedriskunitid
and dc.rekind_id = d.rekind_id
and dc.contracttype = d.contracttype
and dc.thirdpartyid = d.thirdpartyid
left join acsele_alfa_bi.rs_20 rol
on rol.thirdpartyid = d.thirdpartyid
left join acsele_alfa_bi.rs_21 inf
on inf.participationid = d.participationid
left join acsele_alfa_bi.rs_23 cal
on cal.codigo_reasegurador = rol.codigo_reasegurador
;
commit;

create index acsele_alfa_bi.rs_25_idx_1
on acsele_alfa_bi.rs_25 (cor_id, riskunit_id, rekind_id);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.rs_25
;


/* ========================================================================================== */
/* PASO 26 - Sumatorias del detalle por bruto (para REINSURANCECOMMISSION,                    */
/* REINSURANCECESSIONPREMIUM y NETRESERVE)                                                    */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.rs_26;
commit;
*/

create table acsele_alfa_bi.rs_26
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  t.cor_id
	, t.riskunit_id
	, t.rekind_id
	, sum(t.detail_premium)				as sum_detail_premium
	, sum(t.reinsurance_commission)		as sum_reinsurance_commission
from acsele_alfa_bi.rs_25 t
group by t.cor_id
	, t.riskunit_id
	, t.rekind_id
;
commit;

create index acsele_alfa_bi.rs_26_idx_1
on acsele_alfa_bi.rs_26 (cor_id, riskunit_id, rekind_id);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.rs_26
;


/* ========================================================================================== */
/* PASO 27 - RESULTADO ORO: EXT_RISKRESERVEALFA replicada (BRUTO + DETALLE) con los atributos */
/* de la homologacion PCR                                                                     */
/*                                                                                            */
/* Formulas del RRC. GROSSRESERVE: el RRC escribe OPERATION_PREMIUM x GROUP_PERCENTAGE; aqui  */
/* se divide entre 100 porque GROUP_PERCENTAGE esta en porcentaje. REINSURERPERCPARTICIPATION */
/* = porcentaje de participacion x 100, como el RRC. Puntos a validar con Consis en el        */
/* README.                                                                                    */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.reaseguros;
commit;
*/

create table acsele_alfa_bi.reaseguros
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  'BRUTO'		as tipo_registro
	, 100		as percentageparticalfa
	, '-'		as reinsurancecessiontype
	, cast(null as varchar2(100))		as contracttype
	, c.intercommipercentage		as intercommipercentage
	, ((c.commissioncollection * 100) / nullif(c.issuedpremium, 0))		as collectcommipercentage
	, c.expensescommipercentage		as expensescommipercentage
	, (c.commision_intermediary * (c.group_percentage / 100))		as brokeragecommission
	, c.iva		as iva
	, c.commissioncollection		as commissioncollection
	, c.issuedpremium		as issuedpremium
	, (c.issuedpremium * (100 / 100))		as alfapremium
	, 0		as reinsurancecededpremium
	, c.retainedpremium		as retainedpremium
	, (((nvl(c.operation_premium, 0) * (c.group_percentage / 100)) - (c.commision_intermediary * (c.group_percentage / 100))) * c.frnc - ((c.issuedpremium * (nvl(c.expensescommipercentage, 0) / 100)) * c.frnc))		as grossreserve
	, ((((nvl(c.operation_premium, 0) * (c.group_percentage / 100)) - (c.commision_intermediary * (c.group_percentage / 100))) * c.frnc - ((c.issuedpremium * (nvl(c.expensescommipercentage, 0) / 100)) * c.frnc)) + ((nvl(s.sum_reinsurance_commission, 0) - nvl(s.sum_detail_premium, 0)) * c.frnc))		as netreserve
	, nvl(s.sum_reinsurance_commission, 0)		as reinsurancecommission
	, nvl(s.sum_detail_premium, 0)		as reinsurancecessionpremium
	, (c.issuedpremium * (nvl(c.expensescommipercentage, 0) / 100))		as expeditioncosts
	, 0		as administrationcosts
	, (((nvl(c.operation_premium, 0) * (c.group_percentage / 100)) - (c.commision_intermediary * (c.group_percentage / 100))) * c.frnc - ((c.issuedpremium * (nvl(c.expensescommipercentage, 0) / 100)) * c.frnc))		as grossconstitutionreserve
	, 0		as grossliberationreserve
	, 0		as previousgrossamount
	, c.taxcommissioncollection		as taxcommissioncollection
	, c.taxbrokeragecommission		as taxbrokeragecommission
	, 0		as reinsurerperctypecont
	, 0		as cededpremiumcttoautom
	, 0		as cededpremcttoautominte
	, 0		as cededpremcttoautomouter
	, 0		as facultativecededpremium
	, 0		as intefacultcededpremium
	, 0		as outerfacultcededpremium
	, 0		as automaticreinscommi
	, 0		as inteautomaticreinscommi
	, 0		as outerautomaticreinscommi
	, 0		as facultativereinscommi
	, 0		as intefacultareinscommi
	, 0		as outerfacultareinscommi
	, 0		as reinsurerreserve
	, 0		as intreinsurerreserve
	, 0		as outerreinsurerreserve
	, 0		as reinsurercode
	, cast(null as varchar2(500))		as reinsurerdesc
	, cast(null as varchar2(170))		as reinsurerclass
	, cast(null as number)		as reinsurerpercparticipation
	, cast(null as varchar2(170))		as riskratingcompany
	, cast(null as varchar2(170))		as riskrating
	, cast(null as number)		as riskassignedprobability
	, 0		as reserveassetsimpairment
	, 0		as interiorconstitutionreserve
	, 0		as exteriorconstitutionreserve
	, 0		as interiorliberationreserve
	, 0		as exteriorliberationreserve
	, 0		as previousintamount
	, 0		as previousextamount
	, cast(null as varchar2(170))		as reinsureridentifier
	, cast(null as number)		as reinsurer_thirdpartyid
	, cast(null as number)		as participationid
	, c.universo
	, c.courtmonth
	, c.courtyear
	, c.courtstate
	, c.ccy_id
	, c.cor_id
	, c.companycode
	, c.nom_compania
	, c.branchcode
	, c.policynumber
	, c.policyrenewnum
	, c.riskunit_id
	, c.riskunitnum
	, c.coveragenum
	, c.coveragedesc
	, c.endorsementnum
	, c.coveragestartdate
	, c.coverageenddate
	, c.endorsementdate
	, c.tpt_id
	, c.third_desc
	, c.economicsector
	, c.pro_id
	, c.cod_producto
	, c.desc_producto
	, c.cod_ramo_tecnico
	, c.desc_ramo_tecnico
	, c.cod_acuerdo
	, c.poliza_matriz
	, c.des_acuerdo
	, c.coinsurancetype
	, c.validpolicyincator
	, c.validitydays
	, c.systemincomedate
	, c.branchreinsurancecod
	, c.branchreinsurancedesc
	, c.businessunit
	, c.rekind_id
	, c.groupreinsurancecod
	, c.groupreinsurancedesc
	, c.accountingbranchcod_rrc
	, c.ramo_contable_min
	, c.ramo_contable_max
	, c.accountingbranchcod
	, c.policytype_code
	, c.paymentfrequency
	, c.policyinitialdate
	, c.policyfinishdate
	, c.courtdate
	, c.cov_id
	, c.processenddate
	, c.yearrated
	, c.frncnomonth
	, c.frncmonth
	, c.frnc
	, c.accepcoinscommipercent
	, c.totalpremium
	, c.grouppremium
	, c.group_percentage
	, c.operation_premium
	, c.commision_intermediary
from acsele_alfa_bi.rs_24 c
left join acsele_alfa_bi.rs_26 s
on s.cor_id = c.cor_id
and s.riskunit_id = c.riskunit_id
and s.rekind_id = c.rekind_id
union all
select /*+ parallel(3) */
	  'DETALLE'		as tipo_registro
	, 0		as percentageparticalfa
	, (case when c.contracttype in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract') then 'FC' else 'AU' end)		as reinsurancecessiontype
	, c.contracttype		as contracttype
	, cast(null as number)		as intercommipercentage
	, cast(null as number)		as collectcommipercentage
	, cast(null as number)		as expensescommipercentage
	, 0		as brokeragecommission
	, 0		as iva
	, 0		as commissioncollection
	, 0		as issuedpremium
	, 0		as alfapremium
	, 0		as reinsurancecededpremium
	, 0		as retainedpremium
	, 0		as grossreserve
	, 0		as netreserve
	, 0		as reinsurancecommission
	, 0		as reinsurancecessionpremium
	, 0		as expeditioncosts
	, 0		as administrationcosts
	, 0		as grossconstitutionreserve
	, 0		as grossliberationreserve
	, 0		as previousgrossamount
	, 0		as taxcommissioncollection
	, 0		as taxbrokeragecommission
	, 0		as reinsurerperctypecont
	, (case when c.contracttype in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract') then 0 else c.detail_premium end)		as cededpremiumcttoautom
	, (case when c.clase_reaseguro = 'Interior' then (case when c.contracttype in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract') then 0 else c.detail_premium end) else 0 end)		as cededpremcttoautominte
	, (case when c.clase_reaseguro = 'Exterior' then (case when c.contracttype in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract') then 0 else c.detail_premium end) else 0 end)		as cededpremcttoautomouter
	, (case when c.contracttype in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract') then c.detail_premium else 0 end)		as facultativecededpremium
	, (case when c.clase_reaseguro = 'Interior' then (case when c.contracttype in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract') then c.detail_premium else 0 end) else 0 end)		as intefacultcededpremium
	, (case when c.clase_reaseguro = 'Exterior' then (case when c.contracttype in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract') then c.detail_premium else 0 end) else 0 end)		as outerfacultcededpremium
	, (case when c.contracttype in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract') then 0 else c.reinsurance_commission end)		as automaticreinscommi
	, (case when c.clase_reaseguro = 'Interior' then (case when c.contracttype in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract') then 0 else c.reinsurance_commission end) else 0 end)		as inteautomaticreinscommi
	, (case when c.clase_reaseguro = 'Exterior' then (case when c.contracttype in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract') then 0 else c.reinsurance_commission end) else 0 end)		as outerautomaticreinscommi
	, (case when c.contracttype in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract') then c.reinsurance_commission else 0 end)		as facultativereinscommi
	, (case when c.clase_reaseguro = 'Interior' then (case when c.contracttype in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract') then c.reinsurance_commission else 0 end) else 0 end)		as intefacultareinscommi
	, (case when c.clase_reaseguro = 'Exterior' then (case when c.contracttype in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract') then c.reinsurance_commission else 0 end) else 0 end)		as outerfacultareinscommi
	, ((c.detail_premium - c.reinsurance_commission) * c.frnc)		as reinsurerreserve
	, (case when c.clase_reaseguro = 'Interior' then ((c.detail_premium - c.reinsurance_commission) * c.frnc) else 0 end)		as intreinsurerreserve
	, (case when c.clase_reaseguro = 'Exterior' then ((c.detail_premium - c.reinsurance_commission) * c.frnc) else 0 end)		as outerreinsurerreserve
	, to_number(replace(c.codigo_reasegurador, '.0', ''))		as reinsurercode
	, c.thirdpartyname		as reinsurerdesc
	, c.clase_reaseguro		as reinsurerclass
	, (c.reinsurer_part_pct * 100)		as reinsurerpercparticipation
	, c.calificadorariesgo		as riskratingcompany
	, c.calificacionriesgo		as riskrating
	, c.porcentajecalificacion		as riskassignedprobability
	, (((c.detail_premium - c.reinsurance_commission) * c.frnc) * (nvl(c.porcentajecalificacion, 0) / 100))		as reserveassetsimpairment
	, (case when c.clase_reaseguro = 'Interior' then ((c.detail_premium - c.reinsurance_commission) * c.frnc) else 0 end)		as interiorconstitutionreserve
	, (case when c.clase_reaseguro = 'Exterior' then ((c.detail_premium - c.reinsurance_commission) * c.frnc) else 0 end)		as exteriorconstitutionreserve
	, 0		as interiorliberationreserve
	, 0		as exteriorliberationreserve
	, 0		as previousintamount
	, 0		as previousextamount
	, c.reinsurer_documentoidentidad		as reinsureridentifier
	, c.thirdpartyid		as reinsurer_thirdpartyid
	, c.participationid		as participationid
	, c.universo
	, c.courtmonth
	, c.courtyear
	, c.courtstate
	, c.ccy_id
	, c.cor_id
	, c.companycode
	, c.nom_compania
	, c.branchcode
	, c.policynumber
	, c.policyrenewnum
	, c.riskunit_id
	, c.riskunitnum
	, c.coveragenum
	, c.coveragedesc
	, c.endorsementnum
	, c.coveragestartdate
	, c.coverageenddate
	, c.endorsementdate
	, c.tpt_id
	, c.third_desc
	, c.economicsector
	, c.pro_id
	, c.cod_producto
	, c.desc_producto
	, c.cod_ramo_tecnico
	, c.desc_ramo_tecnico
	, c.cod_acuerdo
	, c.poliza_matriz
	, c.des_acuerdo
	, c.coinsurancetype
	, c.validpolicyincator
	, c.validitydays
	, c.systemincomedate
	, c.branchreinsurancecod
	, c.branchreinsurancedesc
	, c.businessunit
	, c.rekind_id
	, c.groupreinsurancecod
	, c.groupreinsurancedesc
	, c.accountingbranchcod_rrc
	, c.ramo_contable_min
	, c.ramo_contable_max
	, c.accountingbranchcod
	, c.policytype_code
	, c.paymentfrequency
	, c.policyinitialdate
	, c.policyfinishdate
	, c.courtdate
	, c.cov_id
	, c.processenddate
	, c.yearrated
	, c.frncnomonth
	, c.frncmonth
	, c.frnc
	, c.accepcoinscommipercent
	, c.totalpremium
	, c.grouppremium
	, c.group_percentage
	, c.operation_premium
	, c.commision_intermediary
from acsele_alfa_bi.rs_25 c
;
commit;

create index acsele_alfa_bi.reaseguros_idx_1
on acsele_alfa_bi.reaseguros (cor_id);

create index acsele_alfa_bi.reaseguros_idx_2
on acsele_alfa_bi.reaseguros (companycode, accountingbranchcod);

create index acsele_alfa_bi.reaseguros_idx_3
on acsele_alfa_bi.reaseguros (tipo_registro);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.reaseguros
;


/* ========================================================================================== */
/* VALIDACIONES                                                                               */
/* ========================================================================================== */

/* 1. Totales para la certificacion por compania, ramo contable y tipo de registro */
select /*+ parallel(3) */
	  r.companycode
	, r.accountingbranchcod
	, r.tipo_registro
	, count(*)									as registros
	, sum(r.issuedpremium)						as issuedpremium
	, sum(r.alfapremium)						as alfapremium
	, sum(r.cededpremiumcttoautom)				as primacedaut
	, sum(r.facultativecededpremium)			as primacedfac
	, sum(r.automaticreinscommi)				as comisionreasegaut
	, sum(r.facultativereinscommi)				as comisionreasegfac
	, sum(r.brokeragecommission)				as brokeragecommission
	, sum(r.commissioncollection)				as commissioncollection
from acsele_alfa_bi.reaseguros r
group by r.companycode
	, r.accountingbranchcod
	, r.tipo_registro
order by r.companycode
	, r.accountingbranchcod
	, r.tipo_registro
;

/* 2. Cuadre del total cedido del bruto (REINSURANCECESSIONPREMIUM) contra la suma de su detalle: debe dar cero */
select /*+ parallel(3) */
	  r.companycode
	, sum(case when r.tipo_registro = 'BRUTO' then r.reinsurancecessionpremium else 0 end)
	  - sum(case when r.tipo_registro = 'DETALLE' then r.cededpremiumcttoautom + r.facultativecededpremium else 0 end)		as diferencia
from acsele_alfa_bi.reaseguros r
group by r.companycode
;

/* 3. Cuadre contra la tabla del boton de Acsel-e para el mismo corte (EXT_RISKRESERVEALFA, octubre 2025) */
select /*+ parallel(3) */
	  e.companycode
	, (case when e.percentageparticalfa = 100 then 'BRUTO' else 'DETALLE' end)		as tipo_registro
	, count(*)										as registros
	, sum(e.issuedpremium)							as issuedpremium
	, sum(e.alfapremium)							as alfapremium
	, sum(e.cededpremiumcttoautom)					as primacedaut
	, sum(e.facultativecededpremium)				as primacedfac
	, sum(e.automaticreinscommi)					as comisionreasegaut
	, sum(e.facultativereinscommi)					as comisionreasegfac
	, sum(e.brokeragecommission)					as brokeragecommission
	, sum(e.commissioncollection)					as commissioncollection
from acsele_alfa.ext_riskreservealfa e
inner join acsele_alfa_bi.cfg_pcr_periodo per
on per.courtmonth = e.courtmonth
and per.courtyear = e.courtyear
group by e.companycode
	, (case when e.percentageparticalfa = 100 then 'BRUTO' else 'DETALLE' end)
;

/* 4. Detalle sin rol de reasegurador o sin calificacion (REINSURERCODE = 0 deja nombre y clase vacios por regla) */
select /*+ parallel(3) */
	  r.companycode
	, sum(case when r.reinsurerclass is null then 1 else 0 end)				as sin_clase
	, sum(case when r.riskrating is null then 1 else 0 end)					as sin_calificacion
from acsele_alfa_bi.reaseguros r
where r.tipo_registro = 'DETALLE'
group by r.companycode
;

/* 5. Reutilizacion: registros por universo (MES = emitidas del periodo; VIGENTE = vigentes al corte, universo heredado de M1) */
select /*+ parallel(3) */
	  r.universo
	, r.tipo_registro
	, count(*)			as registros
from acsele_alfa_bi.reaseguros r
group by r.universo
	, r.tipo_registro
;
