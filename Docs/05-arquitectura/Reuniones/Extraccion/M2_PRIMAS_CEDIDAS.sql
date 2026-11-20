/* ========================================================================================== */
/* M2 PRIMAS CEDIDAS - MALLA 2 DEL PROCESO PCR  -  VERSION 1.5 - 2026-11-09                   */
/*                                                                                            */
/* Salida: acsele_alfa_bi.primas_cedidas (Zona Oro de la malla 2). Misma logica del SP        */
/* sp_prima_cedida_alfa (ext_1 a ext_12).                                                     */
/* DEPENDENCIA: corre despues de la malla 1 (EMISIONES certificada) y REUTILIZA su            */
/* extraccion; solo extrae lo que la malla 1 no trae:                                         */
/*  - REUTILIZA de M1: universo de operaciones (em_opr), operacion, poliza, prepolicy,        */
/* producto y unidades de riesgo (em_3), acuerdo y version por operacion (em_pub_acu), ramo   */
/* contable de las coberturas de Satelite (em_st_cob).                                        */
/*  - EXTRAE NUEVO: preproduct, cadena de reaseguro (reinsuranceoperation, groupinfo,         */
/* component, contract, detail, ri_participation, thirdpartyrole, reasegurador,               */
/* reinsuranceoperationproperty), nombre y versiones del acuerdo (stps_agreementstaticdat,    */
/* stag_agreementversion, preagreement), documento del reasegurador (prethirdparty) y         */
/* coberturas de las operaciones que no son de Satelite (DELTA contra em_st_cob).             */
/*  - PUBLICA para M3: la cadena de reaseguro con las columnas que pide el RRC                */
/* (agregatedriskunitid, policyevent, totalpremium, groupid, premiumamount,                   */
/* rod_proratedpremium, participacion, todos los roles del reasegurador, todas las            */
/* propiedades del detalle).                                                                  */
/* Universo HEREDADO de la malla 1: EMISIONES ya trae las operaciones del periodo (MES) y las */
/* polizas no mensuales vigentes al corte (VIGENTE). Aqui solo se acota a las que tienen      */
/* operacion de reaseguro y producto de las companias de cfg_pcr_compania. La columna         */
/* universo las distingue.                                                                    */
/* Reglas: pivote -> dependientes; inner antes de left; cero subqueries; sin KEEP / OVER /    */
/* DENSE_RANK; sin funciones PL/SQL; esquema en todas las tablas; lectura parallel(3) /       */
/* escritura parallel 2.                                                                      */
/* PRUEBA EN ORACLE - VERSION POR TABLAS (recomendada): paso a paso, con indices y conteo por */
/* tabla. Parametros (periodo, corte, companias, productos, numeros): se cambian SOLO en      */
/* 00_PARAMETROS/PCR_PARAMETROS_v1.5.sql. Conceptos: salen de stst_systemproperty (malla 3).  */
/* ========================================================================================== */


/* ========================================================================================== */
/* PASO 1 - REUTILIZA M1 (em_opr, em_3): operaciones de EMISIONES (MES + VIGENTE) con su      */
/* poliza (policydco, agregatedpolicy, prepolicy, product)                                    */
/*                                                                                            */
/* Un registro por operacion y poliza. Columnas para esta malla y para la malla 3.            */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.pc_1;
commit;
*/

create table acsele_alfa_bi.pc_1
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  e.plcdco_operationpk				as operationpk
	, e.plcdco_dcoid					as dcoid
	, e.plcdco_agregatedobjectid		as agregatedobjectid
	, e.ctx_time_stamp					as time_stamp
	, e.plcdco_initialdate				as policy_begindate
	, e.plcdco_finishdate				as policy_enddate
	, e.grgtdplc_productid				as productid
	, e.prdct0_pro_dcoid				as pro_dcoid
	, e.prplc_static					as prplc_static
	, e.prplc_numeropolizainput			as numeropoliza
	, e.prplc_oficinavalue				as cod_sucursal
	, e.prplc_oficinainput				as nom_sucursal
	, e.prplc_tipopolizavalue			as cod_tipo_poliza
	, e.prplc_tipopolizainput			as des_tipo_poliza
	, e.prplc_frecuenciapagovalue		as cod_frecuencia_pago
	, e.prplc_frecuenciapagoinput		as des_frecuencia_pago
	, e.prplc_monedavalue				as cod_moneda
	, e.prplc_monedainput				as des_moneda
	, (case when e.ctx_time_stamp < per.fecha_inicio then 'VIGENTE' else 'MES' end)		as universo
from acsele_alfa_bi.em_opr o
inner join acsele_alfa_bi.em_3 e
on e.plcdco_operationpk = o.plcdco_operationpk
cross join acsele_alfa_bi.cfg_pcr_periodo per
group by e.plcdco_operationpk
	, e.plcdco_dcoid
	, e.plcdco_agregatedobjectid
	, e.ctx_time_stamp
	, e.plcdco_initialdate
	, e.plcdco_finishdate
	, e.grgtdplc_productid
	, e.prdct0_pro_dcoid
	, e.prplc_static
	, e.prplc_numeropolizainput
	, e.prplc_oficinavalue
	, e.prplc_oficinainput
	, e.prplc_tipopolizavalue
	, e.prplc_tipopolizainput
	, e.prplc_frecuenciapagovalue
	, e.prplc_frecuenciapagoinput
	, e.prplc_monedavalue
	, e.prplc_monedainput
	, (case when e.ctx_time_stamp < per.fecha_inicio then 'VIGENTE' else 'MES' end)
;
commit;

create index acsele_alfa_bi.pc_1_idx_1
on acsele_alfa_bi.pc_1 (operationpk, agregatedobjectid);

create index acsele_alfa_bi.pc_1_idx_2
on acsele_alfa_bi.pc_1 (dcoid);

create index acsele_alfa_bi.pc_1_idx_3
on acsele_alfa_bi.pc_1 (pro_dcoid);

create index acsele_alfa_bi.pc_1_idx_4
on acsele_alfa_bi.pc_1 (universo);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.pc_1
;


/* ========================================================================================== */
/* PASO 1R - REUTILIZA M1 (em_3): unidades de riesgo (riskunitdco) de las operaciones de      */
/* EMISIONES                                                                                  */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.pc_1r;
commit;
*/

create table acsele_alfa_bi.pc_1r
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  e.rskntdco_operationpk			as operationpk
	, e.rskntdco_dcoid					as dcoid
	, e.rskntdco_agregatedobjectid		as agregatedobjectid
	, e.rskntdco_initialdate			as initialdate
	, e.rskntdco_finishdate				as finishdate
from acsele_alfa_bi.em_opr o
inner join acsele_alfa_bi.em_3 e
on e.plcdco_operationpk = o.plcdco_operationpk
group by e.rskntdco_operationpk
	, e.rskntdco_dcoid
	, e.rskntdco_agregatedobjectid
	, e.rskntdco_initialdate
	, e.rskntdco_finishdate
;
commit;

create index acsele_alfa_bi.pc_1r_idx_1
on acsele_alfa_bi.pc_1r (operationpk);

create index acsele_alfa_bi.pc_1r_idx_2
on acsele_alfa_bi.pc_1r (operationpk, agregatedobjectid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.pc_1r
;


/* ========================================================================================== */
/* PASO 1A - REUTILIZA M1 (em_pub_acu): acuerdo por operacion                                 */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.pc_opacu;
commit;
*/

create table acsele_alfa_bi.pc_opacu
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  a.plcdco_operationpk		as operationpk
	, a.ast_id
from acsele_alfa_bi.em_pub_acu a
group by a.plcdco_operationpk
	, a.ast_id
;
commit;

create index acsele_alfa_bi.pc_opacu_idx_1
on acsele_alfa_bi.pc_opacu (operationpk);

create index acsele_alfa_bi.pc_opacu_idx_2
on acsele_alfa_bi.pc_opacu (ast_id);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.pc_opacu
;


/* ========================================================================================== */
/* PASO 2 - EXTRAE NUEVO: preproduct de los productos de EMISIONES (companias de              */
/* cfg_pcr_compania)                                                                          */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.pc_2;
commit;
*/

create table acsele_alfa_bi.pc_2
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  pre.pk
	, pre.companiavalue				as cod_compania
	, pre.companiainput				as nom_compania
	, pre.ramovalue					as cod_ramo_tecnico
	, pre.ramoinput					as nom_ramo_tecnico
	, pre.codigoproductoinput		as cod_producto
	, pre.descripcionproductoinput	as nom_producto
from acsele_alfa_bi.pc_1 p
inner join acsele_alfa.preproduct pre
on pre.pk = p.pro_dcoid
inner join acsele_alfa_bi.cfg_pcr_compania cc
on cc.companiavalue = pre.companiavalue
group by pre.pk
	, pre.companiavalue
	, pre.companiainput
	, pre.ramovalue
	, pre.ramoinput
	, pre.codigoproductoinput
	, pre.descripcionproductoinput
;
commit;

create index acsele_alfa_bi.pc_2_idx_1
on acsele_alfa_bi.pc_2 (pk);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.pc_2
;


/* ========================================================================================== */
/* PASO 3 - EXTRAE NUEVO: operacion de reaseguro (reinsuranceoperation) de las operaciones de */
/* EMISIONES                                                                                  */
/*                                                                                            */
/* Base comun de ext_1, ext_2, ext_3 y ext_5 del SP. Publica agregatedriskunitid, policyevent */
/* y totalpremium para la malla 3.                                                            */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.pc_3;
commit;
*/

create table acsele_alfa_bi.pc_3
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  rso.cor_id
	, rso.agregatedpolicyid
	, rso.operationid
	, rso.agregatedriskunitid
	, rso.policyevent
	, rso.totalpremium
	, rso.ro_date
	, p.dcoid
	, p.universo
	, pre.cod_compania
	, pre.nom_compania
	, pre.cod_ramo_tecnico
	, pre.nom_ramo_tecnico
	, pre.cod_producto
	, pre.nom_producto
from acsele_alfa_bi.pc_1 p
inner join acsele_alfa.reinsuranceoperation rso
on rso.cor_id = p.operationpk
and rso.agregatedpolicyid = p.agregatedobjectid
inner join acsele_alfa_bi.pc_2 pre
on pre.pk = p.pro_dcoid
group by rso.cor_id
	, rso.agregatedpolicyid
	, rso.operationid
	, rso.agregatedriskunitid
	, rso.policyevent
	, rso.totalpremium
	, rso.ro_date
	, p.dcoid
	, p.universo
	, pre.cod_compania
	, pre.nom_compania
	, pre.cod_ramo_tecnico
	, pre.nom_ramo_tecnico
	, pre.cod_producto
	, pre.nom_producto
;
commit;

create index acsele_alfa_bi.pc_3_idx_1
on acsele_alfa_bi.pc_3 (cor_id);

create index acsele_alfa_bi.pc_3_idx_2
on acsele_alfa_bi.pc_3 (operationid);

create index acsele_alfa_bi.pc_3_idx_3
on acsele_alfa_bi.pc_3 (dcoid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.pc_3
;


/* ========================================================================================== */
/* PASO 4 - EXT_1: operacion, compania, producto y fecha inicial de la unidad de riesgo       */
/* (REUTILIZA pc_1r)                                                                          */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.pc_4;
commit;
*/

create table acsele_alfa_bi.pc_4
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  pc3.cor_id										as operacion
	, pc3.dcoid
	, pc3.cod_compania
	, pc3.nom_compania
	, pc3.cod_ramo_tecnico
	, pc3.nom_ramo_tecnico
	, pc3.cod_producto
	, pc3.nom_producto
	, pc3.ro_date									as fe_operacion
	, cast(to_char(pc3.ro_date, 'yyyymm') as int)		as pr_operacion
	, ric.initialdate
	, pc3.operationid
	, pc3.universo
from acsele_alfa_bi.pc_3 pc3
inner join acsele_alfa_bi.pc_1r ric
on ric.operationpk = pc3.cor_id
group by pc3.cor_id
	, pc3.dcoid
	, pc3.cod_compania
	, pc3.nom_compania
	, pc3.cod_ramo_tecnico
	, pc3.nom_ramo_tecnico
	, pc3.cod_producto
	, pc3.nom_producto
	, pc3.ro_date
	, cast(to_char(pc3.ro_date, 'yyyymm') as int)
	, ric.initialdate
	, pc3.operationid
	, pc3.universo
;
commit;

create index acsele_alfa_bi.pc_4_idx_1
on acsele_alfa_bi.pc_4 (operacion);

create index acsele_alfa_bi.pc_4_idx_2
on acsele_alfa_bi.pc_4 (dcoid);

create index acsele_alfa_bi.pc_4_idx_3
on acsele_alfa_bi.pc_4 (operationid);

create index acsele_alfa_bi.pc_4_idx_4
on acsele_alfa_bi.pc_4 (initialdate);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.pc_4
;


/* ========================================================================================== */
/* PASO 5A - EXTRAE NUEVO: numero y nombre del acuerdo (stps_agreementstaticdat) de los       */
/* acuerdos del universo (pc_opacu)                                                           */
/*                                                                                            */
/* M1 ya trae el acuerdo; el nombre no lo trae Vida y los vigentes no tienen numero.          */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.pc_acu;
commit;
*/

create table acsele_alfa_bi.pc_acu
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  st.ast_id
	, st.ast_numeroacu
	, st.ast_nombreacu
from acsele_alfa_bi.pc_opacu a
inner join acsele_alfa_bi.pc_3 pc3
on pc3.cor_id = a.operationpk
inner join acsele_alfa.stps_agreementstaticdat st
on st.ast_id = a.ast_id
group by st.ast_id
	, st.ast_numeroacu
	, st.ast_nombreacu
;
commit;

create index acsele_alfa_bi.pc_acu_idx_1
on acsele_alfa_bi.pc_acu (ast_id);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.pc_acu
;


/* ========================================================================================== */
/* PASO 5 - EXT_2: poliza (pc_1) y acuerdo / poliza matriz (pc_opacu + pc_acu)                */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.pc_5;
commit;
*/

create table acsele_alfa_bi.pc_5
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  p.dcoid						as pk
	, acu.ast_numeroacu				as poliza_matriz
	, p.cod_sucursal
	, p.nom_sucursal
	, p.cod_tipo_poliza
	, p.des_tipo_poliza
	, p.cod_frecuencia_pago
	, p.des_frecuencia_pago
	, p.cod_moneda
	, p.des_moneda
	, acu.ast_id					as cod_acuerdo
	, acu.ast_nombreacu				as des_acuerdo
from acsele_alfa_bi.pc_3 pc3
inner join acsele_alfa_bi.pc_1 p
on p.operationpk = pc3.cor_id
and p.dcoid = pc3.dcoid
inner join acsele_alfa_bi.pc_opacu a
on a.operationpk = p.operationpk
inner join acsele_alfa_bi.pc_acu acu
on acu.ast_id = a.ast_id
group by p.dcoid
	, acu.ast_numeroacu
	, p.cod_sucursal
	, p.nom_sucursal
	, p.cod_tipo_poliza
	, p.des_tipo_poliza
	, p.cod_frecuencia_pago
	, p.des_frecuencia_pago
	, p.cod_moneda
	, p.des_moneda
	, acu.ast_id
	, acu.ast_nombreacu
;
commit;

create index acsele_alfa_bi.pc_5_idx_1
on acsele_alfa_bi.pc_5 (pk);

create index acsele_alfa_bi.pc_5_idx_2
on acsele_alfa_bi.pc_5 (cod_acuerdo);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.pc_5
;


/* ========================================================================================== */
/* PASO 6 - EXTRAE NUEVO: grupo, componente y contrato de reaseguro                           */
/* (reinsuranceoperationgroupinfo, reinsuranceoperationcomponent, ri_reinsurancecontract)     */
/*                                                                                            */
/* Publica para M3 groupid, premiumamount y totalpremium del componente. El contrato va por   */
/* LEFT para no perder componentes que el RRC si usa; el paso 12 exige contrato, como el SP.  */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.pc_6;
commit;
*/

create table acsele_alfa_bi.pc_6
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  rog.operationid
	, rog.reinsurancegroupinfoid
	, rog.groupid
	, rog.premiumamount
	, roc.reinopercomponentid
	, roc.contractid			as cod_contrato
	, roc.parentid				as cod_componente
	, roc.contracttype
	, roc.totalpremium			as roc_totalpremium
	, rc.rcid
	, rc.description			as des_contrato
from acsele_alfa_bi.pc_3 pc3
inner join acsele_alfa.reinsuranceoperationgroupinfo rog
on rog.operationid = pc3.operationid
inner join acsele_alfa.reinsuranceoperationcomponent roc
on roc.reinsurancegroupinfoid = rog.reinsurancegroupinfoid
left join acsele_alfa.ri_reinsurancecontract rc
on rc.rcid = roc.contractid
group by rog.operationid
	, rog.reinsurancegroupinfoid
	, rog.groupid
	, rog.premiumamount
	, roc.reinopercomponentid
	, roc.contractid
	, roc.parentid
	, roc.contracttype
	, roc.totalpremium
	, rc.rcid
	, rc.description
;
commit;

create index acsele_alfa_bi.pc_6_idx_1
on acsele_alfa_bi.pc_6 (operationid);

create index acsele_alfa_bi.pc_6_idx_2
on acsele_alfa_bi.pc_6 (reinopercomponentid);

create index acsele_alfa_bi.pc_6_idx_3
on acsele_alfa_bi.pc_6 (groupid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.pc_6
;


/* ========================================================================================== */
/* PASO 7D - EXTRAE NUEVO: detalle por reasegurador (reinsuranceoperationdetail) de todos los */
/* componentes                                                                                */
/*                                                                                            */
/* Publica para M3 participationid y rod_proratedpremium.                                     */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.pc_7d;
commit;
*/

create table acsele_alfa_bi.pc_7d
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  rod.reinopercomponentid
	, rod.reinsuranceoperationdetailid
	, rod.participationid
	, rod.thirdpartyid
	, rod.thirdpartyname
	, rod.sumassured
	, rod.rod_proratedpremium
from acsele_alfa_bi.pc_6 pc6
inner join acsele_alfa.reinsuranceoperationdetail rod
on rod.reinopercomponentid = pc6.reinopercomponentid
group by rod.reinopercomponentid
	, rod.reinsuranceoperationdetailid
	, rod.participationid
	, rod.thirdpartyid
	, rod.thirdpartyname
	, rod.sumassured
	, rod.rod_proratedpremium
;
commit;

create index acsele_alfa_bi.pc_7d_idx_1
on acsele_alfa_bi.pc_7d (reinopercomponentid);

create index acsele_alfa_bi.pc_7d_idx_2
on acsele_alfa_bi.pc_7d (reinsuranceoperationdetailid);

create index acsele_alfa_bi.pc_7d_idx_3
on acsele_alfa_bi.pc_7d (thirdpartyid);

create index acsele_alfa_bi.pc_7d_idx_4
on acsele_alfa_bi.pc_7d (participationid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.pc_7d
;


/* ========================================================================================== */
/* PASO 7P - EXTRAE NUEVO: participacion del reasegurador (ri_participation)                  */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.pc_7p;
commit;
*/

create table acsele_alfa_bi.pc_7p
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  par.participationid
	, par.percentage
	, par.thirdpartyid				as par_thirdpartyid
from acsele_alfa_bi.pc_7d d
inner join acsele_alfa.ri_participation par
on par.participationid = d.participationid
group by par.participationid
	, par.percentage
	, par.thirdpartyid
;
commit;

create index acsele_alfa_bi.pc_7p_idx_1
on acsele_alfa_bi.pc_7p (participationid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.pc_7p
;


/* ========================================================================================== */
/* PASO 7R - EXTRAE NUEVO: roles del reasegurador (thirdpartyrole, reasegurador), todos los   */
/* roles                                                                                      */
/*                                                                                            */
/* El SP usa el rol 8275 (paso 7); el RRC no filtra rol (malla 3).                            */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.pc_7r;
commit;
*/

create table acsele_alfa_bi.pc_7r
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  tr.thirdpartyid
	, tr.rol_id
	, rea.clasereaseguroinput
	, rea.clasereasegurovalue
	, rea.codigoreaseguradorinput
from acsele_alfa_bi.pc_7d d
inner join acsele_alfa.thirdpartyrole tr
on tr.thirdpartyid = d.thirdpartyid
inner join acsele_alfa.reasegurador rea
on rea.pk = tr.iddcorole
group by tr.thirdpartyid
	, tr.rol_id
	, rea.clasereaseguroinput
	, rea.clasereasegurovalue
	, rea.codigoreaseguradorinput
;
commit;

create index acsele_alfa_bi.pc_7r_idx_1
on acsele_alfa_bi.pc_7r (thirdpartyid);

create index acsele_alfa_bi.pc_7r_idx_2
on acsele_alfa_bi.pc_7r (rol_id);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.pc_7r
;


/* ========================================================================================== */
/* PASO 7 - EXT_4: detalle por reasegurador con participacion y clase (rol 8275), sobre lo ya */
/* extraido                                                                                   */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.pc_7;
commit;
*/

create table acsele_alfa_bi.pc_7
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  d.reinopercomponentid
	, d.reinsuranceoperationdetailid
	, d.thirdpartyid
	, d.thirdpartyname					as nom_tercero
	, par.percentage					as porcentaje_participacion
	, rol.clasereaseguroinput			as des_clase_reasegurador
	, rol.clasereasegurovalue			as cod_clase_reasegurador
	, d.sumassured
from acsele_alfa_bi.pc_7d d
inner join acsele_alfa_bi.pc_7p par
on par.participationid = d.participationid
inner join acsele_alfa_bi.pc_7r rol
on rol.thirdpartyid = d.thirdpartyid
where rol.rol_id = 8275
group by d.reinopercomponentid
	, d.reinsuranceoperationdetailid
	, d.thirdpartyid
	, d.thirdpartyname
	, par.percentage
	, rol.clasereaseguroinput
	, rol.clasereasegurovalue
	, d.sumassured
;
commit;

create index acsele_alfa_bi.pc_7_idx_1
on acsele_alfa_bi.pc_7 (reinopercomponentid);

create index acsele_alfa_bi.pc_7_idx_2
on acsele_alfa_bi.pc_7 (reinsuranceoperationdetailid);

create index acsele_alfa_bi.pc_7_idx_3
on acsele_alfa_bi.pc_7 (thirdpartyid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.pc_7
;


/* ========================================================================================== */
/* PASO 8 - EXT_5: ramo contable de la operacion. REUTILIZA em_st_cob (Satelite) + EXTRAE el  */
/* DELTA (operaciones que M1 no leyo)                                                         */
/*                                                                                            */
/* El delta es un anti-join (LEFT JOIN ... IS NULL) contra lo que ya trajo M1: no se vuelve a */
/* leer coveragedco de las operaciones de Satelite.                                           */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.pc_8;
commit;
*/

create table acsele_alfa_bi.pc_8
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  c.plcdco_operationpk												as operationpk
	, to_number(replace(c.cbrtr_cobramocontablevalue, '.0', ''))		as cod_ramo_contable
	, c.strrnsrnckd_rek_description									as nom_ramo_contable
from acsele_alfa_bi.em_st_cob c
inner join acsele_alfa_bi.pc_3 pc3
on pc3.cor_id = c.plcdco_operationpk
where c.strrnsrnckd_rek_description is not null
group by c.plcdco_operationpk
	, to_number(replace(c.cbrtr_cobramocontablevalue, '.0', ''))
	, c.strrnsrnckd_rek_description
union all
select /*+ parallel(3) */
	  cdco.operationpk
	, rsk.rek_code
	, rsk.rek_description
from acsele_alfa_bi.pc_3 pc3
inner join acsele_alfa.coveragedco cdco
on cdco.operationpk = pc3.cor_id
inner join acsele_alfa.cobertura cob
on cob.pk = cdco.dcoid
inner join acsele_alfa.stri_reinsurancekind rsk
on rsk.rek_code = to_number(replace(cob.cobramocontablevalue, '.0', ''))
left join acsele_alfa_bi.em_st_cob c
on c.plcdco_operationpk = pc3.cor_id
where c.plcdco_operationpk is null
group by cdco.operationpk
	, rsk.rek_code
	, rsk.rek_description
;
commit;

create index acsele_alfa_bi.pc_8_idx_1
on acsele_alfa_bi.pc_8 (operationpk);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.pc_8
;


/* ========================================================================================== */
/* PASO 9 - EXTRAE NUEVO: propiedades del detalle (reinsuranceoperationproperty) de todo el   */
/* detalle                                                                                    */
/*                                                                                            */
/* Publica para M3 (ComPriCedida). El paso final la cruza solo con el detalle del rol 8275,   */
/* como el SP.                                                                                */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.pc_9;
commit;
*/

create table acsele_alfa_bi.pc_9
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  ropr.rod_id
	, ropr.ropp_name
	, ropr.ropp_value
from acsele_alfa_bi.pc_7d d
inner join acsele_alfa.reinsuranceoperationproperty ropr
on ropr.rod_id = d.reinsuranceoperationdetailid
group by ropr.rod_id
	, ropr.ropp_name
	, ropr.ropp_value
;
commit;

create index acsele_alfa_bi.pc_9_idx_1
on acsele_alfa_bi.pc_9 (rod_id);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.pc_9
;


/* ========================================================================================== */
/* PASO 10 - EXT_7: EXTRAE NUEVO todas las versiones de los acuerdos (stag_agreementversion,  */
/* preagreement)                                                                              */
/*                                                                                            */
/* El SP elige la version vigente a la fecha inicial de la unidad de riesgo; M1 solo publica  */
/* la version que eligio cada negocio. Publica para M3 (unidad de negocio).                   */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.pc_10;
commit;
*/

create table acsele_alfa_bi.pc_10
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  agrv.ast_id
	, agrv.dcoid					as agrv_dcoid
	, agrv.ast_fecini				as fe_inicio_acuerdo
	, agrv.ast_fecfin				as fe_fin_acuerdo
	, liunc.tipoproduccionvalue		as cod_tipo_produccion
	, liunc.tipoproduccioninput		as des_tipo_produccion
	, liunc.unidadnegociovalue		as cod_unidad_negocio
	, liunc.unidadnegocioinput		as des_unidad_negocio
	, liunc.lineacreditovalue		as cod_linea_credito
	, liunc.lineacreditoinput		as des_linea_credito
	, liunc.frecuenciapagoinput		as frecuenciapago_acuerdo
from acsele_alfa_bi.pc_acu a
inner join acsele_alfa.stag_agreementversion agrv
on agrv.ast_id = a.ast_id
inner join acsele_alfa.preagreement liunc
on liunc.pk = agrv.dcoid
group by agrv.ast_id
	, agrv.dcoid
	, agrv.ast_fecini
	, agrv.ast_fecfin
	, liunc.tipoproduccionvalue
	, liunc.tipoproduccioninput
	, liunc.unidadnegociovalue
	, liunc.unidadnegocioinput
	, liunc.lineacreditovalue
	, liunc.lineacreditoinput
	, liunc.frecuenciapagoinput
;
commit;

create index acsele_alfa_bi.pc_10_idx_1
on acsele_alfa_bi.pc_10 (ast_id, fe_inicio_acuerdo, fe_fin_acuerdo);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.pc_10
;


/* ========================================================================================== */
/* PASO 11 - EXTRAE NUEVO: documento de los reaseguradores (prethirdparty)                    */
/*                                                                                            */
/* Publica para M3.                                                                           */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.pc_11;
commit;
*/

create table acsele_alfa_bi.pc_11
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  ptp.static
	, ptp.pk
	, ptp.tipodocumentoinput			as tipo_documento_tercero
	, ptp.documentoidentidadinput		as num_documento_tercero
from acsele_alfa_bi.pc_7d d
inner join acsele_alfa.prethirdparty ptp
on ptp.static = d.thirdpartyid
group by ptp.static
	, ptp.pk
	, ptp.tipodocumentoinput
	, ptp.documentoidentidadinput
;
commit;

create index acsele_alfa_bi.pc_11_idx_1
on acsele_alfa_bi.pc_11 (static);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.pc_11
;


/* ========================================================================================== */
/* PASO 12 - EXT_9: une operacion, poliza, version del acuerdo vigente a la fecha inicial de  */
/* la unidad de riesgo, componente con contrato y ramo contable                               */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.pc_12;
commit;
*/

create table acsele_alfa_bi.pc_12
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  e1.operacion
	, e2.poliza_matriz
	, e1.cod_compania
	, e1.nom_compania
	, e1.cod_ramo_tecnico
	, e1.nom_ramo_tecnico
	, e5.cod_ramo_contable
	, e5.nom_ramo_contable
	, e2.cod_sucursal
	, e2.nom_sucursal
	, e2.cod_tipo_poliza
	, e2.des_tipo_poliza
	, e2.cod_frecuencia_pago
	, e2.des_frecuencia_pago
	, e2.cod_acuerdo
	, e2.des_acuerdo
	, e7.fe_inicio_acuerdo
	, e7.fe_fin_acuerdo
	, e1.cod_producto
	, e1.nom_producto
	, e3.cod_contrato
	, e3.cod_componente
	, e3.des_contrato
	, (case
		when upper(e3.contracttype) = 'QUOTASHAREREINSURANCECONTRACT' then 'CUOTAPARTE'
		when upper(e3.contracttype) = 'MANDATORYFACULTATIVECONTRACT' then 'FACULTATIVO'
		when upper(e3.contracttype) = 'EXCEDENTREINSURANCECONTRACT' then 'EXCEDENTE'
		else 'IDENTIFICAR TIPO'
	  end)		as tipo_contrato
	, e1.fe_operacion
	, e1.pr_operacion
	, e7.cod_tipo_produccion
	, e7.des_tipo_produccion
	, e7.cod_unidad_negocio
	, e7.des_unidad_negocio
	, e7.cod_linea_credito
	, e7.des_linea_credito
	, e2.cod_moneda
	, e2.des_moneda
	, e3.contracttype
	, e3.reinopercomponentid
	, e1.universo
from acsele_alfa_bi.pc_4 e1
inner join acsele_alfa_bi.pc_5 e2
on e1.dcoid = e2.pk
inner join acsele_alfa_bi.pc_10 e7
on e2.cod_acuerdo = e7.ast_id
and e7.fe_inicio_acuerdo <= e1.initialdate
and e7.fe_fin_acuerdo >= e1.initialdate
inner join acsele_alfa_bi.pc_6 e3
on e1.operationid = e3.operationid
inner join acsele_alfa_bi.pc_8 e5
on e1.operacion = e5.operationpk
where e3.rcid is not null
group by e1.operacion
	, e2.poliza_matriz
	, e1.cod_compania
	, e1.nom_compania
	, e1.cod_ramo_tecnico
	, e1.nom_ramo_tecnico
	, e5.cod_ramo_contable
	, e5.nom_ramo_contable
	, e2.cod_sucursal
	, e2.nom_sucursal
	, e2.cod_tipo_poliza
	, e2.des_tipo_poliza
	, e2.cod_frecuencia_pago
	, e2.des_frecuencia_pago
	, e2.cod_acuerdo
	, e2.des_acuerdo
	, e7.fe_inicio_acuerdo
	, e7.fe_fin_acuerdo
	, e1.cod_producto
	, e1.nom_producto
	, e3.cod_contrato
	, e3.cod_componente
	, e3.des_contrato
	, (case
		when upper(e3.contracttype) = 'QUOTASHAREREINSURANCECONTRACT' then 'CUOTAPARTE'
		when upper(e3.contracttype) = 'MANDATORYFACULTATIVECONTRACT' then 'FACULTATIVO'
		when upper(e3.contracttype) = 'EXCEDENTREINSURANCECONTRACT' then 'EXCEDENTE'
		else 'IDENTIFICAR TIPO'
	  end)
	, e1.fe_operacion
	, e1.pr_operacion
	, e7.cod_tipo_produccion
	, e7.des_tipo_produccion
	, e7.cod_unidad_negocio
	, e7.des_unidad_negocio
	, e7.cod_linea_credito
	, e7.des_linea_credito
	, e2.cod_moneda
	, e2.des_moneda
	, e3.contracttype
	, e3.reinopercomponentid
	, e1.universo
;
commit;

create index acsele_alfa_bi.pc_12_idx_1
on acsele_alfa_bi.pc_12 (reinopercomponentid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.pc_12
;


/* ========================================================================================== */
/* PASO 13 - EXT_10 + EXT_11: agrega el reasegurador (pc_7) y su documento (pc_11)            */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.pc_13;
commit;
*/

create table acsele_alfa_bi.pc_13
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  e9.operacion
	, e9.poliza_matriz
	, e9.cod_compania
	, e9.nom_compania
	, e9.cod_ramo_tecnico
	, e9.nom_ramo_tecnico
	, e9.cod_ramo_contable
	, e9.nom_ramo_contable
	, e9.cod_sucursal
	, e9.nom_sucursal
	, e9.cod_tipo_poliza
	, e9.des_tipo_poliza
	, e9.cod_frecuencia_pago
	, e9.des_frecuencia_pago
	, e9.cod_acuerdo
	, e9.des_acuerdo
	, e9.fe_inicio_acuerdo
	, e9.fe_fin_acuerdo
	, e9.cod_producto
	, e9.nom_producto
	, e9.cod_contrato
	, e9.cod_componente
	, e9.des_contrato
	, e9.tipo_contrato
	, e8.tipo_documento_tercero
	, e8.num_documento_tercero
	, e4.nom_tercero
	, e4.cod_clase_reasegurador
	, e4.des_clase_reasegurador
	, e4.porcentaje_participacion
	, e9.fe_operacion
	, e9.pr_operacion
	, e9.cod_tipo_produccion
	, e9.des_tipo_produccion
	, e9.cod_unidad_negocio
	, e9.des_unidad_negocio
	, e9.cod_linea_credito
	, e9.des_linea_credito
	, e9.cod_moneda
	, e9.des_moneda
	, e9.contracttype
	, e4.reinsuranceoperationdetailid
	, e4.sumassured
	, e9.universo
from acsele_alfa_bi.pc_12 e9
inner join acsele_alfa_bi.pc_7 e4
on e9.reinopercomponentid = e4.reinopercomponentid
inner join acsele_alfa_bi.pc_11 e8
on e4.thirdpartyid = e8.static
group by e9.operacion
	, e9.poliza_matriz
	, e9.cod_compania
	, e9.nom_compania
	, e9.cod_ramo_tecnico
	, e9.nom_ramo_tecnico
	, e9.cod_ramo_contable
	, e9.nom_ramo_contable
	, e9.cod_sucursal
	, e9.nom_sucursal
	, e9.cod_tipo_poliza
	, e9.des_tipo_poliza
	, e9.cod_frecuencia_pago
	, e9.des_frecuencia_pago
	, e9.cod_acuerdo
	, e9.des_acuerdo
	, e9.fe_inicio_acuerdo
	, e9.fe_fin_acuerdo
	, e9.cod_producto
	, e9.nom_producto
	, e9.cod_contrato
	, e9.cod_componente
	, e9.des_contrato
	, e9.tipo_contrato
	, e8.tipo_documento_tercero
	, e8.num_documento_tercero
	, e4.nom_tercero
	, e4.cod_clase_reasegurador
	, e4.des_clase_reasegurador
	, e4.porcentaje_participacion
	, e9.fe_operacion
	, e9.pr_operacion
	, e9.cod_tipo_produccion
	, e9.des_tipo_produccion
	, e9.cod_unidad_negocio
	, e9.des_unidad_negocio
	, e9.cod_linea_credito
	, e9.des_linea_credito
	, e9.cod_moneda
	, e9.des_moneda
	, e9.contracttype
	, e4.reinsuranceoperationdetailid
	, e4.sumassured
	, e9.universo
;
commit;

create index acsele_alfa_bi.pc_13_idx_1
on acsele_alfa_bi.pc_13 (reinsuranceoperationdetailid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.pc_13
;


/* ========================================================================================== */
/* PASO 14 - RESULTADO ORO (EXT_12): primas, comisiones, depositos, retencion y libre de      */
/* prima cedida por tipo de contrato                                                          */
/*                                                                                            */
/* Mismas 31 expresiones del SP, mas la columna universo (MES / VIGENTE).                     */
/* ========================================================================================== */

/*
drop table acsele_alfa_bi.primas_cedidas;
commit;
*/

create table acsele_alfa_bi.primas_cedidas
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  e3.operacion
	, e3.poliza_matriz
	, e3.cod_compania
	, e3.nom_compania
	, e3.cod_ramo_tecnico
	, e3.nom_ramo_tecnico
	, e3.cod_ramo_contable
	, e3.nom_ramo_contable
	, e3.cod_sucursal
	, e3.nom_sucursal
	, e3.cod_tipo_poliza
	, e3.des_tipo_poliza
	, e3.cod_frecuencia_pago
	, e3.des_frecuencia_pago
	, e3.cod_acuerdo
	, e3.des_acuerdo
	, e3.fe_inicio_acuerdo
	, e3.fe_fin_acuerdo
	, e3.cod_producto
	, e3.nom_producto
	, e3.cod_contrato
	, e3.cod_componente
	, e3.des_contrato
	, e3.tipo_contrato
	, e3.tipo_documento_tercero
	, e3.num_documento_tercero
	, e3.nom_tercero
	, e3.cod_clase_reasegurador
	, e3.des_clase_reasegurador
	, e3.porcentaje_participacion
	, e3.fe_operacion
	, e3.pr_operacion
	, e3.cod_tipo_produccion
	, e3.des_tipo_produccion
	, e3.cod_unidad_negocio
	, e3.des_unidad_negocio
	, e3.cod_linea_credito
	, e3.des_linea_credito
	, e3.cod_moneda
	, e3.des_moneda
	, e3.universo
	, (case when upper(e6.ropp_name) in ('PRICEDIDA','PRICEDIDAIND') then e6.ropp_value else 0 end)		as prima_cedida
	, (case when upper(e6.ropp_name) in ('COMPRICEDIDA','COMPRICEDIDAIND') then e6.ropp_value else 0 end)		as comision_reaseguro
	, (case when upper(e6.ropp_name) in ('DEPPRICEDIDA','DEPPRICEDIDAIND') then e6.ropp_value else 0 end)		as valor_deposito_retenido
	, (case when upper(e6.ropp_name) in ('INTDEPPRICEDIDA','INTDEPPRICEDIDAIND') then e6.ropp_value else 0 end)		as valor_deposito_interes
	, (case when upper(e6.ropp_name) in ('RETPRIMACEDEXT','RETPRIMACEDINT','IMPUESTO','RETPRIMACEDEXTIND') then e6.ropp_value else 0 end)		as retencion_fuente
	, (case when upper(e6.ropp_name) in ('LIBDEPPRIMACEDIDA','LIBDEPPRIMACEDIDAIND') then e6.ropp_value else 0 end)		as libre_prima_cedida
	, (case when upper(e6.ropp_name) in ('PRICEDIDA','PRICEDIDAIND') and upper(e3.contracttype) = 'QUOTASHAREREINSURANCECONTRACT' then e6.ropp_value else 0 end)		as prima_cedida_cp
	, (case when upper(e6.ropp_name) in ('PRICEDIDA','PRICEDIDAIND') and upper(e3.contracttype) = 'MANDATORYFACULTATIVECONTRACT' then e6.ropp_value else 0 end)		as prima_cedida_fc
	, (case when upper(e6.ropp_name) in ('PRICEDIDA','PRICEDIDAIND') and upper(e3.contracttype) = 'EXCEDENTREINSURANCECONTRACT' then e6.ropp_value else 0 end)		as prima_cedida_ex
	, (case when upper(e6.ropp_name) in ('COMPRICEDIDA','COMPRICEDIDAIND') and upper(e3.contracttype) = 'QUOTASHAREREINSURANCECONTRACT' then e6.ropp_value else 0 end)		as comision_reaseguro_cp
	, (case when upper(e6.ropp_name) in ('COMPRICEDIDA','COMPRICEDIDAIND') and upper(e3.contracttype) = 'MANDATORYFACULTATIVECONTRACT' then e6.ropp_value else 0 end)		as comision_reaseguro_fc
	, (case when upper(e6.ropp_name) in ('COMPRICEDIDA','COMPRICEDIDAIND') and upper(e3.contracttype) = 'EXCEDENTREINSURANCECONTRACT' then e6.ropp_value else 0 end)		as comision_reaseguro_ex
	, (case when upper(e6.ropp_name) in ('DEPPRICEDIDA','DEPPRICEDIDAIND') and upper(e3.contracttype) = 'QUOTASHAREREINSURANCECONTRACT' then e6.ropp_value else 0 end)		as valor_deposito_retenido_cp
	, (case when upper(e6.ropp_name) in ('DEPPRICEDIDA','DEPPRICEDIDAIND') and upper(e3.contracttype) = 'MANDATORYFACULTATIVECONTRACT' then e6.ropp_value else 0 end)		as valor_deposito_retenido_fc
	, (case when upper(e6.ropp_name) in ('DEPPRICEDIDA','DEPPRICEDIDAIND') and upper(e3.contracttype) = 'EXCEDENTREINSURANCECONTRACT' then e6.ropp_value else 0 end)		as valor_deposito_retenido_ex
	, (case when upper(e6.ropp_name) in ('INTDEPPRICEDIDA','INTDEPPRICEDIDAIND') and upper(e3.contracttype) = 'QUOTASHAREREINSURANCECONTRACT' then e6.ropp_value else 0 end)		as valor_deposito_interes_cp
	, (case when upper(e6.ropp_name) in ('INTDEPPRICEDIDA','INTDEPPRICEDIDAIND') and upper(e3.contracttype) = 'MANDATORYFACULTATIVECONTRACT' then e6.ropp_value else 0 end)		as valor_deposito_interes_fc
	, (case when upper(e6.ropp_name) in ('INTDEPPRICEDIDA','INTDEPPRICEDIDAIND') and upper(e3.contracttype) = 'EXCEDENTREINSURANCECONTRACT' then e6.ropp_value else 0 end)		as valor_deposito_interes_ex
	, (case when upper(e6.ropp_name) in ('RETPRIMACEDEXT','RETPRIMACEDINT','IMPUESTO','RETPRIMACEDEXTIND') and upper(e3.contracttype) = 'QUOTASHAREREINSURANCECONTRACT' then e6.ropp_value else 0 end)		as retencion_fuente_cp
	, (case when upper(e6.ropp_name) in ('RETPRIMACEDEXT','RETPRIMACEDINT','IMPUESTO','RETPRIMACEDEXTIND') and upper(e3.contracttype) = 'MANDATORYFACULTATIVECONTRACT' then e6.ropp_value else 0 end)		as retencion_fuente_fc
	, (case when upper(e6.ropp_name) in ('RETPRIMACEDEXT','RETPRIMACEDINT','IMPUESTO','RETPRIMACEDEXTIND') and upper(e3.contracttype) = 'EXCEDENTREINSURANCECONTRACT' then e6.ropp_value else 0 end)		as retencion_fuente_ex
	, (case when upper(e6.ropp_name) in ('LIBDEPPRIMACEDIDA','LIBDEPPRIMACEDIDAIND') and upper(e3.contracttype) = 'QUOTASHAREREINSURANCECONTRACT' then e6.ropp_value else 0 end)		as libre_prima_cedida_cp
	, (case when upper(e6.ropp_name) in ('LIBDEPPRIMACEDIDA','LIBDEPPRIMACEDIDAIND') and upper(e3.contracttype) = 'MANDATORYFACULTATIVECONTRACT' then e6.ropp_value else 0 end)		as libre_prima_cedida_fc
	, (case when upper(e6.ropp_name) in ('LIBDEPPRIMACEDIDA','LIBDEPPRIMACEDIDAIND') and upper(e3.contracttype) = 'EXCEDENTREINSURANCECONTRACT' then e6.ropp_value else 0 end)		as libre_prima_cedida_ex
	, (case when upper(e6.ropp_name) in ('PRICEDIDA','PRICEDIDAIND') and upper(e3.contracttype) = 'QUOTASHAREREINSURANCECONTRACT' then e3.sumassured else 0 end)		as valor_asegurado_cp
	, (case when upper(e6.ropp_name) in ('PRICEDIDA','PRICEDIDAIND') and upper(e3.contracttype) = 'MANDATORYFACULTATIVECONTRACT' then e3.sumassured else 0 end)		as valor_asegurado_fc
	, (case when upper(e6.ropp_name) in ('PRICEDIDA','PRICEDIDAIND') and upper(e3.contracttype) = 'EXCEDENTREINSURANCECONTRACT' then e3.sumassured else 0 end)		as valor_asegurado_ex
	, e3.sumassured		as valor_asegurado_re
from acsele_alfa_bi.pc_13 e3
inner join acsele_alfa_bi.pc_9 e6
on e3.reinsuranceoperationdetailid = e6.rod_id
group by e3.operacion
	, e3.poliza_matriz
	, e3.cod_compania
	, e3.nom_compania
	, e3.cod_ramo_tecnico
	, e3.nom_ramo_tecnico
	, e3.cod_ramo_contable
	, e3.nom_ramo_contable
	, e3.cod_sucursal
	, e3.nom_sucursal
	, e3.cod_tipo_poliza
	, e3.des_tipo_poliza
	, e3.cod_frecuencia_pago
	, e3.des_frecuencia_pago
	, e3.cod_acuerdo
	, e3.des_acuerdo
	, e3.fe_inicio_acuerdo
	, e3.fe_fin_acuerdo
	, e3.cod_producto
	, e3.nom_producto
	, e3.cod_contrato
	, e3.cod_componente
	, e3.des_contrato
	, e3.tipo_contrato
	, e3.tipo_documento_tercero
	, e3.num_documento_tercero
	, e3.nom_tercero
	, e3.cod_clase_reasegurador
	, e3.des_clase_reasegurador
	, e3.porcentaje_participacion
	, e3.fe_operacion
	, e3.pr_operacion
	, e3.cod_tipo_produccion
	, e3.des_tipo_produccion
	, e3.cod_unidad_negocio
	, e3.des_unidad_negocio
	, e3.cod_linea_credito
	, e3.des_linea_credito
	, e3.cod_moneda
	, e3.des_moneda
	, e3.universo
	, (case when upper(e6.ropp_name) in ('PRICEDIDA','PRICEDIDAIND') then e6.ropp_value else 0 end)
	, (case when upper(e6.ropp_name) in ('COMPRICEDIDA','COMPRICEDIDAIND') then e6.ropp_value else 0 end)
	, (case when upper(e6.ropp_name) in ('DEPPRICEDIDA','DEPPRICEDIDAIND') then e6.ropp_value else 0 end)
	, (case when upper(e6.ropp_name) in ('INTDEPPRICEDIDA','INTDEPPRICEDIDAIND') then e6.ropp_value else 0 end)
	, (case when upper(e6.ropp_name) in ('RETPRIMACEDEXT','RETPRIMACEDINT','IMPUESTO','RETPRIMACEDEXTIND') then e6.ropp_value else 0 end)
	, (case when upper(e6.ropp_name) in ('LIBDEPPRIMACEDIDA','LIBDEPPRIMACEDIDAIND') then e6.ropp_value else 0 end)
	, (case when upper(e6.ropp_name) in ('PRICEDIDA','PRICEDIDAIND') and upper(e3.contracttype) = 'QUOTASHAREREINSURANCECONTRACT' then e6.ropp_value else 0 end)
	, (case when upper(e6.ropp_name) in ('PRICEDIDA','PRICEDIDAIND') and upper(e3.contracttype) = 'MANDATORYFACULTATIVECONTRACT' then e6.ropp_value else 0 end)
	, (case when upper(e6.ropp_name) in ('PRICEDIDA','PRICEDIDAIND') and upper(e3.contracttype) = 'EXCEDENTREINSURANCECONTRACT' then e6.ropp_value else 0 end)
	, (case when upper(e6.ropp_name) in ('COMPRICEDIDA','COMPRICEDIDAIND') and upper(e3.contracttype) = 'QUOTASHAREREINSURANCECONTRACT' then e6.ropp_value else 0 end)
	, (case when upper(e6.ropp_name) in ('COMPRICEDIDA','COMPRICEDIDAIND') and upper(e3.contracttype) = 'MANDATORYFACULTATIVECONTRACT' then e6.ropp_value else 0 end)
	, (case when upper(e6.ropp_name) in ('COMPRICEDIDA','COMPRICEDIDAIND') and upper(e3.contracttype) = 'EXCEDENTREINSURANCECONTRACT' then e6.ropp_value else 0 end)
	, (case when upper(e6.ropp_name) in ('DEPPRICEDIDA','DEPPRICEDIDAIND') and upper(e3.contracttype) = 'QUOTASHAREREINSURANCECONTRACT' then e6.ropp_value else 0 end)
	, (case when upper(e6.ropp_name) in ('DEPPRICEDIDA','DEPPRICEDIDAIND') and upper(e3.contracttype) = 'MANDATORYFACULTATIVECONTRACT' then e6.ropp_value else 0 end)
	, (case when upper(e6.ropp_name) in ('DEPPRICEDIDA','DEPPRICEDIDAIND') and upper(e3.contracttype) = 'EXCEDENTREINSURANCECONTRACT' then e6.ropp_value else 0 end)
	, (case when upper(e6.ropp_name) in ('INTDEPPRICEDIDA','INTDEPPRICEDIDAIND') and upper(e3.contracttype) = 'QUOTASHAREREINSURANCECONTRACT' then e6.ropp_value else 0 end)
	, (case when upper(e6.ropp_name) in ('INTDEPPRICEDIDA','INTDEPPRICEDIDAIND') and upper(e3.contracttype) = 'MANDATORYFACULTATIVECONTRACT' then e6.ropp_value else 0 end)
	, (case when upper(e6.ropp_name) in ('INTDEPPRICEDIDA','INTDEPPRICEDIDAIND') and upper(e3.contracttype) = 'EXCEDENTREINSURANCECONTRACT' then e6.ropp_value else 0 end)
	, (case when upper(e6.ropp_name) in ('RETPRIMACEDEXT','RETPRIMACEDINT','IMPUESTO','RETPRIMACEDEXTIND') and upper(e3.contracttype) = 'QUOTASHAREREINSURANCECONTRACT' then e6.ropp_value else 0 end)
	, (case when upper(e6.ropp_name) in ('RETPRIMACEDEXT','RETPRIMACEDINT','IMPUESTO','RETPRIMACEDEXTIND') and upper(e3.contracttype) = 'MANDATORYFACULTATIVECONTRACT' then e6.ropp_value else 0 end)
	, (case when upper(e6.ropp_name) in ('RETPRIMACEDEXT','RETPRIMACEDINT','IMPUESTO','RETPRIMACEDEXTIND') and upper(e3.contracttype) = 'EXCEDENTREINSURANCECONTRACT' then e6.ropp_value else 0 end)
	, (case when upper(e6.ropp_name) in ('LIBDEPPRIMACEDIDA','LIBDEPPRIMACEDIDAIND') and upper(e3.contracttype) = 'QUOTASHAREREINSURANCECONTRACT' then e6.ropp_value else 0 end)
	, (case when upper(e6.ropp_name) in ('LIBDEPPRIMACEDIDA','LIBDEPPRIMACEDIDAIND') and upper(e3.contracttype) = 'MANDATORYFACULTATIVECONTRACT' then e6.ropp_value else 0 end)
	, (case when upper(e6.ropp_name) in ('LIBDEPPRIMACEDIDA','LIBDEPPRIMACEDIDAIND') and upper(e3.contracttype) = 'EXCEDENTREINSURANCECONTRACT' then e6.ropp_value else 0 end)
	, (case when upper(e6.ropp_name) in ('PRICEDIDA','PRICEDIDAIND') and upper(e3.contracttype) = 'QUOTASHAREREINSURANCECONTRACT' then e3.sumassured else 0 end)
	, (case when upper(e6.ropp_name) in ('PRICEDIDA','PRICEDIDAIND') and upper(e3.contracttype) = 'MANDATORYFACULTATIVECONTRACT' then e3.sumassured else 0 end)
	, (case when upper(e6.ropp_name) in ('PRICEDIDA','PRICEDIDAIND') and upper(e3.contracttype) = 'EXCEDENTREINSURANCECONTRACT' then e3.sumassured else 0 end)
	, e3.sumassured
;
commit;

create index acsele_alfa_bi.primas_cedidas_idx_1
on acsele_alfa_bi.primas_cedidas (operacion);

create index acsele_alfa_bi.primas_cedidas_idx_2
on acsele_alfa_bi.primas_cedidas (cod_compania, cod_ramo_contable);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.primas_cedidas
;


/* ========================================================================================== */
/* VALIDACIONES                                                                               */
/* ========================================================================================== */

/* 1. Totales para la certificacion: prima cedida y comision por compania, ramo contable y tipo de contrato */
select /*+ parallel(3) */
	  pc.cod_compania
	, pc.nom_compania
	, pc.cod_ramo_contable
	, pc.nom_ramo_contable
	, pc.tipo_contrato
	, count(*)								as registros
	, sum(pc.prima_cedida)					as prima_cedida
	, sum(pc.comision_reaseguro)			as comision_reaseguro
	, sum(pc.retencion_fuente)				as retencion_fuente
from acsele_alfa_bi.primas_cedidas pc
group by pc.cod_compania
	, pc.nom_compania
	, pc.cod_ramo_contable
	, pc.nom_ramo_contable
	, pc.tipo_contrato
order by pc.cod_compania
	, pc.cod_ramo_contable
	, pc.tipo_contrato
;

/* 2. Cuadre contra el SP: correr sp_prima_cedida_alfa con el mismo periodo para cada compania y comparar con primas_cedidas
      filtrada por cod_compania. El SP solo toma operaciones del periodo: comparar contra universo = MES; lo VIGENTE es adicional. */
select /*+ parallel(3) */
	  pc.cod_compania
	, count(*)					as registros
	, sum(pc.prima_cedida)		as prima_cedida
from acsele_alfa_bi.primas_cedidas pc
group by pc.cod_compania
;

/* 3. Tipos de contrato sin homologar (deben ser cero) */
select /*+ parallel(3) */
	  pc.tipo_contrato
	, count(*) as registros
from acsele_alfa_bi.primas_cedidas pc
where pc.tipo_contrato = 'IDENTIFICAR TIPO'
group by pc.tipo_contrato
;
