/* ============================================================================================ */
/* VERSION 1.3 - 2026-11-09 (PCR): la extraccion de EMISIONES queda publicada para las mallas   */
/*    dependientes (Primas cedidas, Reaseguros, Primas PCR). Cambios solo aditivos; EMISIONES   */
/*    no cambia:                                                                                */
/*  - em_3: + prplc_tipopolizavalue, prplc_frecuenciapagovalue, prplc_monedavalue,              */
/*    prdct0_pro_dcoid.                                                                         */
/*  - em_tpb: + stpplcprtcptn_thirdpartyid, stpplcprtcptndco_dcoid, prthrdprty_pk,              */
/*    prthrdprty_tipodocumentoinput.                                                            */
/*  - em_pub_acu (nueva): acuerdo y version de acuerdo por operacion.                           */
/*  - Las tablas em_* NO se borran hasta que certifique la malla 4 (son la extraccion que       */
/*    reutilizan las mallas siguientes).                                                        */
/* ============================================================================================ */
/* ============================================================================================ */
/* E1 EMISIONES - CONSOLIDADO VIDA + GENERALES + SATELITE  ·  VERSION 1.0 - 2026-11-06          */
/* -------------------------------------------------------------------------------------------- */
/* Una sola salida (ACSELE_ALFA_BI.EMISIONES) con la consolidacion optima de los tres armados:  */
/*   E1 VIDA DEUDORES v1.0 · E1 GENERALES v1.1 · E1 SATELITE v1.0 (versiones por tablas).       */
/*                                                                                              */
/* Como se respeta el armado de cada negocio:                                                   */
/*  1. TRONCO COMUN (9 tablas em_*): lo que los tres leen igual se lee una sola vez y solo trae */
/*     datos crudos (superconjunto), sin reglas de negocio: pivote contextoperation +           */
/*    policydco +                                                                               */
/*     paso 3, universos de polizas y operaciones, bases de participantes de poliza, terceros,  */
/*     participantes del objeto asegurado y thirdpartymovementpolicy (la tabla mas grande).     */
/*  2. BIFURCACION: cada negocio sigue su propio armado desde el paso 4 (em_vd_* 13, em_gn_*    */
/*    19,                                                                                       */
/*     em_st_* 28). Cuando toma algo del tronco comun lo cruza primero contra SU universo de    */
/*     polizas / operaciones y despues aplica sus propios filtros, roles, conceptos y signos:   */
/*    cada                                                                                      */
/*     tabla del negocio queda igual a la de su corrida individual.                             */
/*  3. CONSOLIDACION: EMISIONES = UNION ALL de las tres salidas finales, con columnas           */
/*    homologadas                                                                               */
/*     (ORIGEN = compania VIDA / GENERALES; ESTANDAR_CONFIGURACION = VIDA_DEUDORES /          */
/*     GENERALES / SATELITE; 203 columnas). Lo que no aplica a                                  */
/*    un                                                                                        */
/*     negocio va en null. Ver la hoja Homologacion del diccionario.                            */
/* -------------------------------------------------------------------------------------------- */
/* Reglas del modelo: pivote -> dependientes directas -> dependientes de las dependientes;      */
/*    inner                                                                                     */
/* antes de left; cero subqueries; sin KEEP / OVER / DENSE_RANK; sin funciones PL/SQL; todas    */
/*    las                                                                                       */
/* tablas con esquema (acsele_alfa / acsele_alfa_bi); lectura hint parallel(3) / escritura      */
/*    parallel 2.                                                                               */
/* -------------------------------------------------------------------------------------------- */
/* Periodo UNICO para los tres negocios: cambiar las fechas de em_1 (hoy 01/10/2025 -           */
/*    31/10/2025).                                                                              */
/* Ojo: en E1 Vida v1.0 el filtro de fechas estaba comentado (todo el historico); aqui va el    */
/*    periodo.                                                                                  */
/* ============================================================================================ */



/* ============================================================================================ */
/* ##############################  TRONCO COMUN  ##############################                 */
/* ============================================================================================ */

/* ============================================================================================ */
/* COMUN PASO 1 - PIVOTE: contextoperation (status = 2 y periodo de cierre, uno solo para los   */
/*    tres negocios)                                                                            */
/*                                                                                              */
/* Mismo pivote de los tres negocios. En Vida v1.0 el filtro de fechas estaba comentado; aquí   */
/*    se aplica el periodo único.                                                               */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_1;
commit;
*/

create table acsele_alfa_bi.em_1
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  ctx.id				as ctx_id
	, ctx.time_stamp				as ctx_time_stamp
	, ctx.status				as ctx_status
	, 'MES'				as ctx_universo
from acsele_alfa.contextoperation ctx
inner join acsele_alfa_bi.cfg_pcr_periodo p
on ctx.time_stamp between p.fecha_inicio and p.fecha_fin
where ctx.status = 2
union all
select /*+ parallel(3) */
	  co.id
	, co.time_stamp
	, co.status
	, 'VIGENTE'
from acsele_alfa.riskunitdco rdco
inner join acsele_alfa_bi.cfg_pcr_periodo p
on rdco.finishdate >= p.fecha_fin
inner join acsele_alfa.stps_relationagreementpolicy rap
on rap.rsa_policyid = rdco.agregatedparentid
inner join acsele_alfa.stag_agreementversion av
on av.ast_id = rap.rsa_ast_id
and rdco.initialdate >= av.ast_fecini
and rdco.initialdate <= av.ast_fecfin
inner join acsele_alfa.preagreement prea
on prea.pk = av.dcoid
inner join acsele_alfa.contextoperation co
on co.id = rdco.operationpk
and co.time_stamp < p.fecha_inicio
where prea.frecuenciapagoinput != 'Mensual'
and co.status = 2
group by co.id
	, co.time_stamp
	, co.status
;
commit;

create index acsele_alfa_bi.em_1_idx_1
on acsele_alfa_bi.em_1 (ctx_id);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_1
;


/* ============================================================================================ */
/* COMUN PASO 2 - DEPENDIENTE DIRECTA DEL PIVOTE: policydco                                     */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_2;
commit;
*/

create table acsele_alfa_bi.em_2
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  ctx.ctx_id
	, ctx.ctx_time_stamp
	, ctx.ctx_status
	, plcdco.operationpk				as plcdco_operationpk
	, plcdco.dcoid				as plcdco_dcoid
	, plcdco.agregatedobjectid				as plcdco_agregatedobjectid
	, plcdco.iddcoevent				as plcdco_iddcoevent
	, plcdco.initialdate				as plcdco_initialdate
	, plcdco.finishdate				as plcdco_finishdate
	, plcdco.event				as plcdco_event
from acsele_alfa_bi.em_1 ctx
inner join acsele_alfa.policydco plcdco
on ctx.ctx_id = plcdco.operationpk
;
commit;

create index acsele_alfa_bi.em_2_idx_1
on acsele_alfa_bi.em_2 (plcdco_agregatedobjectid);

create index acsele_alfa_bi.em_2_idx_2
on acsele_alfa_bi.em_2 (plcdco_dcoid);

create index acsele_alfa_bi.em_2_idx_3
on acsele_alfa_bi.em_2 (plcdco_operationpk, plcdco_iddcoevent);

create index acsele_alfa_bi.em_2_idx_4
on acsele_alfa_bi.em_2 (plcdco_operationpk);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_2
;


/* ============================================================================================ */
/* COMUN PASO 3 - DEPENDIENTES DE policydco: agregatedpolicy (productos de los 3 negocios),     */
/*    prepolicy, eventdco, riskunitdco                                                          */
/*                                                                                              */
/* Superconjunto de los tres negocios: solo acota productos. product va por LEFT (llave única)  */
/*    para no quitarle filas a Vida ni a Generales; el filtro exacto de cada negocio se vuelve  */
/*    a aplicar en su paso 4.                                                                   */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_3;
commit;
*/

create table acsele_alfa_bi.em_3
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  ctx.ctx_id
	, ctx.ctx_time_stamp
	, ctx.ctx_status
	, ctx.plcdco_operationpk
	, ctx.plcdco_dcoid
	, ctx.plcdco_agregatedobjectid
	, ctx.plcdco_iddcoevent
	, ctx.plcdco_initialdate
	, ctx.plcdco_finishdate
	, ctx.plcdco_event
	, grgtdplc.agregatedpolicyid				as grgtdplc_agregatedpolicyid
	, grgtdplc.productid				as grgtdplc_productid
	, prplc.pk				as prplc_pk
	, prplc.static				as prplc_static
	, prplc.oficinavalue				as prplc_oficinavalue
	, prplc.oficinainput				as prplc_oficinainput
	, prplc.tipopolizainput				as prplc_tipopolizainput
	, prplc.numeropolizainput				as prplc_numeropolizainput
	, prplc.monedainput				as prplc_monedainput
	, prplc.frecuenciapagoinput				as prplc_frecuenciapagoinput
	, vntdco.eventtypeid				as vntdco_eventtypeid
	, rskntdco.operationpk				as rskntdco_operationpk
	, rskntdco.dcoid				as rskntdco_dcoid
	, rskntdco.agregatedobjectid				as rskntdco_agregatedobjectid
	, rskntdco.initialdate				as rskntdco_initialdate
	, rskntdco.finishdate				as rskntdco_finishdate
	, vntdco.iddcoevent				as vntdco_iddcoevent
	, prplc.canalventainput				as prplc_canalventainput
	, prplc.tipopolizavalue				as prplc_tipopolizavalue
	, prplc.frecuenciapagovalue				as prplc_frecuenciapagovalue
	, prplc.monedavalue				as prplc_monedavalue
	, prdct0.pro_dcoid				as prdct0_pro_dcoid
from acsele_alfa_bi.em_2 ctx
inner join acsele_alfa.agregatedpolicy grgtdplc
on ctx.plcdco_agregatedobjectid = grgtdplc.agregatedpolicyid
inner join acsele_alfa.prepolicy prplc
on ctx.plcdco_dcoid = prplc.pk
inner join acsele_alfa.eventdco vntdco
on ctx.plcdco_operationpk = vntdco.operationpk
and ctx.plcdco_iddcoevent = vntdco.iddcoevent
inner join acsele_alfa.riskunitdco rskntdco
on ctx.plcdco_operationpk = rskntdco.operationpk
left join acsele_alfa.product prdct0
on grgtdplc.productid = prdct0.productid
left join acsele_alfa_bi.cfg_pcr_prod_id cpi
on grgtdplc.productid = cpi.productid
left join acsele_alfa_bi.cfg_pcr_prod_desc cpd
on prdct0.description = cpd.descripcion_producto
where (cpi.productid is not null
	or cpd.descripcion_producto is not null)		-- productos activos del catalogo (cfg_pcr_producto): por id o por descripcion
;
commit;

create index acsele_alfa_bi.em_3_idx_1
on acsele_alfa_bi.em_3 (grgtdplc_productid);

create index acsele_alfa_bi.em_3_idx_2
on acsele_alfa_bi.em_3 (prplc_static);

create index acsele_alfa_bi.em_3_idx_3
on acsele_alfa_bi.em_3 (rskntdco_dcoid);

create index acsele_alfa_bi.em_3_idx_4
on acsele_alfa_bi.em_3 (grgtdplc_agregatedpolicyid, rskntdco_agregatedobjectid);

create index acsele_alfa_bi.em_3_idx_5
on acsele_alfa_bi.em_3 (vntdco_eventtypeid);

create index acsele_alfa_bi.em_3_idx_6
on acsele_alfa_bi.em_3 (grgtdplc_agregatedpolicyid);

create index acsele_alfa_bi.em_3_idx_7
on acsele_alfa_bi.em_3 (rskntdco_agregatedobjectid, plcdco_operationpk);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_3
;



/* ============================================================================================ */
/* ##############################  RAMA VIDA  ##############################                    */
/* ============================================================================================ */

/* ============================================================================================ */
/* VIDA · PASO 4 - DEPENDIENTES DEL PASO 3:                                                     */
/*                                                                                              */
/* Lee del tronco común em_3 y aplica el filtro exacto de productos de Vida.                    */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_vd_4;
commit;
*/

create table acsele_alfa_bi.em_vd_4
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  ctx.ctx_id
	, ctx.ctx_time_stamp
	, ctx.ctx_status
	, ctx.plcdco_operationpk
	, ctx.plcdco_dcoid
	, ctx.plcdco_agregatedobjectid
	, ctx.plcdco_iddcoevent
	, ctx.plcdco_initialdate
	, ctx.plcdco_finishdate
	, ctx.plcdco_event
	, ctx.grgtdplc_agregatedpolicyid
	, ctx.grgtdplc_productid
	, ctx.prplc_pk
	, ctx.prplc_static
	, ctx.prplc_oficinavalue
	, ctx.prplc_oficinainput
	, ctx.prplc_tipopolizainput
	, ctx.prplc_numeropolizainput
	, ctx.prplc_monedainput
	, ctx.prplc_frecuenciapagoinput
	, ctx.vntdco_eventtypeid
	, ctx.rskntdco_operationpk
	, ctx.rskntdco_dcoid
	, ctx.rskntdco_agregatedobjectid
	, ctx.rskntdco_initialdate
	, ctx.rskntdco_finishdate
	, prprdct.static						as prprdct_static
	, prprdct.companiavalue					as prprdct_companiavalue
	, prprdct.companiainput					as prprdct_companiainput
	, prprdct.descripcionproductoinput		as prprdct_descripcionproductoinput
	, prprdct.ramovalue						as prprdct_ramovalue
	, prprdct.ramoinput						as prprdct_ramoinput
	, stpsrltngrmntplc.rsa_ast_id			as stpsrltngrmntplc_rsa_ast_id
	, stpsrltngrmntplc.rsa_policyid			as stpsrltngrmntplc_rsa_policyid
	, rvddrs.numerocreditoinput				as rvddrs_numerocreditoinput
	, rvddrs.fechadesembolsoinput			as rvddrs_fechadesembolsoinput	---- nuevo gustavo 4dic23
from acsele_alfa_bi.em_3 ctx
inner join acsele_alfa.preproduct prprdct
on ctx.grgtdplc_productid = prprdct.static
inner join acsele_alfa_bi.cfg_pcr_producto cfgp
on cfgp.productid = prprdct.static
and cfgp.companiavalue = prprdct.companiavalue
and cfgp.estandar = 'VIDA_DEUDORES'
and cfgp.activo = 'S'
inner join acsele_alfa.stps_relationagreementpolicy stpsrltngrmntplc
on ctx.prplc_static = stpsrltngrmntplc.rsa_policyid
inner join acsele_alfa.urvdeudores rvddrs
on ctx.rskntdco_dcoid = rvddrs.pk
;
commit;

create index acsele_alfa_bi.em_vd_4_idx_1
on acsele_alfa_bi.em_vd_4 (prprdct_static);

create index acsele_alfa_bi.em_vd_4_idx_2
on acsele_alfa_bi.em_vd_4 (stpsrltngrmntplc_rsa_ast_id);

create index acsele_alfa_bi.em_vd_4_idx_3
on acsele_alfa_bi.em_vd_4 (rskntdco_agregatedobjectid, plcdco_operationpk);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_vd_4
;


/* ============================================================================================ */
/* VIDA · PASO 5 - DEPENDIENTES DEL PASO 4:                                                     */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_vd_5;
commit;
*/

create table acsele_alfa_bi.em_vd_5
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  ctx.ctx_id
	, ctx.ctx_time_stamp
	, ctx.ctx_status
	, ctx.plcdco_operationpk
	, ctx.plcdco_dcoid
	, ctx.plcdco_agregatedobjectid
	, ctx.plcdco_iddcoevent
	, ctx.plcdco_initialdate
	, ctx.plcdco_finishdate
	, ctx.plcdco_event
	, ctx.grgtdplc_agregatedpolicyid
	, ctx.grgtdplc_productid
	, ctx.prplc_pk
	, ctx.prplc_static
	, ctx.prplc_oficinavalue
	, ctx.prplc_oficinainput
	, ctx.prplc_tipopolizainput
	, ctx.prplc_numeropolizainput
	, ctx.prplc_monedainput
	, ctx.prplc_frecuenciapagoinput
	, ctx.vntdco_eventtypeid
	, ctx.rskntdco_operationpk
	, ctx.rskntdco_dcoid
	, ctx.rskntdco_agregatedobjectid
	, ctx.rskntdco_initialdate
	, ctx.rskntdco_finishdate
	, ctx.prprdct_static
	, ctx.prprdct_companiavalue
	, ctx.prprdct_companiainput
	, ctx.prprdct_descripcionproductoinput
	, ctx.prprdct_ramovalue
	, ctx.prprdct_ramoinput
	, ctx.stpsrltngrmntplc_rsa_ast_id
	, ctx.stpsrltngrmntplc_rsa_policyid
	, ctx.rvddrs_numerocreditoinput
	, ctx.rvddrs_fechadesembolsoinput
	, prdct.description						as prdct_description
	, stpsgrmntsttcdt.ast_id				as stpsgrmntsttcdt_ast_id
	, stpsgrmntsttcdt.ast_numeroacu			as stpsgrmntsttcdt_ast_numeroacu
	, min(nsrncbjctdco.agregatedobjectid)	as nsrncbjctdco_min_objid
from acsele_alfa_bi.em_vd_4 ctx
inner join acsele_alfa.product prdct
on ctx.prprdct_static = prdct.productid
inner join acsele_alfa.stps_agreementstaticdat stpsgrmntsttcdt
on ctx.stpsrltngrmntplc_rsa_ast_id = stpsgrmntsttcdt.ast_id
inner join acsele_alfa.insuranceobjectdco nsrncbjctdco
on ctx.rskntdco_agregatedobjectid = nsrncbjctdco.agregatedparentid
and ctx.plcdco_operationpk = nsrncbjctdco.operationpk
--and nsrncbjctdco.status = 1		-- <== descomentar si el minimo debe ser solo sobre activos
group by ctx.ctx_id
	, ctx.ctx_time_stamp
	, ctx.ctx_status
	, ctx.plcdco_operationpk
	, ctx.plcdco_dcoid
	, ctx.plcdco_agregatedobjectid
	, ctx.plcdco_iddcoevent
	, ctx.plcdco_initialdate
	, ctx.plcdco_finishdate
	, ctx.plcdco_event
	, ctx.grgtdplc_agregatedpolicyid
	, ctx.grgtdplc_productid
	, ctx.prplc_pk
	, ctx.prplc_static
	, ctx.prplc_oficinavalue
	, ctx.prplc_oficinainput
	, ctx.prplc_tipopolizainput
	, ctx.prplc_numeropolizainput
	, ctx.prplc_monedainput
	, ctx.prplc_frecuenciapagoinput
	, ctx.vntdco_eventtypeid
	, ctx.rskntdco_operationpk
	, ctx.rskntdco_dcoid
	, ctx.rskntdco_agregatedobjectid
	, ctx.rskntdco_initialdate
	, ctx.rskntdco_finishdate
	, ctx.prprdct_static
	, ctx.prprdct_companiavalue
	, ctx.prprdct_companiainput
	, ctx.prprdct_descripcionproductoinput
	, ctx.prprdct_ramovalue
	, ctx.prprdct_ramoinput
	, ctx.stpsrltngrmntplc_rsa_ast_id
	, ctx.stpsrltngrmntplc_rsa_policyid
	, ctx.rvddrs_numerocreditoinput
	, ctx.rvddrs_fechadesembolsoinput
	, prdct.description
	, stpsgrmntsttcdt.ast_id
	, stpsgrmntsttcdt.ast_numeroacu
;
commit;

create index acsele_alfa_bi.em_vd_5_idx_1
on acsele_alfa_bi.em_vd_5 (nsrncbjctdco_min_objid, rskntdco_agregatedobjectid, plcdco_operationpk);

create index acsele_alfa_bi.em_vd_5_idx_2
on acsele_alfa_bi.em_vd_5 (stpsgrmntsttcdt_ast_id, rskntdco_initialdate);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_vd_5
;


/* ============================================================================================ */
/* VIDA · PASO 6 - DEPENDIENTES DEL PASO 5:                                                     */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_vd_6;
commit;
*/

create table acsele_alfa_bi.em_vd_6
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  ctx.ctx_id
	, ctx.ctx_time_stamp
	, ctx.ctx_status
	, ctx.plcdco_operationpk
	, ctx.plcdco_dcoid
	, ctx.plcdco_agregatedobjectid
	, ctx.plcdco_iddcoevent
	, ctx.plcdco_initialdate
	, ctx.plcdco_finishdate
	, ctx.plcdco_event
	, ctx.grgtdplc_agregatedpolicyid
	, ctx.grgtdplc_productid
	, ctx.prplc_pk
	, ctx.prplc_static
	, ctx.prplc_oficinavalue
	, ctx.prplc_oficinainput
	, ctx.prplc_tipopolizainput
	, ctx.prplc_numeropolizainput
	, ctx.prplc_monedainput
	, ctx.prplc_frecuenciapagoinput
	, ctx.vntdco_eventtypeid
	, ctx.rskntdco_operationpk
	, ctx.rskntdco_dcoid
	, ctx.rskntdco_agregatedobjectid
	, ctx.rskntdco_initialdate
	, ctx.rskntdco_finishdate
	, ctx.prprdct_static
	, ctx.prprdct_companiavalue
	, ctx.prprdct_companiainput
	, ctx.prprdct_descripcionproductoinput
	, ctx.prprdct_ramovalue
	, ctx.prprdct_ramoinput
	, ctx.stpsrltngrmntplc_rsa_ast_id
	, ctx.stpsrltngrmntplc_rsa_policyid
	, ctx.rvddrs_numerocreditoinput
	, ctx.rvddrs_fechadesembolsoinput
	, ctx.prdct_description
	, ctx.stpsgrmntsttcdt_ast_id
	, ctx.stpsgrmntsttcdt_ast_numeroacu
	, nsrncbjctdco.agregatedobjectid		as nsrncbjctdco_agregatedobjectid
	, nsrncbjctdco.dcoid					as nsrncbjctdco_dcoid
	, staggrmntvrsn.ast_id					as staggrmntvrsn_ast_id
	, staggrmntvrsn.dcoid					as staggrmntvrsn_dcoid
	, staggrmntvrsn.ast_fecini				as staggrmntvrsn_ast_fecini
	, staggrmntvrsn.ast_fecfin				as staggrmntvrsn_ast_fecfin
	, staggrmntvrsn.description				as staggrmntvrsn_description
	, oavddrs.edadvalue						as oavddrs_edadvalue					--inclusion nueva sfc jerg 11abril23
	, oavddrs.fechanacimientoaseginput		as oavddrs_fechanacimientoaseginput	--inclusion nueva sfc jerg 11abril23
	, oavddrs.direccionaseguradoinput		as oavddrs_direccionaseguradoinput		--inclusion nueva sfc jerg 11abril23
	, oavddrs.departamentoinput				as oavddrs_departamentoinput			--inclusion nueva sfc jerg 11abril23
	, oavddrs.departamentovalue				as oavddrs_departamentovalue			--inclusion nueva sfc jerg 11abril23
	, oavddrs.ciudadinput					as oavddrs_ciudadinput					--inclusion nueva sfc jerg 11abril23
	, oavddrs.ciudadvalue					as oavddrs_ciudadvalue					--inclusion nueva sfc jerg 11abril23
	, oavddrs.factorvidavalue				as oavddrs_factorvidavalue				--inclusion nueva sfc jerg 11abril23
from acsele_alfa_bi.em_vd_5 ctx
inner join acsele_alfa.insuranceobjectdco nsrncbjctdco
on ctx.nsrncbjctdco_min_objid = nsrncbjctdco.agregatedobjectid
and ctx.rskntdco_agregatedobjectid = nsrncbjctdco.agregatedparentid
and ctx.plcdco_operationpk = nsrncbjctdco.operationpk
and nsrncbjctdco.status = 1
inner join acsele_alfa.stag_agreementversion staggrmntvrsn
on ctx.stpsgrmntsttcdt_ast_id = staggrmntvrsn.ast_id
and staggrmntvrsn.ast_fecini <= ctx.rskntdco_initialdate
and staggrmntvrsn.ast_fecfin >= ctx.rskntdco_initialdate
inner join acsele_alfa.oavdeudores oavddrs
on nsrncbjctdco.dcoid = oavddrs.pk		--and indaseguradoprimariovalue = '1.0'
;
commit;

create index acsele_alfa_bi.em_vd_6_idx_1
on acsele_alfa_bi.em_vd_6 (staggrmntvrsn_dcoid);

create index acsele_alfa_bi.em_vd_6_idx_2
on acsele_alfa_bi.em_vd_6 (rskntdco_operationpk);

create index acsele_alfa_bi.em_vd_6_idx_3
on acsele_alfa_bi.em_vd_6 (vntdco_eventtypeid);

create index acsele_alfa_bi.em_vd_6_idx_4
on acsele_alfa_bi.em_vd_6 (grgtdplc_agregatedpolicyid);

create index acsele_alfa_bi.em_vd_6_idx_5
on acsele_alfa_bi.em_vd_6 (plcdco_operationpk);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_vd_6
;


/* ============================================================================================ */
/* VIDA · AUX POL - Universo de polizas (pivote de participantes de poliza)                     */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_vd_pol;
commit;
*/

create table acsele_alfa_bi.em_vd_pol
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */ distinct
	  ctx.grgtdplc_agregatedpolicyid
from acsele_alfa_bi.em_vd_6 ctx
;
commit;

create index acsele_alfa_bi.em_vd_pol_idx_1
on acsele_alfa_bi.em_vd_pol (grgtdplc_agregatedpolicyid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_vd_pol
;


/* ============================================================================================ */
/* VIDA · AUX OPR - Universo de operaciones (pivote de participantes de objeto asegurado)       */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_vd_opr;
commit;
*/

create table acsele_alfa_bi.em_vd_opr
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */ distinct
	  ctx.plcdco_operationpk
from acsele_alfa_bi.em_vd_6 ctx
;
commit;

create index acsele_alfa_bi.em_vd_opr_idx_1
on acsele_alfa_bi.em_vd_opr (plcdco_operationpk);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_vd_opr
;



/* ============================================================================================ */
/* ##############################  RAMA GENERALES  ##############################               */
/* ============================================================================================ */

/* ============================================================================================ */
/* GENERALES · PASO 4 - DEPENDIENTES DEL PASO 3: preproduct, stps_relationagreementpolicy,      */
/*                                                                                              */
/* Lee del tronco común em_3 y aplica el filtro exacto de productos de Generales.               */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_gn_4;
commit;
*/

create table acsele_alfa_bi.em_gn_4
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  ctx.ctx_id
	, ctx.ctx_time_stamp
	, ctx.ctx_status
	, ctx.plcdco_operationpk
	, ctx.plcdco_dcoid
	, ctx.plcdco_agregatedobjectid
	, ctx.plcdco_iddcoevent
	, ctx.plcdco_initialdate
	, ctx.plcdco_finishdate
	, ctx.plcdco_event
	, ctx.grgtdplc_agregatedpolicyid
	, ctx.grgtdplc_productid
	, ctx.prplc_pk
	, ctx.prplc_oficinavalue
	, ctx.prplc_oficinainput
	, ctx.prplc_tipopolizainput
	, ctx.prplc_numeropolizainput
	, ctx.prplc_monedainput
	, ctx.vntdco_eventtypeid
	, ctx.vntdco_iddcoevent
	, ctx.rskntdco_operationpk
	, ctx.rskntdco_dcoid
	, ctx.rskntdco_agregatedobjectid
	, ctx.rskntdco_initialdate
	, ctx.rskntdco_finishdate
	, prprdct.static				as prprdct_static
	, prprdct.companiavalue				as prprdct_companiavalue
	, prprdct.companiainput				as prprdct_companiainput
	, prprdct.codigoproductoinput				as prprdct_codigoproductoinput
	, prprdct.ramovalue				as prprdct_ramovalue
	, prprdct.ramoinput				as prprdct_ramoinput
	, stpsrltngrmntplc.rsa_ast_id				as stpsrltngrmntplc_rsa_ast_id
	, grgtdrsknt.agregatedriskunitid				as grgtdrsknt_agregatedriskunitid
	, prrsknts.numunidriesgoinput				as prrsknts_numunidriesgoinput
	, vnttp.description				as vnttp_description
from acsele_alfa_bi.em_3 ctx
inner join acsele_alfa.preproduct prprdct
on ctx.grgtdplc_productid = prprdct.static
inner join acsele_alfa_bi.cfg_pcr_producto cfgp
on cfgp.productid = prprdct.static
and cfgp.companiainput = prprdct.companiainput
and cfgp.estandar = 'GENERALES'
and cfgp.activo = 'S'
inner join acsele_alfa.stps_relationagreementpolicy stpsrltngrmntplc
on ctx.grgtdplc_agregatedpolicyid = stpsrltngrmntplc.rsa_policyid
inner join acsele_alfa.agregatedriskunit grgtdrsknt
on ctx.grgtdplc_agregatedpolicyid = grgtdrsknt.agregatedpolicyid
and ctx.rskntdco_agregatedobjectid = grgtdrsknt.agregatedriskunitid
inner join acsele_alfa.preriskunits prrsknts
on ctx.rskntdco_dcoid = prrsknts.pk
inner join acsele_alfa.eventtype vnttp
on ctx.vntdco_eventtypeid = vnttp.eventtypeid
and vnttp.description not in ('ActualizarGeoreferenciacion')
;
commit;

create index acsele_alfa_bi.em_gn_4_idx_1
on acsele_alfa_bi.em_gn_4 (prprdct_static);

create index acsele_alfa_bi.em_gn_4_idx_2
on acsele_alfa_bi.em_gn_4 (stpsrltngrmntplc_rsa_ast_id);

create index acsele_alfa_bi.em_gn_4_idx_3
on acsele_alfa_bi.em_gn_4 (rskntdco_agregatedobjectid, plcdco_operationpk);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_gn_4
;


/* ============================================================================================ */
/* GENERALES · PASO 5 - DEPENDIENTES DEL PASO 4: product, stps_agreementstaticdat,              */
/*    insuranceobjectdco ->                                                                     */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_gn_5;
commit;
*/

create table acsele_alfa_bi.em_gn_5
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  ctx.ctx_id
	, ctx.ctx_time_stamp
	, ctx.ctx_status
	, ctx.plcdco_operationpk
	, ctx.plcdco_dcoid
	, ctx.plcdco_agregatedobjectid
	, ctx.plcdco_iddcoevent
	, ctx.plcdco_initialdate
	, ctx.plcdco_finishdate
	, ctx.plcdco_event
	, ctx.grgtdplc_agregatedpolicyid
	, ctx.grgtdplc_productid
	, ctx.prplc_pk
	, ctx.prplc_oficinavalue
	, ctx.prplc_oficinainput
	, ctx.prplc_tipopolizainput
	, ctx.prplc_numeropolizainput
	, ctx.prplc_monedainput
	, ctx.vntdco_eventtypeid
	, ctx.vntdco_iddcoevent
	, ctx.rskntdco_operationpk
	, ctx.rskntdco_dcoid
	, ctx.rskntdco_agregatedobjectid
	, ctx.rskntdco_initialdate
	, ctx.rskntdco_finishdate
	, ctx.prprdct_static
	, ctx.prprdct_companiavalue
	, ctx.prprdct_companiainput
	, ctx.prprdct_codigoproductoinput
	, ctx.prprdct_ramovalue
	, ctx.prprdct_ramoinput
	, ctx.stpsrltngrmntplc_rsa_ast_id
	, ctx.grgtdrsknt_agregatedriskunitid
	, ctx.prrsknts_numunidriesgoinput
	, ctx.vnttp_description
	, prdct.description				as prdct_description
	, stpsgrmntsttcdt.ast_id				as stpsgrmntsttcdt_ast_id
	, stpsgrmntsttcdt.ast_numeroacu				as stpsgrmntsttcdt_ast_numeroacu
	, stpsgrmntsttcdt.ast_nombreacu				as stpsgrmntsttcdt_ast_nombreacu
	, min(nsrncbjctdco.agregatedobjectid)	as nsrncbjctdco_min_objid
from acsele_alfa_bi.em_gn_4 ctx
inner join acsele_alfa.product prdct
on ctx.prprdct_static = prdct.productid
inner join acsele_alfa.stps_agreementstaticdat stpsgrmntsttcdt
on ctx.stpsrltngrmntplc_rsa_ast_id = stpsgrmntsttcdt.ast_id
inner join acsele_alfa.insuranceobjectdco nsrncbjctdco
on ctx.rskntdco_agregatedobjectid = nsrncbjctdco.agregatedparentid
and ctx.plcdco_operationpk = nsrncbjctdco.operationpk
--and nsrncbjctdco.status = 1		-- <== descomentar si el minimo debe ser solo sobre activos
group by ctx.ctx_id
	, ctx.ctx_time_stamp
	, ctx.ctx_status
	, ctx.plcdco_operationpk
	, ctx.plcdco_dcoid
	, ctx.plcdco_agregatedobjectid
	, ctx.plcdco_iddcoevent
	, ctx.plcdco_initialdate
	, ctx.plcdco_finishdate
	, ctx.plcdco_event
	, ctx.grgtdplc_agregatedpolicyid
	, ctx.grgtdplc_productid
	, ctx.prplc_pk
	, ctx.prplc_oficinavalue
	, ctx.prplc_oficinainput
	, ctx.prplc_tipopolizainput
	, ctx.prplc_numeropolizainput
	, ctx.prplc_monedainput
	, ctx.vntdco_eventtypeid
	, ctx.vntdco_iddcoevent
	, ctx.rskntdco_operationpk
	, ctx.rskntdco_dcoid
	, ctx.rskntdco_agregatedobjectid
	, ctx.rskntdco_initialdate
	, ctx.rskntdco_finishdate
	, ctx.prprdct_static
	, ctx.prprdct_companiavalue
	, ctx.prprdct_companiainput
	, ctx.prprdct_codigoproductoinput
	, ctx.prprdct_ramovalue
	, ctx.prprdct_ramoinput
	, ctx.stpsrltngrmntplc_rsa_ast_id
	, ctx.grgtdrsknt_agregatedriskunitid
	, ctx.prrsknts_numunidriesgoinput
	, ctx.vnttp_description
	, prdct.description
	, stpsgrmntsttcdt.ast_id
	, stpsgrmntsttcdt.ast_numeroacu
	, stpsgrmntsttcdt.ast_nombreacu
;
commit;

create index acsele_alfa_bi.em_gn_5_idx_1
on acsele_alfa_bi.em_gn_5 (nsrncbjctdco_min_objid, rskntdco_agregatedobjectid, plcdco_operationpk);

create index acsele_alfa_bi.em_gn_5_idx_2
on acsele_alfa_bi.em_gn_5 (stpsgrmntsttcdt_ast_id, rskntdco_initialdate);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_gn_5
;


/* ============================================================================================ */
/* GENERALES · PASO 6 - DEPENDIENTES DEL PASO 5: insuranceobjectdco (registro del MIN, activo), */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_gn_6;
commit;
*/

create table acsele_alfa_bi.em_gn_6
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  ctx.ctx_id
	, ctx.ctx_time_stamp
	, ctx.ctx_status
	, ctx.plcdco_operationpk
	, ctx.plcdco_dcoid
	, ctx.plcdco_agregatedobjectid
	, ctx.plcdco_iddcoevent
	, ctx.plcdco_initialdate
	, ctx.plcdco_finishdate
	, ctx.plcdco_event
	, ctx.grgtdplc_agregatedpolicyid
	, ctx.grgtdplc_productid
	, ctx.prplc_pk
	, ctx.prplc_oficinavalue
	, ctx.prplc_oficinainput
	, ctx.prplc_tipopolizainput
	, ctx.prplc_numeropolizainput
	, ctx.prplc_monedainput
	, ctx.vntdco_eventtypeid
	, ctx.vntdco_iddcoevent
	, ctx.rskntdco_operationpk
	, ctx.rskntdco_dcoid
	, ctx.rskntdco_agregatedobjectid
	, ctx.rskntdco_initialdate
	, ctx.rskntdco_finishdate
	, ctx.prprdct_static
	, ctx.prprdct_companiavalue
	, ctx.prprdct_companiainput
	, ctx.prprdct_codigoproductoinput
	, ctx.prprdct_ramovalue
	, ctx.prprdct_ramoinput
	, ctx.stpsrltngrmntplc_rsa_ast_id
	, ctx.grgtdrsknt_agregatedriskunitid
	, ctx.prrsknts_numunidriesgoinput
	, ctx.vnttp_description
	, ctx.prdct_description
	, ctx.stpsgrmntsttcdt_ast_id
	, ctx.stpsgrmntsttcdt_ast_numeroacu
	, ctx.stpsgrmntsttcdt_ast_nombreacu
	, ctx.nsrncbjctdco_min_objid
	, nsrncbjctdco.agregatedobjectid				as nsrncbjctdco_agregatedobjectid
	, nsrncbjctdco.dcoid				as nsrncbjctdco_dcoid
	, staggrmntvrsn.dcoid				as staggrmntvrsn_dcoid
	, staggrmntvrsn.ast_fecini				as staggrmntvrsn_ast_fecini
	, staggrmntvrsn.ast_fecfin				as staggrmntvrsn_ast_fecfin
	, staggrmntvrsn.status				as staggrmntvrsn_status
from acsele_alfa_bi.em_gn_5 ctx
inner join acsele_alfa.insuranceobjectdco nsrncbjctdco
on ctx.nsrncbjctdco_min_objid = nsrncbjctdco.agregatedobjectid
and ctx.rskntdco_agregatedobjectid = nsrncbjctdco.agregatedparentid
and ctx.plcdco_operationpk = nsrncbjctdco.operationpk
and nsrncbjctdco.status = 1
inner join acsele_alfa.stag_agreementversion staggrmntvrsn
on ctx.stpsgrmntsttcdt_ast_id = staggrmntvrsn.ast_id
and staggrmntvrsn.ast_fecini <= ctx.rskntdco_initialdate
and staggrmntvrsn.ast_fecfin >= ctx.rskntdco_initialdate
;
commit;

create index acsele_alfa_bi.em_gn_6_idx_1
on acsele_alfa_bi.em_gn_6 (plcdco_dcoid);

create index acsele_alfa_bi.em_gn_6_idx_2
on acsele_alfa_bi.em_gn_6 (plcdco_agregatedobjectid);

create index acsele_alfa_bi.em_gn_6_idx_3
on acsele_alfa_bi.em_gn_6 (prplc_pk);

create index acsele_alfa_bi.em_gn_6_idx_4
on acsele_alfa_bi.em_gn_6 (vntdco_iddcoevent);

create index acsele_alfa_bi.em_gn_6_idx_5
on acsele_alfa_bi.em_gn_6 (staggrmntvrsn_dcoid);

create index acsele_alfa_bi.em_gn_6_idx_6
on acsele_alfa_bi.em_gn_6 (rskntdco_dcoid);

create index acsele_alfa_bi.em_gn_6_idx_7
on acsele_alfa_bi.em_gn_6 (nsrncbjctdco_dcoid);

create index acsele_alfa_bi.em_gn_6_idx_8
on acsele_alfa_bi.em_gn_6 (grgtdplc_agregatedpolicyid);

create index acsele_alfa_bi.em_gn_6_idx_9
on acsele_alfa_bi.em_gn_6 (plcdco_operationpk);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_gn_6
;


/* ============================================================================================ */
/* GENERALES · AUX POL - Universo de polizas (pivote de participantes de poliza)                */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_gn_pol;
commit;
*/

create table acsele_alfa_bi.em_gn_pol
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */ distinct
	  ctx.grgtdplc_agregatedpolicyid
from acsele_alfa_bi.em_gn_6 ctx
;
commit;

create index acsele_alfa_bi.em_gn_pol_idx_1
on acsele_alfa_bi.em_gn_pol (grgtdplc_agregatedpolicyid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_gn_pol
;


/* ============================================================================================ */
/* GENERALES · AUX OPR - Universo de operaciones (pivote de movimientos, asegurado y funciones) */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_gn_opr;
commit;
*/

create table acsele_alfa_bi.em_gn_opr
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */ distinct
	  ctx.plcdco_operationpk
	, ctx.plcdco_agregatedobjectid
	, ctx.prprdct_static
from acsele_alfa_bi.em_gn_6 ctx
;
commit;

create index acsele_alfa_bi.em_gn_opr_idx_1
on acsele_alfa_bi.em_gn_opr (plcdco_operationpk, plcdco_agregatedobjectid);

create index acsele_alfa_bi.em_gn_opr_idx_2
on acsele_alfa_bi.em_gn_opr (prprdct_static);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_gn_opr
;



/* ============================================================================================ */
/* ##############################  RAMA SATELITE  ##############################                */
/* ============================================================================================ */

/* ============================================================================================ */
/* SATÉLITE · PASO 4 - DEPENDIENTES DE agregatedpolicy: product (filtro de productos),          */
/*    preproduct,                                                                               */
/*                                                                                              */
/* Lee del tronco común em_3 y aplica el filtro exacto de productos de Satélite.                */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_st_4;
commit;
*/

create table acsele_alfa_bi.em_st_4
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  ctx.ctx_id
	, ctx.ctx_time_stamp
	, ctx.ctx_status
	, ctx.plcdco_operationpk
	, ctx.plcdco_dcoid
	, ctx.plcdco_agregatedobjectid
	, ctx.plcdco_iddcoevent
	, ctx.plcdco_initialdate
	, ctx.plcdco_finishdate
	, ctx.plcdco_event
	, ctx.grgtdplc_agregatedpolicyid
	, ctx.grgtdplc_productid
	, ctx.prplc_pk
	, ctx.prplc_oficinavalue
	, ctx.prplc_oficinainput
	, ctx.prplc_tipopolizainput
	, ctx.prplc_frecuenciapagoinput
	, ctx.prplc_numeropolizainput
	, ctx.prplc_canalventainput
	, ctx.prplc_monedainput
	, ctx.vntdco_eventtypeid
	, ctx.vntdco_iddcoevent
	, ctx.rskntdco_operationpk
	, ctx.rskntdco_dcoid
	, ctx.rskntdco_agregatedobjectid
	, ctx.rskntdco_initialdate
	, ctx.rskntdco_finishdate
	, prdct.description				as prdct_description
	, prprdct.static				as prprdct_static
	, prprdct.companiavalue				as prprdct_companiavalue
	, prprdct.companiainput				as prprdct_companiainput
	, prprdct.codigoproductoinput				as prprdct_codigoproductoinput
	, prprdct.ramovalue				as prprdct_ramovalue
	, prprdct.ramoinput				as prprdct_ramoinput
	, stpsrltngrmntplc.rsa_ast_id				as stpsrltngrmntplc_rsa_ast_id
from acsele_alfa_bi.em_3 ctx
inner join acsele_alfa.product prdct
on ctx.grgtdplc_productid = prdct.productid
inner join acsele_alfa.preproduct prprdct
on ctx.grgtdplc_productid = prprdct.static
inner join acsele_alfa_bi.cfg_pcr_producto cfgp
on cfgp.descripcion_producto = prdct.description
and cfgp.companiavalue = prprdct.companiavalue
and cfgp.estandar = 'SATELITE'
and cfgp.activo = 'S'
inner join acsele_alfa.stps_relationagreementpolicy stpsrltngrmntplc
on ctx.grgtdplc_agregatedpolicyid = stpsrltngrmntplc.rsa_policyid
;
commit;

create index acsele_alfa_bi.em_st_4_idx_1
on acsele_alfa_bi.em_st_4 (stpsrltngrmntplc_rsa_ast_id);

create index acsele_alfa_bi.em_st_4_idx_2
on acsele_alfa_bi.em_st_4 (rskntdco_agregatedobjectid, plcdco_operationpk);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_st_4
;


/* ============================================================================================ */
/* SATÉLITE · PASO 5 - DEPENDIENTES DEL PASO 4: stps_agreementstaticdat, insuranceobjectdco ->  */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_st_5;
commit;
*/

create table acsele_alfa_bi.em_st_5
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  ctx.ctx_id
	, ctx.ctx_time_stamp
	, ctx.ctx_status
	, ctx.plcdco_operationpk
	, ctx.plcdco_dcoid
	, ctx.plcdco_agregatedobjectid
	, ctx.plcdco_iddcoevent
	, ctx.plcdco_initialdate
	, ctx.plcdco_finishdate
	, ctx.plcdco_event
	, ctx.grgtdplc_agregatedpolicyid
	, ctx.grgtdplc_productid
	, ctx.prplc_pk
	, ctx.prplc_oficinavalue
	, ctx.prplc_oficinainput
	, ctx.prplc_tipopolizainput
	, ctx.prplc_frecuenciapagoinput
	, ctx.prplc_numeropolizainput
	, ctx.prplc_canalventainput
	, ctx.prplc_monedainput
	, ctx.vntdco_eventtypeid
	, ctx.vntdco_iddcoevent
	, ctx.rskntdco_operationpk
	, ctx.rskntdco_dcoid
	, ctx.rskntdco_agregatedobjectid
	, ctx.rskntdco_initialdate
	, ctx.rskntdco_finishdate
	, ctx.prdct_description
	, ctx.prprdct_static
	, ctx.prprdct_companiavalue
	, ctx.prprdct_companiainput
	, ctx.prprdct_codigoproductoinput
	, ctx.prprdct_ramovalue
	, ctx.prprdct_ramoinput
	, ctx.stpsrltngrmntplc_rsa_ast_id
	, stpsgrmntsttcdt.ast_id				as stpsgrmntsttcdt_ast_id
	, stpsgrmntsttcdt.ast_numeroacu				as stpsgrmntsttcdt_ast_numeroacu
	, stpsgrmntsttcdt.ast_nombreacu				as stpsgrmntsttcdt_ast_nombreacu
	, min(nsrncbjctdco.agregatedobjectid)	as nsrncbjctdco_min_objid
from acsele_alfa_bi.em_st_4 ctx
inner join acsele_alfa.stps_agreementstaticdat stpsgrmntsttcdt
on ctx.stpsrltngrmntplc_rsa_ast_id = stpsgrmntsttcdt.ast_id
inner join acsele_alfa.insuranceobjectdco nsrncbjctdco
on ctx.rskntdco_agregatedobjectid = nsrncbjctdco.agregatedparentid
and ctx.plcdco_operationpk = nsrncbjctdco.operationpk
--and nsrncbjctdco.status = 1		-- <== descomentar si el minimo debe ser solo sobre activos
group by ctx.ctx_id
	, ctx.ctx_time_stamp
	, ctx.ctx_status
	, ctx.plcdco_operationpk
	, ctx.plcdco_dcoid
	, ctx.plcdco_agregatedobjectid
	, ctx.plcdco_iddcoevent
	, ctx.plcdco_initialdate
	, ctx.plcdco_finishdate
	, ctx.plcdco_event
	, ctx.grgtdplc_agregatedpolicyid
	, ctx.grgtdplc_productid
	, ctx.prplc_pk
	, ctx.prplc_oficinavalue
	, ctx.prplc_oficinainput
	, ctx.prplc_tipopolizainput
	, ctx.prplc_frecuenciapagoinput
	, ctx.prplc_numeropolizainput
	, ctx.prplc_canalventainput
	, ctx.prplc_monedainput
	, ctx.vntdco_eventtypeid
	, ctx.vntdco_iddcoevent
	, ctx.rskntdco_operationpk
	, ctx.rskntdco_dcoid
	, ctx.rskntdco_agregatedobjectid
	, ctx.rskntdco_initialdate
	, ctx.rskntdco_finishdate
	, ctx.prdct_description
	, ctx.prprdct_static
	, ctx.prprdct_companiavalue
	, ctx.prprdct_companiainput
	, ctx.prprdct_codigoproductoinput
	, ctx.prprdct_ramovalue
	, ctx.prprdct_ramoinput
	, ctx.stpsrltngrmntplc_rsa_ast_id
	, stpsgrmntsttcdt.ast_id
	, stpsgrmntsttcdt.ast_numeroacu
	, stpsgrmntsttcdt.ast_nombreacu
;
commit;

create index acsele_alfa_bi.em_st_5_idx_1
on acsele_alfa_bi.em_st_5 (nsrncbjctdco_min_objid, rskntdco_agregatedobjectid, plcdco_operationpk);

create index acsele_alfa_bi.em_st_5_idx_2
on acsele_alfa_bi.em_st_5 (stpsgrmntsttcdt_ast_id, rskntdco_initialdate);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_st_5
;


/* ============================================================================================ */
/* SATÉLITE · PASO 6 - DEPENDIENTES DEL PASO 5: insuranceobjectdco (registro del MIN, activo),  */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_st_6;
commit;
*/

create table acsele_alfa_bi.em_st_6
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  ctx.ctx_id
	, ctx.ctx_time_stamp
	, ctx.ctx_status
	, ctx.plcdco_operationpk
	, ctx.plcdco_dcoid
	, ctx.plcdco_agregatedobjectid
	, ctx.plcdco_iddcoevent
	, ctx.plcdco_initialdate
	, ctx.plcdco_finishdate
	, ctx.plcdco_event
	, ctx.grgtdplc_agregatedpolicyid
	, ctx.grgtdplc_productid
	, ctx.prplc_pk
	, ctx.prplc_oficinavalue
	, ctx.prplc_oficinainput
	, ctx.prplc_tipopolizainput
	, ctx.prplc_frecuenciapagoinput
	, ctx.prplc_numeropolizainput
	, ctx.prplc_canalventainput
	, ctx.prplc_monedainput
	, ctx.vntdco_eventtypeid
	, ctx.vntdco_iddcoevent
	, ctx.rskntdco_operationpk
	, ctx.rskntdco_dcoid
	, ctx.rskntdco_agregatedobjectid
	, ctx.rskntdco_initialdate
	, ctx.rskntdco_finishdate
	, ctx.prdct_description
	, ctx.prprdct_static
	, ctx.prprdct_companiavalue
	, ctx.prprdct_companiainput
	, ctx.prprdct_codigoproductoinput
	, ctx.prprdct_ramovalue
	, ctx.prprdct_ramoinput
	, ctx.stpsrltngrmntplc_rsa_ast_id
	, ctx.stpsgrmntsttcdt_ast_id
	, ctx.stpsgrmntsttcdt_ast_numeroacu
	, ctx.stpsgrmntsttcdt_ast_nombreacu
	, ctx.nsrncbjctdco_min_objid
	, nsrncbjctdco.dcoid				as nsrncbjctdco_dcoid
	, staggrmntvrsn.dcoid				as staggrmntvrsn_dcoid
	, staggrmntvrsn.ast_fecini				as staggrmntvrsn_ast_fecini
	, staggrmntvrsn.ast_fecfin				as staggrmntvrsn_ast_fecfin
	, staggrmntvrsn.status				as staggrmntvrsn_status
from acsele_alfa_bi.em_st_5 ctx
inner join acsele_alfa.insuranceobjectdco nsrncbjctdco
on ctx.nsrncbjctdco_min_objid = nsrncbjctdco.agregatedobjectid
and ctx.rskntdco_agregatedobjectid = nsrncbjctdco.agregatedparentid
and ctx.plcdco_operationpk = nsrncbjctdco.operationpk
and nsrncbjctdco.status = 1
inner join acsele_alfa.stag_agreementversion staggrmntvrsn
on ctx.stpsgrmntsttcdt_ast_id = staggrmntvrsn.ast_id
and staggrmntvrsn.ast_fecini <= ctx.rskntdco_initialdate
and staggrmntvrsn.ast_fecfin >= ctx.rskntdco_initialdate
;
commit;

create index acsele_alfa_bi.em_st_6_idx_1
on acsele_alfa_bi.em_st_6 (plcdco_dcoid);

create index acsele_alfa_bi.em_st_6_idx_2
on acsele_alfa_bi.em_st_6 (plcdco_agregatedobjectid);

create index acsele_alfa_bi.em_st_6_idx_3
on acsele_alfa_bi.em_st_6 (prplc_pk);

create index acsele_alfa_bi.em_st_6_idx_4
on acsele_alfa_bi.em_st_6 (vntdco_iddcoevent);

create index acsele_alfa_bi.em_st_6_idx_5
on acsele_alfa_bi.em_st_6 (staggrmntvrsn_dcoid);

create index acsele_alfa_bi.em_st_6_idx_6
on acsele_alfa_bi.em_st_6 (rskntdco_dcoid);

create index acsele_alfa_bi.em_st_6_idx_7
on acsele_alfa_bi.em_st_6 (nsrncbjctdco_dcoid);

create index acsele_alfa_bi.em_st_6_idx_8
on acsele_alfa_bi.em_st_6 (grgtdplc_agregatedpolicyid);

create index acsele_alfa_bi.em_st_6_idx_9
on acsele_alfa_bi.em_st_6 (plcdco_operationpk, plcdco_agregatedobjectid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_st_6
;


/* ============================================================================================ */
/* SATÉLITE · AUX POL - Universo de polizas (pivote de participantes y de estado final de la    */
/*    poliza)                                                                                   */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_st_pol;
commit;
*/

create table acsele_alfa_bi.em_st_pol
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */ distinct
	  ctx.grgtdplc_agregatedpolicyid
from acsele_alfa_bi.em_st_6 ctx
;
commit;

create index acsele_alfa_bi.em_st_pol_idx_1
on acsele_alfa_bi.em_st_pol (grgtdplc_agregatedpolicyid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_st_pol
;


/* ============================================================================================ */
/* SATÉLITE · AUX OPR - Universo de operaciones y poliza (pivote de movimientos, asegurado,     */
/*    coberturas,                                                                               */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_st_opr;
commit;
*/

create table acsele_alfa_bi.em_st_opr
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */ distinct
	  ctx.plcdco_operationpk
	, ctx.plcdco_agregatedobjectid
from acsele_alfa_bi.em_st_6 ctx
;
commit;

create index acsele_alfa_bi.em_st_opr_idx_1
on acsele_alfa_bi.em_st_opr (plcdco_operationpk, plcdco_agregatedobjectid);

create index acsele_alfa_bi.em_st_opr_idx_2
on acsele_alfa_bi.em_st_opr (plcdco_operationpk);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_st_opr
;



/* ============================================================================================ */
/* ##############################  TRONCO COMUN  ##############################                 */
/* ============================================================================================ */

/* ============================================================================================ */
/* COMUN AUX POL - Universo de pólizas de los tres negocios (union de em_vd_pol, em_gn_pol,     */
/*    em_st_pol)                                                                                */
/*                                                                                              */
/* UNION (con distinct) de los universos de cada negocio: no es subquery, es un compuesto de    */
/*    tres select.                                                                              */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_pol;
commit;
*/

create table acsele_alfa_bi.em_pol
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */ grgtdplc_agregatedpolicyid
from acsele_alfa_bi.em_vd_pol
union
select /*+ parallel(3) */ grgtdplc_agregatedpolicyid
from acsele_alfa_bi.em_gn_pol
union
select /*+ parallel(3) */ grgtdplc_agregatedpolicyid
from acsele_alfa_bi.em_st_pol
;
commit;

create index acsele_alfa_bi.em_pol_idx_1
on acsele_alfa_bi.em_pol (grgtdplc_agregatedpolicyid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_pol
;


/* ============================================================================================ */
/* COMUN AUX OPR - Universo de operaciones de los tres negocios (union de em_vd_opr,            */
/*    em_gn_opr, em_st_opr)                                                                     */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_opr;
commit;
*/

create table acsele_alfa_bi.em_opr
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */ plcdco_operationpk
from acsele_alfa_bi.em_vd_opr
union
select /*+ parallel(3) */ plcdco_operationpk
from acsele_alfa_bi.em_gn_opr
union
select /*+ parallel(3) */ plcdco_operationpk
from acsele_alfa_bi.em_st_opr
;
commit;

create index acsele_alfa_bi.em_opr_idx_1
on acsele_alfa_bi.em_opr (plcdco_operationpk);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_opr
;


/* ============================================================================================ */
/* COMUN PUB ACU - PUBLICACION PARA LAS MALLAS DEPENDIENTES (PCR v1.1): acuerdo y version del   */
/*    acuerdo de cada operacion de EMISIONES, tal como los eligio cada negocio (em_vd_6,        */
/*    em_gn_6, em_st_6). No interviene en EMISIONES; la leen Primas cedidas y Reaseguros.       */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_pub_acu;
commit;
*/

create table acsele_alfa_bi.em_pub_acu
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  vd.plcdco_operationpk
	, 'VIDA'								as origen
	, vd.stpsgrmntsttcdt_ast_id				as ast_id
	, vd.stpsgrmntsttcdt_ast_numeroacu		as ast_numeroacu
	, vd.staggrmntvrsn_dcoid				as agrv_dcoid
	, vd.staggrmntvrsn_ast_fecini			as agrv_fecini
	, vd.staggrmntvrsn_ast_fecfin			as agrv_fecfin
from acsele_alfa_bi.em_vd_6 vd
group by vd.plcdco_operationpk
	, vd.stpsgrmntsttcdt_ast_id
	, vd.stpsgrmntsttcdt_ast_numeroacu
	, vd.staggrmntvrsn_dcoid
	, vd.staggrmntvrsn_ast_fecini
	, vd.staggrmntvrsn_ast_fecfin
union all
select /*+ parallel(3) */
	  gn.plcdco_operationpk
	, 'GENERALES'
	, gn.stpsgrmntsttcdt_ast_id
	, gn.stpsgrmntsttcdt_ast_numeroacu
	, gn.staggrmntvrsn_dcoid
	, gn.staggrmntvrsn_ast_fecini
	, gn.staggrmntvrsn_ast_fecfin
from acsele_alfa_bi.em_gn_6 gn
group by gn.plcdco_operationpk
	, gn.stpsgrmntsttcdt_ast_id
	, gn.stpsgrmntsttcdt_ast_numeroacu
	, gn.staggrmntvrsn_dcoid
	, gn.staggrmntvrsn_ast_fecini
	, gn.staggrmntvrsn_ast_fecfin
union all
select /*+ parallel(3) */
	  st.plcdco_operationpk
	, 'SATELITE'
	, st.stpsgrmntsttcdt_ast_id
	, st.stpsgrmntsttcdt_ast_numeroacu
	, st.staggrmntvrsn_dcoid
	, st.staggrmntvrsn_ast_fecini
	, st.staggrmntvrsn_ast_fecfin
from acsele_alfa_bi.em_st_6 st
group by st.plcdco_operationpk
	, st.stpsgrmntsttcdt_ast_id
	, st.stpsgrmntsttcdt_ast_numeroacu
	, st.staggrmntvrsn_dcoid
	, st.staggrmntvrsn_ast_fecini
	, st.staggrmntvrsn_ast_fecfin
;
commit;

create index acsele_alfa_bi.em_pub_acu_idx_1
on acsele_alfa_bi.em_pub_acu (plcdco_operationpk);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_pub_acu
;


/* ============================================================================================ */
/* COMUN AUX PPB - Participantes de póliza BASE de los tres negocios: todos los roles, con el   */
/*    upper del rol (sin reglas de negocio)                                                     */
/*                                                                                              */
/* Mismos cruces de los tres armados (policyparticipation, stts_role, policyparticipationdco,   */
/*    prethirdparty). Cada negocio filtra sus roles en su propio ppt.                           */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_ppb;
commit;
*/

create table acsele_alfa_bi.em_ppb
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  pol.grgtdplc_agregatedpolicyid
	, sttsrl.rol_id				as sttsrl_rol_id
	, upper(sttsrl.rol_description)				as sttsrl_rol_description_u
	, stpplcprtcptndco.status				as stpplcprtcptndco_status
	, prthrdprty.static				as prthrdprty_static
	, prthrdprty.documentoidentidadinput				as prthrdprty_documentoidentidadinput
	, prthrdprty.nombrepredeterminadoinput				as prthrdprty_nombrepredeterminadoinput
	, sttsrl.rol_description				as sttsrl_rol_description
from acsele_alfa_bi.em_pol pol
inner join acsele_alfa.stpo_policyparticipation stpplcprtcptn
on pol.grgtdplc_agregatedpolicyid = stpplcprtcptn.agregatedparentid
inner join acsele_alfa.stts_role sttsrl
on stpplcprtcptn.rol_id = sttsrl.rol_id
inner join acsele_alfa.stpo_policyparticipationdco stpplcprtcptndco
on stpplcprtcptn.agregatedobjectid = stpplcprtcptndco.agregatedobjectid
inner join acsele_alfa.prethirdparty prthrdprty
on stpplcprtcptn.thirdpartyid = prthrdprty.static
;
commit;

create index acsele_alfa_bi.em_ppb_idx_1
on acsele_alfa_bi.em_ppb (grgtdplc_agregatedpolicyid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_ppb
;


/* ============================================================================================ */
/* COMUN AUX TPB - Terceros de póliza por operación BASE (antes                                 */
/*    concat_third_name_level_policy), Generales y Satélite                                     */
/*                                                                                              */
/* Cada negocio filtra sus roles en su propio tpf.                                              */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_tpb;
commit;
*/

create table acsele_alfa_bi.em_tpb
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  opr.plcdco_operationpk
	, stpplcprtcptn.rol_id				as stpplcprtcptn_rol_id
	, prthrdprty.documentoidentidadinput				as prthrdprty_documentoidentidadinput
	, prthrdprty.nombrepredeterminadoinput				as prthrdprty_nombrepredeterminadoinput
	, prthrdprty.telefonooficinacompletinput				as prthrdprty_telefonooficinacompletinput
	, prthrdprty.emailinput				as prthrdprty_emailinput
	, stpplcprtcptn.thirdpartyid				as stpplcprtcptn_thirdpartyid
	, stpplcprtcptndco.dcoid				as stpplcprtcptndco_dcoid
	, prthrdprty.pk				as prthrdprty_pk
	, prthrdprty.tipodocumentoinput				as prthrdprty_tipodocumentoinput
from acsele_alfa_bi.em_opr opr
inner join acsele_alfa.stpo_policyparticipationdco stpplcprtcptndco
on opr.plcdco_operationpk = stpplcprtcptndco.operationpk
inner join acsele_alfa.stpo_policyparticipation stpplcprtcptn
on stpplcprtcptndco.agregatedobjectid = stpplcprtcptn.agregatedobjectid
inner join acsele_alfa.prethirdparty prthrdprty
on stpplcprtcptn.thirdpartyid = prthrdprty.static
;
commit;

create index acsele_alfa_bi.em_tpb_idx_1
on acsele_alfa_bi.em_tpb (plcdco_operationpk);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_tpb
;


/* ============================================================================================ */
/* COMUN AUX OAB - Participantes del objeto asegurado BASE de los tres negocios: todos los      */
/*    roles (sin reglas de negocio)                                                             */
/*                                                                                              */
/* Mismos cruces de los tres armados. La exclusión del producto 54959 (Generales) y el          */
/*    teléfono por personanatural (Satélite) se aplican en la rama de cada uno.                 */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_oab;
commit;
*/

create table acsele_alfa_bi.em_oab
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  opr.plcdco_operationpk
	, sttsrl.rol_description				as sttsrl_rol_description
	, prthrdprty.tipodocumentoinput				as prthrdprty_tipodocumentoinput
	, prthrdprty.documentoidentidadinput				as prthrdprty_documentoidentidadinput
	, prthrdprty.nombrepredeterminadoinput				as prthrdprty_nombrepredeterminadoinput
	, prthrdprty.emailinput				as prthrdprty_emailinput
	, upper(sttsrl.rol_description)				as sttsrl_rol_description_u
	, prthrdprty.static				as prthrdprty_static
	, prthrdprty.telefonooficinacompletinput				as prthrdprty_telefonooficinacompletinput
from acsele_alfa_bi.em_opr opr
inner join acsele_alfa.stpo_insobjparticipationdco stpnsbjprtcptndco
on opr.plcdco_operationpk = stpnsbjprtcptndco.operationpk
and stpnsbjprtcptndco.status = 1
inner join acsele_alfa.stpo_insobjparticipation stpnsbjprtcptn
on stpnsbjprtcptndco.agregatedobjectid = stpnsbjprtcptn.agregatedobjectid
inner join acsele_alfa.stts_role sttsrl
on stpnsbjprtcptn.rol_id = sttsrl.rol_id
inner join acsele_alfa.agregatedinsuranceobject grgtdnsrncbjct
on stpnsbjprtcptn.agregatedparentid = grgtdnsrncbjct.agregatedinsuranceobjectid
inner join acsele_alfa.prethirdparty prthrdprty
on stpnsbjprtcptn.thirdpartyid = prthrdprty.static
;
commit;

create index acsele_alfa_bi.em_oab_idx_1
on acsele_alfa_bi.em_oab (plcdco_operationpk);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_oab
;


/* ============================================================================================ */
/* COMUN PASO 8 - Movimientos BASE de los tres negocios: thirdpartymovementpolicy por           */
/*    operationpk, con upper de concept y descproperty                                          */
/*                                                                                              */
/* Una sola lectura de la tabla más grande para los tres negocios. Sin filtros: Vida aplica     */
/*    ammount <> 0 y su cruce por operationpk en su paso 9; Generales y Satélite cruzan por     */
/*    operationpk + policyid en el suyo.                                                        */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_tmb;
commit;
*/

create table acsele_alfa_bi.em_tmb
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  thrdprtmvmntplc.operationpk				as thrdprtmvmntplc_operationpk
	, thrdprtmvmntplc.policyid				as thrdprtmvmntplc_policyid
	, thrdprtmvmntplc.concept				as thrdprtmvmntplc_concept
	, upper(thrdprtmvmntplc.concept)				as thrdprtmvmntplc_concept_u
	, upper(thrdprtmvmntplc.descproperty)				as thrdprtmvmntplc_descproperty_u
	, thrdprtmvmntplc.ammount				as thrdprtmvmntplc_ammount
	, thrdprtmvmntplc.descproperty				as thrdprtmvmntplc_descproperty
from acsele_alfa_bi.em_opr opr
inner join acsele_alfa.thirdpartymovementpolicy thrdprtmvmntplc
on opr.plcdco_operationpk = thrdprtmvmntplc.operationpk
;
commit;

create index acsele_alfa_bi.em_tmb_idx_1
on acsele_alfa_bi.em_tmb (thrdprtmvmntplc_operationpk, thrdprtmvmntplc_policyid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_tmb
;



/* ============================================================================================ */
/* ##############################  RAMA VIDA  ##############################                    */
/* ============================================================================================ */

/* ============================================================================================ */
/* VIDA · AUX PPT - Filtro de roles: tomador, agente y pagador maestro                          */
/*                                                                                              */
/* Toma la base común em_ppb y la restringe al universo de pólizas de Vida antes de aplicar     */
/*    sus roles.                                                                                */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_vd_ppt;
commit;
*/

create table acsele_alfa_bi.em_vd_ppt
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  ppb.grgtdplc_agregatedpolicyid
	, ppb.sttsrl_rol_description_u
	, ppb.stpplcprtcptndco_status
	, ppb.prthrdprty_static
	, ppb.prthrdprty_documentoidentidadinput
	, ppb.prthrdprty_nombrepredeterminadoinput
from acsele_alfa_bi.em_ppb ppb
inner join acsele_alfa_bi.em_vd_pol pol
on ppb.grgtdplc_agregatedpolicyid = pol.grgtdplc_agregatedpolicyid
where (ppb.sttsrl_rol_description_u like '%TOMADOR%'
or ppb.sttsrl_rol_description_u like '%AGENTE%'
or ppb.sttsrl_rol_description_u like '%MAESTRO%')
;
commit;

create index acsele_alfa_bi.em_vd_ppt_idx_1
on acsele_alfa_bi.em_vd_ppt (grgtdplc_agregatedpolicyid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_vd_ppt
;


/* ============================================================================================ */
/* VIDA · AUX TOM - Tomador: un solo registro por poliza (MAX + group by, reemplaza ROWNUM <=   */
/*    1)                                                                                        */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_vd_tom;
commit;
*/

create table acsele_alfa_bi.em_vd_tom
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  ppt.grgtdplc_agregatedpolicyid
	, max(ppt.prthrdprty_documentoidentidadinput)		as tom_documentoidentidad
	, max(ppt.prthrdprty_nombrepredeterminadoinput)	as tom_nombre
from acsele_alfa_bi.em_vd_ppt ppt
where ppt.sttsrl_rol_description_u like '%TOMADOR%'
group by ppt.grgtdplc_agregatedpolicyid
;
commit;

create index acsele_alfa_bi.em_vd_tom_idx_1
on acsele_alfa_bi.em_vd_tom (grgtdplc_agregatedpolicyid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_vd_tom
;


/* ============================================================================================ */
/* VIDA · AUX AGE - Agente: un solo registro por poliza (MAX + group by, reemplaza ROWNUM <= 1) */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_vd_age;
commit;
*/

create table acsele_alfa_bi.em_vd_age
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  ppt.grgtdplc_agregatedpolicyid
	, max(ppt.prthrdprty_nombrepredeterminadoinput)	as age_nombre
from acsele_alfa_bi.em_vd_ppt ppt
where ppt.sttsrl_rol_description_u like '%AGENTE%'
group by ppt.grgtdplc_agregatedpolicyid
;
commit;

create index acsele_alfa_bi.em_vd_age_idx_1
on acsele_alfa_bi.em_vd_age (grgtdplc_agregatedpolicyid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_vd_age
;


/* ============================================================================================ */
/* VIDA · AUX PGM - Pagador maestro (participacion activa, status = 1)                          */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_vd_pgm;
commit;
*/

create table acsele_alfa_bi.em_vd_pgm
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  ppt.grgtdplc_agregatedpolicyid
	, max(ppt.prthrdprty_documentoidentidadinput || '-' || ppt.prthrdprty_nombrepredeterminadoinput)	as pgm_informacion
from acsele_alfa_bi.em_vd_ppt ppt
where ppt.sttsrl_rol_description_u like '%MAESTRO%'
and ppt.stpplcprtcptndco_status = 1
group by ppt.grgtdplc_agregatedpolicyid
;
commit;

create index acsele_alfa_bi.em_vd_pgm_idx_1
on acsele_alfa_bi.em_vd_pgm (grgtdplc_agregatedpolicyid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_vd_pgm
;


/* ============================================================================================ */
/* VIDA · AUX AOF - Filtro de rol asegurado                                                     */
/*                                                                                              */
/* Toma la base común em_oab y la restringe a las operaciones de Vida.                          */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_vd_aof;
commit;
*/

create table acsele_alfa_bi.em_vd_aof
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  aob.plcdco_operationpk
	, aob.prthrdprty_nombrepredeterminadoinput
	, aob.prthrdprty_documentoidentidadinput
	, aob.prthrdprty_tipodocumentoinput
from acsele_alfa_bi.em_oab aob
inner join acsele_alfa_bi.em_vd_opr opr
on aob.plcdco_operationpk = opr.plcdco_operationpk
where aob.sttsrl_rol_description like '%Asegurado%'
--where aob.sttsrl_rol_description_u like '%ASEGURADO%'
;
commit;

create index acsele_alfa_bi.em_vd_aof_idx_1
on acsele_alfa_bi.em_vd_aof (plcdco_operationpk);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_vd_aof
;


/* ============================================================================================ */
/* VIDA · AUX ASG - Asegurado por operacion (PNOMASEG, PIDENTASEG, PTIPIDENTASEG): MAX + group  */
/*    by                                                                                        */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_vd_asg;
commit;
*/

create table acsele_alfa_bi.em_vd_asg
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  aof.plcdco_operationpk
	, max(aof.prthrdprty_nombrepredeterminadoinput)	as asg_nombre
	, max(aof.prthrdprty_documentoidentidadinput)	as asg_documentoidentidad
	, max(aof.prthrdprty_tipodocumentoinput)		as asg_tipodocumento		---nb sol de operaciones 25 mayo 2024 jerg
from acsele_alfa_bi.em_vd_aof aof
group by aof.plcdco_operationpk
;
commit;

create index acsele_alfa_bi.em_vd_asg_idx_1
on acsele_alfa_bi.em_vd_asg (plcdco_operationpk);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_vd_asg
;


/* ============================================================================================ */
/* VIDA · PASO 7 - LEFT JOINS (al final): preagreement, agregatedriskunit, eventtype y          */
/*    participantes                                                                             */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_vd_7;
commit;
*/

create table acsele_alfa_bi.em_vd_7
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  ctx.grgtdplc_productid					as idproducto
	, ctx.prprdct_companiavalue					as codigo_compania
	, ctx.prprdct_companiainput					as compania
	, ctx.prprdct_static						as codigo_producto
	, ctx.prprdct_descripcionproductoinput		as producto
	, ctx.prprdct_ramovalue						as codigo_ramo
	, ctx.prprdct_ramoinput						as ramo
	, ctx.prplc_oficinavalue					as codigo_sucursal
	, ctx.prplc_oficinainput					as sucursal
	, tom.tom_documentoidentidad				as identificacion_tomador
	, tom.tom_nombre							as tomador
	, pgm.pgm_informacion						as pagador_maestro
	, ctx.stpsrltngrmntplc_rsa_ast_id			as idacuerdo
	, ctx.stpsgrmntsttcdt_ast_numeroacu			as acuerdo
	, ctx.stpsgrmntsttcdt_ast_id				as st_ast_id
	, ctx.staggrmntvrsn_ast_fecini				as fechaini_acuerdo
	, ctx.staggrmntvrsn_ast_fecfin				as fechafin_acuerdo
	, ctx.prdct_description						as producto_del_acuerdo
	, ctx.prplc_tipopolizainput					as tipo_poliza
	, ctx.prplc_numeropolizainput				as nro_poliza
	, ctx.ctx_time_stamp						as fecha_emision
	, (extract(year from ctx.ctx_time_stamp) || lpad(extract(month from ctx.ctx_time_stamp), 2, 0))	as periodo_operacion
	, ctx.plcdco_initialdate					as fecha_incial_pol
	, ctx.plcdco_finishdate						as fecha_final_pol
	, prgrmnt.tipoproduccioninput				as tipo_produccion
	, prgrmnt.unidadnegocioinput				as unidad_negocio
	, prgrmnt.unidadnegociovalue				as codigo_unidad_negocio
	, ctx.ctx_status							as estado_poliza
	, ctx.ctx_id								as nro_operacion
	, ctx.staggrmntvrsn_description				as linea_credito
	, prgrmnt.lineacreditoinput					as linea_credito_nego
	, nvl(vnttp.description, ctx.plcdco_event)	as descripcion_evento
	, ctx.prplc_monedainput						as moneda_acuerdo
	, grgtdrsknt.agregatedriskunitid			as unidad_riesgo
	, ctx.rvddrs_numerocreditoinput				as numero_credito
	, ctx.rvddrs_fechadesembolsoinput			as fecha_desembolso		---- nuevo gustavo 4dic23
	, ctx.rskntdco_initialdate					as fecha_inicial_ur
	, ctx.rskntdco_finishdate					as fecha_final_ur
	, age.age_nombre							as agente
	, asg.asg_nombre							as pnomaseg
	, asg.asg_documentoidentidad				as pidentaseg
	, asg.asg_tipodocumento						as ptipidentaseg
	, ctx.prplc_frecuenciapagoinput				as frecuencia_pago
	, ctx.stpsrltngrmntplc_rsa_policyid			as rsa_policyid
	, ctx.rskntdco_operationpk					as ri_operationpk
	, ctx.staggrmntvrsn_dcoid					as agrv_dcoid
	, prgrmnt.pk								as liunc_pk
	, ctx.grgtdplc_agregatedpolicyid			as agregatedpolicyid
	, ctx.oavddrs_edadvalue						as edad_aseguado				--inclusion nueva sfc jerg 11abril23
	, ctx.oavddrs_fechanacimientoaseginput		as fecha_nacer_asegurado		--inclusion nueva sfc jerg 11abril23
	, ctx.oavddrs_direccionaseguradoinput		as direccion_aseguado			--inclusion nueva sfc jerg 11abril23
	, ctx.oavddrs_departamentoinput				as desc_departamento			--inclusion nueva sfc jerg 11abril23
	, ctx.oavddrs_departamentovalue				as cod_departamento				--inclusion nueva sfc jerg 11abril23
	, ctx.oavddrs_ciudadinput					as desc_ciudad					--inclusion nueva sfc jerg 11abril23
	, ctx.oavddrs_ciudadvalue					as cod_ciudad					--inclusion nueva sfc jerg 11abril23
	, ctx.oavddrs_factorvidavalue				as factor_vida					--inclusion nueva sfc jerg 11abril23
	, (substr(ctx.oavddrs_departamentovalue, 1, length(ctx.oavddrs_departamentovalue) - 2) || lpad(replace(ctx.oavddrs_ciudadvalue, '.0', ''), 3, '0'))	as codigo_divipola_municipio
from acsele_alfa_bi.em_vd_6 ctx
left join acsele_alfa.preagreement prgrmnt
on ctx.staggrmntvrsn_dcoid = prgrmnt.pk
left join acsele_alfa.agregatedriskunit grgtdrsknt
on ctx.rskntdco_operationpk = grgtdrsknt.operationpk
left join acsele_alfa.eventtype vnttp
on ctx.vntdco_eventtypeid = vnttp.eventtypeid
left join acsele_alfa_bi.em_vd_tom tom
on ctx.grgtdplc_agregatedpolicyid = tom.grgtdplc_agregatedpolicyid
left join acsele_alfa_bi.em_vd_age age
on ctx.grgtdplc_agregatedpolicyid = age.grgtdplc_agregatedpolicyid
left join acsele_alfa_bi.em_vd_pgm pgm
on ctx.grgtdplc_agregatedpolicyid = pgm.grgtdplc_agregatedpolicyid
left join acsele_alfa_bi.em_vd_asg asg
on ctx.plcdco_operationpk = asg.plcdco_operationpk
;
commit;

create index acsele_alfa_bi.em_vd_7_idx_1
on acsele_alfa_bi.em_vd_7 (nro_operacion);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_vd_7
;


/* ============================================================================================ */
/* VIDA · PASO 9 - FILTRO DE TAGS: solo los conceptos que usa el consolidado                    */
/*                                                                                              */
/* Toma la base común de movimientos em_tmb y la restringe a las operaciones de Vida con el     */
/*    mismo cruce de su armado (operationpk; ammount <> 0); luego aplica sus conceptos.         */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_vd_9;
commit;
*/

create table acsele_alfa_bi.em_vd_9
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  tm.thrdprtmvmntplc_operationpk
	, tm.thrdprtmvmntplc_concept
	, tm.thrdprtmvmntplc_concept_u
	, tm.thrdprtmvmntplc_ammount
from acsele_alfa_bi.em_tmb tm
inner join acsele_alfa_bi.em_vd_opr opr
on tm.thrdprtmvmntplc_operationpk = opr.plcdco_operationpk
where tm.thrdprtmvmntplc_ammount <> 0		-- filtro de Vida (estaba en su paso 8)
and tm.thrdprtmvmntplc_concept_u in ('PRIMAEMITIDA','PRIMADEVUELTA','PRIMAANU','PRIMAREHAB','PRIMAREHABIND','PRIMAPERIODOPROVIS','PRIMAANUIND','PRIMAEMITIDAIND',
									'PRIMADEVUELTAIND','PRIMAPERIODOPROVISIND','PRIMAEMITIDADEV','COSTORECAUDACIONDEV','COSTORECAUDACIONDEVIND','COSTORECAUDOANU','COSTORECAUDOANUIND',
									'COSTORECAUDACION','COSTORECAUDACIONIND','COSTORECAUDOPROVIS','COSTORECAUDOPROVISIND','COSTORECAUDOREHAB','COSTORECAUDOREHABIND',
									'IVACOSTORECAUDACIONDEV','IVACOSTORECAUDACIONDEVIND','IVACOSTORECAUDOANU','IVACOSTORECAUDOANUIND','IVACOSTORECAUDACION',
									'IVACOSTORECAUDACIONIND','IVACOSTORECAUDOPROVIS','IVACOSTORECAUDOPROVISIND','IVACOSTORECAUDOREHAB','IVACOSTORECAUDOREHABIND','IVACOSTORECAUDO',
									'COMISIONVIDADEV','COMISIONVIDAINDDEV','COMISIONVIDA','COMISIONVIDAIND','SUMAASEGPERIODO',
									'PRIMA','PRIMADEV','PRIMAEMITIDAPAGVIDAAP','PRIMAEMITIDAVIDAAP','PRIMAEMITIDAPAGDEVVIDAAP','PRIMAEMITIDAANUVIDAAP','COMISIONAGENTE','COMISIONAGENTEDEV',
									'COSTORECAUDACIONDEVVIDAAP','COSTORECAUDACIONANUVIDAAP','COSTORECAUDACIONPAGVIDAAP','COSTORECAUDACIONREHABVIDAAP','COSTORECAUDACIONVIDAAP',
									'IVACOSTORECAUDACIONDEVVIDAAP','IVACOSTORECAUDACIONANUVIDAAP','IVACOSTORECAUDACIONVIDAAP','IVACOSTORECAUDACIONPAGVIDAAP','IVACOSTORECAUDACIONREHABVIDAAP',
									'COSTORECAUDOINCANU','COSTORECAUDOINCENDIODEV','COSTORECAUDOINCPROVIS','COSTORECAUDOINCREHAB','COSTORECAUDOINCENDIO',
									'COSTORECAUDOTERREMOTODEV','COSTORECAUDOTERRANU','COSTORECAUDOTERREMOTO','COSTORECAUDOTERRREHAB','COSTORECAUDOTERRPROVIS')
;
commit;

create index acsele_alfa_bi.em_vd_9_idx_1
on acsele_alfa_bi.em_vd_9 (thrdprtmvmntplc_operationpk);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_vd_9
;



/* ============================================================================================ */
/* ##############################  RAMA GENERALES  ##############################               */
/* ============================================================================================ */

/* ============================================================================================ */
/* GENERALES · AUX PPT - Filtro de roles: tomador (rol_id = 25935) y agente                     */
/*                                                                                              */
/* Toma la base común em_ppb y la restringe al universo de pólizas de Generales antes de        */
/*    aplicar sus roles.                                                                        */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_gn_ppt;
commit;
*/

create table acsele_alfa_bi.em_gn_ppt
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  ppb.grgtdplc_agregatedpolicyid
	, ppb.sttsrl_rol_id
	, ppb.sttsrl_rol_description_u
	, ppb.prthrdprty_static
	, ppb.prthrdprty_nombrepredeterminadoinput
from acsele_alfa_bi.em_ppb ppb
inner join acsele_alfa_bi.em_gn_pol pol
on ppb.grgtdplc_agregatedpolicyid = pol.grgtdplc_agregatedpolicyid
where (ppb.sttsrl_rol_id = 25935
or ppb.sttsrl_rol_description_u like '%AGENTE%')
;
commit;

create index acsele_alfa_bi.em_gn_ppt_idx_1
on acsele_alfa_bi.em_gn_ppt (grgtdplc_agregatedpolicyid);

create index acsele_alfa_bi.em_gn_ppt_idx_2
on acsele_alfa_bi.em_gn_ppt (prthrdprty_static);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_gn_ppt
;


/* ============================================================================================ */
/* GENERALES · AUX TDB - Direccion del tomador BASE: tercero -> libreta de direcciones (por     */
/*    defecto) ->                                                                               */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_gn_tdb;
commit;
*/

create table acsele_alfa_bi.em_gn_tdb
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  ppt.grgtdplc_agregatedpolicyid
	, drccn.departamentoinput				as drccn_departamentoinput
	, drccn.ciudadinput						as drccn_ciudadinput
	, drccn.direccioninput					as drccn_direccioninput
from acsele_alfa_bi.em_gn_ppt ppt
inner join acsele_alfa.stte_thirdparty sttthrdprty
on ppt.prthrdprty_static = sttthrdprty.tpt_id
inner join acsele_alfa.stte_thirdpartyaddressbook sttthrdprtddrssbk
on sttthrdprty.tpt_id = sttthrdprtddrssbk.tpt_id
and sttthrdprtddrssbk.isdefaultaddress = 1
inner join acsele_alfa.direccion drccn
on sttthrdprtddrssbk.iddco = drccn.pk
where ppt.sttsrl_rol_id = 25935
;
commit;

create index acsele_alfa_bi.em_gn_tdb_idx_1
on acsele_alfa_bi.em_gn_tdb (grgtdplc_agregatedpolicyid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_gn_tdb
;


/* ============================================================================================ */
/* GENERALES · AUX TDR - Direccion del tomador por poliza: MAX() + group by (reemplaza ROWNUM   */
/*    <= 1)                                                                                     */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_gn_tdr;
commit;
*/

create table acsele_alfa_bi.em_gn_tdr
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  tdb.grgtdplc_agregatedpolicyid
	, max(tdb.drccn_departamentoinput)		as tom_departamento
	, max(tdb.drccn_ciudadinput)			as tom_ciudad
	, max(tdb.drccn_direccioninput)			as tom_direccion
from acsele_alfa_bi.em_gn_tdb tdb
group by tdb.grgtdplc_agregatedpolicyid
;
commit;

create index acsele_alfa_bi.em_gn_tdr_idx_1
on acsele_alfa_bi.em_gn_tdr (grgtdplc_agregatedpolicyid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_gn_tdr
;


/* ============================================================================================ */
/* GENERALES · AUX AGE - Agente por poliza: MAX() + group by (reemplaza DISTINCT + ROWNUM <= 1) */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_gn_age;
commit;
*/

create table acsele_alfa_bi.em_gn_age
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  ppt.grgtdplc_agregatedpolicyid
	, max(ppt.prthrdprty_nombrepredeterminadoinput)	as age_nombre
from acsele_alfa_bi.em_gn_ppt ppt
where ppt.sttsrl_rol_description_u like '%AGENTE%'
group by ppt.grgtdplc_agregatedpolicyid
;
commit;

create index acsele_alfa_bi.em_gn_age_idx_1
on acsele_alfa_bi.em_gn_age (grgtdplc_agregatedpolicyid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_gn_age
;


/* ============================================================================================ */
/* GENERALES · AUX RUB - Participantes de unidad de riesgo BASE (solo producto 54959 Mi Hogar   */
/*    Seguro):                                                                                  */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_gn_rub;
commit;
*/

create table acsele_alfa_bi.em_gn_rub
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  opr.plcdco_operationpk
	, sttsrl.rol_description				as sttsrl_rol_description
	, prthrdprty.tipodocumentoinput			as prthrdprty_tipodocumentoinput
	, prthrdprty.documentoidentidadinput	as prthrdprty_documentoidentidadinput
	, prthrdprty.nombrepredeterminadoinput	as prthrdprty_nombrepredeterminadoinput
from acsele_alfa_bi.em_gn_opr opr
inner join acsele_alfa.stpo_ruparticipationdco stprprtcptndco
on opr.plcdco_operationpk = stprprtcptndco.operationpk
and stprprtcptndco.status = 1
inner join acsele_alfa.stpo_ruparticipation stprprtcptn
on stprprtcptndco.agregatedobjectid = stprprtcptn.agregatedobjectid
inner join acsele_alfa.stts_role sttsrl
on stprprtcptn.rol_id = sttsrl.rol_id
inner join acsele_alfa.agregatedriskunit grgtdrsknt
on stprprtcptn.agregatedparentid = grgtdrsknt.agregatedriskunitid
inner join acsele_alfa.prethirdparty prthrdprty
on stprprtcptn.thirdpartyid = prthrdprty.static
where opr.prprdct_static = 54959
;
commit;

create index acsele_alfa_bi.em_gn_rub_idx_1
on acsele_alfa_bi.em_gn_rub (sttsrl_rol_description);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_gn_rub
;


/* ============================================================================================ */
/* GENERALES · AUX RUF - Filtro de rol: rol_description = 'AseguradoGenerales'                  */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_gn_ruf;
commit;
*/

create table acsele_alfa_bi.em_gn_ruf
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  rub.plcdco_operationpk
	, rub.prthrdprty_tipodocumentoinput
	, rub.prthrdprty_documentoidentidadinput
	, rub.prthrdprty_nombrepredeterminadoinput
from acsele_alfa_bi.em_gn_rub rub
where rub.sttsrl_rol_description = 'AseguradoGenerales'
;
commit;

create index acsele_alfa_bi.em_gn_ruf_idx_1
on acsele_alfa_bi.em_gn_ruf (plcdco_operationpk);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_gn_ruf
;


/* ============================================================================================ */
/* GENERALES · AUX RUA - Asegurado por operacion (producto 54959): MAX() + group by (reemplaza  */
/*    ROWNUM <= 1)                                                                              */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_gn_rua;
commit;
*/

create table acsele_alfa_bi.em_gn_rua
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  ruf.plcdco_operationpk
	, max(ruf.prthrdprty_tipodocumentoinput)			as rua_tipodocumento
	, max(ruf.prthrdprty_documentoidentidadinput)		as rua_documentoidentidad
	, max(ruf.prthrdprty_nombrepredeterminadoinput)	as rua_nombre
from acsele_alfa_bi.em_gn_ruf ruf
group by ruf.plcdco_operationpk
;
commit;

create index acsele_alfa_bi.em_gn_rua_idx_1
on acsele_alfa_bi.em_gn_rua (plcdco_operationpk);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_gn_rua
;


/* ============================================================================================ */
/* GENERALES · AUX OAF - Filtro de rol: rol_description LIKE '%Asegurado%' (igual que la        */
/*    funcion)                                                                                  */
/*                                                                                              */
/* Toma la base común em_oab y la restringe a las operaciones de Generales (sin el producto     */
/*    54959, igual que su armado).                                                              */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_gn_oaf;
commit;
*/

create table acsele_alfa_bi.em_gn_oaf
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  oab.plcdco_operationpk
	, oab.prthrdprty_tipodocumentoinput
	, oab.prthrdprty_documentoidentidadinput
	, oab.prthrdprty_nombrepredeterminadoinput
from acsele_alfa_bi.em_oab oab
inner join acsele_alfa_bi.em_gn_opr opr
on oab.plcdco_operationpk = opr.plcdco_operationpk
where oab.sttsrl_rol_description like '%Asegurado%'
and opr.prprdct_static <> 54959
;
commit;

create index acsele_alfa_bi.em_gn_oaf_idx_1
on acsele_alfa_bi.em_gn_oaf (plcdco_operationpk);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_gn_oaf
;


/* ============================================================================================ */
/* GENERALES · AUX OAA - Asegurado por operacion (productos distintos de 54959): MAX() + group  */
/*    by                                                                                        */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_gn_oaa;
commit;
*/

create table acsele_alfa_bi.em_gn_oaa
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  oaf.plcdco_operationpk
	, max(oaf.prthrdprty_tipodocumentoinput)			as oaa_tipodocumento
	, max(oaf.prthrdprty_documentoidentidadinput)		as oaa_documentoidentidad
	, max(oaf.prthrdprty_nombrepredeterminadoinput)	as oaa_nombre
from acsele_alfa_bi.em_gn_oaf oaf
group by oaf.plcdco_operationpk
;
commit;

create index acsele_alfa_bi.em_gn_oaa_idx_1
on acsele_alfa_bi.em_gn_oaa (plcdco_operationpk);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_gn_oaa
;


/* ============================================================================================ */
/* GENERALES · AUX TPF - Filtro de roles: 25935 tomador, 33935 pagador maestro, 39937 pagador   */
/*                                                                                              */
/* Toma la base común em_tpb y la restringe a las operaciones de Generales.                     */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_gn_tpf;
commit;
*/

create table acsele_alfa_bi.em_gn_tpf
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  tpb.plcdco_operationpk
	, tpb.stpplcprtcptn_rol_id
	, tpb.prthrdprty_documentoidentidadinput
	, tpb.prthrdprty_nombrepredeterminadoinput
	, tpb.prthrdprty_telefonooficinacompletinput
	, tpb.prthrdprty_emailinput
from acsele_alfa_bi.em_tpb tpb
inner join acsele_alfa_bi.em_gn_opr opr
on tpb.plcdco_operationpk = opr.plcdco_operationpk
where tpb.stpplcprtcptn_rol_id in (25935, 33935, 39937)
;
commit;

create index acsele_alfa_bi.em_gn_tpf_idx_1
on acsele_alfa_bi.em_gn_tpf (plcdco_operationpk);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_gn_tpf
;


/* ============================================================================================ */
/* GENERALES · AUX TPR - Terceros por operacion y rol: LISTAGG separado por coma + group by     */
/*    (igual que la                                                                             */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_gn_tpr;
commit;
*/

create table acsele_alfa_bi.em_gn_tpr
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  tpf.plcdco_operationpk
	, listagg(case when tpf.stpplcprtcptn_rol_id = 25935 then tpf.prthrdprty_documentoidentidadinput end, ',') within group (order by tpf.plcdco_operationpk)	as tpr_tom_documento
	, listagg(case when tpf.stpplcprtcptn_rol_id = 25935 then tpf.prthrdprty_nombrepredeterminadoinput end, ',') within group (order by tpf.plcdco_operationpk)	as tpr_tom_nombre
	, listagg(case when tpf.stpplcprtcptn_rol_id = 25935 then tpf.prthrdprty_telefonooficinacompletinput end, ',') within group (order by tpf.plcdco_operationpk)	as tpr_tom_telefono
	, listagg(case when tpf.stpplcprtcptn_rol_id = 25935 then tpf.prthrdprty_emailinput end, ',') within group (order by tpf.plcdco_operationpk)	as tpr_tom_email
	, listagg(case when tpf.stpplcprtcptn_rol_id = 33935 then tpf.prthrdprty_documentoidentidadinput end, ',') within group (order by tpf.plcdco_operationpk)	as tpr_pgm_documento
	, listagg(case when tpf.stpplcprtcptn_rol_id = 33935 then tpf.prthrdprty_nombrepredeterminadoinput end, ',') within group (order by tpf.plcdco_operationpk)	as tpr_pgm_nombre
	, listagg(case when tpf.stpplcprtcptn_rol_id = 39937 then tpf.prthrdprty_documentoidentidadinput end, ',') within group (order by tpf.plcdco_operationpk)	as tpr_pag_documento
	, listagg(case when tpf.stpplcprtcptn_rol_id = 39937 then tpf.prthrdprty_nombrepredeterminadoinput end, ',') within group (order by tpf.plcdco_operationpk)	as tpr_pag_nombre
from acsele_alfa_bi.em_gn_tpf tpf
group by tpf.plcdco_operationpk
;
commit;

create index acsele_alfa_bi.em_gn_tpr_idx_1
on acsele_alfa_bi.em_gn_tpr (plcdco_operationpk);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_gn_tpr
;


/* ============================================================================================ */
/* GENERALES · PASO 7 - LEFT JOINS (al final): POs, URs, OAs, evento, acuerdo y participantes   */
/*    -> detalle                                                                                */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_gn_7;
commit;
*/

create table acsele_alfa_bi.em_gn_7
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  ctx.grgtdplc_agregatedpolicyid		as idpoliza
	, ctx.grgtdplc_productid		as idproducto
	, replace(ctx.prprdct_companiavalue, '.0', '')		as codigo_compania
	, ctx.prprdct_companiainput		as compania
	, ctx.prprdct_codigoproductoinput		as codigo_producto
	, ctx.prdct_description		as producto
	, case when ctx.grgtdplc_productid not in (55059) then ctx.prprdct_ramovalue else pprtccnfnncr.ramobancasegurosvalue end		as codigo_ramo
	, case when ctx.grgtdplc_productid not in (55059) then ctx.prprdct_ramoinput else pprtccnfnncr.ramobancasegurosinput end		as ramo
	, replace(ctx.prplc_oficinavalue, '.0', '')		as codigo_sucursal
	, ctx.prplc_oficinainput		as sucursal
	, tpr.tpr_tom_documento		as identificacion_tomador
	, tpr.tpr_tom_nombre		as tomador
	, tdr.tom_departamento		as departamento_tom
	, tdr.tom_ciudad		as ciudad_tom
	, tdr.tom_direccion		as direcion_tom
	, tpr.tpr_tom_telefono		as telefono_tomador
	, tpr.tpr_tom_email		as email_tomador
	, tpr.tpr_pgm_documento		as identifica_pagador_maestro
	, tpr.tpr_pgm_nombre		as informacion_pagador_maestro
	, case when ctx.prprdct_static = 54959 then rua.rua_tipodocumento when oaa.plcdco_operationpk is null then '0|0|Asegurado no existe.' when length(oaa.oaa_tipodocumento) > 120 then '0|0|Error en Asegurado.' else oaa.oaa_tipodocumento end		as tipo_identif_asegurado
	, case when ctx.prprdct_static = 54959 then rua.rua_documentoidentidad when oaa.plcdco_operationpk is null then '0|0|Asegurado no existe.' when length(oaa.oaa_documentoidentidad) > 120 then '0|0|Error en Asegurado.' else oaa.oaa_documentoidentidad end		as identif_asegurado
	, case when ctx.prprdct_static = 54959 then rua.rua_nombre when oaa.plcdco_operationpk is null then '0|0|Asegurado no existe.' when length(oaa.oaa_nombre) > 120 then '0|0|Error en Asegurado.' else oaa.oaa_nombre end		as asegurado
	, pmhgrsgr.idempleadorinput		as identificacion_empleador
	, pmhgrsgr.empleadorinput		as nombre_empleador
	, tpr.tpr_pag_documento		as identificacion_pagador
	, tpr.tpr_pag_nombre		as pagador
	, ctx.stpsgrmntsttcdt_ast_id		as idacuerdo
	, to_char(ctx.staggrmntvrsn_ast_fecini, 'DD-MM-YYYY')		as fechaini_acuerdo
	, to_char(ctx.staggrmntvrsn_ast_fecfin, 'DD-MM-YYYY')		as fechafin_acuerdo
	, ctx.stpsgrmntsttcdt_ast_numeroacu		as numero_poliza_matriz
	, ctx.stpsgrmntsttcdt_ast_nombreacu		as nombre_acuerdo
	, case when ctx.staggrmntvrsn_status = '1' then 'ACTIVO' when ctx.staggrmntvrsn_status = '2' then 'SUSPENDIDO' else '' || ctx.staggrmntvrsn_status || '' end		as estado_acuerdo
	, ctx.prplc_tipopolizainput		as tipo_poliza
	, ctx.prplc_numeropolizainput		as nro_poliza
	, prgrmnt.frecuenciapagoinput		as frecuencia_pago
	, nvl(ctx.ctx_time_stamp, '01/01/1999')		as fechaemision
	, (extract(year from ctx.ctx_time_stamp) || lpad(extract(month from ctx.ctx_time_stamp), 2, 0))		as periodo_fechaemision
	, case when ctx.ctx_status = '1' then 'PÓLIZA_PENDIENTE' when ctx.ctx_status = '2' then 'PÓLIZA_EMITIDA' else '' || ctx.ctx_status || '' end		as estado_poliza
	, ctx.plcdco_initialdate		as fecha_incial_pol
	, ctx.plcdco_finishdate		as fecha_final_pol
	, prgrmnt.tipoproduccioninput		as tipo_produccion
	, prgrmnt.unidadnegociovalue		as cod_unidad_negocio
	, prgrmnt.unidadnegocioinput		as unidad_negocio
	, ctx.plcdco_operationpk		as nro_operacion
	, coalesce(oanctrrddrs.tipobieninput, oamhgrsgr.tipobienmihogarseguroinput, rdcnl.tipoconjuntoinput)		as tipo_inmueble
	, coalesce(oanctrrddrs.direccioninmuebleinput, rmhgrsgr.direccioninmuebleinput, rdcnl.direccioninmuebleinput)		as direccion_inmueble
	, coalesce(oanctrrddrs.departamentovalue, rmhgrsgr.departamentovalue, rmscts.departamentovalue, rdcnl.departamentovalue)		as cod_departamento_inmueble
	, coalesce(oanctrrddrs.departamentoinput, rmhgrsgr.departamentoinput, rmscts.departamentoinput, rdcnl.departamentoinput)		as departamento_inmueble
	, coalesce(oanctrrddrs.ciudadvalue, rmhgrsgr.ciudadvalue, rmscts.ciudadvalue, rdcnl.ciudadvalue)		as cod_ciudad_inmueble
	, coalesce(oanctrrddrs.ciudadinput, rmhgrsgr.ciudadinput, rmscts.ciudadinput, rdcnl.ciudadinput)		as ciudad_inmueble
	, coalesce((substr(oanctrrddrs.departamentovalue, 1, length(oanctrrddrs.departamentovalue) - 2) || lpad(replace(oanctrrddrs.ciudadvalue, '.0', ''), 3, '0')),
			 (substr(rmhgrsgr.departamentovalue, 1, length(rmhgrsgr.departamentovalue) - 2) || lpad(replace(rmhgrsgr.ciudadvalue, '.0', ''), 3, '0')),
			 (substr(rmscts.departamentovalue, 1, length(rmscts.departamentovalue) - 2) || lpad(replace(rmscts.ciudadvalue, '.0', ''), 3, '0')),
			 (substr(rdcnl.departamentovalue, 1, length(rdcnl.departamentovalue) - 2) || lpad(replace(rdcnl.ciudadvalue, '.0', ''), 3, '0')))		as cod_divipol_municipio
	, coalesce(oanctrrddrs.codigozonasismicainput, rmhgrsgr.codigozonasismicainput, rdcnl.codigozonasismicavalue)		as cod_zona_sismica
	, coalesce(oanctrrddrs.tipoestructuralinput, oamhgrsgr.tipobienmihogarseguroinput)		as tipo_estructura_bien
	, coalesce(oanctrrddrs.tipoestructuralvalue, oamhgrsgr.tipobienmihogarsegurovalue)		as cod_tipo_estructura_bien
	, oamscts.razamascotainput		as raza_mascota
	, oamscts.tipomascotainput		as tipo_mascota
	, oamscts.edadmascotainput		as edad_mascota
	, oamscts.generomascotainput		as genero_mascota
	, oamscts.observacionesgeninput		as detalle_mascota
	, oamscts.nombremascotainput		as nombre_mascota
	, coalesce(prgrmnt.lineacreditoinput, pdcnl.lineacreditoinput)		as linea_credito
	, nvl(ctx.vnttp_description, ctx.plcdco_event)		as descripcion_evento
	, ctx.prplc_monedainput		as moneda_acuerdo
	, ctx.prrsknts_numunidriesgoinput		as unidad_riesgo
	, coalesce(rvddrs.numerocreditoinput, rnctrrddrs.numerocreditoinput, rprtccnfnncr.numerocreditoinput)		as numero_credito
	, ctx.rskntdco_initialdate		as fecha_inicial_ur
	, ctx.rskntdco_finishdate		as fecha_final_ur
	, ctx.plcdco_operationpk		as nro_operacion_pol
	, coalesce(pmhgrsgr.indorigeninput, pmscts.indorigeninput, pprtccnfnncr.indorigeninput)		as origen_dato
	, nvl(pmhgrsgr.operacionmihogarseguroinput, pmscts.operacionmihogarseguroinput)		as nro_operacion_factura
	, pdcnl.fechainicioproyectoinput		as fecha_inicial_proyecto
	, pdcnl.fechafinproyectoinput		as fecha_final_proyecto
	, pdcnl.periododegraciainput		as periodo_gracia
	, pdcnl.montoanticipovalue		as valor_anticipo
	, rdcnl.nombreedificioinput		as nombre_edificio
	, rdcnl.numeropisosinput		as numero_pisos
	, rdcnl.anoconstruccioninput		as ano_construcion_edificio
	, coalesce(cnclrhgr.fechamovimientoinput, nlr.fechamovimientoinput)		as fecha_mov_cancelanul
	, coalesce(cnclrhgr.fechaefectivainput, nlr.fechaefectivainput)		as fecha_efectiva_cancelanu
	, coalesce(cnclrhgr.motivoanulacioninput, nlr.motivoanulacioninput)		as motivo_cancelanul
	, pprtccnfnncr.llaveinput		as llave_integral_prod
	, pprtccnfnncr.vigenciatecnicainput		as vigencia_tecnica
	, rprtccnfnncr.porccomisrecauddesvalue		as porc_comis_recaudo
	, rprtccnfnncr.porccomisionreadesvalue		as porc_comis_reaseguro
	, rprtccnfnncr.porccomisionreaapvalue		as porc_comis_reaseguro_vda
	, rprtccnfnncr.valorcomisionreasegdesvalue		as valor_comision_reaseguro
	, rprtccnfnncr.valorcomisionreasegapvalue		as valor_comision_reaseguro_vda
	, age.age_nombre		as agente
	, ctx.plcdco_operationpk		as key_operationpk
	, ctx.plcdco_agregatedobjectid		as key_policyid
	, ctx.prprdct_static		as key_productid
from acsele_alfa_bi.em_gn_6 ctx
left join acsele_alfa.pomihogarseguro pmhgrsgr
on ctx.plcdco_dcoid = pmhgrsgr.pk
left join acsele_alfa.povdeudores pvddrs
on ctx.plcdco_agregatedobjectid = pvddrs.static		-- sin columnas: se conserva porque cruza por static y puede multiplicar filas igual que la original
left join acsele_alfa.pomascotas pmscts
on ctx.plcdco_agregatedobjectid = pmscts.static
left join acsele_alfa.podecenal pdcnl
on ctx.plcdco_agregatedobjectid = pdcnl.static
left join acsele_alfa.poproteccionfinanciera pprtccnfnncr
on ctx.prplc_pk = pprtccnfnncr.pk
left join acsele_alfa.cancelarhogar cnclrhgr
on ctx.vntdco_iddcoevent = cnclrhgr.pk
left join acsele_alfa.anular nlr
on ctx.vntdco_iddcoevent = nlr.pk
left join acsele_alfa.preagreement prgrmnt
on ctx.staggrmntvrsn_dcoid = prgrmnt.pk
left join acsele_alfa.urvdeudores rvddrs
on ctx.rskntdco_dcoid = rvddrs.pk
left join acsele_alfa.urincterrdeudores rnctrrddrs
on ctx.rskntdco_dcoid = rnctrrddrs.pk
left join acsele_alfa.urmihogarseguro rmhgrsgr
on ctx.rskntdco_dcoid = rmhgrsgr.pk
left join acsele_alfa.urmascotas rmscts
on ctx.rskntdco_dcoid = rmscts.pk
left join acsele_alfa.urdecenal rdcnl
on ctx.rskntdco_dcoid = rdcnl.pk
left join acsele_alfa.urproteccionfinanciera rprtccnfnncr
on ctx.rskntdco_dcoid = rprtccnfnncr.pk
left join acsele_alfa.oaincterrdeudores oanctrrddrs
on ctx.nsrncbjctdco_dcoid = oanctrrddrs.pk
left join acsele_alfa.oamihogarseguro oamhgrsgr
on ctx.nsrncbjctdco_dcoid = oamhgrsgr.pk
and oamhgrsgr.tipobienmihogarsegurovalue = '1.0'
and oamhgrsgr.pk is not null
left join acsele_alfa.oamascotas oamscts
on ctx.nsrncbjctdco_dcoid = oamscts.pk
left join acsele_alfa_bi.em_gn_tdr tdr
on ctx.grgtdplc_agregatedpolicyid = tdr.grgtdplc_agregatedpolicyid
left join acsele_alfa_bi.em_gn_age age
on ctx.grgtdplc_agregatedpolicyid = age.grgtdplc_agregatedpolicyid
left join acsele_alfa_bi.em_gn_tpr tpr
on ctx.plcdco_operationpk = tpr.plcdco_operationpk
left join acsele_alfa_bi.em_gn_rua rua
on ctx.plcdco_operationpk = rua.plcdco_operationpk
left join acsele_alfa_bi.em_gn_oaa oaa
on ctx.plcdco_operationpk = oaa.plcdco_operationpk
;
commit;

create index acsele_alfa_bi.em_gn_7_idx_1
on acsele_alfa_bi.em_gn_7 (key_operationpk, key_policyid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_gn_7
;


/* ============================================================================================ */
/* GENERALES · PASO 9 - FILTRO DE TAGS: los 227 conceptos de las metricas + suma asegurada      */
/*    (descproperty)                                                                            */
/*                                                                                              */
/* Toma la base común de movimientos em_tmb y la restringe a las operaciones de Generales con   */
/*    el mismo cruce de su armado (operationpk + policyid); luego aplica sus conceptos.         */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_gn_9;
commit;
*/

create table acsele_alfa_bi.em_gn_9
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  tm.thrdprtmvmntplc_operationpk
	, tm.thrdprtmvmntplc_policyid
	, tm.thrdprtmvmntplc_concept
	, tm.thrdprtmvmntplc_concept_u
	, tm.thrdprtmvmntplc_descproperty_u
	, tm.thrdprtmvmntplc_ammount
from acsele_alfa_bi.em_tmb tm
inner join acsele_alfa_bi.em_gn_opr opr
on tm.thrdprtmvmntplc_operationpk = opr.plcdco_operationpk
and tm.thrdprtmvmntplc_policyid = opr.plcdco_agregatedobjectid
where (tm.thrdprtmvmntplc_concept_u in ('PRIMA', 'PRIMAREHAB', 'PRIMAANU', 'PRIMADEV', 'PRIMAPERIODOPROVIS',
									'PRIMAEMITIDAIND', 'PRIMAREHABIND', 'PRIMAANUIND', 'PRIMADEVUELTAIND', 'PRIMAPERIODOPROVISIND',
									'PRIMAINCENDIO', 'PRIMATERREMOTO', 'PRIMAINCENDIOPROVIS', 'PRIMATERREMOTOPROVIS', 'PRIMAINCENDIODEV',
									'PRIMATERREMOTODEV', 'PRIMAINCENDIOREHAB', 'PRIMATERREMOTOREHAB', 'PRIMATERREMOTOANU', 'PRIMAINCENDIOANU',
									'PRIMAASISTENCIA', 'PRIMAASISTENCIADEV', 'PRIMASUSTRACCION', 'PRIMASUSTRACCIONDEV', 'PRIMARESPONSABILIDADCIVIL',
									'PRIMARESPONSABILIDADCIVILDEV', 'PRIMAGENERALES', 'PRIMAGENERALESDEV', 'PRIMAGENERALESPROVIS', 'PRIMAGENERALESPROVISDEV',
									'PRIMARC', 'PRIMARCANU', 'PRIMARCANUDEV', 'PRIMARCDEV', 'PRIMARCPROVIS',
									'PRIMARCREHAB', 'PRIMARCPROVISANU', 'PRIMASUS', 'PRIMASUSDEV', 'PRIMADES',
									'PRIMADESDEV', 'PRIMAVIDA', 'PRIMAVIDADEV', 'PRIMAACC', 'PRIMAACCDEV',
									'COSTORECAUDACION', 'COSTORECAUDACIONDEV', 'COSTORECAUDOPROVIS', 'COSTORECAUDOREHAB', 'COSTORECAUDOANU',
									'COSTORECAUDACIONIND', 'COSTORECAUDACIONDEVIND', 'COSTORECAUDOPROVISIND', 'COSTORECAUDOREHABIND', 'COSTORECAUDOANUIND',
									'COSTORECAUDOINCENDIO', 'COSTORECAUDOTERREMOTO', 'COSTORECAUDOINCPROVIS', 'COSTORECAUDOTERRPROVIS', 'COSTORECAUDOINCENDIODEV',
									'COSTORECAUDOTERREMOTODEV', 'COSTORECAUDOINCREHAB', 'COSTORECAUDOTERRREHAB', 'COSTORECAUDOINCANU', 'COSTORECAUDOTERRANU',
									'COMISIONRECAUDOGENERALES', 'COMISIONRECAUDOGENERALESDEV', 'COMISIONRECAUDOGENERALESPROVIS', 'COMISIONRECAUDOGENERALESPROVISDEV', 'COSTORECAUDORC',
									'COSTORECAUDORCDEV', 'COSTORECAUDORCPROVIS', 'COSTORECAUDORCREHAB', 'COSTORECAUDORCANU', 'COSTORECAUDORCPROVISANU',
									'COMISIONRECAUDODES', 'COMISIONRECAUDODESDEV', 'COMISIONRECAUDOSUS', 'COMISIONRECAUDOSUSDEV', 'COMISIONRECAUDOVIDA',
									'COMISIONRECAUDOVIDADEV', 'COMISIONRECAUDOACC', 'COMISIONRECAUDOACCDEV', 'IVACOSTORECAUDACION', 'IVACOSTORECAUDACIONDEV',
									'IVACOSTORECAUDOPROVIS', 'IVACOSTORECAUDOREHAB', 'IVACOSTORECAUDOANU', 'IVACOSTORECAUDACIONIND', 'IVACOSTORECAUDACIONDEVIND',
									'IVACOSTORECAUDOPROVISIND', 'IVACOSTORECAUDOREHABIND', 'IVACOSTORECAUDOANUIND', 'IVACOSTORECAUDOINCREHAB', 'IVACOSTORECAUDOTERRREHAB',
									'IVACOSTORECAUDOINCE', 'IVACOSTORECAUDOINCEDEV', 'IVACOSTORECAUDOTERR', 'IVACOSTORECAUDOTERRDEV', 'IVACOSTORECAUDOINCPROVIS',
									'IVACOSTORECAUDOTERRPROVIS', 'IVACOSTORECAUDOINCANU', 'IVACOSTORECAUDOINCANUDEV', 'IVACOSTORECAUDOINCENDIODEV', 'IVACOSTORECAUDOTERRANU',
									'IVACOMISIONRECAUDOGENERALES', 'IVACOMISIONRECAUDOGENERALESDEV', 'IVACOMISIONRECAUDOGENERALESPROVIS', 'IVACOMISIONRECAUDOGENERALESPROVISDEV', 'IVACOSTORECAUDORC',
									'IVACOSTORECAUDORCDEV', 'IVACOSTORECAUDORCPROVIS', 'IVACOSTORECAUDORCREHAB', 'IVACOSTORECAUDORCANU', 'IVACOSTORECAUDORCPROVISANU',
									'IVACOMISIONRECAUDODES', 'IVACOMISIONRECAUDODESDEV', 'IVACOMISIONRECAUDOSUS', 'IVACOMISIONRECAUDOSUSDEV', 'IVACOMISIONRECAUDOVIDA',
									'IVACOMISIONRECAUDOVIDADEV', 'IVACOMISIONRECAUDOACC', 'IVACOMISIONRECAUDOACCDEV', 'COMISIONEMITIDA', 'COMISIONDEVUELTA',
									'COMISIONINCENDIOANU', 'COMISIONINCENDIO', 'COMISIONINCENDIOREHAB', 'COMISIONINCENDIOPROVIS', 'IVACOMISIÓN',
									'COMISIONTERREMOTOANU', 'COMISIONTERREMOTO', 'COMISIONTERREMOTOREHAB', 'COMISIONTERREMOTOPROVIS', 'IVACOSTORECAUDOINCENDIO',
									'IVAPRIMA', 'IVAPRIMADEVUELTA', 'IVAPRIMAANULADA', 'IVAPRIMAGENERALES', 'IVAPRIMAGENERALESDEV',
									'IVAPRIMAGENERALESPROVIS', 'IVAPRIMAGENERALESPROVISDEV', 'IVARC', 'IVARCANU', 'IVARCDEV',
									'IVARCPROVIS', 'IVARCREHAB', 'IVAPRIMADES', 'IVAPRIMADESDEV', 'IVAPRIMASUS',
									'IVAPRIMASUSDEV', 'IVAPRIMAACC', 'IVAPRIMAACCDEV', 'IVAINCENDIO', 'IVAINCENDIODEV',
									'IVAINCENDIOREHAB', 'IVAINCENDIOPROVIS', 'IVAINCENDIOANU', 'IVAINCENDIOANUDEV', 'IVATERREMOTO',
									'IVATERREMOTODEV', 'IVATERREMOTOREHAB', 'IVATERREMOTOPROVIS', 'IVATERREMOTOANU', 'IVATERREMOTOANUDEV',
									'IVARCPROVISANU', 'CONTRIBBOMINCENDIOPROVIS', 'CONTRIBBOMINCENDIOREHAB', 'CONTRIBUCIONBOMINCE', 'CONTRIBUCIONBOMINCENDIO',
									'CONTRIBBOMINCENDIOANU', 'CONTRIBUCIONBOMINCEDEV', 'CONTRIBUCIONBOMINCENDIODEV', 'CONTRIBBOMTERREMOTOPROVIS', 'CONTRIBBOMTERREMOTOREHAB',
									'CONTRIBUCIONBOMTERR', 'CONTRIBUCIONBOMTERREMOTO', 'CONTRIBBOMTERREMOTOANU', 'CONTRIBUCIONBOMTERRDEV', 'CONTRIBUCIONBOMTERREMOTODEV',
									'CONTRIBUCIONBOM', 'CONTRIBUCIONBOMDEV', 'CONTRIBUCIONBOMPROVIS', 'CONTRIBUCIONBOMPROVISDEV', 'IVAASISTENCIA',
									'IVAASISTENCIADEV', 'IVASUSTRACCION', 'IVASUSTRACCIONDEV', 'IVARESPONSABILIDADCIVIL', 'IVARESPONSABILIDADCIVILDEV',
									'COMISIONNETAALFAGENERALES', 'COMISIONNETAALFAGENERALESDEV', 'COMISIONNETAALFAGENERALESPROVIS', 'COMISIONNETAALFAGENERALESPROVISDEV', 'COMISIONNETAALFADES',
									'COMISIONNETAALFASUS', 'COMISIONNETAALFADESDEV', 'COMISIONNETAALFASUSDEV', 'COMISIONNETAALFAVIDA', 'COMISIONNETAALFAVIDADEV',
									'COMISIONNETAALFAVIDAPROVIS', 'COMISIONNETAALFAVIDAPROVISDEV', 'COMISIONNETAALFAACC', 'COMISIONNETAALFAACCDEV', 'COMISIONREASEGUROSGENERALES',
									'COMISIONREASEGUROSGENERALESDEV', 'COMISIONREASEGUROSGENERALESPROVIS', 'COMISIONREASEGUROSGENERALESPROVISDEV', 'NETOREASEGUROSDES', 'NETOREASEGUROSDESDEV',
									'NETOREASEGUROSSUS', 'NETOREASEGUROSSUSDEV', 'COMISIONREASEGUROSVIDA', 'COMISIONREASEGUROSVIDADEV', 'NETOREASEGUROSACC',
									'NETOREASEGUROSACCDEV', 'COMISIONTARJETAGENERALES', 'COMISIONTARJETAGENERALESDEV', 'COMISIONTARJETAGENERALESPROVIS', 'COMISIONTARJETADES',
									'COMISIONTARJETADESDEV', 'COMISIONTARJETASUS', 'COMISIONTARJETASUSDEV', 'COMISIONTARJETAVIDA', 'COMISIONTARJETAVIDADEV',
									'COMISIONTARJETAACC', 'COMISIONTARJETAACCDEV')
or (tm.thrdprtmvmntplc_descproperty_u in ('VALORASEGURADOINCENDIOTERREMOTO', 'SUMAASEGURADAPERIODO', 'SAEDIFICIO',
									'SACONTENIDOSGENERALES', 'SACONTENIDOSMOVILES', 'SARESPONSABILIDADCIVIL',
									'SACONTENIDOSESPECIALES')
	and tm.thrdprtmvmntplc_concept like '%Aseg%'))
;
commit;

create index acsele_alfa_bi.em_gn_9_idx_1
on acsele_alfa_bi.em_gn_9 (thrdprtmvmntplc_operationpk, thrdprtmvmntplc_policyid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_gn_9
;


/* ============================================================================================ */
/* GENERALES · PASO 10 - METRICAS: las 45 metricas de movimientos en una sola lectura, group    */
/*    by operacion                                                                              */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_gn_10;
commit;
*/

create table acsele_alfa_bi.em_gn_10
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  tm.thrdprtmvmntplc_operationpk
	, tm.thrdprtmvmntplc_policyid
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('PRIMA', 'PRIMAREHAB', 'PRIMAANU', 'PRIMADEV', 'PRIMAPERIODOPROVIS',
					'PRIMAEMITIDAIND', 'PRIMAREHABIND', 'PRIMAANUIND', 'PRIMADEVUELTAIND', 'PRIMAPERIODOPROVISIND',
					'PRIMAINCENDIO', 'PRIMATERREMOTO', 'PRIMAINCENDIOPROVIS', 'PRIMATERREMOTOPROVIS', 'PRIMAINCENDIODEV',
					'PRIMATERREMOTODEV', 'PRIMAINCENDIOREHAB', 'PRIMATERREMOTOREHAB', 'PRIMATERREMOTOANU', 'PRIMAINCENDIOANU',
					'PRIMAASISTENCIA', 'PRIMAASISTENCIADEV', 'PRIMASUSTRACCION', 'PRIMASUSTRACCIONDEV', 'PRIMARESPONSABILIDADCIVIL',
					'PRIMARESPONSABILIDADCIVILDEV', 'PRIMAGENERALES', 'PRIMAGENERALESDEV', 'PRIMAGENERALESPROVIS', 'PRIMAGENERALESPROVISDEV',
					'PRIMARC', 'PRIMARCANU', 'PRIMARCANUDEV', 'PRIMARCDEV', 'PRIMARCPROVIS',
					'PRIMARCREHAB', 'PRIMARCPROVISANU', 'PRIMASUS', 'PRIMASUSDEV', 'PRIMADES',
					'PRIMADESDEV')
				and tm.thrdprtmvmntplc_ammount <> 0
				then case when tm.thrdprtmvmntplc_concept_u in ('PRIMAANU', 'PRIMAANUIND', 'PRIMAINCENDIODEV', 'PRIMATERREMOTODEV', 'PRIMATERREMOTOANU',
						'PRIMAINCENDIOANU', 'PRIMAGENERALESDEV', 'PRIMADEV', 'PRIMADEVUELTAIND', 'PRIMAASISTENCIADEV',
						'PRIMASUSTRACCIONDEV', 'PRIMARESPONSABILIDADCIVILDEV', 'MONTODEVOLUCION', 'PRIMAGENERALESPROVISDEV', 'PRIMARCANU',
						'PRIMARCANUDEV', 'PRIMARCDEV', 'PRIMARCPROVISANU', 'PRIMASUSDEV', 'PRIMADESDEV')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_prima
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('PRIMAVIDA', 'PRIMAVIDADEV', 'PRIMAACC', 'PRIMAACCDEV')
				and tm.thrdprtmvmntplc_ammount <> 0
				then case when tm.thrdprtmvmntplc_concept_u in ('PRIMAVIDADEV', 'PRIMAACCDEV')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_prima_vida
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('PRIMAINCENDIO', 'PRIMAINCENDIOPROVIS', 'PRIMAINCENDIODEV', 'PRIMAINCENDIOREHAB', 'PRIMAINCENDIOANU')
				and tm.thrdprtmvmntplc_ammount <> 0
				then case when tm.thrdprtmvmntplc_concept_u in ('PRIMAINCENDIODEV', 'PRIMAINCENDIOANU')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_prima_incendio
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('PRIMATERREMOTO', 'PRIMATERREMOTOPROVIS', 'PRIMATERREMOTODEV', 'PRIMATERREMOTOREHAB', 'PRIMATERREMOTOANU')
				and tm.thrdprtmvmntplc_ammount <> 0
				then case when tm.thrdprtmvmntplc_concept_u in ('PRIMATERREMOTODEV', 'PRIMATERREMOTOANU')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_prima_terremoto
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('PRIMARC', 'PRIMARCANU', 'PRIMARCANUDEV', 'PRIMARCDEV', 'PRIMARCPROVIS',
					'PRIMARCREHAB', 'PRIMARCPROVISANU')
				and tm.thrdprtmvmntplc_ammount <> 0
				then case when tm.thrdprtmvmntplc_concept_u in ('PRIMARCANU', 'PRIMARCANUDEV', 'PRIMARCDEV', 'PRIMARCPROVISANU')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_prima_respcivil
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('COSTORECAUDACION', 'COSTORECAUDACIONDEV', 'COSTORECAUDOPROVIS', 'COSTORECAUDOREHAB', 'COSTORECAUDOANU',
					'COSTORECAUDACIONIND', 'COSTORECAUDACIONDEVIND', 'COSTORECAUDOPROVISIND', 'COSTORECAUDOREHABIND', 'COSTORECAUDOANUIND',
					'COSTORECAUDOINCENDIO', 'COSTORECAUDOTERREMOTO', 'COSTORECAUDOINCPROVIS', 'COSTORECAUDOTERRPROVIS', 'COSTORECAUDOINCENDIODEV',
					'COSTORECAUDOTERREMOTODEV', 'COSTORECAUDOINCREHAB', 'COSTORECAUDOTERRREHAB', 'COSTORECAUDOINCANU', 'COSTORECAUDOTERRANU',
					'COMISIONRECAUDOGENERALES', 'COMISIONRECAUDOGENERALESDEV', 'COMISIONRECAUDOGENERALESPROVIS', 'COMISIONRECAUDOGENERALESPROVISDEV', 'COSTORECAUDORC',
					'COSTORECAUDORCDEV', 'COSTORECAUDORCPROVIS', 'COSTORECAUDORCREHAB', 'COSTORECAUDORCANU', 'COSTORECAUDORCPROVISANU',
					'COMISIONRECAUDODES', 'COMISIONRECAUDODESDEV', 'COMISIONRECAUDOSUS', 'COMISIONRECAUDOSUSDEV')
				and tm.thrdprtmvmntplc_ammount <> 0
				then case when tm.thrdprtmvmntplc_concept_u in ('COSTORECAUDACIONDEV', 'COSTORECAUDACIONDEVIND', 'COSTORECAUDOTERREMOTODEV', 'COSTORECAUDOTERRANU', 'COSTORECAUDOINCENDIODEV',
						'COSTORECAUDOINCANU', 'COSTORECAUDOANU', 'COSTORECAUDOANUIND', 'COMISIONRECAUDOGENERALESDEV', 'COMISIONRECAUDOGENERALESPROVISDEV',
						'COSTORECAUDORCDEV', 'COSTORECAUDORCANU', 'COSTORECAUDORCPROVISANU', 'COMISIONRECAUDODESDEV', 'COMISIONRECAUDOSUSDEV')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_costo_recaudacion
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('COMISIONRECAUDOVIDA', 'COMISIONRECAUDOVIDADEV', 'COMISIONRECAUDOACC', 'COMISIONRECAUDOACCDEV')
				and tm.thrdprtmvmntplc_ammount <> 0
				then case when tm.thrdprtmvmntplc_concept_u in ('COMISIONRECAUDOVIDADEV', 'COMISIONRECAUDOACCDEV')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_costo_recaudacion_vida
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('COSTORECAUDOINCENDIO', 'COSTORECAUDOINCPROVIS', 'COSTORECAUDOINCENDIODEV', 'COSTORECAUDOINCREHAB', 'COSTORECAUDOINCANU')
				and tm.thrdprtmvmntplc_ammount <> 0
				then case when tm.thrdprtmvmntplc_concept_u in ('COSTORECAUDOINCENDIODEV', 'COSTORECAUDOINCANU')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_costo_recaudacion_inc
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('COSTORECAUDOTERREMOTO', 'COSTORECAUDOTERRPROVIS', 'COSTORECAUDOTERREMOTODEV', 'COSTORECAUDOTERRREHAB', 'COSTORECAUDOTERRANU')
				and tm.thrdprtmvmntplc_ammount <> 0
				then case when tm.thrdprtmvmntplc_concept_u in ('COSTORECAUDOTERREMOTODEV', 'COSTORECAUDOTERRANU')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_costo_recaudacion_terr
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('COSTORECAUDORC', 'COSTORECAUDORCDEV', 'COSTORECAUDORCPROVIS', 'COSTORECAUDORCREHAB', 'COSTORECAUDORCANU',
					'COSTORECAUDORCPROVISANU')
				and tm.thrdprtmvmntplc_ammount <> 0
				then case when tm.thrdprtmvmntplc_concept_u in ('COSTORECAUDORCDEV', 'COSTORECAUDORCANU')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_costo_recaudacion_respcivil
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('IVACOSTORECAUDACION', 'IVACOSTORECAUDACIONDEV', 'IVACOSTORECAUDOPROVIS', 'IVACOSTORECAUDOREHAB', 'IVACOSTORECAUDOANU',
					'IVACOSTORECAUDACIONIND', 'IVACOSTORECAUDACIONDEVIND', 'IVACOSTORECAUDOPROVISIND', 'IVACOSTORECAUDOREHABIND', 'IVACOSTORECAUDOANUIND',
					'IVACOSTORECAUDOINCREHAB', 'IVACOSTORECAUDOTERRREHAB', 'IVACOSTORECAUDOINCE', 'IVACOSTORECAUDOINCEDEV', 'IVACOSTORECAUDOTERR',
					'IVACOSTORECAUDOTERRDEV', 'IVACOSTORECAUDOINCPROVIS', 'IVACOSTORECAUDOTERRPROVIS', 'IVACOSTORECAUDOINCANU', 'IVACOSTORECAUDOINCANUDEV',
					'IVACOSTORECAUDOINCENDIODEV', 'IVACOSTORECAUDOTERRANU', 'IVACOMISIONRECAUDOGENERALES', 'IVACOMISIONRECAUDOGENERALESDEV', 'IVACOMISIONRECAUDOGENERALESPROVIS',
					'IVACOMISIONRECAUDOGENERALESPROVISDEV', 'IVACOSTORECAUDORC', 'IVACOSTORECAUDORCDEV', 'IVACOSTORECAUDORCPROVIS', 'IVACOSTORECAUDORCREHAB',
					'IVACOSTORECAUDORCANU', 'IVACOSTORECAUDORCPROVISANU', 'IVACOMISIONRECAUDODES', 'IVACOMISIONRECAUDODESDEV', 'IVACOMISIONRECAUDOSUS',
					'IVACOMISIONRECAUDOSUSDEV')
				and tm.thrdprtmvmntplc_ammount <> 0
				then case when tm.thrdprtmvmntplc_concept_u in ('IVACOSTORECAUDACIONDEV', 'IVACOSTORECAUDACIONDEVIND', 'IVACOSTORECAUDOINCEDEV', 'IVACOSTORECAUDOTERRDEV', 'IVACOSTORECAUDOINCANU',
						'IVACOSTORECAUDOINCANUDEV', 'IVACOSTORECAUDOINCENDIODEV', 'IVACOSTORECAUDOTERRANU', 'IVACOSTORECAUDOANU', 'IVACOSTORECAUDOANUIND',
						'IVACOMISIONRECAUDOGENERALESDEV', 'IVACOMISIONRECAUDOGENERALESPROVISDEV', 'IVACOSTORECAUDORCDEV', 'IVACOSTORECAUDORCANU', 'IVACOSTORECAUDORCPROVISANU',
						'IVACOMISIONRECAUDOSUSDEV', 'IVACOMISIONRECAUDODESDEV')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_iva_costo_recaudacion
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('IVACOMISIONRECAUDOVIDA', 'IVACOMISIONRECAUDOVIDADEV', 'IVACOMISIONRECAUDOACC', 'IVACOMISIONRECAUDOACCDEV')
				and tm.thrdprtmvmntplc_ammount <> 0
				then case when tm.thrdprtmvmntplc_concept_u in ('IVACOMISIONRECAUDOVIDADEV', 'IVACOMISIONRECAUDOACCDEV')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_iva_costo_recaudacion_vida
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('COMISIONEMITIDA', 'COMISIONDEVUELTA', 'COMISIONINCENDIOANU', 'COMISIONINCENDIO', 'COMISIONINCENDIOREHAB',
					'COMISIONINCENDIOPROVIS', 'IVACOMISIÓN', 'COMISIONTERREMOTOANU', 'COMISIONTERREMOTO', 'COMISIONTERREMOTOREHAB',
					'COMISIONTERREMOTOPROVIS')
				and tm.thrdprtmvmntplc_ammount <> 0
				then case when tm.thrdprtmvmntplc_concept_u in ('COMISIONDEVUELTA', 'COMISIONINCENDIOANU', 'COMISIONTERREMOTOANU')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_comision_total_interincter
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('COMISIONINCENDIOANU', 'COMISIONINCENDIO', 'COMISIONINCENDIOREHAB', 'COMISIONINCENDIOPROVIS', 'IVACOMISIÓN')
				and tm.thrdprtmvmntplc_ammount <> 0
				then case when tm.thrdprtmvmntplc_concept_u in ('COMISIONINCENDIOANU')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_comision_intermediario_inc
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('COMISIONTERREMOTOANU', 'COMISIONTERREMOTO', 'COMISIONTERREMOTOREHAB', 'COMISIONTERREMOTOPROVIS')
				and tm.thrdprtmvmntplc_ammount <> 0
				then case when tm.thrdprtmvmntplc_concept_u in ('COMISIONTERREMOTOANU')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_comision_intermediario_terr
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('IVACOSTORECAUDOINCE', 'IVACOSTORECAUDOINCEDEV', 'IVACOSTORECAUDOINCREHAB', 'IVACOSTORECAUDOINCPROVIS', 'IVACOSTORECAUDOINCENDIO',
					'IVACOSTORECAUDOINCANU', 'IVACOSTORECAUDOINCANUDEV', 'IVACOSTORECAUDOINCENDIODEV')
				and tm.thrdprtmvmntplc_ammount <> 0
				then case when tm.thrdprtmvmntplc_concept_u in ('IVACOSTORECAUDOINCANU', 'IVACOSTORECAUDOINCANUDEV', 'IVACOSTORECAUDOINCENDIODEV', 'IVACOSTORECAUDOINCEDEV')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_iva_costo_recaudo_inc
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('IVACOSTORECAUDOTERR', 'IVACOSTORECAUDOTERRDEV', 'IVACOSTORECAUDOTERRREHAB', 'IVACOSTORECAUDOTERRPROVIS', 'IVACOSTORECAUDOTERRANU')
				and tm.thrdprtmvmntplc_ammount <> 0
				then case when tm.thrdprtmvmntplc_concept_u in ('IVACOSTORECAUDOTERRDEV', 'IVACOSTORECAUDOTERRANU')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_iva_costo_recaudo_terr
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('IVACOSTORECAUDORC', 'IVACOSTORECAUDORCDEV', 'IVACOSTORECAUDORCREHAB', 'IVACOSTORECAUDORCANU', 'IVACOSTORECAUDORCPROVIS',
					'IVACOSTORECAUDORCPROVISANU')
				and tm.thrdprtmvmntplc_ammount <> 0
				then case when tm.thrdprtmvmntplc_concept_u in ('IVACOSTORECAUDORCDEV', 'IVACOSTORECAUDORCANU', 'IVACOSTORECAUDORCPROVISANU')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_iva_costo_recaudo_rescivil
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('IVACOSTORECAUDOINCPROVIS')
				and tm.thrdprtmvmntplc_ammount <> 0
				then nvl(tm.thrdprtmvmntplc_ammount, 0) end)	as m_iva_costo_recaudo_provinc
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('IVACOSTORECAUDOTERRPROVIS')
				and tm.thrdprtmvmntplc_ammount <> 0
				then nvl(tm.thrdprtmvmntplc_ammount, 0) end)	as m_iva_costo_recaudo_provterr
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('IVAPRIMA', 'IVAPRIMADEVUELTA', 'IVAPRIMAANULADA', 'IVAPRIMAGENERALES', 'IVAPRIMAGENERALESDEV',
					'IVAPRIMAGENERALESPROVIS', 'IVAPRIMAGENERALESPROVISDEV', 'IVARC', 'IVARCANU', 'IVARCDEV',
					'IVARCPROVIS', 'IVARCREHAB', 'IVAPRIMADES', 'IVAPRIMADESDEV', 'IVAPRIMASUS',
					'IVAPRIMASUSDEV', 'IVAPRIMAACC', 'IVAPRIMAACCDEV')
				and tm.thrdprtmvmntplc_ammount <> 0
				then case when tm.thrdprtmvmntplc_concept_u in ('IVAPRIMADEVUELTA', 'IVAPRIMAANULADA', 'IVAPRIMAGENERALESDEV', 'IVAPRIMAGENERALESPROVISDEV', 'IVARCANU',
						'IVARCDEV', 'IVAPRIMADESDEV', 'IVAPRIMASUSDEV', 'IVAPRIMAACCDEV')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_iva_prima_total
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('IVAPRIMADES', 'IVAPRIMADESDEV', 'IVAPRIMASUS', 'IVAPRIMASUSDEV', 'IVAPRIMAACC',
					'IVAPRIMAACCDEV')
				and tm.thrdprtmvmntplc_ammount <> 0
				then case when tm.thrdprtmvmntplc_concept_u in ('IVAPRIMADESDEV', 'IVAPRIMASUSDEV', 'IVAPRIMAACCDEV')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_iva_prima_bancaseg
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('IVAINCENDIO', 'IVAINCENDIODEV', 'IVAINCENDIOREHAB', 'IVAINCENDIOPROVIS', 'IVAINCENDIOANU',
					'IVAINCENDIOANUDEV')
				and tm.thrdprtmvmntplc_ammount <> 0
				then case when tm.thrdprtmvmntplc_concept_u in ('IVAINCENDIODEV', 'IVAINCENDIOANU', 'IVAINCENDIOANUDEV')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_iva_prima_incendio
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('IVATERREMOTO', 'IVATERREMOTODEV', 'IVATERREMOTOREHAB', 'IVATERREMOTOPROVIS', 'IVATERREMOTOANU',
					'IVATERREMOTOANUDEV')
				and tm.thrdprtmvmntplc_ammount <> 0
				then case when tm.thrdprtmvmntplc_concept_u in ('IVATERREMOTODEV', 'IVATERREMOTOANU', 'IVATERREMOTOANUDEV')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_iva_prima_terremoto
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('IVARC', 'IVARCANU', 'IVARCDEV', 'IVARCPROVIS', 'IVARCREHAB')
				and tm.thrdprtmvmntplc_ammount <> 0
				then case when tm.thrdprtmvmntplc_concept_u in ('IVARCANU', 'IVARCDEV')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_iva_prima_repcivil
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('IVAINCENDIOPROVIS')
				and tm.thrdprtmvmntplc_ammount <> 0
				then nvl(tm.thrdprtmvmntplc_ammount, 0) end)	as m_iva_prima_provincendio
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('IVATERREMOTOPROVIS')
				and tm.thrdprtmvmntplc_ammount <> 0
				then nvl(tm.thrdprtmvmntplc_ammount, 0) end)	as m_iva_prima_provterremoto
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('IVARCPROVISANU', 'IVARCPROVIS')
				and tm.thrdprtmvmntplc_ammount <> 0
				then case when tm.thrdprtmvmntplc_concept_u in ('IVARCPROVISANU')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_iva_prima_provrespcivil
	, sum(case when tm.thrdprtmvmntplc_descproperty_u in ('VALORASEGURADOINCENDIOTERREMOTO', 'SUMAASEGURADAPERIODO', 'SAEDIFICIO',
					'SACONTENIDOSGENERALES', 'SACONTENIDOSMOVILES', 'SARESPONSABILIDADCIVIL',
					'SACONTENIDOSESPECIALES')
				and tm.thrdprtmvmntplc_ammount <> 0 and tm.thrdprtmvmntplc_concept like '%Aseg%'
				then nvl(tm.thrdprtmvmntplc_ammount, 0) end)	as m_suma_asegurada_periodo
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('CONTRIBBOMINCENDIOPROVIS', 'CONTRIBBOMINCENDIOREHAB', 'CONTRIBUCIONBOMINCE', 'CONTRIBUCIONBOMINCENDIO', 'CONTRIBBOMINCENDIOANU',
					'CONTRIBUCIONBOMINCEDEV', 'CONTRIBUCIONBOMINCENDIODEV')
				and tm.thrdprtmvmntplc_ammount <> 0
				then case when tm.thrdprtmvmntplc_concept_u in ('CONTRIBBOMINCENDIOANU', 'CONTRIBUCIONBOMINCEDEV', 'CONTRIBUCIONBOMINCENDIODEV')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_contrib_bomberos_incendio
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('CONTRIBBOMTERREMOTOPROVIS', 'CONTRIBBOMTERREMOTOREHAB', 'CONTRIBUCIONBOMTERR', 'CONTRIBUCIONBOMTERREMOTO', 'CONTRIBBOMTERREMOTOANU',
					'CONTRIBUCIONBOMTERRDEV', 'CONTRIBUCIONBOMTERREMOTODEV')
				and tm.thrdprtmvmntplc_ammount <> 0
				then case when tm.thrdprtmvmntplc_concept_u in ('CONTRIBBOMTERREMOTOANU', 'CONTRIBUCIONBOMTERRDEV', 'CONTRIBUCIONBOMTERREMOTODEV')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_contrib_bomberos_terremoto
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('CONTRIBUCIONBOM', 'CONTRIBUCIONBOMDEV', 'CONTRIBUCIONBOMPROVIS', 'CONTRIBUCIONBOMPROVISDEV')
				and tm.thrdprtmvmntplc_ammount <> 0
				then case when tm.thrdprtmvmntplc_concept_u in ('CONTRIBUCIONBOMDEV', 'CONTRIBUCIONBOMPROVISDEV')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_contrib_bomberos_hogar
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('PRIMAASISTENCIA', 'PRIMAASISTENCIADEV')
				and tm.thrdprtmvmntplc_ammount <> 0
				then case when tm.thrdprtmvmntplc_concept_u in ('PRIMAASISTENCIADEV')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_prima_asistencia
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('PRIMASUSTRACCION', 'PRIMASUSTRACCIONDEV')
				and tm.thrdprtmvmntplc_ammount <> 0
				then case when tm.thrdprtmvmntplc_concept_u in ('PRIMASUSTRACCIONDEV')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_prima_sustraccion
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('PRIMARESPONSABILIDADCIVIL', 'PRIMARESPONSABILIDADCIVILDEV')
				and tm.thrdprtmvmntplc_ammount <> 0
				then case when tm.thrdprtmvmntplc_concept_u in ('PRIMARESPONSABILIDADCIVILDEV')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_prima_responsabilidad
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('IVAASISTENCIA', 'IVAASISTENCIADEV')
				and tm.thrdprtmvmntplc_ammount <> 0
				then case when tm.thrdprtmvmntplc_concept_u in ('IVAASISTENCIADEV')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_iva_prima_asistencia
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('IVASUSTRACCION', 'IVASUSTRACCIONDEV')
				and tm.thrdprtmvmntplc_ammount <> 0
				then case when tm.thrdprtmvmntplc_concept_u in ('IVASUSTRACCIONDEV')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_iva_prima_sustraccion
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('IVARESPONSABILIDADCIVIL', 'IVARESPONSABILIDADCIVILDEV')
				and tm.thrdprtmvmntplc_ammount <> 0
				then case when tm.thrdprtmvmntplc_concept_u in ('IVARESPONSABILIDADCIVILDEV')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_iva_prima_responsabilidad
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('IVAPRIMAANULADA')
				and tm.thrdprtmvmntplc_ammount <> 0
				then case when tm.thrdprtmvmntplc_concept_u in ('IVAPRIMAANULADA')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_iva_prima_anulada_mascotas
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('COMISIONNETAALFAGENERALES', 'COMISIONNETAALFAGENERALESDEV', 'COMISIONNETAALFAGENERALESPROVIS', 'COMISIONNETAALFAGENERALESPROVISDEV', 'COMISIONNETAALFADES',
					'COMISIONNETAALFASUS', 'COMISIONNETAALFADESDEV', 'COMISIONNETAALFASUSDEV')
				then case when tm.thrdprtmvmntplc_concept_u in ('COMISIONNETAALFAGENERALESDEV', 'COMISIONNETAALFAGENERALESPROVISDEV', 'COMISIONNETAALFADESDEV', 'COMISIONNETAALFASUSDEV')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_comision_neta_alfa
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('COMISIONNETAALFAVIDA', 'COMISIONNETAALFAVIDADEV', 'COMISIONNETAALFAVIDAPROVIS', 'COMISIONNETAALFAVIDAPROVISDEV', 'COMISIONNETAALFAACC',
					'COMISIONNETAALFAACCDEV')
				then case when tm.thrdprtmvmntplc_concept_u in ('COMISIONNETAALFAVIDADEV', 'COMISIONNETAALFAVIDAPROVISDEV', 'COMISIONNETAALFAACCDEV')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_comision_neta_vida
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('COMISIONREASEGUROSGENERALES', 'COMISIONREASEGUROSGENERALESDEV', 'COMISIONREASEGUROSGENERALESPROVIS', 'COMISIONREASEGUROSGENERALESPROVISDEV', 'NETOREASEGUROSDES',
					'NETOREASEGUROSDESDEV', 'NETOREASEGUROSSUS', 'NETOREASEGUROSSUSDEV')
				then case when tm.thrdprtmvmntplc_concept_u in ('COMISIONREASEGUROSGENERALESDEV', 'COMISIONREASEGUROSGENERALESPROVISDEV', 'NETOREASEGUROSSUSDEV', 'NETOREASEGUROSDESDEV')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_valor_neto_reaseguro
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('COMISIONREASEGUROSVIDA', 'COMISIONREASEGUROSVIDADEV', 'NETOREASEGUROSACC', 'NETOREASEGUROSACCDEV')
				then case when tm.thrdprtmvmntplc_concept_u in ('COMISIONREASEGUROSVIDADEV', 'NETOREASEGUROSACCDEV')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_valor_neto_reaseguro_vida
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('COMISIONTARJETAGENERALES', 'COMISIONTARJETAGENERALESDEV', 'COMISIONTARJETAGENERALESPROVIS', 'COMISIONTARJETADES', 'COMISIONTARJETADESDEV',
					'COMISIONTARJETASUS', 'COMISIONTARJETASUSDEV')
				then case when tm.thrdprtmvmntplc_concept_u in ('COMISIONTARJETAGENERALESDEV')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_comision_tarjetacr
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('COMISIONTARJETAVIDA', 'COMISIONTARJETAVIDADEV', 'COMISIONTARJETAACC', 'COMISIONTARJETAACCDEV')
				then case when tm.thrdprtmvmntplc_concept_u in ('COMISIONTARJETAVIDADEV', 'COMISIONTARJETAACCDEV')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_comision_tarjetacr_vida
from acsele_alfa_bi.em_gn_9 tm
group by tm.thrdprtmvmntplc_operationpk
	, tm.thrdprtmvmntplc_policyid
;
commit;

create index acsele_alfa_bi.em_gn_10_idx_1
on acsele_alfa_bi.em_gn_10 (thrdprtmvmntplc_operationpk, thrdprtmvmntplc_policyid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_gn_10
;



/* ============================================================================================ */
/* ##############################  RAMA SATELITE  ##############################                */
/* ============================================================================================ */

/* ============================================================================================ */
/* SATÉLITE · AUX PPT - Filtro de roles: tomador, rol 25935, maestro, sponsor y agente          */
/*                                                                                              */
/* Toma la base común em_ppb y la restringe al universo de pólizas de Satélite antes de         */
/*    aplicar sus roles.                                                                        */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_st_ppt;
commit;
*/

create table acsele_alfa_bi.em_st_ppt
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  ppb.grgtdplc_agregatedpolicyid
	, ppb.sttsrl_rol_id
	, ppb.sttsrl_rol_description_u
	, ppb.stpplcprtcptndco_status
	, ppb.prthrdprty_static
	, ppb.prthrdprty_documentoidentidadinput
	, ppb.prthrdprty_nombrepredeterminadoinput
from acsele_alfa_bi.em_ppb ppb
inner join acsele_alfa_bi.em_st_pol pol
on ppb.grgtdplc_agregatedpolicyid = pol.grgtdplc_agregatedpolicyid
where (ppb.sttsrl_rol_description_u like '%TOMADOR%'
or ppb.sttsrl_rol_id = 25935
or ppb.sttsrl_rol_description_u like '%MAESTRO%'
or ppb.sttsrl_rol_description_u = 'SPONSOR'
or ppb.sttsrl_rol_description_u like '%AGENTE%')
;
commit;

create index acsele_alfa_bi.em_st_ppt_idx_1
on acsele_alfa_bi.em_st_ppt (grgtdplc_agregatedpolicyid);

create index acsele_alfa_bi.em_st_ppt_idx_2
on acsele_alfa_bi.em_st_ppt (prthrdprty_static);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_st_ppt
;


/* ============================================================================================ */
/* SATÉLITE · AUX TOM - Tomador por poliza (rol LIKE '%TOMADOR%'): documento, nombre y, via     */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_st_tom;
commit;
*/

create table acsele_alfa_bi.em_st_tom
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  ppt.grgtdplc_agregatedpolicyid
	, max(ppt.prthrdprty_documentoidentidadinput)		as tom_documento
	, max(ppt.prthrdprty_nombrepredeterminadoinput)	as tom_nombre
	, max(sgrdvd.fechanacimientoinput)					as tom_av_fechanacimiento
	, max(sgrdvd.sexoinput)							as tom_av_sexo
from acsele_alfa_bi.em_st_ppt ppt
left join acsele_alfa.thirdpartyrole thrdprtrl
on ppt.prthrdprty_static = thrdprtrl.thirdpartyid
left join acsele_alfa.aseguradovida sgrdvd
on thrdprtrl.iddcorole = sgrdvd.pk
where ppt.sttsrl_rol_description_u like '%TOMADOR%'
group by ppt.grgtdplc_agregatedpolicyid
;
commit;

create index acsele_alfa_bi.em_st_tom_idx_1
on acsele_alfa_bi.em_st_tom (grgtdplc_agregatedpolicyid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_st_tom
;


/* ============================================================================================ */
/* SATÉLITE · AUX PGM - Pagador maestro (rol LIKE '%MAESTRO%' y participacion activa)           */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_st_pgm;
commit;
*/

create table acsele_alfa_bi.em_st_pgm
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  ppt.grgtdplc_agregatedpolicyid
	, max(ppt.prthrdprty_documentoidentidadinput || '-' || ppt.prthrdprty_nombrepredeterminadoinput)	as pgm_informacion
from acsele_alfa_bi.em_st_ppt ppt
where ppt.sttsrl_rol_description_u like '%MAESTRO%'
and ppt.stpplcprtcptndco_status = 1
group by ppt.grgtdplc_agregatedpolicyid
;
commit;

create index acsele_alfa_bi.em_st_pgm_idx_1
on acsele_alfa_bi.em_st_pgm (grgtdplc_agregatedpolicyid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_st_pgm
;


/* ============================================================================================ */
/* SATÉLITE · AUX SPO - Sponsor (rol = 'SPONSOR')                                               */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_st_spo;
commit;
*/

create table acsele_alfa_bi.em_st_spo
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  ppt.grgtdplc_agregatedpolicyid
	, max(ppt.prthrdprty_nombrepredeterminadoinput)	as spo_nombre
from acsele_alfa_bi.em_st_ppt ppt
where ppt.sttsrl_rol_description_u = 'SPONSOR'
group by ppt.grgtdplc_agregatedpolicyid
;
commit;

create index acsele_alfa_bi.em_st_spo_idx_1
on acsele_alfa_bi.em_st_spo (grgtdplc_agregatedpolicyid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_st_spo
;


/* ============================================================================================ */
/* SATÉLITE · AUX AGE - Agente (rol LIKE '%AGENTE%')                                            */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_st_age;
commit;
*/

create table acsele_alfa_bi.em_st_age
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  ppt.grgtdplc_agregatedpolicyid
	, max(ppt.prthrdprty_nombrepredeterminadoinput)	as age_nombre
from acsele_alfa_bi.em_st_ppt ppt
where ppt.sttsrl_rol_description_u like '%AGENTE%'
group by ppt.grgtdplc_agregatedpolicyid
;
commit;

create index acsele_alfa_bi.em_st_age_idx_1
on acsele_alfa_bi.em_st_age (grgtdplc_agregatedpolicyid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_st_age
;


/* ============================================================================================ */
/* SATÉLITE · AUX TDB - Direccion del tomador BASE (rol 25935): tercero -> libreta de           */
/*    direcciones (por                                                                          */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_st_tdb;
commit;
*/

create table acsele_alfa_bi.em_st_tdb
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  ppt.grgtdplc_agregatedpolicyid
	, drccn.departamentoinput				as drccn_departamentoinput
	, drccn.ciudadinput						as drccn_ciudadinput
	, (substr(drccn.departamentovalue, 1, length(drccn.departamentovalue) - 2) || lpad(replace(drccn.ciudadvalue, '.0', ''), 3, '0'))	as drccn_divipol
	, to_char(drccn.direccioninput)			as drccn_direccioninput
from acsele_alfa_bi.em_st_ppt ppt
inner join acsele_alfa.stte_thirdparty sttthrdprty
on ppt.prthrdprty_static = sttthrdprty.tpt_id
inner join acsele_alfa.stte_thirdpartyaddressbook sttthrdprtddrssbk
on sttthrdprty.tpt_id = sttthrdprtddrssbk.tpt_id
and sttthrdprtddrssbk.isdefaultaddress = 1
inner join acsele_alfa.direccion drccn
on sttthrdprtddrssbk.iddco = drccn.pk
where ppt.sttsrl_rol_id = 25935
;
commit;

create index acsele_alfa_bi.em_st_tdb_idx_1
on acsele_alfa_bi.em_st_tdb (grgtdplc_agregatedpolicyid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_st_tdb
;


/* ============================================================================================ */
/* SATÉLITE · AUX TDR - Direccion del tomador por poliza: MAX() + group by (reemplaza ROWNUM    */
/*    <= 1)                                                                                     */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_st_tdr;
commit;
*/

create table acsele_alfa_bi.em_st_tdr
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  tdb.grgtdplc_agregatedpolicyid
	, max(tdb.drccn_departamentoinput)		as tom_departamento
	, max(tdb.drccn_ciudadinput)			as tom_ciudad
	, max(tdb.drccn_divipol)				as tom_divipol
	, max(tdb.drccn_direccioninput)			as tom_direccion
from acsele_alfa_bi.em_st_tdb tdb
group by tdb.grgtdplc_agregatedpolicyid
;
commit;

create index acsele_alfa_bi.em_st_tdr_idx_1
on acsele_alfa_bi.em_st_tdr (grgtdplc_agregatedpolicyid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_st_tdr
;


/* ============================================================================================ */
/* SATÉLITE · AUX TPF - Filtro de rol 25935 (tomador)                                           */
/*                                                                                              */
/* Toma la base común em_tpb y la restringe a las operaciones de Satélite.                      */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_st_tpf;
commit;
*/

create table acsele_alfa_bi.em_st_tpf
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  tpb.plcdco_operationpk
	, tpb.prthrdprty_telefonooficinacompletinput
	, tpb.prthrdprty_emailinput
from acsele_alfa_bi.em_tpb tpb
inner join acsele_alfa_bi.em_st_opr opr
on tpb.plcdco_operationpk = opr.plcdco_operationpk
where tpb.stpplcprtcptn_rol_id = 25935
;
commit;

create index acsele_alfa_bi.em_st_tpf_idx_1
on acsele_alfa_bi.em_st_tpf (plcdco_operationpk);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_st_tpf
;


/* ============================================================================================ */
/* SATÉLITE · AUX TPR - Telefono y correo del tomador por operacion: LISTAGG(..., ',') + group  */
/*    by (igual                                                                                 */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_st_tpr;
commit;
*/

create table acsele_alfa_bi.em_st_tpr
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  tpf.plcdco_operationpk
	, listagg(tpf.prthrdprty_telefonooficinacompletinput, ',') within group (order by tpf.plcdco_operationpk)	as tpr_tom_telefono
	, listagg(tpf.prthrdprty_emailinput, ',') within group (order by tpf.plcdco_operationpk)					as tpr_tom_email
from acsele_alfa_bi.em_st_tpf tpf
group by tpf.plcdco_operationpk
;
commit;

create index acsele_alfa_bi.em_st_tpr_idx_1
on acsele_alfa_bi.em_st_tpr (plcdco_operationpk);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_st_tpr
;


/* ============================================================================================ */
/* SATÉLITE · AUX OAF - Filtro de rol: rol_description LIKE '%Asegurado%'                       */
/*                                                                                              */
/* Toma la base común em_oab y la restringe a las operaciones de Satélite. El teléfono por      */
/*    personanatural es solo de Satélite y se queda en su rama.                                 */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_st_oaf;
commit;
*/

create table acsele_alfa_bi.em_st_oaf
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  oab.plcdco_operationpk
	, oab.prthrdprty_tipodocumentoinput
	, oab.prthrdprty_documentoidentidadinput
	, oab.prthrdprty_nombrepredeterminadoinput
	, oab.prthrdprty_emailinput
	, coalesce(oab.prthrdprty_telefonooficinacompletinput, prsnntrl.telefonoinput, prsnntrl.telefonofijocompletoinput, prsnntrl.telefonocelularinput)	as oab_telefono
from acsele_alfa_bi.em_oab oab
inner join acsele_alfa_bi.em_st_opr opr
on oab.plcdco_operationpk = opr.plcdco_operationpk
left join acsele_alfa.personanatural prsnntrl
on oab.prthrdprty_static = prsnntrl.static
where oab.sttsrl_rol_description like '%Asegurado%'
;
commit;

create index acsele_alfa_bi.em_st_oaf_idx_1
on acsele_alfa_bi.em_st_oaf (plcdco_operationpk);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_st_oaf
;


/* ============================================================================================ */
/* SATÉLITE · AUX OAA - Asegurado por operacion: MAX() + group by (igual que la original)       */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_st_oaa;
commit;
*/

create table acsele_alfa_bi.em_st_oaa
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  oaf.plcdco_operationpk
	, max(oaf.prthrdprty_tipodocumentoinput)			as oaa_tipodocumento
	, max(oaf.prthrdprty_documentoidentidadinput)		as oaa_documentoidentidad
	, max(oaf.prthrdprty_nombrepredeterminadoinput)	as oaa_nombre
	, max(oaf.prthrdprty_emailinput)					as oaa_email
	, max(oaf.oab_telefono)							as oaa_telefono
from acsele_alfa_bi.em_st_oaf oaf
group by oaf.plcdco_operationpk
;
commit;

create index acsele_alfa_bi.em_st_oaa_idx_1
on acsele_alfa_bi.em_st_oaa (plcdco_operationpk);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_st_oaa
;


/* ============================================================================================ */
/* SATÉLITE · AUX COB - Coberturas por operacion BASE: coveragedco -> cobertura -> tipo de      */
/*    reaseguro                                                                                 */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_st_cob;
commit;
*/

create table acsele_alfa_bi.em_st_cob
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  opr.plcdco_operationpk
	, cbrtr.cobramocontablevalue			as cbrtr_cobramocontablevalue
	, strrnsrnckd.rek_description			as strrnsrnckd_rek_description
from acsele_alfa_bi.em_st_opr opr
inner join acsele_alfa.coveragedco cvrgdco
on opr.plcdco_operationpk = cvrgdco.operationpk
inner join acsele_alfa.cobertura cbrtr
on cvrgdco.dcoid = cbrtr.pk
left join acsele_alfa.stri_reinsurancekind strrnsrnckd
on to_number(replace(cbrtr.cobramocontablevalue, '.0', '')) = strrnsrnckd.rek_code
;
commit;

create index acsele_alfa_bi.em_st_cob_idx_1
on acsele_alfa_bi.em_st_cob (plcdco_operationpk);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_st_cob
;


/* ============================================================================================ */
/* SATÉLITE · AUX COR - Ramo contable por operacion: MAX() + group by (igual que la original)   */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_st_cor;
commit;
*/

create table acsele_alfa_bi.em_st_cor
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  cob.plcdco_operationpk
	, max(cob.cbrtr_cobramocontablevalue)		as cor_cobramocontablevalue
	, max(cob.strrnsrnckd_rek_description)		as cor_rek_description
from acsele_alfa_bi.em_st_cob cob
group by cob.plcdco_operationpk
;
commit;

create index acsele_alfa_bi.em_st_cor_idx_1
on acsele_alfa_bi.em_st_cor (plcdco_operationpk);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_st_cor
;


/* ============================================================================================ */
/* SATÉLITE · AUX VTB - Valores tributarios BASE (antes funcion fn_valores_tributarios):        */
/*    openitem ->                                                                               */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_st_vtb;
commit;
*/

create table acsele_alfa_bi.em_st_vtb
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  opr.plcdco_operationpk
	, opr.plcdco_agregatedobjectid
	, prprty.simbolo						as prprty_simbolo
	, uaadtl.value							as uaadtl_value
from acsele_alfa_bi.em_st_opr opr
inner join acsele_alfa.openitem pnitm
on opr.plcdco_operationpk = pnitm.operationpk
inner join acsele_alfa.openitemreference pnitmrfrnc
on pnitm.openitemid = pnitmrfrnc.openitemid
and opr.plcdco_agregatedobjectid = pnitmrfrnc.policyid
inner join acsele_alfa.uaadetail uaadtl
on pnitm.openitemid = uaadtl.openitemid
inner join acsele_alfa.property prprty
on uaadtl.property = prprty.propertyid
;
commit;

create index acsele_alfa_bi.em_st_vtb_idx_1
on acsele_alfa_bi.em_st_vtb (prprty_simbolo);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_st_vtb
;


/* ============================================================================================ */
/* SATÉLITE · AUX VTF - Filtro de simbolos: IvaComision, ReteICA, ComisionPersJur,              */
/*    ReteIvaComision                                                                           */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_st_vtf;
commit;
*/

create table acsele_alfa_bi.em_st_vtf
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  vtb.plcdco_operationpk
	, vtb.plcdco_agregatedobjectid
	, vtb.prprty_simbolo
	, vtb.uaadtl_value
from acsele_alfa_bi.em_st_vtb vtb
where vtb.prprty_simbolo in ('IvaComision', 'ReteICA', 'ComisionPersJur', 'ReteIvaComision')
;
commit;

create index acsele_alfa_bi.em_st_vtf_idx_1
on acsele_alfa_bi.em_st_vtf (plcdco_operationpk, plcdco_agregatedobjectid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_st_vtf
;


/* ============================================================================================ */
/* SATÉLITE · AUX VTR - Valores tributarios por operacion y poliza: SUM por simbolo + group by  */
/*    (igual que                                                                                */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_st_vtr;
commit;
*/

create table acsele_alfa_bi.em_st_vtr
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  vtf.plcdco_operationpk
	, vtf.plcdco_agregatedobjectid
	, sum(case when vtf.prprty_simbolo = 'IvaComision' then vtf.uaadtl_value end)	as vtr_ivacomision
	, sum(case when vtf.prprty_simbolo = 'ReteICA' then vtf.uaadtl_value end)	as vtr_reteica
	, sum(case when vtf.prprty_simbolo = 'ComisionPersJur' then vtf.uaadtl_value end)	as vtr_comisionpersjur
	, sum(case when vtf.prprty_simbolo = 'ReteIvaComision' then vtf.uaadtl_value end)	as vtr_reteivacomision
from acsele_alfa_bi.em_st_vtf vtf
group by vtf.plcdco_operationpk
	, vtf.plcdco_agregatedobjectid
;
commit;

create index acsele_alfa_bi.em_st_vtr_idx_1
on acsele_alfa_bi.em_st_vtr (plcdco_operationpk, plcdco_agregatedobjectid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_st_vtr
;


/* ============================================================================================ */
/* SATÉLITE · AUX AGB - Participacion de intermediarios por operacion (reemplaza la             */
/*    subconsulta AGE del                                                                       */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_st_agb;
commit;
*/

create table acsele_alfa_bi.em_st_agb
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  opr.plcdco_operationpk
	, opr.plcdco_agregatedobjectid
	, prtcpcnntrmdrs.porcentajecomisioninput	as agb_porc_comis_agente
	, prtcpcnntrmdrs.porcentajeparticipacvalue	as agb_porc_part_agente
from acsele_alfa_bi.em_st_opr opr
inner join acsele_alfa.stpo_policyparticipationdco stpplcprtcptndco
on opr.plcdco_operationpk = stpplcprtcptndco.operationpk
inner join acsele_alfa.stpo_policyparticipation stpplcprtcptn
on stpplcprtcptndco.agregatedobjectid = stpplcprtcptn.agregatedobjectid
and opr.plcdco_agregatedobjectid = stpplcprtcptn.agregatedparentid
inner join acsele_alfa.participacionintermediarios prtcpcnntrmdrs
on stpplcprtcptndco.dcoid = prtcpcnntrmdrs.pk
inner join acsele_alfa.stte_thirdparty sttthrdprty
on stpplcprtcptn.thirdpartyid = sttthrdprty.tpt_id
inner join acsele_alfa.prethirdparty prthrdprty
on sttthrdprty.iddco = prthrdprty.pk
inner join acsele_alfa.stts_role sttsrl
on stpplcprtcptn.rol_id = sttsrl.rol_id
;
commit;

create index acsele_alfa_bi.em_st_agb_idx_1
on acsele_alfa_bi.em_st_agb (plcdco_operationpk, plcdco_agregatedobjectid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_st_agb
;


/* ============================================================================================ */
/* SATÉLITE · AUX ULO - Primera y ultima operacion de cada poliza (todas las operaciones de     */
/*    policydco)                                                                                */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_st_ulo;
commit;
*/

create table acsele_alfa_bi.em_st_ulo
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  pol.grgtdplc_agregatedpolicyid
	, max(plcdco.operationpk)				as ulo_max_operationpk
	, min(plcdco.operationpk)				as ulo_min_operationpk
from acsele_alfa_bi.em_st_pol pol
inner join acsele_alfa.policydco plcdco
on pol.grgtdplc_agregatedpolicyid = plcdco.agregatedobjectid
group by pol.grgtdplc_agregatedpolicyid
;
commit;

create index acsele_alfa_bi.em_st_ulo_idx_1
on acsele_alfa_bi.em_st_ulo (ulo_max_operationpk);

create index acsele_alfa_bi.em_st_ulo_idx_2
on acsele_alfa_bi.em_st_ulo (grgtdplc_agregatedpolicyid, ulo_min_operationpk);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_st_ulo
;


/* ============================================================================================ */
/* SATÉLITE · AUX EFP - Estado final de la poliza: evento de la ultima operacion (eventdco con  */
/*    policypk                                                                                  */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_st_efp;
commit;
*/

create table acsele_alfa_bi.em_st_efp
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  ulo.grgtdplc_agregatedpolicyid
	, max(vnttp.description)				as efp_description
from acsele_alfa_bi.em_st_ulo ulo
inner join acsele_alfa.eventdco vntdco
on ulo.ulo_max_operationpk = vntdco.operationpk
and vntdco.policypk is not null
inner join acsele_alfa.eventtype vnttp
on vntdco.eventtypeid = vnttp.eventtypeid
group by ulo.grgtdplc_agregatedpolicyid
;
commit;

create index acsele_alfa_bi.em_st_efp_idx_1
on acsele_alfa_bi.em_st_efp (grgtdplc_agregatedpolicyid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_st_efp
;


/* ============================================================================================ */
/* SATÉLITE · AUX PVI - Primera vigencia desde de la poliza: initialdate de la primera          */
/*    operacion                                                                                 */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_st_pvi;
commit;
*/

create table acsele_alfa_bi.em_st_pvi
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  ulo.grgtdplc_agregatedpolicyid
	, max(plcdco.initialdate)				as pvi_initialdate
from acsele_alfa_bi.em_st_ulo ulo
inner join acsele_alfa.policydco plcdco
on ulo.grgtdplc_agregatedpolicyid = plcdco.agregatedobjectid
and ulo.ulo_min_operationpk = plcdco.operationpk
group by ulo.grgtdplc_agregatedpolicyid
;
commit;

create index acsele_alfa_bi.em_st_pvi_idx_1
on acsele_alfa_bi.em_st_pvi (grgtdplc_agregatedpolicyid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_st_pvi
;


/* ============================================================================================ */
/* SATÉLITE · PASO 7 - LEFT JOINS (al final): evento, cancelaciones, POs, URs, OAs, acuerdo y   */
/*    auxiliares                                                                                */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_st_7;
commit;
*/

create table acsele_alfa_bi.em_st_7
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  ctx.grgtdplc_agregatedpolicyid		as idpoliza
	, ctx.grgtdplc_productid		as idproducto
	, replace(ctx.prprdct_companiavalue, '.0', '')		as codigo_compania
	, ctx.prprdct_companiainput		as compania
	, ctx.prprdct_codigoproductoinput		as codigo_producto
	, ctx.prdct_description		as producto
	, case when ctx.grgtdplc_productid not in (55119, 55060, 55059) then ctx.prprdct_ramovalue else coalesce(pprtccnsld.ramobancasegurosvalue, pprtccnvd.ramobancasegurosvalue) end		as codigo_ramo
	, case when ctx.grgtdplc_productid not in (55119, 55060, 55059) then ctx.prprdct_ramoinput else coalesce(pprtccnsld.ramobancasegurosinput, pprtccnvd.ramobancasegurosinput) end		as ramo
	, case when ctx.grgtdplc_productid not in (55119, 55060, 55059) then cor.cor_cobramocontablevalue else coalesce(pprtccnsld.ramobancasegurosvalue, pprtccnvd.ramobancasegurosvalue) end		as codigo_ramo_contable
	, case when ctx.grgtdplc_productid not in (55119, 55060, 55059) then cor.cor_rek_description else coalesce(pprtccnsld.ramobancasegurosinput, pprtccnvd.ramobancasegurosinput) end		as ramo_contable
	, replace(ctx.prplc_oficinavalue, '.0', '')		as codigo_sucursal
	, ctx.prplc_oficinainput		as sucursal
	, tom.tom_documento		as identificacion_tomador
	, tom.tom_nombre		as tomador
	, tdr.tom_departamento		as departamento_tom
	, tdr.tom_ciudad		as ciudad_tom
	, tdr.tom_divipol		as divipol_tom
	, tdr.tom_direccion		as direcion_tom
	, tpr.tpr_tom_telefono		as telefono_tomador
	, tpr.tpr_tom_email		as email_tomador
	, pgm.pgm_informacion		as informacion_pagador_maestro
	, oaa.oaa_tipodocumento		as tip_ident_asegurado
	, oaa.oaa_documentoidentidad		as ident_asegurado
	, oaa.oaa_nombre		as asegurado
	, ctx.stpsgrmntsttcdt_ast_id		as idacuerdo
	, ctx.stpsgrmntsttcdt_ast_numeroacu		as numero_poliza_matriz
	, ctx.stpsgrmntsttcdt_ast_nombreacu		as nombre_acuerdo
	, case when ctx.staggrmntvrsn_status = '1' then 'ACTIVO' when ctx.staggrmntvrsn_status = '2' then 'SUSPENDIDO' else '' || ctx.staggrmntvrsn_status || '' end		as estado_acuerdo
	, to_char(ctx.staggrmntvrsn_ast_fecini, 'DD-MM-YYYY')		as fechaini_acuerdo
	, to_char(ctx.staggrmntvrsn_ast_fecfin, 'DD-MM-YYYY')		as fechafin_acuerdo
	, ctx.prdct_description		as producto_del_acuerdo
	, ctx.prplc_tipopolizainput		as tipo_poliza
	, ctx.prplc_frecuenciapagoinput		as frecuencia_pago
	, ctx.prplc_numeropolizainput		as nro_poliza
	, nvl(ctx.prplc_canalventainput, 0)		as canal_venta
	, coalesce(pprctsgr.promotorinput, ppthlvr.promotorinput, ppthlvr.promotorinput)		as promotor
	, nvl(ctx.ctx_time_stamp, '01/01/1999')		as fechaemision
	, extract(year from nvl(ctx.ctx_time_stamp, '01/01/1999')) || lpad(extract(month from nvl(ctx.ctx_time_stamp, '01/01/1999')), 2, 0)		as periodo_emision
	, ctx.plcdco_initialdate		as fecha_incial_pol
	, ctx.plcdco_finishdate		as fecha_final_pol
	, prgrmnt.tipoproduccioninput		as tipo_produccion
	, prgrmnt.unidadnegocioinput		as unidad_negocio
	, case when ctx.ctx_status = '1' then 'POLIZA_PENDIENTE' when ctx.ctx_status = '2' then 'POLIZA_EMITIDA' else '' || ctx.ctx_status || '' end		as estado_poliza
	, coalesce(pprctsgr.indorigeninput, pccprsnls.indorigeninput, ppthlvr.indorigeninput, pprtccnsld.indorigeninput, pprtccnvd.indorigeninput)		as origen_dato
	, ctx.plcdco_operationpk		as nro_operacion
	, prgrmnt.lineacreditoinput		as linea_credito
	, nvl(vnttp.description, ctx.plcdco_event)		as descripcion_evento
	, coalesce(pprctsgr.tiponegociogasinput, ppthlvr.tiponegociogasinput)		as tipo_negocio
	, ctx.prplc_monedainput		as moneda_acuerdo
	, ctx.rskntdco_agregatedobjectid		as unidad_riesgo
	, coalesce(rvddrs.numerocreditoinput, rprctsgr.numerocreditoinput, rslvfctr.numerocreditoinput, rpthlvr.numerocreditoinput, rprtccnsld.numerocreditoinput, rprtccnvd.numerocreditoinput)		as numero_credito
	, coalesce(rprctsgr.numeropolizagasinput, rslvfctr.numeropolizagasinput, rpthlvr.numeropolizagasinput)		as numero_pol_gas
	, coalesce(rprctsgr.numerocontratoinput, rslvfctr.numerocontratoinput, rpthlvr.numerocontratoinput)		as numero_contrato
	, rprctsgr.plangasinput		as plan_gasera
	, coalesce(rprctsgr.descripcionproductoinput, rslvfctr.descripcionproductoinput, rpthlvr.descripcionproductoinput)		as descrip_producto
	, coalesce(rprctsgr.descripcionventainput, rslvfctr.descripcionventainput, rpthlvr.descripcionventainput)		as descrip_venta
	, coalesce(rprctsgr.codventainput, rslvfctr.codventainput, rpthlvr.codventainput)		as codigo_venta
	, coalesce(rprctsgr.dptosmartflexinput, rslvfctr.dptosmartflexinput, rpthlvr.dptosmartflexinput)		as dpto_smartflex
	, coalesce(rprctsgr.localidadsmartflexinput, rslvfctr.localidadsmartflexinput, rpthlvr.localidadsmartflexinput)		as localida_smartflex
	, nvl(rprctsgr.codigomesgasinput, 0)		as codigo_mes_gas
	, ctx.rskntdco_initialdate		as fecha_inicial_ur
	, ctx.rskntdco_finishdate		as fecha_final_ur
	, ctx.plcdco_operationpk		as nro_operacion_pol
	, coalesce(oprctsgr.tipoaseguradogasinput, oprctsgrpls.tipoaseguradogasinput, oftrprtgd.tipoaseguradogasinput, ovdp.asiscomercialinput, oslvfctr.tipoaseguradogasinput, opthlvr.tipoaseguradogasinput, oxqstrdcnl.tipoaseguradogasinput)		as tipo_asegurado
	, coalesce(oprctsgr.montoaseguradooavalue, oprctsgrpls.montoaseguradooavalue, ovdp.sumaasegcobdosvalue, oslvfctr.montoaseguradomavalue, opthlvr.montoaseguradomavalue, rxqstrdcnl.montoaseguradomavalue)		as monto_asegurado
	, coalesce(rxqstrdcnl.montoaseguradorepatvalue, '0')		as vlr_asegurado_aux_repatriacion
	, coalesce(oprctsgr.documentoidentidadaseginput, oprctsgrpls.documentoidentidadaseginput, oftrprtgd.documentoidentidadaseginput, oslvfctr.documentoidentidadaseginput, opthlvr.documentoidentidadaseginput, oxqstrdcnl.documentoidentidadaseginput)		as identif_asegurado
	, coalesce(oprctsgr.fechanacimientoaseginput, oprctsgrpls.fechanacimientoaseginput, oftrprtgd.fechanacimientoaseginput, oslvfctr.fechanacimientoaseginput, opthlvr.fechanacimientoaseginput, oxqstrdcnl.fechanacimientoaseginput, tom.tom_av_fechanacimiento)		as fecha_nacer_aseg
	, spo.spo_nombre		as sponsor
	, tom.tom_av_sexo		as genero
	, (case when nvl(vnttp.description, ctx.plcdco_event) <> 'Cancelar' then '' else cnclrvdp.motivocancelacionvidaapinput end)		as motivocancelacion
	, ovdp.hijosinput		as hijos
	, ovdp.estadocivilinput		as estado_civil
	, pvdp.prodkeyinput		as campana
	, pvdp.prodkeyvalue		as cod_campana
	, pvdp.prodkeynominput		as nombre_campana
	, pvdp.prodkeynomvalue		as cod_nombre_campana
	, age.age_nombre		as agente
	, coalesce(oprctsgr.valorsegurooavalue, oprctsgrpls.valorsegurooavalue, opthlvr.valorsegurooavalue)		as prima_anual
	, coalesce(oprctsgr.primamensualvalue, oprctsgrpls.primamensualvalue, oslvfctr.valorsegurooavalue, opthlvr.primamensualvalue)		as prima_mensual
	, vtr.vtr_ivacomision		as iva_comis_age
	, vtr.vtr_reteica		as reteica_agente
	, vtr.vtr_comisionpersjur		as retefte_agente
	, vtr.vtr_reteivacomision		as retiva_agente
	, efp.efp_description		as estado_final_pol
	, coalesce(cnclrvdp.fechamovimientoinput, cnclrpys.fechamovimientoinput, nlr.fechamovimientoinput)		as fecha_mov_cancelanul
	, coalesce(cnclrvdp.fechaefectivainput, cnclrpys.fechaefectivainput, nlr.fechaefectivainput)		as fecha_efectiva_cancelanu
	, coalesce(cnclrvdp.motivocancelacionvidaapinput, cnclrpys.motivoanulacioninput, nlr.motivoanulacioninput)		as motivo_cancelanul
	, agb.agb_porc_comis_agente		as porc_comis_agente
	, agb.agb_porc_part_agente		as porc_part_agente
	, coalesce(oprctsgr.sexoinput, oprctsgrpls.sexoinput, opthlvr.sexoinput)		as genero_asegurado
	, coalesce(oprctsgr.departamentoinput, oprctsgrpls.departamentoinput, opthlvr.departamentoinput)		as dpto_asegurado
	, coalesce(oprctsgr.ciudadinput, oprctsgrpls.ciudadinput, opthlvr.ciudadinput)		as ciudad_asegurado
	, coalesce((substr(oprctsgr.departamentovalue, 1, length(oprctsgr.departamentovalue) - 2) || lpad(replace(oprctsgr.ciudadvalue, '.0', ''), 3, '0')),
			 (substr(oprctsgrpls.departamentovalue, 1, length(oprctsgrpls.departamentovalue) - 2) || lpad(replace(oprctsgrpls.ciudadvalue, '.0', ''), 3, '0')),
			 (substr(opthlvr.departamentovalue, 1, length(opthlvr.departamentovalue) - 2) || lpad(replace(opthlvr.ciudadvalue, '.0', ''), 3, '0')))		as divipol_asegurado
	, coalesce(opthlvr.tipomascotainput, oxqstrdcnl.tipomascotainput)		as tipo_mascota
	, opthlvr.generomascotainput		as genero_mascota
	, opthlvr.edadmascotainput		as edad_mascota
	, opthlvr.razamascotainput		as raza_mascota
	, coalesce(opthlvr.nombremascotainput, oxqstrdcnl.nombremascotainput)		as nombre_mascota
	, oxqstrdcnl.tipomascotadosinput		as tipo_mascota_dos_exequias
	, oxqstrdcnl.nombremascota2input		as nombre_mascota_dos_exequias
	, oaa.oaa_email		as email_asegurado
	, oaa.oaa_telefono		as tel_asegurado
	, coalesce(pprtccnsld.nombremovimientoinput, pprtccnvd.nombremovimientoinput)		as desc_movimiento
	, coalesce(pprtccnsld.llaveinput, pprtccnvd.llaveinput)		as llave_integral_prod
	, coalesce(pprtccnsld.vigenciatecnicainput, pprtccnvd.vigenciatecnicainput)		as vigencia_tecnica
	, coalesce(rprtccnsld.porccomisrecaudsalvalue, rprtccnvd.porccomisrecaudvgrvalue)		as porc_comis_recaudo
	, coalesce(rprtccnsld.porccomisionreasalvalue, rprtccnvd.porccomisionreavgrinput)		as porc_comis_reaseguro
	, coalesce(to_number(replace(rprtccnsld.valorcomisionreasegsalvalue, '.', ',')), to_number(replace(rprtccnvd.valorcomisionreasegvgrvalue, '.', ',')))		as valor_comision_reaseguro
	, to_number(replace(rprtccnvd.valorcomisionreasegexeqvalue, '.', ','))		as valor_comision_reaseguro_exe
	, to_number(replace(rprtccnvd.valorcomisionreasegapvalue, '.', ','))		as valor_comision_reaseguro_ap
	, coalesce(oprctsgr.direccionaseguradoinput, oprctsgrpls.direccionaseguradoinput, oftrprtgd.direccionaseguradoinput, oslvfctr.direccionaseguradoinput, opthlvr.direccionaseguradoinput, oxqstrdcnl.direccionaseguradoinput)		as dire_asegurado
	, pvi.pvi_initialdate		as primera_vig_dsd_pol
	, ctx.plcdco_operationpk		as key_operationpk
	, ctx.plcdco_agregatedobjectid		as key_policyid
from acsele_alfa_bi.em_st_6 ctx
left join acsele_alfa.eventtype vnttp
on ctx.vntdco_eventtypeid = vnttp.eventtypeid
left join acsele_alfa.cancelarvidaap cnclrvdp
on ctx.vntdco_iddcoevent = cnclrvdp.pk
left join acsele_alfa.cancelarpys cnclrpys
on ctx.vntdco_iddcoevent = cnclrpys.pk
left join acsele_alfa.anular nlr
on ctx.vntdco_iddcoevent = nlr.pk
left join acsele_alfa.popractiseguro pprctsgr
on ctx.plcdco_agregatedobjectid = pprctsgr.static		-- cruza por static: puede multiplicar filas igual que la original
left join acsele_alfa.poaccpersonales pccprsnls
on ctx.plcdco_dcoid = pccprsnls.pk
left join acsele_alfa.povidaap pvdp
on ctx.plcdco_dcoid = pvdp.pk
left join acsele_alfa.popetlover ppthlvr
on ctx.plcdco_dcoid = ppthlvr.pk
left join acsele_alfa.poproteccionsalud pprtccnsld
on ctx.prplc_pk = pprtccnsld.pk
left join acsele_alfa.poproteccionvida pprtccnvd
on ctx.prplc_pk = pprtccnvd.pk
left join acsele_alfa.preagreement prgrmnt
on ctx.staggrmntvrsn_dcoid = prgrmnt.pk
left join acsele_alfa.urvdeudores rvddrs
on ctx.rskntdco_dcoid = rvddrs.pk
left join acsele_alfa.urpractiseguro rprctsgr
on ctx.rskntdco_dcoid = rprctsgr.pk
left join acsele_alfa.ursalvafactura rslvfctr
on ctx.rskntdco_dcoid = rslvfctr.pk
left join acsele_alfa.urpetlover rpthlvr
on ctx.rskntdco_dcoid = rpthlvr.pk
left join acsele_alfa.urexequiastradicional rxqstrdcnl
on ctx.rskntdco_dcoid = rxqstrdcnl.pk
left join acsele_alfa.urproteccionsalud rprtccnsld
on ctx.rskntdco_dcoid = rprtccnsld.pk
left join acsele_alfa.urproteccionvida rprtccnvd
on ctx.rskntdco_dcoid = rprtccnvd.pk
left join acsele_alfa.oapractiseguro oprctsgr
on ctx.nsrncbjctdco_dcoid = oprctsgr.pk
and oprctsgr.tipoaseguradogasvalue = '1.0'
left join acsele_alfa.oapractiseguroplus oprctsgrpls
on ctx.nsrncbjctdco_dcoid = oprctsgrpls.pk
and oprctsgrpls.tipoaseguradogasvalue = '1.0'
left join acsele_alfa.oafuturoprotegido oftrprtgd
on ctx.nsrncbjctdco_dcoid = oftrprtgd.pk
and oftrprtgd.tipoaseguradogasvalue = '1.0'
left join acsele_alfa.oavidaap ovdp
on ctx.nsrncbjctdco_dcoid = ovdp.pk
left join acsele_alfa.oasalvafactura oslvfctr
on ctx.nsrncbjctdco_dcoid = oslvfctr.pk
and oslvfctr.tipoaseguradogasvalue = '1.0'
left join acsele_alfa.oapetlover opthlvr
on ctx.nsrncbjctdco_dcoid = opthlvr.pk
and opthlvr.tipoaseguradogasvalue = '1.0'
left join acsele_alfa.oaexequiastradicional oxqstrdcnl
on ctx.nsrncbjctdco_dcoid = oxqstrdcnl.pk
left join acsele_alfa_bi.em_st_tom tom
on ctx.grgtdplc_agregatedpolicyid = tom.grgtdplc_agregatedpolicyid
left join acsele_alfa_bi.em_st_pgm pgm
on ctx.grgtdplc_agregatedpolicyid = pgm.grgtdplc_agregatedpolicyid
left join acsele_alfa_bi.em_st_spo spo
on ctx.grgtdplc_agregatedpolicyid = spo.grgtdplc_agregatedpolicyid
left join acsele_alfa_bi.em_st_age age
on ctx.grgtdplc_agregatedpolicyid = age.grgtdplc_agregatedpolicyid
left join acsele_alfa_bi.em_st_tdr tdr
on ctx.grgtdplc_agregatedpolicyid = tdr.grgtdplc_agregatedpolicyid
left join acsele_alfa_bi.em_st_efp efp
on ctx.grgtdplc_agregatedpolicyid = efp.grgtdplc_agregatedpolicyid
left join acsele_alfa_bi.em_st_pvi pvi
on ctx.grgtdplc_agregatedpolicyid = pvi.grgtdplc_agregatedpolicyid
left join acsele_alfa_bi.em_st_tpr tpr
on ctx.plcdco_operationpk = tpr.plcdco_operationpk
left join acsele_alfa_bi.em_st_oaa oaa
on ctx.plcdco_operationpk = oaa.plcdco_operationpk
left join acsele_alfa_bi.em_st_cor cor
on ctx.plcdco_operationpk = cor.plcdco_operationpk
left join acsele_alfa_bi.em_st_vtr vtr
on ctx.plcdco_operationpk = vtr.plcdco_operationpk
and ctx.plcdco_agregatedobjectid = vtr.plcdco_agregatedobjectid
left join acsele_alfa_bi.em_st_agb agb
on ctx.plcdco_operationpk = agb.plcdco_operationpk
and ctx.plcdco_agregatedobjectid = agb.plcdco_agregatedobjectid
;
commit;

create index acsele_alfa_bi.em_st_7_idx_1
on acsele_alfa_bi.em_st_7 (key_operationpk, key_policyid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_st_7
;


/* ============================================================================================ */
/* SATÉLITE · PASO 9 - FILTRO DE TAGS: los 130 conceptos de las metricas + descproperty de      */
/*    exequias                                                                                  */
/*                                                                                              */
/* Toma la base común de movimientos em_tmb y la restringe a las operaciones de Satélite con    */
/*    el mismo cruce de su armado (operationpk + policyid); luego aplica sus conceptos.         */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_st_9;
commit;
*/

create table acsele_alfa_bi.em_st_9
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  tm.thrdprtmvmntplc_operationpk
	, tm.thrdprtmvmntplc_policyid
	, tm.thrdprtmvmntplc_concept
	, tm.thrdprtmvmntplc_concept_u
	, tm.thrdprtmvmntplc_descproperty
	, tm.thrdprtmvmntplc_ammount
from acsele_alfa_bi.em_tmb tm
inner join acsele_alfa_bi.em_st_opr opr
on tm.thrdprtmvmntplc_operationpk = opr.plcdco_operationpk
and tm.thrdprtmvmntplc_policyid = opr.plcdco_agregatedobjectid
where (tm.thrdprtmvmntplc_concept_u in ('PRIMA', 'PRIMAREHAB', 'PRIMAANU', 'PRIMADEV', 'PRIMAPERIODOPROVIS',
									'PRIMAEMITIDAIND', 'PRIMAREHABIND', 'PRIMAANUIND', 'PRIMADEVUELTAIND', 'PRIMAPERIODOPROVISIND',
									'PRIMAEMITIDAPAGVIDAAP', 'PRIMAEMITIDAVIDAAP', 'PRIMAEMITIDAPAGDEVVIDAAP', 'PRIMAEMITIDAANUVIDAAP', 'PRIMAEMITIDADEVVIDAAP',
									'PRIMAEMITIDAREHABVIDAAP', 'PRIMAVIDA', 'PRIMAVIDADEV', 'PRIMAVIDAPROVISDEV', 'PRIMAVIDAPROVIS',
									'PRIMAEXEQ', 'PRIMAEXEQDEV', 'PRIMAVGR', 'PRIMAVGRDEV', 'PRIMAACC',
									'PRIMAACCDEV', 'COMISIONAGENTE', 'COMISIONAGENTEDEV', 'COMISIONVIDA', 'COMISIONVIDADEV',
									'COMISIONVIDAINDDEV', 'COMISIONVIDAIND', 'COMISIONPAGVIDAAP', 'COMISIONPAGVIDAAPDEV', 'COMISIONVIDAAP',
									'COMISIONVIDAAPDEV', 'COSTORECAUDACION', 'COSTORECAUDACIONDEV', 'COSTORECAUDOPROVIS', 'COSTORECAUDOREHAB',
									'COSTORECAUDOANU', 'COSTORECAUDACIONIND', 'COSTORECAUDACIONDEVIND', 'COSTORECAUDOPROVISIND', 'COSTORECAUDOREHABIND',
									'COSTORECAUDOANUIND', 'COSTORECAUDO', 'COSTORECAUDACIONANUVIDAAP', 'COSTORECAUDACIONDEVVIDAAP', 'COSTORECAUDACIONPAGVIDAAP',
									'COSTORECAUDACIONREHABVIDAAP', 'COSTORECAUDACIONVIDAAP', 'COMISIONRECAUDOVIDA', 'COMISIONRECAUDOVIDADEV', 'COMISIONRECAUDOVIDAPROVISDEV',
									'COMISIONRECAUDOVIDAPROVIS', 'COMISIONRECAUDOEXEQ', 'COMISIONRECAUDOEXEQDEV', 'COMISIONRECAUDOVGR', 'COMISIONRECAUDOVGRDEV',
									'COMISIONRECAUDOACC', 'COMISIONRECAUDOACCDEV', 'IVACOSTORECAUDACION', 'IVACOSTORECAUDACIONDEV', 'IVACOSTORECAUDOPROVIS',
									'IVACOSTORECAUDOREHAB', 'IVACOSTORECAUDOANU', 'IVACOSTORECAUDACIONIND', 'IVACOSTORECAUDACIONDEVIND', 'IVACOSTORECAUDOPROVISIND',
									'IVACOSTORECAUDOREHABIND', 'IVACOSTORECAUDOANUIND', 'IVACOSTORECAUDO', 'IVACOSTORECAUDACIONVIDAAP', 'IVACOSTORECAUDACIONPAGVIDAAP',
									'IVACOSTORECAUDACIONDEVVIDAAP', 'IVACOSTORECAUDACIONANUVIDAAP', 'IVACOSTORECAUDACIONREHABVIDAAP', 'IVACOMISIONRECAUDOVIDA', 'IVACOMISIONRECAUDOVIDADEV',
									'IVACOMISIONRECAUDOVIDAPROVISDEV', 'IVACOMISIONRECAUDOVIDAPROVIS', 'IVACOMISIONRECAUDOEXEQ', 'IVACOMISIONRECAUDOEXEQDEV', 'IVACOMISIONRECAUDOVGR',
									'IVACOMISIONRECAUDOVGRDEV', 'IVACOMISIONRECAUDOACC', 'IVACOMISIONRECAUDOACCDEV', 'GASTOSEXPEDICION', 'GASTOSEXPEDICIONDEV',
									'SUMAASEGURADAPERIODO', 'SUMAASEGPERIODO', 'TOTALSUMAASEGMUERTE', 'IVAPRIMAVIDA', 'IVAPRIMAVIDADEV',
									'IVAPRIMAVIDAPROVISDEV', 'IVAPRIMAVIDAPROVIS', 'IVAPRIMAEXEQ', 'IVAPRIMAEXEQDEV', 'IVAPRIMAVGR',
									'IVAPRIMAVGRDEV', 'COMISIONNETAALFAVIDA', 'COMISIONNETAALFAVIDADEV', 'COMISIONNETAALFAVIDAPROVISDEV', 'COMISIONNETAALFAVIDAPROVIS',
									'COMISIONNETAALFAVGR', 'COMISIONNETAALFAVGRDEV', 'COMISIONNETAALFAACC', 'COMISIONNETAALFAACCDEV', 'COMISIONNETAALFAEXEQ',
									'COMISIONNETAALFAEXEQDEV', 'COMISIONREASEGUROSVIDA', 'COMISIONREASEGUROSVIDADEV', 'COMISIONREASEGUROSVIDAPROVISDEV', 'COMISIONREASEGUROSVIDAPROVIS',
									'NETOREASEGUROSEXEQ', 'NETOREASEGUROSEXEQDEV', 'NETOREASEGUROSVGR', 'NETOREASEGUROSVGRDEV', 'NETOREASEGUROSACC',
									'NETOREASEGUROSACCDEV', 'NETOREASEGUROSVIDA', 'NETOREASEGUROSVIDADEV', 'COMISIONTARJETAVIDA', 'COMISIONTARJETAVIDADEV',
									'COMISIONTARJETAVIDAPROVIS', 'COMISIONTARJETAEXEQ', 'COMISIONTARJETAEXEQDEV', 'COMISIONTARJETAVGR', 'COMISIONTARJETAVGRDEV')
or tm.thrdprtmvmntplc_descproperty in ('TotalSumaAsegAuxRepat', 'TotalSumaAsegMuerte'))
;
commit;

create index acsele_alfa_bi.em_st_9_idx_1
on acsele_alfa_bi.em_st_9 (thrdprtmvmntplc_operationpk, thrdprtmvmntplc_policyid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_st_9
;


/* ============================================================================================ */
/* SATÉLITE · PASO 10 - METRICAS: las 12 metricas de movimientos en una sola lectura, group by  */
/*    operacion                                                                                 */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_st_10;
commit;
*/

create table acsele_alfa_bi.em_st_10
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  tm.thrdprtmvmntplc_operationpk
	, tm.thrdprtmvmntplc_policyid
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('PRIMA', 'PRIMAREHAB', 'PRIMAANU', 'PRIMADEV', 'PRIMAPERIODOPROVIS',
					'PRIMAEMITIDAIND', 'PRIMAREHABIND', 'PRIMAANUIND', 'PRIMADEVUELTAIND', 'PRIMAPERIODOPROVISIND',
					'PRIMAEMITIDAPAGVIDAAP', 'PRIMAEMITIDAVIDAAP', 'PRIMAEMITIDAPAGDEVVIDAAP', 'PRIMAEMITIDAANUVIDAAP', 'PRIMAEMITIDADEVVIDAAP',
					'PRIMAEMITIDAREHABVIDAAP', 'PRIMAVIDA', 'PRIMAVIDADEV', 'PRIMAVIDAPROVISDEV', 'PRIMAVIDAPROVIS',
					'PRIMAEXEQ', 'PRIMAEXEQDEV', 'PRIMAVGR', 'PRIMAVGRDEV', 'PRIMAACC',
					'PRIMAACCDEV')
				and tm.thrdprtmvmntplc_ammount <> 0
				then case when tm.thrdprtmvmntplc_concept_u in ('PRIMAANU', 'PRIMAANUIND', 'PRIMAEMITIDADEVVIDAAP', 'PRIMAVIDADEV', 'PRIMADEV',
						'PRIMADEVUELTAIND', 'PRIMAEMITIDAPAGDEVVIDAAP', 'PRIMAEMITIDAANUVIDAAP', 'PRIMAVIDAPROVISDEV', 'PRIMAEXEQDEV',
						'PRIMAVGRDEV', 'PRIMAACCDEV')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_prima
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('COMISIONAGENTE', 'COMISIONAGENTEDEV', 'COMISIONVIDA', 'COMISIONVIDADEV', 'COMISIONVIDAINDDEV',
					'COMISIONVIDAIND', 'COMISIONPAGVIDAAP', 'COMISIONPAGVIDAAPDEV', 'COMISIONVIDAAP', 'COMISIONVIDAAPDEV')
				and tm.thrdprtmvmntplc_ammount <> 0
				then case when tm.thrdprtmvmntplc_concept_u in ('COMISIONAGENTEDEV', 'COMISIONVIDAINDDEV', 'COMISIONVIDADEV', 'COMISIONPAGVIDAAPDEV', 'COMISIONVIDAAPDEV')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_comision_agente
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('COSTORECAUDACION', 'COSTORECAUDACIONDEV', 'COSTORECAUDOPROVIS', 'COSTORECAUDOREHAB', 'COSTORECAUDOANU',
					'COSTORECAUDACIONIND', 'COSTORECAUDACIONDEVIND', 'COSTORECAUDOPROVISIND', 'COSTORECAUDOREHABIND', 'COSTORECAUDOANUIND',
					'COSTORECAUDO', 'COSTORECAUDACIONANUVIDAAP', 'COSTORECAUDACIONDEVVIDAAP', 'COSTORECAUDACIONPAGVIDAAP', 'COSTORECAUDACIONREHABVIDAAP',
					'COSTORECAUDACIONVIDAAP', 'COMISIONRECAUDOVIDA', 'COMISIONRECAUDOVIDADEV', 'COMISIONRECAUDOVIDAPROVISDEV', 'COMISIONRECAUDOVIDAPROVIS',
					'COMISIONRECAUDOEXEQ', 'COMISIONRECAUDOEXEQDEV', 'COMISIONRECAUDOVGR', 'COMISIONRECAUDOVGRDEV', 'COMISIONRECAUDOACC',
					'COMISIONRECAUDOACCDEV')
				and tm.thrdprtmvmntplc_ammount <> 0
				then case when tm.thrdprtmvmntplc_concept_u in ('COSTORECAUDACIONDEV', 'COSTORECAUDACIONDEVIND', 'COSTORECAUDACIONDEVVIDAAP', 'COSTORECAUDOANU', 'COSTORECAUDOANUIND',
						'COSTORECAUDACIONANUVIDAAP', 'COMISIONRECAUDOVIDADEV', 'COMISIONRECAUDOVIDAPROVISDEV', 'COMISIONRECAUDOEXEQDEV', 'COMISIONRECAUDOVGRDEV',
						'COMISIONRECAUDOACCDEV')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_costo_recaudacion
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('IVACOSTORECAUDACION', 'IVACOSTORECAUDACIONDEV', 'IVACOSTORECAUDOPROVIS', 'IVACOSTORECAUDOREHAB', 'IVACOSTORECAUDOANU',
					'IVACOSTORECAUDACIONIND', 'IVACOSTORECAUDACIONDEVIND', 'IVACOSTORECAUDOPROVISIND', 'IVACOSTORECAUDOREHABIND', 'IVACOSTORECAUDOANUIND',
					'IVACOSTORECAUDO', 'IVACOSTORECAUDACIONVIDAAP', 'IVACOSTORECAUDACIONPAGVIDAAP', 'IVACOSTORECAUDACIONDEVVIDAAP', 'IVACOSTORECAUDACIONANUVIDAAP',
					'IVACOSTORECAUDACIONREHABVIDAAP', 'IVACOMISIONRECAUDOVIDA', 'IVACOMISIONRECAUDOVIDADEV', 'IVACOMISIONRECAUDOVIDAPROVISDEV', 'IVACOMISIONRECAUDOVIDAPROVIS',
					'IVACOMISIONRECAUDOEXEQ', 'IVACOMISIONRECAUDOEXEQDEV', 'IVACOMISIONRECAUDOVGR', 'IVACOMISIONRECAUDOVGRDEV', 'IVACOMISIONRECAUDOACC',
					'IVACOMISIONRECAUDOACCDEV')
				and tm.thrdprtmvmntplc_ammount <> 0
				then case when tm.thrdprtmvmntplc_concept_u in ('IVACOSTORECAUDACIONDEV', 'IVACOSTORECAUDACIONDEVIND', 'IVACOSTORECAUDACIONDEVVIDAAP', 'IVACOSTORECAUDOANU', 'IVACOSTORECAUDOANUIND',
						'IVACOSTORECAUDACIONANUVIDAAP', 'IVACOMISIONRECAUDOVIDADEV', 'IVACOMISIONRECAUDOVIDAPROVISDEV', 'IVACOMISIONRECAUDOEXEQDEV', 'IVACOMISIONRECAUDOVGRDEV',
						'IVACOMISIONRECAUDOACCDEV')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_iva_costo_recaudacion
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('GASTOSEXPEDICION', 'GASTOSEXPEDICIONDEV')
				and tm.thrdprtmvmntplc_ammount <> 0
				then case when tm.thrdprtmvmntplc_concept_u in ('GASTOSEXPEDICIONDEV')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_gastos_expedicion
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('SUMAASEGURADAPERIODO', 'SUMAASEGPERIODO', 'TOTALSUMAASEGMUERTE')
				and tm.thrdprtmvmntplc_ammount <> 0
				and tm.thrdprtmvmntplc_concept like '%Aseg%'
				then nvl(tm.thrdprtmvmntplc_ammount, 0) end)	as m_suma_asegurada_periodo
	, sum(case when tm.thrdprtmvmntplc_descproperty in ('TotalSumaAsegAuxRepat') then nvl(tm.thrdprtmvmntplc_ammount, 0) end)	as m_suma_aseg_exeq_aux_repatria
	, sum(case when tm.thrdprtmvmntplc_descproperty in ('TotalSumaAsegMuerte') then nvl(tm.thrdprtmvmntplc_ammount, 0) end)	as m_suma_aseg_exeq_muerte
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('IVAPRIMAVIDA', 'IVAPRIMAVIDADEV', 'IVAPRIMAVIDAPROVISDEV', 'IVAPRIMAVIDAPROVIS', 'IVAPRIMAEXEQ',
					'IVAPRIMAEXEQDEV', 'IVAPRIMAVGR', 'IVAPRIMAVGRDEV')
				and tm.thrdprtmvmntplc_ammount <> 0
				then case when tm.thrdprtmvmntplc_concept_u in ('IVAPRIMAVIDADEV', 'IVAPRIMAVIDAPROVISDEV', 'IVAPRIMAEXEQDEV', 'IVAPRIMAVGRDEV')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_iva_prima
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('COMISIONNETAALFAVIDA', 'COMISIONNETAALFAVIDADEV', 'COMISIONNETAALFAVIDAPROVISDEV', 'COMISIONNETAALFAVIDAPROVIS', 'COMISIONNETAALFAVGR',
					'COMISIONNETAALFAVGRDEV', 'COMISIONNETAALFAACC', 'COMISIONNETAALFAACCDEV', 'COMISIONNETAALFAEXEQ', 'COMISIONNETAALFAEXEQDEV')
				and tm.thrdprtmvmntplc_ammount <> 0
				then case when tm.thrdprtmvmntplc_concept_u in ('COMISIONNETAALFAVIDADEV', 'COMISIONNETAALFAVIDAPROVISDEV', 'COMISIONNETAALFAVGRDEV', 'COMISIONNETAALFAEXEQDEV', 'COMISIONNETAALFAACCDEV')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_comision_neta_alfa
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('COMISIONREASEGUROSVIDA', 'COMISIONREASEGUROSVIDADEV', 'COMISIONREASEGUROSVIDAPROVISDEV', 'COMISIONREASEGUROSVIDAPROVIS', 'NETOREASEGUROSEXEQ',
					'NETOREASEGUROSEXEQDEV', 'NETOREASEGUROSVGR', 'NETOREASEGUROSVGRDEV', 'NETOREASEGUROSACC', 'NETOREASEGUROSACCDEV',
					'NETOREASEGUROSVIDA', 'NETOREASEGUROSVIDADEV')
				and tm.thrdprtmvmntplc_ammount <> 0
				then case when tm.thrdprtmvmntplc_concept_u in ('COMISIONREASEGUROSVIDADEV', 'COMISIONREASEGUROSVIDAPROVISDEV', 'NETOREASEGUROSEXEQDEV', 'NETOREASEGUROSVGRDEV', 'NETOREASEGUROSACCDEV',
						'NETOREASEGUROSVIDADEV')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_valor_neto_reaseguro
	, sum(case when tm.thrdprtmvmntplc_concept_u in ('COMISIONTARJETAVIDA', 'COMISIONTARJETAVIDADEV', 'COMISIONTARJETAVIDAPROVIS', 'COMISIONTARJETAEXEQ', 'COMISIONTARJETAEXEQDEV',
					'COMISIONTARJETAVGR', 'COMISIONTARJETAVGRDEV')
				and tm.thrdprtmvmntplc_ammount <> 0
				then case when tm.thrdprtmvmntplc_concept_u in ('COMISIONTARJETAVIDADEV', 'COMISIONTARJETAEXEQDEV', 'COMISIONTARJETAVGRDEV')
					and nvl(tm.thrdprtmvmntplc_ammount, 0) > 0 then nvl(tm.thrdprtmvmntplc_ammount, 0) * (-1)
					else nvl(tm.thrdprtmvmntplc_ammount, 0) end end)	as m_comision_tarjetacr
from acsele_alfa_bi.em_st_9 tm
group by tm.thrdprtmvmntplc_operationpk
	, tm.thrdprtmvmntplc_policyid
;
commit;

create index acsele_alfa_bi.em_st_10_idx_1
on acsele_alfa_bi.em_st_10 (thrdprtmvmntplc_operationpk, thrdprtmvmntplc_policyid);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.em_st_10
;



/* ============================================================================================ */
/* ##############################  CONSOLIDADO  ##############################                  */
/* ============================================================================================ */

/* ============================================================================================ */
/* CONSOLIDADO - EMISIONES: UNION ALL de las salidas de Generales, Satélite y Vida con          */
/*    columnas homologadas (202 columnas)                                                       */
/*                                                                                              */
/* Cada bloque es exactamente la salida final de su negocio; solo cambia el nombre de algunas   */
/*    columnas (homologación) y se rellena con null lo que no aplica.                           */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.emisiones;
commit;
*/

create table acsele_alfa_bi.emisiones_est
parallel 2
tablespace tsdat_acsele_alfa_rep
as
-- ============================== GENERALES ==============================
select /*+ parallel(3) */
	  cast('GENERALES' as varchar2(30))		as estandar_configuracion
	, ctx.idpoliza				as idpoliza
	, ctx.idproducto				as idproducto
	, ctx.codigo_compania				as codigo_compania
	, ctx.compania				as compania
	, ctx.codigo_producto				as codigo_producto
	, ctx.producto				as producto
	, ctx.codigo_ramo				as codigo_ramo
	, ctx.ramo				as ramo
	, ctx.codigo_sucursal				as codigo_sucursal
	, ctx.sucursal				as sucursal
	, ctx.identificacion_tomador				as identificacion_tomador
	, ctx.tomador				as tomador
	, ctx.departamento_tom				as departamento_tom
	, ctx.ciudad_tom				as ciudad_tom
	, ctx.direcion_tom				as direcion_tom
	, ctx.telefono_tomador				as telefono_tomador
	, ctx.email_tomador				as email_tomador
	, ctx.identifica_pagador_maestro				as identifica_pagador_maestro
	, ctx.informacion_pagador_maestro				as informacion_pagador_maestro
	, ctx.tipo_identif_asegurado				as tip_ident_asegurado
	, ctx.identif_asegurado				as ident_asegurado
	, ctx.asegurado				as asegurado
	, ctx.identificacion_empleador				as identificacion_empleador
	, ctx.nombre_empleador				as nombre_empleador
	, ctx.identificacion_pagador				as identificacion_pagador
	, ctx.pagador				as pagador
	, ctx.idacuerdo				as idacuerdo
	, ctx.fechaini_acuerdo				as fechaini_acuerdo
	, ctx.fechafin_acuerdo				as fechafin_acuerdo
	, ctx.numero_poliza_matriz				as numero_poliza_matriz
	, ctx.nombre_acuerdo				as nombre_acuerdo
	, ctx.estado_acuerdo				as estado_acuerdo
	, ctx.tipo_poliza				as tipo_poliza
	, ctx.nro_poliza				as nro_poliza
	, ctx.frecuencia_pago				as frecuencia_pago
	, ctx.fechaemision				as fechaemision
	, ctx.periodo_fechaemision				as periodo_emision
	, ctx.estado_poliza				as estado_poliza
	, ctx.fecha_incial_pol				as fecha_incial_pol
	, ctx.fecha_final_pol				as fecha_final_pol
	, ctx.tipo_produccion				as tipo_produccion
	, ctx.cod_unidad_negocio				as cod_unidad_negocio
	, ctx.unidad_negocio				as unidad_negocio
	, ctx.nro_operacion				as nro_operacion
	, ctx.tipo_inmueble				as tipo_inmueble
	, ctx.direccion_inmueble				as direccion_inmueble
	, ctx.cod_departamento_inmueble				as cod_departamento_inmueble
	, ctx.departamento_inmueble				as departamento_inmueble
	, ctx.cod_ciudad_inmueble				as cod_ciudad_inmueble
	, ctx.ciudad_inmueble				as ciudad_inmueble
	, ctx.cod_divipol_municipio				as cod_divipol_municipio
	, ctx.cod_zona_sismica				as cod_zona_sismica
	, ctx.tipo_estructura_bien				as tipo_estructura_bien
	, ctx.cod_tipo_estructura_bien				as cod_tipo_estructura_bien
	, ctx.raza_mascota				as raza_mascota
	, ctx.tipo_mascota				as tipo_mascota
	, ctx.edad_mascota				as edad_mascota
	, ctx.genero_mascota				as genero_mascota
	, ctx.detalle_mascota				as detalle_mascota
	, ctx.nombre_mascota				as nombre_mascota
	, ctx.linea_credito				as linea_credito
	, ctx.descripcion_evento				as descripcion_evento
	, ctx.moneda_acuerdo				as moneda_acuerdo
	, ctx.unidad_riesgo				as unidad_riesgo
	, ctx.numero_credito				as numero_credito
	, ctx.fecha_inicial_ur				as fecha_inicial_ur
	, ctx.fecha_final_ur				as fecha_final_ur
	, ctx.nro_operacion_pol				as nro_operacion_pol
	, ctx.origen_dato				as origen_dato
	, ctx.nro_operacion_factura				as nro_operacion_factura
	, ctx.fecha_inicial_proyecto				as fecha_inicial_proyecto
	, ctx.fecha_final_proyecto				as fecha_final_proyecto
	, ctx.periodo_gracia				as periodo_gracia
	, ctx.valor_anticipo				as valor_anticipo
	, ctx.nombre_edificio				as nombre_edificio
	, ctx.numero_pisos				as numero_pisos
	, ctx.ano_construcion_edificio				as ano_construcion_edificio
	, round(m.m_prima, 8)				as prima
	, round(m.m_prima_vida, 8)				as prima_vida
	, nvl(round(m.m_prima_incendio, 8), 0)				as prima_incendio
	, nvl(round(m.m_prima_terremoto, 8), 0)				as prima_terremoto
	, round(m.m_prima_respcivil, 8)				as prima_respcivil
	, nvl(round(m.m_costo_recaudacion, 8), 0)				as costo_recaudacion
	, nvl(round(m.m_costo_recaudacion_vida, 8), 0)				as costo_recaudacion_vida
	, nvl(round(m.m_costo_recaudacion_inc, 8), 0)				as costo_recaudacion_inc
	, nvl(round(m.m_costo_recaudacion_terr, 8), 0)				as costo_recaudacion_terr
	, nvl(round(m.m_costo_recaudacion_respcivil, 8), 0)				as costo_recaudacion_respcivil
	, nvl(round(m.m_iva_costo_recaudacion, 8), 0)				as iva_costo_recaudacion
	, nvl(round(m.m_iva_costo_recaudacion_vida, 8), 0)				as iva_costo_recaudacion_vida
	, nvl(round(m.m_comision_total_interincter, 8), 0)				as comision_total_interincter
	, nvl(round(m.m_comision_intermediario_inc, 8), 0)				as comision_intermediario_inc
	, nvl(round(m.m_comision_intermediario_terr, 8), 0)				as comision_intermediario_terr
	, nvl(round(m.m_iva_costo_recaudo_inc, 8), 0)				as iva_costo_recaudo_inc
	, nvl(round(m.m_iva_costo_recaudo_terr, 8), 0)				as iva_costo_recaudo_terr
	, nvl(round(m.m_iva_costo_recaudo_rescivil, 8), 0)				as iva_costo_recaudo_rescivil
	, nvl(round(m.m_iva_costo_recaudo_provinc, 8), 0)				as iva_costo_recaudo_provinc
	, nvl(round(m.m_iva_costo_recaudo_provterr, 8), 0)				as iva_costo_recaudo_provterr
	, round(m.m_iva_prima_total, 8)				as iva_prima_total
	, round(m.m_iva_prima_bancaseg, 8)				as iva_prima_bancaseg
	, round(m.m_iva_prima_incendio, 8)				as iva_prima_incendio
	, nvl(round(m.m_iva_prima_terremoto, 8), 0)				as iva_prima_terremoto
	, round(m.m_iva_prima_repcivil, 8)				as iva_prima_repcivil
	, nvl(round(m.m_iva_prima_provincendio, 8), 0)				as iva_prima_provincendio
	, nvl(round(m.m_iva_prima_provterremoto, 8), 0)				as iva_prima_provterremoto
	, nvl(round(m.m_iva_prima_provrespcivil, 8), 0)				as iva_prima_provrespcivil
	, nvl(round(m.m_suma_asegurada_periodo, 8), 0)				as suma_asegurada_periodo
	, round(m.m_contrib_bomberos_incendio, 8)				as contrib_bomberos_incendio
	, round(m.m_contrib_bomberos_terremoto, 8)				as contrib_bomberos_terremoto
	, round(m.m_contrib_bomberos_hogar, 8)				as contrib_bomberos_hogar
	, round(m.m_prima_asistencia, 8)				as prima_asistencia
	, round(m.m_prima_sustraccion, 8)				as prima_sustraccion
	, round(m.m_prima_responsabilidad, 8)				as prima_responsabilidad
	, round(m.m_iva_prima_asistencia, 8)				as iva_prima_asistencia
	, round(m.m_iva_prima_sustraccion, 8)				as iva_prima_sustraccion
	, round(m.m_iva_prima_responsabilidad, 8)				as iva_prima_responsabilidad
	, case when ctx.key_productid = 54979 then round(m.m_iva_prima_anulada_mascotas, 8) else 0 end				as iva_prima_anulada_mascotas
	, round(m.m_comision_neta_alfa, 8)				as comision_neta_alfa
	, round(m.m_comision_neta_vida, 8)				as comision_neta_vida
	, round(m.m_valor_neto_reaseguro, 8)				as valor_neto_reaseguro
	, round(m.m_valor_neto_reaseguro_vida, 8)				as valor_neto_reaseguro_vida
	, round(m.m_comision_tarjetacr, 8)				as comision_tarjetacr
	, round(m.m_comision_tarjetacr_vida, 8)				as comision_tarjetacr_vida
	, ctx.fecha_mov_cancelanul				as fecha_mov_cancelanul
	, ctx.fecha_efectiva_cancelanu				as fecha_efectiva_cancelanu
	, ctx.motivo_cancelanul				as motivo_cancelanul
	, ctx.llave_integral_prod				as llave_integral_prod
	, ctx.vigencia_tecnica				as vigencia_tecnica
	, ctx.porc_comis_recaudo				as porc_comis_recaudo
	, ctx.porc_comis_reaseguro				as porc_comis_reaseguro
	, ctx.porc_comis_reaseguro_vda				as porc_comis_reaseguro_vda
	, ctx.valor_comision_reaseguro				as valor_comision_reaseguro
	, ctx.valor_comision_reaseguro_vda				as valor_comision_reaseguro_vda
	, ctx.agente				as agente
	, null				as codigo_ramo_contable
	, null				as ramo_contable
	, null				as divipol_tom
	, null				as producto_del_acuerdo
	, null				as canal_venta
	, null				as promotor
	, null				as tipo_negocio
	, null				as numero_pol_gas
	, null				as numero_contrato
	, null				as plan_gasera
	, null				as descrip_producto
	, null				as descrip_venta
	, null				as codigo_venta
	, null				as dpto_smartflex
	, null				as localida_smartflex
	, null				as codigo_mes_gas
	, null				as tipo_asegurado
	, null				as monto_asegurado
	, null				as vlr_asegurado_aux_repatriacion
	, null				as doc_asegurado_objeto
	, null				as fecha_nacer_aseg
	, null				as sponsor
	, null				as genero_tomador
	, null				as motivocancelacion
	, null				as hijos
	, null				as estado_civil
	, null				as campana
	, null				as cod_campana
	, null				as nombre_campana
	, null				as cod_nombre_campana
	, null				as prima_anual
	, null				as prima_mensual
	, null				as comision_agente
	, null				as gastos_expedicion
	, null				as suma_aseg_exeq_aux_repatria
	, null				as suma_aseg_exeq_muerte
	, null				as iva_comis_age
	, null				as reteica_agente
	, null				as retefte_agente
	, null				as retiva_agente
	, null				as iva_prima
	, null				as estado_final_pol
	, null				as porc_comis_agente
	, null				as porc_part_agente
	, null				as genero_asegurado
	, null				as dpto_asegurado
	, null				as ciudad_asegurado
	, null				as divipol_asegurado
	, null				as tipo_mascota_dos_exequias
	, null				as nombre_mascota_dos_exequias
	, null				as email_asegurado
	, null				as tel_asegurado
	, null				as desc_movimiento
	, null				as valor_comision_reaseguro_exe
	, null				as valor_comision_reaseguro_ap
	, null				as dire_asegurado
	, null				as primera_vig_dsd_pol
	, null				as st_ast_id
	, null				as linea_credito_acuerdo
	, null				as fecha_desembolso
	, null				as rsa_policyid
	, null				as ri_operationpk
	, null				as agrv_dcoid
	, null				as liunc_pk
	, null				as edad_asegurado
	, null				as cod_dpto_asegurado
	, null				as cod_ciudad_asegurado
	, null				as factor_vida
from acsele_alfa_bi.em_gn_7 ctx
left join acsele_alfa_bi.em_gn_10 m
on ctx.key_operationpk = m.thrdprtmvmntplc_operationpk
and ctx.key_policyid = m.thrdprtmvmntplc_policyid
union all
-- ============================== SATELITE ==============================
select /*+ parallel(3) */
	  cast('SATELITE' as varchar2(30))		as estandar_configuracion
	, ctx.idpoliza				as idpoliza
	, ctx.idproducto				as idproducto
	, ctx.codigo_compania				as codigo_compania
	, ctx.compania				as compania
	, ctx.codigo_producto				as codigo_producto
	, ctx.producto				as producto
	, ctx.codigo_ramo				as codigo_ramo
	, ctx.ramo				as ramo
	, ctx.codigo_sucursal				as codigo_sucursal
	, ctx.sucursal				as sucursal
	, ctx.identificacion_tomador				as identificacion_tomador
	, ctx.tomador				as tomador
	, ctx.departamento_tom				as departamento_tom
	, ctx.ciudad_tom				as ciudad_tom
	, ctx.direcion_tom				as direcion_tom
	, ctx.telefono_tomador				as telefono_tomador
	, ctx.email_tomador				as email_tomador
	, null				as identifica_pagador_maestro
	, ctx.informacion_pagador_maestro				as informacion_pagador_maestro
	, ctx.tip_ident_asegurado				as tip_ident_asegurado
	, ctx.ident_asegurado				as ident_asegurado
	, ctx.asegurado				as asegurado
	, null				as identificacion_empleador
	, null				as nombre_empleador
	, null				as identificacion_pagador
	, null				as pagador
	, ctx.idacuerdo				as idacuerdo
	, ctx.fechaini_acuerdo				as fechaini_acuerdo
	, ctx.fechafin_acuerdo				as fechafin_acuerdo
	, ctx.numero_poliza_matriz				as numero_poliza_matriz
	, ctx.nombre_acuerdo				as nombre_acuerdo
	, ctx.estado_acuerdo				as estado_acuerdo
	, ctx.tipo_poliza				as tipo_poliza
	, ctx.nro_poliza				as nro_poliza
	, ctx.frecuencia_pago				as frecuencia_pago
	, ctx.fechaemision				as fechaemision
	, ctx.periodo_emision				as periodo_emision
	, ctx.estado_poliza				as estado_poliza
	, ctx.fecha_incial_pol				as fecha_incial_pol
	, ctx.fecha_final_pol				as fecha_final_pol
	, ctx.tipo_produccion				as tipo_produccion
	, null				as cod_unidad_negocio
	, ctx.unidad_negocio				as unidad_negocio
	, ctx.nro_operacion				as nro_operacion
	, null				as tipo_inmueble
	, null				as direccion_inmueble
	, null				as cod_departamento_inmueble
	, null				as departamento_inmueble
	, null				as cod_ciudad_inmueble
	, null				as ciudad_inmueble
	, null				as cod_divipol_municipio
	, null				as cod_zona_sismica
	, null				as tipo_estructura_bien
	, null				as cod_tipo_estructura_bien
	, ctx.raza_mascota				as raza_mascota
	, ctx.tipo_mascota				as tipo_mascota
	, ctx.edad_mascota				as edad_mascota
	, ctx.genero_mascota				as genero_mascota
	, null				as detalle_mascota
	, ctx.nombre_mascota				as nombre_mascota
	, ctx.linea_credito				as linea_credito
	, ctx.descripcion_evento				as descripcion_evento
	, ctx.moneda_acuerdo				as moneda_acuerdo
	, to_char(ctx.unidad_riesgo)				as unidad_riesgo
	, ctx.numero_credito				as numero_credito
	, ctx.fecha_inicial_ur				as fecha_inicial_ur
	, ctx.fecha_final_ur				as fecha_final_ur
	, ctx.nro_operacion_pol				as nro_operacion_pol
	, ctx.origen_dato				as origen_dato
	, null				as nro_operacion_factura
	, null				as fecha_inicial_proyecto
	, null				as fecha_final_proyecto
	, null				as periodo_gracia
	, null				as valor_anticipo
	, null				as nombre_edificio
	, null				as numero_pisos
	, null				as ano_construcion_edificio
	, round(m.m_prima, 8)				as prima
	, null				as prima_vida
	, null				as prima_incendio
	, null				as prima_terremoto
	, null				as prima_respcivil
	, round(m.m_costo_recaudacion, 8)				as costo_recaudacion
	, null				as costo_recaudacion_vida
	, null				as costo_recaudacion_inc
	, null				as costo_recaudacion_terr
	, null				as costo_recaudacion_respcivil
	, round(m.m_iva_costo_recaudacion, 8)				as iva_costo_recaudacion
	, null				as iva_costo_recaudacion_vida
	, null				as comision_total_interincter
	, null				as comision_intermediario_inc
	, null				as comision_intermediario_terr
	, null				as iva_costo_recaudo_inc
	, null				as iva_costo_recaudo_terr
	, null				as iva_costo_recaudo_rescivil
	, null				as iva_costo_recaudo_provinc
	, null				as iva_costo_recaudo_provterr
	, null				as iva_prima_total
	, null				as iva_prima_bancaseg
	, null				as iva_prima_incendio
	, null				as iva_prima_terremoto
	, null				as iva_prima_repcivil
	, null				as iva_prima_provincendio
	, null				as iva_prima_provterremoto
	, null				as iva_prima_provrespcivil
	, round(m.m_suma_asegurada_periodo, 8)				as suma_asegurada_periodo
	, null				as contrib_bomberos_incendio
	, null				as contrib_bomberos_terremoto
	, null				as contrib_bomberos_hogar
	, null				as prima_asistencia
	, null				as prima_sustraccion
	, null				as prima_responsabilidad
	, null				as iva_prima_asistencia
	, null				as iva_prima_sustraccion
	, null				as iva_prima_responsabilidad
	, null				as iva_prima_anulada_mascotas
	, round(m.m_comision_neta_alfa, 8)				as comision_neta_alfa
	, null				as comision_neta_vida
	, round(m.m_valor_neto_reaseguro, 8)				as valor_neto_reaseguro
	, null				as valor_neto_reaseguro_vida
	, round(m.m_comision_tarjetacr, 8)				as comision_tarjetacr
	, null				as comision_tarjetacr_vida
	, ctx.fecha_mov_cancelanul				as fecha_mov_cancelanul
	, ctx.fecha_efectiva_cancelanu				as fecha_efectiva_cancelanu
	, ctx.motivo_cancelanul				as motivo_cancelanul
	, ctx.llave_integral_prod				as llave_integral_prod
	, ctx.vigencia_tecnica				as vigencia_tecnica
	, ctx.porc_comis_recaudo				as porc_comis_recaudo
	, ctx.porc_comis_reaseguro				as porc_comis_reaseguro
	, null				as porc_comis_reaseguro_vda
	, ctx.valor_comision_reaseguro				as valor_comision_reaseguro
	, null				as valor_comision_reaseguro_vda
	, ctx.agente				as agente
	, ctx.codigo_ramo_contable				as codigo_ramo_contable
	, ctx.ramo_contable				as ramo_contable
	, ctx.divipol_tom				as divipol_tom
	, ctx.producto_del_acuerdo				as producto_del_acuerdo
	, ctx.canal_venta				as canal_venta
	, ctx.promotor				as promotor
	, ctx.tipo_negocio				as tipo_negocio
	, ctx.numero_pol_gas				as numero_pol_gas
	, ctx.numero_contrato				as numero_contrato
	, ctx.plan_gasera				as plan_gasera
	, ctx.descrip_producto				as descrip_producto
	, ctx.descrip_venta				as descrip_venta
	, ctx.codigo_venta				as codigo_venta
	, ctx.dpto_smartflex				as dpto_smartflex
	, ctx.localida_smartflex				as localida_smartflex
	, ctx.codigo_mes_gas				as codigo_mes_gas
	, ctx.tipo_asegurado				as tipo_asegurado
	, ctx.monto_asegurado				as monto_asegurado
	, ctx.vlr_asegurado_aux_repatriacion				as vlr_asegurado_aux_repatriacion
	, ctx.identif_asegurado				as doc_asegurado_objeto
	, ctx.fecha_nacer_aseg				as fecha_nacer_aseg
	, ctx.sponsor				as sponsor
	, ctx.genero				as genero_tomador
	, ctx.motivocancelacion				as motivocancelacion
	, ctx.hijos				as hijos
	, ctx.estado_civil				as estado_civil
	, ctx.campana				as campana
	, ctx.cod_campana				as cod_campana
	, ctx.nombre_campana				as nombre_campana
	, ctx.cod_nombre_campana				as cod_nombre_campana
	, ctx.prima_anual				as prima_anual
	, ctx.prima_mensual				as prima_mensual
	, round(m.m_comision_agente, 8)				as comision_agente
	, round(m.m_gastos_expedicion, 8)				as gastos_expedicion
	, round(m.m_suma_aseg_exeq_aux_repatria, 8)				as suma_aseg_exeq_aux_repatria
	, round(m.m_suma_aseg_exeq_muerte, 8)				as suma_aseg_exeq_muerte
	, ctx.iva_comis_age				as iva_comis_age
	, ctx.reteica_agente				as reteica_agente
	, ctx.retefte_agente				as retefte_agente
	, ctx.retiva_agente				as retiva_agente
	, round(m.m_iva_prima, 8)				as iva_prima
	, ctx.estado_final_pol				as estado_final_pol
	, ctx.porc_comis_agente				as porc_comis_agente
	, ctx.porc_part_agente				as porc_part_agente
	, ctx.genero_asegurado				as genero_asegurado
	, ctx.dpto_asegurado				as dpto_asegurado
	, ctx.ciudad_asegurado				as ciudad_asegurado
	, ctx.divipol_asegurado				as divipol_asegurado
	, ctx.tipo_mascota_dos_exequias				as tipo_mascota_dos_exequias
	, ctx.nombre_mascota_dos_exequias				as nombre_mascota_dos_exequias
	, ctx.email_asegurado				as email_asegurado
	, ctx.tel_asegurado				as tel_asegurado
	, ctx.desc_movimiento				as desc_movimiento
	, ctx.valor_comision_reaseguro_exe				as valor_comision_reaseguro_exe
	, ctx.valor_comision_reaseguro_ap				as valor_comision_reaseguro_ap
	, ctx.dire_asegurado				as dire_asegurado
	, ctx.primera_vig_dsd_pol				as primera_vig_dsd_pol
	, null				as st_ast_id
	, null				as linea_credito_acuerdo
	, null				as fecha_desembolso
	, null				as rsa_policyid
	, null				as ri_operationpk
	, null				as agrv_dcoid
	, null				as liunc_pk
	, null				as edad_asegurado
	, null				as cod_dpto_asegurado
	, null				as cod_ciudad_asegurado
	, null				as factor_vida
from acsele_alfa_bi.em_st_7 ctx
left join acsele_alfa_bi.em_st_10 m
on ctx.key_operationpk = m.thrdprtmvmntplc_operationpk
and ctx.key_policyid = m.thrdprtmvmntplc_policyid
union all
-- ============================== VIDA ==============================
select /*+ parallel(3) */
	  cast('VIDA_DEUDORES' as varchar2(30))		as estandar_configuracion
	, ctx.agregatedpolicyid				as idpoliza
	, ctx.idproducto				as idproducto
	, replace(ctx.codigo_compania, '.0', '')				as codigo_compania
	, ctx.compania				as compania
	, to_char(ctx.codigo_producto)				as codigo_producto
	, ctx.producto				as producto
	, ctx.codigo_ramo				as codigo_ramo
	, ctx.ramo				as ramo
	, replace(ctx.codigo_sucursal, '.0', '')				as codigo_sucursal
	, ctx.sucursal				as sucursal
	, ctx.identificacion_tomador				as identificacion_tomador
	, ctx.tomador				as tomador
	, null				as departamento_tom
	, null				as ciudad_tom
	, null				as direcion_tom
	, null				as telefono_tomador
	, null				as email_tomador
	, null				as identifica_pagador_maestro
	, ctx.pagador_maestro				as informacion_pagador_maestro
	, ctx.ptipidentaseg				as tip_ident_asegurado
	, ctx.pidentaseg				as ident_asegurado
	, ctx.pnomaseg				as asegurado
	, null				as identificacion_empleador
	, null				as nombre_empleador
	, null				as identificacion_pagador
	, null				as pagador
	, ctx.idacuerdo				as idacuerdo
	, to_char(ctx.fechaini_acuerdo, 'DD-MM-YYYY')				as fechaini_acuerdo
	, to_char(ctx.fechafin_acuerdo, 'DD-MM-YYYY')				as fechafin_acuerdo
	, ctx.acuerdo				as numero_poliza_matriz
	, null				as nombre_acuerdo
	, null				as estado_acuerdo
	, ctx.tipo_poliza				as tipo_poliza
	, ctx.nro_poliza				as nro_poliza
	, ctx.frecuencia_pago				as frecuencia_pago
	, ctx.fecha_emision				as fechaemision
	, ctx.periodo_operacion				as periodo_emision
	, to_char(ctx.estado_poliza)				as estado_poliza
	, ctx.fecha_incial_pol				as fecha_incial_pol
	, ctx.fecha_final_pol				as fecha_final_pol
	, ctx.tipo_produccion				as tipo_produccion
	, ctx.codigo_unidad_negocio				as cod_unidad_negocio
	, ctx.unidad_negocio				as unidad_negocio
	, ctx.nro_operacion				as nro_operacion
	, null				as tipo_inmueble
	, null				as direccion_inmueble
	, null				as cod_departamento_inmueble
	, null				as departamento_inmueble
	, null				as cod_ciudad_inmueble
	, null				as ciudad_inmueble
	, null				as cod_divipol_municipio
	, null				as cod_zona_sismica
	, null				as tipo_estructura_bien
	, null				as cod_tipo_estructura_bien
	, null				as raza_mascota
	, null				as tipo_mascota
	, null				as edad_mascota
	, null				as genero_mascota
	, null				as detalle_mascota
	, null				as nombre_mascota
	, ctx.linea_credito_nego				as linea_credito
	, ctx.descripcion_evento				as descripcion_evento
	, ctx.moneda_acuerdo				as moneda_acuerdo
	, to_char(ctx.unidad_riesgo)				as unidad_riesgo
	, ctx.numero_credito				as numero_credito
	, ctx.fecha_inicial_ur				as fecha_inicial_ur
	, ctx.fecha_final_ur				as fecha_final_ur
	, tm.thrdprtmvmntplc_operationpk				as nro_operacion_pol
	, null				as origen_dato
	, null				as nro_operacion_factura
	, null				as fecha_inicial_proyecto
	, null				as fecha_final_proyecto
	, null				as periodo_gracia
	, null				as valor_anticipo
	, null				as nombre_edificio
	, null				as numero_pisos
	, null				as ano_construcion_edificio
	, sum(case
			when tm.thrdprtmvmntplc_concept_u in ('PRIMADEVUELTA','PRIMAANU','PRIMAANUIND','PRIMADEVUELTAIND','PRIMAEMITIDADEV','PRIMAEMITIDAANUVIDAAP','PRIMAEMITIDAPAGDEVVIDAAP') then tm.thrdprtmvmntplc_ammount * (-1)
			when tm.thrdprtmvmntplc_concept_u in ('PRIMAEMITIDA','PRIMAREHAB','PRIMAREHABIND','PRIMAPERIODOPROVIS','PRIMAEMITIDAIND','PRIMAPERIODOPROVISIND','PRIMAEMITIDAVIDAAP','PRIMAEMITIDAPAGVIDAAP') then tm.thrdprtmvmntplc_ammount
		end)				as prima
	, null				as prima_vida
	, null				as prima_incendio
	, null				as prima_terremoto
	, null				as prima_respcivil
	, sum(case
			when tm.thrdprtmvmntplc_concept_u in ('COSTORECAUDACIONDEV','COSTORECAUDACIONDEVIND','COSTORECAUDOANU','COSTORECAUDOANUIND','COSTORECAUDACIONANUVIDAAP','COSTORECAUDACIONDEVVIDAAP') then tm.thrdprtmvmntplc_ammount * (-1)
			when tm.thrdprtmvmntplc_concept_u in ('COSTORECAUDACION','COSTORECAUDACIONIND','COSTORECAUDOPROVIS','COSTORECAUDOPROVISIND','COSTORECAUDOREHAB','COSTORECAUDOREHABIND',
													'COSTORECAUDACIONREHABVIDAAP','COSTORECAUDACIONVIDAAP','COSTORECAUDACIONPAGVIDAAP') then tm.thrdprtmvmntplc_ammount
		end)				as costo_recaudacion
	, null				as costo_recaudacion_vida
	, null				as costo_recaudacion_inc
	, null				as costo_recaudacion_terr
	, null				as costo_recaudacion_respcivil
	, sum(case
			when tm.thrdprtmvmntplc_concept_u in ('IVACOSTORECAUDACIONDEV','IVACOSTORECAUDACIONDEVIND','IVACOSTORECAUDOANU','IVACOSTORECAUDOANUIND','IVACOSTORECAUDACIONDEVVIDAAP','IVACOSTORECAUDACIONANUVIDAAP') then tm.thrdprtmvmntplc_ammount * (-1)
			when tm.thrdprtmvmntplc_concept_u in ('IVACOSTORECAUDACION','IVACOSTORECAUDACIONIND','IVACOSTORECAUDOPROVIS','IVACOSTORECAUDOPROVISIND','IVACOSTORECAUDOREHAB','IVACOSTORECAUDOREHABIND',
													'IVACOSTORECAUDACIONVIDAAP','IVACOSTORECAUDACIONREHABVIDAAP','IVACOSTORECAUDACIONPAGVIDAAP') then tm.thrdprtmvmntplc_ammount
		end)				as iva_costo_recaudacion
	, null				as iva_costo_recaudacion_vida
	, null				as comision_total_interincter
	, null				as comision_intermediario_inc
	, null				as comision_intermediario_terr
	, null				as iva_costo_recaudo_inc
	, null				as iva_costo_recaudo_terr
	, null				as iva_costo_recaudo_rescivil
	, null				as iva_costo_recaudo_provinc
	, null				as iva_costo_recaudo_provterr
	, null				as iva_prima_total
	, null				as iva_prima_bancaseg
	, null				as iva_prima_incendio
	, null				as iva_prima_terremoto
	, null				as iva_prima_repcivil
	, null				as iva_prima_provincendio
	, null				as iva_prima_provterremoto
	, null				as iva_prima_provrespcivil
	, sum(case
			when tm.thrdprtmvmntplc_concept in ('SumaAsegPeriodo') then tm.thrdprtmvmntplc_ammount		-- sensible a mayusculas, igual que la original
		end)				as suma_asegurada_periodo
	, null				as contrib_bomberos_incendio
	, null				as contrib_bomberos_terremoto
	, null				as contrib_bomberos_hogar
	, null				as prima_asistencia
	, null				as prima_sustraccion
	, null				as prima_responsabilidad
	, null				as iva_prima_asistencia
	, null				as iva_prima_sustraccion
	, null				as iva_prima_responsabilidad
	, null				as iva_prima_anulada_mascotas
	, null				as comision_neta_alfa
	, null				as comision_neta_vida
	, null				as valor_neto_reaseguro
	, null				as valor_neto_reaseguro_vida
	, null				as comision_tarjetacr
	, null				as comision_tarjetacr_vida
	, null				as fecha_mov_cancelanul
	, null				as fecha_efectiva_cancelanu
	, null				as motivo_cancelanul
	, null				as llave_integral_prod
	, null				as vigencia_tecnica
	, null				as porc_comis_recaudo
	, null				as porc_comis_reaseguro
	, null				as porc_comis_reaseguro_vda
	, null				as valor_comision_reaseguro
	, null				as valor_comision_reaseguro_vda
	, ctx.agente				as agente
	, null				as codigo_ramo_contable
	, null				as ramo_contable
	, null				as divipol_tom
	, ctx.producto_del_acuerdo				as producto_del_acuerdo
	, null				as canal_venta
	, null				as promotor
	, null				as tipo_negocio
	, null				as numero_pol_gas
	, null				as numero_contrato
	, null				as plan_gasera
	, null				as descrip_producto
	, null				as descrip_venta
	, null				as codigo_venta
	, null				as dpto_smartflex
	, null				as localida_smartflex
	, null				as codigo_mes_gas
	, null				as tipo_asegurado
	, null				as monto_asegurado
	, null				as vlr_asegurado_aux_repatriacion
	, null				as doc_asegurado_objeto
	, ctx.fecha_nacer_asegurado				as fecha_nacer_aseg
	, null				as sponsor
	, null				as genero_tomador
	, null				as motivocancelacion
	, null				as hijos
	, null				as estado_civil
	, null				as campana
	, null				as cod_campana
	, null				as nombre_campana
	, null				as cod_nombre_campana
	, null				as prima_anual
	, null				as prima_mensual
	, sum(case
			when tm.thrdprtmvmntplc_concept_u in ('COMISIONVIDADEV','COMISIONVIDAINDDEV','COMISIONAGENTEDEV') then tm.thrdprtmvmntplc_ammount * (-1)
			when tm.thrdprtmvmntplc_concept_u in ('COMISIONVIDA','COMISIONVIDAIND','COMISIONAGENTE') then tm.thrdprtmvmntplc_ammount
		end)				as comision_agente
	, null				as gastos_expedicion
	, null				as suma_aseg_exeq_aux_repatria
	, null				as suma_aseg_exeq_muerte
	, null				as iva_comis_age
	, null				as reteica_agente
	, null				as retefte_agente
	, null				as retiva_agente
	, null				as iva_prima
	, null				as estado_final_pol
	, null				as porc_comis_agente
	, null				as porc_part_agente
	, null				as genero_asegurado
	, ctx.desc_departamento				as dpto_asegurado
	, ctx.desc_ciudad				as ciudad_asegurado
	, ctx.codigo_divipola_municipio				as divipol_asegurado
	, null				as tipo_mascota_dos_exequias
	, null				as nombre_mascota_dos_exequias
	, null				as email_asegurado
	, null				as tel_asegurado
	, null				as desc_movimiento
	, null				as valor_comision_reaseguro_exe
	, null				as valor_comision_reaseguro_ap
	, ctx.direccion_aseguado				as dire_asegurado
	, null				as primera_vig_dsd_pol
	, ctx.st_ast_id				as st_ast_id
	, ctx.linea_credito				as linea_credito_acuerdo
	, ctx.fecha_desembolso				as fecha_desembolso
	, ctx.rsa_policyid				as rsa_policyid
	, ctx.ri_operationpk				as ri_operationpk
	, ctx.agrv_dcoid				as agrv_dcoid
	, ctx.liunc_pk				as liunc_pk
	, ctx.edad_aseguado				as edad_asegurado
	, ctx.cod_departamento				as cod_dpto_asegurado
	, ctx.cod_ciudad				as cod_ciudad_asegurado
	, ctx.factor_vida				as factor_vida
from acsele_alfa_bi.em_vd_7 ctx
inner join acsele_alfa_bi.em_vd_9 tm
on ctx.nro_operacion = tm.thrdprtmvmntplc_operationpk
group by ctx.idproducto
	, ctx.codigo_compania
	, ctx.compania
	, ctx.codigo_producto
	, ctx.producto
	, ctx.codigo_ramo
	, ctx.ramo
	, ctx.codigo_sucursal
	, ctx.sucursal
	, ctx.identificacion_tomador
	, ctx.tomador
	, ctx.pagador_maestro
	, ctx.idacuerdo
	, ctx.acuerdo
	, ctx.st_ast_id
	, ctx.fechaini_acuerdo
	, ctx.fechafin_acuerdo
	, ctx.producto_del_acuerdo
	, ctx.tipo_poliza
	, ctx.nro_poliza
	, ctx.fecha_emision
	, ctx.periodo_operacion
	, ctx.fecha_incial_pol
	, ctx.fecha_final_pol
	, ctx.tipo_produccion
	, ctx.unidad_negocio
	, ctx.codigo_unidad_negocio
	, ctx.estado_poliza
	, ctx.nro_operacion
	, ctx.linea_credito
	, ctx.linea_credito_nego
	, ctx.descripcion_evento
	, ctx.moneda_acuerdo
	, ctx.unidad_riesgo
	, ctx.numero_credito
	, ctx.fecha_desembolso
	, ctx.fecha_inicial_ur
	, ctx.fecha_final_ur
	, ctx.agente
	, ctx.pnomaseg
	, ctx.pidentaseg
	, ctx.ptipidentaseg
	, ctx.frecuencia_pago
	, ctx.rsa_policyid
	, ctx.ri_operationpk
	, ctx.agrv_dcoid
	, ctx.liunc_pk
	, ctx.agregatedpolicyid
	, tm.thrdprtmvmntplc_operationpk
	, ctx.edad_aseguado
	, ctx.fecha_nacer_asegurado
	, ctx.direccion_aseguado
	, ctx.desc_departamento
	, ctx.cod_departamento
	, ctx.desc_ciudad
	, ctx.cod_ciudad
	, ctx.factor_vida
	, ctx.codigo_divipola_municipio
;
commit;

/* ============================================================================================ */
/* CONSOLIDADO - EMISIONES: homologacion de ORIGEN por compania                                 */
/*    Generales, Vida y Satelite se catalogan por la compania del producto (cfg_pcr_compania):  */
/*    0 = GENERALES, 50 = VIDA. El estandar de configuracion con que se leyo el producto queda  */
/*    en ESTANDAR_CONFIGURACION (VIDA_DEUDORES / GENERALES / SATELITE).                         */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.emisiones;
commit;
*/

create table acsele_alfa_bi.emisiones
parallel 2
tablespace tsdat_acsele_alfa_rep
as
select /*+ parallel(3) */
	  coalesce(cc.origen, 'SIN_HOMOLOGAR')		as origen			-- compania: GENERALES (0) / VIDA (50)
	, e.*														-- estandar_configuracion + 202 columnas
from acsele_alfa_bi.emisiones_est e
left join acsele_alfa_bi.cfg_pcr_compania cc
on e.codigo_compania = cc.codigo_compania
;
commit;

create index acsele_alfa_bi.emisiones_idx_1
on acsele_alfa_bi.emisiones (origen, nro_operacion);

commit;

select
	count(*) as cant_reg
from acsele_alfa_bi.emisiones
;


/* ============================================================================================ */
/* LIMPIEZA OPCIONAL DE TABLAS DE TRABAJO (ejecutar solo despues de validar el resultado)       */
/* ============================================================================================ */

/*
drop table acsele_alfa_bi.em_1;
drop table acsele_alfa_bi.em_2;
drop table acsele_alfa_bi.em_3;
drop table acsele_alfa_bi.em_vd_4;
drop table acsele_alfa_bi.em_vd_5;
drop table acsele_alfa_bi.em_vd_6;
drop table acsele_alfa_bi.em_vd_pol;
drop table acsele_alfa_bi.em_vd_opr;
drop table acsele_alfa_bi.em_gn_4;
drop table acsele_alfa_bi.em_gn_5;
drop table acsele_alfa_bi.em_gn_6;
drop table acsele_alfa_bi.em_gn_pol;
drop table acsele_alfa_bi.em_gn_opr;
drop table acsele_alfa_bi.em_st_4;
drop table acsele_alfa_bi.em_st_5;
drop table acsele_alfa_bi.em_st_6;
drop table acsele_alfa_bi.em_st_pol;
drop table acsele_alfa_bi.em_st_opr;
drop table acsele_alfa_bi.em_pol;
drop table acsele_alfa_bi.em_opr;
drop table acsele_alfa_bi.em_ppb;
drop table acsele_alfa_bi.em_tpb;
drop table acsele_alfa_bi.em_oab;
drop table acsele_alfa_bi.em_tmb;
drop table acsele_alfa_bi.em_vd_ppt;
drop table acsele_alfa_bi.em_vd_tom;
drop table acsele_alfa_bi.em_vd_age;
drop table acsele_alfa_bi.em_vd_pgm;
drop table acsele_alfa_bi.em_vd_aof;
drop table acsele_alfa_bi.em_vd_asg;
drop table acsele_alfa_bi.em_vd_7;
drop table acsele_alfa_bi.em_vd_9;
drop table acsele_alfa_bi.em_gn_ppt;
drop table acsele_alfa_bi.em_gn_tdb;
drop table acsele_alfa_bi.em_gn_tdr;
drop table acsele_alfa_bi.em_gn_age;
drop table acsele_alfa_bi.em_gn_rub;
drop table acsele_alfa_bi.em_gn_ruf;
drop table acsele_alfa_bi.em_gn_rua;
drop table acsele_alfa_bi.em_gn_oaf;
drop table acsele_alfa_bi.em_gn_oaa;
drop table acsele_alfa_bi.em_gn_tpf;
drop table acsele_alfa_bi.em_gn_tpr;
drop table acsele_alfa_bi.em_gn_7;
drop table acsele_alfa_bi.em_gn_9;
drop table acsele_alfa_bi.em_gn_10;
drop table acsele_alfa_bi.em_st_ppt;
drop table acsele_alfa_bi.em_st_tom;
drop table acsele_alfa_bi.em_st_pgm;
drop table acsele_alfa_bi.em_st_spo;
drop table acsele_alfa_bi.em_st_age;
drop table acsele_alfa_bi.em_st_tdb;
drop table acsele_alfa_bi.em_st_tdr;
drop table acsele_alfa_bi.em_st_tpf;
drop table acsele_alfa_bi.em_st_tpr;
drop table acsele_alfa_bi.em_st_oaf;
drop table acsele_alfa_bi.em_st_oaa;
drop table acsele_alfa_bi.em_st_cob;
drop table acsele_alfa_bi.em_st_cor;
drop table acsele_alfa_bi.em_st_vtb;
drop table acsele_alfa_bi.em_st_vtf;
drop table acsele_alfa_bi.em_st_vtr;
drop table acsele_alfa_bi.em_st_agb;
drop table acsele_alfa_bi.em_st_ulo;
drop table acsele_alfa_bi.em_st_efp;
drop table acsele_alfa_bi.em_st_pvi;
drop table acsele_alfa_bi.em_st_7;
drop table acsele_alfa_bi.em_st_9;
drop table acsele_alfa_bi.em_st_10;
drop table acsele_alfa_bi.emisiones_est;
commit;
*/

/* ============================================================================================ */
/* VALIDACION RAPIDA POR NEGOCIO                                                                */
/* ============================================================================================ */

select
	  origen
	, estandar_configuracion
	, count(*)		as cant_reg
	, sum(prima)	as prima
from acsele_alfa_bi.emisiones
group by origen
	, estandar_configuracion
order by origen
	, estandar_configuracion
;

/* Debe dar 0: compania sin homologar en cfg_pcr_compania */
select
	  count(*)		as sin_homologar
from acsele_alfa_bi.emisiones
where origen = 'SIN_HOMOLOGAR'
;
