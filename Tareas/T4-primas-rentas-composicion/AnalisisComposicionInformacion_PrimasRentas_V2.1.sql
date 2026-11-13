--  401  Filas
with prep_rentas_rentas_1 as (
	select
		  anpmvmntspg.periodo_pago						as anpmvmntspg_periodo_pago
		, anpmvmntspg.nid_distribucion_vigencia			as anpmvmntspg_nid_distribucion_vigencia
		, anpmvmntspg.cod_tipo_identificacion			as anpmvmntspg_cod_tipo_identificacion
		, anpmvmntspg.nid_numero_identificacion			as anpmvmntspg_nid_numero_identificacion
		, anpmvmntspg.cod_concepto						as anpmvmntspg_cod_concepto
		, anpmvmntspg.vlr_concepto						as anpmvmntspg_vlr_concepto
		, anpmvmntspg.fec_pago							as anpmvmntspg_fec_pago
		, anpmvmntspg.fec_inicial						as anpmvmntspg_fec_inicial
		, anpmvmntspg.fec_final							as anpmvmntspg_fec_final
		, anpmvmntspg.num_dias_liq_concepto				as anpmvmntspg_num_dias_liq_concepto
		, anpmvmntspg.cod_tipo_identificacion_tutor		as anpmvmntspg_cod_tipo_identificacion_tutor
		, anpmvmntspg.nid_numero_identificacion_tuto	as anpmvmntspg_nid_numero_identificacion_tuto
		, anpmvmntspg.cod_sucursal_giro					as anpmvmntspg_cod_sucursal_giro
		, anpmvmntspg.fec_generacion_nomina				as anpmvmntspg_fec_generacion_nomina
		, anpmvmntspg.nid_numero_solicitud_giro			as anpmvmntspg_nid_numero_solicitud_giro
		, anpmvmntspg.cod_forma_pago					as anpmvmntspg_cod_forma_pago
		, anpmvmntspg.vlr_base							as anpmvmntspg_vlr_base
		, anpmvmntspg.nid_consecutivo_tutor				as anpmvmntspg_nid_consecutivo_tutor
		, anpmvmntspg.cod_miscelaneo					as anpmvmntspg_cod_miscelaneo
		, anpmvmntspg.bln_valor_upc						as anpmvmntspg_bln_valor_upc
		, anpmvmntspg.fec_inhabilitacion				as anpmvmntspg_fec_inhabilitacion
		, anpmvmntspg.comentario_inhabilitacion			as anpmvmntspg_comentario_inhabilitacion
		, anpmvmntspg.bln_rechazado_occired				as anpmvmntspg_bln_rechazado_occired
		, anpmvmntspg.fec_habilitacion					as anpmvmntspg_fec_habilitacion
		, anpmvmntspg.usuario_habilitacion				as anpmvmntspg_usuario_habilitacion
		, anpmvmntspg.cod_estado_movimiento				as anpmvmntspg_cod_estado_movimiento
		, anpmvmntspg.idn_pago_masivo					as anpmvmntspg_idn_pago_masivo
		, anpmvmntspg.fec_modificacion					as anpmvmntspg_fec_modificacion
	from general.anp_movimientos_pago anpmvmntspg
--	where anpmvmntspg.periodo_pago = 202512
	where anpmvmntspg.fec_inicial between to_date('01/06/2026 00:00:00','dd/mm/yyyy hh24:mi:ss') and to_date('30/06/2026 23:59:59','dd/mm/yyyy hh24:mi:ss')
), prep_rentas_rentas_2 as (
	select
		  anpmvmntspg.anpmvmntspg_periodo_pago
		, anpmvmntspg.anpmvmntspg_nid_distribucion_vigencia
		, anpmvmntspg.anpmvmntspg_cod_tipo_identificacion
		, anpmvmntspg.anpmvmntspg_nid_numero_identificacion
		, anpmvmntspg.anpmvmntspg_cod_concepto
		, anpmvmntspg.anpmvmntspg_vlr_concepto
		, anpmvmntspg.anpmvmntspg_fec_pago
		, anpmvmntspg.anpmvmntspg_fec_inicial
		, anpmvmntspg.anpmvmntspg_fec_final
		, anpmvmntspg.anpmvmntspg_num_dias_liq_concepto
		, anpmvmntspg.anpmvmntspg_cod_tipo_identificacion_tutor
		, anpmvmntspg.anpmvmntspg_nid_numero_identificacion_tuto
		, anpmvmntspg.anpmvmntspg_cod_sucursal_giro
		, anpmvmntspg.anpmvmntspg_fec_generacion_nomina
		, anpmvmntspg.anpmvmntspg_nid_numero_solicitud_giro
		, anpmvmntspg.anpmvmntspg_cod_forma_pago
		, anpmvmntspg.anpmvmntspg_vlr_base
		, anpmvmntspg.anpmvmntspg_nid_consecutivo_tutor
		, anpmvmntspg.anpmvmntspg_cod_miscelaneo
		, anpmvmntspg.anpmvmntspg_bln_valor_upc
		, anpmvmntspg.anpmvmntspg_fec_inhabilitacion
		, anpmvmntspg.anpmvmntspg_comentario_inhabilitacion
		, anpmvmntspg.anpmvmntspg_bln_rechazado_occired
		, anpmvmntspg.anpmvmntspg_fec_habilitacion
		, anpmvmntspg.anpmvmntspg_usuario_habilitacion
		, anpmvmntspg.anpmvmntspg_cod_estado_movimiento
		, anpmvmntspg.anpmvmntspg_idn_pago_masivo
		, anpmvmntspg.anpmvmntspg_fec_modificacion
		, to_date((substr(anpmvmntspg.anpmvmntspg_periodo_pago, 1, 4) || '-' || substr(anpmvmntspg.anpmvmntspg_periodo_pago, 5, 2) || '-01'), 'yyyy-mm-dd') as fecha_inicio_periodo_pago_renta
		, last_day(to_date((substr(anpmvmntspg.anpmvmntspg_periodo_pago, 1, 4) || '-' || substr(anpmvmntspg.anpmvmntspg_periodo_pago, 5, 2) || '-01'), 'yyyy-mm-dd')) as fecha_fin_periodo_pago_renta
	from prep_rentas_rentas_1 anpmvmntspg
), prep_rentas_rentas_3 as (
	select
		  anpmvmntspg.anpmvmntspg_periodo_pago
		, anpmvmntspg.anpmvmntspg_nid_distribucion_vigencia
		, anpmvmntspg.anpmvmntspg_cod_tipo_identificacion
		, anpmvmntspg.anpmvmntspg_nid_numero_identificacion
		, anpmvmntspg.anpmvmntspg_cod_concepto
		, anpmvmntspg.anpmvmntspg_vlr_concepto
		, anpmvmntspg.anpmvmntspg_fec_pago
		, anpmvmntspg.anpmvmntspg_fec_inicial
		, anpmvmntspg.anpmvmntspg_fec_final
		, anpmvmntspg.anpmvmntspg_num_dias_liq_concepto
		, anpmvmntspg.anpmvmntspg_cod_tipo_identificacion_tutor
		, anpmvmntspg.anpmvmntspg_nid_numero_identificacion_tuto
		, anpmvmntspg.anpmvmntspg_cod_sucursal_giro
		, anpmvmntspg.anpmvmntspg_fec_generacion_nomina
		, anpmvmntspg.anpmvmntspg_nid_numero_solicitud_giro
		, anpmvmntspg.anpmvmntspg_cod_forma_pago
		, anpmvmntspg.anpmvmntspg_vlr_base
		, anpmvmntspg.anpmvmntspg_nid_consecutivo_tutor
		, anpmvmntspg.anpmvmntspg_cod_miscelaneo
		, anpmvmntspg.anpmvmntspg_bln_valor_upc
		, anpmvmntspg.anpmvmntspg_fec_inhabilitacion
		, anpmvmntspg.anpmvmntspg_comentario_inhabilitacion
		, anpmvmntspg.anpmvmntspg_bln_rechazado_occired
		, anpmvmntspg.anpmvmntspg_fec_habilitacion
		, anpmvmntspg.anpmvmntspg_usuario_habilitacion
		, anpmvmntspg.anpmvmntspg_cod_estado_movimiento
		, anpmvmntspg.anpmvmntspg_idn_pago_masivo
		, anpmvmntspg.anpmvmntspg_fec_modificacion
		, anpmvmntspg.fecha_inicio_periodo_pago_renta
		, anpmvmntspg.fecha_fin_periodo_pago_renta
		, anpdstrbcnvgpnsn.nid_distribucion_vigencia	as anpdstrbcnvgpnsn_nid_distribucion_vigencia
		, anpdstrbcnvgpnsn.nid_version_pension			as anpdstrbcnvgpnsn_nid_version_pension
		, anpdstrbcnvgpnsn.nid_pension					as anpdstrbcnvgpnsn_nid_pension
		, anpdstrbcnvgpnsn.fec_inicial					as anpdstrbcnvgpnsn_fec_inicial
		, anpdstrbcnvgpnsn.fec_final					as anpdstrbcnvgpnsn_fec_final
		, anpdstrbcnvgpnsn.cod_razon_cambio				as anpdstrbcnvgpnsn_cod_razon_cambio
		, anpdstrbcnvgpnsn.comentarios					as anpdstrbcnvgpnsn_comentarios
		, anpdstrbcnvgpnsn.usuario_creacion				as anpdstrbcnvgpnsn_usuario_creacion
	from prep_rentas_rentas_2 anpmvmntspg
	inner join general.anp_distribucion_vig_pension anpdstrbcnvgpnsn
	on anpmvmntspg.anpmvmntspg_nid_distribucion_vigencia = anpdstrbcnvgpnsn.nid_distribucion_vigencia
), prep_rentas_rentas_4 as (
	select
		  anpmvmntspg.anpmvmntspg_periodo_pago
		, anpmvmntspg.anpmvmntspg_nid_distribucion_vigencia
		, anpmvmntspg.anpmvmntspg_cod_tipo_identificacion
		, anpmvmntspg.anpmvmntspg_nid_numero_identificacion
		, anpmvmntspg.anpmvmntspg_cod_concepto
		, anpmvmntspg.anpmvmntspg_vlr_concepto
		, anpmvmntspg.anpmvmntspg_fec_pago
		, anpmvmntspg.anpmvmntspg_fec_inicial
		, anpmvmntspg.anpmvmntspg_fec_final
		, anpmvmntspg.anpmvmntspg_num_dias_liq_concepto
		, anpmvmntspg.anpmvmntspg_cod_tipo_identificacion_tutor
		, anpmvmntspg.anpmvmntspg_nid_numero_identificacion_tuto
		, anpmvmntspg.anpmvmntspg_cod_sucursal_giro
		, anpmvmntspg.anpmvmntspg_fec_generacion_nomina
		, anpmvmntspg.anpmvmntspg_nid_numero_solicitud_giro
		, anpmvmntspg.anpmvmntspg_cod_forma_pago
		, anpmvmntspg.anpmvmntspg_vlr_base
		, anpmvmntspg.anpmvmntspg_nid_consecutivo_tutor
		, anpmvmntspg.anpmvmntspg_cod_miscelaneo
		, anpmvmntspg.anpmvmntspg_bln_valor_upc
		, anpmvmntspg.anpmvmntspg_fec_inhabilitacion
		, anpmvmntspg.anpmvmntspg_comentario_inhabilitacion
		, anpmvmntspg.anpmvmntspg_bln_rechazado_occired
		, anpmvmntspg.anpmvmntspg_fec_habilitacion
		, anpmvmntspg.anpmvmntspg_usuario_habilitacion
		, anpmvmntspg.anpmvmntspg_cod_estado_movimiento
		, anpmvmntspg.anpmvmntspg_idn_pago_masivo
		, anpmvmntspg.anpmvmntspg_fec_modificacion
		, anpmvmntspg.fecha_inicio_periodo_pago_renta
		, anpmvmntspg.fecha_fin_periodo_pago_renta
		, anpmvmntspg.anpdstrbcnvgpnsn_nid_distribucion_vigencia
		, anpmvmntspg.anpdstrbcnvgpnsn_nid_version_pension
		, anpmvmntspg.anpdstrbcnvgpnsn_nid_pension
		, anpmvmntspg.anpdstrbcnvgpnsn_fec_inicial
		, anpmvmntspg.anpdstrbcnvgpnsn_fec_final
		, anpmvmntspg.anpdstrbcnvgpnsn_cod_razon_cambio
		, anpmvmntspg.anpdstrbcnvgpnsn_comentarios
		, anpmvmntspg.anpdstrbcnvgpnsn_usuario_creacion
		, anppnsns.nid_pension					as anppnsns_nid_pension
		, anppnsns.nid_procedencia				as anppnsns_nid_procedencia
		, anppnsns.vlr_pension_inicial			as anppnsns_vlr_pension_inicial
		, anppnsns.vlr_pension_final			as anppnsns_vlr_pension_final
		, anppnsns.cod_procedencia				as anppnsns_cod_procedencia
		, anppnsns.cod_origen					as anppnsns_cod_origen
		, anppnsns.bln_definitivo				as anppnsns_bln_definitivo
		, anppnsns.cod_tipo_identificacion		as anppnsns_cod_tipo_identificacion
		, anppnsns.nid_numero_identificacion	as anppnsns_nid_numero_identificacion
		, anppnsns.cod_sucursal_origen			as anppnsns_cod_sucursal_origen
		, anppnsns.nid_siniestro				as anppnsns_nid_siniestro
		, anppnsns.fec_expedicion				as anppnsns_fec_expedicion
		, anppnsns.num_mesadas					as anppnsns_num_mesadas
	from prep_rentas_rentas_3 anpmvmntspg
	inner join general.anp_pensiones anppnsns
	on anpmvmntspg.anpdstrbcnvgpnsn_nid_pension = anppnsns.nid_pension
), prep_rentas_rentas_5 as (
	select
		  anpmvmntspg.anpmvmntspg_periodo_pago
		, anpmvmntspg.anpmvmntspg_nid_distribucion_vigencia
		, anpmvmntspg.anpmvmntspg_cod_tipo_identificacion
		, anpmvmntspg.anpmvmntspg_nid_numero_identificacion
		, anpmvmntspg.anpmvmntspg_cod_concepto
		, anpmvmntspg.anpmvmntspg_vlr_concepto
		, anpmvmntspg.anpmvmntspg_fec_pago
		, anpmvmntspg.anpmvmntspg_fec_inicial
		, anpmvmntspg.anpmvmntspg_fec_final
		, anpmvmntspg.anpmvmntspg_num_dias_liq_concepto
		, anpmvmntspg.anpmvmntspg_cod_tipo_identificacion_tutor
		, anpmvmntspg.anpmvmntspg_nid_numero_identificacion_tuto
		, anpmvmntspg.anpmvmntspg_cod_sucursal_giro
		, anpmvmntspg.anpmvmntspg_fec_generacion_nomina
		, anpmvmntspg.anpmvmntspg_nid_numero_solicitud_giro
		, anpmvmntspg.anpmvmntspg_cod_forma_pago
		, anpmvmntspg.anpmvmntspg_vlr_base
		, anpmvmntspg.anpmvmntspg_nid_consecutivo_tutor
		, anpmvmntspg.anpmvmntspg_cod_miscelaneo
		, anpmvmntspg.anpmvmntspg_bln_valor_upc
		, anpmvmntspg.anpmvmntspg_fec_inhabilitacion
		, anpmvmntspg.anpmvmntspg_comentario_inhabilitacion
		, anpmvmntspg.anpmvmntspg_bln_rechazado_occired
		, anpmvmntspg.anpmvmntspg_fec_habilitacion
		, anpmvmntspg.anpmvmntspg_usuario_habilitacion
		, anpmvmntspg.anpmvmntspg_cod_estado_movimiento
		, anpmvmntspg.anpmvmntspg_idn_pago_masivo
		, anpmvmntspg.anpmvmntspg_fec_modificacion
		, anpmvmntspg.fecha_inicio_periodo_pago_renta
		, anpmvmntspg.fecha_fin_periodo_pago_renta
		, anpmvmntspg.anpdstrbcnvgpnsn_nid_distribucion_vigencia
		, anpmvmntspg.anpdstrbcnvgpnsn_nid_version_pension
		, anpmvmntspg.anpdstrbcnvgpnsn_nid_pension
		, anpmvmntspg.anpdstrbcnvgpnsn_fec_inicial
		, anpmvmntspg.anpdstrbcnvgpnsn_fec_final
		, anpmvmntspg.anpdstrbcnvgpnsn_cod_razon_cambio
		, anpmvmntspg.anpdstrbcnvgpnsn_comentarios
		, anpmvmntspg.anpdstrbcnvgpnsn_usuario_creacion
		, anpmvmntspg.anppnsns_nid_pension
		, anpmvmntspg.anppnsns_nid_procedencia
		, anpmvmntspg.anppnsns_vlr_pension_inicial
		, anpmvmntspg.anppnsns_vlr_pension_final
		, anpmvmntspg.anppnsns_cod_procedencia
		, anpmvmntspg.anppnsns_cod_origen
		, anpmvmntspg.anppnsns_bln_definitivo
		, anpmvmntspg.anppnsns_cod_tipo_identificacion
		, anpmvmntspg.anppnsns_nid_numero_identificacion
		, anpmvmntspg.anppnsns_cod_sucursal_origen
		, anpmvmntspg.anppnsns_nid_siniestro
		, anpmvmntspg.anppnsns_fec_expedicion
		, anpmvmntspg.anppnsns_num_mesadas		
		, prvrnts.nid_rentas					as prvrnts_nid_rentas
		, prvrnts.fec_expedicion				as prvrnts_fec_expedicion
		, prvrnts.fec_vigencia_inicial			as prvrnts_fec_vigencia_inicial
		, prvrnts.vlr_salario_base				as prvrnts_vlr_salario_base
		, prvrnts.vlr_prima_unica				as prvrnts_vlr_prima_unica
		, prvrnts.vlr_cuenta_individual			as prvrnts_vlr_cuenta_individual
		, prvrnts.vlr_bono_pensional			as prvrnts_vlr_bono_pensional
		, prvrnts.vlr_pension_inicial			as prvrnts_vlr_pension_inicial
		, prvrnts.vlr_pension_actual			as prvrnts_vlr_pension_actual
		, prvrnts.fec_ult_incremento_pension	as prvrnts_fec_ult_incremento_pension
		, prvrnts.pcj_ult_incremento_pension	as prvrnts_pcj_ult_incremento_pension
		, prvrnts.vlr_indemnizacion_seguro		as prvrnts_vlr_indemnizacion_seguro
		, prvrnts.vlr_otros_aportes				as prvrnts_vlr_otros_aportes
		, prvrnts.fec_pago						as prvrnts_fec_pago
		, prvrnts.lugar_pago					as prvrnts_lugar_pago
		, prvrnts.simbolo_point					as prvrnts_simbolo_point
		, prvrnts.ramo_point					as prvrnts_ramo_point
		, prvrnts.modulo_point					as prvrnts_modulo_point
		, prvrnts.endoso_point					as prvrnts_endoso_point
		, prvrnts.edo							as prvrnts_edo
		, prvrnts.bln_poliza_emitida			as prvrnts_bln_poliza_emitida
		, prvrnts.bln_actualizado_point			as prvrnts_bln_actualizado_point
		, prvrnts.cod_departamento				as prvrnts_cod_departamento
		, prvrnts.cod_ciudad					as prvrnts_cod_ciudad
		, prvrnts.cod_origen					as prvrnts_cod_origen
		, prvrnts.cod_grado_invalidez			as prvrnts_cod_grado_invalidez
		, prvrnts.cod_opcion_calculo			as prvrnts_cod_opcion_calculo
		, prvrnts.nid_siniestro					as prvrnts_nid_siniestro
		, prvrnts.nid_cotizacion				as prvrnts_nid_cotizacion
		, prvrnts.cod_sucursal_alfa				as prvrnts_cod_sucursal_alfa
		, prvrnts.cod_compania					as prvrnts_cod_compania
		, prvrnts.cod_tipo_identificacion		as prvrnts_cod_tipo_identificacion
		, prvrnts.nid_numero_identificacion		as prvrnts_nid_numero_identificacion
		, prvrnts.nid_afiliado					as prvrnts_nid_afiliado
		, prvrnts.cod_afp						as prvrnts_cod_afp
		, prvrnts.num_dias_trabajados			as prvrnts_num_dias_trabajados
		, prvrnts.lugar_expedicion				as prvrnts_lugar_expedicion
		, prvrnts.cod_estado_persona			as prvrnts_cod_estado_persona
		, prvrnts.pcj_pension					as prvrnts_pcj_pension
		, prvrnts.num_endoso_ult				as prvrnts_num_endoso_ult
		, prvrnts.cod_tipo_operacion			as prvrnts_cod_tipo_operacion
		, prvrnts.bln_procesado_point			as prvrnts_bln_procesado_point
		, prvrnts.fec_proceso_point				as prvrnts_fec_proceso_point
		, prvrnts.fec_envio_point				as prvrnts_fec_envio_point
		, prvrnts.nid_poliza_point				as prvrnts_nid_poliza_point
		, prvrnts.vlr_prima_unica_bono			as prvrnts_vlr_prima_unica_bono
		, prvrnts.vlr_prima_unica_suma			as prvrnts_vlr_prima_unica_suma
		, prvrnts.vlr_prima_unica_garantia		as prvrnts_vlr_prima_unica_garantia
		, prvrnts.vlr_pagado_bono				as prvrnts_vlr_pagado_bono
		, prvrnts.vlr_pagado_suma				as prvrnts_vlr_pagado_suma
		, prvrnts.vlr_pagado_garantia			as prvrnts_vlr_pagado_garantia
		, prvrnts.vlr_capital_var				as prvrnts_vlr_capital_var
		, prvrnts.nid_simulacion				as prvrnts_nid_simulacion
		, prvrnts.vlr_acumulado_pagos			as prvrnts_vlr_acumulado_pagos
		, prvrnts.vlr_prima_cuenta_individual	as prvrnts_vlr_prima_cuenta_individual
		, prvrnts.vlr_prima_unica_otros_aportes	as prvrnts_vlr_prima_unica_otros_aportes
		, prvrnts.nid_recuperacion				as prvrnts_nid_recuperacion
		, prvrnts.fec_invalidacion_registro		as prvrnts_fec_invalidacion_registro
		, prvrnts.fec_acumulado_pagos			as prvrnts_fec_acumulado_pagos
		, prvrnts.nid_renta_migracion			as prvrnts_nid_renta_migracion
		, prvrnts.cod_modalidad_renta			as prvrnts_cod_modalidad_renta
		, prvrnts.cod_clase_renta				as prvrnts_cod_clase_renta
		, prvrnts.cod_categoria_renta			as prvrnts_cod_categoria_renta
		, prvrnts.resultado_ejecucion_calculo	as prvrnts_resultado_ejecucion_calculo
		, prvrnts.num_reliquidacion				as prvrnts_num_reliquidacion
		, prvrnts.cod_causa_origen_comun		as prvrnts_cod_causa_origen_comun
		, prvrnts.fec_estructuracion_invalidez	as prvrnts_fec_estructuracion_invalidez
		, prvrnts.bln_tiene_derecho				as prvrnts_bln_tiene_derecho
		, prvrnts.cod_causa_retiro_super		as prvrnts_cod_causa_retiro_super
		, prvrnts.fec_rehabilitacion			as prvrnts_fec_rehabilitacion
		, prvrnts.num_resolucion				as prvrnts_num_resolucion
		, prvrnts.fec_resolucion				as prvrnts_fec_resolucion
		, prvrnts.num_mesadas					as prvrnts_num_mesadas
		, prvrnts.pcj_interes_reserva			as prvrnts_pcj_interes_reserva
		, prvrnts.pcj_interes_tecnico			as prvrnts_pcj_interes_tecnico
		, prvrnts.usuario_sincronizacion		as prvrnts_usuario_sincronizacion
		, prvrnts.usuario_creacion				as prvrnts_usuario_creacion
		, prvrnts.fec_reconocimiento_pension	as prvrnts_fec_reconocimiento_pension
		, prvrnts.fec_docsoptejud				as prvrnts_fec_docsoptejud
		, prvrnts.observaciones_fallojud		as prvrnts_observaciones_fallojud
		, prvrnts.pt_tk							as prvrnts_pt_tk
		, prvrnts.pc_j12						as prvrnts_pc_j12
		, prvrnts.vlr_exceso_smmlv_ipc			as prvrnts_vlr_exceso_smmlv_ipc
		, prvrnts.vlr_exceso_smmlv_ipc_exp		as prvrnts_vlr_exceso_smmlv_ipc_exp
		, prvrnts.pc_ad							as prvrnts_pc_ad
		, prvrnts.pc_ad_exp						as prvrnts_pc_ad_exp
		, prvrnts.cod_factor_seguridad			as prvrnts_cod_factor_seguridad
		, prvrnts.vlr_gastos_exp_fijos			as prvrnts_vlr_gastos_exp_fijos
		, prvrnts.vlr_gastos_adm_fijos			as prvrnts_vlr_gastos_adm_fijos
		, prvrnts.pcj_gastos_exp				as prvrnts_pcj_gastos_exp
		, prvrnts.pcj_administracion			as prvrnts_pcj_administracion
		, prvrnts.cod_origen_sin				as prvrnts_cod_origen_sin
		, prvrnts.fec_causacion					as prvrnts_fec_causacion
		, prvrnts.cod_origen_ini				as prvrnts_cod_origen_ini
		, prvrnts.fec_ultima_mod				as prvrnts_fec_ultima_mod
		, prvrnts.pcj_pcl_inicial				as prvrnts_pcj_pcl_inicial
		, prvrnts.pcj_pcl_actual				as prvrnts_pcj_pcl_actual			
	from prep_rentas_rentas_4 anpmvmntspg
	inner join prv.prv_rentas prvrnts
	on anpmvmntspg.anppnsns_nid_procedencia = prvrnts.nid_rentas
	and prvrnts.fec_expedicion between anpmvmntspg.fecha_inicio_periodo_pago_renta and anpmvmntspg.fecha_fin_periodo_pago_renta  -- Ajuste
), prep_rentas_rentas_6 as (
	select
		  anpmvmntspg.anpmvmntspg_periodo_pago
		, anpmvmntspg.anpmvmntspg_nid_distribucion_vigencia
		, anpmvmntspg.anpmvmntspg_cod_tipo_identificacion
		, anpmvmntspg.anpmvmntspg_nid_numero_identificacion
		, anpmvmntspg.anpmvmntspg_cod_concepto
		, anpmvmntspg.anpmvmntspg_vlr_concepto
		, anpmvmntspg.anpmvmntspg_fec_pago
		, anpmvmntspg.anpmvmntspg_fec_inicial
		, anpmvmntspg.anpmvmntspg_fec_final
		, anpmvmntspg.anpmvmntspg_num_dias_liq_concepto
		, anpmvmntspg.anpmvmntspg_cod_tipo_identificacion_tutor
		, anpmvmntspg.anpmvmntspg_nid_numero_identificacion_tuto
		, anpmvmntspg.anpmvmntspg_cod_sucursal_giro
		, anpmvmntspg.anpmvmntspg_fec_generacion_nomina
		, anpmvmntspg.anpmvmntspg_nid_numero_solicitud_giro
		, anpmvmntspg.anpmvmntspg_cod_forma_pago
		, anpmvmntspg.anpmvmntspg_vlr_base
		, anpmvmntspg.anpmvmntspg_nid_consecutivo_tutor
		, anpmvmntspg.anpmvmntspg_cod_miscelaneo
		, anpmvmntspg.anpmvmntspg_bln_valor_upc
		, anpmvmntspg.anpmvmntspg_fec_inhabilitacion
		, anpmvmntspg.anpmvmntspg_comentario_inhabilitacion
		, anpmvmntspg.anpmvmntspg_bln_rechazado_occired
		, anpmvmntspg.anpmvmntspg_fec_habilitacion
		, anpmvmntspg.anpmvmntspg_usuario_habilitacion
		, anpmvmntspg.anpmvmntspg_cod_estado_movimiento
		, anpmvmntspg.anpmvmntspg_idn_pago_masivo
		, anpmvmntspg.anpmvmntspg_fec_modificacion
		, anpmvmntspg.fecha_inicio_periodo_pago_renta
		, anpmvmntspg.fecha_fin_periodo_pago_renta
		, anpmvmntspg.anpdstrbcnvgpnsn_nid_distribucion_vigencia
		, anpmvmntspg.anpdstrbcnvgpnsn_nid_version_pension
		, anpmvmntspg.anpdstrbcnvgpnsn_nid_pension
		, anpmvmntspg.anpdstrbcnvgpnsn_fec_inicial
		, anpmvmntspg.anpdstrbcnvgpnsn_fec_final
		, anpmvmntspg.anpdstrbcnvgpnsn_cod_razon_cambio
		, anpmvmntspg.anpdstrbcnvgpnsn_comentarios
		, anpmvmntspg.anpdstrbcnvgpnsn_usuario_creacion
		, anpmvmntspg.anppnsns_nid_pension
		, anpmvmntspg.anppnsns_nid_procedencia
		, anpmvmntspg.anppnsns_vlr_pension_inicial
		, anpmvmntspg.anppnsns_vlr_pension_final
		, anpmvmntspg.anppnsns_cod_procedencia
		, anpmvmntspg.anppnsns_cod_origen
		, anpmvmntspg.anppnsns_bln_definitivo
		, anpmvmntspg.anppnsns_cod_tipo_identificacion
		, anpmvmntspg.anppnsns_nid_numero_identificacion
		, anpmvmntspg.anppnsns_cod_sucursal_origen
		, anpmvmntspg.anppnsns_nid_siniestro
		, anpmvmntspg.anppnsns_fec_expedicion
		, anpmvmntspg.anppnsns_num_mesadas			
		, anpmvmntspg.prvrnts_nid_rentas
		, anpmvmntspg.prvrnts_fec_expedicion
		, anpmvmntspg.prvrnts_fec_vigencia_inicial
		, anpmvmntspg.prvrnts_vlr_salario_base
		, anpmvmntspg.prvrnts_vlr_prima_unica
		, anpmvmntspg.prvrnts_vlr_cuenta_individual
		, anpmvmntspg.prvrnts_vlr_bono_pensional
		, anpmvmntspg.prvrnts_vlr_pension_inicial
		, anpmvmntspg.prvrnts_vlr_pension_actual
		, anpmvmntspg.prvrnts_fec_ult_incremento_pension
		, anpmvmntspg.prvrnts_pcj_ult_incremento_pension
		, anpmvmntspg.prvrnts_vlr_indemnizacion_seguro
		, anpmvmntspg.prvrnts_vlr_otros_aportes
		, anpmvmntspg.prvrnts_fec_pago
		, anpmvmntspg.prvrnts_lugar_pago
		, anpmvmntspg.prvrnts_simbolo_point
		, anpmvmntspg.prvrnts_ramo_point
		, anpmvmntspg.prvrnts_modulo_point
		, anpmvmntspg.prvrnts_endoso_point
		, anpmvmntspg.prvrnts_edo
		, anpmvmntspg.prvrnts_bln_poliza_emitida
		, anpmvmntspg.prvrnts_bln_actualizado_point
		, anpmvmntspg.prvrnts_cod_departamento
		, anpmvmntspg.prvrnts_cod_ciudad
		, anpmvmntspg.prvrnts_cod_origen
		, anpmvmntspg.prvrnts_cod_grado_invalidez
		, anpmvmntspg.prvrnts_cod_opcion_calculo
		, anpmvmntspg.prvrnts_nid_siniestro
		, anpmvmntspg.prvrnts_nid_cotizacion
		, anpmvmntspg.prvrnts_cod_sucursal_alfa
		, anpmvmntspg.prvrnts_cod_compania
		, anpmvmntspg.prvrnts_cod_tipo_identificacion
		, anpmvmntspg.prvrnts_nid_numero_identificacion
		, anpmvmntspg.prvrnts_nid_afiliado
		, anpmvmntspg.prvrnts_cod_afp
		, anpmvmntspg.prvrnts_num_dias_trabajados
		, anpmvmntspg.prvrnts_lugar_expedicion
		, anpmvmntspg.prvrnts_cod_estado_persona
		, anpmvmntspg.prvrnts_pcj_pension
		, anpmvmntspg.prvrnts_num_endoso_ult
		, anpmvmntspg.prvrnts_cod_tipo_operacion
		, anpmvmntspg.prvrnts_bln_procesado_point
		, anpmvmntspg.prvrnts_fec_proceso_point
		, anpmvmntspg.prvrnts_fec_envio_point
		, anpmvmntspg.prvrnts_nid_poliza_point
		, anpmvmntspg.prvrnts_vlr_prima_unica_bono
		, anpmvmntspg.prvrnts_vlr_prima_unica_suma
		, anpmvmntspg.prvrnts_vlr_prima_unica_garantia
		, anpmvmntspg.prvrnts_vlr_pagado_bono
		, anpmvmntspg.prvrnts_vlr_pagado_suma
		, anpmvmntspg.prvrnts_vlr_pagado_garantia
		, anpmvmntspg.prvrnts_vlr_capital_var
		, anpmvmntspg.prvrnts_nid_simulacion
		, anpmvmntspg.prvrnts_vlr_acumulado_pagos
		, anpmvmntspg.prvrnts_vlr_prima_cuenta_individual
		, anpmvmntspg.prvrnts_vlr_prima_unica_otros_aportes
		, anpmvmntspg.prvrnts_nid_recuperacion
		, anpmvmntspg.prvrnts_fec_invalidacion_registro
		, anpmvmntspg.prvrnts_fec_acumulado_pagos
		, anpmvmntspg.prvrnts_nid_renta_migracion
		, anpmvmntspg.prvrnts_cod_modalidad_renta
		, anpmvmntspg.prvrnts_cod_clase_renta
		, anpmvmntspg.prvrnts_cod_categoria_renta
		, anpmvmntspg.prvrnts_resultado_ejecucion_calculo
		, anpmvmntspg.prvrnts_num_reliquidacion
		, anpmvmntspg.prvrnts_cod_causa_origen_comun
		, anpmvmntspg.prvrnts_fec_estructuracion_invalidez
		, anpmvmntspg.prvrnts_bln_tiene_derecho
		, anpmvmntspg.prvrnts_cod_causa_retiro_super
		, anpmvmntspg.prvrnts_fec_rehabilitacion
		, anpmvmntspg.prvrnts_num_resolucion
		, anpmvmntspg.prvrnts_fec_resolucion
		, anpmvmntspg.prvrnts_num_mesadas
		, anpmvmntspg.prvrnts_pcj_interes_reserva
		, anpmvmntspg.prvrnts_pcj_interes_tecnico
		, anpmvmntspg.prvrnts_usuario_sincronizacion
		, anpmvmntspg.prvrnts_usuario_creacion
		, anpmvmntspg.prvrnts_fec_reconocimiento_pension
		, anpmvmntspg.prvrnts_fec_docsoptejud
		, anpmvmntspg.prvrnts_observaciones_fallojud
		, anpmvmntspg.prvrnts_pt_tk
		, anpmvmntspg.prvrnts_pc_j12
		, anpmvmntspg.prvrnts_vlr_exceso_smmlv_ipc
		, anpmvmntspg.prvrnts_vlr_exceso_smmlv_ipc_exp
		, anpmvmntspg.prvrnts_pc_ad
		, anpmvmntspg.prvrnts_pc_ad_exp
		, anpmvmntspg.prvrnts_cod_factor_seguridad
		, anpmvmntspg.prvrnts_vlr_gastos_exp_fijos
		, anpmvmntspg.prvrnts_vlr_gastos_adm_fijos
		, anpmvmntspg.prvrnts_pcj_gastos_exp
		, anpmvmntspg.prvrnts_pcj_administracion
		, anpmvmntspg.prvrnts_cod_origen_sin
		, anpmvmntspg.prvrnts_fec_causacion
		, anpmvmntspg.prvrnts_cod_origen_ini
		, anpmvmntspg.prvrnts_fec_ultima_mod
		, anpmvmntspg.prvrnts_pcj_pcl_inicial
		, anpmvmntspg.prvrnts_pcj_pcl_actual		
		, prvsldsmtmtc.nid_rentas					as prvsldsmtmtc_nid_rentas
		, prvsldsmtmtc.nid_calculo					as prvsldsmtmtc_nid_calculo
		, prvsldsmtmtc.max							as prvsldsmtmtc_max
		, prvsldsmtmtc.vlr_reserva_calculada		as prvsldsmtmtc_vlr_reserva_calculada
		, prvsldsmtmtc.periodo						as prvsldsmtmtc_periodo
		, prvsldsmtmtc.periodo_ingreso				as prvsldsmtmtc_periodo_ingreso
		, prvsldsmtmtc.vlr_pension_real				as prvsldsmtmtc_vlr_pension_real
		, prvsldsmtmtc.fec_corte_calculo			as prvsldsmtmtc_fec_corte_calculo
		, prvsldsmtmtc.vlr_reserva_contable			as prvsldsmtmtc_vlr_reserva_contable
		, prvsldsmtmtc.vlr_diferencia_resolucion	as prvsldsmtmtc_vlr_diferencia_resolucion
		, prvsldsmtmtc.bln_caso_transicion			as prvsldsmtmtc_bln_caso_transicion
		, prvsldsmtmtc.mes_diferido_transicion		as prvsldsmtmtc_mes_diferido_transicion
		, prvsldsmtmtc.pcj_aplicado_transicion		as prvsldsmtmtc_pcj_aplicado_transicion
		, cgrrgns.cod_origen						as cgrrgns_cod_origen
		, cgrrgns.nombre							as cgrrgns_nombre
		, cgrrgns.abreviatura						as cgrrgns_abreviatura
		, cgrrgns.cod_origen_super					as cgrrgns_cod_origen_super
		, cgrcncpts.cod_concepto					as cgrcncpts_cod_concepto
		, cgrcncpts.nombre_concepto					as cgrcncpts_nombre_concepto
		, cgrcncpts.cod_tipo_concepto				as cgrcncpts_cod_tipo_concepto
		, cgrcncpts.bln_predeterminada				as cgrcncpts_bln_predeterminada
		, cgrcncpts.bln_informativa					as cgrcncpts_bln_informativa
		, cgrcncpts.vlr_fijo						as cgrcncpts_vlr_fijo
		, cgrcncpts.pcj_pension						as cgrcncpts_pcj_pension
		, cgrcncpts.bln_agrupar_nit					as cgrcncpts_bln_agrupar_nit
		, cgrcncpts.cod_aproximacion_valores		as cgrcncpts_cod_aproximacion_valores
		, cgrcncpts.bln_porcentaje					as cgrcncpts_bln_porcentaje
		, cgrcncpts.bln_valor_fijo					as cgrcncpts_bln_valor_fijo
		, cgrcncpts.bln_valor_total					as cgrcncpts_bln_valor_total
		, cgrcncpts.bln_valor_base					as cgrcncpts_bln_valor_base
		, cgrcncpts.cod_aproximacion_base			as cgrcncpts_cod_aproximacion_base
		, cgrcncpts.pcj_vlr_base					as cgrcncpts_pcj_vlr_base
		, cgrcncpts.bln_neto						as cgrcncpts_bln_neto
		, cgrcncpts.bln_ley							as cgrcncpts_bln_ley
		, cgrcncpts.bln_prima						as cgrcncpts_bln_prima
	from prep_rentas_rentas_5 anpmvmntspg
	inner join prv.prv_saldos_matematica prvsldsmtmtc
	on anpmvmntspg.prvrnts_nid_rentas = prvsldsmtmtc.nid_rentas
	and anpmvmntspg.fecha_fin_periodo_pago_renta >= prvsldsmtmtc.fec_corte_calculo
	inner join general.cgr_origenes cgrrgns
	on anpmvmntspg.prvrnts_cod_origen = cgrrgns.cod_origen
	inner join general.cgr_conceptos cgrcncpts
	on anpmvmntspg.anpmvmntspg_cod_concepto = cgrcncpts.cod_concepto
), prep_rentas_rentas_7 as (
	select	
		  anpmvmntspg.prvrnts_nid_rentas
		, max(anpmvmntspg.prvsldsmtmtc_nid_calculo) 		as prvsldsmtmtc_nid_calculo
		, max(anpmvmntspg.prvsldsmtmtc_fec_corte_calculo) 	as prvsldsmtmtc_fec_corte_calculo -- ojo con este el nid_calculo es la primaria lo ideal es volver a cruzar con silver para que saque este atributo desde la tabla adecuada
		, anpmvmntspg.prvrnts_fec_expedicion
		, anpmvmntspg.prvrnts_fec_invalidacion_registro
		, anpmvmntspg.prvrnts_fec_envio_point
		, anpmvmntspg.prvrnts_ramo_point
		, anpmvmntspg.prvrnts_cod_origen
		, anpmvmntspg.cgrrgns_nombre
		, anpmvmntspg.prvrnts_vlr_prima_unica
		, anpmvmntspg.anpmvmntspg_cod_concepto
		, anpmvmntspg.cgrcncpts_nombre_concepto
		, anpmvmntspg.anpmvmntspg_periodo_pago
		, anpmvmntspg.fecha_inicio_periodo_pago_renta
		, anpmvmntspg.fecha_fin_periodo_pago_renta
	from prep_rentas_rentas_6 anpmvmntspg
	group by anpmvmntspg.prvrnts_nid_rentas
		, anpmvmntspg.prvrnts_fec_expedicion
		, anpmvmntspg.prvrnts_fec_invalidacion_registro
		, anpmvmntspg.prvrnts_fec_envio_point
		, anpmvmntspg.prvrnts_ramo_point
		, anpmvmntspg.prvrnts_cod_origen
		, anpmvmntspg.cgrrgns_nombre
		, anpmvmntspg.prvrnts_vlr_prima_unica
		, anpmvmntspg.anpmvmntspg_cod_concepto
		, anpmvmntspg.cgrcncpts_nombre_concepto
		, anpmvmntspg.anpmvmntspg_periodo_pago
		, anpmvmntspg.fecha_inicio_periodo_pago_renta
		, anpmvmntspg.fecha_fin_periodo_pago_renta
), prep_rentas_rentas_8 as (
	select	
		  anpmvmntspg.prvrnts_nid_rentas									as nid_renta
		, anpmvmntspg.prvsldsmtmtc_nid_calculo								as nid_calculo
		, anpmvmntspg.prvsldsmtmtc_fec_corte_calculo						as fec_calculo
		, to_char(anpmvmntspg.prvsldsmtmtc_fec_corte_calculo, 'yyyymm') 	as per_calculo
		, anpmvmntspg.prvrnts_fec_expedicion								as fec_expedicion_renta
		, to_char(anpmvmntspg.prvrnts_fec_expedicion, 'yyyymm') 			as per_expedicion_renta
		, anpmvmntspg.prvrnts_fec_invalidacion_registro						as fec_invalidacion_registro
		, to_char(anpmvmntspg.prvrnts_fec_invalidacion_registro, 'yyyymm')	as per_invalidacion_registro
		, anpmvmntspg.prvrnts_fec_envio_point								as fec_point_renta
		, to_char(anpmvmntspg.prvrnts_fec_envio_point, 'yyyymm')			as per_point_renta
		, anpmvmntspg.prvrnts_ramo_point									as ramo
		, anpmvmntspg.prvrnts_cod_origen									as cod_tipo_renta
		, anpmvmntspg.cgrrgns_nombre										as dsc_tipo_renta
		, anpmvmntspg.prvrnts_vlr_prima_unica								as vlr_renta
		, anpmvmntspg.anpmvmntspg_cod_concepto								as cod_concepto_pago_renta
		, anpmvmntspg.cgrcncpts_nombre_concepto								as dsc_concepto_pago_renta
		, anpmvmntspg.anpmvmntspg_periodo_pago								as per_pago_renta
		, anpmvmntspg.fecha_inicio_periodo_pago_renta						as fec_ini_per_pago_renta
		, anpmvmntspg.fecha_fin_periodo_pago_renta							as fec_fin_per_pago_renta
		, (case
				when anpmvmntspg.prvrnts_fec_invalidacion_registro is not null then 1
				else 0
			end)															as val_reg_invalido_null
	from prep_rentas_rentas_7 anpmvmntspg
	where anpmvmntspg.anpmvmntspg_cod_concepto = 1
), prep_rentas_rentas_9 as (
	select
		  anpmvmntspg.nid_renta
		, anpmvmntspg.nid_calculo
		, anpmvmntspg.fec_calculo
		, anpmvmntspg.per_calculo
		, anpmvmntspg.fec_expedicion_renta
		, anpmvmntspg.per_expedicion_renta
		, anpmvmntspg.fec_invalidacion_registro
		, anpmvmntspg.per_invalidacion_registro
		, anpmvmntspg.fec_point_renta
		, anpmvmntspg.per_point_renta
		, anpmvmntspg.ramo
		, anpmvmntspg.cod_tipo_renta
		, anpmvmntspg.dsc_tipo_renta
		, anpmvmntspg.vlr_renta
		, anpmvmntspg.cod_concepto_pago_renta
		, anpmvmntspg.dsc_concepto_pago_renta
		, anpmvmntspg.per_pago_renta
		, anpmvmntspg.fec_ini_per_pago_renta
		, anpmvmntspg.fec_fin_per_pago_renta
		, anpmvmntspg.val_reg_invalido_null
		, (case
				when anpmvmntspg.fec_invalidacion_registro < anpmvmntspg.fec_ini_per_pago_renta and anpmvmntspg.val_reg_invalido_null = 1 then 1
				else 0
			end) as val_reg_invalido
	from prep_rentas_rentas_8 anpmvmntspg
)
select
	  nid_renta
	, nid_calculo
	, fec_calculo
	, per_calculo
	, fec_expedicion_renta
	, per_expedicion_renta
	, fec_invalidacion_registro
	, per_invalidacion_registro
	, fec_point_renta
	, per_point_renta
	, ramo
	, cod_tipo_renta
	, dsc_tipo_renta
	, vlr_renta
	, cod_concepto_pago_renta
	, dsc_concepto_pago_renta
	, per_pago_renta
	, fec_ini_per_pago_renta
	, fec_fin_per_pago_renta
	, val_reg_invalido_null
	, val_reg_invalido
from prep_rentas_rentas_9
order by nid_renta
	, nid_calculo
;