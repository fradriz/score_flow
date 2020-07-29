set hive.server2.logging.operation.level=NONE;

INSERT OVERWRITE TABLE ${DEST_DB}.ctr_comp_score_flow_plus_atr_weekly PARTITION (archive=${ARCHV})
SELECT  
	default.ptyprotectstr(infocheck_id,"TK_ctr_NUMERIC_LEGAL") as infocheck_id,
	default.ptyprotectstr(informconf_id,"TK_ctr_NUMERIC_LEGAL") as informconf_id,
	default.ptyprotectstr(pers_id,"TK_ctr_ID_LEGAL") as pers_id,
	tipo_doc_id,
	inf_conv_cant_abiertas,
	inf_dem_cant_total,
	inf_dem_cant_total_cable,
	inf_dem_cant_total_ccredito,
	inf_dem_cant_total_comercio,
	inf_dem_cant_total_sf,
	inf_dem_cant_total_spub,
	inf_dem_cant_total_telco,
	inf_inc_cant_abiertas,
	inf_inhib_cant_abiertas,
	inf_mor_cant_total,
	inf_mor_cant_total_cable,
	inf_mor_cant_total_ccredito,
	inf_mor_cant_total_sf,
	inf_mor_cant_total_spub,
	inf_mor_cant_total_telco,
	inf_qui_cant_abiertas,
	inf_rem_cant_abiertas,
	scorecard,
	ich_ch_re_cant_i_fond,
	ich_cons_cant_total,
	ich_cons_cant_banco_admin,
	distrito,
	edad,
	estado_civil,
	sexo,
	ich_fecha_ult_cons_priv,
	ips_flag_actividad,
	max_flow,
	g_max_venc_ten_36,
	inf_cant_dist_afi_u6m,
	inf_cant_dist_afi_ccred_u3m,
	inf_cant_dist_afi_telco_u12m,
	inf_cons_cant_telco_u1m,
	inf_cons_cant_total_telco,
	inf_cons_cant_u1m,
	inf_cons_ult_dias_ccredito,
	inf_cons_ult_dias_sf,
	inf_morce_cant_31_45_abiertas,
	inf_morce_cant_46_60_abiertas,
	inf_morce_cant_61_90_abiertas,
	inf_morce_cant_abiertas,
	p_cant_atraso,
	p_saldo_vencido_ult_act,
	p_sdot_monto_gt75_m1,
	situacion_bancaria,
	tc_close_total,
	tc_max_dias_atraso_u6m,
	tc_primera_act,
	tc_prom_pmin_limcr_m1,
	tc_svenc_sdot_gt0_u12m,
	fecha_obs,
	score_pos,
	flow_pos,
	archv,
	time_stamp
FROM 
	${SOURCE_DB}.ctr_comp_score_flow_plus_atr;