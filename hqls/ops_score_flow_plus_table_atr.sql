set hive.server2.logging.operation.level=NONE;

DROP TABLE IF EXISTS ${DEST_DB}.ctr_comp_score_flow_plus_atr;

CREATE EXTERNAL TABLE ${DEST_DB}.ctr_comp_score_flow_plus_atr (
	infocheck_id string,
	informconf_id string,
	pers_id string,
	tipo_doc_id int,
	inf_conv_cant_abiertas bigint,
	inf_dem_cant_total bigint,
	inf_dem_cant_total_cable bigint,
	inf_dem_cant_total_ccredito bigint,
	inf_dem_cant_total_comercio bigint,
	inf_dem_cant_total_sf bigint,
	inf_dem_cant_total_spub bigint,
	inf_dem_cant_total_telco bigint,
	inf_inc_cant_abiertas bigint,
	inf_inhib_cant_abiertas bigint,
	inf_mor_cant_total bigint,
	inf_mor_cant_total_cable bigint,
	inf_mor_cant_total_ccredito bigint,
	inf_mor_cant_total_sf bigint,
	inf_mor_cant_total_spub bigint,
	inf_mor_cant_total_telco bigint,
	inf_qui_cant_abiertas bigint,
	inf_rem_cant_abiertas bigint,
	scorecard int,
	ich_ch_re_cant_i_fond bigint,
	ich_cons_cant_total bigint,
	ich_cons_cant_banco_admin bigint,
	distrito string,
	edad int,
	estado_civil int,
	ich_fecha_ult_cons_priv string,
	ips_flag_actividad int,
	max_flow string,
	g_max_venc_ten_36 double,
	inf_cant_dist_afi_u6m bigint,
	inf_cant_dist_afi_ccred_u3m bigint,
	inf_cant_dist_afi_telco_u12m bigint,
	inf_cons_cant_telco_u1m bigint,
	inf_cons_cant_total_telco bigint,
	inf_cons_cant_u1m bigint,
	inf_cons_ult_dias_ccredito int,
	inf_cons_ult_dias_sf int,
	inf_morce_cant_31_45_abiertas bigint,
	inf_morce_cant_46_60_abiertas bigint,
	inf_morce_cant_61_90_abiertas bigint,
	inf_morce_cant_abiertas bigint,
	p_cant_atraso bigint,
	p_saldo_vencido_ult_act bigint,
	p_sdot_monto_gt75_m1 bigint,
	situacion_bancaria int,
	tc_close_total bigint,
	tc_max_dias_atraso_u6m string,
	tc_primera_act int,
	tc_prom_pmin_limcr_m1 double,
	tc_svenc_sdot_gt0_u12m bigint,
	fecha_obs string,
	score_pos int,
	flow_pos string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED as textfile
LOCATION
	'${LOCATION}'
TBLPROPERTIES(
    'skip.header.line.count'='1',
    'serialization.null.format'='',
    'retrieve.charset'='ISO-8859-1',
    'store.charset'='ISO-8859-1');
