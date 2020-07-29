set hive.server2.logging.operation.level=NONE;

INSERT OVERWRITE TABLE ${DEST_DB}.ctr_comp_score_flow_plus PARTITION (archive=${ARCHV})
SELECT 
	infocheck_id,
	informconf_id,
	pers_id,
	tipo_doc_id,
	edad,
	max_flow,
	sexo,
	fecha_obs,
	score_pos,
	flow_pos,
	archv,
	time_stamp
FROM 
	${SOURCE_DB}.ctr_comp_score_flow_plus_atr;
