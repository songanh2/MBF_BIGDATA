{
	 "description":"ADME_SIM_DEVICE_ACTIVITY_EDR",
	 "sqlStatements": [
	  {
		"sql"      : "
						SELECT  
								cast (from_unixtime(unix_timestamp(a.day_key,'yyyyMMdd'),'yyyyMM') as INTEGER) as MO_KEY,
								cast(NULL as string)										 as UUID,
								cast(NULL as timestamp) 									 as EIR_RECORD_DT,
								a.msisdn_2 													 as SERVICE_NBR,
								cast(NULL as string) 										 as SERVICE_NBR_VANITY_IND,
								cast(b.ACCT_SRVC_INSTANCE_KEY as decimal(27,0)) 			 as ACCT_SRVC_INSTANCE_KEY,
								cast(b.ACCT_KEY as decimal(27,0)) 							 as ACCT_KEY,
								cast(b.BILLABLE_ACCT_KEY as decimal(27,0)) 					 as BILLABLE_ACCT_KEY,
								cast(b.CUST_KEY as decimal(27,0)) 							 as CUST_KEY,
								b.CUST_TYP_CD,
								b.NTWK_QOS_GRP_CD,
								cast(b.ACTIVATION_DT as timestamp) 							 as ACCT_ACTIVATION_DT,
								cast(b.CBS_ACTIVATION_DT as timestamp) 						 as ACCT_CBS_ACTIVATION_DT,
								b.LFCYCL_STAT_CD 											 as ACCT_LFCYCL_STAT_CD,
								cast(b.ACTIVATION_DT as timestamp) 							 as SRVC_ACTIVATION_DT,
								cast(b.ACTIVATION_DT as timestamp) 							 as SRVC_CBS_ACTIVATION_DT,
								b.LFCYCL_STAT_CD 											 as SRVC_LFCYCL_STAT_CD,
								cast(b.PROD_LINE_KEY as INTEGER) 						 as PROD_LINE_KEY,
								cast(b.USAGE_PLAN_KEY as BIGINT) 					 as USAGE_PLAN_KEY,
								b.USAGE_PLAN_CD,
								cast(NULL as string) 										 as USAGE_PLAN_TYP_CD,
								cast(NULL as string) 										 as SIM_SRL_NBR,
								cast(NULL as string) 										 as SIM_CARD_TYP_CD,
								CASE WHEN a.lte == '1' THEN 'LTE' ELSE 'GSM' END 			 as NTWK_TECH_TYP_CD,
								CASE WHEN a.lte == '1' THEN 'LTE'
								WHEN a.`_3gp` == '1' THEN '3GP'
								WHEN a.`_3g` == '1' THEN '3G'
								ELSE '2G' END 												 as NTWK_TECH_CD,
								cast(a.IMSI as decimal(20,0)) 								 as SIM_IMSI,
								cast(NULL as BIGINT) 								 as SIM_GEO_STATE_KEY,
								cast(NULL as string) 										 as SIM_PUK1,
								cast(NULL as string) 										 as SIM_PUK2,
								cast(a.IMEI as decimal(20,0)) 								 as DEVICE_IMEI,
								CASE WHEN a.device_category == '1' THEN 'Pluggable Card'
								WHEN a.device_category == '2' THEN 'Modem/GSM Gateway'
								WHEN a.device_category == '3' THEN 'M2M equipment'
								WHEN a.device_category == '4' THEN 'Feature phone'
								WHEN a.device_category == '5' THEN 'Smartphone'
								WHEN a.device_category == '6' THEN 'Tablet'
								WHEN a.device_category == '7' THEN 'Basic'
								ELSE 
								CASE
										   WHEN (   LOWER (a.os_name) LIKE '%ios%'
												 OR LOWER (a.os_name) LIKE '%android%'
												 OR LOWER (a.os_name) LIKE '%windows%'
												 OR LOWER (a.os_name) LIKE '%blackberry%'
												 OR LOWER (a.os_name) LIKE '%bada%'
												 OR LOWER (a.os_name) LIKE '%palm%'
												 OR LOWER (a.os_name) LIKE '%meego%'
												 OR LOWER (a.os_name) LIKE '%linux%'
												 OR LOWER (a.os_name) LIKE '%firefox%'
												 OR LOWER (a.os_name) LIKE '%tizen%'
												 OR LOWER (a.os_name) LIKE '%symbian series60%'
												 OR LOWER (a.os_name) LIKE '%symbian belle%'
												 OR LOWER (a.os_name) LIKE '%microsoft%'
												 OR LOWER (a.os_name) LIKE '%apple%'
												 OR LOWER (a.os_name) LIKE '%alibaba%')
										   THEN
											   'Smartphone'
										   ELSE
											   'Basic'
									   END
								END 													 as DEVICE_TYP_CD,
								a.os_name 												 as DEVICE_OS_TYP_CD,
								cast (c.BRAND_KEY as BIGINT) 					 as DEVICE_BRND_KEY,
								a.brand_name 											 as DEVICE_BRND_CD,
								cast (d.HNDST_MDL_KEY as BIGINT) 				 as DEVICE_MDL_KEY,
								a.model_name 											 as DEVICE_MDL_CD,
								d.HNDST_MDL_TYP_CD 										 as DEVICE_MDL_TYP_CD,
								cast(a.tac as BIGINT) 							 as DEVICE_TAC_CD,
								cast(substr(a.tac,-2) as INTEGER) 					 as DEVICE_FAC_CD,
								cast(NULL as string) 									 as DEVICE_PURCHASED_IND,
								cast(a.last_configuration as timestamp) 				 as DEVICE_CONFIG_SYNC_DT,
								cast(NULL as string) 									 as HLR_SWITCH_CD,
								cast(NULL as BIGINT) 							 as HLR_NTWK_MGNT_CENTRE_KEY,
								cast(NULL as string) 									 as HLR_NTWK_MGNT_CENTRE_CD,
								cast(NULL as BIGINT) 							 as HLR_BSNS_RGN_KEY,
								cast(NULL as string) 									 as HLR_BSNS_RGN_CD,
								cast(NULL as BIGINT) 							 as HLR_GEO_STATE_KEY,
								cast(NULL as string) 									 as HLR_GEO_STATE_CD,
								cast(NULL as BIGINT) 							 as HLR_BSNS_CLUSTER_KEY,
								cast(NULL as string) 									 as HLR_BSNS_CLUSTER_CD,
								cast(NULL as string) 									 as VLR_SWITCH_CD,
								cast(NULL as BIGINT) 							 as VLR_NTWK_MGNT_CENTRE_KEY,
								cast(NULL as string) 									 as VLR_NTWK_MGNT_CENTRE_CD,
								cast(NULL as BIGINT) 							 as VLR_BSNS_RGN_KEY,
								cast(NULL as string) 									 as VLR_BSNS_RGN_CD,
								cast(NULL as BIGINT) 							 as VLR_GEO_STATE_KEY,
								cast(NULL as string) 									 as VLR_GEO_STATE_CD,
								cast(NULL as BIGINT) 							 as VLR_BSNS_CLUSTER_KEY,
								cast(NULL as string) 									 as VLR_BSNS_CLUSTER_CD,
								cast(NULL as string) 									 as VLR_ROMING_IND,
								cast(NULL as string) 									 as VLR_INTL_DMSTC_IND,
								cast(NULL as string) 									 as VLR_HOME_COUNTRY_CD,
								cast(NULL as BIGINT) 							 as VLR_EXTRNL_CSP_KEY,
								cast(NULL as string) 									 as VLR_EXTRNL_CSP_CD,
								cast(NULL as string) 									 as VLR_STAT_CD,
								cast(NULL as timestamp) 								 as VLR_ATTACHED_DT,
								cast(NULL as timestamp) 								 as VLR_DETACHED_DT,
								cast(NULL as timestamp) 								 as VLR_DELETED_DT,
								cast(NULL as timestamp) 								 as VLR_LAST_ACCESS_TIME,
								cast(NULL as string) 									 as VLR_LOC_GCI,
								cast(NULL as BIGINT) 							 as VLR_LOC_CELL_KEY,
								cast(NULL as string) 									 as VLR_LOC_CELL_CD,
								cast(NULL as BIGINT) 							 as VLR_LOC_CELL_SITE_KEY,
								cast(NULL as string) 									 as VLR_LOC_CELL_SITE_CD,
								cast(NULL as BIGINT) 							 as VLR_LOC_GEO_STATE_KEY,
								cast(NULL as string) 									 as VLR_LOC_GEO_STATE_CD,
								cast(NULL as BIGINT) 							 as VLR_LOC_GEO_DSTRCT_KEY,
								cast(NULL as string) 									 as VLR_LOC_GEO_DSTRCT_CD,
								cast(NULL as BIGINT) 							 as VLR_LOC_GEO_CITY_KEY,
								cast(NULL as string) 									 as VLR_LOC_GEO_CITY_CD,
								cast(NULL as decimal(20,0)) 							 as LAST_ACTIVITY_CELL_KEY,
								cast(NULL as string) 									 as LAST_ACTIVITY_CELL_CD,
								cast(NULL as decimal(20,0)) 							 as LAST_ACTIVITY_CELL_SITE_KEY,
								cast(NULL as string) 									 as LAST_ACTIVITY_CELL_SITE_CD,
								cast(NULL as BIGINT) 							 as LAST_ACTIVITY_GEO_STATE_KEY,
								cast(NULL as string) 									 as LAST_ACTIVITY_GEO_STATE_CD,
								cast(NULL as decimal(20,0)) 							 as LAST_ACTIVITY_GEO_CITY_KEY,
								cast(NULL as string) 									 as LAST_ACTIVITY_GEO_CITY_CD,
								cast(NULL as decimal(20,0)) 							 as LAST_ACTIVITY_GEO_DSTRCT_KEY,
								cast(NULL as string) 									 as LAST_ACTIVITY_GEO_DSTRCT_CD,
								cast(NULL as timestamp) 								 as LAST_DATA_ACTIVITY_DT,
								cast(NULL as timestamp) 								 as LAST_VOICE_OG_CALL_DT,
								cast(NULL as timestamp) 								 as LAST_VOICE_IC_CALL_DT,
								cast(NULL as timestamp) 								 as LAST_SMS_OG_CALL_DT,
								cast(NULL as timestamp) 								 as LAST_SMS_IC_CALL_DT,
								cast(NULL as string) 									 as CDC_IND,
								cast(NULL as string) 									 as CDC_RSN_CD,
								cast(NULL as string) 									 as FRAUD_RISK_IND,
								cast(NULL as string) 									 as FRAUD_RISK_RSN_CD,
								cast(NULL as string) 									 as STAT_CD,
								cast(NULL as timestamp) 								 as EFF_FROM_DT,
								cast(NULL as timestamp) 								 as EFF_TO_DT,
								CAST(1 AS BIGINT) 										 as SRC_SYS_KEY,
								'ADC' 													 as SRC_SYS_CD,
								CURRENT_TIMESTAMP() 									 as LOAD_DT,
								CURRENT_TIMESTAMP() 									 as LAST_UPDT_DT,
								'SYSTEM'												 as LAST_UPDT_BY,
								'1' 													 as CURR_IND,
								1 														 as WRHS_ID
						FROM 
						(
							SELECT CASE WHEN LENGTH(regexp_replace(regexp_replace(msisdn,'[+\\\\-\\\\(\\\\)\\\\s]', ''),'^0?','')) == 9 AND INSTR(SUBSTR((regexp_replace(regexp_replace(msisdn,'[+\\\\-\\\\(\\\\)\\\\s]', ''),'^0?','')),0,1),'8') == 1
							THEN regexp_replace(regexp_replace(msisdn,'[+\\\\-\\\\(\\\\)\\\\s]', ''),'^0?','') ELSE 
							regexp_replace(regexp_replace(regexp_replace(msisdn,'[+\\\\-\\\\(\\\\)\\\\s]', ''),'^0?',''),'^(84)?','') END as msisdn_2,* 
							FROM mbf_datalake.adc_daily 
							WHERE day_key = $day_key
						) a 
						LEFT JOIN
						(SELECT * FROM mbf_bigdata.admr_account_service WHERE day_key = $day_key
						) b ON a.msisdn_2 = b.SERVICE_NBR 
						LEFT JOIN
						(SELECT * FROM mbf_bigdata.admr_hndst_brand WHERE day_key = $day_key
						) c on a.brand_name = c.brand_cd 
						LEFT JOIN
						(SELECT * FROM mbf_bigdata.admr_hndst_model WHERE day_key = $day_key
						) d on a.model_name = d.hndst_mdl_cd and a.brand_name = d.brand_cd		       
					",
		"tempTable" : "ADME_SIM_DEVICE_ACTIVITY_EDR_TMP",
		"countSourceRecord" : "1"
	  }
	]
}