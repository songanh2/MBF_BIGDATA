{
	 "description":"ADME_SERVICE_ACTIVATION_EDR",
	 "sqlStatements": [
	  {
		"sql"      	:"
						SELECT
							SUBSTRING('$day_key',0,6) 								AS MO_KEY,
							B.ACCT_SRVC_INSTANCE_KEY  								AS ACCT_SRVC_INSTANCE_KEY,
							B.ACCT_KEY 												AS ACCT_KEY,
							B.CUST_KEY 												AS CUST_KEY,
							B.BILLABLE_ACCT_KEY 									AS BILLABLE_ACCT_KEY,
							B.CORPORATE_IND 										AS CORPORATE_IND,
							B.NTWK_QOS_GRP_CD 										AS NTWK_QOS_GRP_CD,			
							B.PROD_LINE_KEY 										AS PROD_LINE_KEY,
							B.SERVICE_NBR											AS SERVICE_NBR,
							B.USAGE_PLAN_KEY										AS USAGE_PLAN_KEY,
							B.USAGE_PLAN_CD 										AS USAGE_PLAN_CD,
							B.USAGE_PLAN_TYP_CD 									AS USAGE_PLAN_TYP_CD,
							B.CRM_OFFER_KEY 										AS CRM_OFFER_KEY,
							B.CRM_OFFER_CD											AS CRM_OFFER_CD,
							B.SWITCH_CD												AS SWITCH_CD,
							B.CAF_STAT_CD		 									AS CAF_STAT_CD,
							B.ACQSTN_DT 										    AS ACQSTN_DT,
							B.ACTIVATION_SERVICE_RQST_DT 							AS ACTIVATION_SERVICE_RQST_DT,
							B.ACTIVATION_LAPU_NBR									AS ACTIVATION_LAPU_NBR,
							B.FIRST_VOICE_CALL_DT 									AS FIRST_VOICE_CALL_DT,
							B.ACTIVATION_DT 										AS ACTIVATION_DT,
							B.CBS_ACTIVATION_DT 									AS CBS_ACTIVATION_DT,
							B.LFCYCL_STAT_CD										AS LFCYCL_STAT_CD,
							B.STAT_CD												AS STAT_CD,
							B.EFF_FROM_DT											AS EFF_FROM_DT,
							B.EFF_TO_DT 											AS EFF_TO_DT,
							B.ACQSTN_BSNS_OUTLET_KEY								AS ACQSTN_BSNS_OUTLET_KEY,
							B.ACQSTN_BSNS_OUTLET_CD									AS ACQSTN_BSNS_OUTLET_CD,
							B.ACQSTN_BSNS_OUTLET_TYP_CD								AS ACQSTN_BSNS_OUTLET_TYP_CD,
							B.ACQSTN_CELL_SITE_KEY 									AS ACQSTN_CELL_SITE_KEY,
							B.ACQSTN_SALES_AGENT_ID									AS ACQSTN_SALES_AGENT_ID,
							B.SOURCE_OF_SIGNUP_CD									AS SOURCE_OF_SIGNUP_CD,
							B.KEY_ACCT_MANAGER_ID 									AS KEY_ACCT_MANAGER_ID,
							B.HOME_CELL_KEY 										AS HOME_CELL_KEY,
							B.HOME_CELL_SITE_KEY 									AS HOME_CELL_SITE_KEY,
							CAST(B.MAIN_ACCT_BAL AS DECIMAL(30,6)) 					AS MAIN_ACCT_BAL,
							B.SIM_EFF_FROM_DT 										AS SIM_EFF_FROM_DT,
							B.SIM_BSNS_OUTLET_KEY 								 	AS SIM_BSNS_OUTLET_KEY,
							B.SIM_BSNS_OUTLET_CD 									AS SIM_BSNS_OUTLET_CD,
							B.SIM_BSNS_OUTLET_TYP_CD 								AS SIM_BSNS_OUTLET_TYP_CD,
							B.SIM_TYP_CD											AS SIM_TYP_CD,
							B.SIM_SERIAL_NBR										AS SIM_SERIAL_NBR,
							B.SIM_IMSI 												AS SIM_IMSI,
							B.SIM_GEO_STATE_KEY 									AS SIM_GEO_STATE_KEY,
							B.HNDST_TYP_CD 										 	AS HNDST_TYP_CD,
							B.HNDST_OS_TYP_CD 										AS HNDST_OS_TYP_CD,
							B.HNDST_BRND_KEY 										AS HNDST_BRND_KEY,
							B.HNDST_BRND_CD 										AS HNDST_BRND_CD,
							B.HNDST_MDL_KEY 										AS HNDST_MDL_KEY,
							B.HNDST_MDL_CD 											AS HNDST_MDL_CD,
							B.HNDST_MDL_TYP_CD 										AS HNDST_MDL_TYP_CD,
							B.HNDST_IMEI 											AS HNDST_IMEI,
							B.HNDST_TAC_CD  										AS HNDST_TAC_CD,
							B.HNDST_FAC_CD 											AS HNDST_FAC_CD,
							B.MNP_IND												AS MNP_IND,
							B.MNP_MIGRATION_DT 										AS MNP_MIGRATION_DT,
							B.MNP_MIGRATION_BACK_DT 								AS MNP_MIGRATION_BACK_DT,
							B.MNP_EXTRNL_CSP_KEY  									AS MNP_EXTRNL_CSP_KEY,
							B.MNP_EXTRNL_CSP_CD										AS MNP_EXTRNL_CSP_CD,
							B.MNP_CSP_TYP_CD										AS MNP_CSP_TYP_CD,
							B.LAST_ACTIVITY_DT 										AS LAST_ACTIVITY_DT,
							B.LAST_ACTIVITY_CELL_KEY 								AS LAST_ACTIVITY_CELL_KEY,
							B.LAST_ACTIVITY_CELL_SITE_KEY 							AS LAST_ACTIVITY_CELL_SITE_KEY,
							B.LAST_ACTIVITY_GEO_STATE_KEY							AS LAST_ACTIVITY_GEO_STATE_KEY,
							B.LAST_ACTIVITY_GEO_CITY_KEY 							AS LAST_ACTIVITY_GEO_CITY_KEY,
							B.LAST_ACTIVITY_GEO_DSTRCT_KEY 							AS LAST_ACTIVITY_GEO_DSTRCT_KEY,
							B.LAST_DATA_ACTIVITY_DT 								AS LAST_DATA_ACTIVITY_DT,
							B.LAST_DATA_IMEI			 							AS LAST_DATA_IMEI,
							B.LAST_VLR_SWITCH_CD 									AS VLR_SWITCH_CD,
							B.LAST_VLR_STAT_CD 										AS VLR_STAT_CD,
							B.LAST_VLR_ATTACHED_DT 									AS VLR_ATTACHED_DT,
							B.LAST_VLR_LATEST_ACCESS_TIME 							AS VLR_LATEST_ACCESS_TIME,
							B.LAST_VLR_IMEI 										AS VLR_IMEI,
							B.LAST_VLR_IMSI 						 				AS VLR_IMSI,
							B.LAST_VLR_CELL_KEY 									AS VLR_CELL_KEY,
							B.LAST_VLR_CELL_SITE_KEY 								AS VLR_CELL_SITE_KEY,
							B.LAST_VLR_GEO_STATE_KEY 								AS VLR_GEO_STATE_KEY,
							B.LAST_VLR_GEO_CITY_KEY 								AS VLR_GEO_CITY_KEY,
							B.LAST_VLR_GEO_DSTRCT_KEY 								AS VLR_GEO_DSTRCT_KEY,
							B.CREDIT_BALANCE_AMT 									AS CREDIT_BALANCE_AMT,
							B.FIRST_RCHRG_DT 										AS FIRST_RCHRG_DT,
							B.FIRST_RCHRG_AMT 										AS FIRST_RCHRG_AMT,
							B.FIRST_RCHRG_TYP 										AS FIRST_RCHRG_TYP,
							B.FIRST_RCHRG_OUTLET_KEY 								AS FIRST_RCHRG_OUTLET_KEY,
							CAST(NULL AS BIGINT)									AS SRC_SYS_KEY,
							CAST(NULL AS STRING) 								 	AS SRC_SYS_CD,
							CURRENT_TIMESTAMP() 									AS LOAD_DT,
							CURRENT_TIMESTAMP() 									AS LAST_UPDT_DT,
							CAST(NULL AS STRING) 									AS LAST_UPDT_BY,
							'1' 													AS CURR_IND,
							1 														AS WRHS_ID
						FROM
							MBF_DATALAKE.MC_ACTION_AUDIT A
						LEFT JOIN MBF_BIGDATA.ADMD_SERVICE_TWIN_CLM_ACTVTY B ON
							CAST(CONCAT('1',A.SUB_ID) AS STRING) = CAST(B.ACCT_KEY AS STRING) AND B.DAY_KEY='$day_key'
						WHERE
							A.DAY_KEY = '$day_key'
							AND (A.ACTION_ID IN ('94','84')
								OR (A.ACTION_ID = '00'
									AND A.REASON_ID IN
									('5782',
									 '1560',
									 '1563',
									 '1564',
									 '1565',
									 '2',
									 '3686',
									 '130474',
									 '130554',
									 '5622',
									 '6103',
									 '130497')
									)
								)												
						",
		"tempTable"  : "ADME_SERVICE_ACTIVATION_EDR_tmp",
		"countSourceRecord":"1"
	  }   	  
	]
}