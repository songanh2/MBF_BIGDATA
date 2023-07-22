{
	 "description":"ADME_RATED_VAS_EDR",
	 "sqlStatements": [
	  {
		"sql"      : "
					SELECT * FROM
						(
							SELECT 
								ROW_NUMBER() OVER (PARTITION BY C4.CELL_KEY_4G ORDER BY C4.CELL_GCI_CD DESC) AS ROWNUM, C4.* 
							FROM 
							(
								SELECT			
									CASE WHEN CELL_TYP_CD = '4G' THEN CONCAT(SPLIT(CELL_KEY,'[\\-]')[0],'-',SPLIT(CELL_KEY,'[\\-]')[1],'-',SPLIT(CELL_KEY,'[\\-]')[3],'-',SPLIT(CELL_KEY,'[\\-]')[4])
									ELSE CELL_KEY END AS CELL_KEY_4G,
									*
								FROM MBF_BIGDATA.ADMR_CELL WHERE DAY_KEY = '$day_key' AND NVL(CELL_DSCR,'') NOT LIKE '%TEST%'
							) C4
						) X WHERE X.ROWNUM = 1
					",
		"tempTable" : "CELL_4G_STD",
		"countSourceRecord" : "0",
		"description": "STG_CDR_IN_ENRICH enrich du lieu NON_CELL ca tu nguon flexi, cell_4G tren flexi co dang mcc-mnc-nodebid-cell, nen khi join cell_4g vao admr_cell phai bo TAC"
	  },		 
	  {
		"sql"      	:"
			SELECT 
					CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(A.CALL_STA_TIME,'dd/MM/yyyy HH:mm:ss'),'yyyyMM') 	AS INTEGER) 		AS MO_KEY,
					CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(A.CALL_STA_TIME,'dd/MM/yyyy HH:mm:ss'),'HH') 		AS INTEGER) 		AS HOUR_KEY,
					CAST(NULL AS STRING) 																					AS UUID,
					CAST(NULL AS STRING) 																					AS EVT_CD,
					A.STT 																									AS EVT_SEQ_NBR,
					CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(A.CALL_STA_TIME,'dd/MM/yyyy HH:mm:ss')) AS TIMESTAMP)					AS EVT_START_DT,
					CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(A.CALL_END_TIME,'dd/MM/yyyy HH:mm:ss')) AS TIMESTAMP)					AS EVT_END_DT,
					CAST(NULL AS TIMESTAMP) 																				AS BLLG_START_DT,
					A.FTP_FILENAME 																							AS FILE_NAME,
					CAST(NULL AS TIMESTAMP) 																				AS FILE_DT,
					CAST(NULL AS STRING) 																					AS FILE_PREFIX_CD,
					CAST(NULL AS TIMESTAMP) 																				AS REC_PROCESS_DT,
					CAST(NULL AS STRING) 																					AS SWITCH_CD,
					B.ACCT_SRVC_INSTANCE_KEY 																				AS ACCT_SRVC_INSTANCE_KEY,
					CASE WHEN LENGTH(NVL(A.ORG_CALL_ID,'')) <> 0
							THEN
							CASE
								WHEN SUBSTR(A.ORG_CALL_ID, 1, 4) = '0001'
								THEN
									SUBSTR(A.ORG_CALL_ID, 5)
								ELSE
									CAST(CAST(A.ORG_CALL_ID AS INT) AS STRING)
							END
						ELSE A.CALLING_ISDN_STD 	
					END																										AS SERVICE_NBR,
					B.ACCT_KEY 																								AS ACCT_KEY,
					B.BILLABLE_ACCT_KEY 																					AS BILLABLE_ACCT_KEY,
					B.CUST_KEY 																								AS CUST_KEY,
					B.CUST_TYP_CD 																							AS CUST_TYP_CD,
					B.NTWK_QOS_GRP_CD 																						AS NTWK_QOS_GRP_CD,
					B.ACTIVATION_DT 																						AS ACCT_ACTIVATION_DT,
					B.CBS_ACTIVATION_DT 																					AS ACCT_CBS_ACTIVATION_DT,
					B.LFCYCL_STAT_CD 																						AS ACCT_LFCYCL_STAT_CD,
					B.ACTIVATION_DT 																						AS SRVC_ACTIVATION_DT,
					B.CBS_ACTIVATION_DT 																					AS SRVC_CBS_ACTIVATION_DT,
					B.LFCYCL_STAT_CD 																						AS SRVC_LFCYCL_STAT_CD,
					B.PROD_LINE_KEY 																						AS PROD_LINE_KEY,
					B.USAGE_PLAN_KEY 																						AS USAGE_PLAN_KEY,
					B.USAGE_PLAN_CD 																						AS USAGE_PLAN_CD,
					B.USAGE_PLAN_TYP_CD 																					AS USAGE_PLAN_TYP_CD,
					B.CRM_OFFER_KEY 																						AS OFFER_KEY,
					B.CRM_OFFER_CD 																							AS OFFER_CD,
					CAST(NULL AS DECIMAL(20,0))																				AS PROD_KEY,
					CAST(NULL AS STRING)																					AS PROD_CD,
					A.ITEM_ID																								AS ITEM_KEY,
					A.SUB_ITEM_ID																							AS SUB_ITEM_CD,				
					'VAS' 																									AS EVT_CLASS_CD,
					NVL(A.EVT_CTGRY_CD,'UNKNOWN') 																			AS EVT_CTGRY_CD,
					NVL(A.CALL_TYP_CD,'UNKNOWN')  																			AS EVT_TYP_CD,
					'VAS' 																									AS USAGE_TYP_CD,
					NVL(A.CALL_CTGRY_CD,'UNKNOWN') 																			AS CALL_CTGRY_CD,
					NVL(A.CALL_TYP_CD,'UNKNOWN')  																			AS CALL_TYP_CD,
					CAST(NULL AS STRING)																					AS EVT_STAT_CD,
					CAST(NULL AS STRING)																					AS EVT_TMNT_RSN_CD,
					CAST(NULL AS STRING)																					AS BLLG_STAT_CD,
					CAST(NULL AS STRING)																					AS BLLG_STAT_RSN_CD,
					CAST(NULL AS BIGINT)																					AS BLLG_OPRTR_KEY,
					CAST(NULL AS STRING)																					AS BLLG_OPRTR_CD,
					CAST(NULL AS DECIMAL(30,0))																				AS CONTENT_KEY,
					CAST(NULL AS STRING)																					AS CONTENT_CD,
					CAST(NULL AS STRING)																					AS CONTENT_TYP_CD,
					CAST(NULL AS STRING)																					AS CONTENT_CATEGORY_CD,
					CAST(NULL AS STRING)																					AS MEDIA_TYP_CD,
					CAST(NULL AS STRING)																					AS CONTENT_NAME,
					CAST(NULL AS BIGINT)																					AS CONTENT_PROVIDER_ORG_KEY,
					CAST(NULL AS STRING)																					AS CONTENT_PROVIDER_ORG_CD,
					CAST(NULL AS STRING)																					AS CONTENT_PROVIDER_NAME,
					CAST(NULL AS STRING)																					AS LANG_CD,
					CAST(NULL AS STRING)																					AS SHORT_CD,
					CAST(NULL AS STRING)																					AS CP_ID,
					CAST(NULL AS STRING)																					AS PROMOTION_CD,
					CAST(NULL AS STRING)																					AS MIGRATION_IND,
					CAST(NULL AS DECIMAL(20,0))																				AS PREV_PROD_KEY,
					CAST(NULL AS STRING)																					AS PREV_PROD_CD,
					CAST(NULL AS DECIMAL(30,0))																				AS PREV_CONTENT_KEY,
					CAST(NULL AS STRING)																					AS PREV_CONTENT_CD,
					CAST(NULL AS TIMESTAMP)																					AS LAST_CHARING_DT,
					CAST(NULL AS TIMESTAMP)																					AS NEXT_CHARGING_DT,
					CAST(NULL AS STRING)																					AS CHARGING_REQUEST_INFO,
					CAST(NULL AS STRING)																					AS CHARGING_RESPONSE_INFO,
					CAST(NULL AS STRING)																					AS ACTVTN_RESPONSE_INFO,
					CAST(NULL AS STRING)																					AS ACTVTN_CHANNEL_CD,
					CAST(NULL AS TIMESTAMP)																					AS ACTVTN_DT,
					CAST(NULL AS TIMESTAMP)																					AS FIRST_TIME_ACTVTN_DT,
					CASE WHEN LENGTH(NVL(A.ORG_CALL_ID,'')) <> 0
							THEN
							CASE
								WHEN SUBSTR(A.ORG_CALL_ID, 1, 4) = '0001'
								THEN
									SUBSTR(A.ORG_CALL_ID, 5)
								ELSE
									CAST(CAST(A.ORG_CALL_ID AS INT) AS STRING)
							END
						ELSE A.CALLING_ISDN_STD 	
					END																										AS ORIGNTNG_NBR,
					A.IMSI 																									AS ORIGNTNG_IMSI,
					CAST(NULL AS STRING)																					AS ORIGNTNG_IMEI,
					'0' 																									AS TESTNG_NBR_IND,
					CAST(NULL AS STRING)																					AS ORIGNTNG_PLMN_CD,
					CAST(NULL AS STRING)																					AS ORIGNTNG_NETWORK_TYP,
					SUBSTRING(A.IMSI,1,3) 																					AS ORIGNTNG_HOME_COUNTRY_CD,
					CAST(NULL AS STRING) 																					AS ORIGNTNG_HOME_AREA_CD,
					SUBSTRING(A.IMSI,4,2) 																					AS ORIGNTNG_HOME_NETWORK_CD ,
					(CASE WHEN A.IMSI NOT LIKE '452%' OR A.IMSI NOT LIKE '45201%' THEN SUBSTRING(A.IMSI,1,3) ELSE CAST(NULL AS STRING) END) 	 	AS ORIGNTNG_ROAM_COUNTRY_CD,
					CAST(NULL AS STRING) 		AS ORIGNTNG_ROAM_AREA_CD,
					(CASE WHEN A.IMSI NOT LIKE '452%' OR A.IMSI NOT LIKE '45201%' THEN SUBSTRING(A.IMSI,4,2) ELSE CAST(NULL AS STRING) END)			AS ORIGNTNG_ROAM_NETWORK_CD,
					CAST(NULL AS STRING)																					AS CCN_SCP_REF_CD,
					CAST(NULL AS STRING)																					AS IN_SCP_REF_CD,
					CAST(NULL AS STRING)																					AS ONNET_IND,					
					(CASE WHEN (A.LOCATION_INDICATOR = 2 OR (A.LOCATION LIKE '8491%' OR NVL(A.CELL_ID,'') LIKE '452-02%')) 
						  THEN '1' ELSE CAST(NULL AS STRING) END) 															AS ROAMING_IND,					
					CAST(NULL AS STRING)																					AS SBRP_TYP_CD,
					CAST(NULL AS INTEGER)																					AS SBRP_VLDT_PERIOD_DAYS,
					CAST(NULL AS STRING)																					AS RENEWAL_IND,
					CAST(NULL AS TIMESTAMP)																					AS LAST_RENEWED_DT,
					CAST(NULL AS TIMESTAMP)																					AS NEXT_RENEW_DT,
					CAST(NULL AS TIMESTAMP)																					AS SBRP_START_DT,
					CAST(NULL AS TIMESTAMP)																					AS SBRP_STOP_DT,
					CAST(NULL AS STRING)																					AS SBRP_STAT_CD,
					CAST(NULL AS DECIMAL(27,8))																				AS SBRP_RC_AMT,
					CAST(NULL AS DECIMAL(27,8))																				AS SBRP_NRC_AMT,
					CAST(NULL AS DECIMAL(10,4))																				AS CP_RVN_SHARE_PCT,
					CAST(NULL AS DECIMAL(10,4))																				AS CSP_RVN_SHARE_PCT,
					A.CELL_ID																								AS ORIGNTNG_GCI_CD,
					
					CAST(NVL(C.ADDR_LAT,C4G.ADDR_LAT) AS DECIMAL(27,8))														AS ADDR_LAT,
					CAST(NVL(C.ADDR_LON,C4G.ADDR_LON) AS DECIMAL(27,8))														AS ADDR_LON,
					NVL(C.CELL_KEY,C4G.CELL_KEY) 				 															AS CELL_KEY,
					NVL(C.CELL_CD,C4G.CELL_CD) 				 																AS CELL_CD,
					NVL(C.CELL_SITE_KEY,C4G.CELL_SITE_KEY) 		 															AS CELL_SITE_KEY,
					NVL(C.CELL_SITE_CD,C4G.CELL_SITE_CD)			 														AS CELL_SITE_CD,								
					NVL(C.NTWK_MGNT_CENTRE_KEY,C4G.NTWK_MGNT_CENTRE_KEY) 	 												AS NTWK_MGNT_CENTRE_KEY,
					NVL(C.NTWK_MGNT_CENTRE_CD,C4G.NTWK_MGNT_CENTRE_CD) 	 													AS NTWK_MGNT_CENTRE_CD,
					NVL(C.BSNS_RGN_KEY,C4G.BSNS_RGN_KEY) 			 														AS BSNS_RGN_KEY,
					NVL(C.BSNS_RGN_CD,C4G.BSNS_RGN_CD) 			 															AS BSNS_RGN_CD,
					NVL(C.BSNS_CLUSTER_KEY,C4G.BSNS_CLUSTER_KEY) 		 													AS BSNS_CLUSTER_KEY,
					NVL(C.BSNS_CLUSTER_CD,C4G.BSNS_CLUSTER_CD) 		 														AS BSNS_CLUSTER_CD,
					NVL(C.BSNS_MINICLUSTER_KEY,C4G.BSNS_MINICLUSTER_KEY) 	 												AS BSNS_MINICLUSTER_KEY,
					NVL(C.BSNS_MINICLUSTER_CD,C4G.BSNS_MINICLUSTER_CD) 	 													AS BSNS_MINICLUSTER_CD,
					NVL(C.GEO_CNTRY_KEY,C4G.GEO_CNTRY_KEY) 		 															AS GEO_CNTRY_KEY,
					NVL(C.GEO_CNTRY_CD,C4G.GEO_CNTRY_CD) 			 														AS GEO_CNTRY_CD,
					NVL(C.GEO_STATE_KEY,C4G.GEO_STATE_KEY) 		 															AS GEO_STATE_KEY,
					NVL(C.GEO_STATE_CD,C4G.GEO_STATE_CD) 			 														AS GEO_STATE_CD,
					NVL(C.GEO_DSTRCT_KEY,C4G.GEO_DSTRCT_KEY) 		 														AS GEO_DSTRCT_KEY,
					NVL(C.GEO_DSTRCT_CD,C4G.GEO_DSTRCT_CD) 		 															AS GEO_DSTRCT_CD,
					NVL(C.GEO_CITY_KEY,C4G.GEO_CITY_KEY) 			 														AS GEO_CITY_KEY,
					NVL(C.GEO_CITY_CD,C4G.GEO_CITY_CD) 			 															AS GEO_CITY_CD,	
					
					B.ACQSTN_DT																								AS ACQSTN_DT,
					B.ACQSTN_BSNS_OUTLET_KEY 																				AS ACQSTN_BSNS_OUTLET_KEY,
					B.ACQSTN_BSNS_OUTLET_CD	 																				AS ACQSTN_BSNS_OUTLET_CD,
					CAST(NULL AS STRING)																					AS BILL_GRP_CD,
					CAST(NULL AS BIGINT)																					AS BILL_CYCLE_KEY,
					CAST(NULL AS INTEGER)																					AS CHRG_HEAD_KEY,
					CAST(NULL AS STRING)																					AS CHRG_HEAD_CD,
					'VND'  																									AS CRNCY_CD,
					A.USED_DURATION  																						AS DRTN,
					CAST(NULL AS BIGINT)																					AS RTD_DRTN,
					CAST(NULL AS BIGINT)																					AS FREE_DRTN,
					CAST(NULL AS BIGINT)																					AS CHRGD_DRTN,
					CAST(NULL AS BIGINT)																					AS INTCONN_DRTN,
					CAST(NULL AS BIGINT)																					AS TOT_FLUX,
					CAST(NULL AS BIGINT)																					AS UP_FLUX,
					CAST(NULL AS BIGINT)																					AS DOWN_FLUX,
					CAST(NULL AS STRING)																					AS FLUX_UOM_CD,
					CAST(NULL AS BIGINT)																					AS RTD_USG,
					CAST(NULL AS BIGINT)																					AS FREE_USG,
					CAST(NULL AS BIGINT)																					AS CHRGBL_USG,
					CAST(NULL AS BIGINT)																					AS INTCONN_USG,
					CAST(NULL AS STRING)																					AS CHRGNG_PRTY_IND,
					CAST(NULL AS STRING)																					AS OTHR_CHRGNG_NBR,
					CAST(NULL AS STRING)																					AS CHRGNG_IND,
					CAST(NULL AS STRING)																					AS CHRGING_ZONE_CD,
					CAST(NULL AS STRING)																					AS RTD_CLASS_CD,
					CAST(NULL AS STRING)																					AS RTNG_INFO,
					CAST((CASE WHEN A.ACC_PROFILE NOT LIKE 'TS%' THEN A.CREDIT_CHARGED ELSE 0 END) AS DECIMAL(27,8)) 		AS PREPAID_MAIN_ACCT_DECRMNT_AMT,
					CAST((CASE WHEN A.ACC_PROFILE LIKE 'TS%' THEN A.CREDIT_CHARGED ELSE 0 END) AS DECIMAL(27,8)) 			AS POSTPAID_MAIN_ACCT_DECRMNT_AMT,
					CAST(NULL AS DECIMAL(27,8))																				AS MAIN_ACCT_BAL_AMT_BEFR_IMPACT,
					CAST(NULL AS DECIMAL(27,8))																				AS MAIN_ACCT_BAL_AMT_AFTR_IMPACT,
					CAST(NULL AS DECIMAL(27,8))																				AS CHRGD_AMT,
					CAST(NULL AS DECIMAL(27,8))																				AS MARK_UP_AMT,
					CAST(	
						(CASE WHEN W1.WALLET_TYP_CD IN('SECONDARY_MAIN_BALANCE') THEN A.TK1_CONSUMED ELSE 0 END) + 
						(CASE WHEN W2.WALLET_TYP_CD IN('SECONDARY_MAIN_BALANCE') THEN A.TK2_CONSUMED ELSE 0 END) + 
						(CASE WHEN W3.WALLET_TYP_CD IN('SECONDARY_MAIN_BALANCE') THEN A.TK3_CONSUMED ELSE 0 END) + 
						(CASE WHEN W4.WALLET_TYP_CD IN('SECONDARY_MAIN_BALANCE') THEN A.TK4_CONSUMED ELSE 0 END) + 
						(CASE WHEN W5.WALLET_TYP_CD IN('SECONDARY_MAIN_BALANCE') THEN A.TK5_CONSUMED ELSE 0 END) + 
						(CASE WHEN W6.WALLET_TYP_CD IN('SECONDARY_MAIN_BALANCE') THEN A.TK6_CONSUMED ELSE 0 END) + 
						(CASE WHEN W7.WALLET_TYP_CD IN('SECONDARY_MAIN_BALANCE') THEN A.TK7_CONSUMED ELSE 0 END) + 
						(CASE WHEN W8.WALLET_TYP_CD IN('SECONDARY_MAIN_BALANCE') THEN A.TK8_CONSUMED ELSE 0 END) + 
						(CASE WHEN W9.WALLET_TYP_CD IN('SECONDARY_MAIN_BALANCE') THEN A.TK9_CONSUMED ELSE 0 END) + 
						(CASE WHEN W10.WALLET_TYP_CD IN('SECONDARY_MAIN_BALANCE') THEN A.TK10_CONSUMED ELSE 0 END) 
					AS DECIMAL(27,8)) 																						AS DISCOUNT_AMT,
					CAST(NULL AS DECIMAL(27,8))																				AS TAX_AMT,
					CAST(	 
						A.CREDIT_CHARGED + 
						(CASE WHEN W1.WALLET_TYP_CD IN('MAIN_BALANCE','SECONDARY_MAIN_BALANCE') THEN A.TK1_CONSUMED ELSE 0 END) + 
						(CASE WHEN W2.WALLET_TYP_CD IN('MAIN_BALANCE','SECONDARY_MAIN_BALANCE') THEN A.TK2_CONSUMED ELSE 0 END) + 
						(CASE WHEN W3.WALLET_TYP_CD IN('MAIN_BALANCE','SECONDARY_MAIN_BALANCE') THEN A.TK3_CONSUMED ELSE 0 END) + 
						(CASE WHEN W4.WALLET_TYP_CD IN('MAIN_BALANCE','SECONDARY_MAIN_BALANCE') THEN A.TK4_CONSUMED ELSE 0 END) + 
						(CASE WHEN W5.WALLET_TYP_CD IN('MAIN_BALANCE','SECONDARY_MAIN_BALANCE') THEN A.TK5_CONSUMED ELSE 0 END) + 
						(CASE WHEN W6.WALLET_TYP_CD IN('MAIN_BALANCE','SECONDARY_MAIN_BALANCE') THEN A.TK6_CONSUMED ELSE 0 END) + 
						(CASE WHEN W7.WALLET_TYP_CD IN('MAIN_BALANCE','SECONDARY_MAIN_BALANCE') THEN A.TK7_CONSUMED ELSE 0 END) + 
						(CASE WHEN W8.WALLET_TYP_CD IN('MAIN_BALANCE','SECONDARY_MAIN_BALANCE') THEN A.TK8_CONSUMED ELSE 0 END) + 
						(CASE WHEN W9.WALLET_TYP_CD IN('MAIN_BALANCE','SECONDARY_MAIN_BALANCE') THEN A.TK9_CONSUMED ELSE 0 END) + 
						(CASE WHEN W10.WALLET_TYP_CD IN('MAIN_BALANCE','SECONDARY_MAIN_BALANCE') THEN A.TK10_CONSUMED ELSE 0 END) 
					AS DECIMAL(27,8)) 																						AS BLLD_AMT,
					CAST(A.CREDIT_CHARGED AS DECIMAL(27,8)) 																AS RVN_AMT,
					CAST(NULL AS DECIMAL(27,8))																				AS REFUND_AMT,
					CAST(NULL AS DECIMAL(27,8))																				AS ECB_REFUND_AMT,
					CAST(NULL AS DECIMAL(27,8))																				AS INTERNAL_COST_AMT,
					CAST(NULL AS DECIMAL(27,8))																				AS INTCONN_COST_AMT,
					CAST(NULL AS DECIMAL(27,8))																				AS INTCONN_RVN_AMT,
					CAST(NULL AS DECIMAL(27,8))																				AS BONUS_DISC_REWARD_AMT,
					CAST(NULL AS BIGINT)																					AS BONUS_UNIT_CREDIT_REWARD_AMT,
					CAST(A.CREDIT_CHARGED AS DECIMAL(27,8)) 																AS MAIN_ACCT_RVN_AMT,
					CAST(NULL AS DECIMAL(27,8))																				AS ECB_RVN_AMT,
					CAST(NULL AS BIGINT)																					AS ECB_USG,
					CAST(NULL AS DECIMAL(27,8))																				AS ECB_TAX_AMT,
					CAST(W1.WALLET_KEY 			AS BIGINT) 																	AS WALLET1_KEY,
					CAST(A.TK1_CONSUMED 		AS DECIMAL(27,8)) 															AS WALLET1_BAL_DECRMNT_AMT,
					CAST(W2.WALLET_KEY 			AS BIGINT) 																	AS WALLET2_KEY,
					CAST(A.TK2_CONSUMED 		AS DECIMAL(27,8)) 															AS WALLET2_BAL_DECRMNT_AMT,
					CAST(W3.WALLET_KEY 			AS BIGINT) 																	AS WALLET3_KEY,
					CAST(A.TK3_CONSUMED 		AS DECIMAL(27,8)) 															AS WALLET3_BAL_DECRMNT_AMT,
					CAST(W4.WALLET_KEY 			AS BIGINT) 																	AS WALLET4_KEY,
					CAST(A.TK4_CONSUMED 		AS DECIMAL(27,8)) 															AS WALLET4_BAL_DECRMNT_AMT,
					CAST(W5.WALLET_KEY 			AS BIGINT) 																	AS WALLET5_KEY,
					CAST(A.TK5_CONSUMED 		AS DECIMAL(27,8)) 															AS WALLET5_BAL_DECRMNT_AMT,
					CAST(W6.WALLET_KEY 			AS BIGINT) 																	AS WALLET6_KEY,
					CAST(A.TK6_CONSUMED 		AS DECIMAL(27,8)) 															AS WALLET6_BAL_DECRMNT_AMT,
					CAST(W7.WALLET_KEY 			AS BIGINT) 																	AS WALLET7_KEY,
					CAST(A.TK7_CONSUMED 		AS DECIMAL(27,8)) 															AS WALLET7_BAL_DECRMNT_AMT,
					CAST(W8.WALLET_KEY 			AS BIGINT) 																	AS WALLET8_KEY,
					CAST(A.TK8_CONSUMED 		AS DECIMAL(27,8)) 															AS WALLET8_BAL_DECRMNT_AMT,
					CAST(W9.WALLET_KEY 			AS BIGINT) 																	AS WALLET9_KEY,
					CAST(A.TK9_CONSUMED 		AS DECIMAL(27,8)) 															AS WALLET9_BAL_DECRMNT_AMT,
					CAST(W10.WALLET_KEY 		AS BIGINT) 																	AS WALLET10_KEY,
					CAST(A.TK10_CONSUMED 		AS DECIMAL(27,8)) 															AS WALLET10_BAL_DECRMNT_AMT,
					B.LOYALTY_RANK_SCORE																					AS LOYALTY_RANK_SCORE,
					B.LOYALTY_SCORE_DT																						AS LOYALTY_SCORE_DT,
					B.CREDIT_SCORE																							AS CREDIT_SCORE,
					B.CREDIT_CLASS_CD																						AS CREDIT_CLASS_CD,
					B.CREDIT_SCORE_METHOD																					AS CREDIT_SCORE_METHOD,
					B.CREDIT_SCORE_DT																						AS CREDIT_SCORE_DT,
					B.RISK_IND																								AS RISK_IND,
					CAST(NULL AS STRING)																					AS ACCT_SEGMENT_CD,
					CAST(NULL AS STRING)																					AS ACCT_SEGMENT_DT,
					CAST(NULL AS STRING)																					AS CMPGN_CD,
					CAST(6 AS BIGINT) 																						AS SRC_SYS_KEY,
					'ICC' 																									AS SRC_SYS_CD,
					CURRENT_TIMESTAMP() 																					AS LOAD_DT,
					'1' 																									AS CURR_IND,
					1 																										AS WRHS_ID,
					A.STT,A.CALL_TYPE,A.CALLING_ISDN_STD,A.CALL_STA_TIME,A.FTP_FILENAME,
					A.ITEM_ID,A.SUB_ITEM_ID			
			FROM
			MBF_STAGE.STG_CDR_IN_ENRICH A 
			LEFT JOIN MBF_BIGDATA.ADMR_OFFER F on F.day_key = '$day_key' and upper(F.offer_cd) = upper(A.TRANSACTION_DESCRIPTION) AND A.CALL_TYPE = 415
			LEFT JOIN MBF_BIGDATA.ADMR_ACCOUNT_SERVICE B ON B.SERVICE_NBR = (CASE WHEN LENGTH(NVL(A.ORG_CALL_ID,'')) <> 0
																					THEN
																					CASE
																						WHEN SUBSTR(A.ORG_CALL_ID, 1, 4) = '0001'
																						THEN
																							SUBSTR(A.ORG_CALL_ID, 5)
																						ELSE
																							CAST(CAST(A.ORG_CALL_ID AS INT) AS STRING)
																					END
																				ELSE A.CALLING_ISDN_STD 	
																			END	) AND B.DAY_KEY = '$day_key'
			LEFT JOIN MBF_BIGDATA.ADMR_CELL C ON A.CELL_ID = C.CELL_GCI_CD AND C.DAY_KEY = '$day_key'
			LEFT JOIN CELL_4G_STD C4G ON A.CELL_ID = C4G.CELL_GCI_CD
			LEFT JOIN MBF_BIGDATA.ADMR_WALLET W1 ON W1.WALLET_CBS_CD = LOWER(A.TK1) AND W1.DAY_KEY = '$day_key'
			AND ((A.CALL_TYPE_IND IN ('OTHER','UNKNOWN') AND W1.ROW_NUM1 = 1)  OR (W1.USAGE_TYP_CD IN ('CONVERGENT','VOICE') AND A.CALL_TYPE_IND IN ('IC','OG') AND W1.ROW_NUM = 1) OR (W1.USAGE_TYP_CD IN ('CONVERGENT','SMS') AND A.CALL_TYPE_IND = 'SMO' AND W1.ROW_NUM = 1))		
			LEFT JOIN MBF_BIGDATA.ADMR_WALLET W2 ON W2.WALLET_CBS_CD = LOWER(A.TK2)  AND W2.DAY_KEY = '$day_key'
			AND ((A.CALL_TYPE_IND IN ('OTHER','UNKNOWN') AND W2.ROW_NUM1 = 1)  OR (W2.USAGE_TYP_CD IN ('CONVERGENT','VOICE') AND A.CALL_TYPE_IND IN ('IC','OG') AND W2.ROW_NUM = 1) OR (W2.USAGE_TYP_CD IN ('CONVERGENT','SMS') AND A.CALL_TYPE_IND = 'SMO' AND W2.ROW_NUM = 1))		
			LEFT JOIN MBF_BIGDATA.ADMR_WALLET W3 ON W3.WALLET_CBS_CD = LOWER(A.TK3)  AND W3.DAY_KEY = '$day_key'
			AND ((A.CALL_TYPE_IND IN ('OTHER','UNKNOWN') AND W3.ROW_NUM1 = 1)  OR (W3.USAGE_TYP_CD IN ('CONVERGENT','VOICE') AND A.CALL_TYPE_IND IN ('IC','OG') AND W3.ROW_NUM = 1) OR (W3.USAGE_TYP_CD IN ('CONVERGENT','SMS') AND A.CALL_TYPE_IND = 'SMO' AND W3.ROW_NUM = 1))		
			LEFT JOIN MBF_BIGDATA.ADMR_WALLET W4 ON W4.WALLET_CBS_CD = LOWER(A.TK4)  AND W4.DAY_KEY = '$day_key'	
			AND ((A.CALL_TYPE_IND IN ('OTHER','UNKNOWN') AND W4.ROW_NUM1 = 1)  OR (W4.USAGE_TYP_CD IN ('CONVERGENT','VOICE') AND A.CALL_TYPE_IND IN ('IC','OG') AND W4.ROW_NUM = 1) OR (W4.USAGE_TYP_CD IN ('CONVERGENT','SMS') AND A.CALL_TYPE_IND = 'SMO' AND W4.ROW_NUM = 1))			
			LEFT JOIN MBF_BIGDATA.ADMR_WALLET W5 ON W5.WALLET_CBS_CD = LOWER(A.TK5) AND W5.DAY_KEY = '$day_key'			
			AND ((A.CALL_TYPE_IND IN ('OTHER','UNKNOWN') AND W5.ROW_NUM1 = 1)  OR (W5.USAGE_TYP_CD IN ('CONVERGENT','VOICE') AND A.CALL_TYPE_IND IN ('IC','OG') AND W5.ROW_NUM = 1) OR (W5.USAGE_TYP_CD IN ('CONVERGENT','SMS') AND A.CALL_TYPE_IND = 'SMO' AND W5.ROW_NUM = 1))
			LEFT JOIN MBF_BIGDATA.ADMR_WALLET W6 ON W6.WALLET_CBS_CD = LOWER(A.TK6) AND W6.DAY_KEY = '$day_key'	
			AND ((A.CALL_TYPE_IND IN ('OTHER','UNKNOWN') AND W6.ROW_NUM1 = 1)  OR (W6.USAGE_TYP_CD IN ('CONVERGENT','VOICE') AND A.CALL_TYPE_IND IN ('IC','OG') AND W6.ROW_NUM = 1) OR (W6.USAGE_TYP_CD IN ('CONVERGENT','SMS') AND A.CALL_TYPE_IND = 'SMO' AND W6.ROW_NUM = 1))		
			LEFT JOIN MBF_BIGDATA.ADMR_WALLET W7 ON W7.WALLET_CBS_CD = LOWER(A.TK7) AND W7.DAY_KEY = '$day_key'
			AND ((A.CALL_TYPE_IND IN ('OTHER','UNKNOWN') AND W7.ROW_NUM1 = 1)  OR (W7.USAGE_TYP_CD IN ('CONVERGENT','VOICE') AND A.CALL_TYPE_IND IN ('IC','OG') AND W7.ROW_NUM = 1) OR (W7.USAGE_TYP_CD IN ('CONVERGENT','SMS') AND A.CALL_TYPE_IND = 'SMO' AND W7.ROW_NUM = 1))			
			LEFT JOIN MBF_BIGDATA.ADMR_WALLET W8 ON W8.WALLET_CBS_CD = LOWER(A.TK8)  AND W8.DAY_KEY = '$day_key'
			AND ((A.CALL_TYPE_IND IN ('OTHER','UNKNOWN') AND W8.ROW_NUM1 = 1)  OR (W8.USAGE_TYP_CD IN ('CONVERGENT','VOICE') AND A.CALL_TYPE_IND IN ('IC','OG') AND W8.ROW_NUM = 1) OR (W8.USAGE_TYP_CD IN ('CONVERGENT','SMS') AND A.CALL_TYPE_IND = 'SMO' AND W8.ROW_NUM = 1))			
			LEFT JOIN MBF_BIGDATA.ADMR_WALLET W9 ON W9.WALLET_CBS_CD = LOWER(A.TK9)  AND W9.DAY_KEY = '$day_key'
			AND ((A.CALL_TYPE_IND IN ('OTHER','UNKNOWN') AND W9.ROW_NUM1 = 1)  OR (W9.USAGE_TYP_CD IN ('CONVERGENT','VOICE') AND A.CALL_TYPE_IND IN ('IC','OG') AND W9.ROW_NUM = 1) OR (W9.USAGE_TYP_CD IN ('CONVERGENT','SMS') AND A.CALL_TYPE_IND = 'SMO' AND W9.ROW_NUM = 1))			
			LEFT JOIN MBF_BIGDATA.ADMR_WALLET W10 ON W10.WALLET_CBS_CD = LOWER(A.TK10)  AND W10.DAY_KEY = '$day_key'
			AND ((A.CALL_TYPE_IND IN ('OTHER','UNKNOWN') AND W10.ROW_NUM1 = 1)  OR (W10.USAGE_TYP_CD IN ('CONVERGENT','VOICE') AND A.CALL_TYPE_IND IN ('IC','OG') AND W10.ROW_NUM = 1) OR (W10.USAGE_TYP_CD IN ('CONVERGENT','SMS') AND A.CALL_TYPE_IND = 'SMO' AND W10.ROW_NUM = 1))		
			WHERE (
					(
						(
							(A.CALL_TYPE_IND = 'SMO' AND A.CALL_TYPE = 21 AND LENGTH(A.CALLED_ISDN)<10)
							 OR (
									A.CALL_TYPE_IND = 'OG' AND CALL_TYPE = 1 AND ( A.called_isdn LIKE '041900%'
																		OR A.called_isdn LIKE '1900%'
																		OR A.called_isdn LIKE '041800%'
																		OR A.called_isdn LIKE '1800%')
								)
						
						
						) AND NOT (NVL(A.LOCATION_INDICATOR,-1) = 2 OR NVL(A.`LOCATION`,'') LIKE '8491%' OR NVL(A.CELL_ID,'') LIKE '452-02%')
					) 
					OR (A.CALL_TYPE_IND = 'OTHER' AND A.CALL_TYPE IN (184, 186)) 
					OR (A.call_type = 415 and not ((nvl(A.transaction_description,'') like 'UT%' and cast(SUBSTRING(A.scratch_value,1,LENGTH(scratch_value)-2) as int) > 0)  or NVL(A.transaction_description,'') like '%HU%'))
				  ) AND A.DAY_KEY = '$day_key' AND F.OFFER_KEY IS NULL
		",
		"tempTable"  : "ADME_RATED_VAS_EDR_TMP",
		"countSourceRecord":"0"
	  },
   	  {
		"sql"      	:"
						SELECT 
							X.*
						FROM (
								SELECT
									A.*,
									B.*,
									ROW_NUMBER () OVER (PARTITION BY A.STT,A.CALL_TYPE,A.CALLING_ISDN_STD,A.CALL_STA_TIME,A.FTP_FILENAME,A.ITEM_KEY,A.SUB_ITEM_CD,B.ITEM_ID_DTTT, B.SUB_ITEM_ID ORDER BY B.INCLUDE_SUB_ITEM_ID DESC, B.EFFECT_FROM DESC) ROW_NUM_SUB,
									ROW_NUMBER () OVER (PARTITION BY A.STT,A.CALL_TYPE,A.CALLING_ISDN_STD,A.CALL_STA_TIME,A.FTP_FILENAME,A.ITEM_KEY,A.SUB_ITEM_CD,B.ITEM_ID_DTTT ORDER BY B.EFFECT_FROM DESC) ROW_NUM
								FROM ADME_RATED_VAS_EDR_TMP A
								LEFT OUTER JOIN MBF_DATALAKE.MAP_TREE B
								ON FROM_UNIXTIME(UNIX_TIMESTAMP(CONCAT(SUBSTRING(B.EFFECT_FROM,0,4),SUBSTRING(B.EFFECT_FROM,6,2),'01'), 'yyyyMMdd')) <= FROM_UNIXTIME(UNIX_TIMESTAMP('$day_key', 'yyyyMMdd'))
								AND FROM_UNIXTIME(UNIX_TIMESTAMP('$day_key', 'yyyyMMdd')) < CASE WHEN (B.EFFECT_UNTIL IS NULL OR B.EFFECT_UNTIL = '') THEN FROM_UNIXTIME(UNIX_TIMESTAMP('$day_key', 'yyyyMMdd') + 1) 
								ELSE DATE_ADD(LAST_DAY(FROM_UNIXTIME(UNIX_TIMESTAMP(CONCAT(SUBSTRING(B.EFFECT_UNTIL,0,4),SUBSTRING(B.EFFECT_UNTIL,6,2),'01') , 'yyyyMMdd'))),1)  END
								AND NVL (B.ITEM_ID_DTTT, -1) = NVL (A.ITEM_KEY, -1)
								AND ((B.INCLUDE_SUB_ITEM_ID = 1 AND NVL (UPPER(B.SUB_ITEM_ID), '-1') = NVL (UPPER (A.SUB_ITEM_CD), '-1')) OR B.INCLUDE_SUB_ITEM_ID = 0)
						) X WHERE X.ROW_NUM_SUB = 1 AND X.ROW_NUM = 1
		",
		"tempTable"  : "ADME_RATED_VAS_EDR_TMP1",
		"countSourceRecord":"0"
	  },
   	  {
		"sql"      	:"
						SELECT 
							A.MO_KEY,																						
							A.HOUR_KEY,																					
							A.UUID,				
							A.EVT_CD,				
							A.EVT_SEQ_NBR,
							A.EVT_START_DT,																				
							A.EVT_END_DT,																				
							A.BLLG_START_DT,																								
							A.FILE_NAME,																					
							A.FILE_DT,																							
							A.FILE_PREFIX_CD,																						
							A.REC_PROCESS_DT,																							
							A.SWITCH_CD,																						
							A.ACCT_SRVC_INSTANCE_KEY,																							
							A.SERVICE_NBR,																					
							A.ACCT_KEY,																			
							A.BILLABLE_ACCT_KEY,																						
							A.CUST_KEY,																			
							A.CUST_TYP_CD,																				
							A.NTWK_QOS_GRP_CD,																					
							A.ACCT_ACTIVATION_DT,																					
							A.ACCT_CBS_ACTIVATION_DT,																						
							A.ACCT_LFCYCL_STAT_CD,																					
							A.SRVC_ACTIVATION_DT,																					
							A.SRVC_CBS_ACTIVATION_DT,																						
							A.SRVC_LFCYCL_STAT_CD,																					
							A.PROD_LINE_KEY,																					
							A.USAGE_PLAN_KEY,																					
							A.USAGE_PLAN_CD,																					
							A.USAGE_PLAN_TYP_CD,																						
							A.OFFER_KEY,																					
							A.OFFER_CD,																				
							B.PROD_KEY,																								
							B.PROD_CD,																							
							A.ITEM_KEY,																				
							A.SUB_ITEM_CD,																					
							A.EVT_CLASS_CD,																			
							A.EVT_CTGRY_CD,																								
							A.EVT_TYP_CD,																								
							A.USAGE_TYP_CD,																		
							A.CALL_CTGRY_CD,																								
							A.CALL_TYP_CD,																								
							A.EVT_STAT_CD,																							
							A.EVT_TMNT_RSN_CD,																							
							A.BLLG_STAT_CD,																							
							A.BLLG_STAT_RSN_CD,																							
							A.BLLG_OPRTR_KEY,																							
							A.BLLG_OPRTR_CD,																							
							A.CONTENT_KEY,																								
							A.CONTENT_CD,																							
							A.CONTENT_TYP_CD,																							
							A.CONTENT_CATEGORY_CD,																							
							A.MEDIA_TYP_CD,																							
							A.CONTENT_NAME,																							
							A.CONTENT_PROVIDER_ORG_KEY,																							
							A.CONTENT_PROVIDER_ORG_CD,																							
							A.CONTENT_PROVIDER_NAME,																							
							A.LANG_CD,																							
							A.SHORT_CD,																							
							A.CP_ID,																							
							A.PROMOTION_CD,																							
							A.MIGRATION_IND,																							
							A.PREV_PROD_KEY,																								
							A.PREV_PROD_CD,																							
							A.PREV_CONTENT_KEY,																								
							A.PREV_CONTENT_CD,																							
							A.LAST_CHARING_DT,																							
							A.NEXT_CHARGING_DT,																							
							A.CHARGING_REQUEST_INFO,																							
							A.CHARGING_RESPONSE_INFO,																							
							A.ACTVTN_RESPONSE_INFO,																							
							A.ACTVTN_CHANNEL_CD,																							
							A.ACTVTN_DT,																							
							A.FIRST_TIME_ACTVTN_DT,																							
							A.ORIGNTNG_NBR,																						
							A.ORIGNTNG_IMSI,																			
							A.ORIGNTNG_IMEI,																							
							A.TESTNG_NBR_IND,																			
							A.ORIGNTNG_PLMN_CD,																							
							A.ORIGNTNG_NETWORK_TYP,																							
							A.ORIGNTNG_HOME_COUNTRY_CD,																							
							A.ORIGNTNG_HOME_AREA_CD,																							
							A.ORIGNTNG_HOME_NETWORK_CD ,																							
							A.ORIGNTNG_ROAM_COUNTRY_CD,																							
							A.ORIGNTNG_ROAM_AREA_CD,																							
							A.ORIGNTNG_ROAM_NETWORK_CD,																						
							A.CCN_SCP_REF_CD,																							
							A.IN_SCP_REF_CD,																							
							A.ONNET_IND,																							
							A.ROAMING_IND,
							A.SBRP_TYP_CD,																							
							A.SBRP_VLDT_PERIOD_DAYS,																							
							A.RENEWAL_IND,																							
							A.LAST_RENEWED_DT,																							
							A.NEXT_RENEW_DT,																							
							A.SBRP_START_DT,																							
							A.SBRP_STOP_DT,																							
							A.SBRP_STAT_CD,																							
							A.SBRP_RC_AMT,																							
							A.SBRP_NRC_AMT,																							
							A.CP_RVN_SHARE_PCT,																							
							A.CSP_RVN_SHARE_PCT,																							
							A.ORIGNTNG_GCI_CD,																							
							A.ADDR_LAT,																				
							A.ADDR_LON,																				
							A.CELL_KEY,																				
							A.CELL_CD,																				
							A.CELL_SITE_KEY,																					
							A.CELL_SITE_CD,																					
							A.NTWK_MGNT_CENTRE_KEY,																							
							A.NTWK_MGNT_CENTRE_CD,																							
							A.BSNS_RGN_KEY,																					
							A.BSNS_RGN_CD,																					
							A.BSNS_CLUSTER_KEY,																						
							A.BSNS_CLUSTER_CD,																						
							A.BSNS_MINICLUSTER_KEY,																							
							A.BSNS_MINICLUSTER_CD,																							
							A.GEO_CNTRY_KEY,																					
							A.GEO_CNTRY_CD,																					
							A.GEO_STATE_KEY,																					
							A.GEO_STATE_CD,																					
							A.GEO_DSTRCT_KEY,																						
							A.GEO_DSTRCT_CD,																					
							A.GEO_CITY_KEY,																					
							A.GEO_CITY_CD,																					
							A.ACQSTN_DT,																				
							A.ACQSTN_BSNS_OUTLET_KEY,																								
							A.ACQSTN_BSNS_OUTLET_CD,																							
							A.BILL_GRP_CD,																							
							A.BILL_CYCLE_KEY,																							
							A.CHRG_HEAD_KEY,																							
							A.CHRG_HEAD_CD,																							
							A.CRNCY_CD,																			
							A.DRTN,																						
							A.RTD_DRTN,																							
							A.FREE_DRTN,																							
							A.CHRGD_DRTN,																							
							A.INTCONN_DRTN,																							
							A.TOT_FLUX,																							
							A.UP_FLUX,																							
							A.DOWN_FLUX,																							
							A.FLUX_UOM_CD,																							
							A.RTD_USG,																							
							A.FREE_USG,																							
							A.CHRGBL_USG,																							
							A.INTCONN_USG,																							
							A.CHRGNG_PRTY_IND,																							
							A.OTHR_CHRGNG_NBR,																							
							A.CHRGNG_IND,																							
							A.CHRGING_ZONE_CD,																							
							A.RTD_CLASS_CD,																							
							A.RTNG_INFO,
							A.PREPAID_MAIN_ACCT_DECRMNT_AMT,
							A.POSTPAID_MAIN_ACCT_DECRMNT_AMT,
							A.MAIN_ACCT_BAL_AMT_BEFR_IMPACT,
							A.MAIN_ACCT_BAL_AMT_AFTR_IMPACT,
							A.CHRGD_AMT,
							A.MARK_UP_AMT,
							A.DISCOUNT_AMT,
							A.TAX_AMT,
							A.BLLD_AMT,														
							A.RVN_AMT,
							A.REFUND_AMT,
							A.ECB_REFUND_AMT,
							A.INTERNAL_COST_AMT,
							A.INTCONN_COST_AMT,
							A.INTCONN_RVN_AMT,
							A.BONUS_DISC_REWARD_AMT,
							A.BONUS_UNIT_CREDIT_REWARD_AMT,
							A.MAIN_ACCT_RVN_AMT,
							A.ECB_RVN_AMT,
							A.ECB_USG,
							A.ECB_TAX_AMT,
							A.WALLET1_KEY,
							A.WALLET1_BAL_DECRMNT_AMT,
							A.WALLET2_KEY,
							A.WALLET2_BAL_DECRMNT_AMT,
							A.WALLET3_KEY,
							A.WALLET3_BAL_DECRMNT_AMT,
							A.WALLET4_KEY,
							A.WALLET4_BAL_DECRMNT_AMT,
							A.WALLET5_KEY,
							A.WALLET5_BAL_DECRMNT_AMT,
							A.WALLET6_KEY,
							A.WALLET6_BAL_DECRMNT_AMT,
							A.WALLET7_KEY,
							A.WALLET7_BAL_DECRMNT_AMT,
							A.WALLET8_KEY,
							A.WALLET8_BAL_DECRMNT_AMT,
							A.WALLET9_KEY,
							A.WALLET9_BAL_DECRMNT_AMT,
							A.WALLET10_KEY,
							A.WALLET10_BAL_DECRMNT_AMT,
							A.LOYALTY_RANK_SCORE,
							A.LOYALTY_SCORE_DT ,
							A.CREDIT_SCORE ,
							A.CREDIT_CLASS_CD,
							A.CREDIT_SCORE_METHOD,
							A.CREDIT_SCORE_DT,
							A.RISK_IND,
							A.ACCT_SEGMENT_CD,
							A.ACCT_SEGMENT_DT,
							A.CMPGN_CD,
							A.SRC_SYS_KEY,
							A.SRC_SYS_CD,
							A.LOAD_DT,
							A.CURR_IND,
							A.WRHS_ID									
						FROM ADME_RATED_VAS_EDR_TMP1 A
						LEFT OUTER JOIN MBF_BIGDATA.ADMR_PRODUCT B ON NVL(A.TREE_ID,'157') = B.PROD_KEY AND B.DAY_KEY = '$day_key'
		",
		"tempTable"  : "ADME_RATED_VAS_EDR_TMP1",
		"countSourceRecord":"0"
	  }   	     	  
	]
}