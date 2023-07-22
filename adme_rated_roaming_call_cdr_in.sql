{
	 "description":"adme_rated_roaming_call_cdr",
	 "sqlStatements": [
      {
		"sql"      	:"
						SELECT 
							CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(A.CALL_STA_TIME,'DD/MM/YYYY HH:MM:SS'),'YYYYMM') AS INTEGER)  	AS MO_KEY,
							CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(A.CALL_STA_TIME,'DD/MM/YYYY HH:MM:SS'),'HH') AS INTEGER)  		AS HOUR_KEY,
							CAST(NULL AS STRING) 																				AS UUID,
							CAST(NULL AS STRING) 																				AS EVT_CD,
							A.STT 																								AS EVT_SEQ_NBR,
							CAST(NULL AS BIGINT) 																				AS PRTIAL_REC_NBR,
							CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(A.CALL_STA_TIME,'DD/MM/YYYY HH:MM:SS')) AS TIMESTAMP) 			AS EVT_START_DT,
							CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(A.CALL_END_TIME,'DD/MM/YYYY HH:MM:SS')) AS TIMESTAMP) 			AS EVT_END_DT,
							CAST(NULL AS TIMESTAMP) 																			AS BLLG_START_DT,
							A.FTP_FILENAME																						AS TAP_FILE_NAME,
							CAST(NULL AS TIMESTAMP) 																			AS TAP_FILE_DT,
							CAST(NULL AS STRING) 																				AS TAP_FILE_PREFIX_CD,
							CAST(NULL AS TIMESTAMP) 																			AS REC_PROCESS_DT,
							CAST(NULL AS STRING) 																				AS SWITCH_CD,
							'RATED_O' 																							AS RMNG_DRCTN_CD,
							CAST(NULL AS STRING) 																				AS TAP_DRCTN_CD,
							B.ACCT_SRVC_INSTANCE_KEY 																			AS ACCT_SRVC_INSTANCE_KEY,
							B.SERVICE_NBR 																						AS SERVICE_NBR,
							B.ACCT_KEY 																							AS ACCT_KEY,
							B.BILLABLE_ACCT_KEY 																				AS BILLABLE_ACCT_KEY,
							B.CUST_KEY 																							AS CUST_KEY,
							B.CUST_TYP_CD 																						AS CUST_TYP_CD,
							B.NTWK_QOS_GRP_CD 																					AS NTWK_QOS_GRP_CD,
							B.ACTIVATION_DT 																					AS ACCT_ACTIVATION_DT,
							B.CBS_ACTIVATION_DT 																				AS ACCT_CBS_ACTIVATION_DT,
							B.LFCYCL_STAT_CD 																					AS ACCT_LFCYCL_STAT_CD,
							B.ACTIVATION_DT 																					AS SRVC_ACTIVATION_DT,
							B.CBS_ACTIVATION_DT 																				AS SRVC_CBS_ACTIVATION_DT,
							B.LFCYCL_STAT_CD 																					AS SRVC_LFCYCL_STAT_CD,
							B.PROD_LINE_KEY 																					AS PROD_LINE_KEY,
							B.USAGE_PLAN_KEY 																					AS USAGE_PLAN_KEY,
							B.USAGE_PLAN_CD 																					AS USAGE_PLAN_CD,
							B.USAGE_PLAN_TYP_CD 																				AS USAGE_PLAN_TYP_CD,
							B.CRM_OFFER_KEY 																					AS OFFER_KEY,
							B.CRM_OFFER_CD 																						AS OFFER_CD,
							CAST(NULL AS DECIMAL(20,0)) 																		AS PROD_KEY,
							CAST(NULL AS STRING) 																				AS PROD_CD,
							CASE 
								WHEN A.CALL_TYPE_IND IN ('SMO') 	THEN 'SMS'
								WHEN A.CALL_TYPE_IND IN ('IC','OG') THEN 'VOICE' 
								ELSE 'UNKNOWN'
							END 																								AS EVT_CLASS_CD,
							A.EVT_CTGRY_CD																	 					AS EVT_CTGRY_CD,
							A.CALL_TYP_CD	 																					AS EVT_TYP_CD,
							CASE 
								WHEN A.CALL_TYPE_IND IN ('SMO') 	THEN 'SMS'
								WHEN A.CALL_TYPE_IND IN ('IC','OG') THEN 'VOICE' 
								ELSE 'UNKNOWN'
							END 																								AS USAGE_TYP_CD,
							A.CALL_CTGRY_CD 																					AS CALL_CTGRY_CD,
							A.CALL_TYP_CD 																						AS CALL_TYP_CD,	
							CAST(NULL AS STRING) AS CALL_STAT_CD,
							CAST(NULL AS STRING) AS CALL_TMNT_RSN_CD,
							CASE
								 WHEN LENGTH(NVL(A.ORG_CALL_ID,'')) <> 0
								  THEN
								   CASE
									 WHEN SUBSTR(A.ORG_CALL_ID, 1, 4) = '0001'
									 THEN
									   SUBSTR(A.ORG_CALL_ID, 5)
									 ELSE
									   CAST(CAST(A.ORG_CALL_ID AS INT) AS STRING)
								   END							 
								 ELSE A.CALLING_ISDN_STD
							END																									AS ORIGNTNG_NBR,		
							CASE WHEN A.CALL_TYPE_IND = 'IC' THEN CAST(NULL AS STRING) ELSE A.IMSI END 							AS ORIGNTNG_IMSI,						
							CAST(NULL AS STRING) 	 																			AS ORIGNTNG_IMEI,
							CAST(NULL AS STRING) 	 																			AS ORIGNTNG_CARRIER,	
							A.CALLED_ISDN_STD        																			AS TRMNTNG_NBR,
							CASE WHEN A.CALL_TYPE_IND = 'IC' THEN A.IMSI ELSE CAST(NULL AS STRING) END 							AS TRMNTNG_IMSI,						
							CAST(NULL AS STRING) 	 																			AS TRMNTNG_IMEI,
							CAST(NULL AS STRING) 	 																			AS TRMNTNG_CARRIER,
							
							CAST(NULL AS STRING) 																				AS DLD_NBR,
							CAST(NULL AS BIGINT) 																				AS ORIGNTNG_CUG_KEY,
							CAST(NULL AS BIGINT) 																				AS TRMNTNG_CUG_KEY,
							CASE WHEN A.LOCATION_INDICATOR = 2 THEN 'I' ELSE 'D' END 											AS INTL_DMSTC_IND,
							CAST(NULL AS STRING) 																				AS FNF_IND,
							CAST(NULL AS STRING) 																				AS SPECIAL_DLD_NBR_IND,
							CAST(NULL AS STRING) 																				AS TESTNG_NBR_IND,
							CASE 
								WHEN A.CALL_TYPE_IND IN ('OG','SMO') THEN 'O'
								WHEN A.CALL_TYPE_IND IN ('IC','SMT') THEN 'I'
							END 																								AS EVT_DRCTN_CD,	
							
							CASE 
								WHEN CALL_TYP_CD LIKE '%ONNET%' THEN '1' 
								WHEN CALL_TYP_CD LIKE '%OFFNET%'  THEN '0' 
							END 																								AS ONNET_OFFNET_IND,
							CASE WHEN A.CALL_TYPE = 20 THEN '20' ELSE '00' END 													AS REDRCTD_IND,
							CAST(NULL AS STRING) 																				AS REDRCTD_NBR,
							CAST(NULL AS STRING) 																				AS REDRCTD_IMSI,
							CAST(NULL AS STRING) 																				AS REDRCTD_IMEI,
							CAST(NULL AS STRING) 																				AS REDRCTD_CARRIER,
							CAST(NULL AS STRING) 																				AS THRD_PRTY_HOME_CNTRY_CD,
							CAST(NULL AS STRING) 																				AS ORIGNTNG_MSS_CD,
							CAST(NULL AS STRING) 																				AS TRMNTNG_MSS_CD,
							CAST(NULL AS STRING) 	 																			AS ORIGNTNG_PLMN_CD,
							CAST(NULL AS STRING) 	 																			AS TRMNTNG_PLMN_CD,
							CAST(NULL AS STRING) 	 																			AS ORIGNTNG_NETWORK_TYP,
							CAST(NULL AS STRING) 	 																			AS TRMNTNG_NETWORK_TYP,	
							CASE WHEN A.CALL_TYPE_IND = 'IC' THEN CAST(NULL AS STRING) ELSE SUBSTRING(A.IMSI,1,3) END 			AS ORIGNTNG_HOME_COUNTRY_CD,						
							CAST(NULL AS STRING) 																				AS ORIGNTNG_HOME_AREA_CD,												
							CASE WHEN A.CALL_TYPE_IND = 'IC' THEN CAST(NULL AS STRING) ELSE SUBSTRING(A.IMSI,4,2) END 			AS ORIGNTNG_HOME_NETWORK_CD,						
							(CASE WHEN A.IMSI NOT LIKE '452%' OR A.IMSI NOT LIKE '45201%' THEN SUBSTRING(A.IMSI,1,3)
								  ELSE CAST(NULL AS STRING)
							END) 																								AS ORIGNTNG_ROAM_COUNTRY_CD,
							CAST(NULL AS STRING) 																				AS ORIGNTNG_ROAM_AREA_CD,
							(CASE WHEN A.IMSI NOT LIKE '452%' OR A.IMSI NOT LIKE '45201%' THEN SUBSTRING(A.IMSI,4,2)
								  ELSE CAST(NULL AS STRING)
							END) 																								AS ORIGNTNG_ROAM_NETWORK_CD,		
							CAST(NULL AS STRING) 																				AS ORIGNTNG_NTWK_ROUTE_CD,
							
							CASE WHEN A.CALL_TYPE_IND = 'IC' THEN SUBSTRING(A.IMSI,1,3) ELSE CAST(NULL AS STRING) END 			AS TRMNTNG_HOME_COUNTRY_CD,						
							CAST(NULL AS STRING) 																				AS TRMNTNG_HOME_AREA_CD,						
							CASE WHEN A.CALL_TYPE_IND = 'IC' THEN SUBSTRING(A.IMSI,4,2) ELSE CAST(NULL AS STRING) END 			AS TRMNTNG_HOME_NETWORK_CD,						
							CAST(NULL AS STRING) 	 																			AS TRMNTNG_ROAM_COUNTRY_CD,
							CAST(NULL AS STRING) 	 																			AS TRMNTNG_ROAM_AREA_CD,
							CAST(NULL AS STRING) 	 																			AS TRMNTNG_ROAM_NETWORK_CD,		
							CAST(NULL AS STRING) 																				AS TRMNTNG_NTWK_ROUTE_CD,
							A.CELL_ID 				 																			AS FIRST_ORIGNTNG_GCI_CD,
							A.CELL_ID 				 																			AS LAST_ORIGNTNG_GCI_CD,	
							CAST(NULL AS STRING) 																				AS TRMNTNG_GCI_CD,	
							(CASE WHEN A.EVT_DRCTN_CD = 'I' AND A.CALL_TYP_CD NOT IN('VOICE_ONNET_IC','SMS_ONNET_IC') 
								  THEN C.EXTRNL_CSP_KEY ELSE  CAST(NULL AS BIGINT)  END)										AS ORIGNTNG_OPRTR_KEY,
							(CASE WHEN A.EVT_DRCTN_CD = 'I' AND  A.CALL_TYP_CD NOT IN('VOICE_ONNET_IC','SMS_ONNET_IC') 
								  THEN C.EXTRNL_CSP_CD ELSE CAST(NULL AS STRING) END)											AS ORIGNTNG_OPRTR_CD,
							(CASE WHEN A.EVT_DRCTN_CD = 'O' AND  A.CALL_TYP_CD NOT IN('VOICE_ONNET_OG','SMS_ONNET_OG') 
								  THEN C.EXTRNL_CSP_KEY ELSE CAST(NULL AS BIGINT) END)	 										AS TRMNTNG_OPRTR_KEY,
							(CASE WHEN A.EVT_DRCTN_CD = 'O' AND A.CALL_TYP_CD NOT IN('VOICE_ONNET_OG','SMS_ONNET_OG') 
								  THEN C.EXTRNL_CSP_CD ELSE CAST(NULL AS STRING)  END)	 										AS TRMNTNG_OPRTR_CD,		  
							CAST(NULL AS DECIMAL(27,8)) 																		AS ADDR_LAT,
							CAST(NULL AS DECIMAL(27,8)) 																		AS ADDR_LON,
							CAST(NULL AS STRING) 																				AS FIRST_CELL_KEY,
							CAST(NULL AS STRING) 																				AS FIRST_CELL_CD,
							CAST(NULL AS BIGINT) 																				AS FIRST_CELL_SITE_KEY,
							CAST(NULL AS STRING) 																				AS FIRST_CELL_SITE_CD,
							CAST(NULL AS STRING) 																				AS LAST_CELL_KEY,
							CAST(NULL AS STRING) 																				AS LAST_CELL_CD,
							CAST(NULL AS BIGINT) 																				AS LAST_CELL_SITE_KEY,
							CAST(NULL AS STRING) 																				AS LAST_CELL_SITE_CD,
							CAST(NULL AS BIGINT) 																				AS NTWK_MGNT_CENTRE_KEY,
							CAST(NULL AS STRING) 																				AS NTWK_MGNT_CENTRE_CD,
							CAST(NULL AS BIGINT) 																				AS BSNS_RGN_KEY,
							CAST(NULL AS STRING) 																				AS BSNS_RGN_CD,
							CAST(NULL AS BIGINT) 																				AS BSNS_CLUSTER_KEY,
							CAST(NULL AS STRING) 																				AS BSNS_CLUSTER_CD,
							CAST(NULL AS BIGINT) 																				AS BSNS_MINICLUSTER_KEY,
							CAST(NULL AS STRING) 																				AS BSNS_MINICLUSTER_CD,
							CAST(NULL AS BIGINT) 																				AS GEO_CNTRY_KEY,
							CAST(NULL AS STRING) 																				AS GEO_CNTRY_CD,
							CAST(NULL AS BIGINT) 																				AS GEO_STATE_KEY,
							CAST(NULL AS STRING) 																				AS GEO_STATE_CD,
							CAST(NULL AS STRING) 																				AS GEO_DSTRCT_KEY,
							CAST(NULL AS STRING) 																				AS GEO_DSTRCT_CD,
							CAST(NULL AS BIGINT) 																				AS GEO_CITY_KEY,
							CAST(NULL AS STRING) 																				AS GEO_CITY_CD,
							CAST(NULL AS TIMESTAMP) 																			AS ACQSTN_DT,
							CAST(NULL AS BIGINT) 																				AS ACQSTN_BSNS_OUTLET_KEY,
							CAST(NULL AS STRING) 																				AS ACQSTN_BSNS_OUTLET_CD,
							CAST(NULL AS BIGINT) 																				AS BLLG_OPRTR_KEY,
							CAST(NULL AS STRING) 																				AS BLLG_OPRTR_CD,
							CAST(NULL AS STRING) 																				AS BILL_GRP_CD,
							CAST(NULL AS BIGINT) 																				AS BILL_CYCLE_KEY,
							CAST(NULL AS INTEGER) 																				AS CHRG_HEAD_KEY,
							CAST(NULL AS STRING) 																				AS CHRG_HEAD_CD,
							CAST(NULL AS STRING) 																				AS CRNCY_CD,
							CAST(NULL AS STRING) 																				AS USG_UOM_CD,
							CAST(NULL AS STRING) 																				AS INTCONN_CRNCY_CD,
							CAST(NULL AS DECIMAL(27,8)) 																		AS EXCHG_RATE_VAL,
							CAST(A.USED_DURATION AS BIGINT) 																	AS DRTN,
							CASE WHEN A.CALL_TYPE_IND = 'SMO' THEN CAST(1 AS BIGINT)
							ELSE 	
								A.USED_DURATION - (
									(CASE WHEN W1.WALLET_TYP_CD = 'VOICE_UNIT_CREDIT' THEN A.TK1_CONSUMED ELSE 0 END) + 
									(CASE WHEN W2.WALLET_TYP_CD = 'VOICE_UNIT_CREDIT' THEN A.TK2_CONSUMED ELSE 0 END) + 
									(CASE WHEN W3.WALLET_TYP_CD = 'VOICE_UNIT_CREDIT' THEN A.TK3_CONSUMED ELSE 0 END) + 
									(CASE WHEN W4.WALLET_TYP_CD = 'VOICE_UNIT_CREDIT' THEN A.TK4_CONSUMED ELSE 0 END) + 
									(CASE WHEN W5.WALLET_TYP_CD = 'VOICE_UNIT_CREDIT' THEN A.TK5_CONSUMED ELSE 0 END) + 
									(CASE WHEN W6.WALLET_TYP_CD = 'VOICE_UNIT_CREDIT' THEN A.TK6_CONSUMED ELSE 0 END) + 
									(CASE WHEN W7.WALLET_TYP_CD = 'VOICE_UNIT_CREDIT' THEN A.TK7_CONSUMED ELSE 0 END) + 
									(CASE WHEN W8.WALLET_TYP_CD = 'VOICE_UNIT_CREDIT' THEN A.TK8_CONSUMED ELSE 0 END) + 
									(CASE WHEN W9.WALLET_TYP_CD = 'VOICE_UNIT_CREDIT' THEN A.TK9_CONSUMED ELSE 0 END) + 
									(CASE WHEN W10.WALLET_TYP_CD = 'VOICE_UNIT_CREDIT' THEN A.TK10_CONSUMED ELSE 0 END) 
								)		
							END 																								AS RTD_USG,
							
							CASE WHEN A.CALL_TYPE_IND = 'SMO' THEN 
									CASE WHEN A.CREDIT_CHARGED +
									(CASE WHEN W1.WALLET_TYP_CD IN ('MAIN_BALANCE','SECONDARY_MAIN_BALANCE') THEN A.TK1_CONSUMED ELSE 0 END) + 
									(CASE WHEN W2.WALLET_TYP_CD IN ('MAIN_BALANCE','SECONDARY_MAIN_BALANCE') THEN A.TK2_CONSUMED ELSE 0 END) +
									(CASE WHEN W3.WALLET_TYP_CD IN ('MAIN_BALANCE','SECONDARY_MAIN_BALANCE') THEN A.TK3_CONSUMED ELSE 0 END) +
									(CASE WHEN W4.WALLET_TYP_CD IN ('MAIN_BALANCE','SECONDARY_MAIN_BALANCE') THEN A.TK4_CONSUMED ELSE 0 END) +
									(CASE WHEN W5.WALLET_TYP_CD IN ('MAIN_BALANCE','SECONDARY_MAIN_BALANCE') THEN A.TK5_CONSUMED ELSE 0 END) +
									(CASE WHEN W6.WALLET_TYP_CD IN ('MAIN_BALANCE','SECONDARY_MAIN_BALANCE') THEN A.TK6_CONSUMED ELSE 0 END) +
									(CASE WHEN W7.WALLET_TYP_CD IN ('MAIN_BALANCE','SECONDARY_MAIN_BALANCE') THEN A.TK7_CONSUMED ELSE 0 END) +
									(CASE WHEN W8.WALLET_TYP_CD IN ('MAIN_BALANCE','SECONDARY_MAIN_BALANCE') THEN A.TK8_CONSUMED ELSE 0 END) +
									(CASE WHEN W9.WALLET_TYP_CD IN ('MAIN_BALANCE','SECONDARY_MAIN_BALANCE') THEN A.TK9_CONSUMED ELSE 0 END) +
									(CASE WHEN W10.WALLET_TYP_CD IN ('MAIN_BALANCE','SECONDARY_MAIN_BALANCE') THEN A.TK10_CONSUMED ELSE 0 END) > 0 
										  THEN CAST(0 AS BIGINT) ELSE CAST(1 AS BIGINT) END 
							ELSE 	
									(CASE WHEN W1.WALLET_TYP_CD = 'VOICE_UNIT_CREDIT' THEN A.TK1_CONSUMED ELSE 0 END) + 
									(CASE WHEN W2.WALLET_TYP_CD = 'VOICE_UNIT_CREDIT' THEN A.TK2_CONSUMED ELSE 0 END) + 
									(CASE WHEN W3.WALLET_TYP_CD = 'VOICE_UNIT_CREDIT' THEN A.TK3_CONSUMED ELSE 0 END) + 
									(CASE WHEN W4.WALLET_TYP_CD = 'VOICE_UNIT_CREDIT' THEN A.TK4_CONSUMED ELSE 0 END) + 
									(CASE WHEN W5.WALLET_TYP_CD = 'VOICE_UNIT_CREDIT' THEN A.TK5_CONSUMED ELSE 0 END) + 
									(CASE WHEN W6.WALLET_TYP_CD = 'VOICE_UNIT_CREDIT' THEN A.TK6_CONSUMED ELSE 0 END) + 
									(CASE WHEN W7.WALLET_TYP_CD = 'VOICE_UNIT_CREDIT' THEN A.TK7_CONSUMED ELSE 0 END) + 
									(CASE WHEN W8.WALLET_TYP_CD = 'VOICE_UNIT_CREDIT' THEN A.TK8_CONSUMED ELSE 0 END) + 
									(CASE WHEN W9.WALLET_TYP_CD = 'VOICE_UNIT_CREDIT' THEN A.TK9_CONSUMED ELSE 0 END) + 
									(CASE WHEN W10.WALLET_TYP_CD = 'VOICE_UNIT_CREDIT' THEN A.TK10_CONSUMED ELSE 0 END) 	
							END 																								AS FREE_USG,
							
							
							CASE WHEN A.CALL_TYPE_IND = 'SMO' THEN 
									CASE WHEN A.CREDIT_CHARGED +
									(CASE WHEN W1.WALLET_TYP_CD IN ('MAIN_BALANCE','SECONDARY_MAIN_BALANCE') THEN A.TK1_CONSUMED ELSE 0 END) + 
									(CASE WHEN W2.WALLET_TYP_CD IN ('MAIN_BALANCE','SECONDARY_MAIN_BALANCE') THEN A.TK2_CONSUMED ELSE 0 END) +
									(CASE WHEN W3.WALLET_TYP_CD IN ('MAIN_BALANCE','SECONDARY_MAIN_BALANCE') THEN A.TK3_CONSUMED ELSE 0 END) +
									(CASE WHEN W4.WALLET_TYP_CD IN ('MAIN_BALANCE','SECONDARY_MAIN_BALANCE') THEN A.TK4_CONSUMED ELSE 0 END) +
									(CASE WHEN W5.WALLET_TYP_CD IN ('MAIN_BALANCE','SECONDARY_MAIN_BALANCE') THEN A.TK5_CONSUMED ELSE 0 END) +
									(CASE WHEN W6.WALLET_TYP_CD IN ('MAIN_BALANCE','SECONDARY_MAIN_BALANCE') THEN A.TK6_CONSUMED ELSE 0 END) +
									(CASE WHEN W7.WALLET_TYP_CD IN ('MAIN_BALANCE','SECONDARY_MAIN_BALANCE') THEN A.TK7_CONSUMED ELSE 0 END) +
									(CASE WHEN W8.WALLET_TYP_CD IN ('MAIN_BALANCE','SECONDARY_MAIN_BALANCE') THEN A.TK8_CONSUMED ELSE 0 END) +
									(CASE WHEN W9.WALLET_TYP_CD IN ('MAIN_BALANCE','SECONDARY_MAIN_BALANCE') THEN A.TK9_CONSUMED ELSE 0 END) +
									(CASE WHEN W10.WALLET_TYP_CD IN ('MAIN_BALANCE','SECONDARY_MAIN_BALANCE') THEN A.TK10_CONSUMED ELSE 0 END) > 0 
										  THEN CAST(1 AS BIGINT) ELSE CAST(0 AS BIGINT) END 
							ELSE 	
									CASE WHEN A.CREDIT_CHARGED > 0 THEN	A.USED_DURATION - (
										(CASE WHEN W1.WALLET_TYP_CD = 'VOICE_UNIT_CREDIT' THEN A.TK1_CONSUMED ELSE 0 END) + 
										(CASE WHEN W2.WALLET_TYP_CD = 'VOICE_UNIT_CREDIT' THEN A.TK2_CONSUMED ELSE 0 END) + 
										(CASE WHEN W3.WALLET_TYP_CD = 'VOICE_UNIT_CREDIT' THEN A.TK3_CONSUMED ELSE 0 END) + 
										(CASE WHEN W4.WALLET_TYP_CD = 'VOICE_UNIT_CREDIT' THEN A.TK4_CONSUMED ELSE 0 END) + 
										(CASE WHEN W5.WALLET_TYP_CD = 'VOICE_UNIT_CREDIT' THEN A.TK5_CONSUMED ELSE 0 END) + 
										(CASE WHEN W6.WALLET_TYP_CD = 'VOICE_UNIT_CREDIT' THEN A.TK6_CONSUMED ELSE 0 END) + 
										(CASE WHEN W7.WALLET_TYP_CD = 'VOICE_UNIT_CREDIT' THEN A.TK7_CONSUMED ELSE 0 END) + 
										(CASE WHEN W8.WALLET_TYP_CD = 'VOICE_UNIT_CREDIT' THEN A.TK8_CONSUMED ELSE 0 END) + 
										(CASE WHEN W9.WALLET_TYP_CD = 'VOICE_UNIT_CREDIT' THEN A.TK9_CONSUMED ELSE 0 END) + 
										(CASE WHEN W10.WALLET_TYP_CD = 'VOICE_UNIT_CREDIT' THEN A.TK10_CONSUMED ELSE 0 END) 
									) ELSE CAST(0 AS BIGINT)
									END 
							END 																								AS CHRGBL_USG,	
								
							CAST(NULL AS BIGINT) 																				AS INTCONN_USG,
							CAST(NULL AS STRING) 																				AS CHRGNG_PRTY_IND,
							CAST(NULL AS STRING) 																				AS OTHR_CHRGNG_NBR,
							CAST(NULL AS STRING) 																				AS CHRGNG_IND,
							CAST(NULL AS STRING) 																				AS CHRGING_ZONE_CD,
							CAST(NULL AS STRING) 																				AS RTD_CLASS_CD,
							CAST(NULL AS STRING) 																				AS RTNG_INFO,
							CAST(NULL AS DECIMAL(30,2)) 																		AS CHRGD_AMT,
							CAST(NULL AS DECIMAL(30,2)) 																		AS MARK_UP_AMT,
							
							CAST((CASE WHEN W1.WALLET_TYP_CD IN ('SECONDARY_MAIN_BALANCE') THEN A.TK1_CONSUMED ELSE 0 END) + 
							(CASE WHEN W2.WALLET_TYP_CD IN ('SECONDARY_MAIN_BALANCE') THEN A.TK2_CONSUMED ELSE 0 END) +
							(CASE WHEN W3.WALLET_TYP_CD IN ('SECONDARY_MAIN_BALANCE') THEN A.TK3_CONSUMED ELSE 0 END) +
							(CASE WHEN W4.WALLET_TYP_CD IN ('SECONDARY_MAIN_BALANCE') THEN A.TK4_CONSUMED ELSE 0 END) +
							(CASE WHEN W5.WALLET_TYP_CD IN ('SECONDARY_MAIN_BALANCE') THEN A.TK5_CONSUMED ELSE 0 END) +
							(CASE WHEN W6.WALLET_TYP_CD IN ('SECONDARY_MAIN_BALANCE') THEN A.TK6_CONSUMED ELSE 0 END) +
							(CASE WHEN W7.WALLET_TYP_CD IN ('SECONDARY_MAIN_BALANCE') THEN A.TK7_CONSUMED ELSE 0 END) +
							(CASE WHEN W8.WALLET_TYP_CD IN ('SECONDARY_MAIN_BALANCE') THEN A.TK8_CONSUMED ELSE 0 END) +
							(CASE WHEN W9.WALLET_TYP_CD IN ('SECONDARY_MAIN_BALANCE') THEN A.TK9_CONSUMED ELSE 0 END) +
							(CASE WHEN W10.WALLET_TYP_CD IN ('SECONDARY_MAIN_BALANCE') THEN A.TK10_CONSUMED ELSE 0 END) 
								AS DECIMAL(27,8))																				AS DISCOUNT_AMT,	
							CAST(NULL AS DECIMAL(30,2)) 																		AS TAX_AMT,
							CAST(
							(CASE WHEN W1.WALLET_TYP_CD IN ('MAIN_BALANCE','SECONDARY_MAIN_BALANCE') THEN A.TK1_CONSUMED ELSE 0 END) + 
							(CASE WHEN W2.WALLET_TYP_CD IN ('MAIN_BALANCE','SECONDARY_MAIN_BALANCE') THEN A.TK2_CONSUMED ELSE 0 END) +
							(CASE WHEN W3.WALLET_TYP_CD IN ('MAIN_BALANCE','SECONDARY_MAIN_BALANCE') THEN A.TK3_CONSUMED ELSE 0 END) +
							(CASE WHEN W4.WALLET_TYP_CD IN ('MAIN_BALANCE','SECONDARY_MAIN_BALANCE') THEN A.TK4_CONSUMED ELSE 0 END) +
							(CASE WHEN W5.WALLET_TYP_CD IN ('MAIN_BALANCE','SECONDARY_MAIN_BALANCE') THEN A.TK5_CONSUMED ELSE 0 END) +
							(CASE WHEN W6.WALLET_TYP_CD IN ('MAIN_BALANCE','SECONDARY_MAIN_BALANCE') THEN A.TK6_CONSUMED ELSE 0 END) +
							(CASE WHEN W7.WALLET_TYP_CD IN ('MAIN_BALANCE','SECONDARY_MAIN_BALANCE') THEN A.TK7_CONSUMED ELSE 0 END) +
							(CASE WHEN W8.WALLET_TYP_CD IN ('MAIN_BALANCE','SECONDARY_MAIN_BALANCE') THEN A.TK8_CONSUMED ELSE 0 END) +
							(CASE WHEN W9.WALLET_TYP_CD IN ('MAIN_BALANCE','SECONDARY_MAIN_BALANCE') THEN A.TK9_CONSUMED ELSE 0 END) +
							(CASE WHEN W10.WALLET_TYP_CD IN ('MAIN_BALANCE','SECONDARY_MAIN_BALANCE') THEN A.TK10_CONSUMED ELSE 0 END) 		
								AS DECIMAL(27,8)) 																				AS BLLD_AMT,
							CAST(A.CREDIT_CHARGED AS DECIMAL(27,8)) 					 										AS RVN_AMT,
							CAST(NULL AS DECIMAL(30,2)) 																		AS INTERNAL_COST_AMT,
							CAST(NULL AS DECIMAL(30,2)) 																		AS INTCONN_COST_AMT,
							CAST(NULL AS DECIMAL(30,2)) 																		AS INTCONN_RVN_AMT,
							CAST(NULL AS STRING) 																				AS RMNG_BANK_TYP,
							CAST(NULL AS STRING) 																				AS RMNG_BANK_UOM_CD,
							CAST(NULL AS DECIMAL(27,8)) 																		AS RMNG_BANK_BALANCE_BEFR_IMPACT,
							CAST(NULL AS DECIMAL(27,8)) 																		AS RMNG_BANK_BALANCE_AFTR_IMPACT,
							CAST(NULL AS STRING) 																				AS BLLG_STAT_RSN_CD,
							CAST(B.LOYALTY_RANK_SCORE AS DECIMAL(12,4)) 														AS LOYALTY_RANK_SCORE,
							CAST(B.LOYALTY_SCORE_DT AS TIMESTAMP) 																AS LOYALTY_SCORE_DT,
							CAST(B.CREDIT_SCORE AS DECIMAL(12,4)) 																AS CREDIT_SCORE,
							CAST(B.CREDIT_CLASS_CD AS STRING) 																	AS CREDIT_CLASS_CD,
							CAST(B.CREDIT_SCORE_METHOD AS STRING) 																AS CREDIT_SCORE_METHOD,
							CAST(B.CREDIT_SCORE_DT AS TIMESTAMP) 																AS CREDIT_SCORE_DT,
							CAST(B.RISK_IND AS INTEGER) 																		AS RISK_IND,
							CAST(NULL AS STRING) 																				AS ACCT_SEGMENT_CD,
							CAST(NULL AS TIMESTAMP) 																			AS ACCT_SEGMENT_DT,
							CAST(NULL AS STRING) 																				AS CMPGN_CD,
							CAST(26 AS BIGINT) 																					AS SRC_SYS_KEY,
							CAST('BILLING_GW' AS STRING) 																		AS SRC_SYS_CD,
							CURRENT_TIMESTAMP() 																				AS LOAD_DT,
							CAST('1' AS STRING) 																				AS CURR_IND,
							CAST(1 AS INTEGER) 																					AS WRHS_ID
						FROM MBF_STAGE.STG_CDR_IN_ENRICH A
						LEFT JOIN MBF_BIGDATA.ADMR_ACCOUNT_SERVICE B 
						ON B.SERVICE_NBR = 	CASE
												 WHEN LENGTH(NVL(A.ORG_CALL_ID,'')) <> 0
												  THEN
												   CASE
													 WHEN SUBSTR(A.ORG_CALL_ID, 1, 4) = '0001'
													 THEN
													   SUBSTR(A.ORG_CALL_ID, 5)
													 ELSE
													   CAST(CAST(A.ORG_CALL_ID AS INT) AS STRING)
												   END
													 WHEN A.EVT_DRCTN_CD = 'I' THEN A.CALLED_ISDN_STD
													 ELSE A.CALLING_ISDN_STD
											  END	
						AND B.DAY_KEY = '$day_key'
						LEFT JOIN MBF_BIGDATA.ADMR_EXTERNAL_CSP C ON A.PLMN_PLMN_ID = C.PLMN_CD AND C.CSP_TYP_CD = 'MOBILE' AND C.DAY_KEY = '$day_key'
						LEFT JOIN MBF_BIGDATA.ADMR_WALLET W1 ON W1.WALLET_CBS_CD = LOWER(A.TK1) AND W1.DAY_KEY = '$day_key'
						AND W1.ROW_NUM = 1 AND W1.USAGE_TYP_CD IN ('CONVERGENT','VOICE','SMS') 
						LEFT JOIN MBF_BIGDATA.ADMR_WALLET W2 ON W2.WALLET_CBS_CD = LOWER(A.TK2) AND W2.DAY_KEY = '$day_key'
						AND W2.ROW_NUM = 1 AND W2.USAGE_TYP_CD IN ('CONVERGENT','VOICE','SMS') 
						LEFT JOIN MBF_BIGDATA.ADMR_WALLET W3 ON W3.WALLET_CBS_CD = LOWER(A.TK3) AND W3.DAY_KEY = '$day_key'
						AND W3.ROW_NUM = 1 AND W3.USAGE_TYP_CD IN ('CONVERGENT','VOICE','SMS') 
						LEFT JOIN MBF_BIGDATA.ADMR_WALLET W4 ON W4.WALLET_CBS_CD = LOWER(A.TK4) AND W4.DAY_KEY = '$day_key'
						AND W4.ROW_NUM = 1 AND W4.USAGE_TYP_CD IN ('CONVERGENT','VOICE','SMS') 
						LEFT JOIN MBF_BIGDATA.ADMR_WALLET W5 ON W5.WALLET_CBS_CD = LOWER(A.TK5) AND W5.DAY_KEY = '$day_key'
						AND W5.ROW_NUM = 1 AND W5.USAGE_TYP_CD IN ('CONVERGENT','VOICE','SMS') 
						LEFT JOIN MBF_BIGDATA.ADMR_WALLET W6 ON W6.WALLET_CBS_CD = LOWER(A.TK6) AND W6.DAY_KEY = '$day_key'
						AND W6.ROW_NUM = 1 AND W6.USAGE_TYP_CD IN ('CONVERGENT','VOICE','SMS') 
						LEFT JOIN MBF_BIGDATA.ADMR_WALLET W7 ON W7.WALLET_CBS_CD = LOWER(A.TK7) AND W7.DAY_KEY = '$day_key'
						AND W7.ROW_NUM = 1 AND W7.USAGE_TYP_CD IN ('CONVERGENT','VOICE','SMS') 
						LEFT JOIN MBF_BIGDATA.ADMR_WALLET W8 ON W8.WALLET_CBS_CD = LOWER(A.TK8) AND W8.DAY_KEY = '$day_key'
						AND W8.ROW_NUM = 1 AND W8.USAGE_TYP_CD IN ('CONVERGENT','VOICE','SMS') 
						LEFT JOIN MBF_BIGDATA.ADMR_WALLET W9 ON W9.WALLET_CBS_CD = LOWER(A.TK9) AND W9.DAY_KEY = '$day_key'
						AND W9.ROW_NUM = 1 AND W9.USAGE_TYP_CD IN ('CONVERGENT','VOICE','SMS') 
						LEFT JOIN MBF_BIGDATA.ADMR_WALLET W10 ON W10.WALLET_CBS_CD = LOWER(A.TK10) AND W10.DAY_KEY = '$day_key'
						AND W10.ROW_NUM = 1 AND W10.USAGE_TYP_CD IN ('CONVERGENT','VOICE','SMS') 
						WHERE A.DAY_KEY = '$day_key' 						
						AND (A.LOCATION_INDICATOR = 2 OR A.`LOCATION` LIKE '8491%'OR A.CELL_ID LIKE '452-02')
						AND ((A.CALL_TYPE_IND = 'SMO' AND A.CALL_TYPE IN(21)) OR ((A.CALL_TYPE_IND = 'IC' AND A.CALL_TYPE = 16) OR (A.CALL_TYPE_IND = 'OG' AND A.CALL_TYPE IN (1,5,20))))
                    ",
		"tempTable"  : "adme_rated_roaming_call_cdr_tmp",
		"countSourceRecord":"0"
	  }
	]
}