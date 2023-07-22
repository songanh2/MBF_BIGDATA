{
	 "description":"ADME_RATED_ROAMING_DATA",
	 "sqlStatements": [
	  {
		"sql"      	:"
						SELECT
					    	CAST(CONCAT(SUBSTRING(A.CALL_STA_TIME, 7, 4), SUBSTRING(A.CALL_STA_TIME, 4, 2)) AS INTEGER)					AS MO_KEY,
							CAST(SUBSTRING(A.CALL_STA_TIME, 12, 2) AS INTEGER)															AS HOUR_KEY,
							CAST(NULL AS STRING) 																						AS UUID,
							CAST(NULL AS STRING) 																						AS EVT_CD,
							CAST(A.STT AS BIGINT) 																						AS EVT_SEQ_NBR,
							CAST(NULL AS BIGINT) 																						AS PRTIAL_REC_NBR,
							CAST(UNIX_TIMESTAMP(A.CALL_STA_TIME,'dd/MM/yyyy HH:mm:ss') AS TIMESTAMP) 									AS EVT_START_DT,
					        CAST(UNIX_TIMESTAMP(A.CALL_END_TIME,'dd/MM/yyyy HH:mm:ss') AS TIMESTAMP) 									AS EVT_END_DT,
							CAST(NULL AS TIMESTAMP) 																					AS BLLG_START_DT,
							A.FTP_FILENAME  																							AS TAP_FILE_NAME,
							CAST(NULL AS TIMESTAMP) 																					AS TAP_FILE_DT,
							CAST(NULL AS STRING) 																						AS TAP_FILE_PREFIX_CD,
							CAST(NULL AS TIMESTAMP) 																					AS REC_PROCESS_DT,
							CAST(NULL AS STRING) 																						AS SWITCH_CD,
							'RATED-O'																									AS RMNG_DRCTN_CD,
							CAST(NULL AS STRING)																						AS TAP_DRCTN_CD,
							B.ACCT_SRVC_INSTANCE_KEY 																					AS ACCT_SRVC_INSTANCE_KEY,
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
							END 																										AS SERVICE_NBR,
							B.ACCT_KEY  																								AS ACCT_KEY,
							B.BILLABLE_ACCT_KEY 																						AS BILLABLE_ACCT_KEY,
							B.CUST_KEY 																									AS CUST_KEY,
							B.CUST_TYP_CD 																								AS CUST_TYP_CD,
							B.NTWK_QOS_GRP_CD 																							AS NTWK_QOS_GRP_CD,
							CAST(B.ACTIVATION_DT AS TIMESTAMP) 																			AS ACCT_ACTIVATION_DT,
							CAST(B.CBS_ACTIVATION_DT AS TIMESTAMP) 																		AS ACCT_CBS_ACTIVATION_DT,
							B.LFCYCL_STAT_CD 																							AS ACCT_LFCYCL_STAT_CD,
							CAST(B.ACTIVATION_DT AS TIMESTAMP) 																			AS SRVC_ACTIVATION_DT,
							CAST(B.CBS_ACTIVATION_DT AS TIMESTAMP) 																		AS SRVC_CBS_ACTIVATION_DT,
							CAST(B.LFCYCL_STAT_CD AS STRING) 																			AS SRVC_LFCYCL_STAT_CD,
							CAST(B.PROD_LINE_KEY AS INTEGER) 																			AS PROD_LINE_KEY,
							CAST(B.USAGE_PLAN_KEY AS BIGINT) 																			AS USAGE_PLAN_KEY,
							B.USAGE_PLAN_CD 																							AS USAGE_PLAN_CD,
							B.USAGE_PLAN_TYP_CD 																						AS USAGE_PLAN_TYP_CD,
							CAST(B.CRM_OFFER_KEY AS BIGINT) 																			AS OFFER_KEY,
							B.CRM_OFFER_CD 																								AS OFFER_CD,
							CAST(NULL AS DECIMAL(20,0))																                	AS PROD_KEY,
							CAST(NULL AS STRING)																				    	AS PROD_CD,
							'DATA' 																										AS EVT_CLASS_CD,
							A.EVT_CTGRY_CD 																								AS EVT_CTGRY_CD,
							A.CALL_TYP_CD 																								AS EVT_TYP_CD,
							'DATA' 																										AS USAGE_TYP_CD,
							A.CALL_CTGRY_CD 																							AS CALL_CTGRY_CD,
							A.CALL_TYP_CD 																								AS CALL_TYP_CD,
							CAST(NULL AS STRING) 																						AS CALL_STAT_CD,
							CAST(NULL AS STRING) 																						AS CALL_TMNT_RSN_CD,
							CAST(NULL AS STRING) 																						AS NTWK_SESSION_ID,
							CAST(NULL AS STRING) 																						AS BLLG_SESSION_ID,
							CAST(NULL AS STRING) 																						AS QOS_RQSTD,
							CAST(NULL AS STRING) 																						AS QOS_NEGOTIATED,
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
							END 																										AS ORIGNTNG_NBR,
							A.IMSI 																										AS ORIGNTNG_IMSI,
							CAST(NULL AS STRING) 																						AS ORIGNTNG_IMEI,
							CASE WHEN A.LOCATION_INDICATOR = 2 THEN 'I' ELSE 'D' END													AS INTL_DMSTC_IND,
							CAST(NULL AS STRING)  																						AS TESTNG_NBR_IND,
							CAST(NULL AS STRING)  																						AS ONNET_OFFNET_IND,
							CAST(NULL AS STRING)  																						AS NTWK_ACCS_POINT_IP,
							CAST(NULL AS STRING)  																						AS NTWK_SESSION_CONTROL_POINT_IP,
							CAST(NULL AS STRING)  																						AS HOST_IP,
							CAST(NULL AS STRING)  																						AS DOMAIN_NAME,
							CAST(NULL AS STRING)  																						AS ACCESSED_URL,
							CAST(NULL AS STRING)  																						AS DVC_OS_NAME,
							CAST(NULL AS STRING)  																						AS BROWSING_ACTVTY_TYP,
							CAST(NULL AS STRING)  																						AS ORIGNTNG_PLMN_CD,
							CAST(NULL AS STRING)  																						AS ORIGNTNG_NETWORK_TYP,
							SUBSTRING(A.IMSI, 0, 3)  																					AS ORIGNTNG_HOME_COUNTRY_CD,
							CAST(NULL AS STRING)  																						AS ORIGNTNG_HOME_AREA_CD,
							SUBSTRING(A.IMSI, 4, 2)  																					AS ORIGNTNG_HOME_NETWORK_CD,
							(CASE WHEN A.IMSI NOT LIKE '452%' THEN SUBSTRING(A.IMSI,1,3) ELSE NULL END)									AS ORIGNTNG_ROAM_COUNTRY_CD,
							CAST(NULL AS STRING) 																						AS ORIGNTNG_ROAM_AREA_CD,
						    (CASE WHEN A.IMSI NOT LIKE '452%' THEN SUBSTRING(A.IMSI,4,2) ELSE NULL END)									AS ORIGNTNG_ROAM_NETWORK_CD,
							A.CELL_ID   																								AS FIRST_ORIGNTNG_GCI_CD,
							A.CELL_ID 																									AS LAST_ORIGNTNG_GCI_CD,
							CAST(NULL AS bigint) 																						AS ORIGNTNG_OPRTR_KEY	,
							CAST(NULL AS STRING) 																						AS ORIGNTNG_OPRTR_CD	,
							CAST(NULL AS decimal(27,8)) 																				AS ADDR_LAT	,
							CAST(NULL AS decimal(27,8)) 																				AS ADDR_LON	,
							CAST(NULL AS bigint) 																						AS FIRST_CELL_KEY	,
							CAST(NULL AS STRING) 																						AS FIRST_CELL_CD	,
							CAST(NULL AS bigint) 																						AS FIRST_CELL_SITE_KEY	,
							CAST(NULL AS STRING) 																						AS FIRST_CELL_SITE_CD	,
							CAST(NULL AS bigint) 																						AS LAST_CELL_KEY	,
							CAST(NULL AS STRING) 																						AS LAST_CELL_CD	,
							CAST(NULL AS bigint) 																						AS LAST_CELL_SITE_KEY	,
							CAST(NULL AS STRING) 																						AS LAST_CELL_SITE_CD	,
							CAST(NULL AS bigint) 																						AS NTWK_MGNT_CENTRE_KEY	,
							CAST(NULL AS STRING) 																						AS NTWK_MGNT_CENTRE_CD	,
							CAST(NULL AS bigint) 																						AS BSNS_RGN_KEY	,
							CAST(NULL AS STRING) 																						AS BSNS_RGN_CD	,
							CAST(NULL AS bigint) 																						AS BSNS_CLUSTER_KEY	,
							CAST(NULL AS STRING) 																						AS BSNS_CLUSTER_CD	,
							CAST(NULL AS bigint) 																						AS BSNS_MINICLUSTER_KEY	,
							CAST(NULL AS STRING) 																						AS BSNS_MINICLUSTER_CD	,
							CAST(NULL AS bigint) 																						AS GEO_CNTRY_KEY	,
							CAST(NULL AS STRING) 																						AS GEO_CNTRY_CD	,
							CAST(NULL AS bigint) 																						AS GEO_STATE_KEY	,
							CAST(NULL AS STRING) 																						AS GEO_STATE_CD	,
							CAST(NULL AS bigint) 																						AS GEO_DSTRCT_KEY	,
							CAST(NULL AS STRING) 																						AS GEO_DSTRCT_CD	,
							CAST(NULL AS bigint) 																						AS GEO_CITY_KEY	,
							CAST(NULL AS STRING) 																						AS GEO_CITY_CD	,	
							CAST(B.ACQSTN_DT AS TIMESTAMP)  																			AS ACQSTN_DT,
							CAST(B.ACQSTN_BSNS_OUTLET_KEY AS BIGINT)  																	AS ACQSTN_BSNS_OUTLET_KEY,
							B.ACQSTN_BSNS_OUTLET_CD																						AS ACQSTN_BSNS_OUTLET_CD,
							CAST(NULL AS BIGINT)  																						AS BLLG_OPRTR_KEY,
							CAST(NULL AS STRING)  																						AS BLLG_OPRTR_CD,
							CAST(NULL AS STRING)  																						AS BILL_GRP_CD,
							CAST(NULL AS BIGINT)  																						AS BILL_CYCLE_KEY,
							CAST(NULL AS INTEGER)  																						AS CHRG_HEAD_KEY,
							CAST(NULL AS STRING)  																						AS CHRG_HEAD_CD,
							CAST(NULL AS STRING)  																						AS CRNCY_CD,
							CAST(NULL AS STRING)  																						AS USG_UOM_CD,
							CAST(NULL AS STRING) 																						AS INTCONN_CRNCY_CD	,
							CAST(NULL AS decimal(27,8)) 																				AS EXCHG_RATE_VAL	,
							CAST(A.USED_DURATION AS BIGINT)  																			AS DRTN,
							CAST(NULL AS BIGINT)  																						AS RTD_DRTN,
							CAST(NULL AS BIGINT) 																						AS FREE_DRTN,
							CAST(NULL AS BIGINT)  																						AS CHRGD_DRTN,
							CAST(NULL AS BIGINT)  																						AS INTCONN_DRTN,
							CAST(NULL AS BIGINT)  																						AS TOT_FLUX,
							CAST(NULL AS BIGINT)  																						AS UP_FLUX,
							CAST(NULL AS BIGINT)  																						AS DOWN_FLUX,
							CAST(NULL AS STRING)  																						AS FLUX_UOM_CD,
							CAST(NULL AS BIGINT)  																						AS RTD_USG,
							CAST(
							(CASE
								WHEN W1.WALLET_TYP_CD = 'DATA_UNIT_CREDIT' THEN A.TK1_CONSUMED
								ELSE 0
							END) +
							(CASE
								WHEN W2.WALLET_TYP_CD = 'DATA_UNIT_CREDIT' THEN A.TK2_CONSUMED
								ELSE 0
							END) +
							(CASE
								WHEN W3.WALLET_TYP_CD = 'DATA_UNIT_CREDIT' THEN A.TK3_CONSUMED
								ELSE 0
							END) +
							(CASE
								WHEN W4.WALLET_TYP_CD = 'DATA_UNIT_CREDIT' THEN A.TK4_CONSUMED
								ELSE 0
							END) +
							(CASE
								WHEN W5.WALLET_TYP_CD = 'DATA_UNIT_CREDIT' THEN A.TK5_CONSUMED
								ELSE 0
							END) +
							(CASE
								WHEN W6.WALLET_TYP_CD = 'DATA_UNIT_CREDIT' THEN A.TK6_CONSUMED
								ELSE 0
							END) +
							(CASE
								WHEN W7.WALLET_TYP_CD = 'DATA_UNIT_CREDIT' THEN A.TK7_CONSUMED
								ELSE 0
							END) +
							(CASE
								WHEN W8.WALLET_TYP_CD = 'DATA_UNIT_CREDIT' THEN A.TK8_CONSUMED
								ELSE 0
							END) +
							(CASE
								WHEN W9.WALLET_TYP_CD = 'DATA_UNIT_CREDIT' THEN A.TK9_CONSUMED
								ELSE 0
							END) +
							(CASE
								WHEN W10.WALLET_TYP_CD = 'DATA_UNIT_CREDIT' THEN A.TK10_CONSUMED
								ELSE 0
							END) AS BIGINT)  																							AS FREE_USG,
							CAST(NULL AS BIGINT)  																						AS CHRGBL_USG,
							CAST(NULL AS BIGINT)  																						AS INTCONN_USG,
							CAST(NULL AS STRING)  																						AS CHRGNG_PRTY_IND,
							CAST(NULL AS STRING)  																						AS OTHR_CHRGNG_NBR,
							CAST(NULL AS STRING)  																						AS CHRGNG_IND,
							CAST(NULL AS STRING)  																						AS CHRGING_ZONE_CD,
							CAST(NULL AS STRING)  																						AS RTD_CLASS_CD,
							CAST(NULL AS STRING)  																						AS RTNG_INFO,
							CAST(NULL AS DECIMAL(27,8))  																				AS CHRGD_AMT,
							CAST(NULL AS DECIMAL(27,8))  																				AS MARK_UP_AMT,
							CAST(
							(CASE
								WHEN W1.WALLET_TYP_CD = 'SECONDARY_MAIN_BALANCE' THEN A.TK1_CONSUMED
								ELSE 0
							END) +
							(CASE
								WHEN W2.WALLET_TYP_CD = 'SECONDARY_MAIN_BALANCE' THEN A.TK2_CONSUMED
								ELSE 0
							END) +
							(CASE
								WHEN W3.WALLET_TYP_CD = 'SECONDARY_MAIN_BALANCE' THEN A.TK3_CONSUMED
								ELSE 0
							END) +
							(CASE
								WHEN W4.WALLET_TYP_CD = 'SECONDARY_MAIN_BALANCE' THEN A.TK4_CONSUMED
								ELSE 0
							END) +
							(CASE
								WHEN W5.WALLET_TYP_CD = 'SECONDARY_MAIN_BALANCE' THEN A.TK5_CONSUMED
								ELSE 0
							END) +
							(CASE
								WHEN W6.WALLET_TYP_CD = 'SECONDARY_MAIN_BALANCE' THEN A.TK6_CONSUMED
								ELSE 0
							END) +
							(CASE
								WHEN W7.WALLET_TYP_CD = 'SECONDARY_MAIN_BALANCE' THEN A.TK7_CONSUMED
								ELSE 0
							END) +
							(CASE
								WHEN W8.WALLET_TYP_CD = 'SECONDARY_MAIN_BALANCE' THEN A.TK8_CONSUMED
								ELSE 0
							END) +
							(CASE
								WHEN W9.WALLET_TYP_CD = 'SECONDARY_MAIN_BALANCE' THEN A.TK9_CONSUMED
								ELSE 0
							END) +
							(CASE
								WHEN W10.WALLET_TYP_CD = 'SECONDARY_MAIN_BALANCE' THEN A.TK10_CONSUMED
								ELSE 0
							END) AS DECIMAL(27,8))  																					AS DISCOUNT_AMT,
							CAST(NULL AS DECIMAL(27,8))  																				AS TAX_AMT,
							CAST(A.CREDIT_CHARGED +
							(CASE
								WHEN W1.WALLET_TYP_CD IN ( 'MAIN_BALANCE', 'SECONDARY_MAIN_BALANCE') THEN A.TK1_CONSUMED
								ELSE 0
							END) +
							(CASE
								WHEN W2.WALLET_TYP_CD IN ( 'MAIN_BALANCE', 'SECONDARY_MAIN_BALANCE') THEN A.TK2_CONSUMED
								ELSE 0
							END) +
							(CASE
								WHEN W3.WALLET_TYP_CD IN ( 'MAIN_BALANCE', 'SECONDARY_MAIN_BALANCE') THEN A.TK3_CONSUMED
								ELSE 0
							END) +
							(CASE
								WHEN W4.WALLET_TYP_CD IN ( 'MAIN_BALANCE', 'SECONDARY_MAIN_BALANCE') THEN A.TK4_CONSUMED
								ELSE 0
							END) +
							(CASE
								WHEN W5.WALLET_TYP_CD IN ( 'MAIN_BALANCE', 'SECONDARY_MAIN_BALANCE') THEN A.TK5_CONSUMED
								ELSE 0
							END) +
							(CASE
								WHEN W6.WALLET_TYP_CD IN ( 'MAIN_BALANCE', 'SECONDARY_MAIN_BALANCE') THEN A.TK6_CONSUMED
								ELSE 0
							END) +
							(CASE
								WHEN W7.WALLET_TYP_CD IN ( 'MAIN_BALANCE', 'SECONDARY_MAIN_BALANCE') THEN A.TK7_CONSUMED
								ELSE 0
							END) +
							(CASE
								WHEN W8.WALLET_TYP_CD IN ( 'MAIN_BALANCE', 'SECONDARY_MAIN_BALANCE') THEN A.TK8_CONSUMED
								ELSE 0
							END) +
							(CASE
								WHEN W9.WALLET_TYP_CD IN ( 'MAIN_BALANCE', 'SECONDARY_MAIN_BALANCE') THEN A.TK9_CONSUMED
								ELSE 0
							END) +
							(CASE
								WHEN W10.WALLET_TYP_CD IN ( 'MAIN_BALANCE', 'SECONDARY_MAIN_BALANCE') THEN A.TK10_CONSUMED
								ELSE 0
							END) AS DECIMAL(27,8))  																					AS BLLD_AMT,
							CAST(A.CREDIT_CHARGED AS DECIMAL(27,8)) 																	AS RVN_AMT,
							CAST(NULL AS DECIMAL(27,8)) 																				AS INTERNAL_COST_AMT,
							CAST(NULL AS DECIMAL(27,8)) 																				AS INTCONN_COST_AMT,
							CAST(NULL AS DECIMAL(27,8)) 																				AS INTCONN_RVN_AMT,
							CAST(NULL AS STRING) 																						AS RMNG_BANK_TYP	,
							CAST(NULL AS STRING) 																						AS RMNG_BANK_UOM_CD	,
							CAST(NULL AS decimal(27,8)) 																				AS RMNG_BANK_BALANCE_BEFR_IMPACT	,
							CAST(NULL AS decimal(27,8)) 																				AS RMNG_BANK_BALANCE_AFTR_IMPACT	,
							CAST(NULL AS STRING)  																						AS BLLG_STAT_RSN_CD,
							CAST(B.LOYALTY_RANK_SCORE AS DECIMAL(12,4))  																AS LOYALTY_RANK_SCORE,
							CAST(B.LOYALTY_SCORE_DT AS TIMESTAMP)  																		AS LOYALTY_SCORE_DT,
							CAST(B.CREDIT_SCORE AS DECIMAL(12,4))  																		AS CREDIT_SCORE,
							B.CREDIT_CLASS_CD  																							AS CREDIT_CLASS_CD,
							B.CREDIT_SCORE_METHOD 																						AS CREDIT_SCORE_METHOD,
							CAST(B.CREDIT_SCORE_DT AS TIMESTAMP)  																		AS CREDIT_SCORE_DT,
							CAST(B.RISK_IND AS INTEGER)  																				AS RISK_IND,
							CAST(NULL AS STRING)  																						AS ACCT_SEGMENT_CD,
							CAST(NULL AS STRING)  																						AS ACCT_SEGMENT_DT,
							CAST(NULL AS STRING)  																						AS CMPGN_CD,
							CAST(6 AS BIGINT)  																						AS SRC_SYS_KEY,
							'ICC'																								AS SRC_SYS_CD,
							CURRENT_TIMESTAMP()  																						AS LOAD_DT,
							'1'  																										AS CURR_IND,
							CAST (1 AS INTEGER)  																						AS WRHS_ID
						FROM 
						MBF_STAGE.STG_CDR_IN_ENRICH A
							
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
																						END ) AND B.DAY_KEY = '$day_key'
						LEFT JOIN MBF_BIGDATA.ADMR_CELL C ON A.CELL_ID = C.CELL_GCI_CD AND C.DAY_KEY = '$day_key'
						LEFT JOIN MBF_BIGDATA.ADMR_WALLET W1 ON LOWER(A.TK1) = W1.WALLET_CBS_CD AND W1.DAY_KEY = '$day_key' AND W1.USAGE_TYP_CD IN ('CONVERGENT', 'DATA') AND W1.ROW_NUM = 1
						LEFT JOIN MBF_BIGDATA.ADMR_WALLET W2 ON LOWER(A.TK2) = W2.WALLET_CBS_CD AND W2.DAY_KEY = '$day_key' AND W2.USAGE_TYP_CD IN ('CONVERGENT', 'DATA') AND W2.ROW_NUM = 1
						LEFT JOIN MBF_BIGDATA.ADMR_WALLET W3 ON LOWER(A.TK3) = W3.WALLET_CBS_CD AND W3.DAY_KEY = '$day_key' AND W3.USAGE_TYP_CD IN ('CONVERGENT', 'DATA') AND W3.ROW_NUM = 1
						LEFT JOIN MBF_BIGDATA.ADMR_WALLET W4 ON LOWER(A.TK4) = W4.WALLET_CBS_CD AND W4.DAY_KEY = '$day_key' AND W4.USAGE_TYP_CD IN ('CONVERGENT', 'DATA') AND W4.ROW_NUM = 1
						LEFT JOIN MBF_BIGDATA.ADMR_WALLET W5 ON LOWER(A.TK5) = W5.WALLET_CBS_CD AND W5.DAY_KEY = '$day_key' AND W5.USAGE_TYP_CD IN ('CONVERGENT', 'DATA') AND W5.ROW_NUM = 1
						LEFT JOIN MBF_BIGDATA.ADMR_WALLET W6 ON LOWER(A.TK6) = W6.WALLET_CBS_CD AND W6.DAY_KEY = '$day_key' AND W6.USAGE_TYP_CD IN ('CONVERGENT', 'DATA') AND W6.ROW_NUM = 1
						LEFT JOIN MBF_BIGDATA.ADMR_WALLET W7 ON LOWER(A.TK7) = W7.WALLET_CBS_CD AND W7.DAY_KEY = '$day_key' AND W7.USAGE_TYP_CD IN ('CONVERGENT', 'DATA') AND W7.ROW_NUM = 1
						LEFT JOIN MBF_BIGDATA.ADMR_WALLET W8 ON LOWER(A.TK8) = W8.WALLET_CBS_CD AND W8.DAY_KEY = '$day_key' AND W8.USAGE_TYP_CD IN ('CONVERGENT', 'DATA') AND W8.ROW_NUM = 1
						LEFT JOIN MBF_BIGDATA.ADMR_WALLET W9 ON LOWER(A.TK9) = W9.WALLET_CBS_CD AND W9.DAY_KEY = '$day_key' AND W9.USAGE_TYP_CD IN ('CONVERGENT', 'DATA') AND W9.ROW_NUM = 1
						LEFT JOIN MBF_BIGDATA.ADMR_WALLET W10 ON LOWER(A.TK10) = W10.WALLET_CBS_CD AND W10.DAY_KEY = '$day_key' AND W10.USAGE_TYP_CD IN ('CONVERGENT', 'DATA') AND W10.ROW_NUM = 1
						WHERE  
						A.CALL_TYPE IN (189) AND A.CALL_TYPE_IND = 'GPRS' AND (NVL(A.LOCATION_INDICATOR,-1) = 2 OR NVL(A.CELL_ID,'') LIKE '452-02%' OR NVL(A.LOCATION,'') like '8491%') AND  A.DAY_KEY='$day_key' 
						
		",
		"tempTable"  : "CDR_DATA_TMP",
		"countSourceRecord":"1"
	  } ,
	   {
	  "sql"      	:"
			SELECT
					CAST(SUBSTR('$day_key',1,6) AS INTEGER)	AS MO_KEY,
					CAST(NULL AS integer) 					AS HOUR_KEY	,
					CAST(NULL AS STRING) 					AS UUID	,
					CAST(NULL AS STRING) 					AS EVT_CD	,
					CAST(NULL AS bigint) 					AS EVT_SEQ_NBR	,
					CAST(NULL AS bigint) 					AS PRTIAL_REC_NBR	,
					CAST(NULL AS timestamp) 				AS EVT_START_DT	,
					CAST(NULL AS timestamp) 				AS EVT_END_DT	,
					CAST(NULL AS timestamp) 				AS BLLG_START_DT	,
					CAST(NULL AS STRING) 					AS TAP_FILE_NAME	,
					CAST(NULL AS timestamp) 				AS TAP_FILE_DT	,
					CAST(NULL AS STRING) 					AS TAP_FILE_PREFIX_CD	,
					CAST(NULL AS timestamp) 				AS REC_PROCESS_DT	,
					CAST(NULL AS STRING) 					AS SWITCH_CD	,
					CASE WHEN A.SERVICE_NAME ='Thue bao VMS CVQT' THEN 'O' ELSE 'I' END 						AS RMNG_DRCTN_CD,
					CAST(NULL AS STRING) 					AS TAP_DRCTN_CD	, 
					B.ACCT_SRVC_INSTANCE_KEY 				AS ACCT_SRVC_INSTANCE_KEY,
					B.SERVICE_NBR							AS SERVICE_NBR,
					B.ACCT_KEY 								AS ACCT_KEY,
					B.BILLABLE_ACCT_KEY		 				AS BILLABLE_ACCT_KEY	,
					B.CUST_KEY				 				AS CUST_KEY					,
					B.CUST_TYP_CD			 				AS CUST_TYP_CD					,
					B.NTWK_QOS_GRP_CD		 				AS NTWK_QOS_GRP_CD				,
					B.ACTIVATION_DT	 	       				AS ACCT_ACTIVATION_DT			,
					B.CBS_ACTIVATION_DT 					AS ACCT_CBS_ACTIVATION_DT		,
					B.LFCYCL_STAT_CD	 					AS ACCT_LFCYCL_STAT_CD			,
					B.ACTIVATION_DT	 						AS SRVC_ACTIVATION_DT			,
					B.CBS_ACTIVATION_DT 					AS SRVC_CBS_ACTIVATION_DT		,
					B.LFCYCL_STAT_CD	 					AS SRVC_LFCYCL_STAT_CD			,
					B.PROD_LINE_KEY			 				AS PROD_LINE_KEY				,
					B.USAGE_PLAN_KEY		 				AS USAGE_PLAN_KEY				,
					B.USAGE_PLAN_CD			 				AS USAGE_PLAN_CD				,
					B.USAGE_PLAN_TYP_CD		 				AS USAGE_PLAN_TYP_CD			,	
					B.CRM_OFFER_KEY				 			AS OFFER_KEY					,
					B.CRM_OFFER_CD				 			AS OFFER_CD					,
					CAST(NULL AS decimal(20,0)) 			AS PROD_KEY	,
					CAST(NULL AS STRING) 					AS PROD_CD	,
					'DATA' 									AS EVT_CLASS_CD,
					'DATA_RMNG_ISD' 						AS EVT_CTGRY_CD	,
					'DATA_RMNG_ISD' 						AS EVT_TYP_CD	,
					'DATA' 									AS USAGE_TYP_CD	,
					'DATA_RMNG_ISD' 						AS CALL_CTGRY_CD	,
					'DATA_RMNG_ISD' 						AS CALL_TYP_CD	,
					CAST(NULL AS STRING) 					AS CALL_STAT_CD	,
					CAST(NULL AS STRING) 					AS CALL_TMNT_RSN_CD	,
					CAST(NULL AS STRING) 					AS NTWK_SESSION_ID	,
					CAST(NULL AS STRING) 					AS  BLLG_SESSION_ID	,
					CAST(NULL AS STRING) 					AS  QOS_RQSTD	,
					CAST(NULL AS STRING) 					AS  QOS_NEGOTIATED	,
					CAST(NULL AS STRING) 					AS ORIGNTNG_NBR	,
					A.IMSI 									AS ORIGNTNG_IMSI	,
					CAST(NULL AS STRING) 					AS ORIGNTNG_IMEI	,
					'I' 									AS INTL_DMSTC_IND	,
					CAST(NULL AS STRING) 					AS TESTNG_NBR_IND	,
					'1' 									AS ONNET_OFFNET_IND	,
					CAST(NULL AS STRING) 					AS NTWK_ACCS_POINT_IP	,
					CAST(NULL AS STRING) 					AS NTWK_SESSION_CONTROL_POINT_IP	,
					CAST(NULL AS STRING) 					AS HOST_IP	,
					CAST(NULL AS STRING) 					AS DOMAIN_NAME	,
					CAST(NULL AS STRING) 					AS ACCESSED_URL	,
					CAST(NULL AS STRING) 					AS DVC_OS_NAME	,
					CAST(NULL AS STRING) 					AS BROWSING_ACTVTY_TYP	,
					CAST(NULL AS STRING) 					AS ORIGNTNG_PLMN_CD	,
					CAST(NULL AS STRING) 					AS ORIGNTNG_NETWORK_TYP	,
					'452' 									AS ORIGNTNG_HOME_COUNTRY_CD	,
					CAST(NULL AS STRING) 					AS ORIGNTNG_HOME_AREA_CD	,
					'01' 									AS ORIGNTNG_HOME_NETWORK_CD	,
					D.MCC_CD 								AS ORIGNTNG_ROAM_COUNTRY_CD	,
					CAST(NULL AS STRING) 					AS ORIGNTNG_ROAM_AREA_CD	,
					D.MNC_CD 								AS ORIGNTNG_ROAM_NETWORK_CD	,
					CAST(NULL AS STRING) 					AS FIRST_ORIGNTNG_GCI_CD	,
					CAST(NULL AS STRING) 					AS LAST_ORIGNTNG_GCI_CD	,
					CAST(NULL AS bigint) 					AS ORIGNTNG_OPRTR_KEY	,
					CAST(NULL AS STRING) 					AS ORIGNTNG_OPRTR_CD	,
					CAST(NULL AS decimal(27,8)) 			AS ADDR_LAT	,
					CAST(NULL AS decimal(27,8)) 			AS ADDR_LON	,
					CAST(NULL AS bigint) 					AS FIRST_CELL_KEY	,
					CAST(NULL AS STRING) 					AS FIRST_CELL_CD	,
					CAST(NULL AS bigint) 					AS FIRST_CELL_SITE_KEY	,
					CAST(NULL AS STRING) 					AS FIRST_CELL_SITE_CD	,
					CAST(NULL AS bigint) 					AS LAST_CELL_KEY	,
					CAST(NULL AS STRING) 					AS LAST_CELL_CD	,
					CAST(NULL AS bigint) 					AS LAST_CELL_SITE_KEY	,
					CAST(NULL AS STRING) 					AS LAST_CELL_SITE_CD	,
					CAST(NULL AS bigint) 					AS NTWK_MGNT_CENTRE_KEY	,
					CAST(NULL AS STRING) 					AS NTWK_MGNT_CENTRE_CD	,
					CAST(NULL AS bigint) 					AS BSNS_RGN_KEY	,
					CAST(NULL AS STRING) 					AS BSNS_RGN_CD	,
					CAST(NULL AS bigint) 					AS BSNS_CLUSTER_KEY	,
					CAST(NULL AS STRING) 					AS BSNS_CLUSTER_CD	,
					CAST(NULL AS bigint) 					AS BSNS_MINICLUSTER_KEY	,
					CAST(NULL AS STRING) 					AS BSNS_MINICLUSTER_CD	,
					CAST(NULL AS bigint) 					AS GEO_CNTRY_KEY	,
					CAST(NULL AS STRING) 					AS GEO_CNTRY_CD	,
					CAST(NULL AS bigint) 					AS GEO_STATE_KEY	,
					CAST(NULL AS STRING) 					AS GEO_STATE_CD	,
					CAST(NULL AS bigint) 					AS GEO_DSTRCT_KEY	,
					CAST(NULL AS STRING) 					AS GEO_DSTRCT_CD	,
					CAST(NULL AS bigint) 					AS GEO_CITY_KEY	,
					CAST(NULL AS STRING) 					AS GEO_CITY_CD	,
					CAST(NULL AS timestamp) 				AS ACQSTN_DT	,
					CAST(NULL AS bigint) 					AS ACQSTN_BSNS_OUTLET_KEY	,
					CAST(NULL AS STRING) 					AS ACQSTN_BSNS_OUTLET_CD	,
					CAST(NULL AS bigint) 					AS BLLG_OPRTR_KEY	,
					CAST(NULL AS STRING) 					AS BLLG_OPRTR_CD	,
					CAST(NULL AS STRING) 					AS BILL_GRP_CD	,
					CAST(NULL AS bigint) 					AS BILL_CYCLE_KEY	,
					CAST(NULL AS integer) 					AS CHRG_HEAD_KEY	,
					CAST(NULL AS STRING) 					AS CHRG_HEAD_CD	,
					CAST(NULL AS STRING) 					AS CRNCY_CD	,
					CAST(NULL AS STRING) 					AS USG_UOM_CD	,
					CAST(NULL AS STRING) 					AS INTCONN_CRNCY_CD	,
					CAST(NULL AS decimal(27,8)) 			AS EXCHG_RATE_VAL	,
					CAST(A.DURATION AS BIGINT) 				AS DRTN	,
					CAST(A.BLOCK_DURATION AS BIGINT) 		AS RTD_DRTN	,
					CAST(NULL AS bigint) 					AS FREE_DRTN	,
					CAST(A.DURATION AS BIGINT) 				AS CHRGD_DRTN	,
					CAST(NULL AS bigint) 					AS INTCONN_DRTN	,
					CAST(A.VOLUME AS bigint) 				AS TOT_FLUX	,
					CAST(NULL AS bigint) 					AS UP_FLUX	,
					CAST(NULL AS bigint) 					AS DOWN_FLUX	,
					CAST(NULL AS STRING) 					AS FLUX_UOM_CD	,
					CAST(A.VOLUME_CHARGED AS BIGINT) 		AS RTD_USG	,
					CAST(NULL AS BIGINT) 					AS FREE_USG	,
					CAST(A.VOLUME_CHARGED AS BIGINT) 		AS CHRGBL_USG	,
					CAST(NULL AS bigint) 					AS INTCONN_USG	,
					CAST(NULL AS STRING) 					AS CHRGNG_PRTY_IND	,
					CAST(NULL AS STRING) 					AS OTHR_CHRGNG_NBR	,
					CAST(NULL AS STRING) 					AS CHRGNG_IND	,
					CAST(NULL AS STRING) 					AS CHRGING_ZONE_CD	,
					CAST(NULL AS STRING) 					AS RTD_CLASS_CD	,
					CAST(NULL AS STRING) 					AS RTNG_INFO	,
					CAST(NULL AS decimal(27,8)) 			AS CHRGD_AMT	,
					CAST(NULL AS decimal(27,8)) 			AS MARK_UP_AMT	,
					CAST(NULL AS decimal(27,8)) 			AS DISCOUNT_AMT	,
					CAST(NULL AS decimal(27,8)) 			AS TAX_AMT	,
					CAST(NULL AS decimal(27,8)) 			AS BLLD_AMT	,
					CAST(nvl(A.DOANH_THU_ZONE_VND,0) AS decimal(27,8)) AS RVN_AMT	,
					CAST(NULL AS decimal(27,8)) 			AS INTERNAL_COST_AMT	,
					CAST(NULL AS decimal(27,8)) 			AS INTCONN_COST_AMT	,
					CAST(NULL AS decimal(27,8)) 			AS INTCONN_RVN_AMT	,
					CAST(NULL AS STRING) 					AS RMNG_BANK_TYP	,
					CAST(NULL AS STRING) 					AS RMNG_BANK_UOM_CD	,
					CAST(NULL AS decimal(27,8)) 			AS RMNG_BANK_BALANCE_BEFR_IMPACT	,
					CAST(NULL AS decimal(27,8)) 			AS RMNG_BANK_BALANCE_AFTR_IMPACT	,
					CAST(NULL AS STRING) 					AS BLLG_STAT_RSN_CD	,
					B.LOYALTY_RANK_SCORE					AS LOYALTY_RANK_SCORE	,
					B.LOYALTY_SCORE_DT						AS LOYALTY_SCORE_DT			,
					B.CREDIT_SCORE							AS CREDIT_SCORE			,
					B.CREDIT_CLASS_CD						AS CREDIT_CLASS_CD		,
					B.CREDIT_SCORE_METHOD					AS CREDIT_SCORE_METHOD	,
					B.CREDIT_SCORE_DT						AS CREDIT_SCORE_DT		,
					B.RISK_IND								AS RISK_IND				,
					CAST(NULL AS STRING)					AS ACCT_SEGMENT_CD	,
					CAST(NULL AS STRING)					AS ACCT_SEGMENT_DT	,
					CAST(NULL AS STRING) 					AS CMPGN_CD	,
					CAST(26 AS BIGINT) 						AS SRC_SYS_KEY	,
					'BILLING_GW' 							AS SRC_SYS_CD	,
					CURRENT_TIMESTAMP() 					AS LOAD_DT	,
					'1' 									AS CURR_IND	,
					1 										AS WRHS_ID	
			FROM
			MBF_DATALAKE.IR_RPT_IMSI_IOT_VNDATE A
			
			LEFT JOIN MBF_BIGDATA.ADMR_ACCOUNT_SERVICE B ON B.IMSI = A.IMSI AND B.DAY_KEY = '$day_key'
			
			LEFT JOIN MBF_BIGDATA.ADMR_EXTERNAL_CSP D ON A.TADIGNAME  = D.EXTRNL_CSP_CD 
														  AND ((D.VSTATUS = 'Y' AND D.CSP_TYP_CD = 'IRP') OR D.CSP_TYP_CD = 'MOBILE') AND D.DAY_KEY = '$day_key'
			WHERE A.DAY_KEY = '$day_key' AND A.SERVICE_TYPE = 'GPRS'
			",

		"tempTable"  : "IR_RPT_TMP",
		"countSourceRecord":"0"
	  } ,  
	  
	  {
	  "sql"      	:"
			SELECT
					 MO_KEY,
					 HOUR_KEY	,
					 UUID	,
					 EVT_CD	,
					 EVT_SEQ_NBR	,
					 PRTIAL_REC_NBR	,
					 EVT_START_DT	,
					 EVT_END_DT	,
					 BLLG_START_DT	,
					 TAP_FILE_NAME	,
					 TAP_FILE_DT	,
					 TAP_FILE_PREFIX_CD	,
					 REC_PROCESS_DT	,
					 SWITCH_CD	,
					 RMNG_DRCTN_CD,
					 TAP_DRCTN_CD	, 
					 ACCT_SRVC_INSTANCE_KEY,
					 SERVICE_NBR,
					 ACCT_KEY,
					 BILLABLE_ACCT_KEY	,
					 CUST_KEY					,
					 CUST_TYP_CD					,
					 NTWK_QOS_GRP_CD				,
					 ACCT_ACTIVATION_DT			,
					 ACCT_CBS_ACTIVATION_DT		,
					 ACCT_LFCYCL_STAT_CD			,
					 SRVC_ACTIVATION_DT			,
					 SRVC_CBS_ACTIVATION_DT		,
					 SRVC_LFCYCL_STAT_CD			,
					 PROD_LINE_KEY				,
					 USAGE_PLAN_KEY				,
					 USAGE_PLAN_CD				,
					 USAGE_PLAN_TYP_CD			,	
					 OFFER_KEY					,
					 OFFER_CD					,
					 PROD_KEY	,
					 PROD_CD	,
					 EVT_CLASS_CD,
					 EVT_CTGRY_CD	,
					 EVT_TYP_CD	,
					 USAGE_TYP_CD	,
					 CALL_CTGRY_CD	,
					 CALL_TYP_CD	,
					 CALL_STAT_CD	,
					 CALL_TMNT_RSN_CD	,
					 NTWK_SESSION_ID	,
					 BLLG_SESSION_ID	,
					 QOS_RQSTD	,
					 QOS_NEGOTIATED	,
					 ORIGNTNG_NBR	,
					 ORIGNTNG_IMSI	,
					 ORIGNTNG_IMEI	,
					 INTL_DMSTC_IND	,
					 TESTNG_NBR_IND	,
					 ONNET_OFFNET_IND	,
					 NTWK_ACCS_POINT_IP	,
					 NTWK_SESSION_CONTROL_POINT_IP	,
					 HOST_IP	,
					 DOMAIN_NAME	,
					 ACCESSED_URL	,
					 DVC_OS_NAME	,
					 BROWSING_ACTVTY_TYP	,
					 ORIGNTNG_PLMN_CD	,
					 ORIGNTNG_NETWORK_TYP	,
					 ORIGNTNG_HOME_COUNTRY_CD	,
					 ORIGNTNG_HOME_AREA_CD	,
					 ORIGNTNG_HOME_NETWORK_CD	,
					 ORIGNTNG_ROAM_COUNTRY_CD	,
					 ORIGNTNG_ROAM_AREA_CD	,
					 ORIGNTNG_ROAM_NETWORK_CD	,
					 FIRST_ORIGNTNG_GCI_CD	,
					 LAST_ORIGNTNG_GCI_CD	,
					 ORIGNTNG_OPRTR_KEY	,
					 ORIGNTNG_OPRTR_CD	,
					 ADDR_LAT	,
					 ADDR_LON	,
					 FIRST_CELL_KEY	,
					 FIRST_CELL_CD	,
					 FIRST_CELL_SITE_KEY	,
					 FIRST_CELL_SITE_CD	,
					 LAST_CELL_KEY	,
					 LAST_CELL_CD	,
					 LAST_CELL_SITE_KEY	,
					 LAST_CELL_SITE_CD	,
					 NTWK_MGNT_CENTRE_KEY	,
					 NTWK_MGNT_CENTRE_CD	,
					 BSNS_RGN_KEY	,
					 BSNS_RGN_CD	,
					 BSNS_CLUSTER_KEY	,
					 BSNS_CLUSTER_CD	,
					 BSNS_MINICLUSTER_KEY	,
					 BSNS_MINICLUSTER_CD	,
					 GEO_CNTRY_KEY	,
					 GEO_CNTRY_CD	,
					 GEO_STATE_KEY	,
					 GEO_STATE_CD	,
					 GEO_DSTRCT_KEY	,
					 GEO_DSTRCT_CD	,
					 GEO_CITY_KEY	,
					 GEO_CITY_CD	,
					 ACQSTN_DT	,
					 ACQSTN_BSNS_OUTLET_KEY	,
					 ACQSTN_BSNS_OUTLET_CD	,
					 BLLG_OPRTR_KEY	,
					 BLLG_OPRTR_CD	,
					 BILL_GRP_CD	,
					 BILL_CYCLE_KEY	,
					 CHRG_HEAD_KEY	,
					 CHRG_HEAD_CD	,
					 CRNCY_CD	,
					 USG_UOM_CD	,
					 INTCONN_CRNCY_CD	,
					 EXCHG_RATE_VAL	,
					 DRTN	,
					 RTD_DRTN	,
					 FREE_DRTN	,
					 CHRGD_DRTN	,
					 INTCONN_DRTN	,
					 TOT_FLUX	,
					 UP_FLUX	,
					 DOWN_FLUX	,
					 FLUX_UOM_CD	,
					 RTD_USG	,
					 FREE_USG	,
					 CHRGBL_USG	,
					 INTCONN_USG	,
					 CHRGNG_PRTY_IND	,
					 OTHR_CHRGNG_NBR	,
					 CHRGNG_IND	,
					 CHRGING_ZONE_CD	,
					 RTD_CLASS_CD	,
					 RTNG_INFO	,
					 CHRGD_AMT	,
					 MARK_UP_AMT	,
					 DISCOUNT_AMT	,
					 TAX_AMT	,
					 BLLD_AMT	,
					 RVN_AMT	,
					 INTERNAL_COST_AMT	,
					 INTCONN_COST_AMT	,
					 INTCONN_RVN_AMT	,
					 RMNG_BANK_TYP	,
					 RMNG_BANK_UOM_CD	,
					 RMNG_BANK_BALANCE_BEFR_IMPACT	,
					 RMNG_BANK_BALANCE_AFTR_IMPACT	,
					 BLLG_STAT_RSN_CD	,
					 LOYALTY_RANK_SCORE	,
					 LOYALTY_SCORE_DT			,
					 CREDIT_SCORE			,
					 CREDIT_CLASS_CD		,
					 CREDIT_SCORE_METHOD	,
					 CREDIT_SCORE_DT		,
					 RISK_IND				,
					 ACCT_SEGMENT_CD	,
					 ACCT_SEGMENT_DT	,
					 CMPGN_CD	,
					 SRC_SYS_KEY	,
					 SRC_SYS_CD	,
					 LOAD_DT	,
					 CURR_IND	,
					 WRHS_ID	
			FROM
			(
				SELECT * FROM CDR_DATA_TMP
				UNION ALL 
				SELECT * FROM IR_RPT_TMP
			
			) A
			",

		"tempTable"  : "ADME_RATED_ROAMING_DATA_TMP",
		"countSourceRecord":"0"
	  }   	  
	]
}