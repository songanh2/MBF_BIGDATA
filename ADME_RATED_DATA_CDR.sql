{
	 "description":"ADME_RATED_DATA_CDR",
	 "sqlStatements": [
	  {
		"sql"      : "
						SELECT * FROM
						(
						  SELECT 
							ROW_NUMBER() OVER (PARTITION BY C4.CELL_KEY_4G ORDER BY C4.cell_gci_cd DESC) AS ROWNUM, C4.* 
						  FROM 
						  (
							SELECT      
							  CASE WHEN CELL_TYP_CD = '4G' THEN CONCAT(SPLIT(CELL_KEY,'[\\-]')[0],'-',SPLIT(CELL_KEY,'[\\-]')[1],'-',SPLIT(CELL_KEY,'[\\-]')[3],'-',SPLIT(CELL_KEY,'[\\-]')[4])
							  ELSE CELL_KEY END AS CELL_KEY_4G,
							  *
							FROM MBF_BIGDATA.ADMR_CELL WHERE DAY_KEY = '$day_key' AND nvl(CELL_DSCR,'') NOT LIKE '%TEST%'
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
							CONCAT(SUBSTRING(A.CALL_STA_TIME, 7, 4), SUBSTRING(A.CALL_STA_TIME, 4, 2)) 									AS MO_KEY,
							SUBSTRING(A.CALL_STA_TIME, 12, 2) 																			AS HOUR_KEY,
							CAST(NULL AS STRING) 																						AS UUID,
							CAST(NULL AS STRING) 																						AS EVT_CD,
							CAST(A.STT AS BIGINT) 																						AS EVT_SEQ_NBR,
							CAST(NULL AS BIGINT) 																						AS PRTIAL_REC_NBR,
							CAST(UNIX_TIMESTAMP(A.CALL_STA_TIME,'dd/MM/yyyy HH:mm:ss') AS TIMESTAMP) 									AS EVT_START_DT,
							CAST(UNIX_TIMESTAMP(A.CALL_END_TIME,'dd/MM/yyyy HH:mm:ss') AS TIMESTAMP) 									AS EVT_END_DT,
							CAST(NULL AS TIMESTAMP) 																					AS EVT_BLLG_START_DT,
							A.FTP_FILENAME  																							AS FILE_NAME,
							CAST(NULL AS TIMESTAMP) 																					AS FILE_DT,
							CAST(NULL AS STRING) 																						AS FILE_PREFIX_CD,
							CAST(NULL AS TIMESTAMP) 																					AS REC_PROCESS_DT,
							CAST(NULL AS STRING) 																						AS SWITCH_CD,
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
							CAST(NVL(A.TREE_ID,157) AS DECIMAL(20,0))																	AS PROD_KEY,
							P.PROD_CD																									AS PROD_CD,
							A.ITEM_ID																									AS ITEM_KEY,
							A.SUB_ITEM_ID																								AS SUB_ITEM_CD,
							'DATA' 																										AS EVT_CLASS_CD,
							A.EVT_CTGRY_CD 																								AS EVT_CTGRY_CD,
							A.CALL_TYP_CD 																								AS EVT_TYP_CD,
							'VND' 																										AS CRNCY_CD,
							'BYTES' 																									AS USG_UOM_CD,
							'VND' 																										AS INTCONN_CRNCY_CD,
							CAST(NULL AS DECIMAL(27,8)) 																				AS EXCHG_RATE_VAL,
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
							'D'																											AS INTL_DMSTC_IND,
							CAST(NULL AS STRING)																						AS RMNG_IND,	
							CAST(NULL AS STRING)  																						AS FNF_IND,
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
							CAST(NULL AS STRING)																						AS ORIGNTNG_ROAM_COUNTRY_CD,
							CAST(NULL AS STRING) 																						AS ORIGNTNG_ROAM_AREA_CD,
						    CAST(NULL AS STRING)																						AS ORIGNTNG_ROAM_NETWORK_CD,
							A.CELL_ID   																								AS FIRST_ORIGNTNG_GCI_CD,
							A.CELL_ID 																									AS LAST_ORIGNTNG_GCI_CD,
							CAST(NULL AS BIGINT) 																						AS ORIGNTNG_OPRTR_KEY,
							CAST(NULL AS STRING) 																						AS ORIGNTNG_OPRTR_CD,
							CAST(NULL AS BIGINT)  																						AS BLLG_OPRTR_KEY,
							CAST(NULL AS STRING)  																						AS BLLG_OPRTR_CD,
							NVL(C.ADDR_LAT,C4G.ADDR_LAT)																				AS ADDR_LAT,
							NVL(C.ADDR_LON,C4G.ADDR_LON)																				AS ADDR_LON,								
							NVL(C.CELL_KEY,C4G.CELL_KEY)				 																AS FIRST_CELL_KEY,
							NVL(C.CELL_CD,C4G.CELL_CD) 				 																	AS FIRST_CELL_CD,
							NVL(C.CELL_SITE_KEY,C4G.CELL_SITE_KEY) 		 																AS FIRST_CELL_SITE_KEY,								
							NVL(C.CELL_SITE_CD,C4G.CELL_SITE_CD)			 															AS FIRST_CELL_SITE_CD,								
							NVL(C.CELL_TYP_CD,C4G.CELL_TYP_CD)																			AS FIRST_CELL_TYPE_CD,								
							NVL(C.CELL_KEY,C4G.CELL_KEY)				 																AS LAST_CELL_KEY,
							NVL(C.CELL_CD,C4G.CELL_CD)			 																		AS LAST_CELL_CD,
							NVL(C.CELL_SITE_KEY,C4G.CELL_SITE_KEY) 		 																AS LAST_CELL_SITE_KEY,																
							NVL(C.CELL_SITE_CD,C4G.CELL_SITE_CD)			 															AS LAST_CELL_SITE_CD,								
							NVL(C.CELL_TYP_CD,C4G.CELL_TYP_CD) 																			AS LAST_CELL_TYPE_CD,								
							NVL(C.NTWK_MGNT_CENTRE_KEY,C4G.NTWK_MGNT_CENTRE_KEY) 	 													AS NTWK_MGNT_CENTRE_KEY,
							NVL(C.NTWK_MGNT_CENTRE_CD,C4G.NTWK_MGNT_CENTRE_CD) 	 														AS NTWK_MGNT_CENTRE_CD,
							NVL(C.BSNS_RGN_KEY,C4G.BSNS_RGN_KEY)			 															AS BSNS_RGN_KEY,
							NVL(C.BSNS_RGN_CD,C4G.BSNS_RGN_CD)			 																AS BSNS_RGN_CD,
							NVL(C.BSNS_CLUSTER_KEY,C4G.BSNS_CLUSTER_KEY) 		 														AS BSNS_CLUSTER_KEY,
							NVL(C.BSNS_CLUSTER_CD,C4G.BSNS_CLUSTER_CD)		 															AS BSNS_CLUSTER_CD,
							NVL(C.BSNS_MINICLUSTER_KEY,C4G.BSNS_MINICLUSTER_KEY)	 													AS BSNS_MINICLUSTER_KEY,
							NVL(C.BSNS_MINICLUSTER_CD,C4G.BSNS_MINICLUSTER_CD) 	 														AS BSNS_MINICLUSTER_CD,
							NVL(C.GEO_CNTRY_KEY,C4G.GEO_CNTRY_KEY)	 																	AS GEO_CNTRY_KEY,
							NVL(C.GEO_CNTRY_CD,C4G.GEO_CNTRY_CD)			 															AS GEO_CNTRY_CD,
							NVL(C.GEO_STATE_KEY,C4G.GEO_STATE_KEY) 		 																AS GEO_STATE_KEY,
							NVL(C.GEO_STATE_CD,C4G.GEO_STATE_CD) 			 															AS GEO_STATE_CD,
							NVL(C.GEO_DSTRCT_KEY,C4G.GEO_DSTRCT_KEY) 		 															AS GEO_DSTRCT_KEY,
							NVL(C.GEO_DSTRCT_CD,C4G.GEO_DSTRCT_CD) 		 																AS GEO_DSTRCT_CD,
							NVL(C.GEO_CITY_KEY,C4G.GEO_CITY_KEY) 			 															AS GEO_CITY_KEY,
							NVL(C.GEO_CITY_CD,C4G.GEO_CITY_CD) 			 																AS GEO_CITY_CD,		

							
							CAST(B.ACQSTN_DT AS TIMESTAMP)  																			AS ACQSTN_DT,
							CAST(B.ACQSTN_BSNS_OUTLET_KEY AS BIGINT)  																	AS ACQSTN_BSNS_OUTLET_KEY,
							B.ACQSTN_BSNS_OUTLET_CD																						AS ACQSTN_BSNS_OUTLET_CD,
							CAST(NULL AS STRING)  																						AS BILL_GRP_CD,
							CAST(NULL AS BIGINT)  																						AS BILL_CYCLE_KEY,
							CAST(NULL AS INTEGER)  																						AS CHRG_HEAD_KEY,
							CAST(NULL AS STRING)  																						AS CHRG_HEAD_CD,
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
							CAST
							(CASE
								WHEN A.ACC_PROFILE NOT LIKE 'TS%' THEN A.CREDIT_CHARGED
								ELSE 0
							END AS DECIMAL(27,8))  																						AS PREPAID_MAIN_ACCT_DECRMNT_AMT,
							CAST
							(CASE
								WHEN A.ACC_PROFILE LIKE 'TS%' THEN A.CREDIT_CHARGED
								ELSE 0
							END AS DECIMAL(27,8))  																						AS POSTPAID_MAIN_ACCT_DECRMNT_AMT,
							CAST(NULL AS DECIMAL(27,8))  																				AS MAIN_ACCT_BAL_AMT_BEFR_IMPACT,
							CAST(NULL AS DECIMAL(27,8))  																				AS MAIN_ACCT_BAL_AMT_AFTR_IMPACT,
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
							CAST(NULL AS DECIMAL(27,8)) 																				AS BONUS_DISC_REWARD_AMT,
							CAST(NULL AS BIGINT) 																						AS BONUS_UNIT_CREDIT_REWARD_AMT,
							CAST(A.CREDIT_CHARGED AS DECIMAL(27,8)) 																	AS MAIN_ACCT_RVN_AMT,
							CAST(
							(CASE
								WHEN W1.WALLET_TYP_CD = 'ECB_BALANCE' THEN A.TK1_CONSUMED
								ELSE 0
							END) +
							(CASE
								WHEN W2.WALLET_TYP_CD = 'ECB_BALANCE' THEN A.TK2_CONSUMED
								ELSE 0
							END) +
							(CASE
								WHEN W3.WALLET_TYP_CD = 'ECB_BALANCE' THEN A.TK3_CONSUMED
								ELSE 0
							END) +
							(CASE
								WHEN W4.WALLET_TYP_CD = 'ECB_BALANCE' THEN A.TK4_CONSUMED
								ELSE 0
							END) +
							(CASE
								WHEN W5.WALLET_TYP_CD = 'ECB_BALANCE' THEN A.TK5_CONSUMED
								ELSE 0
							END) +
							(CASE
								WHEN W6.WALLET_TYP_CD = 'ECB_BALANCE' THEN A.TK6_CONSUMED
								ELSE 0
							END) +
							(CASE
								WHEN W7.WALLET_TYP_CD = 'ECB_BALANCE' THEN A.TK7_CONSUMED
								ELSE 0
							END) +
							(CASE
								WHEN W8.WALLET_TYP_CD = 'ECB_BALANCE' THEN A.TK8_CONSUMED
								ELSE 0
							END) +
							(CASE
								WHEN W9.WALLET_TYP_CD = 'ECB_BALANCE' THEN A.TK9_CONSUMED
								ELSE 0
							END) +
							(CASE
								WHEN W10.WALLET_TYP_CD = 'ECB_BALANCE' THEN A.TK10_CONSUMED
								ELSE 0
							END) AS DECIMAL(27,8)) 																						AS ECB_RVN_AMT,
							CAST(NULL AS BIGINT)  																						AS ECB_USG,
							CAST(NULL AS DECIMAL(27,8))  																				AS ECB_TAX_AMT,
							CAST(NULL AS STRING)  																						AS BLLG_STAT_RSN_CD,
							CAST(W1.WALLET_KEY AS BIGINT)  																				AS WALLET1_KEY,
							CAST(A.TK1_CONSUMED AS DECIMAL(27,8))  																		AS WALLET1_BAL_DECRMNT_AMT,
							CAST(W2.WALLET_KEY AS BIGINT)  																				AS WALLET2_KEY,
							CAST(A.TK2_CONSUMED AS DECIMAL(27,8))  																		AS WALLET2_BAL_DECRMNT_AMT,
							CAST(W3.WALLET_KEY AS BIGINT)  																				AS WALLET3_KEY,
							CAST(A.TK3_CONSUMED AS DECIMAL(27,8))  																		AS WALLET3_BAL_DECRMNT_AMT,
							CAST(W4.WALLET_KEY AS BIGINT)  																				AS WALLET4_KEY,
							CAST(A.TK4_CONSUMED AS DECIMAL(27,8))  																		AS WALLET4_BAL_DECRMNT_AMT,
							CAST(W5.WALLET_KEY AS BIGINT)  																				AS WALLET5_KEY,
							CAST(A.TK5_CONSUMED AS DECIMAL(27,8))  																		AS WALLET5_BAL_DECRMNT_AMT,
							CAST(W6.WALLET_KEY AS BIGINT)  																				AS WALLET6_KEY,
							CAST(A.TK6_CONSUMED AS DECIMAL(27,8))  																		AS WALLET6_BAL_DECRMNT_AMT,
							CAST(W7.WALLET_KEY AS BIGINT)  																				AS WALLET7_KEY,
							CAST(A.TK7_CONSUMED AS DECIMAL(27,8))  																		AS WALLET7_BAL_DECRMNT_AMT,
							CAST(W8.WALLET_KEY AS BIGINT)  																				AS WALLET8_KEY,
							CAST(A.TK8_CONSUMED AS DECIMAL(27,8))  																		AS WALLET8_BAL_DECRMNT_AMT,
							CAST(W9.WALLET_KEY AS BIGINT)  																				AS WALLET9_KEY,
							CAST(A.TK9_CONSUMED AS DECIMAL(27,8))  																		AS WALLET9_BAL_DECRMNT_AMT,
							CAST(W10.WALLET_KEY AS BIGINT)  																			AS WALLET10_KEY,
							CAST(A.TK10_CONSUMED AS DECIMAL(27,8))  																	AS WALLET10_BAL_DECRMNT_AMT,
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
							CAST(6 AS BIGINT)  																							AS SRC_SYS_KEY,
							'ICC'  																										AS SRC_SYS_CD,
							CURRENT_TIMESTAMP()  																						AS LOAD_DT,
							'1'  																										AS CURR_IND,
							CAST (1 AS INTEGER)  																						AS WRHS_ID
						FROM
						(SELECT INC.* , MT.TREE_ID,
								ROW_NUMBER () OVER (PARTITION BY INC.CALL_STA_TIME,INC.CALLING_ISDN,INC.TRANSACTION_DESCRIPTION, INC.ftp_filename, INC.stt,INC.ITEM_ID,INC.SUB_ITEM_ID, mt.ITEM_ID_DTTT, mt.SUB_ITEM_ID ORDER BY mt.INCLUDE_SUB_ITEM_ID DESC, mt.EFFECT_FROM DESC) ROW_NUM_SUB,
								ROW_NUMBER () OVER (PARTITION BY INC.CALL_STA_TIME,INC.CALLING_ISDN,INC.TRANSACTION_DESCRIPTION, INC.ftp_filename, INC.stt,INC.ITEM_ID,INC.SUB_ITEM_ID,mt.ITEM_ID_DTTT ORDER BY mt.EFFECT_FROM DESC) ROW_NUM
							FROM MBF_STAGE.STG_CDR_IN_ENRICH  INC
							LEFT OUTER JOIN MBF_DATALAKE.MAP_TREE MT
								ON FROM_UNIXTIME(UNIX_TIMESTAMP(CONCAT(SUBSTRING(mt.EFFECT_FROM,0,4),SUBSTRING(mt.EFFECT_FROM,6,2),'01'), 'yyyyMMdd')) <= FROM_UNIXTIME(UNIX_TIMESTAMP('$day_key', 'yyyyMMdd'))
								AND FROM_UNIXTIME(UNIX_TIMESTAMP('$day_key', 'yyyyMMdd')) < CASE WHEN (mt.EFFECT_UNTIL IS NULL OR mt.EFFECT_UNTIL = '') THEN FROM_UNIXTIME(UNIX_TIMESTAMP('$day_key', 'yyyyMMdd')+1) 
								ELSE DATE_ADD(LAST_DAY(FROM_UNIXTIME(UNIX_TIMESTAMP(CONCAT(SUBSTRING(mt.EFFECT_UNTIL,0,4),SUBSTRING(mt.EFFECT_UNTIL,6,2),'01') , 'yyyyMMdd'))),1)  END
								AND NVL (mt.ITEM_ID_DTTT, '-1') = NVL (INC.ITEM_ID, '-1')
								AND ((mt.INCLUDE_SUB_ITEM_ID = '1' AND NVL (UPPER(mt.SUB_ITEM_ID), '-1') = NVL (UPPER (INC.SUB_ITEM_ID), '-1')) OR mt.INCLUDE_SUB_ITEM_ID = '0')
						WHERE  INC.CALL_TYPE IN (189) AND INC.CALL_TYPE_IND = 'GPRS' AND NOT (NVL(INC.LOCATION_INDICATOR,-1) = 2 OR NVL(INC.CELL_ID,'') LIKE '452-02%' OR NVL(INC.LOCATION,'') like '8491%') AND  INC.DAY_KEY='$day_key') A
						LEFT JOIN MBF_BIGDATA.ADMR_PRODUCT P ON P.PROD_KEY = NVL(A.TREE_ID,'157') AND P.DAY_KEY = '$day_key'	
							
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
						LEFT JOIN CELL_4G_STD C4G ON A.CELL_ID = C4G.CELL_KEY_4G			
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
						WHERE A.ROW_NUM_SUB = 1 AND A.ROW_NUM = 1
		",
		"tempTable"  : "ADME_RATED_DATA_CDR_tmp",
		"countSourceRecord":"1"
	  }   	  
	]
}