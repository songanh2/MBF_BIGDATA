{
	 "description":"ADME_RAW_DATA_CDR",
	 "sourceTables":"MBF_DATALAKE.FLEXI,MBF_DATALAKE.FLEXI_LATE",
	 "description":"Cell tren GGSN quy hoach theo format mcc-mnc-lac-cell",
	 "sqlStatements": [
	  {
		"sql"      : "
					SELECT * FROM
						(
							SELECT 
								ROW_NUMBER() OVER (PARTITION BY C4.CELL_KEY_N ORDER BY C4.CELL_GCI_CD DESC) AS ROWNUM, C4.* 
							FROM 
							(
								SELECT			
									CASE WHEN CELL_TYP_CD = '4G' THEN CONCAT(SPLIT(CELL_KEY,'[\\-]')[0],'-',SPLIT(CELL_KEY,'[\\-]')[1],'-',SPLIT(CELL_KEY,'[\\-]')[3],'-',SPLIT(CELL_KEY,'[\\-]')[4])
									ELSE CELL_KEY END AS CELL_KEY_N,
									*
								FROM MBF_BIGDATA.ADMR_CELL WHERE DAY_KEY = '$day_key' AND CELL_DSCR NOT LIKE '%TEST%'
							) C4
						) X WHERE X.ROWNUM = 1
					",
		"tempTable" : "CELL_TMP",
		"countSourceRecord" : "0"
	  },	 
	  {
		"sql"      : "
						SELECT 													
							CAST (FROM_UNIXTIME(UNIX_TIMESTAMP(A.RECORD_OPENING_TIME,'E MMM dd HH:mm:ss yyyy'),'yyyyMM') AS STRING) 				 AS MO_KEY,
							CAST (FROM_UNIXTIME(UNIX_TIMESTAMP(A.RECORD_OPENING_TIME,'E MMM dd HH:mm:ss yyyy'),'HH') AS STRING) 				 	 AS HOUR_KEY,
							CAST(NULL AS STRING) 																									 AS UUID,
							A.CHARGING_ID 																									 		 AS EVT_CD,
							CAST(A.LOCAL_SEQUENCE_NUMBER_V1 AS BIGINT) 																				 AS EVT_SEQ_NBR,
							CAST(NULL AS BIGINT) 																									 AS PRTIAL_REC_NBR,
							CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(A.RECORD_OPENING_TIME,'E MMM dd HH:mm:ss yyyy'),'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP) 	 AS EVT_START_DT,
							CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(A.TIME_OF_REPORT,'E MMM dd HH:mm:ss yyyy'),'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP) 		 AS EVT_END_DT,
							CAST(NULL AS TIMESTAMP) 																								 AS EVT_BLLG_START_DT,
							A.FTP_FILENAME 																											 AS FILE_NAME,
							CAST(NULL AS TIMESTAMP) 																								 AS FILE_DT,
							CAST(NULL AS STRING) 																									 AS FILE_PREFIX_CD,
							CAST(NULL AS TIMESTAMP) 																								 AS REC_PROCESS_DT,
							CAST(NULL AS STRING) 																									 AS SWITCH_CD,
							B.ACCT_SRVC_INSTANCE_KEY																								 AS ACCT_SRVC_INSTANCE_KEY,
							A.ISDN_STD 																												 AS SERVICE_NBR,
							B.ACCT_KEY																												 AS ACCT_KEY,
							B.BILLABLE_ACCT_KEY																										 AS BILLABLE_ACCT_KEY,
							B.CUST_KEY																												 AS CUST_KEY,
							B.CUST_TYP_CD																											 AS CUST_TYP_CD,
							B.NTWK_QOS_GRP_CD																										 AS NTWK_QOS_GRP_CD,
							B.ACTIVATION_DT 																										 AS ACCT_ACTIVATION_DT,
							B.CBS_ACTIVATION_DT 																									 AS ACCT_CBS_ACTIVATION_DT,
							B.LFCYCL_STAT_CD 																										 AS ACCT_LFCYCL_STAT_CD,
							B.ACTIVATION_DT 																										 AS SRVC_ACTIVATION_DT,
							B.CBS_ACTIVATION_DT 																									 AS SRVC_CBS_ACTIVATION_DT,
							B.LFCYCL_STAT_CD 																										 AS SRVC_LFCYCL_STAT_CD,
							B.PROD_LINE_KEY																											 AS PROD_LINE_KEY,
							B.USAGE_PLAN_KEY																										 AS USAGE_PLAN_KEY,
							B.USAGE_PLAN_CD																											 AS USAGE_PLAN_CD,
							B.USAGE_PLAN_TYP_CD																										 AS USAGE_PLAN_TYP_CD,
							B.CRM_OFFER_KEY 																										 AS OFFER_KEY,
							B.CRM_OFFER_CD 																											 AS OFFER_CD,
							CAST(NULL AS DECIMAL(20,0))																								 AS PROD_KEY,
							CAST(NULL AS STRING) 																									 AS PROD_CD,
							'DATA' 																													 AS EVT_CLASS_CD,
							CASE WHEN A.ROAMING_IDX = '2' THEN 'DATA_RMNG_ISD'
								 WHEN A.ROAMING_IDX = '1' THEN 'DATA_RMNG_LOCAL'
								 WHEN A.ROAMING_IDX = '0' THEN 'DATA_LOCAL'
							ELSE 'UNKNOWN' END 																										 AS EVT_CTGRY_CD,
							CASE WHEN A.ROAMING_IDX = '2' THEN 'DATA_RMNG_ISD'
								 WHEN A.ROAMING_IDX = '1' THEN 'DATA_RMNG_LOCAL'
								 WHEN A.ROAMING_IDX = '0' THEN 'DATA_LOCAL'
							ELSE 'UNKNOWN' END 																										 AS EVT_TYP_CD,
							'DATA' 																													 AS USAGE_TYP_CD,
							CASE WHEN A.ROAMING_IDX = '2' THEN 'DATA_RMNG_ISD'
								 WHEN A.ROAMING_IDX = '1' THEN 'DATA_RMNG_LOCAL'
								 WHEN A.ROAMING_IDX = '0' THEN 'DATA_LOCAL'
							ELSE 'UNKNOWN' END 																										 AS CALL_CTGRY_CD,
							CASE WHEN A.ROAMING_IDX = '2' THEN 'DATA_RMNG_ISD'
								 WHEN A.ROAMING_IDX = '1' THEN 'DATA_RMNG_LOCAL'
								 WHEN A.ROAMING_IDX = '0' THEN 'DATA_LOCAL'
							ELSE 'UNKNOWN' END 																										 AS CALL_TYP_CD,
							CAST(NULL AS STRING) 																									 AS CALL_STAT_CD,
							CAST(NULL AS STRING) 																									 AS CALL_TMNT_RSN_CD,
							CAST(NULL AS STRING) 																									 AS NTWK_SESSION_ID,
							CAST(NULL AS STRING) 																									 AS BLLG_SESSION_ID,
							CAST(NULL AS STRING) 																									 AS QOS_RQSTD,
							A.QOS_INFORMATION 																										 AS QOS_NEGOTIATED,
							A.ISDN_STD 																												 AS ORIGNTNG_NBR,
							A.SERVED_IMSI 																											 AS ORIGNTNG_IMSI,
							A.SERVED_IMEISV 																										 AS ORIGNTNG_IMEI,
							CASE WHEN A.ROAMING_IDX = '2' THEN 'I' ELSE 'D' END 																	 AS INTL_DMSTC_IND,
							CASE WHEN (A.ROAMING_IDX = '1' OR A.ROAMING_IDX = '2') THEN '1' ELSE CAST(NULL AS STRING) END 							 AS RMNG_IND,
							CAST(NULL AS STRING) 																									 AS FNF_IND,
							CAST(NULL AS STRING) 																									 AS TESTNG_NBR_IND,
							CAST('1' AS STRING) 																									 AS ONNET_OFFNET_IND,
							A.ACCESS_POINT_NAME_NETWORK_IDENTIFIER 																					 AS NTWK_ACCS_POINT_IP,
							A.GGSN_ADDRESS 																											 AS NTWK_SESSION_CONTROL_POINT_IP,
							A.SERVED_PDP_ADDRESS 																									 AS HOST_IP,
							CAST(NULL AS STRING) 																									 AS DOMAIN_NAME,
							CAST(NULL AS STRING) 																									 AS ACCESSED_URL,
							CAST(NULL AS STRING) 																									 AS DVC_OS_NAME,
							CAST(NULL AS STRING) 																									 AS BROWSING_ACTVTY_TYP,
							A.SGSN_PLMN_IDENTIFIER_V1 																								 AS ORIGNTNG_PLMN_CD,
							CAST(NULL AS STRING) 																									 AS ORIGNTNG_NETWORK_TYP,
							SUBSTRING(A.SERVED_IMSI,1,3) 																							 AS ORIGNTNG_HOME_COUNTRY_CD,
							CAST(NULL AS STRING) 																									 AS ORIGNTNG_HOME_AREA_CD,
							SUBSTRING(A.SERVED_IMSI,4,2) 																							 AS ORIGNTNG_HOME_NETWORK_CD,
							CASE WHEN (A.ROAMING_IDX = '1' OR A.ROAMING_IDX = '2') THEN SUBSTRING(A.SERVED_IMSI,1,3) ELSE NULL END 					 AS ORIGNTNG_ROAM_COUNTRY_CD,
							CAST(NULL AS STRING) 																									 AS ORIGNTNG_ROAM_AREA_CD,
							CASE WHEN (A.ROAMING_IDX = '1' OR A.ROAMING_IDX = '2') THEN SUBSTRING(A.SERVED_IMSI,4,2) ELSE NULL END 					 AS ORIGNTNG_ROAM_NETWORK_CD,
							A.CELL_CODE 																											 AS FIRST_ORIGNTNG_GCI_CD,
							CAST(NULL AS STRING) 																									 AS LAST_ORIGNTNG_GCI_CD,
							CAST(NULL AS BIGINT) 																									 AS ORIGNTNG_OPRTR_KEY,
							CAST(NULL AS STRING) 																									 AS ORIGNTNG_OPRTR_CD,
							CAST(NULL AS BIGINT) 																									 AS BLLG_OPRTR_KEY,
							CAST(NULL AS STRING) 																									 AS BLLG_OPRTR_CD,
							CAST(D.ADDR_LAT AS DECIMAL(27,8)) 																						 AS ADDR_LAT,
							CAST(D.ADDR_LON AS DECIMAL(27,8)) 																						 AS ADDR_LON,
							CASE
								 WHEN A.rat_type_v1 IN ('EUTRAN') THEN '4G'
								 WHEN A.rat_type_v1 IN ('UTRAN', '3G RAN') THEN '3G'
								 ELSE '2G' 
							END																														 AS CELL_TYP_CD,
							D.CELL_KEY 																												 AS FIRST_CELL_KEY,
							D.CELL_CD 																												 AS FIRST_CELL_CD,
							D.CELL_SITE_KEY 																										 AS FIRST_CELL_SITE_KEY,
							D.CELL_SITE_CD 																											 AS FIRST_CELL_SITE_CD,
							CAST(NULL AS STRING) 																									 AS LAST_CELL_KEY,
							CAST(NULL AS STRING) 																									 AS LAST_CELL_CD,
							CAST(NULL AS BIGINT) 																									 AS LAST_CELL_SITE_KEY,
							CAST(NULL AS STRING) 																									 AS LAST_CELL_SITE_CD,
							D.NTWK_MGNT_CENTRE_KEY																									 AS NTWK_MGNT_CENTRE_KEY,
							D.NTWK_MGNT_CENTRE_CD																								     AS NTWK_MGNT_CENTRE_CD,
							D.BSNS_RGN_KEY																											 AS BSNS_RGN_KEY,
							D.BSNS_RGN_CD																											 AS BSNS_RGN_CD,
							D.BSNS_CLUSTER_KEY																										 AS BSNS_CLUSTER_KEY,
							D.BSNS_CLUSTER_CD																										 AS BSNS_CLUSTER_CD,
							D.BSNS_MINICLUSTER_KEY																									 AS BSNS_MINICLUSTER_KEY,
							D.BSNS_MINICLUSTER_CD																									 AS BSNS_MINICLUSTER_CD,
							D.GEO_CNTRY_KEY																											 AS GEO_CNTRY_KEY,
							D.GEO_CNTRY_CD																											 AS GEO_CNTRY_CD,
							D.GEO_STATE_KEY																											 AS GEO_STATE_KEY,
							D.GEO_STATE_CD																											 AS GEO_STATE_CD,
							D.GEO_DSTRCT_KEY																										 AS GEO_DSTRCT_KEY,
							D.GEO_DSTRCT_CD																										     AS GEO_DSTRCT_CD,
							D.GEO_CITY_KEY																											 AS GEO_CITY_KEY,
							D.GEO_CITY_CD 																											 AS GEO_CITY_CD,
							B.ACQSTN_DT        																										 AS ACQSTN_DT,
							B.ACQSTN_BSNS_OUTLET_KEY                                                                                                 AS ACQSTN_BSNS_OUTLET_KEY,
							B.ACQSTN_BSNS_OUTLET_CD																									 AS ACQSTN_BSNS_OUTLET_CD,
							CAST(NULL AS STRING) 																									 AS BILL_GRP_CD,
							CAST(NULL AS BIGINT) 																									 AS BILL_CYCLE_KEY,
							CAST(NULL AS INT) 																										 AS CHRG_HEAD_KEY,
							CAST(NULL AS STRING) 																									 AS CHRG_HEAD_CD,
							'VND' 																													 AS CRNCY_CD,
							'BYTES' 																												 AS USG_UOM_CD,
							'VND' 																													 AS INTCONN_CRNCY_CD,
							CAST(NULL AS DECIMAL(27,8)) 																							 AS EXCHG_RATE_VAL,
							CAST(A.DURATION AS BIGINT) 																								 AS DRTN,
							CAST(NULL AS BIGINT) 																									 AS RTD_DRTN,
							CAST(NULL AS BIGINT) 																									 AS FREE_DRTN,
							CAST(NULL AS BIGINT) 																									 AS CHRGD_DRTN,
							CAST(NULL AS BIGINT) 																									 AS INTCONN_DRTN,
							CAST(A.DATA_VOLUME_FLOW_BASED_CHARGING_UPLINK + A.DATA_VOLUME_FBC_DOWNLINK AS BIGINT) 									 AS TOT_FLUX,
							CAST(A.DATA_VOLUME_FLOW_BASED_CHARGING_UPLINK AS BIGINT) 																 AS UP_FLUX,
							CAST(A.DATA_VOLUME_FBC_DOWNLINK AS BIGINT) 																				 AS DOWN_FLUX,
							'BYTES' 																												 AS FLUX_UOM_CD,
							CAST(NULL AS BIGINT) 																									 AS RTD_USG,
							CAST(NULL AS BIGINT) 																									 AS FREE_USG,
							CAST(NULL AS BIGINT) 																									 AS CHRGBL_USG,
							CAST(NULL AS BIGINT) 																									 AS INTCONN_USG,
							CAST(NULL AS STRING) 																									 AS CHRGNG_PRTY_IND,
							CAST(NULL AS STRING) 																									 AS OTHR_CHRGNG_NBR,
							CAST(NULL AS STRING) 																									 AS CHRGNG_IND,
							CAST(NULL AS STRING) 																									 AS CHRGING_ZONE_CD,
							CAST(NULL AS STRING) 																									 AS RTD_CLASS_CD,
							CAST(NULL AS STRING) 																									 AS RTNG_INFO,
							CAST(NULL AS DECIMAL(30,2)) 																							 AS CHRGD_AMT,
							CAST(NULL AS DECIMAL(30,2)) 																							 AS MARK_UP_AMT,
							CAST(NULL AS DECIMAL(30,2)) 																							 AS DISCOUNT_AMT,
							CAST(NULL AS DECIMAL(30,2)) 																							 AS TAX_AMT,
							CAST(NULL AS DECIMAL(30,2)) 																							 AS BLLD_AMT,
							CAST(NULL AS DECIMAL(30,2)) 																							 AS RVN_AMT,
							CAST(NULL AS DECIMAL(30,2)) 																							 AS INTERNAL_COST_AMT,
							CAST(NULL AS DECIMAL(30,2)) 																							 AS INTCONN_COST_AMT,
							CAST(NULL AS DECIMAL(30,2)) 																							 AS INTCONN_RVN_AMT,
							CAST(NULL AS STRING) 																									 AS BLLG_STAT_RSN_CD,
							B.LOYALTY_RANK_SCORE																									 AS LOYALTY_RANK_SCORE,
							B.LOYALTY_SCORE_DT																										 AS LOYALTY_SCORE_DT,
							B.CREDIT_SCORE																											 AS CREDIT_SCORE,
							B.CREDIT_CLASS_CD 																										 AS CREDIT_CLASS_CD,
							B.CREDIT_SCORE_METHOD                                            														 AS CREDIT_SCORE_METHOD,
							B.CREDIT_SCORE_DT																										 AS CREDIT_SCORE_DT,
							B.RISK_IND																												 AS RISK_IND,
							CAST(NULL AS STRING) 																									 AS ACCT_SEGMENT_CD,
							CAST(NULL AS STRING) 																									 AS ACCT_SEGMENT_DT,
							CAST(NULL AS STRING) 																									 AS CMPGN_CD,
							CAST(4 AS BIGINT) 																										 AS SRC_SYS_KEY,
							'GGSN' 																													 AS SRC_SYS_CD,
							CURRENT_TIMESTAMP() 																									 AS LOAD_DT,
							'1' 																													 AS CURR_IND,
							1 																														 AS WRHS_ID                  
						FROM 
						(
							SELECT * FROM MBF_DATALAKE.FLEXI WHERE DAY_KEY = '$day_key'
							UNION ALL
							SELECT * FROM MBF_DATALAKE.FLEXI_LATE WHERE DAY_KEY = '$day_key'
						) A						 
						LEFT OUTER JOIN MBF_BIGDATA.ADMR_ACCOUNT_SERVICE B ON A.ISDN_STD = B.SERVICE_NBR AND B.DAY_KEY = '$day_key'
						LEFT OUTER JOIN CELL_TMP D ON A.CELL_CODE = D.CELL_KEY_N						
					",
		"tempTable" : "ADME_RAW_DATA_CDR_TMP",
		"countSourceRecord" : "0"
	  }
	]
}