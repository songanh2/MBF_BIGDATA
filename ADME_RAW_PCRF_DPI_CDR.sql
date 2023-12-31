{
	 "description":"ADME_RAW_PCRF_DPI_CDR",	 
	 "sqlStatements": [
	  {
		"sql"      	:"
						SELECT
							A.*,
							(CASE WHEN MSISDN IS NULL THEN CAST(NULL AS STRING)
								WHEN SUBSTRING(REPLACE(MSISDN,'+',''),1,1) = '0' AND SUBSTRING(SUBSTRING(REPLACE(MSISDN,'+',''),2),1,1) ='8' AND LENGTH(SUBSTRING(REPLACE(MSISDN,'+',''),2)) = 9 
								THEN SUBSTRING(REPLACE(MSISDN,'+',''),2)
								WHEN SUBSTRING(REPLACE(MSISDN,'+',''),1,1) = '8' AND LENGTH(SUBSTRING(REPLACE(MSISDN,'+',''),1)) = 9 
								THEN SUBSTRING(REPLACE(MSISDN,'+',''),1)
								WHEN SUBSTRING(REPLACE(MSISDN,'+',''),1,2) = '84' 
								THEN SUBSTRING(REPLACE(MSISDN,'+',''),3)
								WHEN SUBSTRING(REPLACE(MSISDN,'+',''),1,3) = '084'
								THEN SUBSTRING(REPLACE(MSISDN,'+',''),4)
								WHEN SUBSTRING(REPLACE(MSISDN,'+',''),1,1) = '0' 
								THEN SUBSTRING(REPLACE(MSISDN,'+',''),2)
								END) AS SERVICE_NBR							
						FROM
							(
								SELECT `TIME` AS TIME_,* FROM MBF_DATALAKE.EDR_HN WHERE DAY_KEY = '$day_key'
								UNION ALL
								SELECT `TIME` AS TIME_,* FROM MBF_DATALAKE.EDR_HCM WHERE DAY_KEY = '$day_key'
							) A
		",
		"tempTable"  : "ADME_RAW_PCRF_DPI_CDR_TMP",
		"countSourceRecord":"1"
	  },	 
	  {
		"sql"      	:"
						SELECT
							CAST(SUBSTRING('$day_key', 0, 6) AS STRING)							AS MO_KEY,
							SUBSTRING(A.TIME_, 12, 2) 								   			AS HOUR_KEY,
							CAST(NULL AS STRING) 									   			AS UUID,
							CAST(NULL AS STRING) 												AS SESSION_REF_CD,
							CAST(A.SERVICE_START_DATE_TIME_ AS TIMESTAMP) 						AS SESSION_START_DT,
							CAST(A.SERVICE_END_DATE_TIME AS TIMESTAMP) 							AS SESSION_END_DT,
							CAST(NULL AS TIMESTAMP) 											AS BLLG_START_DT,
							A.FTP_FILENAME 														AS FILE_NAME,
							CAST(NULL AS TIMESTAMP) 											AS FILE_DT,
							CAST(NULL AS STRING) 												AS FILE_PREFIX_CD,
							CAST(NULL AS TIMESTAMP) 											AS REC_PROCESS_DT,
							CAST(NULL AS STRING) 												AS SWITCH_CD,
							B.ACCT_SRVC_INSTANCE_KEY											AS ACCT_SRVC_INSTANCE_KEY,
							B.SERVICE_NBR														AS SERVICE_NBR,							
							B.ACCT_KEY															AS ACCT_KEY,
							B.BILLABLE_ACCT_KEY													AS BILLABLE_ACCT_KEY,
							CAST(NULL AS STRING) 												AS BILL_GRP_CD,
							CAST(NULL AS BIGINT) 												AS BILL_CYCLE_KEY,
							B.CUST_KEY 															AS CUST_KEY,
							B.CUST_TYP_CD														AS CUST_TYP_CD,
							B.NTWK_QOS_GRP_CD													AS NTWK_QOS_GRP_CD,
							CAST(B.ACTIVATION_DT AS TIMESTAMP) 									AS ACCT_ACTIVATION_DT,
							CAST(B.CBS_ACTIVATION_DT AS TIMESTAMP) 								AS ACCT_CBS_ACTIVATION_DT,
							B.LFCYCL_STAT_CD													AS ACCT_LFCYCL_STAT_CD,
							CAST(B.ACTIVATION_DT AS TIMESTAMP) 									AS SRVC_ACTIVATION_DT,
							CAST(B.CBS_ACTIVATION_DT AS TIMESTAMP) 								AS SRVC_CBS_ACTIVATION_DT,
							B.LFCYCL_STAT_CD													AS SRVC_LFCYCL_STAT_CD,
							CAST(B.PROD_LINE_KEY AS INTEGER) 									AS PROD_LINE_KEY,
							CAST(B.USAGE_PLAN_KEY AS BIGINT)								 	AS USAGE_PLAN_KEY,
							B.USAGE_PLAN_CD														AS USAGE_PLAN_CD,
							B.USAGE_PLAN_TYP_CD 												AS USAGE_PLAN_TYP_CD,
							CAST(B.CRM_OFFER_KEY AS BIGINT) 									AS OFFER_KEY,
							B.CRM_OFFER_CD  													AS OFFER_CD,
							C.OFFER_TYP_CD 														AS OFFER_TYP_CD,
							CAST(NULL AS STRING) 												AS PROD_KEY,
							CAST(NULL AS STRING) 												AS PROD_CD,
							'DATA' 																AS EVT_CLASS_CD,
							CASE
								WHEN A.IMSI NOT LIKE '452%' THEN 'DATA_RMNG_ISD'
								WHEN A.IMSI NOT LIKE '45201%' THEN 'DATA_RMNG_LOCAL'
								WHEN A.IMSI LIKE '452%' THEN 'DATA_LOCAL'
								ELSE 'UNKNOWN'
							END 																AS EVT_CTGRY_CD,
							CASE
								WHEN A.IMSI NOT LIKE '452%' THEN 'DATA_RMNG_ISD'
								WHEN A.IMSI NOT LIKE '45201%' THEN 'DATA_RMNG_LOCAL'
								WHEN A.IMSI LIKE '452%' THEN 'DATA_LOCAL'
								ELSE 'UNKNOWN'
							END 																AS EVT_TYP_CD,
							A.SESSION_ID 														AS NTWK_SESSION_ID,
							CAST(NULL AS STRING) 												AS BLLG_SESSION_ID,
							A.HOME_SERVICE_ZONE 												AS PCRF_HOME_SERVICE_ZONE_CD,
							CAST(NULL AS STRING) 												AS PCRF_SUBSCRIBER_PROFL_CD,
							A.SERVICE_PACKAGE_NAME 												AS PCRF_PROFL_CTGRY_CD,
							A.SERVICE_NAME 														AS PCRF_SRVC_PROFL_CD,
							CAST(NULL AS STRING) 												AS QOS_RQSTD,
							CAST(NULL AS STRING) 												AS QOS_NEGOTIATED,
							A.MSISDN 															AS ORIGNTNG_NBR,
							A.IMSI																AS ORIGNTNG_IMSI,
							A.IMEI 																AS ORIGNTNG_IMEI,
							CAST(NULL AS STRING) 												AS TESTNG_NBR_IND,
							CAST(NULL AS STRING) 												AS INTL_DMSTC_IND,
							CASE
								WHEN A.IMSI NOT LIKE '452%'
								OR A.IMSI NOT LIKE '45201%' THEN '1'
								ELSE NULL
							END 																AS RMNG_IND,
							CAST(NULL AS INTEGER) 												AS NTWK_TECH_TYP_KEY,
							CAST(NULL AS STRING) 												AS ONNET_OFFNET_IND,
							A.MCCMNC  															AS ORIGNTNG_PLMN_CD,
							CAST(NULL AS STRING) 												AS ORIGNTNG_NETWORK_TYP,
							SUBSTRING(A.IMSI, 1, 3) 											AS ORIGNTNG_HOME_COUNTRY_CD,
							CAST(NULL AS STRING) 												AS ORIGNTNG_HOME_AREA_CD,
							SUBSTRING(A.IMSI, 4, 2) 											AS ORIGNTNG_HOME_NETWORK_CD,
							CASE
								WHEN A.IMSI NOT LIKE '452%'
								OR A.IMSI NOT LIKE '45201%' THEN SUBSTRING(A.IMSI, 1, 3)
								ELSE NULL
							END 																AS ORIGNTNG_ROAM_COUNTRY_CD,
							CAST(NULL AS STRING) 												AS ORIGNTNG_ROAM_AREA_CD,
							(CASE
								WHEN A.IMSI NOT LIKE '452%'
								OR A.IMSI NOT LIKE '45201%' THEN SUBSTRING(A.IMSI, 4, 2)
								ELSE NULL
							END) 																AS ORIGNTNG_ROAM_NETWORK_CD,
							NVL(A.CGI,A.ECGI) 													AS FIRST_ORIGNTNG_GCI_CD,
							CAST(NULL AS STRING) 												AS LAST_ORIGNTNG_GCI_CD,
							CAST(NULL AS BIGINT) 												AS ORIGNTNG_NTWK_OPRTR_KEY,
							CAST(NULL AS STRING) 												AS ORIGNTNG_NTWK_OPRTR_CD,
							CAST(NULL AS BIGINT) 												AS BLLG_OPRTR_KEY,
							CAST(NULL AS STRING) 												AS BLLG_OPRTR_CD,
							CAST(NULL AS STRING) 												AS BLLG_STAT_CD,
							CAST(NULL AS STRING) 												AS BLLG_STAT_RSN_CD,
							A.APN																AS NTWK_ACCS_POINT_IP,
							A.SGSNADDRESS 														AS NTWK_SESSION_CONTROL_POINT_IP,
							CAST(NULL AS BIGINT) 												AS NTWK_ACCS_POINT_SWITCH_KEY,
							CAST(NULL AS BIGINT) 												AS NTWK_SESSION_CNTRL_SWITCH_KEY,
							CAST(NULL AS STRING) 												AS HOST_IP,
							CAST(NULL AS STRING) 												AS ACCESSED_URL,
							CAST(NULL AS STRING) 												AS DSTN_SERVER_IP,
							CAST(NULL AS BIGINT) 												AS DSTN_SERVER_PORT,
							CAST(NULL AS STRING) 												AS PROTOCOL_TYP_CD,
							CAST(NULL AS STRING) 												AS APPL_CTGRY_CD,
							CAST(NULL AS STRING) 												AS APPL_SUB_CTGRY_CD,
							CAST(NULL AS STRING) 												AS RTD_CLASS_GRP_CD,
							CAST(NULL AS STRING) 												AS DRTN_UOM_CD,							
							CAST((UNIX_TIMESTAMP(A.SERVICE_START_DATE_TIME_) - UNIX_TIMESTAMP(A.SERVICE_END_DATE_TIME)) AS BIGINT) AS DRTN,
							CAST(NULL AS BIGINT) 												AS INTCONN_DRTN,
							CAST(NULL AS STRING) 												AS FLUX_UOM_CD,
							CAST(NULL AS BIGINT) 												AS TOT_FLUX,
							CAST(NULL AS BIGINT) 												AS UP_FLUX,
							CAST(NULL AS BIGINT) 												AS DOWN_FLUX,
							CAST(NULL AS BIGINT) 												AS INTCONN_USG,
							CAST(NULL AS STRING) 												AS SESSION_TMNT_STAT_CD,
							CAST(NULL AS STRING) 												AS SESSION_TMNT_RSN_CD,
							CAST(D.ADDR_LAT AS DECIMAL(27,8 )) 									AS ADDR_LAT,
							CAST(D.ADDR_LON AS DECIMAL(27,8)) 									AS ADDR_LON,
							CAST(D.CELL_KEY AS STRING) 											AS FIRST_CELL_KEY,
							D.CELL_CD 															AS FIRST_CELL_CD,
							CAST(D.CELL_SITE_KEY AS BIGINT) 									AS FIRST_CELL_SITE_KEY,
							D.CELL_SITE_CD 														AS FIRST_CELL_SITE_CD,
							CAST(NULL AS STRING) 												AS LAST_CELL_KEY,
							CAST(NULL AS STRING)											 	AS LAST_CELL_CD,
							CAST(NULL AS BIGINT) 												AS LAST_CELL_SITE_KEY,
							CAST(NULL AS STRING) 												AS LAST_CELL_SITE_CD,
							CAST(D.NTWK_MGNT_CENTRE_KEY AS BIGINT) 								AS NTWK_MGNT_CENTRE_KEY,
							D.NTWK_MGNT_CENTRE_CD  												AS NTWK_MGNT_CENTRE_CD,
							CAST(B.BSNS_RGN_KEY AS BIGINT) 										AS BSNS_RGN_KEY,
							B.BSNS_RGN_CD 														AS BSNS_RGN_CD,
							CAST(B.BSNS_CLUSTER_KEY AS BIGINT) 									AS BSNS_CLUSTER_KEY,
							B.BSNS_CLUSTER_CD 													AS BSNS_CLUSTER_CD,
							CAST(B.BSNS_MINICLUSTER_KEY AS BIGINT) 								AS BSNS_MINICLUSTER_KEY,
							B.BSNS_MINICLUSTER_CD 												AS BSNS_MINICLUSTER_CD,
							CAST(B.GEO_CNTRY_KEY AS BIGINT) 									AS GEO_CNTRY_KEY,
							B.GEO_CNTRY_CD 														AS GEO_CNTRY_CD,
							CAST(B.GEO_STATE_KEY AS BIGINT) 									AS GEO_STATE_KEY,
							B.GEO_STATE_CD														AS GEO_STATE_CD,
							CAST(B.GEO_DSTRCT_KEY AS BIGINT) 									AS GEO_DSTRCT_KEY,
							B.GEO_DSTRCT_CD												 		AS GEO_DSTRCT_CD,
							CAST(B.GEO_CITY_KEY AS BIGINT) 										AS GEO_CITY_KEY,	
							B.GEO_CITY_CD 														AS GEO_CITY_CD,
							CAST(B.ACQSTN_DT AS TIMESTAMP) 										AS ACQSTN_DT,
							CAST(B.ACQSTN_BSNS_OUTLET_KEY AS BIGINT) 							AS ACQSTN_BSNS_OUTLET_KEY,
							B.ACQSTN_BSNS_OUTLET_CD												AS ACQSTN_BSNS_OUTLET_CD,
							CAST(B.LOYALTY_RANK_SCORE AS DECIMAL(12,4)) 						AS LOYALTY_RANK_SCORE,
							B.LOYALTY_SCORE_DT 													AS LOYALTY_SCORE_DT,
							CAST(B.CREDIT_SCORE AS DECIMAL(12,4)) 								AS CREDIT_SCORE,
							B.CREDIT_CLASS_CD													AS CREDIT_CLASS_CD,
							B.CREDIT_SCORE_METHOD 												AS CREDIT_SCORE_METHOD,
							CAST(B.CREDIT_SCORE_DT AS TIMESTAMP) 								AS CREDIT_SCORE_DT,
							CAST(B.RISK_IND AS INTEGER) 										AS RISK_IND,
							CAST(NULL AS STRING)		 										AS ACCT_SEGMENT_CD,
							CAST(NULL AS STRING) 												AS ACCT_SEGMENT_DT,
							CAST(NULL AS STRING) 												AS CMPGN_CD,
							CAST (17 AS BIGINT) 												AS SRC_SYS_KEY,
							'PCRF' 																AS SRC_SYS_CD,
							CURRENT_TIMESTAMP() 												AS LOAD_DT,
							'1' 																AS CURR_IND,
							CAST(1 AS INTEGER) 													AS WRHS_ID
						FROM ADME_RAW_PCRF_DPI_CDR_TMP A
						LEFT JOIN MBF_BIGDATA.ADMR_ACCOUNT_SERVICE B ON A.SERVICE_NBR = B.SERVICE_NBR AND B.DAY_KEY = '$day_key'
						LEFT JOIN MBF_BIGDATA.ADMR_OFFER C ON B.CRM_OFFER_KEY = C.OFFER_KEY AND C.DAY_KEY = '$day_key'
						LEFT JOIN MBF_BIGDATA.ADMR_CELL D ON D.CELL_KEY = NVL(A.CGI,A.ECGI) AND D.DAY_KEY = '$day_key'
		",
		"tempTable"  : "ADME_RAW_PCRF_DPI_CDR_TMP",
		"countSourceRecord":"1"
	  }   	  
	]
}