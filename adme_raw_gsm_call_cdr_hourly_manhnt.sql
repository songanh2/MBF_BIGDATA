{
	 "description":"ADME_RAW_GSM_CALL_CDR",
	 "createBy":"ManhNT",
	 "createDate":"01/08/2021",
	 "modifiedBy":"",
	 "modifiedDate":"",
	 "sqlStatements": [
	  {
		"sql"      : "
						SELECT
							X.STT,
							X.FC,
							X.CALL_TYPE,
							X.PO_CODE,
							X.TAX_AIRTIME,
							X.TAX_IDD,
							X.TAX_SERVICE,
							X.CALLING_ISDN,
							X.IMSI,
							X.CALL_STA_TIME,
							X.DURATION,
							X.CALL_END_TIME,
							X.CALLED_ISDN,
							X.CELL_ID,
							X.SERVICE_CENTER,
							X.IC_ROUTE,
							X.OG_ROUTE,
							X.TAR_CLASS,
							X.TS_CODE,
							X.BS_CODE,
							X.IN_MARK,
							X.CHAR_INDI,
							X.ORG_CALL_ID,
							X.REC_SEQ_NUM,
							X.TRANSLATE_NUM,
							X.CALLING_IMEI,
							X.CALLING_ORG,
							X.CALLED_ORG,
							X.SUBS_TYPE,
							X.BL_AIR,
							X.BL_IDD_SER,
							X.CALLING_CEN,
							X.CALLED_CEN,
							X.COLLECT_TYPE,
							X.MESS_TYPE,
							X.SYSTEM_TYPE,
							X.RATEINDICATION,
							X.FCIDATA,
							X.NETWORKCALLREFERENCE,
							X.LEVELOFCAMELSERVICE,
							X.SERVICEKEY,
							X.GSM_SCF_ADDRESS,
							X.CALL_IDENTIFICATION_NUMBER,
							X.SEIZURETIME,
							X.CALLEMLPPPRIORITY,
							X.CAUSEFORTERM,
							X.SUPPLSERVICESUSED,
							X.CAMELDESTINATIONNUMBER,
							X.SMSRESULT,
							X.CALLERIP,
							X.CALLEDIP,
							X.CALL_TYPE_DETAIL,
							X.RMNUMBER,
							X.ORIGINALCALLEDNUMBER,
							X.CALLING_ISDN_CHANGE,
							X.CALLED_ISDN_CHANGE,
							X.CALLPOSITION,
							X.ALERTINGTIME,
							X.SETUPTIME,
							X.DIAGNOSTICS,
							X.CNAPCODE,
							X.CNAPRESULT,
							X.CALLEDPARTYMNPINFO,
							X.TIMEOFIAMRECEPTION,
							X.TIMEOFSETUPRECEPTION,
							X.TIMEOFALERTINGRECEPTION,
							X.CNAPSUCCESSFLAG,
							X.FTP_FILENAME,
							X.CALLED_ISDN_STD,
							X.CALLING_ISDN_STD,
							X.CELL_ID_STD,
							X.CALL_TYP_CD,
							X.IN_NET_DATE,
							X.NET_OPERATOR_FROM_ID,
							X.NET_OPERATOR_TO_ID,							
							X.MNP_ID,
							X.PLMN_PLMN_ID,
							X.PLMN_CALL_TYPE_ID,
							X.PLMN_CALL_TYPE,
							X.PLMN_EXACT,
							X.PLMN_ISDN,
							X.BCT_PLMN_ID,
							X.BCT_CALL_TYPE_ID,
							X.BCT_PLMN_NAME,
							X.EVT_DRCTN_CD,
							X.EVT_CTGRY_CD,
							X.CALL_CTGRY_CD,
							X.ORG_KEY,
							X.CSP_TYP_CD
						FROM (		
							SELECT 	
								ROW_NUMBER() OVER (PARTITION BY A.STT,A.CALL_TYPE,A.CALLING_ISDN_STD,A.CALL_STA_TIME,A.FTP_FILENAME ORDER BY A.CALLING_ISDN_STD) AS ROW_NUM,
								A.*,
								B.IN_NET_DATE,
								B.NET_OPERATOR_FROM_ID,
								B.NET_OPERATOR_TO_ID,	
								C.MNP_ID,
								CASE 
									WHEN A.CALL_TYPE_DETAIL IN('MOC','RCA','TSO','TSC','TCA','MFWD') THEN
											CASE WHEN A.IMSI NOT LIKE '452%' 																			THEN 'VOICE_RMNG_ISD_OG'									 
												 WHEN A.IMSI NOT LIKE '45201%' 																			THEN 'VOICE_RMNG_LOCAL_OG'									 									 
												 WHEN (C.MNP_ID = '1' OR A.PLMN_PLMN_ID = '1') 															THEN 'VOICE_ONNET_OG'
												 WHEN A.BCT_PLMN_ID IS NOT NULL AND A.BCT_CALL_TYPE_ID IN('5013','278','298','6844','5484')				THEN 'VOICE_OFFNET_OG'
												 WHEN A.BCT_PLMN_ID IS NOT NULL AND A.BCT_CALL_TYPE_ID NOT IN('5013','278','298','6844','5484')			THEN 'VOICE_PSTN_OG'
												 WHEN A.PLMN_PLMN_ID IS NOT NULL AND A.BCT_PLMN_ID IS NULL      										THEN 'VOICE_ISD_OG'	  		 
											ELSE 'UNKNOWN' END											
									WHEN A.CALL_TYPE_DETAIL IN('MTC','TSI') THEN
											CASE WHEN A.IMSI NOT LIKE '452%' 																			THEN 'VOICE_RMNG_ISD_IC'									 
												 WHEN A.IMSI NOT LIKE '45201%' 																			THEN 'VOICE_RMNG_LOCAL_IC'									 									 
												 WHEN (C.PLMN_ID = '1' OR A.PLMN_PLMN_ID = '1') 														THEN 'VOICE_ONNET_IC'
												 WHEN A.BCT_PLMN_ID IS NOT NULL AND A.BCT_CALL_TYPE_ID IN('5013','278','298','6844','5484')				THEN 'VOICE_OFFNET_IC'
												 WHEN A.BCT_PLMN_ID IS NOT NULL AND A.BCT_CALL_TYPE_ID NOT IN('5013','278','298','6844','5484')			THEN 'VOICE_PSTN_IC'
												 WHEN A.PLMN_PLMN_ID IS NOT NULL AND A.BCT_PLMN_ID IS NULL      										THEN 'VOICE_ISD_IC'	  		 
											ELSE 'UNKNOWN' END																							
									WHEN A.CALL_TYPE_DETAIL = 'SMO' THEN
											CASE WHEN A.IMSI NOT LIKE '452%' 																			THEN 'SMS_RMNG_ISD_OG'
												 WHEN A.IMSI NOT LIKE '45201%' 																			THEN 'SMS_RMNG_LOCAL_OG'
												 WHEN (C.MNP_ID = '1' OR A.PLMN_PLMN_ID = '1') 				    										THEN 'SMS_ONNET_OG'
												 WHEN A.BCT_PLMN_ID IS NOT NULL								    										THEN 'SMS_OFFNET_OG'
												 WHEN A.PLMN_PLMN_ID IS NOT NULL AND A.BCT_PLMN_ID IS NULL      										THEN 'SMS_ISD_OG'	  		 
											ELSE 'UNKNOWN' END 		
									WHEN A.CALL_TYPE_DETAIL = 'SMT' THEN
											CASE WHEN A.IMSI NOT LIKE '452%' 																			THEN 'SMS_RMNG_ISD_IC'									 
												 WHEN A.IMSI NOT LIKE '45201%' 																			THEN 'SMS_RMNG_LOCAL_IC'									 									 
												 WHEN (C.PLMN_ID = '1' OR A.PLMN_PLMN_ID = '1') 														THEN 'SMS_ONNET_IC'
												 WHEN A.BCT_PLMN_ID IS NOT NULL								    										THEN 'SMS_OFFNET_IC'
												 WHEN A.PLMN_PLMN_ID IS NOT NULL AND A.BCT_PLMN_ID IS NULL      										THEN 'SMS_ISD_IC'	  		 
											ELSE 'UNKNOWN' END														
								ELSE NULL END AS CALL_TYP_CD,
										
								CASE WHEN A.CALL_TYPE_DETAIL IN('MTC','TSI','SMT') 							THEN 'I'
									 WHEN A.CALL_TYPE_DETAIL IN('MOC','RCA','TSO','TSC','SMO','TCA','MFWD') THEN 'O'
									 ELSE 'UNKNOWN' END	EVT_DRCTN_CD,												
								
								CASE 
									WHEN A.CALL_TYPE_DETAIL IN ('MOC','MTC','RCA','TSI','TSO','TSC','TCA','MFWD') THEN
											CASE WHEN A.IMSI NOT LIKE '452%' 														THEN 'VOICE_RMNG_ISD'									 
												 WHEN A.IMSI NOT LIKE '45201%' 														THEN 'VOICE_RMNG_LOCAL'									 									 
												 WHEN C.MNP_ID = '1' OR A.PLMN_PLMN_ID = '1' OR A.BCT_PLMN_ID IS NOT NULL			THEN 'VOICE_LOCAL'					 
												 WHEN A.PLMN_PLMN_ID IS NOT NULL AND A.BCT_PLMN_ID IS NULL      					THEN 'VOICE_ISD'	  		 
											ELSE 'UNKNOWN' END											
									WHEN A.CALL_TYPE_DETAIL IN('SMO','SMT') THEN
											CASE WHEN A.IMSI NOT LIKE '452%' 														THEN 'SMS_RMNG_ISD'									 
												 WHEN A.IMSI NOT LIKE '45201%' 														THEN 'SMS_RMNG_LOCAL'									 									 
												 WHEN C.MNP_ID = '1' OR A.PLMN_PLMN_ID = '1' OR A.BCT_PLMN_ID IS NOT NULL			THEN 'SMS_LOCAL'					 
												 WHEN A.PLMN_PLMN_ID IS NOT NULL AND A.BCT_PLMN_ID IS NULL      					THEN 'SMS_ISD'	  		 
											ELSE 'UNKNOWN' END																					
									ELSE NULL END AS EVT_CTGRY_CD,	
									
								CASE 
									WHEN A.CALL_TYPE_DETAIL IN ('MOC','MTC','RCA','TSI','TSO','TSC','TCA','MFWD') THEN
											CASE WHEN A.IMSI NOT LIKE '452%' 														THEN 'VOICE_RMNG_ISD'									 
												 WHEN A.IMSI NOT LIKE '45201%' 														THEN 'VOICE_RMNG_LOCAL'									 									 
											 WHEN (C.PLMN_ID = '1' OR A.PLMN_PLMN_ID = '1') 										THEN 'VOICE_LOCAL_ONNET'
												 WHEN A.BCT_PLMN_ID IS NOT NULL								   						THEN 'VOICE_LOCAL_OFFNET'			 
												 WHEN A.PLMN_PLMN_ID IS NOT NULL AND A.BCT_PLMN_ID IS NULL      					THEN 'VOICE_ISD'	  		 
											ELSE 'UNKNOWN' END											
									WHEN A.CALL_TYPE_DETAIL IN('SMO','SMT') THEN
											CASE WHEN A.IMSI NOT LIKE '452%' 														THEN 'SMS_RMNG_ISD'									 
												 WHEN A.IMSI NOT LIKE '45201%' 														THEN 'SMS_RMNG_LOCAL'									 									 
												 WHEN C.MNP_ID = '1' OR A.PLMN_PLMN_ID = '1' OR A.BCT_PLMN_ID IS NOT NULL			THEN 'SMS_LOCAL'					 
												 WHEN A.PLMN_PLMN_ID IS NOT NULL AND A.BCT_PLMN_ID IS NULL      					THEN 'SMS_ISD'	  		 
											ELSE 'UNKNOWN' END												
									ELSE NULL END AS CALL_CTGRY_CD,
									
								CASE WHEN C.MNP_ID = '1' 			 THEN CAST(C.MNP_ID AS BIGINT) 
									 WHEN A.BCT_PLMN_ID IS NOT NULL  THEN CAST(A.BCT_PLMN_ID AS BIGINT)	
									 WHEN A.PLMN_PLMN_ID IS NOT NULL THEN CAST(A.PLMN_PLMN_ID AS BIGINT)	
									 ELSE CAST(NULL AS BIGINT) END	AS ORG_KEY,								
									 
								CASE WHEN C.MNP_ID = '1' 			 THEN 'MOBILE'
									 WHEN A.BCT_PLMN_ID IS NOT NULL  THEN 'MOBILE'
									 WHEN A.PLMN_PLMN_ID IS NOT NULL THEN 'IRP'
									 ELSE CAST(NULL AS STRING) END	AS CSP_TYP_CD									 
							FROM (
								SELECT A.* FROM MBF_DATALAKE.MSC_CENTER_1 A WHERE A.DAY_KEY='$day_key' AND A.HOUR_KEY='$hour_key' 
								UNION ALL
								SELECT A.* FROM MBF_DATALAKE.MSC_CENTER_2 A WHERE A.DAY_KEY='$day_key' AND A.HOUR_KEY='$hour_key'
								UNION ALL
								SELECT A.* FROM MBF_DATALAKE.MSC_CENTER_3 A WHERE A.DAY_KEY='$day_key' AND A.HOUR_KEY='$hour_key'							
							) A 								
							LEFT JOIN MBF_DATALAKE.MNP_ISDN B 
								ON A.CALLED_ISDN_STD = B.ISDN AND B.STATUS = '1' AND B.DAY_KEY = DATE_FORMAT(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP('$day_key' , 'yyyyMMdd')), 1),'yyyyMMdd')
								AND FROM_UNIXTIME(UNIX_TIMESTAMP(B.IN_NET_DATE, 'yyyy-MM-dd HH:mm:ss.S')) <= DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP('$day_key', 'yyyyMMdd')), 1)
								AND DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP('$day_key', 'yyyyMMdd')), 1) <= FROM_UNIXTIME(UNIX_TIMESTAMP(NVL(B.OUT_NET_DATE, (SUBSTRING(DATE_FORMAT(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP('$day_key' , 'yyyyMMdd')), 1),'yyyyMMdd'),1,4) || '-' || SUBSTRING(DATE_FORMAT(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP('$day_key' , 'yyyyMMdd')), 1),'yyyyMMdd'),5,2) || '-' || SUBSTRING(DATE_FORMAT(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP('$day_key' , 'yyyyMMdd')), 1),'yyyyMMdd'),7,2)) || ' 00:00:00.0'), 'yyyy-MM-dd HH:mm:ss.S'))								
							LEFT JOIN (SELECT DISTINCT MNP_ID, PLMN_ID FROM MBF_DATALAKE.MAP_MNP) C ON B.NET_OPERATOR_TO_ID = C.MNP_ID														
						) X WHERE X.ROW_NUM = 1
					 ",
		"tempTable" : "ADME_RAW_GSM_CALL_CDR_TMP",
		"countSourceRecord" : "0"		
	  },	 
	  {
		"sql"      : "
				SELECT
					X.MO_KEY,X.UUID,X.EVT_CD,X.EVT_SEQ_NBR,X.PRTIAL_REC_NBR,X.EVT_START_DT,X.EVT_END_DT,X.EVT_BLLG_START_DT,X.FILE_NAME,X.FILE_DT,X.FILE_PREFIX_CD,X.REC_PROCESS_DT,X.SWITCH_CD,X. ACCT_SRVC_INSTANCE_KEY,X.SERVICE_NBR,X.ACCT_KEY,X.BILLABLE_ACCT_KEY,X.CUST_KEY,X.CUST_TYP_CD ,X.NTWK_QOS_GRP_CD,X.ACCT_ACTIVATION_DT,X.ACCT_CBS_ACTIVATION_DT,X.ACCT_LFCYCL_STAT_CD,X.SRVC_ACTIVATION_DT,X.SRVC_CBS_ACTIVATION_DT,X.SRVC_LFCYCL_STAT_CD,X.PROD_LINE_KEY,X.USAGE_PLAN_KEY,X.USAGE_PLAN_CD,X.USAGE_PLAN_TYP_CD,X.OFFER_KEY,
					X.OFFER_CD,X.PROD_KEY,X.PROD_CD,X.EVT_CLASS_CD,X.EVT_CTGRY_CD,X.EVT_TYP_CD,X.USAGE_TYP_CD,X.CALL_CTGRY_CD,X.CALL_TYP_CD,X.CALL_STAT_CD,X.CALL_TMNT_RSN_CD,X.ORIGNTNG_NBR,X.ORIGNTNG_IMSI,X.ORIGNTNG_IMEI,X.ORIGNTNG_CARRIER,X.ORIGNTNG_NTWK_ROUTE_CD,X.TRMNTNG_NBR,
					X.TRMNTNG_IMSI,X.TRMNTNG_IMEI,X.TRMNTNG_CARRIER,X.TRMNTNG_NTWK_ROUTE_CD,X.DLD_NBR,X.ORIGNTNG_CUG_KEY,X.TRMNTNG_CUG_KEY,X.INTL_DMSTC_IND,X.RMNG_IND,X.FNF_IND,X.SPECIAL_DLD_NBR_IND,X.TESTNG_NBR_IND,X.EVT_DRCTN_CD,X.ONNET_OFFNET_IND,X.REDRCTD_IND,X.REDRCTD_NBR,
					X.REDRCTD_IMSI,X.REDRCTD_IMEI,X.REDRCTD_CARRIER,X.THRD_PRTY_HOME_CNTRY_CD,X.ORIGNTNG_MSS_CD,X.TRMNTNG_MSS_CD,X.ORIGNTNG_PLMN_CD,X.TRMNTNG_PLMN_CD,X.ORIGNTNG_NETWORK_TYP,X.TRMNTNG_NETWORK_TYP,X.ORIGNTNG_HOME_COUNTRY_CD,X.ORIGNTNG_HOME_AREA_CD,
					X.ORIGNTNG_HOME_NETWORK_CD,X.ORIGNTNG_ROAM_COUNTRY_CD,X.ORIGNTNG_ROAM_AREA_CD,X.ORIGNTNG_ROAM_NETWORK_CD,X.TRMNTNG_HOME_COUNTRY_CD,X.TRMNTNG_HOME_AREA_CD,X.TRMNTNG_HOME_NETWORK_CD,X.TRMNTNG_ROAM_COUNTRY_CD,X.TRMNTNG_ROAM_AREA_CD,
					X.TRMNTNG_ROAM_NETWORK_CD,X.FIRST_ORIGNTNG_GCI_CD,X.LAST_ORIGNTNG_GCI_CD,X.ORIGNTNG_OPRTR_KEY,X.ORIGNTNG_OPRTR_CD,X.TRMNTNG_GCI_CD,X.TRMNTNG_OPRTR_KEY,X.TRMNTNG_OPRTR_CD,X.BLLG_OPRTR_KEY,X.BLLG_OPRTR_CD,X.ADDR_LAT,X.ADDR_LON,X.FIRST_CELL_KEY,
					X.FIRST_CELL_CD,X.FIRST_CELL_TYP_CD,X.FIRST_CELL_SITE_KEY,X.FIRST_CELL_SITE_CD,X.LAST_CELL_KEY,X.LAST_CELL_CD,X.LAST_CELL_TYP_CD,X.LAST_CELL_SITE_KEY,X.LAST_CELL_SITE_CD,X.NTWK_MGNT_CENTRE_KEY,X.NTWK_MGNT_CENTRE_CD,X.BSNS_RGN_KEY,X.BSNS_RGN_CD,
					X.BSNS_CLUSTER_KEY,X.BSNS_CLUSTER_CD,X.BSNS_MINICLUSTER_KEY,X.BSNS_MINICLUSTER_CD,X.GEO_CNTRY_KEY,X.GEO_CNTRY_CD,X.GEO_STATE_KEY,X.GEO_STATE_CD,X.GEO_DSTRCT_KEY,X.GEO_DSTRCT_CD,X.GEO_CITY_KEY,X.GEO_CITY_CD,X.ACQSTN_DT,X.ACQSTN_BSNS_OUTLET_KEY,
					X.ACQSTN_BSNS_OUTLET_CD,X.BILL_GRP_CD,X.BILL_CYCLE_KEY,X.CHRG_HEAD_KEY,X.CHRG_HEAD_CD,X.CRNCY_CD,X.USG_UOM_CD,X.INTCONN_CRNCY_CD,X.EXCHG_RATE_VAL,X.DRTN,X.RTD_USG,X. FREE_USG,X.CHRGBL_USG,X.INTCONN_USG,X.CHRGNG_PRTY_IND,X.OTHR_CHRGNG_NBR,X.CHRGNG_IND,
					X.CHRGING_ZONE_CD,X.RTD_CLASS_CD,X.RTNG_INFO,X.CHRGD_AMT,X.MARK_UP_AMT,X.DISCOUNT_AMT,X.TAX_AMT,X.BLLD_AMT,X.RVN_AMT,X.INTERNAL_COST_AMT,X.INTCONN_COST_AMT,X.INTCONN_RVN_AMT,X.BLLG_STAT_RSN_CD,X.LOYALTY_RANK_SCORE,X.LOYALTY_SCORE_DT,X.CREDIT_SCORE,
					X.CREDIT_CLASS_CD,X.CREDIT_SCORE_METHOD,X.CREDIT_SCORE_DT,X.RISK_IND,X.ACCT_SEGMENT_CD,X.ACCT_SEGMENT_DT,X.CMPGN_CD,X.SRC_SYS_KEY,X.SRC_SYS_CD,X.LOAD_DT,X.CURR_IND,X.WRHS_ID
				FROM (
						SELECT 
							ROW_NUMBER() OVER (PARTITION BY A.STT,A.CALL_TYPE,A.CALLING_ISDN_STD,A.CALL_STA_TIME,A.FTP_FILENAME ORDER BY A.CALLING_ISDN_STD) AS ROW_NUM_1,
							CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(A.CALL_STA_TIME,'dd/MM/yyyy HH:mm:ss'),'yyyyMM') AS STRING)  						AS MO_KEY,	
							CAST(NULL AS STRING) 																								AS UUID,
							CAST(NULL AS STRING) 																								AS EVT_CD,
							CAST(A.STT AS BIGINT) 																								AS EVT_SEQ_NBR,	
							CAST(NULL AS BIGINT) 																								AS PRTIAL_REC_NBR,	
							CAST(UNIX_TIMESTAMP(A.CALL_STA_TIME,'dd/MM/yyyy HH:mm:ss') AS TIMESTAMP) 											AS EVT_START_DT,	 	      
							CAST((UNIX_TIMESTAMP(A.CALL_STA_TIME,'dd/MM/yyyy HH:mm:ss') + NVL(A.DURATION,0)) AS TIMESTAMP) 						AS EVT_END_DT,
							CAST(NULL AS TIMESTAMP) 																							AS EVT_BLLG_START_DT,	
							A.FTP_FILENAME 																										AS FILE_NAME,
							CAST (NULL AS TIMESTAMP) 																							AS FILE_DT,
							CAST (NULL AS STRING) 																							    AS FILE_PREFIX_CD,
							CAST (NULL AS TIMESTAMP) 																							AS REC_PROCESS_DT,
							CAST (NULL AS STRING) 																							    AS SWITCH_CD,	 
							B.ACCT_SRVC_INSTANCE_KEY 																							AS ACCT_SRVC_INSTANCE_KEY,
							A.CALLING_ISDN_STD																									AS SERVICE_NBR,
							B.ACCT_KEY 																											AS ACCT_KEY,
							B.BILLABLE_ACCT_KEY  																								AS BILLABLE_ACCT_KEY,
							B.CUST_KEY 																											AS CUST_KEY,
							B.CUST_TYP_CD						      																			AS CUST_TYP_CD ,
							B.NTWK_QOS_GRP_CD																								    AS NTWK_QOS_GRP_CD,
							B.ACTIVATION_DT																							            AS ACCT_ACTIVATION_DT,
							B.CBS_ACTIVATION_DT																						            AS ACCT_CBS_ACTIVATION_DT,
							B.LFCYCL_STAT_CD																					                AS ACCT_LFCYCL_STAT_CD,
							CAST(NULL AS TIMESTAMP) 																							AS SRVC_ACTIVATION_DT,
							CAST(NULL AS TIMESTAMP) 																							AS SRVC_CBS_ACTIVATION_DT,
							CAST(NULL AS STRING) 																						    	AS SRVC_LFCYCL_STAT_CD,
							B.PROD_LINE_KEY 																									AS PROD_LINE_KEY,
							B.USAGE_PLAN_KEY 																									AS USAGE_PLAN_KEY,
							B.USAGE_PLAN_CD 																									AS USAGE_PLAN_CD,
							B.USAGE_PLAN_TYP_CD 																								AS USAGE_PLAN_TYP_CD,	
							B.CRM_OFFER_KEY																										AS OFFER_KEY,
							B.CRM_OFFER_CD																										AS OFFER_CD,
							CAST(NULL AS STRING) 																				         		AS PROD_KEY,
							CAST(NULL AS STRING)																								AS PROD_CD,							
							CASE WHEN CALL_TYPE_DETAIL IN('MOC','MTC','RCA','TSI','TSO','TSC','TCA','MFWD') THEN 'VOICE'
								 WHEN CALL_TYPE_DETAIL IN('SMO','SMT') THEN 'SMS'
								 ELSE 'UNKNOWN' END																								AS EVT_CLASS_CD,								
							A.EVT_CTGRY_CD																										AS EVT_CTGRY_CD,
							A.CALL_TYP_CD 																										AS EVT_TYP_CD,							
							CASE WHEN CALL_TYPE_DETAIL IN('MOC','MTC','RCA','TSI','TSO','TSC','TCA','MFWD') THEN 'VOICE'
								 WHEN CALL_TYPE_DETAIL IN('SMO','SMT') THEN 'SMS'
								 ELSE 'UNKNOWN' END																								AS USAGE_TYP_CD,															
							A.CALL_CTGRY_CD																										AS CALL_CTGRY_CD,
							A.CALL_TYP_CD 																										AS CALL_TYP_CD,
							CAST(NULL AS STRING)																								AS CALL_STAT_CD,
							CAST(NULL AS STRING)																								AS CALL_TMNT_RSN_CD,	
							
							CASE WHEN A.EVT_DRCTN_CD = 'O' THEN A.CALLING_ISDN_STD ELSE A.CALLED_ISDN_STD END									AS ORIGNTNG_NBR,
							CASE WHEN A.EVT_DRCTN_CD = 'O' THEN A.IMSI ELSE CAST(NULL AS STRING) END 											AS ORIGNTNG_IMSI,
							CASE WHEN A.EVT_DRCTN_CD = 'O' THEN A.CALLING_IMEI ELSE CAST(NULL AS STRING) END									AS ORIGNTNG_IMEI,
							CAST(NULL AS STRING)																								AS ORIGNTNG_CARRIER,
							CAST(NULL AS STRING)																								AS ORIGNTNG_NTWK_ROUTE_CD,								
							
							CASE WHEN A.EVT_DRCTN_CD = 'O' THEN A.CALLED_ISDN_STD ELSE A.CALLING_ISDN_STD END									AS TRMNTNG_NBR,
							CASE WHEN A.EVT_DRCTN_CD = 'O' THEN CAST(NULL AS STRING) ELSE A.IMSI END 											AS TRMNTNG_IMSI,
							CASE WHEN A.EVT_DRCTN_CD = 'O' THEN CAST(NULL AS STRING) ELSE A.CALLING_IMEI END									AS TRMNTNG_IMEI,														
							CAST(NULL AS STRING)																								AS TRMNTNG_CARRIER,
							CAST(NULL AS STRING)																								AS TRMNTNG_NTWK_ROUTE_CD,
							CAST(NULL AS STRING)																								AS DLD_NBR,
							CAST(NULL AS BIGINT) 																								AS ORIGNTNG_CUG_KEY,
							CAST(NULL AS BIGINT) 																								AS TRMNTNG_CUG_KEY,
							CAST(NULL AS STRING)																								AS INTL_DMSTC_IND,
							CASE WHEN A.IMSI NOT LIKE '45201%' THEN '1' ELSE CAST(NULL AS STRING) END 											AS RMNG_IND,
							CAST(NULL AS STRING)																								AS FNF_IND,
							CAST(NULL AS STRING)																								AS SPECIAL_DLD_NBR_IND,
							CAST(NULL AS STRING)																								AS TESTNG_NBR_IND,	
							A.EVT_DRCTN_CD 																										AS EVT_DRCTN_CD,
							CAST(NULL AS STRING)																								AS ONNET_OFFNET_IND,
							CAST(NULL AS STRING)																								AS REDRCTD_IND,
							A.CALLING_ORG 																									    AS REDRCTD_NBR,
							CAST(NULL AS STRING)																								AS REDRCTD_IMSI,
							CAST(NULL AS STRING)																								AS REDRCTD_IMEI,
							CAST(NULL AS STRING)																								AS REDRCTD_CARRIER,
							CAST(NULL AS STRING)																								AS THRD_PRTY_HOME_CNTRY_CD,
							CAST(NULL AS STRING)																								AS ORIGNTNG_MSS_CD,
							CAST(NULL AS STRING)																								AS TRMNTNG_MSS_CD,
							CASE WHEN A.EVT_DRCTN_CD = 'O' THEN CAST(NULL AS STRING) ELSE D.PLMN_CD END											AS ORIGNTNG_PLMN_CD,
							CASE WHEN A.EVT_DRCTN_CD = 'O' THEN D.PLMN_CD ELSE CAST(NULL AS STRING) END											AS TRMNTNG_PLMN_CD,
							CAST(NULL AS STRING)																								AS ORIGNTNG_NETWORK_TYP,
							CAST(NULL AS STRING)																								AS TRMNTNG_NETWORK_TYP,
							CASE WHEN A.EVT_DRCTN_CD = 'O' THEN SUBSTRING(A.IMSI,1,3) ELSE CAST(NULL AS STRING) END  							AS ORIGNTNG_HOME_COUNTRY_CD,
							CAST(NULL AS STRING)																								AS ORIGNTNG_HOME_AREA_CD,
							CASE WHEN A.EVT_DRCTN_CD = 'O' THEN SUBSTRING(A.IMSI,4,2) ELSE CAST(NULL AS STRING) END                             AS ORIGNTNG_HOME_NETWORK_CD,
							CASE WHEN A.IMSI NOT LIKE '45201%' THEN SUBSTRING(A.IMSI,1,3) ELSE CAST(NULL AS STRING) END                         AS ORIGNTNG_ROAM_COUNTRY_CD,
							CAST(NULL AS STRING)																								AS ORIGNTNG_ROAM_AREA_CD,
							CASE WHEN A.IMSI NOT LIKE '45201%' THEN SUBSTRING(A.IMSI,4,2) ELSE CAST(NULL AS STRING) END                         AS ORIGNTNG_ROAM_NETWORK_CD,
							CASE WHEN A.EVT_DRCTN_CD = 'O' THEN CAST(NULL AS STRING) ELSE SUBSTRING(A.IMSI,1,3) END								AS TRMNTNG_HOME_COUNTRY_CD,
							CAST(NULL AS STRING)																								AS TRMNTNG_HOME_AREA_CD,														
							CASE WHEN A.EVT_DRCTN_CD = 'O' THEN CAST(NULL AS STRING) ELSE SUBSTRING(A.IMSI,4,2) END                             AS TRMNTNG_HOME_NETWORK_CD,							
							CAST(NULL AS STRING)																								AS TRMNTNG_ROAM_COUNTRY_CD,
							CAST(NULL AS STRING)																								AS TRMNTNG_ROAM_AREA_CD,
							CAST(NULL AS STRING)																								AS TRMNTNG_ROAM_NETWORK_CD,
							A.CELL_ID      	  																									AS FIRST_ORIGNTNG_GCI_CD,
							CAST(NULL AS STRING)																								AS LAST_ORIGNTNG_GCI_CD,	
							CASE WHEN A.EVT_DRCTN_CD = 'O' THEN CAST(NULL AS BIGINT) ELSE D.EXTRNL_CSP_KEY END									AS ORIGNTNG_OPRTR_KEY,							
							CASE WHEN A.EVT_DRCTN_CD = 'O' THEN CAST(NULL AS BIGINT) ELSE D.EXTRNL_CSP_CD END									AS ORIGNTNG_OPRTR_CD,							
							CAST(NULL AS STRING)																								AS TRMNTNG_GCI_CD,
							CASE WHEN A.EVT_DRCTN_CD = 'O' THEN D.EXTRNL_CSP_KEY ELSE CAST(NULL AS BIGINT) END									AS TRMNTNG_OPRTR_KEY,							
							CASE WHEN A.EVT_DRCTN_CD = 'O' THEN D.EXTRNL_CSP_CD ELSE CAST(NULL AS BIGINT) END 									AS TRMNTNG_OPRTR_CD,
							CAST(NULL AS BIGINT) 																								AS BLLG_OPRTR_KEY,
							CAST(NULL AS STRING)																								AS BLLG_OPRTR_CD,
							C.ADDR_LAT 																											AS ADDR_LAT,
							C.ADDR_LON 																											AS ADDR_LON,
							C.CELL_KEY 																											AS FIRST_CELL_KEY,
							C.CELL_CD																											AS FIRST_CELL_CD,
							C.CELL_TYP_CD																									    AS FIRST_CELL_TYP_CD,
							C.CELL_SITE_KEY 																									AS FIRST_CELL_SITE_KEY,
							C.CELL_SITE_CD																										AS FIRST_CELL_SITE_CD,
							CAST(NULL AS STRING) 																								AS LAST_CELL_KEY,
							CAST(NULL AS STRING)																								AS LAST_CELL_CD,
							CAST(NULL AS STRING)																								AS LAST_CELL_TYP_CD,
							CAST(NULL AS BIGINT) 																								AS LAST_CELL_SITE_KEY,
							CAST(NULL AS STRING)																								AS LAST_CELL_SITE_CD,
							C.NTWK_MGNT_CENTRE_KEY 																								AS NTWK_MGNT_CENTRE_KEY,
							C.NTWK_MGNT_CENTRE_CD 																								AS NTWK_MGNT_CENTRE_CD,	
							C.BSNS_RGN_KEY 																										AS BSNS_RGN_KEY,
							C.BSNS_RGN_CD 																										AS BSNS_RGN_CD,
							C.BSNS_CLUSTER_KEY 																									AS BSNS_CLUSTER_KEY,
							C.BSNS_CLUSTER_CD 																									AS BSNS_CLUSTER_CD,
							C.BSNS_MINICLUSTER_KEY 																								AS BSNS_MINICLUSTER_KEY,
							C.BSNS_MINICLUSTER_CD 																								AS BSNS_MINICLUSTER_CD,
							C.GEO_CNTRY_KEY 																									AS GEO_CNTRY_KEY,
							C.GEO_CNTRY_CD 																										AS GEO_CNTRY_CD,
							C.GEO_STATE_KEY 																									AS GEO_STATE_KEY,
							C.GEO_STATE_CD 																										AS GEO_STATE_CD,
							C.GEO_DSTRCT_KEY 																									AS GEO_DSTRCT_KEY,
							C.GEO_DSTRCT_CD 																									AS GEO_DSTRCT_CD,
							C.GEO_CITY_KEY 																										AS GEO_CITY_KEY,
							C.GEO_CITY_CD 																										AS GEO_CITY_CD,
							B.ACQSTN_DT 																										AS ACQSTN_DT,
							B.ACQSTN_BSNS_OUTLET_KEY 																							AS ACQSTN_BSNS_OUTLET_KEY,
							B.ACQSTN_BSNS_OUTLET_CD 																							AS ACQSTN_BSNS_OUTLET_CD,
							CAST(NULL AS STRING)																								AS BILL_GRP_CD,
							CAST(NULL AS BIGINT)																								AS BILL_CYCLE_KEY,
							CAST(NULL AS INTEGER)																								AS CHRG_HEAD_KEY,
							CAST(NULL AS STRING)																								AS CHRG_HEAD_CD,
							'VND' 																											    AS CRNCY_CD,
							CASE WHEN CALL_TYPE_DETAIL IN('MOC','MTC','RCA','TSI','TSO','TSC','TCA','MFWD') THEN 'SEC'
								 WHEN CALL_TYPE_DETAIL IN('SMO','SMT') THEN 'UNIT'
								 ELSE '' END																									AS USG_UOM_CD,	
							'VND' 																												AS INTCONN_CRNCY_CD,
							CAST(NULL AS DECIMAL(27,8)) 																						AS EXCHG_RATE_VAL,
							CAST(A.DURATION AS BIGINT)																							AS DRTN,
							CAST(A.DURATION AS BIGINT)																							AS RTD_USG, 	
							CAST(NULL AS BIGINT)																							    AS FREE_USG,
							CAST(NULL AS BIGINT)																							    AS CHRGBL_USG,
							CAST(NULL AS BIGINT)																							    AS INTCONN_USG,
							CAST(NULL AS STRING)																								AS CHRGNG_PRTY_IND,
							CAST(NULL AS STRING)																								AS OTHR_CHRGNG_NBR,
							CAST(NULL AS STRING)																								AS CHRGNG_IND,
							CAST(NULL AS STRING)																								AS CHRGING_ZONE_CD,
							CAST(NULL AS STRING)																								AS RTD_CLASS_CD,
							CAST(NULL AS STRING)																								AS RTNG_INFO,
							CAST(NULL AS DECIMAL(27,8)) 																						AS CHRGD_AMT,
							CAST(NULL AS DECIMAL(27,8)) 																						AS MARK_UP_AMT,
							CAST(NULL AS DECIMAL(27,8)) 																						AS DISCOUNT_AMT,
							CAST(NULL AS DECIMAL(27,8)) 																						AS TAX_AMT,
							CAST(NULL AS DECIMAL(27,8)) 																						AS BLLD_AMT,
							CAST(NULL AS DECIMAL(27,8)) 																						AS RVN_AMT,
							CAST(NULL AS DECIMAL(27,8)) 																						AS INTERNAL_COST_AMT,
							CAST(NULL AS DECIMAL(27,8)) 																						AS INTCONN_COST_AMT,
							CAST(NULL AS DECIMAL(27,8)) 																						AS INTCONN_RVN_AMT,
							CAST(NULL AS STRING)																								AS BLLG_STAT_RSN_CD,
							B.LOYALTY_RANK_SCORE 																								AS LOYALTY_RANK_SCORE,
							B.LOYALTY_SCORE_DT																									AS LOYALTY_SCORE_DT,
							B.CREDIT_SCORE				 																						AS CREDIT_SCORE,
							B.CREDIT_CLASS_CD 																									AS CREDIT_CLASS_CD,
							B.CREDIT_SCORE_METHOD 																								AS CREDIT_SCORE_METHOD,
							B.CREDIT_SCORE_DT 																									AS CREDIT_SCORE_DT,
							B.RISK_IND 																											AS RISK_IND,
							CAST(NULL AS STRING)																								AS ACCT_SEGMENT_CD,
							CAST(NULL AS STRING)																								AS ACCT_SEGMENT_DT,
							CAST(NULL AS STRING)																								AS CMPGN_CD,
							20   																												AS SRC_SYS_KEY,
							'SMSC' 																												AS SRC_SYS_CD,
							CURRENT_TIMESTAMP() 																								AS LOAD_DT,
							'1' 																												AS CURR_IND,
							1 																													AS WRHS_ID                      
						FROM ADME_RAW_GSM_CALL_CDR_TMP A 
						LEFT OUTER JOIN MBF_BIGDATA.ADMR_ACCOUNT_SERVICE B
						ON A.CALLING_ISDN_STD = B.SERVICE_NBR AND B.DAY_KEY = DATE_FORMAT(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP('$day_key' , 'yyyyMMdd')), 1),'yyyyMMdd')
						LEFT JOIN MBF_BIGDATA.ADMR_CELL C ON C.CELL_KEY = A.CELL_ID_STD AND C.DAY_KEY = DATE_FORMAT(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP('$day_key' , 'yyyyMMdd')), 1),'yyyyMMdd')
						LEFT JOIN MBF_BIGDATA.ADMR_EXTERNAL_CSP D ON A.ORG_KEY = D.ORG_KEY AND A.CSP_TYP_CD = D.CSP_TYP_CD AND D.DAY_KEY = '$day_key'		
					) X WHERE X.ROW_NUM_1 = 1
					 ",
		"tempTable" : "",
		"countSourceRecord" : "0"		
	  }	  
	]
}


