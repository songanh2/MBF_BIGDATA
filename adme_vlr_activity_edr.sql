{
	 "description":"ADME_VLR_ACTIVITY_EDR",
	 "sqlStatements": [
	  {
		"sql"      	:"
					SELECT 
						cast(from_unixtime(unix_timestamp('$day_key','yyyyMMdd'),'yyyyMM') as INTEGER)	AS MO_KEY,
						CAST(NULL AS STRING) 												AS UUID,
						CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(A.START_DATE,'yyyy-MM-dd hh:mm:ssa')) AS TIMESTAMP)		AS VLR_RECORD_DT,
						CAST(NULL AS STRING) 												AS FILE_NAME,
						CAST(NULL AS TIMESTAMP) 											AS FILE_DT,
						CAST(NULL AS STRING) 												AS FILE_PREFIX_CD,
						CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(A.START_DATE,'yyyy-MM-dd hh:mm:ssa')) AS TIMESTAMP)		AS REC_PROCESS_DT,
						B.ACCT_SRVC_INSTANCE_KEY 											AS ACCT_SRVC_INSTANCE_KEY,
						B.ACCT_KEY 															AS ACCT_KEY,
						B.BILLABLE_ACCT_KEY 												AS BILLABLE_ACCT_KEY,
						B.CUST_KEY 															AS CUST_KEY,
						B.CUST_TYP_CD 														AS CUST_TYP_CD,
						B.NTWK_QOS_GRP_CD 													AS NTWK_QOS_GRP_CD,
						B.ACTIVATION_DT 													AS ACCT_ACTIVATION_DT,
						B.CBS_ACTIVATION_DT 												AS ACCT_CBS_ACTIVATION_DT,
						B.LFCYCL_STAT_CD 													AS ACCT_LFCYCL_STAT_CD,
						B.ACTIVATION_DT 													AS SRVC_ACTIVATION_DT,
						B.ACTIVATION_DT 													AS SRVC_CBS_ACTIVATION_DT,
						B.LFCYCL_STAT_CD 													AS SRVC_LFCYCL_STAT_CD,
						B.PROD_LINE_KEY 													AS PROD_LINE_KEY,
						B.USAGE_PLAN_KEY 													AS USAGE_PLAN_KEY,
						B.USAGE_PLAN_CD 													AS USAGE_PLAN_CD,
						B.USAGE_PLAN_TYP_CD 												AS USAGE_PLAN_TYP_CD,
						cast(A.SWITCH_ID as string)											AS VLR_SWITCH_CD,
						C.NTWK_MGNT_CENTRE_KEY 												AS VLR_NTWK_MGNT_CENTRE_KEY,
						C.NTWK_MGNT_CENTRE_CD 												AS VLR_NTWK_MGNT_CENTRE_CD,
						504003 																AS VLR_GEO_CNTRY_KEY,
						'VN' 																AS VLR_GEO_CNTRY_CD,
						C.BSNS_RGN_KEY 														AS VLR_BSNS_RGN_KEY,
						C.BSNS_RGN_CD 														AS VLR_BSNS_RGN_CD,
						C.BSNS_CLUSTER_KEY 													AS VLR_BSNS_CLUSTER_KEY,
						C.BSNS_CLUSTER_CD 													AS VLR_BSNS_CLUSTER_CD,
						C.BSNS_MINICLUSTER_KEY												AS VLR_BSNS_MINICLUSTER_KEY,
						C.BSNS_MINICLUSTER_CD 												AS VLR_BSNS_MINICLUSTER_CD,
						C.GEO_STATE_KEY 													AS VLR_GEO_STATE_KEY,
						C.GEO_STATE_CD 														AS VLR_GEO_STATE_CD,
						C.GEO_DSTRCT_KEY 													AS VLR_GEO_DSTRCT_KEY,
						C.GEO_DSTRCT_CD 													AS VLR_GEO_DSTRCT_CD,
						C.GEO_CITY_KEY														AS VLR_GEO_CITY_KEY,
						C.GEO_CITY_CD 														AS VLR_GEO_CITY_CD,
						A.SW_STATE															AS VLR_STAT_CD,
						(CASE 
							WHEN A.STATUS_GPRS	IN ('0','3') THEN '2G'
							WHEN A.STATUS_GPRS = '1' THEN '3G'
							WHEN A.STATUS_GPRS = '2' THEN '4G'
						END) 																AS VLR_TYPE,
						A.ISDN_CH															AS VISITOR_SERVICE_NBR,
						(CASE WHEN SUBSTRING(A.IMSI,1,5) <> '45201' THEN '1' ELSE '0' END) 	AS VISITOR_ROMING_IND,
						(CASE WHEN SUBSTRING(A.IMSI,1,3) = '452' THEN 'D' ELSE 'I' END) 	AS VISITOR_INTL_DMSTC_IND,
						F.GEO_CNTRY_CD														AS VISITOR_HOME_COUNTRY_CD,
						F.EXTRNL_CSP_KEY													AS VISITOR_EXTRNL_CSP_KEY,
						F.EXTRNL_CSP_CD 													AS VISITOR_EXTRNL_CSP_CD,
						(CASE WHEN A.SW_STATE  = 'Detached' THEN '0' 
							  WHEN A.SW_STATE IN ('IDLE','Attached','BUSY') OR A.SW_STATE IS NULL  THEN '1'
							  WHEN A.SW_STATE IN ('DET','IDET') THEN '2' END)				AS VISITOR_VLR_STAT_CD,
						(Case When BB.VISITOR_VLR_STAT_CD <>  '1'  and (A.SW_STATE IN ('IDLE','Attached','BUSY') OR A.SW_STATE IS NULL)  then cast(from_unixtime(unix_timestamp('$day_key','yyyyMMdd')) as timestamp) end)	AS VISITOR_VLR_ATTACHED_DT,
						(Case When BB.VISITOR_VLR_STAT_CD <>  '0'  and A.SW_STATE  = 'Detached'  then cast(from_unixtime(unix_timestamp('$day_key','yyyyMMdd')) as timestamp) end) AS VISITOR_VLR_DETACHED_DT,
						CAST(NULL AS INTEGER) 												AS VISITOR_VLR_DETTACHED_IN_HOUR,
						(Case When BB.VISITOR_VLR_STAT_CD <>  '2'  and  A.SW_STATE IN ('DET','IDET')  then cast(from_unixtime(unix_timestamp('$day_key','yyyyMMdd')) as timestamp) end)	AS VISITOR_VLR_DELETED_DT,
						(Case When A.SW_STATE IN ('IDLE','Attached','BUSY') OR A.SW_STATE IS NULL  then cast(from_unixtime(unix_timestamp('$day_key','yyyyMMdd')) as timestamp) end)	AS VISITOR_VLR_LAST_ACCESS_TIME,
						BB.VISITOR_VLR_STAT_CD												AS VISITOR_VLR_PREV_STAT_CD,
						(Case When BB.VISITOR_VLR_STAT_CD <> (CASE  WHEN A.SW_STATE  = 'Detached' THEN '0' 
																	WHEN A.SW_STATE IN ('IDLE','Attached','BUSY') OR A.SW_STATE IS NULL  THEN '1'
																	WHEN A.SW_STATE IN ('DET','IDET') THEN '2' END)  then cast(from_unixtime(unix_timestamp('$day_key','yyyyMMdd')) as timestamp) 
						end)																AS VISITOR_VLR_STAT_CHANGE_DT,
						CAST(NULL AS bigint) 												AS VISITOR_VLR_LAC,
						CAST(NULL AS bigint) 												AS VISITOR_VLR_CI,
						C.CELL_ID 															AS VISITOR_VLR_GCI,
						C.CELL_KEY															AS VISITOR_VLR_CELL_KEY,
						SUBSTRING(A.CELL_ID,LENGTH(split(A.CELL_ID,'-')[0]) + LENGTH(split(A.CELL_ID,'-')[1]) + LENGTH(split(A.CELL_ID,'-')[2]) + 4) AS VISITOR_VLR_CELL_CD,
						C.CELL_SITE_KEY			 											AS VISITOR_VLR_CELL_SITE_KEY,
						C.CELL_SITE_CD 														AS VISITOR_VLR_CELL_SITE_CD,
						C.CELL_TYP_CD 														AS VISITOR_VLR_CELL_TYP_CD,
						cast(A.IMEI as decimal(20,0))										AS VISITOR_VLR_IMEI,
						cast(A.IMSI as decimal(20,0))										AS VISITOR_VLR_IMSI,
						CAST(NULL AS bigint) 												AS SIM_GEO_STATE_KEY,
						CAST(NULL AS STRING) 												AS HNDST_TYP_CD,
						A.OS 																AS HNDST_OS_TYP_CD,
						E.BRAND_KEY 														AS HNDST_BRND_KEY,
						A.BRAND 															AS HNDST_BRND_CD,
						E.HNDST_MDL_KEY 													AS HNDST_MDL_KEY,
						A.MODEL 															AS HNDST_MDL_CD,
						E.HNDST_MDL_TYP_CD 													AS HNDST_MDL_TYP_CD,
						CAST(NULL AS decimal(20,0)) 										AS HNDST_IMEI,
						CAST(NULL AS bigint)											 	AS HNDST_TAC_CD,
						CAST(NULL AS INTEGER) 												AS HNDST_FAC_CD,
						CAST(NULL AS bigint) 												AS NTWK_MGNT_CENTRE_KEY,
						CAST(NULL AS STRING) 												AS NTWK_MGNT_CENTRE_CD,
						B.BSNS_RGN_KEY 														AS BSNS_RGN_KEY,
						B.BSNS_RGN_CD 														AS BSNS_RGN_CD,
						B.BSNS_CLUSTER_KEY 													AS BSNS_CLUSTER_KEY,
						B.BSNS_CLUSTER_CD 													AS BSNS_CLUSTER_CD,
						B.BSNS_MINICLUSTER_KEY 												AS BSNS_MINICLUSTER_KEY,
						B.BSNS_MINICLUSTER_CD 												AS BSNS_MINICLUSTER_CD,
						504003 																AS GEO_CNTRY_KEY,
						'VN' 																AS GEO_CNTRY_CD,
						B.GEO_STATE_KEY													 	AS GEO_STATE_KEY,
						B.GEO_STATE_CD 														AS GEO_STATE_CD,
						B.GEO_DSTRCT_KEY 													AS GEO_DSTRCT_KEY,
						B.GEO_DSTRCT_CD 													AS GEO_DSTRCT_CD,
						B.GEO_CITY_KEY 														AS GEO_CITY_KEY,
						B.GEO_CITY_CD 														AS GEO_CITY_CD,
						B.ACTIVATION_DT 													AS ACQSTN_DT,
						B.ACQSTN_BSNS_OUTLET_KEY 											AS ACQSTN_BSNS_OUTLET_KEY,
						B.ACQSTN_BSNS_OUTLET_CD 											AS ACQSTN_BSNS_OUTLET_CD,
						B.LOYALTY_RANK_SCORE 												AS LOYALTY_RANK_SCORE,
						B.LOYALTY_SCORE_DT 													AS LOYALTY_SCORE_DT,
						CAST(NULL AS DECIMAL(12,4)) 										AS CREDIT_SCORE,
						CAST(NULL AS STRING) 												AS CREDIT_CLASS_CD,
						CAST(NULL AS STRING) 												AS CREDIT_SCORE_METHOD,
						CAST(NULL AS TIMESTAMP) 											AS CREDIT_SCORE_DT,
						CAST(NULL AS INTEGER) 												AS RISK_IND,
						CAST(NULL AS STRING) 												AS ACCT_SEGMENT_CD,
						CAST(NULL AS STRING) 												AS ACCT_SEGMENT_DT,
						CAST(NULL AS STRING) 												AS CMPGN_CD,
						32 																	AS SRC_SYS_KEY,
						'VLR' 																AS SRC_SYS_CD,
						CURRENT_TIMESTAMP() 												AS LOAD_DT,
						CURRENT_TIMESTAMP() 												AS LAST_UPDT_DT,
						'SYSTEM' 															AS LAST_UPDT_BY,
						'1' 																AS CURR_IND,
						1 																	AS WRHS_ID
					FROM
					
					(SELECT START_DATE, SWITCH_ID, IMSI, CELL_ID, IMEI, OS, BRAND, MODEL,ISDN,day_key,sw_state,STATUS_GPRS,
					 (CASE  WHEN ISDN IS NULL THEN CAST(NULL AS STRING)
							WHEN SUBSTRING(REPLACE(ISDN,'+',''),1,1) = '0' AND SUBSTRING(SUBSTRING(REPLACE(ISDN,'+',''),2),1,1) ='8' AND LENGTH(SUBSTRING(REPLACE(ISDN,'+',''),2)) = 9 
								THEN SUBSTRING(REPLACE(ISDN,'+',''),2)
							WHEN SUBSTRING(REPLACE(ISDN,'+',''),1,1) = '8' AND LENGTH(SUBSTRING(REPLACE(ISDN,'+',''),1)) = 9 
								THEN SUBSTRING(REPLACE(ISDN,'+',''),1)
							WHEN SUBSTRING(REPLACE(ISDN,'+',''),1,2) = '84' 
								THEN SUBSTRING(REPLACE(ISDN,'+',''),3)
							WHEN SUBSTRING(REPLACE(ISDN,'+',''),1,3) = '084'
								THEN SUBSTRING(REPLACE(ISDN,'+',''),4)
							WHEN SUBSTRING(REPLACE(ISDN,'+',''),1,1) = '0' 
								THEN SUBSTRING(REPLACE(ISDN,'+',''),2)
					 END) AS ISDN_CH, row_number() over(partition by isdn order by isdn) row_num 
					FROM MBF_DATALAKE.DUMP_VLR 	WHERE DAY_KEY = '$day_key') A 
					LEFT JOIN MBF_BIGDATA.ADMR_ACCOUNT_SERVICE B 									        	ON A.ISDN_CH = B.service_nbr  AND B.DAY_KEY = '$day_key'
					LEFT JOIN MBF_BIGDATA.ADMR_CELL C 													        ON A.CELL_ID = C.CELL_KEY AND C.DAY_KEY = '$day_key'
					LEFT JOIN MBF_BIGDATA.ADMR_HNDST_MODEL E 										        	ON A.MODEL = E.HNDST_MDL_CD and A.BRAND = E.BRAND_CD  AND E.DAY_KEY = '$day_key'
					LEFT JOIN MBF_BIGDATA.ADMR_EXTERNAL_CSP F										        	ON F.IMSI_START_CD = SUBSTRING(TRIM(A.IMSI),1,5) AND F.VSTATUS = 'Y' AND F.CSP_TYP_CD = 'IRP' AND F.DAY_KEY = '$day_key'
					LEFT JOIN MBF_BIGDATA.ADME_VLR_ACTIVITY_EDR BB on BB.VISITOR_SERVICE_NBR = A.ISDN_CH AND BB.DAY_KEY = DATE_FORMAT(DATE_ADD(FROM_UNIXTIME(UNIX_TIMESTAMP('$day_key','yyyyMMdd')),-1),'yyyyMMdd')
					where a.row_num = 1
					
					 ",
		"tempTable"  : "ADME_VLR_ACTIVITY_EDR_TMP",
		"countSourceRecord":"1"
	  }   	  
	]
}