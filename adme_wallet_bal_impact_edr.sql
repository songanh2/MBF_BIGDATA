{
	 "description":"ADME_WALLET_BAL_IMPACT_EDR",
	 "sqlStatements": [
	  {
		"sql"      : "
						SELECT 
							A.*
						FROM (
							SELECT 
								A.*,																								
								TK_BONUS.TK TK 
							FROM (
									SELECT 										
										CALL_STA_TIME,
										CALL_END_TIME,
										CALLED_ISDN,
										TRANSACTION_DESCRIPTION,
										FTP_FILENAME,
										CASE WHEN LENGTH(NVL(ORG_CALL_ID,'')) <> 0
											  THEN
											   CASE
												 WHEN SUBSTR(ORG_CALL_ID, 1, 4) = '0001'
												 THEN
												   SUBSTR(ORG_CALL_ID, 5)
												 ELSE
												   CAST(CAST(ORG_CALL_ID AS INT) AS STRING)
											   END
											 WHEN EVT_DRCTN_CD = 'I' THEN CALLED_ISDN_STD
											 ELSE CALLING_ISDN_STD
										END AS CALLING_ISDN_STD,				
										CALL_TYPE_IND,
										CALL_TYPE,
										SCRATCH_VALUE,
										EVT_CTGRY_CD,							
										CALL_CTGRY_CD,
										CALL_TYP_CD,
										EVT_DRCTN_CD,
										CALLED_ISDN_STD,									
										TK1,TK2,TK3,TK4,TK5,TK6,TK7,TK8,TK9,TK10,
										TK1_CONSUMED,TK2_CONSUMED,TK3_CONSUMED,TK4_CONSUMED,TK5_CONSUMED,TK6_CONSUMED,TK7_CONSUMED,TK8_CONSUMED,TK9_CONSUMED,TK10_CONSUMED,
										TK1_INITIAL,TK2_INITIAL,TK3_INITIAL,TK4_INITIAL,TK5_INITIAL,TK6_INITIAL,TK7_INITIAL,TK8_INITIAL,TK9_INITIAL,TK10_INITIAL,
										TK1_REMAINING,TK2_REMAINING,TK3_REMAINING,TK4_REMAINING,TK5_REMAINING,TK6_REMAINING,TK7_REMAINING,TK8_REMAINING,TK9_REMAINING,TK10_REMAINING,
										TK1_LIMIT_DATE,TK2_LIMIT_DATE,TK3_LIMIT_DATE,TK4_LIMIT_DATE,TK5_LIMIT_DATE,TK6_LIMIT_DATE,TK7_LIMIT_DATE,TK8_LIMIT_DATE,TK9_LIMIT_DATE,TK10_LIMIT_DATE,
										CELL_ID,
										IMSI,
										'CREDIT' AS TK11,
										CREDIT_BEFORE_ADJUSTMENT AS TK11_INITIAL,
										CREDIT_CHARGED AS TK11_CONSUMED,
										CREDIT_REMAINING AS TK11_REMAINING 
									FROM MBF_STAGE.STG_CDR_IN_ENRICH
									WHERE DAY_KEY = '$day_key'
								) A
							LATERAL VIEW EXPLODE(ARRAY(						
													CONCAT_WS(':',TK1,CAST(NVL(TK1_CONSUMED,'') AS STRING),CAST(NVL(TK1_INITIAL,'') AS STRING),CAST(NVL(TK1_REMAINING,'') AS STRING), CAST(TK1_LIMIT_DATE AS STRING)),
													CONCAT_WS(':',TK2,CAST(NVL(TK2_CONSUMED,'') AS STRING),CAST(NVL(TK2_INITIAL,'') AS STRING),CAST(NVL(TK2_REMAINING,'') AS STRING), CAST(TK2_LIMIT_DATE AS STRING)),
													CONCAT_WS(':',TK3,CAST(NVL(TK3_CONSUMED,'') AS STRING),CAST(NVL(TK3_INITIAL,'') AS STRING),CAST(NVL(TK3_REMAINING,'') AS STRING), CAST(TK3_LIMIT_DATE AS STRING)),
													CONCAT_WS(':',TK4,CAST(NVL(TK4_CONSUMED,'') AS STRING),CAST(NVL(TK4_INITIAL,'') AS STRING),CAST(NVL(TK4_REMAINING,'') AS STRING), CAST(TK4_LIMIT_DATE AS STRING)),
													CONCAT_WS(':',TK5,CAST(NVL(TK5_CONSUMED,'') AS STRING),CAST(NVL(TK5_INITIAL,'') AS STRING),CAST(NVL(TK5_REMAINING,'') AS STRING), CAST(TK5_LIMIT_DATE AS STRING)),
													CONCAT_WS(':',TK6,CAST(NVL(TK6_CONSUMED,'') AS STRING),CAST(NVL(TK6_INITIAL,'') AS STRING),CAST(NVL(TK6_REMAINING,'') AS STRING), CAST(TK6_LIMIT_DATE AS STRING)),
													CONCAT_WS(':',TK7,CAST(NVL(TK7_CONSUMED,'') AS STRING),CAST(NVL(TK7_INITIAL,'') AS STRING),CAST(NVL(TK7_REMAINING,'') AS STRING), CAST(TK7_LIMIT_DATE AS STRING)),
													CONCAT_WS(':',TK8,CAST(NVL(TK8_CONSUMED,'') AS STRING),CAST(NVL(TK8_INITIAL,'') AS STRING),CAST(NVL(TK8_REMAINING,'') AS STRING), CAST(TK8_LIMIT_DATE AS STRING)),
													CONCAT_WS(':',TK9,CAST(NVL(TK9_CONSUMED,'') AS STRING),CAST(NVL(TK9_INITIAL,'') AS STRING),CAST(NVL(TK9_REMAINING,'') AS STRING), CAST(TK9_LIMIT_DATE AS STRING)),
													CONCAT_WS(':',TK10,CAST(NVL(TK10_CONSUMED,'') AS STRING),CAST(NVL(TK10_INITIAL,'') AS STRING),CAST(NVL(TK10_REMAINING,'') AS STRING), CAST(TK10_LIMIT_DATE AS STRING)),
													CONCAT_WS(':',TK11,CAST(NVL(TK11_CONSUMED,'') AS STRING),CAST(NVL(TK11_INITIAL,'') AS STRING),CAST(NVL(TK11_REMAINING,'') AS STRING), CAST(NULL AS STRING))
												)) TK_BONUS AS TK) A
							WHERE CAST(SPLIT(A.TK,':')[1] AS BIGINT) > 0		       
					",
		"tempTable" : "ADME_WALLET_BAL_IMPACT_EDR_TMP",
		"countSourceRecord" : "0"
	  },	 
	  {
		"sql"      : "
						SELECT 
							CAST (FROM_UNIXTIME(UNIX_TIMESTAMP(A.CALL_STA_TIME,'dd/MM/yyyy HH:mm:ss'),'yyyyMM') AS INTEGER) 				 AS MO_KEY ,
							CAST(NULL AS STRING)										 AS UUID,
							CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(A.CALL_STA_TIME,'dd/MM/yyyy HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP) AS EVT_START_DT,
							CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(A.CALL_END_TIME,'dd/MM/yyyy HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP) AS EVT_END_DT,
							CAST(NULL AS TIMESTAMP) 									 AS EVT_BLLG_START_DT,
							A.FTP_FILENAME FILE_NAME,
							CAST(NULL AS TIMESTAMP) 									 AS FILE_DT,
							CAST(NULL AS STRING)										 AS FILE_PREFIX_CD,
							CAST(NULL AS TIMESTAMP) 									 AS REC_PROCESS_DT,
							CAST(NULL AS STRING)										 AS SWITCH_CD,
							B.ACCT_SRVC_INSTANCE_KEY,
							A.CALLING_ISDN_STD											 AS SERVICE_NBR,
							B.ACCT_KEY,
							B.BILLABLE_ACCT_KEY,
							B.CUST_KEY,
							B.CUST_TYP_CD,
							CAST(NULL AS STRING) 										 AS NTWK_QOS_GRP_CD,
							B.ACTIVATION_DT 											 AS ACCT_ACTIVATION_DT,
							B.CBS_ACTIVATION_DT 										 AS ACCT_CBS_ACTIVATION_DT,
							B.LFCYCL_STAT_CD 											 AS ACCT_LFCYCL_STAT_CD,
							CAST(NULL AS TIMESTAMP) 									 AS SRVC_ACTIVATION_DT,
							CAST(NULL AS TIMESTAMP) 									 AS SRVC_CBS_ACTIVATION_DT,
							CAST(NULL AS STRING) 										 AS SRVC_LFCYCL_STAT_CD,
							B.PROD_LINE_KEY,
							B.USAGE_PLAN_KEY,
							B.USAGE_PLAN_CD,
							B.USAGE_PLAN_TYP_CD,
							B.CRM_OFFER_KEY 											 AS OFFER_KEY,
							B.CRM_OFFER_CD 												 AS OFFER_CD,
							CAST(NULL AS DECIMAL(20,0)) 								 AS PROD_KEY,
							CAST(NULL AS STRING) 										 AS PROD_CD,
							CASE WHEN (a.CALL_TYPE_IND = 'OG' AND a.CALL_TYPE IN (1,5,20) AND (a.called_isdn NOT LIKE '041900%'
                               AND a.called_isdn NOT LIKE '1900%'
                               AND a.called_isdn NOT LIKE '041800%'
                               AND a.called_isdn NOT LIKE '1800%')) OR a.CALL_TYPE_IND = 'IC' 											   THEN 'VOICE'							
								 WHEN (A.CALL_TYPE_IND = 'SMO' AND A.CALL_TYPE = 21 AND LENGTH(CALLED_ISDN) >= 10) THEN 'SMS' 
								 WHEN a.CALL_TYPE_IND = 'GPRS' and a.call_type = 189 			   THEN 'DATA'
								 WHEN ((A.CALL_TYPE_IND = 'SMO' AND A.CALL_TYPE = 21 AND LENGTH(A.CALLED_ISDN)<10)
									OR	  (A.CALL_TYPE_IND = 'OG' AND CALL_TYPE = 1 AND ( A.called_isdn LIKE '041900%'
																					OR A.called_isdn LIKE '1900%'
																					OR A.called_isdn LIKE '041800%'
																					OR A.called_isdn LIKE '1800%'))
									OR 	  (A.CALL_TYPE_IND = 'OTHER' AND A.CALL_TYPE IN (184, 186))
									OR 	  (A.call_type = 415 and not ((A.transaction_description like 'UT%' and cast(SUBSTRING(A.scratch_value,1,LENGTH(scratch_value)-2) as int) > 0)  or A.transaction_description like '%HU%'))
									) THEN 'VAS'
								 WHEN (a.call_type_ind = 'OTHER' AND a.call_type in (176,177)) 
											OR (a.CALL_TYPE = 415 
												and not ((a.transaction_description like 'UT%' and cast(SUBSTRING(a.scratch_value,1,LENGTH(scratch_value)-2) as int) > 0)  or a.transaction_description like '%HU%')) THEN 'SBRP'
								 WHEN ((A.CALL_TYPE IN (164,464,360) and A.call_type_ind IN ('UNKNOWN','OTHER'))
													OR (A.CALL_TYPE = 415 AND A.CALL_TYPE_IND = 'OTHER' AND ((A.TRANSACTION_DESCRIPTION LIKE 'UT%' AND SPLIT(A.SCRATCH_VALUE,'-')[0] > 0) OR A.TRANSACTION_DESCRIPTION LIKE '%HU%'))
													OR (A.call_type IN (193, 209,179,320) AND A.CALL_TYPE_IND = 'OTHER' and split(A.scratch_value,'-')[0] > 0)) THEN 'RCHRG'												
								 ELSE 'UNKNOWN' END 									 AS EVT_CLASS_CD,
								 
							A.EVT_CTGRY_CD 												 AS EVT_CTGRY_CD,
							A.CALL_TYP_CD 												 AS EVT_TYP_CD,
							
							CASE WHEN (a.CALL_TYPE_IND = 'OG' AND a.CALL_TYPE IN (1,5,20) AND (a.called_isdn NOT LIKE '041900%'
                               AND a.called_isdn NOT LIKE '1900%'
                               AND a.called_isdn NOT LIKE '041800%'
                               AND a.called_isdn NOT LIKE '1800%')) OR a.CALL_TYPE_IND = 'IC' 											   THEN 'VOICE'							
								 WHEN (A.CALL_TYPE_IND = 'SMO' AND A.CALL_TYPE = 21 AND LENGTH(CALLED_ISDN) >= 10) THEN 'SMS' 
								 WHEN a.call_type_ind = 'GPRS' and a.call_type = 189 			   THEN 'DATA'
								 WHEN ((A.CALL_TYPE_IND = 'SMO' AND A.CALL_TYPE = 21 AND LENGTH(A.CALLED_ISDN)<10)
									OR	  (A.CALL_TYPE_IND = 'OG' AND CALL_TYPE = 1 AND ( A.called_isdn LIKE '041900%'
																					OR A.called_isdn LIKE '1900%'
																					OR A.called_isdn LIKE '041800%'
																					OR A.called_isdn LIKE '1800%'))
									OR 	  (A.CALL_TYPE_IND = 'OTHER' AND A.CALL_TYPE IN (184, 186))
									OR 	  (A.call_type = 415 and not ((A.transaction_description like 'UT%' and cast(SUBSTRING(A.scratch_value,1,LENGTH(scratch_value)-2) as int) > 0)  or A.transaction_description like '%HU%'))
									) THEN 'VAS'
								 WHEN (a.call_type_ind = 'OTHER' AND a.call_type in (176,177)) 
											OR (a.CALL_TYPE = 415 
												and not ((a.transaction_description like 'UT%' and cast(SUBSTRING(a.scratch_value,1,LENGTH(scratch_value)-2) as int) > 0)  or a.transaction_description like '%HU%')) THEN 'SBRP'
								WHEN ((A.CALL_TYPE IN (164,464,360) and A.call_type_ind IN ('UNKNOWN','OTHER'))
													OR (A.CALL_TYPE = 415 AND A.CALL_TYPE_IND = 'OTHER' AND ((A.TRANSACTION_DESCRIPTION LIKE 'UT%' AND SPLIT(A.SCRATCH_VALUE,'-')[0] > 0) OR A.TRANSACTION_DESCRIPTION LIKE '%HU%'))
													OR (A.call_type IN (193, 209,179,320) AND A.CALL_TYPE_IND = 'OTHER' and split(A.scratch_value,'-')[0] > 0)) THEN 'RCHRG'
								ELSE 'UNKNOWN' END  									 AS USAGE_TYP_CD,							
							
							A.CALL_CTGRY_CD						 						 AS CALL_CTGRY_CD,
							A.CALL_TYP_CD,
							CAST(NULL AS STRING) 										 AS BILL_GRP_CD,
							CAST(NULL AS BIGINT) 										 AS BILL_CYCLE_KEY,
							CAST(NULL AS INTEGER) 										 AS CHRG_HEAD_KEY,
							CAST(NULL AS STRING) 										 AS CHRG_HEAD_CD,
							CAST(NULL AS STRING) 										 AS BLLG_STAT_RSN_CD,
							CAST(NULL AS STRING) 										 AS EVT_ENTITY_NAME,
							W.WALLET_KEY,
							W.WALLET_CD,
							W.WALLET_TYP_CD,
							W.UOM_CD AS WALLET_UOM_CD,
							CAST(NULL AS INTEGER)																							 AS WALLET_BAL_IMPACT_TYP,
							CAST(CASE WHEN SPLIT(A.TK,':')[1] = '' THEN 0 ELSE SPLIT(A.TK,':')[1] END AS DECIMAL(27,8)) 					 AS WALLET_BAL_IMPACT_AMT,
							CAST(CASE WHEN SPLIT(A.TK,':')[2] = '' THEN 0 ELSE SPLIT(A.TK,':')[2] END AS DECIMAL(27,8)) 					 AS WALLET_BAL_VALUE_BEFR_IMPACT,
							CAST(CASE WHEN SPLIT(A.TK,':')[3] = '' THEN 0 ELSE SPLIT(A.TK,':')[3] END AS DECIMAL(27,8)) 					 AS WALLET_BAL_VALUE_AFTR_IMPACT,							
							CAST(NULL AS TIMESTAMP)																							 AS WALLET_VLD_TO_DT_BEFOR_IMPACT,
							CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(SPLIT(A.TK,':')[4],'dd/MM/yyyy HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP) AS WALLET_VLD_TO_DT_AFTR_IMPACT,
							CAST(A.CALL_TYPE AS STRING)										 AS WALLET_IMPACT_RSN_CD,
							CAST(NULL AS STRING)										 	 AS PROMO_INFO,
							cast(C.ADDR_LAT as DECIMAL(27,8)) as ADDR_LAT,
							cast(C.ADDR_LON as DECIMAL(27,8)) as ADDR_LON,
							C.CELL_KEY,
							C.CELL_CD,
							C.CELL_SITE_KEY,
							C.CELL_SITE_CD,
							C.NTWK_MGNT_CENTRE_KEY,
							C.NTWK_MGNT_CENTRE_CD,
							C.BSNS_RGN_KEY,
							C.BSNS_RGN_CD,
							C.BSNS_CLUSTER_KEY,
							C.BSNS_CLUSTER_CD,
							C.BSNS_MINICLUSTER_KEY,
							C.BSNS_MINICLUSTER_CD,
							C.GEO_CNTRY_KEY,
							C.GEO_CNTRY_CD,
							C.GEO_STATE_KEY,
							C.GEO_STATE_CD,
							C.GEO_DSTRCT_KEY,
							C.GEO_DSTRCT_CD,
							C.GEO_CITY_KEY,
							C.GEO_CITY_CD,
							B.ACQSTN_DT,
							B.ACQSTN_BSNS_OUTLET_KEY,
							B.ACQSTN_BSNS_OUTLET_CD,
							B.LOYALTY_RANK_SCORE,
							CAST(NULL AS TIMESTAMP)										 AS LOYALTY_SCORE_DT,
							B.CREDIT_SCORE,
							B.CREDIT_CLASS_CD,
							B.CREDIT_SCORE_METHOD,
							B.CREDIT_SCORE_DT,
							B.RISK_IND,
							CAST(NULL AS STRING)										 AS ACCT_SEGMENT_CD,
							CAST(NULL AS STRING)										 AS ACCT_SEGMENT_DT,
							CAST(NULL AS STRING)										 AS CMPGN_CD,
							CAST(6 AS BIGINT) AS SRC_SYS_KEY,
							'ICC' AS SRC_SYS_CD,
							CURRENT_TIMESTAMP() AS LOAD_DT,
							'1' CURR_IND,
							1 WRHS_ID
						FROM ADME_WALLET_BAL_IMPACT_EDR_TMP A
						LEFT JOIN MBF_BIGDATA.ADMR_ACCOUNT_SERVICE B ON A.CALLING_ISDN_STD = B.SERVICE_NBR AND B.DAY_KEY = '$day_key'
						LEFT JOIN MBF_BIGDATA.ADMR_OFFER F ON F.DAY_KEY = '$day_key' AND UPPER(F.OFFER_CD) = UPPER(A.TRANSACTION_DESCRIPTION) AND A.CALL_TYPE = 415
						LEFT JOIN MBF_BIGDATA.ADMR_CELL C ON A.CELL_ID = C.CELL_GCI_CD AND C.DAY_KEY = '$day_key'
						LEFT JOIN (SELECT * FROM MBF_BIGDATA.ADMR_WALLET WHERE DAY_KEY = '$day_key') W
						ON LOWER(W.WALLET_CBS_CD) = LOWER(SPLIT(A.TK,':')[0]) 
						AND ( 
								(
									A.CALL_TYPE_IND IN ('OTHER', 'UNKNOWN')  and w.ROW_NUM1 = 1) 
									OR (w.usage_typ_cd in ('VOICE' ,'CONVERGENT') and a.call_type_ind in ('IC', 'OG') and w.ROW_NUM = 1)
									OR (w.usage_typ_cd in ('SMS' ,'CONVERGENT') and a.call_type_ind = 'SMO' and w.ROW_NUM = 1) 
									OR (w.usage_typ_cd in ('DATA' ,'CONVERGENT') and a.call_type_ind = 'GPRS' and w.ROW_NUM = 1) 
								)				
					",
		"tempTable" : "",
		"countSourceRecord" : "0"
	  }
	]
}