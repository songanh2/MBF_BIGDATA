{
	 "description":"ADME_WALLET_BONUS_EDR",
	 "sqlStatements": [
	 {
		"sql"      	:"
						SELECT 
						CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(B.CALL_STA_TIME,'dd/MM/yyyy HH:mm:ss'),'yyyyMM') AS INTEGER) 		AS MO_KEY,
						CAST(NULL AS STRING) 									AS UUID,
						CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(B.CALL_STA_TIME,'dd/MM/yyyy HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') AS timestamp) AS BONUS_TXN_DT,
						CAST(NULL AS TIMESTAMP) 								AS BLLG_START_DT,
						B.FTP_FILENAME 											AS FILE_NAME,
						CAST(NULL AS TIMESTAMP) 								AS FILE_DT,
						CAST(NULL AS STRING) 									AS FILE_PREFIX_CD,
						CAST(NULL AS TIMESTAMP) 								AS REC_PROCESS_DT,
						CAST(NULL AS STRING) 									AS SWITCH_CD,
						C.ACCT_SRVC_INSTANCE_KEY 								AS ACCT_SRVC_INSTANCE_KEY,
						CASE
								 WHEN LENGTH(NVL(B.ORG_CALL_ID,'')) <> 0
									THEN
									 CASE
										 WHEN SUBSTR(B.ORG_CALL_ID, 1, 4) = '0001'
										 THEN
											 SUBSTR(B.ORG_CALL_ID, 5)
										 ELSE
											 CAST(CAST(B.ORG_CALL_ID AS INT) AS STRING)
									 END
								 WHEN B.EVT_DRCTN_CD = 'I' THEN B.CALLED_ISDN_STD 
								 ELSE  B.CALLING_ISDN_STD
						END 													AS SERVICE_NBR,
						C.ACCT_KEY 												AS ACCT_KEY,
						C.BILLABLE_ACCT_KEY 									AS BILLABLE_ACCT_KEY,
						C.CUST_KEY 												AS CUST_KEY,
						C.CUST_TYP_CD 											AS CUST_TYP_CD,
						CAST(NULL AS STRING) 									AS NTWK_QOS_GRP_CD,
						C.ACTIVATION_DT 										AS ACCT_ACTIVATION_DT,
						C.CBS_ACTIVATION_DT 									AS ACCT_CBS_ACTIVATION_DT,
						C.LFCYCL_STAT_CD 										AS ACCT_LFCYCL_STAT_CD,
						CAST(NULL AS TIMESTAMP) 								AS SRVC_ACTIVATION_DT,
						CAST(NULL AS TIMESTAMP)  								AS SRVC_CBS_ACTIVATION_DT,
						CAST(NULL AS string) 									AS SRVC_LFCYCL_STAT_CD,
						C.PROD_LINE_KEY 										AS PROD_LINE_KEY,
						C.USAGE_PLAN_KEY 										AS USAGE_PLAN_KEY,
						C.USAGE_PLAN_CD 										AS USAGE_PLAN_CD,
						C.USAGE_PLAN_TYP_CD 									AS USAGE_PLAN_TYP_CD,
						C.CRM_OFFER_KEY 										AS OFFER_KEY,
						C.CRM_OFFER_CD 											AS OFFER_CD,
						CAST(NULL AS decimal(20,0)) 							AS PROD_KEY,
						CAST(NULL AS STRING) 									AS PROD_CD,
						CASE WHEN (B.CALL_TYPE_IND = 'OG' AND B.CALL_TYPE IN (1,5,20) AND (B.called_isdn NOT LIKE '041900%'
                               AND B.called_isdn NOT LIKE '1900%'
                               AND B.called_isdn NOT LIKE '041800%'
                               AND B.called_isdn NOT LIKE '1800%')) OR B.CALL_TYPE_IND = 'IC' 											   THEN 'VOICE'							
								 WHEN (B.CALL_TYPE_IND = 'SMO' AND B.CALL_TYPE = 21 AND LENGTH(CALLED_ISDN) >= 10) THEN 'SMS' 
								 WHEN B.call_type_ind = 'GPRS' and B.call_type = 189 			   THEN 'DATA'
								 WHEN ((B.CALL_TYPE_IND = 'SMO' AND B.CALL_TYPE = 21 AND LENGTH(B.CALLED_ISDN)<10)
									 OR	  (B.CALL_TYPE_IND = 'OG' AND CALL_TYPE = 1 AND ( B.called_isdn LIKE '041900%'
															OR B.called_isdn LIKE '1900%'
															OR B.called_isdn LIKE '041800%'
															OR B.called_isdn LIKE '1800%'))
									 OR 	  (B.CALL_TYPE_IND = 'OTHER' AND B.CALL_TYPE IN (184, 186))
									 OR 	  (B.call_type = 415 and not ((B.transaction_description like 'UT%' and cast(SUBSTRING(B.scratch_value,1,LENGTH(scratch_value)-2) as int) > 0)  or B.transaction_description like '%HU%'))
								) THEN 'VAS'
								 WHEN (B.call_type_ind = 'OTHER' AND B.call_type in (176,177)) 
									OR (B.CALL_TYPE = 415 
									and not ((B.transaction_description like 'UT%' and cast(SUBSTRING(B.scratch_value,1,LENGTH(scratch_value)-2) as int) > 0)  or B.transaction_description like '%HU%')) THEN 'SBRP'
								 ELSE 'UNKNOWN' END 									 AS EVT_CLASS_CD,
						B.EVT_CTGRY_CD											AS EVT_CTGRY_CD,
						B.CALL_TYP_CD											AS EVT_TYP_CD,
						CASE WHEN (B.CALL_TYPE_IND = 'OG' AND B.CALL_TYPE IN (1,5,20) AND (B.called_isdn NOT LIKE '041900%'
                               AND B.called_isdn NOT LIKE '1900%'
                               AND B.called_isdn NOT LIKE '041800%'
                               AND B.called_isdn NOT LIKE '1800%')) OR B.CALL_TYPE_IND = 'IC' 											   THEN 'VOICE'							
								 WHEN (B.CALL_TYPE_IND = 'SMO' AND B.CALL_TYPE = 21 AND LENGTH(CALLED_ISDN) >= 10) THEN 'SMS' 
								 WHEN B.call_type_ind = 'GPRS' and B.call_type = 189 			   THEN 'DATA'
								 WHEN ((B.CALL_TYPE_IND = 'SMO' AND B.CALL_TYPE = 21 AND LENGTH(B.CALLED_ISDN)<10)
									 OR	  (B.CALL_TYPE_IND = 'OG' AND CALL_TYPE = 1 AND ( B.called_isdn LIKE '041900%'
															OR B.called_isdn LIKE '1900%'
															OR B.called_isdn LIKE '041800%'
															OR B.called_isdn LIKE '1800%'))
									 OR 	  (B.CALL_TYPE_IND = 'OTHER' AND B.CALL_TYPE IN (184, 186))
									 OR 	  (B.call_type = 415 and not ((B.transaction_description like 'UT%' and cast(SUBSTRING(B.scratch_value,1,LENGTH(scratch_value)-2) as int) > 0)  or B.transaction_description like '%HU%'))
								) THEN 'VAS'
								 WHEN (B.call_type_ind = 'OTHER' AND B.call_type in (176,177)) 
									OR (B.CALL_TYPE = 415 
									and not ((B.transaction_description like 'UT%' and cast(SUBSTRING(B.scratch_value,1,LENGTH(scratch_value)-2) as int) > 0)  or B.transaction_description like '%HU%')) THEN 'SBRP'
								 ELSE 'UNKNOWN' END						        AS USAGE_TYP_CD,
						CAST(NULL AS STRING) 									AS BILL_GRP_CD,
						CAST(NULL AS BIGINT) 									AS BILL_CYCLE_KEY,
						CAST(NULL AS integer) 									AS CHRG_HEAD_KEY,
						CAST(NULL AS STRING)									AS CHRG_HEAD_CD,
						CAST(NULL AS STRING)			 						AS BLLG_STAT_RSN_CD,
						W.WALLET_KEY 											AS WALLET_KEY,
						W.WALLET_CD 											AS WALLET_CD,
						W.WALLET_TYP_CD 										AS WALLET_TYPE_CD,
						W.UOM_CD 												AS WALLET_UOM_CD,
						CAST(NULL AS INTEGER) 									AS WALLET_BONUS_TYP,
						cast(CASE WHEN split(B.tk,':')[1] <> '' THEN split(B.tk,':')[1] ELSE '0' END as decimal(27,8)) 				as WALLET_BONUS_AMT,
						cast(CASE WHEN split(B.tk,':')[2] <> '' THEN split(B.tk,':')[2] ELSE '0' END as decimal(27,8)) 				as WALLET_BAL_VALUE_BEFR_IMPACT,
						cast(CASE WHEN split(B.tk,':')[3] <> '' THEN split(B.tk,':')[3] ELSE '0' END as decimal(27,8)) 				as WALLET_BAL_VALUE_AFTR_IMPACT,
						CAST(NULL AS TIMESTAMP) 								AS WALLET_VLD_TO_DT_BEFOR_IMPACT,
						CAST(NULL AS TIMESTAMP) 								AS WALLET_VLD_TO_DT_AFTR_IMPACT,
						CAST(NULL AS STRING) 									AS  WALLET_BONUS_RSN_CD,
						CAST(NULL AS STRING) 									AS PROMO_INFO,
						cast(D.ADDR_LAT as decimal(27,8))						AS ADDR_LAT,
						cast(D.ADDR_LON as decimal(27,8))						AS ADDR_LON,
						D.CELL_KEY												AS CELL_KEY,
						D.CELL_CD 												AS CELL_CD,
						D.CELL_SITE_KEY 										AS CELL_SITE_KEY,
						D.CELL_SITE_CD 											AS CELL_SITE_CD,
						D.NTWK_MGNT_CENTRE_KEY 									AS NTWK_MGNT_CENTRE_KEY,
						D.NTWK_MGNT_CENTRE_CD 									AS NTWK_MGNT_CENTRE_CD,
						D.BSNS_RGN_KEY 											AS BSNS_RGN_KEY,
						D.BSNS_RGN_CD 											AS BSNS_RGN_CD,
						D.BSNS_CLUSTER_KEY 										AS BSNS_CLUSTER_KEY,
						D.BSNS_CLUSTER_CD 										AS BSNS_CLUSTER_CD,
						D.BSNS_MINICLUSTER_KEY 									AS BSNS_MINICLUSTER_KEY,
						D.BSNS_MINICLUSTER_CD 									AS BSNS_MINICLUSTER_CD,
						D.GEO_CNTRY_KEY 										AS GEO_CNTRY_KEY,
						D.GEO_CNTRY_CD 											AS GEO_CNTRY_CD,
						D.GEO_STATE_KEY 										AS GEO_STATE_KEY,
						D.GEO_STATE_CD 											AS GEO_STATE_CD,
						D.GEO_DSTRCT_KEY 										AS GEO_DSTRCT_KEY,
						D.GEO_DSTRCT_CD 										AS GEO_DSTRCT_CD,
						D.GEO_CITY_KEY 											AS GEO_CITY_KEY,
						D.GEO_CITY_CD 											AS GEO_CITY_CD,
						C.ACQSTN_DT 											AS ACQSTN_DT,
						C.ACQSTN_BSNS_OUTLET_KEY 								AS ACQSTN_BSNS_OUTLET_KEY,
						C.ACQSTN_BSNS_OUTLET_CD 								AS ACQSTN_BSNS_OUTLET_CD,
						C.LOYALTY_RANK_SCORE 									AS LOYALTY_RANK_SCORE,
						CAST(NULL AS TIMESTAMP) 								AS LOYALTY_SCORE_DT,
						C.CREDIT_SCORE 											AS CREDIT_SCORE,
						C.CREDIT_CLASS_CD 										AS CREDIT_CLASS_CD,
						C.CREDIT_SCORE_METHOD 									AS CREDIT_SCORE_METHOD,
						C.CREDIT_SCORE_DT 										AS CREDIT_SCORE_DT,
						C.RISK_IND 												AS RISK_IND,
						CAST(NULL AS STRING) 									AS ACCT_SEGMENT_CD,
						CAST(NULL AS STRING) 									AS ACCT_SEGMENT_DT,
						CAST(NULL AS STRING) 									AS CMPGN_CD,
						6 														AS SRC_SYS_KEY,
						'ICC' 													AS SRC_SYS_CD,
						CURRENT_TIMESTAMP() 									AS LOAD_DT,
						'1' 													AS CURR_IND,
						1 														AS WRHS_ID 
					FROM
					(
					select  transaction_description,scratch_value,ORG_CALL_ID,EVT_CTGRY_CD,EVT_DRCTN_CD,
						call_sta_time,ftp_filename,CALLING_ISDN_STD,CALLED_ISDN_STD,called_isdn, CELL_ID,CALL_TYPE, CALL_TYP_CD,call_type_ind, tk_bonus.tk tk
					from MBF_STAGE.STG_CDR_IN_ENRICH a
					lateral view 
					explode(array(
						concat_ws(':',NVL(tk1,''),cast(NVL(tk1_consumed,'') as string),cast(NVL(tk1_initial,'') as string),cast(NVL(tk1_remaining,'') as string)),
						concat_ws(':',NVL(tk2,''),cast(NVL(tk2_consumed,'') as string),cast(NVL(tk2_initial,'') as string),cast(NVL(tk2_remaining,'') as string)),
						concat_ws(':',NVL(tk3,''),cast(NVL(tk3_consumed,'') as string),cast(NVL(tk3_initial,'') as string),cast(NVL(tk3_remaining,'') as string)),
						concat_ws(':',NVL(tk4,''),cast(NVL(tk4_consumed,'') as string),cast(NVL(tk4_initial,'') as string),cast(NVL(tk4_remaining,'') as string)),
						concat_ws(':',NVL(tk5,''),cast(NVL(tk5_consumed,'') as string),cast(NVL(tk5_initial,'') as string),cast(NVL(tk5_remaining,'') as string)),
						concat_ws(':',NVL(tk6,''),cast(NVL(tk6_consumed,'') as string),cast(NVL(tk6_initial,'') as string),cast(NVL(tk6_remaining,'') as string)),
						concat_ws(':',NVL(tk7,''),cast(NVL(tk7_consumed,'') as string),cast(NVL(tk7_initial,'') as string),cast(NVL(tk7_remaining,'') as string)),
						concat_ws(':',NVL(tk8,''),cast(NVL(tk8_consumed,'') as string),cast(NVL(tk8_initial,'') as string),cast(NVL(tk8_remaining,'') as string)),
						concat_ws(':',NVL(tk9,''),cast(NVL(tk9_consumed,'') as string),cast(NVL(tk9_initial,'') as string),cast(NVL(tk9_remaining,'') as string)),
						concat_ws(':',NVL(tk10,''),cast(NVL(tk10_consumed,'') as string),cast(NVL(tk10_initial,'') as string),cast(NVL(tk10_remaining,'') as string))
					)) tk_bonus as tk where a.day_key = '$day_key' and a.call_type_ind in ('OG','IC','SMO','GPRS') ) B
					
					LEFT JOIN MBF_BIGDATA.ADMR_ACCOUNT_SERVICE C ON C.SERVICE_NBR = CASE
																						WHEN LENGTH(NVL(B.ORG_CALL_ID,'')) <> 0
																							THEN
																							CASE
																								WHEN SUBSTR(B.ORG_CALL_ID, 1, 4) = '0001'
																								THEN
																									SUBSTR(B.ORG_CALL_ID, 5)
																								ELSE
																									CAST(CAST(B.ORG_CALL_ID AS INT) AS STRING)
																							END
																						 WHEN B.EVT_DRCTN_CD = 'I' THEN B.CALLED_ISDN_STD 
																						 ELSE  B.CALLING_ISDN_STD
																				END   AND C.DAY_KEY = '$day_key'
					JOIN MBF_BIGDATA.ADMR_CELL D ON D.CELL_GCI_CD = B.CELL_ID AND D.DAY_KEY = '$day_key'
					join mbf_bigdata.admr_wallet w on w.wallet_cbs_cd = lower(split(B.tk,':')[0]) and W.WALLET_TYP_CD = 'SECONDARY_MAIN_BALANCE'
					and ((b.call_type_ind in ('OTHER', 'UNKNOWN')  and w.ROW_NUM1 = 1) 
						  OR (w.usage_typ_cd in ('VOICE' ,'CONVERGENT') and b.call_type_ind in ('IC', 'OG') and w.ROW_NUM = 1)
						  OR (w.usage_typ_cd in ('SMS' ,'CONVERGENT') and b.call_type_ind = 'SMO' and w.ROW_NUM = 1) 
						  OR (w.usage_typ_cd in ('DATA' ,'CONVERGENT') and b.call_type_ind = 'GPRS' and w.ROW_NUM = 1) 
					)
					and w.day_key = '$day_key'	
					WHERE cast(split(B.tk,':')[1] as bigint) > 0 
					 ",
		"tempTable"  : "ADME_WALLET_BONUS_EDR_TMP",
		"countSourceRecord":"1"
	  }   	  
	]
}