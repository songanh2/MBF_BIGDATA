{
	 "description":"ADME_PLAN_CHANGE_EDR",
	 "sqlStatements": [
	 {
		"sql"      	:"

					SELECT 
					CAST(FROM_UNIXTIME(UNIX_TIMESTAMP('$day_key','yyyyMMdd'),'yyyyMM') AS INTEGER) AS	MO_KEY,
					A.ACCT_SRVC_INSTANCE_KEY,
					A.SERVICE_NBR,
					A.ACCT_KEY,
					A.BILLABLE_ACCT_KEY,
					A.CUST_KEY,
					A.CUST_TYP_CD,
					A.NTWK_QOS_GRP_CD,
					A.ACTIVATION_DT,
					A.CBS_ACTIVATION_DT,
					A.LFCYCL_STAT_CD,
					A.ACQSTN_DT,
					A.ACQSTN_BSNS_OUTLET_KEY,
					A.ACQSTN_BSNS_OUTLET_CD,
					A.PROD_LINE_KEY,
					B.PROD_LINE_KEY as PREV_PROD_LINE_KEY,
					cast(FROM_UNIXTIME(UNIX_TIMESTAMP(A.DAY_KEY,'yyyyMMdd')) as timestamp) as PROD_LINE_CHANGE_DT,
					A.USAGE_PLAN_KEY,
					A.USAGE_PLAN_CD,
					A.USAGE_PLAN_TYP_CD,
					A.PREV_USAGE_PLAN_KEY,
					B.USAGE_PLAN_CD as PREV_USAGE_PLAN_CD,
					B.USAGE_PLAN_TYP_CD as PREV_USAGE_PLAN_TYP_CD,
					A.USAGE_PLAN_CHANGE_DT,
					A.CRM_OFFER_KEY,
					A.CRM_OFFER_CD,
					B.CRM_OFFER_KEY as PREV_CRM_OFFER_KEY,
					B.CRM_OFFER_CD as PREV_CRM_OFFER_CD,
					cast(FROM_UNIXTIME(UNIX_TIMESTAMP(A.DAY_KEY,'yyyyMMdd')) as timestamp) as CRM_OFFER_CHANGE_DT,
					cast(null as bigint) as CELL_KEY,
					cast(null as string) as CELL_CD,
					cast(null as bigint) as CELL_SITE_KEY,
					cast(null as string) as CELL_SITE_CD,
					cast(null as bigint) as NTWK_MGNT_CENTRE_KEY,
					cast(null as string) as NTWK_MGNT_CENTRE_CD,
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
					A.LOYALTY_RANK_SCORE,
					A.LOYALTY_SCORE_DT,
					A.CREDIT_SCORE,
					A.CREDIT_CLASS_CD,
					A.CREDIT_SCORE_METHOD,
					A.CREDIT_SCORE_DT,
					A.RISK_IND,
					cast(null as string) as ACCT_SEGMENT_CD,
					cast(null as string) as ACCT_SEGMENT_DT,
					cast(null as string) as CMPGN_CD,
					cast(7 as bigint) as SRC_SYS_KEY,
					A.SRC_SYS_CD,
					CURRENT_TIMESTAMP() AS LOAD_DT,
					'1' AS CURR_IND,
					1 AS WRHS_ID
					FROM 
							  MBF_BIGDATA.ADMR_ACCOUNT_SERVICE A 
					LEFT JOIN MBF_BIGDATA.ADMR_ACCOUNT_SERVICE B 	ON A.ACCT_SRVC_INSTANCE_KEY = B.ACCT_SRVC_INSTANCE_KEY 
																	AND B.DAY_KEY = DATE_FORMAT(DATE_ADD(FROM_UNIXTIME(UNIX_TIMESTAMP('$day_key','yyyyMMdd')),-1),'yyyyMMdd')
					WHERE 
					((B.CRM_OFFER_CD IS NOT NULL  AND A.CRM_OFFER_CD <> B.CRM_OFFER_CD) 
					OR (B.PROD_LINE_KEY IS NOT NULL AND A.PROD_LINE_KEY <> B.PROD_LINE_KEY))
					AND A.DAY_KEY = '$day_key'

				 ",
		"tempTable"  : "ADME_PLAN_CHANGE_EDR_TMP",
		"countSourceRecord":"1"
	  }   	  
	]
}