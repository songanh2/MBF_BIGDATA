{
	 "description":"etl_adme_wallet_bal_forfeit_edr",
	 "sqlStatements": [
      {
		"sql"      	:"
					SELECT 
						CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(cast(dbh.DUMP_DATETIME as string),'dd/MM/yyyy HH:mm:ss'),'yyyyMM') as INTEGER) as MO_KEY,
						CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(cast(dbh.DUMP_DATETIME as string),'dd/MM/yyyy HH:mm:ss'),'HH') as INTEGER) as HOUR_KEY,
						CAST(NULL as STRING) as UUID,
						CAST(NULL as TIMESTAMP) as EVT_DT,
						CAST(NULL as TIMESTAMP) as EVT_BLLG_START_DT,
						CAST(dbh.ftp_filename as STRING) as FILE_NAME,
						CAST(NULL as TIMESTAMP) as FILE_DT,
						CAST(NULL as STRING) as FILE_PREFIX_CD,
						CAST(NULL as TIMESTAMP) as REC_PROCESS_DT,
						CAST(NULL as STRING) as SWITCH_CD,
						CAST(aas.acct_srvc_instance_key as DECIMAL(27,0)) as ACCT_SRVC_INSTANCE_KEY,
						CAST(dbh.msisdn as STRING) as SERVICE_NBR,
						CAST(aas.acct_key as DECIMAL(27,0)) as ACCT_KEY,
						CAST(aas.billable_acct_key as DECIMAL(27,0)) as BILLABLE_ACCT_KEY,
						CAST(aas.cust_key as DECIMAL(27,0)) as CUST_KEY,
						CAST(aas.cust_typ_cd as STRING) as CUST_TYP_CD,
						CAST(aas.ntwk_qos_grp_cd as STRING) as NTWK_QOS_GRP_CD,
						CAST(NULL as TIMESTAMP) as ACCT_ACTIVATION_DT,
						CAST(NULL as TIMESTAMP) as ACCT_CBS_ACTIVATION_DT,
						CAST(aas.lfcycl_stat_cd as STRING) as ACCT_LFCYCL_STAT_CD,
						CAST(aas.activation_dt as TIMESTAMP) as SRVC_ACTIVATION_DT,
						CAST(aas.cbs_activation_dt as TIMESTAMP) as SRVC_CBS_ACTIVATION_DT,
						CAST(aas.lfcycl_stat_cd as STRING) as SRVC_LFCYCL_STAT_CD,
						CAST(aas.prod_line_key as INTEGER) as PROD_LINE_KEY,
						CAST(aas.usage_plan_key as BIGINT) as USAGE_PLAN_KEY,
						CAST(aas.usage_plan_cd as STRING) as USAGE_PLAN_CD,
						CAST(aas.usage_plan_typ_cd as STRING) as USAGE_PLAN_TYP_CD,
						CAST(aas.crm_offer_key as BIGINT) as OFFER_KEY,
						CAST(aas.crm_offer_cd as STRING) as OFFER_CD,
						CAST(NULL as DECIMAL(20,0)) as PROD_KEY,
						CAST(NULL as STRING) as PROD_CD,
						CAST(NULL as STRING) as EVT_CLASS_CD,
						CAST(NULL as STRING) as EVT_CTGRY_CD,
						CAST(NULL as STRING) as EVT_TYP_CD,
						CAST(NULL as STRING) as USAGE_TYP_CD,
						CAST(NULL as STRING) as BILL_GRP_CD,
						CAST(NULL as BIGINT) as BILL_CYCLE_KEY,
						CAST(NULL as INTEGER) as CHRG_HEAD_KEY,
						CAST(NULL as STRING) as CHRG_HEAD_CD,
						CAST(NULL as STRING) as BLLG_STAT_RSN_CD,
						CAST(aw.wallet_key as BIGINT) as WALLET_KEY,
						CAST(aw.wallet_cd as STRING) as WALLET_CD,
						CAST(aw.wallet_typ_cd as STRING) as WALLET_TYP_CD,
						CAST(aw.uom_cd as STRING) as WALLET_UOM_CD,
						CAST(2 as INTEGER) as WALLET_BAL_FORFEIT_TYP,
						CAST(dbh.wallet_amt as DECIMAL(27,8)) as WALLET_BAL_FORFEIT_AMT,
						CAST(NULL as DECIMAL(27,8)) as WALLET_BAL_VALUE_BEFR_IMPACT,
						CAST(NULL as DECIMAL(27,8)) as WALLET_BAL_VALUE_AFTR_IMPACT,
						CAST(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(cast(dbh.WALLET_DT as string),'dd/MM/yyyy'))) as TIMESTAMP) as WALLET_VLD_TO_DT_BEFOR_IMPACT,
						CAST(NULL as TIMESTAMP) as WALLET_VLD_TO_DT_AFTR_IMPACT,
						CAST(NULL as STRING) as WALLET_FORFEIT_RSN_CD,
						CAST(NULL as DECIMAL(27,8)) as ADDR_LAT,
						CAST(NULL as DECIMAL(27,8)) as ADDR_LON,
						CAST(NULL as BIGINT) as CELL_KEY,
						CAST(NULL as STRING) as CELL_CD,
						CAST(NULL as BIGINT) as CELL_SITE_KEY,
						CAST(NULL as STRING) as CELL_SITE_CD,
						CAST(NULL as BIGINT) as NTWK_MGNT_CENTRE_KEY,
						CAST(NULL as STRING) as NTWK_MGNT_CENTRE_CD,
						CAST(aas.bsns_rgn_key as BIGINT) as BSNS_RGN_KEY,
						CAST(aas.bsns_rgn_cd as STRING) as BSNS_RGN_CD,
						CAST(aas.bsns_cluster_key as BIGINT) as BSNS_CLUSTER_KEY,
						CAST(aas.bsns_cluster_cd as STRING) as BSNS_CLUSTER_CD,
						CAST(aas.bsns_minicluster_key as BIGINT) as BSNS_MINICLUSTER_KEY,
						CAST(aas.bsns_minicluster_cd as STRING) as BSNS_MINICLUSTER_CD,
						CAST(504003 as BIGINT) as GEO_CNTRY_KEY,
						CAST('VN' as STRING) as GEO_CNTRY_CD,
						CAST(aas.geo_state_key as BIGINT) as GEO_STATE_KEY,
						CAST(aas.geo_state_cd as STRING) as GEO_STATE_CD,
						CAST(aas.geo_dstrct_key as BIGINT) as GEO_DSTRCT_KEY,
						CAST(aas.geo_dstrct_cd as STRING) as GEO_DSTRCT_CD,
						CAST(aas.geo_city_key as BIGINT) as GEO_CITY_KEY,
						CAST(aas.geo_city_cd as STRING) as GEO_CITY_CD,
						CAST(NULL as TIMESTAMP) as ACQSTN_DT,
						CAST(aas.acqstn_bsns_outlet_key as BIGINT) as ACQSTN_BSNS_OUTLET_KEY,
						CAST(aas.acqstn_bsns_outlet_cd as STRING) as ACQSTN_BSNS_OUTLET_CD,
						CAST(aas.loyalty_rank_score as DECIMAL(12,4)) as LOYALTY_RANK_SCORE,
						CAST(aas.loyalty_score_dt as TIMESTAMP) as LOYALTY_SCORE_DT,
						CAST(NULL as DECIMAL(12,4)) as CREDIT_SCORE,
						CAST(NULL as STRING) as CREDIT_CLASS_CD,
						CAST(NULL as STRING) as CREDIT_SCORE_METHOD,
						CAST(NULL as TIMESTAMP) as CREDIT_SCORE_DT,
						CAST(NULL as INTEGER) as RISK_IND,
						CAST(NULL as STRING) as ACCT_SEGMENT_CD,
						CAST(NULL as STRING) as ACCT_SEGMENT_DT,
						CAST(NULL as STRING) as CMPGN_CD,
						CAST(7 as BIGINT) as SRC_SYS_KEY,
						CAST(dbh.src_sys_cd as STRING) as SRC_SYS_CD,
						CAST(CURRENT_TIMESTAMP() as TIMESTAMP) as LOAD_DT,
						CAST('1' as STRING) as CURR_IND,
						CAST(1 as INTEGER) as WRHS_ID
					FROM
						(
							SELECT
								WALLET_CD,
								CASE WHEN VALUE IS NULL THEN 0
									ELSE SPLIT(VALUE, ';')[0]
								END	AS WALLET_AMT,
								CASE WHEN VALUE IS NULL THEN NULL
									ELSE SPLIT(VALUE, ';')[1]
								END	AS WALLET_DT,
								y.*
							FROM 
							(
								SELECT 
							    	x.MSISDN,
							    	x.PROFILE_ID,
							    	x.DUMP_DATETIME,
									x.FTP_FILENAME,
									x.DAY_KEY,
									x.SRC_SYS_CD,
							    	MAP
							    		(
							    			'KM1', KM1 || ';' || KM1_STRING,
											'KM2', KM2 || ';' || KM2_STRING,
											'KMDK1', KMDK1 || ';' || KMDK1_STRING,
											'KMDK2', KMDK2 || ';' || KMDK2_STRING,
											'BONUS2', BONUS2 || ';' || BONUS2_STRING,
											'BONUS', BONUS || ';' || BONUS_STRING,
											'VOICE_KM', VOICE_KM || ';' || VOICE_KM_STRING,
											'VOICE2', VOICE2 || ';' || VOICE2_STRING,
											'VOICE', VOICE || ';' || VOICE_STRING,
											'VOICE_OP', VOICE_OP || ';' || VOICE_OP_STRING,
											'SMS_KM', SMS_KM || ';' || SMS_KM_STRING,
											'SMS2', SMS2 || ';' || SMS2_STRING,
											'SMS', SMS || ';' || SMS_STRING,
											'SMS_OP', SMS_OP || ';' || SMS_OP_STRING,
											'SMSREFILL', SMSREFILL || ';' || SMSREFILL_STRING,
											'SUBSC_SMS', SUBSC_SMS || ';' || SUBSC_SMS_STRING,
											'PREODICSMS', PREODICSMS || ';' || PREODICSMS_STRING,
											'GPRS_KM', GPRS_KM || ';' || GPRS_KM_STRING,
											'GPRS2', GPRS2 || ';' || GPRS2_STRING,
											'GPRS', GPRS || ';' || GPRS_STRING,
											'VIDEO', VIDEO || ';' || VIDEO_STRING,
											'VOICE_LM', VOICE_LM || ';' || VOICE_LM_STRING,
											'SMS_LM', SMS_LM || ';' || SMS_LM_STRING,
											'VOICE_PKG', VOICE_PKG || ';' || VOICE_PKG_STRING,
											'SMS_PKG', SMS_PKG || ';' || SMS_PKG_STRING,
											'GPRS4', GPRS4 || ';' || GPRS4_STRING,
											'VOICE_PKG1', VOICE_PKG1 || ';' || VOICE_PKG1_STRING,
											'CREDIT1', CREDIT1 || ';' || CREDIT1_STRING,
											'MGPRS', MGPRS || ';' || MGPRS_STRING,
											'MVNPT_0', MVNPT_0 || ';' || MVNPT_0_STRING,
											'MVNPT1_0', MVNPT1_0 || ';' || MVNPT1_0_STRING,
											'MVOICE_LM', MVOICE_LM || ';' || MVOICE_LM_STRING,
											'MSMS_LM', MSMS_LM || ';' || MSMS_LM_STRING,
											'MVOICE_0', MVOICE_0 || ';' || MVOICE_0_STRING,
											'MSMS', MSMS || ';' || MSMS_STRING,
											'KM2T', KM2T || ';' || KM2T_STRING,
											'KM3T', KM3T || ';' || KM3T_STRING,
											'VOICE_TH', VOICE_TH || ';' || VOICE_TH_STRING,
											'KM1T', KM1T || ';' || KM1T_STRING,
											'GPRS_0', GPRS_0 || ';' || GPRS_0_STRING,
											'SMSVNPT', SMSVNPT || ';' || SMSVNPT_STRING,
											'MSMSVNPT', MSMSVNPT || ';' || MSMSVNPT_STRING,
											'WALLET1', WALLET1 || ';' || WALLET1_STRING,
											'WALLET2', WALLET2 || ';' || WALLET2_STRING,
											'WALLET3', WALLET3 || ';' || WALLET3_STRING,
											'G_VOICE', G_VOICE || ';' || G_VOICE_STRING,
											'G_DATA', G_DATA || ';' || G_DATA_STRING,
											'DATA_KM1', DATA_KM1 || ';' || DATA_KM1_STRING,
											'KM99T', KM99T || ';' || KM99T_STRING,
											'GPRS31', GPRS31 || ';' || GPRS31_STRING,
											'GPRS6', GPRS6 || ';' || GPRS6_STRING,
											'KMKNDL', KMKNDL || ';' || KMKNDL_STRING,
											'VOICE_LM_DL', VOICE_LM_DL || ';' || VOICE_LM_DL_STRING,
											'SMS_LM_DL', SMS_LM_DL || ';' || SMS_LM_DL_STRING,
											'GPRS5', GPRS5 || ';' || GPRS5_STRING,
											'DATAZ1', DATAZ1 || ';' || DATAZ1_STRING,
											'DATAZ2', DATAZ2 || ';' || DATAZ2_STRING,
											'DATAZ3', DATAZ3 || ';' || DATAZ3_STRING,
											'KM4', KM4 || ';' || KM4_STRING,
											'KMDK4', KMDK4 || ';' || KMDK4_STRING,
											'KM4T', KM4T || ';' || KM4T_STRING,
											'THOAI_LM1', THOAI_LM1 || ';' || THOAI_LM1_STRING,
											'TK1', TK1 || ';' || TK1_STRING,
											'KM1V', KM1V || ';' || KM1V_STRING,
											'KM2V', KM2V || ';' || KM2V_STRING,
											'KM3V', KM3V || ';' || KM3V_STRING,
											'KM4V', KM4V || ';' || KM4V_STRING,
											'DATA_LN', DATA_LN || ';' || DATA_LN_STRING,
											'DATA_VC', DATA_VC || ';' || DATA_VC_STRING,
											'VOICE_LMZ', VOICE_LMZ || ';' || VOICE_LMZ_STRING,
											'VORMO_0', VORMO_0 || ';' || VORMO_0_STRING,
											'VORMI_0', VORMI_0 || ';' || VORMI_0_STRING,
											'SMSRM_0', SMSRM_0 || ';' || SMSRM_0_STRING,
											'VOICE_SP1', VOICE_SP1 || ';' || VOICE_SP1_STRING,
											'VOICE_SP2', VOICE_SP2 || ';' || VOICE_SP2_STRING,
											'SMS_SP1', SMS_SP1 || ';' || SMS_SP1_STRING,
											'SMS_SP2', SMS_SP2 || ';' || SMS_SP2_STRING,
											'TK_AP1', TK_AP1 || ';' || TK_AP1_STRING,
											'TK_AP2', TK_AP2 || ';' || TK_AP2_STRING
							    		) AS WALLET_MAP
							    FROM
							    (	
							    	SELECT
					    			*
							    	FROM
							    		(
								    		SELECT 
								    			MSISDN,
												PROFILE_ID,
												KM1,
												KM1_STRING,
												KM2,
												KM2_STRING,
												KMDK1,
												KMDK1_STRING,
												KMDK2,
												KMDK2_STRING,
												BONUS2,
												BONUS2_STRING,
												BONUS,
												BONUS_STRING,
												VOICE_KM,
												VOICE_KM_STRING,
												VOICE2,
												VOICE2_STRING,
												VOICE,
												VOICE_STRING,
												VOICE_OP,
												VOICE_OP_STRING,
												SMS_KM,
												SMS_KM_STRING,
												SMS2,
												SMS2_STRING,
												SMS,
												SMS_STRING,
												SMS_OP,
												SMS_OP_STRING,
												SMSREFILL,
												SMSREFILL_STRING,
												SUBSC_SMS,
												SUBSC_SMS_STRING,
												PREODICSMS,
												PREODICSMS_STRING,
												GPRS_KM,
												GPRS_KM_STRING,
												GPRS2,
												GPRS2_STRING,
												GPRS,
												GPRS_STRING,
												VIDEO,
												VIDEO_STRING,
												VOICE_LM,
												VOICE_LM_STRING,
												SMS_LM,
												SMS_LM_STRING,
												VOICE_PKG,
												VOICE_PKG_STRING,
												SMS_PKG,
												SMS_PKG_STRING,
												GPRS4,
												GPRS4_STRING,
												VOICE_PKG1,
												VOICE_PKG1_STRING,
												CREDIT1,
												CREDIT1_STRING,
												MGPRS,
												MGPRS_STRING,
												MVNPT_0,
												MVNPT_0_STRING,
												MVNPT1_0,
												MVNPT1_0_STRING,
												MVOICE_LM,
												MVOICE_LM_STRING,
												MSMS_LM,
												MSMS_LM_STRING,
												MVOICE_0,
												MVOICE_0_STRING,
												MSMS,
												MSMS_STRING,
												KM2T,
												KM2T_STRING,
												KM3T,
												KM3T_STRING,
												VOICE_TH,
												VOICE_TH_STRING,
												KM1T,
												KM1T_STRING,
												GPRS_0,
												GPRS_0_STRING,
												SMSVNPT,
												SMSVNPT_STRING,
												MSMSVNPT,
												MSMSVNPT_STRING,
												WALLET1,
												WALLET1_STRING,
												WALLET2,
												WALLET2_STRING,
												WALLET3,
												WALLET3_STRING,
												G_VOICE,
												G_VOICE_STRING,
												G_DATA,
												G_DATA_STRING,
												DATA_KM1,
												DATA_KM1_STRING,
												KM99T,
												KM99T_STRING,
												GPRS31,
												GPRS31_STRING,
												GPRS6,
												GPRS6_STRING,
												KMKNDL,
												KMKNDL_STRING,
												VOICE_LM_DL,
												VOICE_LM_DL_STRING,
												SMS_LM_DL,
												SMS_LM_DL_STRING,
												GPRS5,
												GPRS5_STRING,
												DATAZ1,
												DATAZ1_STRING,
												DATAZ2,
												DATAZ2_STRING,
												DATAZ3,
												DATAZ3_STRING,
												KM4,
												KM4_STRING,
												KMDK4,
												KMDK4_STRING,
												KM4T,
												KM4T_STRING,
												THOAI_LM1,
												THOAI_LM1_STRING,
												TK1,
												TK1_STRING,
												KM1V,
												KM1V_STRING,
												KM2V,
												KM2V_STRING,
												KM3V,
												KM3V_STRING,
												KM4V,
												KM4V_STRING,
												DATA_LN,
												DATA_LN_STRING,
												DATA_VC,
												DATA_VC_STRING,
												VOICE_LMZ,
												VOICE_LMZ_STRING,
												VORMO_0,
												VORMO_0_STRING,
												VORMI_0,
												VORMI_0_STRING,
												SMSRM_0,
												SMSRM_0_STRING,
												VOICE_SP1,
												VOICE_SP1_STRING,
												VOICE_SP2,
												VOICE_SP2_STRING,
												SMS_SP1,
												SMS_SP1_STRING,
												SMS_SP2,
												SMS_SP2_STRING,
												TK_AP1,
												TK_AP1_STRING,
												TK_AP2,
												TK_AP2_STRING,
												DUMP_DATETIME,
												FTP_FILENAME,
												DAY_KEY,
												'IN_DUMP_HN' as SRC_SYS_CD
								    		FROM MBF_DATALAKE.DUMP_BUNDLE_HN WHERE DAY_KEY = date_format(date_add(from_unixtime(unix_timestamp('$day_key','yyyyMMdd')),1),'yyyyMMdd') AND UPPER(MSISDN) <> 'MSISDN'
								    		UNION ALL
								    		SELECT 
								    			MSISDN,
												PROFILE_ID,
												KM1,
												KM1_STRING,
												KM2,
												KM2_STRING,
												KMDK1,
												KMDK1_STRING,
												KMDK2,
												KMDK2_STRING,
												BONUS2,
												BONUS2_STRING,
												BONUS,
												BONUS_STRING,
												VOICE_KM,
												VOICE_KM_STRING,
												VOICE2,
												VOICE2_STRING,
												VOICE,
												VOICE_STRING,
												VOICE_OP,
												VOICE_OP_STRING,
												SMS_KM,
												SMS_KM_STRING,
												SMS2,
												SMS2_STRING,
												SMS,
												SMS_STRING,
												SMS_OP,
												SMS_OP_STRING,
												SMSREFILL,
												SMSREFILL_STRING,
												SUBSC_SMS,
												SUBSC_SMS_STRING,
												PREODICSMS,
												PREODICSMS_STRING,
												GPRS_KM,
												GPRS_KM_STRING,
												GPRS2,
												GPRS2_STRING,
												GPRS,
												GPRS_STRING,
												VIDEO,
												VIDEO_STRING,
												VOICE_LM,
												VOICE_LM_STRING,
												SMS_LM,
												SMS_LM_STRING,
												VOICE_PKG,
												VOICE_PKG_STRING,
												SMS_PKG,
												SMS_PKG_STRING,
												GPRS4,
												GPRS4_STRING,
												VOICE_PKG1,
												VOICE_PKG1_STRING,
												CREDIT1,
												CREDIT1_STRING,
												MGPRS,
												MGPRS_STRING,
												MVNPT_0,
												MVNPT_0_STRING,
												MVNPT1_0,
												MVNPT1_0_STRING,
												MVOICE_LM,
												MVOICE_LM_STRING,
												MSMS_LM,
												MSMS_LM_STRING,
												MVOICE_0,
												MVOICE_0_STRING,
												MSMS,
												MSMS_STRING,
												KM2T,
												KM2T_STRING,
												KM3T,
												KM3T_STRING,
												VOICE_TH,
												VOICE_TH_STRING,
												KM1T,
												KM1T_STRING,
												GPRS_0,
												GPRS_0_STRING,
												SMSVNPT,
												SMSVNPT_STRING,
												MSMSVNPT,
												MSMSVNPT_STRING,
												WALLET1,
												WALLET1_STRING,
												WALLET2,
												WALLET2_STRING,
												WALLET3,
												WALLET3_STRING,
												G_VOICE,
												G_VOICE_STRING,
												G_DATA,
												G_DATA_STRING,
												DATA_KM1,
												DATA_KM1_STRING,
												KM99T,
												KM99T_STRING,
												GPRS31,
												GPRS31_STRING,
												GPRS6,
												GPRS6_STRING,
												KMKNDL,
												KMKNDL_STRING,
												VOICE_LM_DL,
												VOICE_LM_DL_STRING,
												SMS_LM_DL,
												SMS_LM_DL_STRING,
												GPRS5,
												GPRS5_STRING,
												DATAZ1,
												DATAZ1_STRING,
												DATAZ2,
												DATAZ2_STRING,
												DATAZ3,
												DATAZ3_STRING,
												KM4,
												KM4_STRING,
												KMDK4,
												KMDK4_STRING,
												KM4T,
												KM4T_STRING,
												THOAI_LM1,
												THOAI_LM1_STRING,
												TK1,
												TK1_STRING,
												KM1V,
												KM1V_STRING,
												KM2V,
												KM2V_STRING,
												KM3V,
												KM3V_STRING,
												KM4V,
												KM4V_STRING,
												DATA_LN,
												DATA_LN_STRING,
												DATA_VC,
												DATA_VC_STRING,
												VOICE_LMZ,
												VOICE_LMZ_STRING,
												VORMO_0,
												VORMO_0_STRING,
												VORMI_0,
												VORMI_0_STRING,
												SMSRM_0,
												SMSRM_0_STRING,
												VOICE_SP1,
												VOICE_SP1_STRING,
												VOICE_SP2,
												VOICE_SP2_STRING,
												SMS_SP1,
												SMS_SP1_STRING,
												SMS_SP2,
												SMS_SP2_STRING,
												TK_AP1,
												TK_AP1_STRING,
												TK_AP2,
												TK_AP2_STRING,
												DUMP_DATETIME,
												FTP_FILENAME,
												DAY_KEY,
												'IN_DUMP_HCM' as SRC_SYS_CD
								    		FROM MBF_DATALAKE.DUMP_BUNDLE_HCM WHERE DAY_KEY = date_format(date_add(from_unixtime(unix_timestamp('$day_key','yyyyMMdd')),1),'yyyyMMdd') AND UPPER(MSISDN) <> 'MSISDN'
						    			) u
								) x
							) y LATERAL VIEW EXPLODE(WALLET_MAP) wm AS WALLET_CD, VALUE
						) dbh
					LEFT JOIN
						(SELECT * FROM MBF_BIGDATA.ADMR_WALLET WHERE DAY_KEY = '$day_key' and CURR_IND = '1') aw
					ON
						UPPER(dbh.WALLET_CD) = UPPER(aw.WALLET_CD)
					LEFT JOIN 
						(SELECT * FROM MBF_BIGDATA.ADMR_ACCOUNT_SERVICE WHERE DAY_KEY = '$day_key' and CURR_IND = '1') aas
					ON
						dbh.MSISDN = aas.SERVICE_NBR
					WHERE 
						dbh.WALLET_AMT > 0 AND aas.lfcycl_stat_cd = '4'
                    ",
		"tempTable"  : "adme_wallet_bal_forfeit_edr_tmp",
		"countSourceRecord":"0"
	  }   	  
	]
}