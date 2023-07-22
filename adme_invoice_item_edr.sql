{
	 "description":"ADME_INVOICE_ITEM_EDR",
	 "sqlStatements": [
	  {
		"sql"      : "
						SELECT 							
							cast (from_unixtime(unix_timestamp(b.`MONTH`,'yyyy-MM-dd'),'yyyyMM') as INTEGER) 				 as MO_KEY ,
							cast (from_unixtime(unix_timestamp(b.`MONTH`,'yyyy-MM-dd HH'),'HH') as INTEGER) 			 as HOUR_KEY,
							cast (NULL as string) 																			 as UUID,
							cast (b.last_pay_date as timestamp) 															 as INVC_DT, 
							cast (from_unixtime(unix_timestamp(b.sta_date,'yyyy-MM-dd'),'yyyyMM') as INTEGER) 			 as BILL_CYCLE_KEY, 
							b.invoice_id 																					 as INVC_NBR_REF,
							C.CUST_KEY 						AS CUST_KEY,
							C.ACCT_KEY 						AS ACCT_KEY,
							a.isdn 							as SERVICE_NBR,
							cast (NULL as INTEGER) 									as INVC_ITEM_SRL_NBR,
							cast (NULL as string) 									 as BILL_HEAD_CD,
							(CASE 
								WHEN A.TYPE IN ('102','104','108','106','107') THEN 'VOICE_OFFNET_OG'
								WHEN A.TYPE IN ('201','300','802','701','702','703','704','800') THEN 'VAS_UC_CHRG'
								WHEN A.TYPE  = '10' THEN 'SMS_PACK_NRC_CHRG'
								WHEN A.TYPE  = '101' THEN 'VOICE_ONNET_OG'
								WHEN A.TYPE IN ('103','105','109') THEN 'VOICE_PSTN_OG'
								WHEN A.TYPE = '32' THEN 'VOICE_ISD_OG'
								WHEN A.TYPE IN ('360','350','351','352','353','354','355','356','357','358','359','399','400') THEN 'SMS_VAS'
								WHEN A.TYPE IN ('304','308','307','306','302') THEN 'SMS_OFFNET_OG'
								WHEN A.TYPE = '310' THEN 'SMS_ISD_OG'
								WHEN A.TYPE IN ('303','305','309') THEN 'SMS_PSTN_OG'
								WHEN A.TYPE = '301' THEN 'SMS_ONNET_OG'
								WHEN A.TYPE IN ('401','500') THEN 'DATA_LOCAL'
								WHEN A.TYPE = '402' THEN 'DATA_VAS'
								WHEN A.TYPE IN ('604','654') THEN 'SMS_RMNG_ISD_IC'
								WHEN A.TYPE IN ('603','653') THEN 'SMS_RMNG_ISD_OG'
								WHEN A.TYPE IN ('651','601') THEN 'VOICE_RMNG_ISD_OG'
								WHEN A.TYPE IN ('602','652') THEN 'VOICE_RMNG_ISD_IC'
								WHEN A.TYPE IN ('605','656') THEN 'DATA_RMNG_ISD'
								WHEN A.TYPE IN ('11','504') THEN 'BUNDLE_PACK_NRC_CHRG'
								WHEN A.TYPE = '803' THEN 'HNDST_FEES'
								WHEN A.TYPE IN ('12','13') THEN 'RMNG_PACK_CHRG'
								WHEN A.TYPE IN ('510','502','600') THEN 'DATA_PACK_NRC_CHRG'
								WHEN A.TYPE IN ('8','9') THEN 'VOICE_PACK_NRC_CHRG'
								WHEN A.TYPE IN ('501','503','801') THEN 'VAS_NRC_CHRG'
								WHEN A.TYPE = '99' THEN 'MONTH_FEE'
								WHEN A.TYPE IN ('97','98') AND CAST(A.CHARGE_VAT AS DECIMAL(10,2)) > 0 THEN 'DEBIT_ADJ_AMT'
								WHEN A.TYPE IN ('97','98') AND CAST(A.CHARGE_VAT AS DECIMAL(10,2)) < 0 THEN 'CREDIT_ADJ_AMT'
							END) 													as CHRG_HEAD_CD,
							cast (NULL as string) 									 as CHRG_TYP_CD,
							cast (c.CRM_OFFER_KEY as BIGINT)										 as OFFER_KEY,
							c.CRM_OFFER_CD 											 as OFFER_CD,
							d.OFFER_TYP_CD,
							a.type  					as BILL_ITEMS,
							cast (NULL as decimal(20,0)) 							 as PROD_KEY,
							cast (NULL as string) 									 as PROD_CD,
							'VND' 													 as USG_UOM_CD,
							'VND' 													 as CRNCY_CD,
							cast (a.DURATION as BIGINT)						 as DRTN,
							cast (NULL as BIGINT) 							 as RTD_USG,
							cast (NULL as BIGINT) 							 as FREE_USG,
							cast (NULL as BIGINT) 							 as CHRGBL_USG,
							cast (NULL as BIGINT) 							 as INTCONN_OG_USG,
							cast (NULL as BIGINT) 							 as INTCONN_IC_USG,
							cast (NULL as decimal(27,8)) 							 as CHRG_OS_AMT,
							cast (a.CHARGE as decimal(27,8)) 						 as CHRG_AMT,
							cast (NULL as decimal(27,8)) 						 as DEBIT_ADJ_AMT,
							cast (NULL as decimal(27,8)) 						 as CREDIT_ADJ_AMT,
							cast (a.CHARGE_VAT - a.CHARGE as decimal(27,8))									 as TAX_AMT,
							cast (a.CHARGE_VAT + a.PROM_AMOUNT_VAT + a.PROM_AMOUNT_VAT_2 as decimal(27,8))	 as BLLD_AMT,
							cast (a.CHARGE_VAT as decimal(27,8))											 as RVN_AMT,
							cast (NULL as decimal(27,8)) 							 as INTCONN_COST_AMT,
							cast (NULL as decimal(27,8)) 							 as INTCONN_RVN_AMT,
							cast (NULL as string) 									 as DISPUTE_IND,
							cast (NULL as decimal(27,8)) 							 as DISPUTE_AMT,
							cast (NULL as string) 									 as CHRG_WAIVED_IND,
							CAST(24 AS BIGINT) 										 as SRC_SYS_KEY,
							'BILLING_GOLD' 											 as SRC_SYS_CD,
							CURRENT_TIMESTAMP() 									 as LOAD_DT,
							'1' 													 as CURR_IND,
							1 														 as WRHS_ID
						FROM (
							SELECT * FROM mbf_datalake.bc_dttd_sub WHERE day_key = $day_key
						) a
						left join (
							 SELECT * FROM mbf_datalake.b4_invoice WHERE day_key = $day_key
						) b on a.cust_id = b.cust_id 
							and from_unixtime(unix_timestamp(a.`month`,'yyyy-MM-dd'),'yyyy-MM-dd') = from_unixtime(unix_timestamp(b.`month`,'yyyy-MM-dd'),'yyyy-MM-dd')
						left join (
							select * from mbf_bigdata.admr_account_service where day_key = $day_key
						) c on c.SERVICE_NBR = a.isdn
						left join (
							select * from mbf_bigdata.admr_offer WHERE day_key = $day_key
						) d on d.offer_key = c.crm_offer_key		       
					",
		"tempTable" : "ADME_INVOICE_ITEM_EDR_TMP",
		"countSourceRecord" : "1"
	  }
	]
}