{
	 "description":"ADME_INVOICE_PAYMENT_ALLOC_EDR",
	 "sqlStatements": [
	  {
		"sql"      	:"
					SELECT 
						CAST(DATE_FORMAT(A.MONTH,'yyyyMM') AS INTEGER) 		AS MO_KEY ,
						CAST(DATE_FORMAT(A.MONTH,'HH') AS INTEGER) 			AS HOUR_KEY ,
						CAST(NULL AS STRING) 								AS UUID ,
						A.INVOICE_ID 										AS INVC_NBR_REF ,
						CAST(A.MONTH AS TIMESTAMP) 							AS INVC_DT ,
						CAST(NULL AS DECIMAL(27,8)) 						AS INVC_AMT ,
						CAST(A.BILL_CYCLE_ID AS INTEGER) 					AS BILL_CYCLE_KEY ,
						A.PAYMENT_ID 										AS PYMT_REF_NBR ,
						CAST(C.CREATE_DATE AS TIMESTAMP) 					AS PYMT_DT ,
						CAST(C.PAYMENT_AMOUNT AS DECIMAL(27,8)) 			AS PYMT_AMT ,
						CAST(A.AMOUNT_VAT AS DECIMAL(27,8)) 				AS INVC_PYMT_ALLOC_AMT ,
						CAST(NULL AS STRING) 								AS FULL_INVC_PAID_IND ,
						CAST(A.CREATE_DATE AS TIMESTAMP) 					AS INVC_PYMT_ALLOC_DT ,
						CAST(NULL AS TIMESTAMP) 							AS PYMT_ALLOC_REVERSAL_DT ,
						A.STATUS 											AS INVC_PYMT_ALLOC_STAT_CD ,
						CAST(NULL AS STRING) 								AS INVC_PYMT_ALLOC_STAT_RSN_CD ,
						24 													AS SRC_SYS_KEY ,
						'BILLING_GOLD' 										AS SRC_SYS_CD ,
						CURRENT_TIMESTAMP() 								AS LOAD_DT ,
						'1' 												AS CURR_IND ,
						1 													AS WRHS_ID
					FROM MBF_DATALAKE.B4_PAYMENT_DETAIL A
					JOIN MBF_DATALAKE.B4_PAYMENT C ON A.PAYMENT_ID = C.PAYMENT_ID 
					WHERE 
					A.DAY_KEY = '$day_key'  and C.DAY_KEY = '$day_key'
					 ",
		"tempTable"  : "ADME_INVOICE_PAYMENT_ALLOC_EDR_TEMP",
		"countSourceRecord":"1"
	  }   	  
	]
}