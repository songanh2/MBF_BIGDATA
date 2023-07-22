CREATE OR REPLACE package pck_merge_data_p2 as 
  PROCEDURE PRC_MERGE_MARK_HISTORY_DTL(p_Member_ID  Number) ;
  FUNCTION FN_GET_NEXT_SEQ_VAL Return Number;
end pck_merge_data_p2;
/


CREATE OR REPLACE package body pck_merge_data_p2 as

  PROCEDURE PRC_MERGE_MARK_HISTORY_DTL(p_member_id  number)  as
  
    P_SUM_DATE          Date:= TRUNC(sysdate,'MONTH');
    V_CUR_MONTH         NUMBER:=TO_NUMBER(TO_CHAR(P_SUM_DATE,'YYYYMM'));
    --V_CUR_MO_KEY        NUMBER:=TO_NUMBER(TO_CHAR(ADD_MONTHS(P_SUM_DATE,-1),'YYYYMM'));
    V_CUR_MO_KEY        NUMBER:=202304;
    V_PRE_MO_KEY        NUMBER:=TO_NUMBER(TO_CHAR(ADD_MONTHS(P_SUM_DATE,-2),'YYYYMM'));
    
    V_USE_MONTHS        NUMBER:= 6;
  
    Cursor c_Last_Mo_Key(v_Member_ID        Number,
                         p_CUR_MO_KEY       Number,
                         p_PRE_MO_KEY       Number) Is
        Select  To_Number(To_Char(ADD_MONTHS(Max(BILL_CYCLE),1),'YYYYMM')) Cur_Mo_Key,
                To_Number(To_Char(Max(BILL_CYCLE),'YYYYMM')) Pre_Mo_Key
        From (
            Select *
            From dwd_mark_history
            Where member_id=v_Member_ID or v_Member_ID is null
            And (Mo_Key = p_CUR_MO_KEY
            Or Mo_Key = p_PRE_MO_KEY)
        );    
  begin
  
    Open c_Last_Mo_Key(p_Member_ID,v_cur_mo_key, v_pre_mo_key);
    Fetch c_Last_Mo_Key Into v_cur_mo_key,v_pre_mo_key;
    Close c_Last_Mo_Key;    
    
    INSERT /*+ APPEND nologging */ INTO DWD_MARK_HISTORY_DTL_P2 (
        bill_cycle,
        mark_date,
        member_key,
        member_id,
        trans_id,
        balance_type,
        mark_balance_id,
        mark_item_id,
        org_mark,
        mark,
        usg_mark,
        remain_mark,
        acc_usg_mark,
        last_trans_date,
        mark_expire_date,
        mark_src_table,
        status,
        create_date,
        data_code
    )   
    SELECT
      TRUNC(SYSDATE,'MM')       AS BILL_CYCLE,
      TRUNC(SYSDATE,'MM')       AS MARK_DATE,
      MEMBER_KEY,
      MEMBER_ID, 
      PCK_MERGE_DATA_P2.FN_GET_NEXT_SEQ_VAL()           AS TRANS_ID,  
      '04'                                              AS BALANCE_TYPE,  
      2                                                 AS MARK_BALANCE_ID,  
      '602'                                             AS MARK_ITEM_ID,  --Fix điểm thưởng sử dụng ngay
      SUM(ORG_MARK)                                     AS ORG_MARK,   
      SUM(MARK - USG_MARK)                              AS MARK, 
      SUM(USG_MARK)                                     AS USG_MARK, 
      SUM(REMAIN_MARK)                                  AS REMAIN_MARK,
      SUM(ACC_USG_MARK)                                 AS ACC_USG_MARK,
      NULL                                              AS LAST_TRANS_DATE,
      TRUNC(ADD_MONTHS(SYSDATE,V_USE_MONTHS),'MM')      AS MARK_EXPIRE_DATE, 
      '0'                                               AS MARK_SRC_TABLE,
      '1'                                               AS STATUS,
      SYSDATE                                           AS CREATE_DATE,
      NULL                                              AS DATA_CODE      
    FROM (  
        SELECT   
                A.BILL_CYCLE,
                A.MARK_DATE,
                A.MEMBER_KEY,            
                A.MEMBER_ID                             AS MEMBER_ID, 
                A.TRANS_ID                              AS TRANS_ID, 
                A.BALANCE_TYPE                          AS BALANCE_TYPE, 
                A.MARK_BALANCE_ID                       AS MARK_BALANCE_ID, 
                A.MARK_ITEM_ID                          AS MARK_ITEM_ID, 
                A.ORG_MARK                              AS ORG_MARK, 
                NVL(A.MARK,0)                           AS MARK, 
                NVL(B.USG_MARK,0)                       AS USG_MARK, 
                NVL(A.REMAIN_MARK,0)                    AS REMAIN_MARK, 
                NVL(A.ACC_USG_MARK,0)                   AS ACC_USG_MARK,
                NVL(A.MARK_EXPIRE_DATE,SYSDATE)         AS MARK_EXPIRE_DATE, 
                A.MARK_SRC_TABLE                        AS MARK_SRC_TABLE  
        FROM DWD_MARK_HISTORY_DTL A  
        LEFT OUTER JOIN ( 
                        SELECT  
                            MEMBER_ID, 
                            MINUS_TRANS_ID, 
                            SUM(MARK) USG_MARK 
                        FROM ( 
                            SELECT X.MO_KEY, X.MEMBER_ID, Y.MINUS_TRANS_ID, SUM(NVL(Y.MARK,0)) MARK 
                            FROM DWB_MARK_TRANS X, DWB_MARK_TRANS_DTL Y 
                            WHERE X.MO_KEY = Y.MO_KEY  
                            AND X.TRANS_ID = Y.TRANS_ID  
                            AND X.MO_KEY >= V_CUR_MO_KEY 
                            AND X.MO_KEY <= TO_NUMBER(TO_CHAR(SYSDATE,'YYYYMM')) 
                            AND X.MEMBER_ID = V_CUR_MO_KEY  
                            AND X.STATUS <> '0' 
                            GROUP BY X.MO_KEY, X.MEMBER_ID, Y.MINUS_TRANS_ID                         
                            UNION ALL                         
                            SELECT X.MO_KEY, X.MEMBER_ID, Y.MINUS_TRANS_ID, SUM(NVL(Y.MARK,0)) MARK 
                            FROM DWB_MARK_GOODS X, DWB_MARK_GOODS_DTL Y 
                            WHERE X.MO_KEY = Y.MO_KEY  
                            AND X.TRANS_ID = Y.TRANS_ID 
                            AND X.MO_KEY >= V_CUR_MO_KEY 				
                            AND X.MO_KEY <= TO_NUMBER(TO_CHAR(SYSDATE,'YYYYMM')) 
                            AND X.MEMBER_ID = V_CUR_MO_KEY  
                            AND X.STATUS <> '0' 
                            GROUP BY X.MO_KEY, X.MEMBER_ID, Y.MINUS_TRANS_ID 
                        ) GROUP BY MEMBER_ID,MINUS_TRANS_ID 
                        ) B   
        ON A.MEMBER_ID = B.MEMBER_ID 
        AND A.TRANS_ID = B.MINUS_TRANS_ID  				
        WHERE A.MEMBER_ID = p_member_id or p_member_id is null 
    ) WHERE MARK > 0 AND (MARK - USG_MARK) > 0
        GROUP BY MEMBER_KEY, MEMBER_ID
        ORDER BY MARK_EXPIRE_DATE ASC;        
  end prc_merge_mark_history_dtl;

  FUNCTION FN_GET_NEXT_SEQ_VAL return number as
    v_next_seq_val number;
  begin
    select SEQ_MARK_TRANS.nextval
            into v_next_seq_val
            from dual;    
    return v_next_seq_val;
  end fn_get_next_seq_val;

end pck_merge_data_p2;
/
