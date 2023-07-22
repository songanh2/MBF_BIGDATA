--------------------------------------------------------
--  File created - Friday-June-30-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure GET_CURRENT_MARK_SP
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "KNDL_GOLD"."GET_CURRENT_MARK_SP" ( p_Member_ID    IN  Number,
                                  p_Balance_Type IN  Varchar2,
                                  p_Mark         OUT Number) Is

    P_SUM_DATE          Date:= TRUNC(sysdate,'MONTH');
    V_CUR_MO_KEY        NUMBER:= TO_NUMBER(TO_CHAR(P_SUM_DATE,'YYYYMM'));
    V_NEXT_MO_KEY       NUMBER;

    v_Mo_Key            Number;
    v_Out_balance_type  Varchar(10);
    v_Min_Expire_Date   Date;

    Cursor c_Last_Mo_Key(v_Member_ID        Number) Is
        Select  To_Number(To_Char(Max(BILL_CYCLE),'YYYYMM')) Cur_Mo_Key,
                To_Number(To_Char(ADD_MONTHS(Max(BILL_CYCLE),1),'YYYYMM')) Next_Mo_Key
        From (
            Select BILL_CYCLE
            From (
                Select *
                From dwd_mark_history
                Where member_id=v_Member_ID
                Order by BILL_CYCLE desc
            ) Where rownum = 1
        );

    /*Cursor c_Last_Mo_Key(v_Member_ID        Number,
                         p_CUR_MO_KEY       Number,
                         p_PRE_MO_KEY       Number) Is
        Select  To_Number(To_Char(ADD_MONTHS(Max(BILL_CYCLE),1),'YYYYMM')) Cur_Mo_Key,
                To_Number(To_Char(Max(BILL_CYCLE),'YYYYMM')) Pre_Mo_Key
        From (
            Select *
            From dwd_mark_history
            Where member_id=v_Member_ID
            And (Mo_Key = p_CUR_MO_KEY
            Or Mo_Key = p_PRE_MO_KEY)
        );*/

    Cursor c_Balance_Type(v_Member_ID       Number,
                          p_Next_Mo_key     Number,
                          p_Cur_Date        Date,
                          v_Balance_Type    Varchar) Is
    Select  BALANCE_TYPE,
            SUM(REMAIN_MARK) REMAIN_MARK,
            MIN(MARK_EXPIRE_DATE) MIN_EXPIRE_DATE
    FROM (
            SELECT  A.MEMBER_ID                 AS MEMBER_ID,
                    A.TRANS_ID                  AS TRANS_ID,
                    A.BALANCE_TYPE              AS BALANCE_TYPE,
                    A.MARK_BALANCE_ID           AS MARK_BALANCE_ID,
                    A.MARK_ITEM_ID              AS MARK_ITEM_ID,
                    A.ORG_MARK                  AS ORG_MARK,
                    NVL(A.MARK,0)               AS MARK,
                    NVL(A.REMAIN_MARK,0)        AS REMAIN_MARK,
                    --A.USG_MARK                  AS USG_MARK_DTL,
                    --CASE WHEN ADD_MONTHS(trunc(sysdate,'MONTH'),-1) < LAST_TRANS_DATE AND LAST_TRANS_DATE < sysdate  THEN NVL(USG_MARK,0) ELSE 0 END AS USG_MARK_DTL,                    
                    A.MARK_EXPIRE_DATE          AS MARK_EXPIRE_DATE,
                    A.MARK_SRC_TABLE            AS MARK_SRC_TABLE,
                    SYSDATE                     AS CREATE_DATE,
                    'ADMIN'                     AS CREATE_BY                    
            FROM DWD_MARK_HISTORY_DTL A
            WHERE A.member_id = v_Member_ID
            AND  nvl(A.MARK_EXPIRE_DATE,p_Cur_Date) >= p_Cur_Date
        ) WHERE (REMAIN_MARK > 0)          
          AND (BALANCE_TYPE = v_Balance_Type Or v_Balance_Type is null)
    Group by BALANCE_TYPE;


BEGIN
    p_Mark := 0;

    Open c_Last_Mo_Key(p_Member_ID);
    Fetch c_Last_Mo_Key Into V_CUR_MO_KEY,V_NEXT_MO_KEY;
    Close c_Last_Mo_Key;

    If(v_cur_mo_key is null) Then
        P_SUM_DATE  := TRUNC(sysdate,'MONTH');
        --V_CUR_MO_KEY := TO_NUMBER(TO_CHAR(ADD_MONTHS(P_SUM_DATE,-1),'YYYYMM'));
        --V_NEXT_MO_KEY := TO_NUMBER(TO_CHAR(P_SUM_DATE,'YYYYMM'));
        V_CUR_MO_KEY := TO_NUMBER(TO_CHAR(ADD_MONTHS(P_SUM_DATE,-2),'YYYYMM'));
        V_NEXT_MO_KEY := TO_NUMBER(TO_CHAR(ADD_MONTHS(P_SUM_DATE,-1),'YYYYMM'));        
    End If;

    --dbms_output.put_line('V_CUR_MO_KEY:' || V_CUR_MO_KEY);
    --dbms_output.put_line('V_NEXT_MO_KEY:' || V_NEXT_MO_KEY);

    If(p_Balance_Type is not null) Then
        Open c_Balance_Type(p_Member_ID,v_Next_Mo_Key, sysdate, p_Balance_Type);
        Fetch c_Balance_Type into v_Out_balance_type, p_Mark, v_Min_Expire_Date;
        Close c_Balance_Type;
--        dbms_output.put_line(p_Balance_Type);
    Else
        For v_Total_Mark in c_Balance_Type(p_Member_ID,v_Next_Mo_Key, sysdate,p_Balance_Type) Loop
            v_Min_Expire_Date := v_Total_Mark.MIN_EXPIRE_DATE;
            p_Mark := p_Mark + v_Total_Mark.REMAIN_MARK;

            If(v_Min_Expire_Date > v_Total_Mark.MIN_EXPIRE_DATE) Then
                v_Min_Expire_Date := v_Total_Mark.MIN_EXPIRE_DATE;
            End If;
        End Loop;
    End If;
    --dbms_output.put_line('p_Mark:' || p_Mark);
EXCEPTION
    WHEN OTHERS THEN
--        If c_Total_Mark%isOpen Then Close c_Total_Mark; End If;
        If c_Balance_Type%isOpen Then Close c_Balance_Type; End If;
        Raise_Application_Error(-20084,SQLErrm);
END; -- Procedure

/
