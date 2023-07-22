CREATE OR REPLACE PACKAGE "PCK_PROCESS_RANK" IS
    FUNCTION GET_RANK_LIST(p_MEMBER_ID      NUMBER,
                           p_SUM_DATE       DATE) RETURN T_RANK_TAB PIPELINED ;

    PROCEDURE  RANK_PROCESS_ALL(P_SUM_DATE       DATE) ;


    PROCEDURE  RANK_PROCESS_SP( P_MEMBER_ID      NUMBER,
                                P_SUM_DATE       DATE,
                                P_RERUN          BOOLEAN DEFAULT FALSE,
                                P_APPLY_TIME     VARCHAR DEFAULT 'SYSDATE',
                                p_CHANEL         VARCHAR DEFAULT 'WEB',
                                P_IP_ADDRESS     VARCHAR DEFAULT '127.0.0.1',
                                P_NEXT_RANK_DATE DATE DEFAULT NULL
                                );

    PROCEDURE  RANK_CHANGE_PROCESS_SP(P_MEMBER_ID      NUMBER,
                                      P_SUM_DATE       DATE,
                                      p_TO_RANK_ID     NUMBER,
                                      P_REASON_CODE    VARCHAR,
                                      p_USER_NAME      VARCHAR,
                                      p_CHANEL         VARCHAR DEFAULT 'WEB',
                                      P_IP_ADDRESS     VARCHAR DEFAULT '127.0.0.1');

    PROCEDURE  PRC_RANK_VIP_HISTORY(p_SUM_DATE       DATE);

    PROCEDURE MERGE_UPDATE_RANK_VIP_TH
    (
      P_MEMBER_KEY   IN       VARCHAR2,
      P_ERROR        OUT      VARCHAR2
    );        
END;
/


CREATE OR REPLACE PACKAGE BODY                       PCK_PROCESS_RANK IS
        V_ETL_SERVER_NAME      VARCHAR2 (100) := 'MBF_KNDL_NEW';
        V_SOURCE_SYSTEM_CD     VARCHAR2 (100) := 'MBF_KNDL_NEW';
        V_SOURCE_OBJECT_TYPE   VARCHAR2 (100) := 'MBF_KNDL_NEW';
        V_ETL_USERNAME         VARCHAR2 (100) := 'ADMIN';
        V_SQLERRORCODE         VARCHAR2 (1000);
        V_SQLERRORTEXT         VARCHAR2 (4000);


        PROCEDURE PRC_GET_RANK_INFO(p_Rank_ID       Number, 
                                    p_Sum_Date      Date,
                                    p_From_Mark     Out Number,
                                    p_To_Mark       Out Number,
                                    p_Alert_Mark    Out Number) Is
            Cursor c_Rank_Info Is
                Select From_Mark, To_Mark, ALERT_MARK
                From DWL_RANK_PROFILE RP
                Where Rank_ID = p_Rank_ID
                AND TRUNC(p_Sum_Date,'MONTH') >= Trunc(RP.STA_DATETIME,'MONTH')
                AND (TRUNC(p_Sum_Date,'MONTH') < RP.END_DATETIME Or RP.END_DATETIME Is Null);

            v_Value         Number;
        BEGIN
            Open c_Rank_Info;
            Fetch c_Rank_Info into p_From_Mark,p_To_Mark,p_Alert_Mark;
                If c_Rank_Info%NOTFOUND Then
                    RAISE_APPLICATION_ERROR(-20085,'Khong tim thay tham so RANK_PROFILE cua hang ' || p_Rank_ID);
                End If;
            Close c_Rank_Info;
        EXCEPTION
            WHEN OTHERS THEN
                If c_Rank_Info%isOpen Then Close c_Rank_Info; End If;
                RAISE_APPLICATION_ERROR(-20084,'FNC_GET_RANK_MONTHS:' || SQLERRM);
        END;

        FUNCTION FNC_GET_RANK_MONTHS RETURN NUMBER is
            Cursor c_Rank_Months Is
                Select value
                From DWL_AP_PARAM
                Where name = 'RANK_MONTHS';
            v_Value         Number;
        BEGIN
            Open c_Rank_Months;
            Fetch c_Rank_Months into v_Value;
                If c_Rank_Months%NOTFOUND Then
                    RAISE_APPLICATION_ERROR(-20085,'Khong tim thay tham so RANK_MONTHS tren AP_PARAM');
                End If;
            Close c_Rank_Months;

            Return v_Value;
        EXCEPTION
            WHEN OTHERS THEN
                If c_Rank_Months%isOpen Then Close c_Rank_Months; End If;
                RAISE_APPLICATION_ERROR(-20084,'FNC_GET_RANK_MONTHS:' || SQLERRM);
        END;

        FUNCTION FNC_GET_AP_PARAM(  p_Name    Varchar2,
                                    p_Type     Varchar2) RETURN NUMBER is
            Cursor c_Rank_Months Is
                Select value
                From DWL_AP_PARAM
                Where name = p_Name
                And Type = p_Type;
            v_Value         Number;
        BEGIN
            Open c_Rank_Months;
            Fetch c_Rank_Months into v_Value;
                If c_Rank_Months%NOTFOUND Then
                    RAISE_APPLICATION_ERROR(-20085,'Khong tim thay tham so ' || p_Name || '-' || p_Type || ' tren AP_PARAM');
                End If;
            Close c_Rank_Months;

            Return v_Value;
        EXCEPTION
            WHEN OTHERS THEN
                If c_Rank_Months%isOpen Then Close c_Rank_Months; End If;
                RAISE_APPLICATION_ERROR(-20084,'FNC_GET_RANK_MONTHS:' || SQLERRM);
        END;

        PROCEDURE  RANK_MEMBER_PROCESS_SP( P_MEMBER_ID          NUMBER,
                                           P_SUM_DATE           DATE,
                                           P_APPLY_TIME         VARCHAR2,
                                           P_STA_DATETIME       DATE,
                                           v_RANK_CODE          VARCHAR2,
                                           v_RANK_DATE          DATE,
                                           v_RANK_MARK          NUMBER,
                                           v_TO_RANK_ID         NUMBER,
                                           v_OLD_RANK_ID        NUMBER,
                                           v_PARTNER_RANK_ID    NUMBER,
                                           v_PARTNER_ID         NUMBER,
                                           V_MEMBER_LINK_ID     NUMBER,
                                           V_REASON_CODE        VARCHAR2,
                                           v_CREATE_BY          VARCHAR2,
                                           v_ROW_ID             VARCHAR2,
                                           v_RANK_ORDER         NUMBER,
                                           v_OLD_RANK_ORDER     NUMBER,
                                           v_MOB_TYPE           VARCHAR2,
                                           v_MEMBER_KEY         VARCHAR2,
                                           v_CHANEL             VARCHAR2 Default 'WEB',
                                           v_IP_ADDRESS         VARCHAR2 Default '127.0.0.1',
                                           v_Use_Months         Number Default 0,
                                           P_NEXT_RANK_DATE     DATE DEFAULT NULL,
                                           v_ONNET_SUB          VARCHAR2
                                           ) IS

            v_Min_Rank_Mark_MC      NUMBER;
            v_Min_Rank_Mark_MG      NUMBER;
            v_Min_Rank_Mark_Months  NUMBER;
            v_Min_Rank_EndDate      DATE;

            p_Rank_Months       NUMBER;

            V_MO_KEY            NUMBER:=TO_NUMBER(TO_CHAR(P_SUM_DATE,'YYYYMM'));
            V_NEXT_RANK_DATE    DATE;

            V_NEXT_MONTH        DATE:= TRUNC(p_Sum_Date,'MONTH');
            V_NEXT_MO_KEY       NUMBER:= TO_NUMBER(TO_CHAR(V_NEXT_MONTH,'YYYYMM'));
            V_NEXT_DAY_KEY      NUMBER:= TO_NUMBER(TO_CHAR(V_NEXT_MONTH,'YYYYMMDD'));

            v_Sta_DateTime      Date:= TRUNC(P_SUM_DATE,'MONTH') + 14;
            v_End_DateTime      Date:= v_Sta_DateTime - 1;
            v_Months            Number;
        BEGIN
            p_Rank_Months       := FNC_GET_RANK_MONTHS();
            V_NEXT_RANK_DATE    := CASE WHEN P_NEXT_RANK_DATE IS NULL THEN TRUNC(ADD_MONTHS(P_SUM_DATE,p_Rank_Months),'MONTH') ELSE P_NEXT_RANK_DATE END;

            --dbms_output.put_line('Thang Hang');
            v_Min_Rank_Mark_MC := FNC_GET_AP_PARAM('MIN_RANK_MARK','MC');
            v_Min_Rank_Mark_MG := FNC_GET_AP_PARAM('MIN_RANK_MARK','MG');
            v_Min_Rank_Mark_Months := FNC_GET_AP_PARAM('MIN_RANK_MARK','MONTHS_BLACKLIST');
            v_Months := v_Use_Months;--p_Rank_Months - ROUND(MONTHS_BETWEEN(v_RANK_DATE,P_SUM_DATE));
            --Thang Hang
            --dbms_output.put_line('RANK_CODE:' || v_RANK_CODE);
            --dbms_output.put_line('A:' || To_Char(sysdate,'dd/mm/yyyy hh24:mi:ss'));

            If(v_RANK_CODE = 'PROFILE') Then

                Update DWB_RANK_HISTORY Set End_datetime = v_End_DateTime, Rank_Date = v_RANK_DATE
                Where rowid = v_ROW_ID;

                --Kiem tra trong truong hop thang chay du lieu trung voi thang xet hang
                --Thi cap nhat RANK_DATE de bo qua truong hop giu hang
                If(v_RANK_DATE = P_SUM_DATE) Then
                    Update DWR_MEMBER Set Rank_ID = v_TO_RANK_ID, Rank_Date = V_NEXT_RANK_DATE, last_rank_date = rank_date
                    Where Member_ID = P_MEMBER_ID;
                Else
                    Update DWR_MEMBER Set Rank_ID = v_TO_RANK_ID
                    Where Member_ID = P_MEMBER_ID;
                End If;

                Insert into DWB_RANK_HISTORY(mo_key,day_key,member_id,rank_type,months,rank_date,rank_id,PRE_RANK_ID,rank_mark,sta_datetime,end_datetime,reason_code,create_date,create_by)
                Values(V_NEXT_MO_KEY,V_NEXT_DAY_KEY,P_MEMBER_ID,'01',v_Months,v_RANK_DATE,v_TO_RANK_ID,v_OLD_RANK_ID,v_RANK_MARK,v_Sta_DateTime,null,NVL(V_REASON_CODE,'RANK_01'),sysdate,'ADMIN');

                --Cho vao lich su
                Insert into dwb_action_audit(record_datetime,action_audit_id,member_id,action_id,chanel,user_name,ip_address,reason_code,table_name,member_key)
                values(sysdate,SEQ_ACTION_AUDIT.NEXTVAL,P_MEMBER_ID,'06',v_CHANEL,'SYSTEM',v_IP_ADDRESS,NVL(V_REASON_CODE,'RANK_01'),'DWR_MEMBER',v_MEMBER_KEY);
            End If;
            --dbms_output.put_line('B:' || To_Char(sysdate,'dd/mm/yyyy hh24:mi:ss'));
            If(v_RANK_CODE = 'EXCHANGE') Then
                Update DWB_RANK_HISTORY Set End_datetime = v_End_DateTime, Rank_Date = v_RANK_DATE
                Where rowid = v_ROW_ID;

                Update DWR_MEMBER Set Rank_ID = v_TO_RANK_ID, Rank_Date = V_NEXT_RANK_DATE, last_rank_date = rank_date
                Where Member_ID = P_MEMBER_ID;

                Insert into DWB_RANK_HISTORY(mo_key,day_key,member_id,rank_type,months,rank_date,rank_id,PRE_RANK_ID,rank_mark,sta_datetime,end_datetime,reason_code,partner_rank_id,partner_id,member_links_id,create_date,create_by)
                Values(V_NEXT_MO_KEY,V_NEXT_DAY_KEY,P_MEMBER_ID,'02',v_Months,v_RANK_DATE,v_TO_RANK_ID,v_OLD_RANK_ID,v_RANK_MARK,v_Sta_DateTime,null,NVL(V_REASON_CODE,'RANK_02'),v_PARTNER_RANK_ID,v_PARTNER_ID,V_MEMBER_LINK_ID,sysdate,'ADMIN');

                --Cho vao lich su
                Insert into dwb_action_audit(record_datetime,action_audit_id,member_id,action_id,chanel,user_name,ip_address,reason_code,table_name,member_key)
                values(sysdate,SEQ_ACTION_AUDIT.NEXTVAL,P_MEMBER_ID,'06',v_CHANEL,'SYSTEM',v_IP_ADDRESS,NVL(V_REASON_CODE,'RANK_02'),'DWR_MEMBER',v_MEMBER_KEY);
            End If;

            --dbms_output.put_line('C:' || To_Char(sysdate,'dd/mm/yyyy hh24:mi:ss'));
            If(v_RANK_CODE = 'VIP') Then
                --V_REASON_CODE := NVL(v_Up_Rank.REASON_CODE,'RANK_02');
                If(P_APPLY_TIME = 'SYSDATE') Then
                    v_Sta_DateTime := P_STA_DATETIME;
                    If(P_STA_DATETIME > Trunc(sysdate)) Then
                        v_End_DateTime := P_STA_DATETIME - 1;
                    Else
                        v_End_DateTime := P_STA_DATETIME;
                    End If;
                End If;

                If(v_ROW_ID is not null) Then
                    Update DWB_RANK_HISTORY Set End_datetime = v_End_DateTime, Rank_Date = v_RANK_DATE
                    Where rowid = v_ROW_ID;
                Else
                    v_Sta_DateTime := P_STA_DATETIME;
                End If;

                Update DWR_MEMBER Set Rank_ID = v_TO_RANK_ID, 
                                        Rank_Date = V_NEXT_RANK_DATE
                Where Member_ID = P_MEMBER_ID;

                Insert into DWB_RANK_HISTORY(mo_key,day_key,member_id,rank_type,months,rank_date,rank_id,PRE_RANK_ID,rank_mark,sta_datetime,end_datetime,reason_code,partner_rank_id,partner_id,member_links_id,create_date,create_by)
                Values(V_NEXT_MO_KEY,V_NEXT_DAY_KEY,P_MEMBER_ID,'03',v_Months,v_RANK_DATE,v_TO_RANK_ID,v_OLD_RANK_ID,0,v_Sta_DateTime,null,V_REASON_CODE,null,null,null,sysdate,v_CREATE_BY);

                --Cho vao lich su
                Insert into dwb_action_audit(record_datetime,action_audit_id,member_id,action_id,chanel,user_name,ip_address,reason_code,table_name,member_key)
                values(sysdate,SEQ_ACTION_AUDIT.NEXTVAL,P_MEMBER_ID,'06',v_CHANEL,v_CREATE_BY,v_IP_ADDRESS,V_REASON_CODE,'DWR_MEMBER',v_MEMBER_KEY);
            End If;

            --Giu Hang va Ha hang
            --dbms_output.put_line('D:' || To_Char(sysdate,'dd/mm/yyyy hh24:mi:ss'));
            If(v_RANK_CODE = 'RANK_DOWN') Then
                --dbms_output.put_line('D1:' || To_Char(sysdate,'dd/mm/yyyy hh24:mi:ss'));
                If(v_RANK_ORDER = v_OLD_RANK_ORDER) Then    --Giu Nguyen Hang
                    Update DWB_RANK_HISTORY Set End_datetime = v_End_DateTime, Rank_Date = v_RANK_DATE
                    Where rowid = v_ROW_ID;

                    Update DWR_MEMBER Set   Rank_ID = v_TO_RANK_ID,
                                            Rank_date = ADD_MONTHS(v_Rank_Date,p_Rank_Months),
                                            Last_Rank_Date = Rank_Date
                    Where Member_ID = P_MEMBER_ID;

                    Insert into DWB_RANK_HISTORY(mo_key,day_key,member_id,rank_type,Months,rank_date,rank_id,PRE_RANK_ID,rank_mark,sta_datetime,end_datetime,reason_code,create_date,create_by)
                    Values(V_NEXT_MO_KEY,V_NEXT_DAY_KEY,P_MEMBER_ID,'06',v_Months,v_RANK_DATE,v_TO_RANK_ID,v_OLD_RANK_ID,v_RANK_MARK,v_Sta_DateTime,null,'RANK_27',sysdate,'ADMIN');

                    --Cho vao lich su
                    Insert into dwb_action_audit(record_datetime,action_audit_id,member_id,action_id,chanel,user_name,ip_address,reason_code,table_name,member_key)
                    values(sysdate,SEQ_ACTION_AUDIT.NEXTVAL,P_MEMBER_ID,'06',v_CHANEL,'SYSTEM',v_IP_ADDRESS,'RANK_27','DWR_MEMBER',v_MEMBER_KEY);
                ElsIf(v_RANK_ORDER < v_OLD_RANK_ORDER) Then --Ha Hang
                    Update DWB_RANK_HISTORY Set End_datetime = v_End_DateTime, Rank_Date = v_Rank_Date
                    Where rowid = v_ROW_ID;

                    Update DWR_MEMBER Set   Rank_ID = v_TO_RANK_ID,
                                            Rank_date = ADD_MONTHS(v_Rank_Date,p_Rank_Months),
                                            Last_Rank_Date = Rank_Date
                    Where Member_ID = P_MEMBER_ID;

                    Insert into DWB_RANK_HISTORY(mo_key,day_key,member_id,rank_type,Months,rank_date,rank_id,rank_mark,sta_datetime,end_datetime,reason_code,create_date,create_by)
                    Values(V_NEXT_MO_KEY,V_NEXT_DAY_KEY,P_MEMBER_ID,'04',v_Months,v_RANK_DATE,v_TO_RANK_ID,v_Rank_Mark,v_Sta_DateTime,null,'RANK_05',sysdate,'ADMIN');

                    --Cho vao lich su
                    Insert into dwb_action_audit(record_datetime,action_audit_id,member_id,action_id,chanel,user_name,ip_address,reason_code,table_name,member_key)
                    values(sysdate,SEQ_ACTION_AUDIT.NEXTVAL,P_MEMBER_ID,'06',v_CHANEL,'SYSTEM',v_IP_ADDRESS,'RANK_05','DWR_MEMBER',v_MEMBER_KEY);
                End If;
                --dbms_output.put_line('D2:RANK_ORDER:' || v_RANK_ORDER || '|OLD_RANK_ORDER:' || v_OLD_RANK_ORDER || To_Char(sysdate,'dd/mm/yyyy hh24:mi:ss'));
                --Huy hoi vien neu khong du diem tich luy toi thieu
                If((v_Mob_Type in ('MG','MF') And v_Rank_Mark < v_Min_Rank_Mark_MG)
                    Or (v_Mob_Type = 'MC' And v_Rank_Mark < v_Min_Rank_Mark_MC) and v_ONNET_SUB = '1') Then

                    If(sysdate > p_Sum_Date) Then
                        v_Min_Rank_EndDate := sysdate;
                    Else
                        v_Min_Rank_EndDate := LAST_DAY(p_Sum_Date);
                    End if;

                    Update  DWR_MEMBER set Mem_Type= '02', Rank_ID = 985,
                            LAST_UPD_DATE=sysdate,LAST_UPD_BY='SYSTEM'
                    Where Member_ID = P_MEMBER_ID;
                    --Cho vao lich su
                    Insert into dwb_action_audit(record_datetime,action_audit_id,member_id,action_id,chanel,user_name,ip_address,reason_code,table_name,member_key)
                    values(sysdate,SEQ_ACTION_AUDIT.NEXTVAL,P_MEMBER_ID,'03',v_CHANEL,'SYSTEM',v_IP_ADDRESS,'DEATH_MARK','DWR_MEMBER',v_MEMBER_KEY);
                    --Cho vao blacklist
                    Insert into dwl_member_black_list(member_key,sta_datetime,end_datetime,black_list_type,member_id,mo_key)
                    values(v_MEMBER_KEY,trunc(v_Min_Rank_EndDate),ADD_MONTHS(trunc(v_Min_Rank_EndDate),v_Min_Rank_Mark_Months),'02',P_MEMBER_ID,To_Number(To_Char(v_Min_Rank_EndDate,'YYYYMM')));
                ElsIf(v_ONNET_SUB = '2') then
                    Update  DWR_MEMBER set status= '0', END_DATETIME = sysdate,
                            LAST_UPD_DATE=sysdate,LAST_UPD_BY='SYSTEM'
                    Where Member_ID = P_MEMBER_ID;    
                    --Cho vao lich su
                    Insert into dwb_action_audit(record_datetime,action_audit_id,member_id,action_id,chanel,user_name,ip_address,reason_code,table_name,member_key)
                    values(sysdate,SEQ_ACTION_AUDIT.NEXTVAL,P_MEMBER_ID,'03',v_CHANEL,'SYSTEM',v_IP_ADDRESS,'KHDN_04','DWR_MEMBER',v_MEMBER_KEY);                    
                End If;
            End If;

            --dbms_output.put_line('E:' || To_Char(sysdate,'dd/mm/yyyy hh24:mi:ss'));
        EXCEPTION
            WHEN OTHERS THEN
                Rollback;
                RAISE_APPLICATION_ERROR(-20084,SQLERRM);
        END; -- PROCEDURE


        FUNCTION FNC_GET_HOME_PARTNER RETURN NUMBER is
            Cursor c_Home_Partner Is
                Select Partner_ID
                From DWR_PARTNER
                Where PARTNER_TYPE='H';
            v_Value         Number;
        BEGIN
            Open c_Home_Partner;
            Fetch c_Home_Partner into v_Value;
                If c_Home_Partner%NOTFOUND Then
                    RAISE_APPLICATION_ERROR(-20085,'Khong tim thay tham so HOME_PARTNER tren DWR_PARTNER');
                End If;
            Close c_Home_Partner;

            Return v_Value;
        EXCEPTION
            WHEN OTHERS THEN
                If c_Home_Partner%isOpen Then Close c_Home_Partner; End If;
                RAISE_APPLICATION_ERROR(-20084,'FNC_GET_HOME_PARTNER:' || SQLERRM);
        END;

        FUNCTION GET_RANK_LIST(p_MEMBER_ID      NUMBER,
                               p_SUM_DATE       DATE) RETURN T_RANK_TAB PIPELINED AS
            --Thang Hang dac cach
            Cursor c_Rank_Vip_List(P_NEXT_RANK_ID   Number) Is
                Select  RV.MEMBER_ID, RV.MEMBER_KEY, RH.MOB_TYPE,RV.RANK_ID,RV.rank_order,0 RANK_MARK, RH.rank_id OLD_RANK_ID,  RV.CREATE_BY, RH.ROW_ID,
                        RH.OLD_RANK_ORDER, RV.STA_DATETIME, RH.RANK_DATE,RV.REASON_CODE,
                        MONTHS_BETWEEN(TRUNC(p_SUM_DATE,'MONTH'),TRUNC(RH.STA_DATETIME,'MONTH')) USE_MONTHS, ONNET_SUB
                From
                    (
                        Select x.member_key,x.Mob_Type,x.rank_date,x.member_id,x.rank_id,b.rank_order old_rank_order,x.sta_datetime,x.end_datetime, x.ROW_ID, x.ONNET_SUB
                        From (
                            Select c.member_key,c.Mob_Type ,c.rank_date,c.member_id,nvl(a.rank_id,c.rank_id) Rank_id,
                                   a.sta_datetime, a.end_datetime, a.rowid ROW_ID, c.ONNET_SUB
                            From dwr_member c
                            left join dwb_Rank_History a
                            on c.member_id = a.member_id                            
                            and p_SUM_DATE >= Trunc(a.STA_DATETIME,'MONTH')
                            and (p_SUM_DATE < a.END_DATETIME Or a.END_DATETIME Is Null)
                            Where c.member_id = p_MEMBER_ID
                            and c.status = '1'
                            and c.mem_type='01'
                            ) x, dwl_rank b
                        Where nvl(P_NEXT_RANK_ID,x.rank_id) = b.rank_id
                    ) RH,
                    (
                        Select a.member_id,a.member_key,a.rank_id,b.rank_order, a.CREATE_BY,
                               STA_DATETIME, a.REASON_CODE
                        From dwb_rank_vip a, dwl_rank b
                        Where a.rank_id = b.rank_id
                        and a.member_id = p_MEMBER_ID
                        and p_SUM_DATE >= Trunc(a.STA_DATETIME,'MONTH')
                        and (p_SUM_DATE < a.END_DATETIME Or a.END_DATETIME Is Null)
                    ) RV
                Where RV.RANK_ORDER >= RH.OLD_RANK_ORDER
                And RV.Member_ID = RH.Member_ID
                ORDER BY RH.END_DATETIME DESC NULLS FIRST;

            --Thang Hang theo hang doi tac
            Cursor c_Rank_Exchange_List(p_MO_KEY            Number,
                                        p_Partner_ID        Number,
                                        p_Home_Partner_ID   Number,
                                        p_Rank_Months       Number) Is
                SELECT *
                FROM (
                    SELECT  B.MEMBER_ID,A.MEMBER_KEY, B.MIN_MARK,B.RANK_EXC_ORDER,NVL(A.RANK_MARK,0) RANK_MARK, B.FROM_RANK_ID, B.TO_RANK_ID,B.PARTNER_ID,B.PARTNER_RANK_ID,
                            NVL(A.RANK_ID,C.RANK_ID)   OLD_RANK_ID, NVL(NVL(A.RANK_ORDER,C.OLD_RANK_ORDER),-1) OLD_RANK_ORDER,B.REASON_CODE,
                            MONTHS_BETWEEN(TRUNC(p_SUM_DATE,'MONTH'),TRUNC(C.STA_DATETIME,'MONTH')) USE_MONTHS,
                            A.RANK_DATE,C.STA_DATETIME, C.END_DATETIME, C.RANK_TYPE,
                            NVL(C.MEMBER_LINKS_ID,-1) OLD_MEMBER_LINKS_ID, B.NEW_MEMBER_LINKS_ID, C.ROWID ROW_ID,
                            ROW_NUMBER() OVER(PARTITION BY A.MEMBER_ID ORDER BY MIN_MARK DESC,STA_DATETIME DESC) AS ROW_NUMBER, B.ONNET_SUB
                    FROM (
                            Select  ML.MEMBER_ID, ML.RANK_ID PARTNER_RANK_ID, ML.PARTNER_ID, RE.MIN_MARK,RE.FROM_RANK_ID, RE.TO_RANK_ID,
                                    R1.RANK_ORDER RANK_EXC_ORDER, RE.REASON_CODE, ML.MEMBER_LINKS_ID NEW_MEMBER_LINKS_ID, M.ONNET_SUB
                            From DWR_MEMBER M, DWB_MEMBER_LINKS ML, DWL_RANK_EXCHANGE RE, DWL_RANK R1
                            Where TRUNC(p_SUM_DATE) >= TRUNC(ML.STA_DATETIME,'MONTH') AND ((TRUNC(p_SUM_DATE) < ML.END_DATETIME AND LAST_DAY(p_SUM_DATE) <= NVL(ML.FINISH_DATETIME,LAST_DAY(p_SUM_DATE))) Or ML.END_DATETIME Is Null)
                            AND TRUNC(p_SUM_DATE) >= RE.STA_DATETIME AND (TRUNC(p_SUM_DATE) < RE.END_DATETIME Or RE.END_DATETIME Is Null)
                            AND ML.partner_id = re.from_partner_id
                            AND RE.to_partner_id = p_Home_Partner_ID
                            AND ML.rank_id = RE.from_rank_id
                            AND RE.TO_RANK_ID = R1.RANK_ID
                            AND ML.PARTNER_ID = p_Partner_ID
                            AND ML.MEMBER_ID = p_MEMBER_ID
                            AND ML.MEMBER_ID = M.MEMBER_ID
                            AND M.STATUS = '1'
                            AND M.MEM_TYPE = '01'
                            ) B
                        LEFT OUTER JOIN (   SELECT MM.MEMBER_KEY, MH.MEMBER_ID, NVL(SUM_RANK_MARK,0) RANK_MARK, RANK_DATE,RANK_ID,RANK_ORDER
                                            FROM DWD_MARK_HISTORY MH, (
                                                                SELECT MEMBER_ID, STA_DATETIME,RANK_DATE,M.RANK_ID, R3.RANK_ORDER, MEMBER_KEY
                                                                FROM DWR_MEMBER M, DWL_RANK R3
                                                                WHERE M.RANK_ID = R3.RANK_ID
                                                                AND M.STATUS = '1'
                                                                AND MEM_TYPE = '01'
                                                                AND STA_DATETIME < LAST_DAY(p_SUM_DATE) + 1
                                                                AND M.MEMBER_ID = p_MEMBER_ID
                                                                ) MM
                                            WHERE MH.MEMBER_ID = MM.MEMBER_ID
                                            AND MH.MO_KEY = p_Mo_Key
                                            AND MH.BALANCE_TYPE = '01'
                                            AND MH.MEMBER_ID = p_MEMBER_ID
                                        ) A
                            ON B.MEMBER_ID = A.MEMBER_ID
                            --AND A.RANK_MARK > B.MIN_MARK
                        LEFT OUTER JOIN (SELECT *
                                         FROM (
                                         SELECT RH.*,R1.RANK_ORDER OLD_RANK_ORDER,
                                                ROW_NUMBER() OVER(PARTITION BY MEMBER_ID ORDER BY STA_DATETIME DESC,END_DATETIME DESC) AS ROW_NUMBER
                                         FROM DWB_RANK_HISTORY RH , DWL_RANK R1
                                         WHERE RH.RANK_ID = R1.RANK_ID
                                           AND TRUNC(p_SUM_DATE) >= Trunc(RH.STA_DATETIME,'MONTH') AND (TRUNC(p_SUM_DATE) < RH.END_DATETIME Or RH.END_DATETIME Is Null)
                                           AND RH.MEMBER_ID = p_MEMBER_ID
                                         ) WHERE ROW_NUMBER = 1) C
                            ON B.MEMBER_ID = C.MEMBER_ID
                ) WHERE RANK_EXC_ORDER >= OLD_RANK_ORDER
                AND OLD_MEMBER_LINKS_ID != NEW_MEMBER_LINKS_ID
                AND NVL(RANK_MARK,0) >= MIN_MARK;

            --Thang Hang theo diem
            Cursor c_Rank_Profile_List(p_Mo_Key         Number,
                                       p_Rank_Months   Number) Is
            SELECT *
            FROM (
                SELECT  A.MEMBER_ID,A.MEMBER_KEY, B.RANK_ID,B.RANK_ORDER,A.RANK_MARK, B.FROM_MARK, B.TO_MARK,B.MONTHS,B.ALERT_MARK,
                        NVL(A.RANK_ID,C.RANK_ID)   OLD_RANK_ID, NVL(NVL(A.RANK_ORDER,C.OLD_RANK_ORDER),-1) OLD_RANK_ORDER,
                        MONTHS_BETWEEN(TRUNC(p_SUM_DATE,'MONTH'),TRUNC(C.STA_DATETIME,'MONTH')) USE_MONTHS,
                        A.RANK_DATE,C.STA_DATETIME, C.END_DATETIME, C.ROWID ROW_ID, A.ONNET_SUB
                FROM (  SELECT MM.MEMBER_KEY, MH.MEMBER_ID, NVL(SUM_RANK_MARK,0) RANK_MARK, RANK_DATE,RANK_ID,RANK_ORDER,ONNET_SUB
                        FROM DWD_MARK_HISTORY MH, (
                                            SELECT MEMBER_ID, STA_DATETIME,RANK_DATE,M.RANK_ID, R3.RANK_ORDER, MEMBER_KEY, ONNET_SUB
                                            FROM DWR_MEMBER M, DWL_RANK R3
                                            WHERE M.RANK_ID = R3.RANK_ID
                                            AND M.STATUS = '1'
                                            AND MEM_TYPE = '01'
                                            AND STA_DATETIME < LAST_DAY(p_SUM_DATE) + 1
                                            AND M.MEMBER_ID = p_MEMBER_ID
                                            ) MM
                        WHERE MH.MEMBER_ID = MM.MEMBER_ID
                        AND MH.MO_KEY = p_Mo_Key
                        AND MH.BALANCE_TYPE = '01'
                        AND MH.MEMBER_ID = p_MEMBER_ID
                    ) A
                    LEFT OUTER JOIN (
                                    Select *
                                    From (
                                        SELECT RP.*,R2.RANK_ORDER RANK_ORDER, ROW_NUMBER() OVER(PARTITION BY RP.RANK_ID ORDER BY STA_DATETIME DESC) AS ROW_NUMBER
                                        FROM DWL_RANK_PROFILE RP , DWL_RANK R2
                                        WHERE RP.RANK_ID = R2.RANK_ID
                                        AND TRUNC(p_SUM_DATE) >= RP.STA_DATETIME AND (TRUNC(p_SUM_DATE) < RP.END_DATETIME Or RP.END_DATETIME Is Null)
                                    )
                                    Where ROW_NUMBER = 1
                                    ) B
                        ON A.RANK_MARK BETWEEN B.FROM_MARK AND B.TO_MARK
                    LEFT OUTER JOIN (SELECT *
                                     FROM (
                                     SELECT RH.*,R1.RANK_ORDER OLD_RANK_ORDER, ROW_NUMBER() OVER(PARTITION BY RH.MEMBER_ID ORDER BY STA_DATETIME DESC) AS ROW_NUMBER
                                     FROM DWB_RANK_HISTORY RH , DWL_RANK R1
                                     WHERE RH.RANK_ID = R1.RANK_ID
                                        AND TRUNC(p_SUM_DATE) >= Trunc(RH.STA_DATETIME,'MONTH')
                                        AND (TRUNC(p_SUM_DATE) < RH.END_DATETIME Or RH.END_DATETIME Is Null)
                                        AND RH.MEMBER_ID = p_MEMBER_ID
                                     ) WHERE ROW_NUMBER = 1) C
                        ON A.MEMBER_ID = C.MEMBER_ID
                )
            WHERE RANK_ORDER > OLD_RANK_ORDER;
            
            --Thang Hang theo diem lấy lùi lại 12 tháng
            Cursor c_Rank_Profile_List_12(p_Mo_Key         Number,
                                       p_Rank_Months   Number) Is
            SELECT *
            FROM (
                SELECT  A.MEMBER_ID,A.MEMBER_KEY, B.RANK_ID,B.RANK_ORDER,A.RANK_MARK, B.FROM_MARK, B.TO_MARK,B.MONTHS,B.ALERT_MARK,
                        NVL(A.RANK_ID,C.RANK_ID)   OLD_RANK_ID, NVL(NVL(A.RANK_ORDER,C.OLD_RANK_ORDER),-1) OLD_RANK_ORDER,
                        MONTHS_BETWEEN(TRUNC(p_SUM_DATE,'MONTH'),TRUNC(C.STA_DATETIME,'MONTH')) USE_MONTHS,
                        A.RANK_DATE,C.STA_DATETIME, C.END_DATETIME, C.ROWID ROW_ID, A.ONNET_SUB
                FROM (  SELECT MM.MEMBER_KEY, MH.MEMBER_ID, SUM(NVL(MARK,0)) RANK_MARK, RANK_DATE,RANK_ID,RANK_ORDER,ONNET_SUB
                        FROM DWD_MARK_HISTORY MH, (
                                            SELECT MEMBER_ID, STA_DATETIME,RANK_DATE,M.RANK_ID, R3.RANK_ORDER, MEMBER_KEY, ONNET_SUB
                                            FROM DWR_MEMBER M, DWL_RANK R3
                                            WHERE M.RANK_ID = R3.RANK_ID
                                            AND M.STATUS = '1'
                                            AND MEM_TYPE = '01'
                                            AND STA_DATETIME < LAST_DAY(p_SUM_DATE) + 1
                                            AND M.MEMBER_ID = p_MEMBER_ID
                                            ) MM
                        WHERE MH.MEMBER_ID = MM.MEMBER_ID
                        AND MH.MO_KEY >= TO_NUMBER(TO_CHAR(ADD_MONTHS(to_date(p_Mo_Key,'yyyyMM'),-p_Rank_Months),'YYYYMM'))
                        AND MH.BALANCE_TYPE = '01'
                        AND MH.MEMBER_ID = p_MEMBER_ID
                        GROUP BY MM.MEMBER_KEY, MH.MEMBER_ID, RANK_DATE,RANK_ID,RANK_ORDER, ONNET_SUB
                    ) A 
                    LEFT OUTER JOIN (
                                    Select *
                                    From (
                                        SELECT RP.*,R2.RANK_ORDER RANK_ORDER, ROW_NUMBER() OVER(PARTITION BY RP.RANK_ID ORDER BY STA_DATETIME DESC) AS ROW_NUMBER
                                        FROM DWL_RANK_PROFILE RP , DWL_RANK R2
                                        WHERE RP.RANK_ID = R2.RANK_ID
                                        AND TRUNC(p_SUM_DATE) >= RP.STA_DATETIME AND (TRUNC(p_SUM_DATE) < RP.END_DATETIME Or RP.END_DATETIME Is Null)
                                    )
                                    Where ROW_NUMBER = 1
                                    ) B
                        ON A.RANK_MARK BETWEEN B.FROM_MARK AND B.TO_MARK
                    LEFT OUTER JOIN (SELECT *
                                     FROM (
                                     SELECT RH.*,R1.RANK_ORDER OLD_RANK_ORDER, ROW_NUMBER() OVER(PARTITION BY RH.MEMBER_ID ORDER BY STA_DATETIME DESC) AS ROW_NUMBER
                                     FROM DWB_RANK_HISTORY RH , DWL_RANK R1
                                     WHERE RH.RANK_ID = R1.RANK_ID
                                        AND TRUNC(p_SUM_DATE) >= Trunc(RH.STA_DATETIME,'MONTH')
                                        AND (TRUNC(p_SUM_DATE) < RH.END_DATETIME Or RH.END_DATETIME Is Null)
                                        AND RH.MEMBER_ID = p_MEMBER_ID
                                     ) WHERE ROW_NUMBER = 1) C
                        ON A.MEMBER_ID = C.MEMBER_ID
                )
            WHERE RANK_ORDER > OLD_RANK_ORDER;            

/*            SELECT *
            FROM (
                SELECT  A.MEMBER_ID,A.MEMBER_KEY, B.RANK_ID,B.RANK_ORDER,A.RANK_MARK, B.FROM_MARK, B.TO_MARK,B.MONTHS,B.ALERT_MARK,
                        NVL(A.RANK_ID,C.RANK_ID)   OLD_RANK_ID, NVL(NVL(A.RANK_ORDER,C.OLD_RANK_ORDER),-1) OLD_RANK_ORDER,
                        MONTHS_BETWEEN(TRUNC(p_SUM_DATE,'MONTH'),TRUNC(C.STA_DATETIME,'MONTH')) USE_MONTHS,
                        A.RANK_DATE,C.STA_DATETIME, C.END_DATETIME, C.ROWID ROW_ID
                FROM (  SELECT MM.MEMBER_KEY, MH.MEMBER_ID, SUM(MARK) RANK_MARK, RANK_DATE,RANK_ID,RANK_ORDER
                        FROM DWD_MARK_HISTORY MH, (
                                            SELECT MEMBER_ID, STA_DATETIME, TO_NUMBER(TO_CHAR(ADD_MONTHS(NVL(RANK_DATE,STA_DATETIME),-p_Rank_Months),'YYYYMM')) MO_KEY,RANK_DATE,M.RANK_ID, R3.RANK_ORDER, MEMBER_KEY
                                            FROM DWR_MEMBER M, DWL_RANK R3
                                            WHERE M.RANK_ID = R3.RANK_ID
                                            AND M.STATUS = '1'
                                            AND MEM_TYPE = '01'
                                            AND STA_DATETIME < LAST_DAY(p_SUM_DATE) + 1
                                            AND M.MEMBER_ID = p_MEMBER_ID
                                            ) MM
                        WHERE MH.MEMBER_ID = MM.MEMBER_ID
                        AND MH.MO_KEY >= MM.MO_KEY
                        AND MH.MO_KEY < p_Mo_Key
                        AND MH.BALANCE_TYPE = '01'
                        AND MH.MEMBER_ID = p_MEMBER_ID
                        GROUP BY MH.MEMBER_ID, RANK_DATE,RANK_ID,RANK_ORDER,MEMBER_KEY
                    ) A
                    LEFT OUTER JOIN (
                                    Select *
                                    From (
                                        SELECT RP.*,R2.RANK_ORDER RANK_ORDER, ROW_NUMBER() OVER(PARTITION BY RP.RANK_ID ORDER BY STA_DATETIME DESC) AS ROW_NUMBER
                                        FROM DWL_RANK_PROFILE RP , DWL_RANK R2
                                        WHERE RP.RANK_ID = R2.RANK_ID
                                        AND TRUNC(p_SUM_DATE) >= RP.STA_DATETIME AND (TRUNC(p_SUM_DATE) < RP.END_DATETIME Or RP.END_DATETIME Is Null)
                                    )
                                    Where ROW_NUMBER = 1
                                    ) B
                        ON A.RANK_MARK BETWEEN B.FROM_MARK AND B.TO_MARK
                    LEFT OUTER JOIN (SELECT *
                                     FROM (
                                     SELECT RH.*,R1.RANK_ORDER OLD_RANK_ORDER, ROW_NUMBER() OVER(PARTITION BY RH.MEMBER_ID ORDER BY STA_DATETIME DESC) AS ROW_NUMBER
                                     FROM DWB_RANK_HISTORY RH , DWL_RANK R1
                                     WHERE RH.RANK_ID = R1.RANK_ID
                                        AND TRUNC(p_SUM_DATE) >= Trunc(RH.STA_DATETIME,'MONTH')
                                        AND (TRUNC(p_SUM_DATE) < RH.END_DATETIME Or RH.END_DATETIME Is Null)
                                        AND RH.MEMBER_ID = p_MEMBER_ID
                                     ) WHERE ROW_NUMBER = 1) C
                        ON A.MEMBER_ID = C.MEMBER_ID
                )
            WHERE RANK_ORDER > OLD_RANK_ORDER;*/

            --Ha Hang theo chu ky
            Cursor c_Rank_Data(p_Bill_Cycle     Date,
                               p_Rank_Months    Number,
                               p_Mo_Key         Number) Is
                SELECT  A.MEMBER_KEY,A.MEMBER_ID, B.RANK_ID,B.RANK_ORDER,A.RANK_MARK, B.FROM_MARK, B.TO_MARK,B.MONTHS,B.ALERT_MARK,
                        NVL(A.RANK_ID,C.RANK_ID)   OLD_RANK_ID, NVL(NVL(A.RANK_ORDER,C.OLD_RANK_ORDER),-1) OLD_RANK_ORDER,
                        MONTHS_BETWEEN(TRUNC(p_SUM_DATE,'MONTH'),TRUNC(C.STA_DATETIME,'MONTH')) USE_MONTHS,
                        A.MOB_TYPE,A.RANK_DATE,C.STA_DATETIME, C.END_DATETIME, C.ROWID ROW_ID, A.ONNET_SUB
                FROM (  
                        SELECT  M.MEMBER_ID,M.MOB_TYPE, STA_DATETIME, NVL(SUM_RANK_MARK,0) RANK_MARK, 
                                RANK_DATE,M.RANK_ID, R3.RANK_ORDER, M.MEMBER_KEY, M.ONNET_SUB
                        FROM DWR_MEMBER M
                        INNER JOIN DWL_RANK R3
                            ON M.RANK_ID = R3.RANK_ID
                        LEFT OUTER JOIN DWD_MARK_HISTORY MH
                            ON M.MEMBER_ID = MH.MEMBER_ID
                            AND MH.MO_KEY = p_Mo_Key
                            AND MH.BALANCE_TYPE = '01'
                        WHERE M.STATUS = '1'
                          AND M.MEM_TYPE = '01'
                          AND M.MEMBER_ID = P_MEMBER_ID
                          AND M.RANK_DATE = p_Bill_Cycle
                          AND M.STA_DATETIME < ADD_MONTHS(p_Bill_Cycle,1)
                          AND ((MONTHS_BETWEEN(M.RANK_DATE,M.LAST_RANK_DATE) = 0) OR (MONTHS_BETWEEN(M.RANK_DATE,M.LAST_RANK_DATE) = p_Rank_Months))
                    ) A
                    LEFT OUTER JOIN (
                                    SELECT RP.*,R2.RANK_ORDER RANK_ORDER,ROW_NUMBER() OVER(PARTITION BY RP.RANK_ID ORDER BY STA_DATETIME DESC) AS ROW_NUMBER
                                    FROM DWL_RANK_PROFILE RP , DWL_RANK R2
                                    WHERE RP.RANK_ID = R2.RANK_ID
                                    AND TRUNC(p_Bill_Cycle) >= RP.STA_DATETIME AND (TRUNC(p_Bill_Cycle) < RP.END_DATETIME Or RP.END_DATETIME Is Null)
                                    ) B
                        ON A.RANK_MARK BETWEEN B.FROM_MARK AND B.TO_MARK
                        AND ROW_NUMBER = 1
                    LEFT OUTER JOIN (SELECT *
                                     FROM (
                                     SELECT RH.*,R1.RANK_ORDER OLD_RANK_ORDER, ROW_NUMBER() OVER(PARTITION BY MEMBER_ID ORDER BY STA_DATETIME DESC) AS ROW_NUMBER
                                     FROM DWB_RANK_HISTORY RH , DWL_RANK R1
                                     WHERE RH.RANK_ID = R1.RANK_ID
                                       AND TRUNC(p_Bill_Cycle) >= Trunc(RH.STA_DATETIME,'MONTH') AND (TRUNC(p_Bill_Cycle) < RH.END_DATETIME Or RH.END_DATETIME Is Null)
                                       AND RH.MEMBER_ID = P_MEMBER_ID
                                     ) WHERE ROW_NUMBER = 1) C
                        ON A.MEMBER_ID = C.MEMBER_ID;
            --Lay lui lai 12 thang            
            Cursor c_Rank_Data_12(p_Bill_Cycle     Date,
                               p_Rank_Months    Number,
                               p_Mo_Key         Number) Is
                SELECT  A.MEMBER_KEY,A.MEMBER_ID, B.RANK_ID,B.RANK_ORDER,A.RANK_MARK, B.FROM_MARK, B.TO_MARK,B.MONTHS,B.ALERT_MARK,
                        NVL(A.RANK_ID,C.RANK_ID)   OLD_RANK_ID, NVL(NVL(A.RANK_ORDER,C.OLD_RANK_ORDER),-1) OLD_RANK_ORDER,
                        MONTHS_BETWEEN(TRUNC(p_SUM_DATE,'MONTH'),TRUNC(C.STA_DATETIME,'MONTH')) USE_MONTHS,
                        A.MOB_TYPE,A.RANK_DATE,C.STA_DATETIME, C.END_DATETIME, C.ROWID ROW_ID, A.ONNET_SUB
                FROM (  
                        SELECT  M.MEMBER_ID,M.MOB_TYPE, STA_DATETIME, SUM(NVL(MARK,0)) RANK_MARK, 
                                RANK_DATE,M.RANK_ID, R3.RANK_ORDER, M.MEMBER_KEY, M.ONNET_SUB
                        FROM DWR_MEMBER M
                        INNER JOIN DWL_RANK R3
                            ON M.RANK_ID = R3.RANK_ID
                        LEFT OUTER JOIN DWD_MARK_HISTORY MH
                            ON M.MEMBER_ID = MH.MEMBER_ID
                            AND MH.MO_KEY >= TO_NUMBER(TO_CHAR(ADD_MONTHS(to_date(p_Mo_Key,'yyyyMM'),-p_Rank_Months),'YYYYMM'))
                            AND MH.BALANCE_TYPE = '01'
                        WHERE M.STATUS = '1'
                          AND M.MEM_TYPE = '01'
                          AND M.MEMBER_ID = P_MEMBER_ID
                          AND M.RANK_DATE = p_Bill_Cycle
                          AND M.STA_DATETIME < ADD_MONTHS(p_Bill_Cycle,1)
                          AND ((MONTHS_BETWEEN(M.RANK_DATE,M.LAST_RANK_DATE) <> 0) AND (MONTHS_BETWEEN(M.RANK_DATE,M.LAST_RANK_DATE) < p_Rank_Months))
                        GROUP BY  M.MEMBER_ID,M.MOB_TYPE, M.STA_DATETIME, M.RANK_DATE, M.RANK_ID,RANK_ORDER, M.MEMBER_KEY, M.ONNET_SUB
                    ) A
                    LEFT OUTER JOIN (
                                    SELECT RP.*,R2.RANK_ORDER RANK_ORDER,ROW_NUMBER() OVER(PARTITION BY RP.RANK_ID ORDER BY STA_DATETIME DESC) AS ROW_NUMBER
                                    FROM DWL_RANK_PROFILE RP , DWL_RANK R2
                                    WHERE RP.RANK_ID = R2.RANK_ID
                                    AND TRUNC(p_Bill_Cycle) >= RP.STA_DATETIME AND (TRUNC(p_Bill_Cycle) < RP.END_DATETIME Or RP.END_DATETIME Is Null)
                                    ) B
                        ON A.RANK_MARK BETWEEN B.FROM_MARK AND B.TO_MARK
                        AND ROW_NUMBER = 1
                    LEFT OUTER JOIN (SELECT *
                                     FROM (
                                     SELECT RH.*,R1.RANK_ORDER OLD_RANK_ORDER, ROW_NUMBER() OVER(PARTITION BY MEMBER_ID ORDER BY STA_DATETIME DESC) AS ROW_NUMBER
                                     FROM DWB_RANK_HISTORY RH , DWL_RANK R1
                                     WHERE RH.RANK_ID = R1.RANK_ID
                                       AND TRUNC(p_Bill_Cycle) >= Trunc(RH.STA_DATETIME,'MONTH') AND (TRUNC(p_Bill_Cycle) < RH.END_DATETIME Or RH.END_DATETIME Is Null)
                                       AND RH.MEMBER_ID = P_MEMBER_ID
                                     ) WHERE ROW_NUMBER = 1) C
                        ON A.MEMBER_ID = C.MEMBER_ID;                        

                /*SELECT  A.MEMBER_KEY,A.MEMBER_ID, B.RANK_ID,B.RANK_ORDER,A.RANK_MARK, B.FROM_MARK, B.TO_MARK,B.MONTHS,B.ALERT_MARK,
                        NVL(A.RANK_ID,C.RANK_ID)   OLD_RANK_ID, NVL(NVL(A.RANK_ORDER,C.OLD_RANK_ORDER),-1) OLD_RANK_ORDER,
                        MONTHS_BETWEEN(TRUNC(p_SUM_DATE,'MONTH'),TRUNC(C.STA_DATETIME,'MONTH')) USE_MONTHS,
                        A.MOB_TYPE,A.RANK_DATE,C.STA_DATETIME, C.END_DATETIME, C.ROWID ROW_ID
                FROM (  Select MEMBER_ID,MOB_TYPE, SUM(MARK) RANK_MARK, RANK_DATE,RANK_ID, RANK_ORDER, MEMBER_KEY
                        FROM (
                                SELECT  M.MEMBER_ID,M.MOB_TYPE, STA_DATETIME, NVL(MARK,0) MARK, RANK_DATE,M.RANK_ID, R3.RANK_ORDER, M.MEMBER_KEY
                                FROM DWR_MEMBER M
                                INNER JOIN DWL_RANK R3
                                    ON M.RANK_ID = R3.RANK_ID
                                LEFT OUTER JOIN DWD_MARK_HISTORY MH
                                    ON M.MEMBER_ID = MH.MEMBER_ID
                                    AND MH.MO_KEY >= TO_NUMBER(TO_CHAR(ADD_MONTHS(NVL(RANK_DATE,STA_DATETIME),-p_Rank_Months),'YYYYMM'))
                                    AND MH.MO_KEY < p_Mo_Key
                                    AND MH.BALANCE_TYPE = '01'
                                WHERE M.STATUS = '1'
                                  AND M.MEM_TYPE = '01'
                                  AND M.MEMBER_ID = P_MEMBER_ID
                                  AND M.RANK_DATE = p_Bill_Cycle
                                  AND M.STA_DATETIME < ADD_MONTHS(p_Bill_Cycle,1)
                        ) GROUP BY MEMBER_ID,MOB_TYPE, RANK_DATE, RANK_ID, RANK_ORDER, MEMBER_KEY
                    ) A
                    LEFT OUTER JOIN (
                                    SELECT RP.*,R2.RANK_ORDER RANK_ORDER,ROW_NUMBER() OVER(PARTITION BY RP.RANK_ID ORDER BY STA_DATETIME DESC) AS ROW_NUMBER
                                    FROM DWL_RANK_PROFILE RP , DWL_RANK R2
                                    WHERE RP.RANK_ID = R2.RANK_ID
                                    AND TRUNC(p_Bill_Cycle) >= RP.STA_DATETIME AND (TRUNC(p_Bill_Cycle) < RP.END_DATETIME Or RP.END_DATETIME Is Null)
                                    ) B
                        ON A.RANK_MARK BETWEEN B.FROM_MARK AND B.TO_MARK
                        AND ROW_NUMBER = 1
                    LEFT OUTER JOIN (SELECT *
                                     FROM (
                                     SELECT RH.*,R1.RANK_ORDER OLD_RANK_ORDER, ROW_NUMBER() OVER(PARTITION BY MEMBER_ID ORDER BY STA_DATETIME DESC) AS ROW_NUMBER
                                     FROM DWB_RANK_HISTORY RH , DWL_RANK R1
                                     WHERE RH.RANK_ID = R1.RANK_ID
                                       AND TRUNC(p_Bill_Cycle) >= Trunc(RH.STA_DATETIME,'MONTH') AND (TRUNC(p_Bill_Cycle) < RH.END_DATETIME Or RH.END_DATETIME Is Null)
                                       AND RH.MEMBER_ID = P_MEMBER_ID
                                     ) WHERE ROW_NUMBER = 1) C
                        ON A.MEMBER_ID = C.MEMBER_ID;*/

            l_rank_row  T_RANK_ROW:= T_RANK_ROW(NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
                                                NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
                                                NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);

            Cursor c_Last_Mo_Key(v_Member_ID        Number) Is
            Select  To_Number(To_Char(BILL_CYCLE,'YYYYMM')) Cur_Mo_Key
            From (
                Select BILL_CYCLE
                From (
                    Select *
                    From dwd_mark_history
                    Where member_id=v_Member_ID
                    Order by BILL_CYCLE desc
                ) Where rownum = 1
            );

            Cursor c_Month_Between_Rank_Date(v_Member_ID        Number) Is
            Select MONTHS_BETWEEN(M.RANK_DATE,M.LAST_RANK_DATE) MONTH_CNT from dwr_member M where member_id = v_Member_ID;
            
            V_MO_KEY            NUMBER:=TO_NUMBER(TO_CHAR(P_SUM_DATE,'YYYYMM'));
            P_GLP_PARTNER_ID    NUMBER:=285;
            v_Home_Partner_ID   Number;
            v_Rank_Months       Number;

            v_NEXT_RANK_ID      NUMBER;
            v_Month_Cnt         NUMBER;  
        BEGIN
            v_Rank_Months     := FNC_GET_RANK_MONTHS();
            v_Home_Partner_ID := FNC_GET_HOME_PARTNER();

            Open c_Last_Mo_Key(p_MEMBER_ID);
            Fetch c_Last_Mo_Key Into v_MO_KEY;
            Close c_Last_Mo_Key;
            
            Open c_Month_Between_Rank_Date(p_MEMBER_ID);
            Fetch c_Month_Between_Rank_Date Into v_Month_Cnt;
            Close c_Month_Between_Rank_Date;
            
            --Ha Hang theo chu ky
            IF(v_Month_Cnt < 12) Then
                For v_Data in c_Rank_Data_12(p_SUM_DATE,v_Rank_Months,V_MO_KEY) Loop
                    IF(v_Data.RANK_ORDER < v_Data.OLD_RANK_ORDER) Then
                        v_NEXT_RANK_ID := v_Data.RANK_ID;
                    END IF;
    
                    l_rank_row.ORDER_ID := 4;
                    l_rank_row.RANK_CODE := 'RANK_DOWN';
                    l_rank_row.MEMBER_ID := v_Data.Member_ID;
                    l_rank_row.MEMBER_KEY := v_Data.MEMBER_KEY;
                    l_rank_row.RANK_ORDER := v_Data.RANK_ORDER;
                    l_rank_row.RANK_MARK := v_Data.RANK_MARK;
                    l_rank_row.TO_RANK_ID := v_Data.RANK_ID;
                    l_rank_row.MOB_TYPE := v_Data.MOB_TYPE;
    
                    l_rank_row.FROM_MARK := v_Data.FROM_MARK;
                    l_rank_row.TO_MARK := v_Data.TO_MARK;
                    l_rank_row.MONTHS := v_Data.MONTHS;
                    l_rank_row.ALERT_MARK := v_Data.ALERT_MARK;
    
                    l_rank_row.OLD_RANK_ID := v_Data.OLD_RANK_ID;
                    l_rank_row.OLD_RANK_ORDER := v_Data.OLD_RANK_ORDER;
                    l_rank_row.RANK_DATE := v_Data.RANK_DATE;
                    l_rank_row.USE_MONTHS := v_Data.USE_MONTHS;
                    l_rank_row.STA_DATETIME := v_Data.STA_DATETIME;
                    l_rank_row.END_DATETIME := v_Data.END_DATETIME;
    
                    l_rank_row.ROW_ID := v_Data.ROW_ID;
                    l_rank_row.ONNET_SUB := v_Data.ONNET_SUB;
                    PIPE ROW (l_rank_row);
                End Loop;                        
            ELSE
                For v_Data in c_Rank_Data(p_SUM_DATE,v_Rank_Months,V_MO_KEY) Loop
                    IF(v_Data.RANK_ORDER < v_Data.OLD_RANK_ORDER) Then
                        v_NEXT_RANK_ID := v_Data.RANK_ID;
                    END IF;
    
                    l_rank_row.ORDER_ID := 4;
                    l_rank_row.RANK_CODE := 'RANK_DOWN';
                    l_rank_row.MEMBER_ID := v_Data.Member_ID;
                    l_rank_row.MEMBER_KEY := v_Data.MEMBER_KEY;
                    l_rank_row.RANK_ORDER := v_Data.RANK_ORDER;
                    l_rank_row.RANK_MARK := v_Data.RANK_MARK;
                    l_rank_row.TO_RANK_ID := v_Data.RANK_ID;
                    l_rank_row.MOB_TYPE := v_Data.MOB_TYPE;
    
                    l_rank_row.FROM_MARK := v_Data.FROM_MARK;
                    l_rank_row.TO_MARK := v_Data.TO_MARK;
                    l_rank_row.MONTHS := v_Data.MONTHS;
                    l_rank_row.ALERT_MARK := v_Data.ALERT_MARK;
    
                    l_rank_row.OLD_RANK_ID := v_Data.OLD_RANK_ID;
                    l_rank_row.OLD_RANK_ORDER := v_Data.OLD_RANK_ORDER;
                    l_rank_row.RANK_DATE := v_Data.RANK_DATE;
                    l_rank_row.USE_MONTHS := v_Data.USE_MONTHS;
                    l_rank_row.STA_DATETIME := v_Data.STA_DATETIME;
                    l_rank_row.END_DATETIME := v_Data.END_DATETIME;
    
                    l_rank_row.ROW_ID := v_Data.ROW_ID;
                    l_rank_row.ONNET_SUB := v_Data.ONNET_SUB;
                    PIPE ROW (l_rank_row);
                End Loop;            
            END IF;


            --Thang Hang dac cach
            l_rank_row := T_RANK_ROW(NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
                                     NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
                                     NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);

            For v_Data in c_Rank_Vip_List(v_NEXT_RANK_ID) Loop
                l_rank_row.ORDER_ID := 1;
                l_rank_row.RANK_CODE := 'VIP';
                l_rank_row.MEMBER_ID := v_Data.Member_ID;
                l_rank_row.MEMBER_KEY := v_Data.MEMBER_KEY;
                l_rank_row.TO_RANK_ID := v_Data.RANK_ID;
                l_rank_row.RANK_ORDER := v_Data.RANK_ORDER;
                l_rank_row.RANK_MARK := v_Data.RANK_MARK;
                l_rank_row.OLD_RANK_ID := v_Data.OLD_RANK_ID;
                l_rank_row.OLD_RANK_ORDER := v_Data.OLD_RANK_ORDER;
                l_rank_row.RANK_DATE := v_Data.RANK_DATE;
                l_rank_row.REASON_CODE := v_Data.REASON_CODE;
                l_rank_row.USE_MONTHS := v_Data.USE_MONTHS;
                l_rank_row.STA_DATETIME := v_Data.STA_DATETIME;
                l_rank_row.CREATE_BY := v_Data.CREATE_BY;
                l_rank_row.ROW_ID := v_Data.ROW_ID;
                l_rank_row.ONNET_SUB := v_Data.ONNET_SUB;
                PIPE ROW (l_rank_row);
            End Loop;

            --Thang Hang theo hang doi tac
            l_rank_row := T_RANK_ROW(NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
                                            NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
                                            NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
            For v_Data in c_Rank_Exchange_List(V_MO_KEY,P_GLP_PARTNER_ID,v_Home_Partner_ID,v_Rank_Months) Loop
                l_rank_row.ORDER_ID := 2;
                l_rank_row.RANK_CODE := 'EXCHANGE';
                l_rank_row.MEMBER_ID := v_Data.Member_ID;
                l_rank_row.MEMBER_KEY := v_Data.MEMBER_KEY;
                l_rank_row.MIN_MARK := v_Data.MIN_MARK;
                l_rank_row.RANK_ORDER := v_Data.RANK_EXC_ORDER;
                l_rank_row.RANK_MARK := v_Data.RANK_MARK;

                l_rank_row.FROM_RANK_ID := v_Data.FROM_RANK_ID;
                l_rank_row.TO_RANK_ID := v_Data.TO_RANK_ID;
                l_rank_row.PARTNER_ID := v_Data.PARTNER_ID;
                l_rank_row.PARTNER_RANK_ID := v_Data.PARTNER_RANK_ID;

                l_rank_row.OLD_RANK_ID := v_Data.OLD_RANK_ID;
                l_rank_row.OLD_RANK_ORDER := v_Data.OLD_RANK_ORDER;
                l_rank_row.RANK_DATE := v_Data.RANK_DATE;
                l_rank_row.RANK_TYPE := v_Data.RANK_TYPE;
                l_rank_row.REASON_CODE := v_Data.REASON_CODE;
                l_rank_row.USE_MONTHS := v_Data.USE_MONTHS;
                l_rank_row.STA_DATETIME := v_Data.STA_DATETIME;
                l_rank_row.END_DATETIME := v_Data.END_DATETIME;

                l_rank_row.OLD_MEMBER_LINK_ID := v_Data.OLD_MEMBER_LINKS_ID;
                l_rank_row.NEW_MEMBER_LINK_ID := v_Data.NEW_MEMBER_LINKS_ID;

                l_rank_row.ROW_ID := v_Data.ROW_ID;
                l_rank_row.ONNET_SUB := v_Data.ONNET_SUB;
                PIPE ROW (l_rank_row);
            End Loop;

            --Thang Hang theo diem
            l_rank_row := T_RANK_ROW(NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
                                     NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
                                     NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
                                     
            IF(v_Month_Cnt < 12) Then
                For v_Data in c_Rank_Profile_List_12(V_MO_KEY,v_Rank_Months) Loop
                    l_rank_row.ORDER_ID := 3;
                    l_rank_row.RANK_CODE := 'PROFILE';
                    l_rank_row.MEMBER_ID := v_Data.Member_ID;
                    l_rank_row.MEMBER_KEY := v_Data.MEMBER_KEY;
                    l_rank_row.RANK_ORDER := v_Data.RANK_ORDER;
                    l_rank_row.RANK_MARK := v_Data.RANK_MARK;
                    l_rank_row.TO_RANK_ID := v_Data.RANK_ID;
    
                    l_rank_row.FROM_MARK := v_Data.FROM_MARK;
                    l_rank_row.TO_MARK := v_Data.TO_MARK;
                    l_rank_row.MONTHS := v_Data.MONTHS;
                    l_rank_row.ALERT_MARK := v_Data.ALERT_MARK;
    
                    l_rank_row.OLD_RANK_ID := v_Data.OLD_RANK_ID;
                    l_rank_row.OLD_RANK_ORDER := v_Data.OLD_RANK_ORDER;
                    l_rank_row.RANK_DATE := v_Data.RANK_DATE;
                    l_rank_row.USE_MONTHS := v_Data.USE_MONTHS;
                    l_rank_row.STA_DATETIME := v_Data.STA_DATETIME;
                    l_rank_row.END_DATETIME := v_Data.END_DATETIME;
    
                    l_rank_row.ROW_ID := v_Data.ROW_ID;
                    l_rank_row.ONNET_SUB := v_Data.ONNET_SUB;
                    PIPE ROW (l_rank_row);
                End Loop;                
            ELSE
                For v_Data in c_Rank_Profile_List(V_MO_KEY,v_Rank_Months) Loop
                    l_rank_row.ORDER_ID := 3;
                    l_rank_row.RANK_CODE := 'PROFILE';
                    l_rank_row.MEMBER_ID := v_Data.Member_ID;
                    l_rank_row.MEMBER_KEY := v_Data.MEMBER_KEY;
                    l_rank_row.RANK_ORDER := v_Data.RANK_ORDER;
                    l_rank_row.RANK_MARK := v_Data.RANK_MARK;
                    l_rank_row.TO_RANK_ID := v_Data.RANK_ID;
    
                    l_rank_row.FROM_MARK := v_Data.FROM_MARK;
                    l_rank_row.TO_MARK := v_Data.TO_MARK;
                    l_rank_row.MONTHS := v_Data.MONTHS;
                    l_rank_row.ALERT_MARK := v_Data.ALERT_MARK;
    
                    l_rank_row.OLD_RANK_ID := v_Data.OLD_RANK_ID;
                    l_rank_row.OLD_RANK_ORDER := v_Data.OLD_RANK_ORDER;
                    l_rank_row.RANK_DATE := v_Data.RANK_DATE;
                    l_rank_row.USE_MONTHS := v_Data.USE_MONTHS;
                    l_rank_row.STA_DATETIME := v_Data.STA_DATETIME;
                    l_rank_row.END_DATETIME := v_Data.END_DATETIME;
    
                    l_rank_row.ROW_ID := v_Data.ROW_ID;
                    l_rank_row.ONNET_SUB := v_Data.ONNET_SUB;
                    PIPE ROW (l_rank_row);
                End Loop;            
            END IF;

            RETURN;
        END;

        PROCEDURE INS_RANK_LIST(P_SUM_DATE       DATE) AS

            --LOG
            V_ETL_STARTIME         DATE := SYSDATE;
            V_ETL_PROCESSOR_NAME   VARCHAR2 (100) := 'INS_RANK_LIST';
            V_ETL_TASK_NAME        VARCHAR2 (100) := 'INS_RANK_LIST';
            V_SOURCE_OBJECT_NAME   VARCHAR2 (100) := 'MBF_KNDL_NEW.DWD_MARK_HISTORY_DTL';
            V_TARGET_TABLE         VARCHAR2 (100) := 'MBF_KNDL_NEW.DWB_RANK_HISTORY_TMP';
            V_SOURCE_RECORD_CNT    NUMBER (20)    := 0;
            V_TARGET_RECORD_CNT    NUMBER (20)    := 0;
            V_TASK_STARTTIME       DATE;
            V_TASK_ENDTIME         DATE;
            V_TASK_STATUS_CD       VARCHAR2 (100) := 'SUCCESS';
            V_STATUS               VARCHAR2 (100) := '5';
            V_TASK_DRTN            NUMBER(15);
            V_ETL_ENDTIME          DATE;
            V_DATA_PROCESS_DATE    NUMBER  := TO_NUMBER(TO_CHAR(P_SUM_DATE,'YYYYMMDD'));
            ---------------------------------------------

            P_GLP_PARTNER_ID    NUMBER:=285;
            v_Home_Partner_ID   Number;
            V_MO_KEY            NUMBER:=TO_NUMBER(TO_CHAR(ADD_MONTHS(P_SUM_DATE,-1),'YYYYMM'));
            p_Rank_Months       Number;
        BEGIN
            p_Rank_Months       := FNC_GET_RANK_MONTHS();
            v_Home_Partner_ID   := FNC_GET_HOME_PARTNER();            
            --TRUNCATE_TABLE('DWB_RANK_HISTORY_TMP');
            DELETE FROM DWB_RANK_HISTORY_TMP;
            PRC_RANK_VIP_HISTORY(P_SUM_DATE);
            --------------------------------------------------------------------
            --TASK#01
            V_ETL_PROCESSOR_NAME    := 'INS_RANK_LIST';      
            V_TASK_STARTTIME	      := SYSDATE;
            V_ETL_TASK_NAME		      := 'INS_RANK_LIST#RANK_VIP';
            V_SOURCE_OBJECT_NAME	  := 'DWB_RANK_VIP';
            V_TARGET_TABLE		      := 'DWB_RANK_HISTORY_TMP';
            V_TARGET_RECORD_CNT	    := 0;

            INSERT INTO DWB_RANK_HISTORY_TMP(ORDER_ID,RANK_CODE,MEMBER_ID, MEMBER_KEY, TO_RANK_ID, RANK_ORDER, RANK_MARK, OLD_RANK_ID, CREATE_BY, ROW_ID, OLD_RANK_ORDER, STA_DATETIME, RANK_DATE, REASON_CODE,USE_MONTHS,ONNET_SUB)
            SELECT  /*+ PARALLEL(RH,4) */ 1 ORDER_ID, 'VIP' RANK_CODE, RV.MEMBER_ID, RV.MEMBER_KEY,RV.RANK_ID,RV.rank_order,0 RANK_MARK, RH.rank_id OLD_RANK_ID,  RV.CREATE_BY, RH.ROW_ID,
                    RH.OLD_RANK_ORDER, RV.STA_DATETIME, RH.RANK_DATE,RV.REASON_CODE,
                    MONTHS_BETWEEN(TRUNC(p_SUM_DATE,'MONTH'),TRUNC(RH.STA_DATETIME,'MONTH')) USE_MONTHS, ONNET_SUB
            FROM
                (
                    Select x.member_key,x.rank_date,x.member_id,x.rank_id Rank_id,b.rank_order old_rank_order,x.sta_datetime,x.end_datetime, x.ROW_ID, x.ONNET_SUB
                    From (
                        Select c.member_key,c.rank_date,c.member_id,nvl(a.rank_id,c.rank_id) Rank_id,
                               a.sta_datetime, a.end_datetime, a.rowid ROW_ID, c.ONNET_SUB
                        From dwr_member c
                        left join dwb_Rank_History a
                        on c.member_id = a.member_id
                        and p_SUM_DATE >= Trunc(a.STA_DATETIME,'MONTH')
                        and (p_SUM_DATE < a.END_DATETIME Or a.END_DATETIME Is Null)
                        Where c.status = '1'
                        And Mem_Type = '01'
                        ) x, dwl_rank b
                    Where x.rank_id = b.rank_id
                ) RH,
                (
                    Select a.member_id,a.member_key,a.rank_id,b.rank_order, a.CREATE_BY,
                           Trunc(a.STA_DATETIME,'MONTH') STA_DATETIME, a.REASON_CODE,
                           ROW_NUMBER() OVER( PARTITION BY a.member_key ORDER BY a.member_id,a.create_date DESC ) order_rank
                    From dwb_rank_vip a, dwl_rank b
                    Where a.rank_id = b.rank_id
                    and p_SUM_DATE >= Trunc(a.STA_DATETIME,'MONTH')
                    and (p_SUM_DATE < a.END_DATETIME Or a.END_DATETIME Is Null)
                ) RV
            WHERE RV.RANK_ORDER >= RH.OLD_RANK_ORDER
            And RV.Member_ID = RH.Member_ID
            AND RV.order_rank=1
            ORDER BY RH.END_DATETIME DESC NULLS FIRST;

            V_TARGET_RECORD_CNT :=  SQL%ROWCOUNT;
            V_TASK_DRTN := ROUND((SYSDATE - V_TASK_STARTTIME)*24*60*60,0);
            V_TASK_ENDTIME := SYSDATE;
            V_ETL_ENDTIME := SYSDATE;
            /*INSERT STG_ETL_LOG*/
            PCK_LOG.INSERT_STG_ETL_LOG
            (
                V_ETL_STARTIME,
                V_ETL_SERVER_NAME,V_ETL_PROCESSOR_NAME,
                V_ETL_TASK_NAME,V_SOURCE_SYSTEM_CD,
                V_SOURCE_OBJECT_TYPE,
                V_SOURCE_OBJECT_NAME,V_TARGET_TABLE,V_SOURCE_RECORD_CNT,
                V_TARGET_RECORD_CNT,V_TASK_STARTTIME,V_TASK_ENDTIME,
                V_TASK_STATUS_CD,
                V_STATUS,V_TASK_DRTN,V_ETL_ENDTIME,V_ETL_USERNAME,V_DATA_PROCESS_DATE
            );
            --------------------------------------------------------------------
            --TASK#02
            V_ETL_PROCESSOR_NAME    := 'INS_RANK_LIST';      
            V_TASK_STARTTIME	      := SYSDATE;
            V_ETL_TASK_NAME		      := 'INS_RANK_LIST#RANK_EXCHANGE';
            V_SOURCE_OBJECT_NAME	  := 'DWB_MEMBER_LINKS';
            V_TARGET_TABLE		      := 'DWB_RANK_HISTORY_TMP';
            V_TARGET_RECORD_CNT	    := 0;

            INSERT INTO DWB_RANK_HISTORY_TMP(ORDER_ID,RANK_CODE,MEMBER_ID, MEMBER_KEY, MIN_MARK, RANK_ORDER, RANK_MARK, FROM_RANK_ID, TO_RANK_ID, PARTNER_ID, PARTNER_RANK_ID, OLD_RANK_ID, OLD_RANK_ORDER, REASON_CODE, USE_MONTHS, RANK_DATE, STA_DATETIME, END_DATETIME, RANK_TYPE, OLD_MEMBER_LINK_ID, NEW_MEMBER_LINK_ID, ROW_ID, ONNET_SUB)
            SELECT 2 ORDER_ID,'EXCHANGE' RANK_CODE,MEMBER_ID, MEMBER_KEY, MIN_MARK, RANK_EXC_ORDER, RANK_MARK, FROM_RANK_ID, TO_RANK_ID, PARTNER_ID, PARTNER_RANK_ID, OLD_RANK_ID, OLD_RANK_ORDER, REASON_CODE, USE_MONTHS, RANK_DATE, STA_DATETIME, END_DATETIME, RANK_TYPE, OLD_MEMBER_LINKS_ID, NEW_MEMBER_LINKS_ID, ROW_ID, ONNET_SUB
            FROM (
                SELECT  B.MEMBER_ID,A.MEMBER_KEY, B.MIN_MARK,B.RANK_EXC_ORDER,NVL(A.RANK_MARK,0) RANK_MARK, B.FROM_RANK_ID, B.TO_RANK_ID,B.PARTNER_ID,B.PARTNER_RANK_ID,
                        NVL(A.RANK_ID,C.RANK_ID)   OLD_RANK_ID, NVL(NVL(A.RANK_ORDER,C.OLD_RANK_ORDER),-1) OLD_RANK_ORDER,B.REASON_CODE,
                        MONTHS_BETWEEN(TRUNC(p_SUM_DATE,'MONTH'),TRUNC(C.STA_DATETIME,'MONTH')) USE_MONTHS,
                        A.RANK_DATE,C.STA_DATETIME, C.END_DATETIME, C.RANK_TYPE,
                        NVL(C.MEMBER_LINKS_ID,-1) OLD_MEMBER_LINKS_ID, B.NEW_MEMBER_LINKS_ID, C.ROWID ROW_ID,
                        ONNET_SUB
                FROM (
                        Select  ML.MEMBER_ID, ML.RANK_ID PARTNER_RANK_ID, ML.PARTNER_ID, RE.MIN_MARK,RE.FROM_RANK_ID, RE.TO_RANK_ID,
                                R1.RANK_ORDER RANK_EXC_ORDER, RE.REASON_CODE, ML.MEMBER_LINKS_ID NEW_MEMBER_LINKS_ID, M.ONNET_SUB
                        From DWR_MEMBER M, DWB_MEMBER_LINKS ML, DWL_RANK_EXCHANGE RE, DWL_RANK R1
                        Where TRUNC(p_SUM_DATE) >= TRUNC(ML.STA_DATETIME,'MONTH') AND ((TRUNC(p_SUM_DATE) < ML.END_DATETIME AND LAST_DAY(p_SUM_DATE) <= NVL(ML.FINISH_DATETIME,LAST_DAY(p_SUM_DATE))) Or ML.END_DATETIME Is Null)
                        AND TRUNC(p_SUM_DATE) >= RE.STA_DATETIME AND (TRUNC(p_SUM_DATE) < RE.END_DATETIME Or RE.END_DATETIME Is Null)
                        AND ML.partner_id = re.from_partner_id
                        AND RE.to_partner_id = v_Home_Partner_ID
                        AND ML.rank_id = RE.from_rank_id
                        AND RE.TO_RANK_ID = R1.RANK_ID
                        AND ML.PARTNER_ID = p_GLP_Partner_ID
                        AND M.MEMBER_ID = ML.MEMBER_ID
                        AND M.STATUS = '1'                        
                        ) B
                    LEFT OUTER JOIN (                          
                                        SELECT  /*+ PARALLEL(MH,4) PARALLEL(M,4) */ M.MEMBER_ID, STA_DATETIME, NVL(SUM_RANK_MARK,0) RANK_MARK, RANK_DATE,M.RANK_ID, R3.RANK_ORDER, M.MEMBER_KEY
                                        FROM DWR_MEMBER M, DWL_RANK R3, DWD_MARK_HISTORY MH
                                        WHERE M.MEMBER_ID = MH.MEMBER_ID
                                          AND M.RANK_ID = R3.RANK_ID
                                          AND MH.MO_KEY = V_MO_KEY
                                          AND MH.BALANCE_TYPE = '01'
                                          AND M.STATUS = '1'
                                          AND M.MEM_TYPE = '01'
                                          AND M.STA_DATETIME < p_SUM_DATE
                                    ) A
                        ON B.MEMBER_ID = A.MEMBER_ID
                        --AND A.RANK_MARK > B.MIN_MARK
                    LEFT OUTER JOIN (SELECT *
                                     FROM (
                                         SELECT RH.*,R1.RANK_ORDER OLD_RANK_ORDER,
                                                ROW_NUMBER() OVER(PARTITION BY MEMBER_ID ORDER BY STA_DATETIME DESC,END_DATETIME DESC) AS ROW_NUMBER
                                         FROM DWB_RANK_HISTORY RH , DWL_RANK R1
                                         WHERE RH.RANK_ID = R1.RANK_ID
                                           AND TRUNC(p_SUM_DATE) >= Trunc(RH.STA_DATETIME,'MONTH') AND (TRUNC(p_SUM_DATE) < RH.END_DATETIME Or RH.END_DATETIME Is Null)
                                     ) WHERE ROW_NUMBER = 1) C
                        ON B.MEMBER_ID = C.MEMBER_ID
            ) WHERE RANK_EXC_ORDER >= OLD_RANK_ORDER
            AND OLD_MEMBER_LINKS_ID != NEW_MEMBER_LINKS_ID
            AND NVL(RANK_MARK,0) >= MIN_MARK;

            V_TARGET_RECORD_CNT :=  SQL%ROWCOUNT;
            V_TASK_DRTN := ROUND((SYSDATE - V_TASK_STARTTIME)*24*60*60,0);
            V_TASK_ENDTIME := SYSDATE;
            V_ETL_ENDTIME := SYSDATE;
            /*INSERT STG_ETL_LOG*/
            PCK_LOG.INSERT_STG_ETL_LOG
            (
                V_ETL_STARTIME,
                V_ETL_SERVER_NAME,V_ETL_PROCESSOR_NAME,
                V_ETL_TASK_NAME,V_SOURCE_SYSTEM_CD,
                V_SOURCE_OBJECT_TYPE,
                V_SOURCE_OBJECT_NAME,V_TARGET_TABLE,V_SOURCE_RECORD_CNT,
                V_TARGET_RECORD_CNT,V_TASK_STARTTIME,V_TASK_ENDTIME,
                V_TASK_STATUS_CD,
                V_STATUS,V_TASK_DRTN,V_ETL_ENDTIME,V_ETL_USERNAME,V_DATA_PROCESS_DATE
            );

            --------------------------------------------------------------------
            --TASK#03
            V_ETL_PROCESSOR_NAME    := 'INS_RANK_LIST';      
            V_TASK_STARTTIME	      := SYSDATE;
            V_ETL_TASK_NAME		      := 'INS_RANK_LIST#RANK_PROFILE';
            V_SOURCE_OBJECT_NAME	  := 'DWD_MARK_HISTORY';
            V_TARGET_TABLE		      := 'DWB_RANK_HISTORY_TMP';
            V_TARGET_RECORD_CNT	    := 0;

            --Dung SUM_RANK_MARK
            INSERT INTO DWB_RANK_HISTORY_TMP(ORDER_ID,RANK_CODE,MEMBER_ID, MEMBER_KEY, TO_RANK_ID, RANK_ORDER, RANK_MARK, FROM_MARK, TO_MARK, MONTHS, ALERT_MARK, OLD_RANK_ID, OLD_RANK_ORDER, USE_MONTHS, RANK_DATE, STA_DATETIME, END_DATETIME, ROW_ID, ONNET_SUB)
            SELECT 3 ORDER_ID,'PROFILE' RANK_CODE,MEMBER_ID, MEMBER_KEY, RANK_ID, RANK_ORDER, RANK_MARK, FROM_MARK, TO_MARK, MONTHS, ALERT_MARK, OLD_RANK_ID, OLD_RANK_ORDER, USE_MONTHS, RANK_DATE, STA_DATETIME, END_DATETIME, ROW_ID, ONNET_SUB
            FROM (
                SELECT  A.MEMBER_ID,A.MEMBER_KEY, B.RANK_ID,B.RANK_ORDER,A.RANK_MARK, B.FROM_MARK, B.TO_MARK,B.MONTHS,B.ALERT_MARK,
                        NVL(A.RANK_ID,C.RANK_ID)   OLD_RANK_ID, NVL(NVL(A.RANK_ORDER,C.OLD_RANK_ORDER),-1) OLD_RANK_ORDER,
                        MONTHS_BETWEEN(TRUNC(p_SUM_DATE,'MONTH'),TRUNC(C.STA_DATETIME,'MONTH')) USE_MONTHS,
                        A.RANK_DATE,C.STA_DATETIME, C.END_DATETIME, C.ROWID ROW_ID, A.ONNET_SUB
                FROM (          
                        SELECT  /*+ PARALLEL(MH,4) PARALLEL(M,4) */ M.MEMBER_ID, STA_DATETIME, NVL(SUM_RANK_MARK,0) RANK_MARK, RANK_DATE,M.RANK_ID, R3.RANK_ORDER, M.MEMBER_KEY, M.ONNET_SUB
                        FROM DWR_MEMBER M, DWL_RANK R3, DWD_MARK_HISTORY MH
                        WHERE M.MEMBER_ID = MH.MEMBER_ID
                          AND M.RANK_ID = R3.RANK_ID
                          AND MH.MO_KEY = V_MO_KEY
                          AND MH.BALANCE_TYPE = '01'
                          AND M.STATUS = '1'
                          AND M.MEM_TYPE = '01'
                          AND M.STA_DATETIME < p_SUM_DATE
                          AND ((MONTHS_BETWEEN(M.RANK_DATE,M.LAST_RANK_DATE) = 0) OR (MONTHS_BETWEEN(M.RANK_DATE,M.LAST_RANK_DATE) = p_Rank_Months))
                    ) A
                    LEFT OUTER JOIN (
                                        SELECT *
                                        FROM (
                                            SELECT RP.*,R2.RANK_ORDER RANK_ORDER, ROW_NUMBER() OVER(PARTITION BY RP.RANK_ID ORDER BY STA_DATETIME DESC) AS ROW_NUMBER
                                            FROM DWL_RANK_PROFILE RP , DWL_RANK R2
                                            WHERE RP.RANK_ID = R2.RANK_ID
                                            AND TRUNC(P_SUM_DATE) >= RP.STA_DATETIME AND (TRUNC(P_SUM_DATE) < RP.END_DATETIME OR RP.END_DATETIME IS NULL)
                                        )
                                        WHERE ROW_NUMBER = 1
                                    ) B
                        ON A.RANK_MARK BETWEEN B.FROM_MARK AND B.TO_MARK
                    LEFT OUTER JOIN (
                                         SELECT *
                                         FROM (
                                             SELECT RH.*,R1.RANK_ORDER OLD_RANK_ORDER, ROW_NUMBER() OVER(PARTITION BY RH.MEMBER_ID ORDER BY STA_DATETIME DESC) AS ROW_NUMBER
                                             FROM DWB_RANK_HISTORY RH , DWL_RANK R1
                                             WHERE RH.RANK_ID = R1.RANK_ID
                                               AND TRUNC(p_SUM_DATE) >= Trunc(RH.STA_DATETIME,'MONTH')
                                               AND (TRUNC(p_SUM_DATE) < RH.END_DATETIME Or RH.END_DATETIME Is Null)
                                         ) WHERE ROW_NUMBER = 1
                                     ) C
                        ON A.MEMBER_ID = C.MEMBER_ID
                )
            WHERE RANK_ORDER > OLD_RANK_ORDER;

            --Lay mark lui lai 12 thang - case VNA
            INSERT INTO DWB_RANK_HISTORY_TMP(ORDER_ID,RANK_CODE,MEMBER_ID, MEMBER_KEY, TO_RANK_ID, RANK_ORDER, RANK_MARK, FROM_MARK, TO_MARK, MONTHS, ALERT_MARK, OLD_RANK_ID, OLD_RANK_ORDER, USE_MONTHS, RANK_DATE, STA_DATETIME, END_DATETIME, ROW_ID, ONNET_SUB)
            SELECT 3 ORDER_ID,'PROFILE' RANK_CODE,MEMBER_ID, MEMBER_KEY, RANK_ID, RANK_ORDER, RANK_MARK, FROM_MARK, TO_MARK, MONTHS, ALERT_MARK, OLD_RANK_ID, OLD_RANK_ORDER, USE_MONTHS, RANK_DATE, STA_DATETIME, END_DATETIME, ROW_ID, ONNET_SUB
            FROM (
                SELECT  A.MEMBER_ID,A.MEMBER_KEY, B.RANK_ID,B.RANK_ORDER,A.RANK_MARK, B.FROM_MARK, B.TO_MARK,B.MONTHS,B.ALERT_MARK,
                        NVL(A.RANK_ID,C.RANK_ID)   OLD_RANK_ID, NVL(NVL(A.RANK_ORDER,C.OLD_RANK_ORDER),-1) OLD_RANK_ORDER,
                        MONTHS_BETWEEN(TRUNC(p_SUM_DATE,'MONTH'),TRUNC(C.STA_DATETIME,'MONTH')) USE_MONTHS,
                        A.RANK_DATE,C.STA_DATETIME, C.END_DATETIME, C.ROWID ROW_ID, A.ONNET_SUB
                FROM (          
                        SELECT  /*+ PARALLEL(MH,4) PARALLEL(M,4) */ M.MEMBER_ID, STA_DATETIME, SUM(NVL(MARK,0)) RANK_MARK, RANK_DATE,M.RANK_ID, R3.RANK_ORDER, M.MEMBER_KEY, M.ONNET_SUB
                        FROM DWR_MEMBER M, DWL_RANK R3, DWD_MARK_HISTORY MH
                        WHERE M.MEMBER_ID = MH.MEMBER_ID
                          AND M.RANK_ID = R3.RANK_ID
                          AND MH.MO_KEY >= TO_NUMBER(TO_CHAR(ADD_MONTHS(NVL(RANK_DATE,STA_DATETIME),-p_Rank_Months),'YYYYMM'))
                          AND MH.BALANCE_TYPE = '01'
                          AND M.STATUS = '1'
                          AND M.MEM_TYPE = '01'
                          AND M.STA_DATETIME < p_SUM_DATE
                          AND ((MONTHS_BETWEEN(M.RANK_DATE,M.LAST_RANK_DATE) <> 0) AND (MONTHS_BETWEEN(M.RANK_DATE,M.LAST_RANK_DATE) < p_Rank_Months))
                          GROUP BY M.MEMBER_ID, STA_DATETIME ,RANK_DATE, M.RANK_ID,R3.RANK_ORDER, M.MEMBER_KEY, M.ONNET_SUB
                    ) A
                    LEFT OUTER JOIN (
                                        SELECT *
                                        FROM (
                                            SELECT RP.*,R2.RANK_ORDER RANK_ORDER, ROW_NUMBER() OVER(PARTITION BY RP.RANK_ID ORDER BY STA_DATETIME DESC) AS ROW_NUMBER
                                            FROM DWL_RANK_PROFILE RP , DWL_RANK R2
                                            WHERE RP.RANK_ID = R2.RANK_ID
                                            AND TRUNC(P_SUM_DATE) >= RP.STA_DATETIME AND (TRUNC(P_SUM_DATE) < RP.END_DATETIME OR RP.END_DATETIME IS NULL)
                                        )
                                        WHERE ROW_NUMBER = 1
                                    ) B
                        ON A.RANK_MARK BETWEEN B.FROM_MARK AND B.TO_MARK
                    LEFT OUTER JOIN (
                                         SELECT *
                                         FROM (
                                             SELECT RH.*,R1.RANK_ORDER OLD_RANK_ORDER, ROW_NUMBER() OVER(PARTITION BY RH.MEMBER_ID ORDER BY STA_DATETIME DESC) AS ROW_NUMBER
                                             FROM DWB_RANK_HISTORY RH , DWL_RANK R1
                                             WHERE RH.RANK_ID = R1.RANK_ID
                                               AND TRUNC(p_SUM_DATE) >= Trunc(RH.STA_DATETIME,'MONTH')
                                               AND (TRUNC(p_SUM_DATE) < RH.END_DATETIME Or RH.END_DATETIME Is Null)
                                         ) WHERE ROW_NUMBER = 1
                                     ) C
                        ON A.MEMBER_ID = C.MEMBER_ID
                )
            WHERE RANK_ORDER > OLD_RANK_ORDER;            

            V_TARGET_RECORD_CNT :=  SQL%ROWCOUNT;
            V_TASK_DRTN := ROUND((SYSDATE - V_TASK_STARTTIME)*24*60*60,0);
            V_TASK_ENDTIME := SYSDATE;
            V_ETL_ENDTIME := SYSDATE;
            /*INSERT STG_ETL_LOG*/
            PCK_LOG.INSERT_STG_ETL_LOG
            (
                V_ETL_STARTIME,
                V_ETL_SERVER_NAME,V_ETL_PROCESSOR_NAME,
                V_ETL_TASK_NAME,V_SOURCE_SYSTEM_CD,
                V_SOURCE_OBJECT_TYPE,
                V_SOURCE_OBJECT_NAME,V_TARGET_TABLE,V_SOURCE_RECORD_CNT,
                V_TARGET_RECORD_CNT,V_TASK_STARTTIME,V_TASK_ENDTIME,
                V_TASK_STATUS_CD,
                V_STATUS,V_TASK_DRTN,V_ETL_ENDTIME,V_ETL_USERNAME,V_DATA_PROCESS_DATE
            );
            --------------------------------------------------------------------
            --TASK#04
            V_ETL_PROCESSOR_NAME    := 'INS_RANK_LIST';      
            V_TASK_STARTTIME	      := SYSDATE;
            V_ETL_TASK_NAME		      := 'INS_RANK_LIST#RANK_DOWN';
            V_SOURCE_OBJECT_NAME	  := 'DWD_MARK_HISTORY,DWR_MEMBER';
            V_TARGET_TABLE		      := 'DWB_RANK_HISTORY_TMP';
            V_TARGET_RECORD_CNT	    := 0;

            INSERT INTO DWB_RANK_HISTORY_TMP(ORDER_ID,RANK_CODE,MEMBER_KEY, MEMBER_ID, TO_RANK_ID, RANK_ORDER, RANK_MARK, FROM_MARK, TO_MARK, MONTHS, ALERT_MARK, OLD_RANK_ID, OLD_RANK_ORDER, USE_MONTHS, MOB_TYPE, RANK_DATE, STA_DATETIME, END_DATETIME, ROW_ID, ONNET_SUB)
            SELECT 4 ORDER_ID, 'RANK_DOWN' RANK_CODE,A.MEMBER_KEY,A.MEMBER_ID, B.RANK_ID,B.RANK_ORDER,A.RANK_MARK, B.FROM_MARK, B.TO_MARK,B.MONTHS,B.ALERT_MARK,
                        NVL(A.RANK_ID,C.RANK_ID)   OLD_RANK_ID, NVL(NVL(A.RANK_ORDER,C.OLD_RANK_ORDER),-1) OLD_RANK_ORDER,
                        MONTHS_BETWEEN(TRUNC(p_SUM_DATE,'MONTH'),TRUNC(C.STA_DATETIME,'MONTH')) USE_MONTHS,
                        A.MOB_TYPE,A.RANK_DATE,C.STA_DATETIME, C.END_DATETIME, C.ROWID ROW_ID, A.ONNET_SUB
                FROM (  
                        SELECT  /*+ PARALLEL(MH,4) PARALLEL(M,4) */ M.MEMBER_ID,M.MOB_TYPE, STA_DATETIME, NVL(SUM_RANK_MARK,0) RANK_MARK, RANK_DATE,M.RANK_ID, R3.RANK_ORDER, M.MEMBER_KEY, M.ONNET_SUB
                        FROM DWR_MEMBER M
                        INNER JOIN DWL_RANK R3
                            ON M.RANK_ID = R3.RANK_ID
                        LEFT OUTER JOIN DWD_MARK_HISTORY MH
                            ON M.MEMBER_ID = MH.MEMBER_ID
                            AND MH.MO_KEY = V_MO_KEY
                            AND MH.BALANCE_TYPE = '01'
                        WHERE M.STATUS = '1'
                          AND M.MEM_TYPE = '01'
                          AND M.RANK_DATE = p_SUM_DATE
                          AND M.STA_DATETIME < p_SUM_DATE
                          AND ((MONTHS_BETWEEN(M.RANK_DATE,M.LAST_RANK_DATE) = 0) OR (MONTHS_BETWEEN(M.RANK_DATE,M.LAST_RANK_DATE) = p_Rank_Months))
                    ) A
                    LEFT OUTER JOIN (
                                    SELECT RP.*,R2.RANK_ORDER RANK_ORDER,ROW_NUMBER() OVER(PARTITION BY RP.RANK_ID ORDER BY STA_DATETIME DESC) AS ROW_NUMBER
                                    FROM DWL_RANK_PROFILE RP , DWL_RANK R2
                                    WHERE RP.RANK_ID = R2.RANK_ID
                                    AND TRUNC(p_SUM_DATE) >= RP.STA_DATETIME AND (TRUNC(p_SUM_DATE) < RP.END_DATETIME Or RP.END_DATETIME Is Null)
                                    ) B
                        ON A.RANK_MARK BETWEEN B.FROM_MARK AND B.TO_MARK
                        AND ROW_NUMBER = 1
                    LEFT OUTER JOIN (SELECT *
                                     FROM (
                                         SELECT RH.*,R1.RANK_ORDER OLD_RANK_ORDER, ROW_NUMBER() OVER(PARTITION BY MEMBER_ID ORDER BY STA_DATETIME DESC) AS ROW_NUMBER
                                         FROM DWB_RANK_HISTORY RH , DWL_RANK R1
                                         WHERE RH.RANK_ID = R1.RANK_ID
                                           AND TRUNC(p_SUM_DATE) >= Trunc(RH.STA_DATETIME,'MONTH') AND (TRUNC(p_SUM_DATE) < RH.END_DATETIME Or RH.END_DATETIME Is Null)
                                     ) WHERE ROW_NUMBER = 1) C
                        ON A.MEMBER_ID = C.MEMBER_ID;
            
            --Lay lui ai 12 thang            
            INSERT INTO DWB_RANK_HISTORY_TMP(ORDER_ID,RANK_CODE,MEMBER_KEY, MEMBER_ID, TO_RANK_ID, RANK_ORDER, RANK_MARK, FROM_MARK, TO_MARK, MONTHS, ALERT_MARK, OLD_RANK_ID, OLD_RANK_ORDER, USE_MONTHS, MOB_TYPE, RANK_DATE, STA_DATETIME, END_DATETIME, ROW_ID, ONNET_SUB)
            SELECT 4 ORDER_ID, 'RANK_DOWN' RANK_CODE,A.MEMBER_KEY,A.MEMBER_ID, B.RANK_ID,B.RANK_ORDER,A.RANK_MARK, B.FROM_MARK, B.TO_MARK,B.MONTHS,B.ALERT_MARK,
                        NVL(A.RANK_ID,C.RANK_ID)   OLD_RANK_ID, NVL(NVL(A.RANK_ORDER,C.OLD_RANK_ORDER),-1) OLD_RANK_ORDER,
                        MONTHS_BETWEEN(TRUNC(p_SUM_DATE,'MONTH'),TRUNC(C.STA_DATETIME,'MONTH')) USE_MONTHS,
                        A.MOB_TYPE,A.RANK_DATE,C.STA_DATETIME, C.END_DATETIME, C.ROWID ROW_ID, A.ONNET_SUB
                FROM (  
                        SELECT  /*+ PARALLEL(MH,4) PARALLEL(M,4) */ M.MEMBER_ID,M.MOB_TYPE, STA_DATETIME, NVL(SUM_RANK_MARK,0) RANK_MARK, RANK_DATE,M.RANK_ID, R3.RANK_ORDER, M.MEMBER_KEY, M.ONNET_SUB
                        FROM DWR_MEMBER M
                        INNER JOIN DWL_RANK R3
                            ON M.RANK_ID = R3.RANK_ID
                        LEFT OUTER JOIN DWD_MARK_HISTORY MH
                            ON M.MEMBER_ID = MH.MEMBER_ID
                            AND MH.MO_KEY >= TO_NUMBER(TO_CHAR(ADD_MONTHS(NVL(RANK_DATE,STA_DATETIME),-p_Rank_Months),'YYYYMM'))
                            AND MH.BALANCE_TYPE = '01'
                        WHERE M.STATUS = '1'
                          AND M.MEM_TYPE = '01'
                          AND M.RANK_DATE = p_SUM_DATE
                          AND M.STA_DATETIME < p_SUM_DATE
                          AND ((MONTHS_BETWEEN(M.RANK_DATE,M.LAST_RANK_DATE) <> 0) AND (MONTHS_BETWEEN(M.RANK_DATE,M.LAST_RANK_DATE) < p_Rank_Months))
                    ) A
                    LEFT OUTER JOIN (
                                    SELECT RP.*,R2.RANK_ORDER RANK_ORDER,ROW_NUMBER() OVER(PARTITION BY RP.RANK_ID ORDER BY STA_DATETIME DESC) AS ROW_NUMBER
                                    FROM DWL_RANK_PROFILE RP , DWL_RANK R2
                                    WHERE RP.RANK_ID = R2.RANK_ID
                                    AND TRUNC(p_SUM_DATE) >= RP.STA_DATETIME AND (TRUNC(p_SUM_DATE) < RP.END_DATETIME Or RP.END_DATETIME Is Null)
                                    ) B
                        ON A.RANK_MARK BETWEEN B.FROM_MARK AND B.TO_MARK
                        AND ROW_NUMBER = 1
                    LEFT OUTER JOIN (SELECT *
                                     FROM (
                                         SELECT RH.*,R1.RANK_ORDER OLD_RANK_ORDER, ROW_NUMBER() OVER(PARTITION BY MEMBER_ID ORDER BY STA_DATETIME DESC) AS ROW_NUMBER
                                         FROM DWB_RANK_HISTORY RH , DWL_RANK R1
                                         WHERE RH.RANK_ID = R1.RANK_ID
                                           AND TRUNC(p_SUM_DATE) >= Trunc(RH.STA_DATETIME,'MONTH') AND (TRUNC(p_SUM_DATE) < RH.END_DATETIME Or RH.END_DATETIME Is Null)
                                     ) WHERE ROW_NUMBER = 1) C
                        ON A.MEMBER_ID = C.MEMBER_ID;                        

            V_TARGET_RECORD_CNT :=  SQL%ROWCOUNT;
            V_TASK_DRTN := ROUND((SYSDATE - V_TASK_STARTTIME)*24*60*60,0);
            V_TASK_ENDTIME := SYSDATE;
            V_ETL_ENDTIME := SYSDATE;
            /*INSERT STG_ETL_LOG*/
            PCK_LOG.INSERT_STG_ETL_LOG
            (
                V_ETL_STARTIME,
                V_ETL_SERVER_NAME,V_ETL_PROCESSOR_NAME,
                V_ETL_TASK_NAME,V_SOURCE_SYSTEM_CD,
                V_SOURCE_OBJECT_TYPE,
                V_SOURCE_OBJECT_NAME,V_TARGET_TABLE,V_SOURCE_RECORD_CNT,
                V_TARGET_RECORD_CNT,V_TASK_STARTTIME,V_TASK_ENDTIME,
                V_TASK_STATUS_CD,
                V_STATUS,V_TASK_DRTN,V_ETL_ENDTIME,V_ETL_USERNAME,V_DATA_PROCESS_DATE
            );

        EXCEPTION
            WHEN OTHERS THEN
                RAISE_APPLICATION_ERROR(-20084,SQLERRM);
        END;

        PROCEDURE  RANK_PROCESS_SP( P_MEMBER_ID      NUMBER,
                                    P_SUM_DATE       DATE,
                                    P_RERUN          BOOLEAN DEFAULT FALSE,
                                    P_APPLY_TIME     VARCHAR DEFAULT 'SYSDATE',
                                    p_CHANEL         VARCHAR DEFAULT 'WEB',
                                    P_IP_ADDRESS     VARCHAR DEFAULT '127.0.0.1',
                                    P_NEXT_RANK_DATE DATE DEFAULT NULL
                                    ) IS

            --------------------------------------------------------------------
            Cursor c_Up_Rank(v_MEMBER_ID        Number,
                             v_SUM_DATE         Date) Is
            Select *
            From (
            Select * from table(pck_process_rank.GET_RANK_LIST(v_MEMBER_ID,v_SUM_DATE))
/*            Where (RANK_ORDER > OLD_RANK_ORDER
                Or (RANK_ORDER = OLD_RANK_ORDER And Rank_Date = v_SUM_DATE)
                )*/
            Order by RANK_ORDER desc,ORDER_ID asc
            ) Where rownum = 1;

            Cursor c_Down_Rank(v_MEMBER_ID        Number,
                               v_SUM_DATE         Date) Is
            Select * from table(pck_process_rank.GET_RANK_LIST(v_MEMBER_ID,v_SUM_DATE))
            Where RANK_CODE =  'RANK_DOWN';
            --------------------------------------------------------------------
            Cursor c_Data(v_MEMBER_ID        Number) is
            Select *
            From (
                Select a.rowid ROW_ID,a.rank_id,nvl(a.Rank_date,b.last_rank_Date) Rank_date,a.member_id, 
                       ROW_NUMBER() OVER(PARTITION BY A.MEMBER_ID ORDER BY MO_KEY DESC) AS ROW_NUMBER
                From dwb_rank_history a, Dwr_member b
                Where a.MEMBER_ID = v_MEMBER_ID
                And a.member_id = b.member_id
            ) Where ROW_NUMBER = 2;
            --------------------------------------------------------------------

            V_MO_KEY            NUMBER:=TO_NUMBER(TO_CHAR(P_SUM_DATE,'YYYYMM'));
        BEGIN

            If(P_RERUN = TRUE) Then
                For v_Data in c_Data(p_Member_ID) Loop
                    Update dwr_member set Rank_ID = v_Data.Rank_ID, Rank_Date = v_Data.Rank_Date, Mem_Type='01'
                    Where MEMBER_ID = v_Data.Member_ID;

                    Delete from dwb_rank_history Where MEMBER_ID = v_Data.Member_ID And Mo_Key = V_MO_KEY;
                    Delete from DWL_MEMBER_BLACK_LIST Where MEMBER_ID = v_Data.Member_ID And Mo_Key = V_MO_KEY;
                End Loop;
            End If;

            --Xu ly xet hang
            For v_Up_Rank in c_Up_Rank(p_MEMBER_ID, P_SUM_DATE) Loop
                IF(P_SUM_DATE != v_Up_Rank.RANK_DATE
                    and v_Up_Rank.RANK_ORDER = v_Up_Rank.OLD_RANK_ORDER) Then
                    dbms_output.put_line('Khong Thang Hang nua do hang bang nhau va chua den han xet hang');
                Else
                    RANK_MEMBER_PROCESS_SP(P_MEMBER_ID,
                                           P_SUM_DATE,
                                           P_APPLY_TIME,
                                           v_Up_Rank.STA_DATETIME,
                                           v_Up_Rank.RANK_CODE,
                                           v_Up_Rank.RANK_DATE,
                                           v_Up_Rank.RANK_MARK,
                                           v_Up_Rank.TO_RANK_ID,
                                           v_Up_Rank.OLD_RANK_ID,
                                           v_Up_Rank.PARTNER_RANK_ID,
                                           v_Up_Rank.PARTNER_ID,
                                           v_Up_Rank.NEW_MEMBER_LINK_ID,
                                           v_Up_Rank.REASON_CODE,
                                           v_Up_Rank.CREATE_BY,
                                           v_Up_Rank.ROW_ID,
                                           v_Up_Rank.RANK_ORDER,
                                           v_Up_Rank.OLD_RANK_ORDER,
                                           v_Up_Rank.MOB_TYPE,
                                           v_Up_Rank.MEMBER_KEY,
                                           p_CHANEL,
                                           P_IP_ADDRESS,
                                           v_Up_Rank.Use_Months,
                                           P_NEXT_RANK_DATE,
                                           'v_Up_Rank.ONNET_SUB');
                End If;
            End Loop;
        EXCEPTION
            WHEN OTHERS THEN
                Rollback;
                RAISE_APPLICATION_ERROR(-20084,SQLERRM);
        END; -- PROCEDURE     

        PROCEDURE  RANK_PROCESS_ALL(P_SUM_DATE       DATE) IS
            --LOG
            V_ETL_STARTIME         DATE := SYSDATE;
            V_ETL_PROCESSOR_NAME   VARCHAR2 (100) := 'RANK_PROCESS_ALL';
            V_ETL_TASK_NAME        VARCHAR2 (100) := 'RANK_PROCESS_ALL';
            V_SOURCE_OBJECT_NAME   VARCHAR2 (100) := 'MBF_KNDL_NEW.DWD_MARK_HISTORY';
            V_TARGET_TABLE         VARCHAR2 (100) := 'MBF_KNDL_NEW.DWB_RANK_HISTORY';
            V_SOURCE_RECORD_CNT    NUMBER (20)    := 0;
            V_TARGET_RECORD_CNT    NUMBER (20)    := 0;
            V_TASK_STARTTIME       DATE;
            V_TASK_ENDTIME         DATE;
            V_TASK_STATUS_CD       VARCHAR2 (100) := 'SUCCESS';
            V_STATUS               VARCHAR2 (100) := '5';
            V_TASK_DRTN            NUMBER(15);
            V_ETL_ENDTIME          DATE;
            V_DATA_PROCESS_DATE    NUMBER  := TO_NUMBER(TO_CHAR(P_SUM_DATE,'YYYYMMDD'));
            ---------------------------------------------
            v_Message              Varchar2(1000);

            Cursor c_Rank_Data Is
            Select *
            From (
                Select a.*, ROW_NUMBER() OVER(PARTITION BY A.MEMBER_ID ORDER BY RANK_ORDER DESC,ORDER_ID ASC) AS ROW_NUMBER
                From DWB_RANK_HISTORY_TMP a
            ) Where ROW_NUMBER = 1;

        BEGIN
            INS_RANK_LIST(P_SUM_DATE);
            Commit;
            For v_Up_Rank in c_Rank_Data Loop
                IF(P_SUM_DATE != v_Up_Rank.RANK_DATE
                    and v_Up_Rank.RANK_ORDER = v_Up_Rank.OLD_RANK_ORDER) Then
                    v_Message := 'Khong Thang Hang nua do hang bang nhau va chua den han xet hang!...';
                Else
                    RANK_MEMBER_PROCESS_SP(v_Up_Rank.MEMBER_ID,
                                           P_SUM_DATE,
                                           'MONTHLY',
                                           v_Up_Rank.STA_DATETIME,
                                           v_Up_Rank.RANK_CODE,
                                           v_Up_Rank.RANK_DATE,
                                           v_Up_Rank.RANK_MARK,
                                           v_Up_Rank.TO_RANK_ID,
                                           v_Up_Rank.OLD_RANK_ID,
                                           v_Up_Rank.PARTNER_RANK_ID,
                                           v_Up_Rank.PARTNER_ID,
                                           v_Up_Rank.NEW_MEMBER_LINK_ID,
                                           v_Up_Rank.REASON_CODE,
                                           v_Up_Rank.CREATE_BY,
                                           v_Up_Rank.ROW_ID,
                                           v_Up_Rank.RANK_ORDER,
                                           v_Up_Rank.OLD_RANK_ORDER,
                                           v_Up_Rank.MOB_TYPE,
                                           v_Up_Rank.MEMBER_KEY,
                                           'AUTO',
                                           '127.0.0.1',
                                           v_Up_Rank.Use_Months,
                                           NULL,
                                           v_Up_Rank.ONNET_SUB);
                End If;
            End Loop;
            Commit;
          /*INSERT STG_ETL_LOG*/
          V_ETL_ENDTIME := SYSDATE;
          PCK_LOG.INSERT_STG_ETL_LOG
          (
              V_ETL_STARTIME,
              V_ETL_SERVER_NAME,V_ETL_PROCESSOR_NAME,
              V_ETL_TASK_NAME,V_SOURCE_SYSTEM_CD,
              V_SOURCE_OBJECT_TYPE,
              V_SOURCE_OBJECT_NAME,V_TARGET_TABLE,V_SOURCE_RECORD_CNT,
              V_TARGET_RECORD_CNT,V_TASK_STARTTIME,V_TASK_ENDTIME,
              V_TASK_STATUS_CD,
              V_STATUS,V_TASK_DRTN,V_ETL_ENDTIME,V_ETL_USERNAME,V_DATA_PROCESS_DATE
          );
        EXCEPTION
            WHEN OTHERS THEN
               ROLLBACK;
               V_SQLERRORCODE := SQLCODE;
               V_SQLERRORTEXT := SQLERRM;
            /*INSERT STG_ETL_ERRLOG*/
              PCK_LOG.INSERT_STG_ETL_ERRORLOG
              (
                  V_SQLERRORCODE,
                  'ETL_ERROR',
                  SUBSTR (V_SQLERRORTEXT, 1, 1020),
                  V_ETL_STARTIME,
                  'MBF_KNDL_NEW',
                  V_ETL_PROCESSOR_NAME,
                  1,
                  V_ETL_TASK_NAME,
                  NULL,
                  'NOT_SEND',
                  NULL,
                  'NOT_SEND'
              );
              Raise_Application_Error(-20084,'RANK_PROCESS_ALL:' || SQLErrm);
        END; -- Procedure

    PROCEDURE MERGE_UPDATE_RANK_VIP_TH
      (
          P_MEMBER_KEY   IN       VARCHAR2,              
          P_ERROR        OUT      VARCHAR2
      )
    /*######################################################################################################################
    # Created By     : ManhNT,
    # Create Date    : 07/08/2020
    # Version Number : 1
    # Description    : 
    ######################################################################################################################*/
    AS
      V_ERROR             VARCHAR(2000);
    BEGIN
        MERGE INTO DWB_RANK_VIP A 
        USING 
        ( 
            SELECT  
                M.MEMBER_ID, 
                M.MEM_TYPE, 
                M.MEMBER_KEY, 
                V.SHORT_NAME, 
                R.RANK_ID FROM_RANK_ID, 
                R.RANK_NAME FROM_RANK_NAME, 
                R1.RANK_ID TO_RANK_ID, 
                R1.RANK_NAME TO_RANK_NAME, 
                V.VIP_TYPE,                                                    
                V.STA_DATE , 
                V.END_DATE, 
                V.DESCRIPTION 
            FROM DWR_MEMBER M  
            JOIN V_VIP_TH V ON M.MEMBER_KEY = V.ISDN  
            JOIN DWL_RANK R ON V.SHORT_NAME = R.RANK_CODE AND R.STATUS = '1' 
            JOIN DWL_RANK_EXCHANGE RE ON R.RANK_ID = RE.FROM_RANK_ID AND RE.STA_DATETIME <= TRUNC(SYSDATE) AND TRUNC(SYSDATE) <= NVL(RE.END_DATETIME,TRUNC(SYSDATE)) 
            JOIN DWL_RANK R1 ON R1.RANK_ID = RE.TO_RANK_ID AND R1.STATUS = '1' 
            WHERE M.STATUS = '1' AND M.END_DATETIME IS NULL AND M.MEMBER_KEY = P_MEMBER_KEY 
        ) B ON (A.MEMBER_ID = B.MEMBER_ID AND A.MEMBER_KEY = B.MEMBER_KEY AND A.STA_DATETIME = B.STA_DATE) 
        WHEN MATCHED 
        THEN 
            UPDATE SET END_DATETIME = LAST_DAY(SYSDATE), STATUS = '1', VIP_STATUS = '1', NOTE = B.DESCRIPTION, LAST_UPD_DATE = SYSDATE, REASON_CODE = 'RANK_03'
        WHEN NOT MATCHED 
        THEN 
            INSERT (RANK_VIP_ID,MEMBER_ID,MEMBER_KEY,RANK_ID,STA_DATETIME,END_DATETIME,STATUS,VIP_STATUS,NOTE,CREATE_DATE,CREATE_BY,LAST_UPD_DATE,LAST_UPD_BY, REASON_CODE)  
            VALUES (RANK_VIP_SEQ.NEXTVAL,B.MEMBER_ID,B.MEMBER_KEY,B.TO_RANK_ID,B.STA_DATE,LAST_DAY(SYSDATE),'1','1',B.DESCRIPTION,SYSDATE,'SYNC',SYSDATE,'SYNC','RANK_03');

      COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            Rollback;
            RAISE_APPLICATION_ERROR(-20084,'MERGE_UPDATE_RANK_VIP_TH:' || SQLERRM);
    END;

PROCEDURE  RANK_CHANGE_PROCESS_SP(P_MEMBER_ID      NUMBER,
                                    P_SUM_DATE       DATE,
                                    p_TO_RANK_ID     NUMBER,
                                    P_REASON_CODE    VARCHAR,
                                    p_USER_NAME      VARCHAR,
                                    p_CHANEL         VARCHAR DEFAULT 'WEB',
                                    P_IP_ADDRESS     VARCHAR DEFAULT '127.0.0.1') IS
        --------------------------------------------------------------------
        Cursor c_Rank_His Is
        Select *
        From (
            Select  b.member_key,nvl(a.rank_date,b.rank_date) cur_rank_date, nvl(b.rank_id,a.rank_id) cur_rank_ID,c.rank_order,
                    a.rowid ROW_ID,a.*, 
                    ROW_NUMBER() OVER(PARTITION BY A.MEMBER_ID ORDER BY a.mo_key DESC, a.STA_DATETIME DESC, a.END_DATETIME NULLS FIRST) AS ROW_NUMBER
            From dwb_rank_history a, dwr_member b, dwl_rank c
            Where a.member_id = b.member_id
            and b.rank_id = c.rank_id
            and a.member_id=P_MEMBER_ID
        ) Where ROW_NUMBER = 1;
        --------------------------------------------------------------------
        Cursor c_Check_Rank(v_Home_PartnerID    Number) Is
            Select Rank_Order
            From DWL_RANK
            Where Rank_ID = p_TO_RANK_ID
            And Partner_ID = v_Home_PartnerID;

        v_MO_KEY            NUMBER:=TO_NUMBER(TO_CHAR(P_SUM_DATE,'YYYYMM'));
        v_DAY_KEY           NUMBER:=TO_NUMBER(TO_CHAR(P_SUM_DATE,'YYYYMMDD'));
        v_Sta_DateTime      Date:=trunc(P_SUM_DATE);
        v_End_DateTime      Date:=v_Sta_DateTime-1;
        v_New_Rank_Order    Number;
        v_Rank_Type         Varchar2(2):='07';
    BEGIN
        If(P_REASON_CODE is null) Then
            RAISE_APPLICATION_ERROR(-20084,'Bien P_REASON_CODE khong duoc de trong' );
        End if;

        If(p_USER_NAME is null) Then
            RAISE_APPLICATION_ERROR(-20084,'Bien p_USER_NAME khong duoc de trong' );
        End if;

        If(p_TO_RANK_ID is null) Then
            RAISE_APPLICATION_ERROR(-20084,'Bien p_TO_RANK_ID khong duoc de trong' );
        Else
            open c_Check_Rank(FNC_GET_HOME_PARTNER());
            Fetch c_Check_Rank into v_New_Rank_Order;
            If c_Check_Rank%NOTFOUND Then
                RAISE_APPLICATION_ERROR(-20085,'Khong tim thay hang p_TO_RANK_ID tren DWL_RANK');
            End If;
            Close c_Check_Rank;                
        End if;

        For v_Data in c_Rank_His Loop
            /*If(v_New_Rank_Order > v_Data.RANK_ORDER) Then
                v_Rank_Type := '07';--v_Rank_Type := '03';
            ElsIf(v_New_Rank_Order <= v_Data.RANK_ORDER) Then
                v_Rank_Type := '07';
            End If;*/
            If(trunc(v_Data.Sta_DateTime) = trunc(P_SUM_DATE)) Then
                v_End_DateTime := v_Sta_DateTime;
            End If;

            Update DWB_RANK_HISTORY Set End_datetime = v_End_DateTime, Rank_Date = v_Data.CUR_RANK_DATE
            Where rowid = v_Data.ROW_ID;

            Update DWR_MEMBER Set Rank_ID = p_TO_RANK_ID, Rank_Date = ADD_MONTHS(TRUNC(P_SUM_DATE,'MONTH'),12)
            Where Member_ID = P_MEMBER_ID;

            Update DWB_RANK_VIP set END_DATETIME=p_SUM_DATE,
                                    LAST_UPD_DATE=sysdate,
                                    LAST_UPD_BY=p_USER_NAME
            Where member_id = v_Data.MEMBER_ID
            And (END_DATETIME is null Or end_datetime >= p_SUM_DATE);

            Insert into DWB_RANK_HISTORY(mo_key,day_key,member_id,rank_type,months,rank_date,rank_id,PRE_RANK_ID,rank_mark,sta_datetime,end_datetime,reason_code,partner_rank_id,partner_id,member_links_id,create_date,create_by)
            Values(v_MO_KEY,v_DAY_KEY,v_Data.MEMBER_ID,v_Rank_Type,Months_between(trunc(P_SUM_DATE,'MONTH'),trunc(v_Data.STA_DATETIME,'MONTH')),v_Data.CUR_RANK_DATE,p_TO_RANK_ID,v_Data.RANK_ID,0,v_Sta_DateTime,null,p_REASON_CODE,null,null,null,sysdate,p_USER_NAME);

            --Cho vao lich su
            Insert into dwb_action_audit(record_datetime,action_audit_id,member_id,action_id,chanel,user_name,ip_address,reason_code,table_name,member_key)
            values(sysdate,SEQ_ACTION_AUDIT.NEXTVAL,v_Data.MEMBER_ID,'06',p_CHANEL,p_USER_NAME,p_IP_ADDRESS,p_REASON_CODE,'DWR_MEMBER',v_Data.Member_key);
        End Loop;
    EXCEPTION
        WHEN OTHERS THEN
            Rollback;
            If c_Check_Rank%isOpen Then Close c_Check_Rank; End If;
            RAISE_APPLICATION_ERROR(-20084,SQLERRM);
    END; -- PROCEDURE

    PROCEDURE  PRC_RANK_VIP_HISTORY(p_SUM_DATE       DATE) Is

        Cursor c_Data is
            Select a.rank_id,a.member_id,a.REASON_CODE,b.member_key
            from dwb_rank_history a, dwr_member b
            Where b.rank_date = p_SUM_DATE
            and a.mo_key=To_Number(To_Char(ADD_MONTHS(p_SUM_DATE,-12),'YYYYMM'))
            and a.member_id = b.member_id
            and A.END_DATETIME is null
            and b.status = '1'
            and a.rank_id = b.rank_id
            and a.REASON_CODE IN ('RANK_10', 'RANK_15','RANK_DC_25');

        Cursor c_Rank_Vip(v_Member_ID   Number,
                          v_Member_Key  Number,
                          v_Reason_Code Varchar2,
                          v_Rank_ID     Number) Is
         Select ROW_ID
         From (
            select x.rowid ROW_ID,x.*,ROW_NUMBER() OVER( PARTITION BY x.member_key ORDER BY x.create_date DESC ) order_rank
            from DWB_RANK_VIP x 
            Where x.member_id=v_Member_ID
            and x.member_key=v_Member_Key
            and x.Reason_Code=v_Reason_Code
            and x.Rank_ID=v_Rank_ID
         ) Where rank_id in (259,260)
         and order_rank=1;                      

          --LOG
          V_ETL_STARTIME         DATE := SYSDATE;
          V_ETL_PROCESSOR_NAME   VARCHAR2 (100) := 'PRC_RANK_VIP_HISTORY';
          V_ETL_TASK_NAME        VARCHAR2 (100) := 'PRC_RANK_VIP_HISTORY';
          V_SOURCE_OBJECT_NAME   VARCHAR2 (100) := 'MBF_KNDL_NEW.DWD_MARK_HISTORY_DTL';
          V_TARGET_TABLE         VARCHAR2 (100) := 'MBF_KNDL_NEW.DWB_RANK_HISTORY_TMP';
          V_SOURCE_RECORD_CNT    NUMBER (20)    := 0;
          V_TARGET_RECORD_CNT    NUMBER (20)    := 0;
          V_TASK_STARTTIME       DATE;
          V_TASK_ENDTIME         DATE;
          V_TASK_STATUS_CD       VARCHAR2 (100) := 'SUCCESS';
          V_STATUS               VARCHAR2 (100) := '5';
          V_TASK_DRTN            NUMBER(15);
          V_ETL_ENDTIME          DATE;
          V_DATA_PROCESS_DATE    NUMBER  := TO_NUMBER(TO_CHAR(P_SUM_DATE,'YYYYMMDD'));
          ---------------------------------------------         
         v_Row_ID       Varchar2(100);
    Begin
          --------------------------------------------------------------------
          --TASK#01
          V_ETL_PROCESSOR_NAME    := 'PRC_RANK_VIP_HISTORY';      
          V_TASK_STARTTIME	      := SYSDATE;
          V_ETL_TASK_NAME		      := 'PRC_RANK_VIP_HISTORY#MERGE';
          V_SOURCE_OBJECT_NAME	  := 'V_VIP_TH';
          V_TARGET_TABLE		      := 'DWB_RANK_VIP';
          V_TARGET_RECORD_CNT	    := 0;            
          --------------------------------------------------------------------
          MERGE INTO DWB_RANK_VIP A 
          USING 
          ( 
              SELECT  
                  M.MEMBER_ID, 
                  M.MEM_TYPE, 
                  M.MEMBER_KEY, 
                  V.SHORT_NAME, 
                  R.RANK_ID FROM_RANK_ID, 
                  R.RANK_NAME FROM_RANK_NAME, 
                  R1.RANK_ID TO_RANK_ID, 
                  R1.RANK_NAME TO_RANK_NAME, 
                  V.VIP_TYPE,                                                    
                  V.STA_DATE , 
                  V.END_DATE, 
                  V.DESCRIPTION 
              FROM DWR_MEMBER M  
              JOIN V_VIP_TH V ON M.MEMBER_KEY = V.ISDN  
              JOIN DWL_RANK R ON V.SHORT_NAME = R.RANK_CODE AND R.STATUS = '1' 
              JOIN DWL_RANK_EXCHANGE RE ON R.RANK_ID = RE.FROM_RANK_ID AND RE.STA_DATETIME <= TRUNC(SYSDATE) AND TRUNC(SYSDATE) <= NVL(RE.END_DATETIME,TRUNC(SYSDATE)) 
              JOIN DWL_RANK R1 ON R1.RANK_ID = RE.TO_RANK_ID AND R1.STATUS = '1' 
              WHERE M.STATUS = '1' AND M.END_DATETIME IS NULL 
          ) B ON (A.MEMBER_ID = B.MEMBER_ID AND A.MEMBER_KEY = B.MEMBER_KEY AND A.STA_DATETIME = B.STA_DATE) 
          WHEN MATCHED 
          THEN 
              UPDATE SET END_DATETIME = LAST_DAY(P_SUM_DATE), STATUS = '1', VIP_STATUS = '1', NOTE = B.DESCRIPTION, LAST_UPD_DATE = SYSDATE,LAST_UPD_BY = 'SYSTEM', REASON_CODE = 'RANK_03'
          WHEN NOT MATCHED 
          THEN 
              INSERT (RANK_VIP_ID,MEMBER_ID,MEMBER_KEY,RANK_ID,STA_DATETIME,END_DATETIME,STATUS,VIP_STATUS,NOTE,CREATE_DATE,CREATE_BY,LAST_UPD_DATE,LAST_UPD_BY, REASON_CODE)  
              VALUES (RANK_VIP_SEQ.NEXTVAL,B.MEMBER_ID,B.MEMBER_KEY,B.TO_RANK_ID,B.STA_DATE,LAST_DAY(P_SUM_DATE),'1','1',B.DESCRIPTION,SYSDATE,'SYSTEM',SYSDATE,'SYSTEM','RANK_03');            

          V_TARGET_RECORD_CNT :=  SQL%ROWCOUNT;
          V_TASK_DRTN := ROUND((SYSDATE - V_TASK_STARTTIME)*24*60*60,0);
          V_TASK_ENDTIME := SYSDATE;
          V_ETL_ENDTIME := SYSDATE;
          /*INSERT STG_ETL_LOG*/
          PCK_LOG.INSERT_STG_ETL_LOG
          (
              V_ETL_STARTIME,
              V_ETL_SERVER_NAME,V_ETL_PROCESSOR_NAME,
              V_ETL_TASK_NAME,V_SOURCE_SYSTEM_CD,
              V_SOURCE_OBJECT_TYPE,
              V_SOURCE_OBJECT_NAME,V_TARGET_TABLE,V_SOURCE_RECORD_CNT,
              V_TARGET_RECORD_CNT,V_TASK_STARTTIME,V_TASK_ENDTIME,
              V_TASK_STATUS_CD,
              V_STATUS,V_TASK_DRTN,V_ETL_ENDTIME,V_ETL_USERNAME,V_DATA_PROCESS_DATE
          );                
          --------------------------------------------------------------------
          --TASK#01
          V_ETL_PROCESSOR_NAME    := 'PRC_RANK_VIP_HISTORY';      
          V_TASK_STARTTIME	      := SYSDATE;
          V_ETL_TASK_NAME		      := 'PRC_RANK_VIP_HISTORY#MERGE';
          V_SOURCE_OBJECT_NAME	  := 'DWB_RANK_HISTORY';
          V_TARGET_TABLE		      := 'DWB_RANK_VIP';
          V_TARGET_RECORD_CNT	    := 0;            
          --------------------------------------------------------------------            
          For v_Data in c_Data Loop
              V_TARGET_RECORD_CNT := V_TARGET_RECORD_CNT + 1;
              Open c_Rank_Vip(v_Data.Member_ID,v_Data.Member_Key,v_Data.Reason_Code,v_Data.rank_id);
              Fetch c_Rank_Vip into v_Row_ID;
              Close c_Rank_Vip;

              If(v_Row_ID is not null) Then
                  update DWB_RANK_VIP set END_DATETIME=LAST_DAY(p_SUM_DATE),LAST_UPD_DATE=sysdate,LAST_UPD_BY='PRC_RANK_VIP_HISTORY' Where rowid = v_Row_ID;
              Else
                  Insert into DWB_RANK_VIP (RANK_VIP_ID,MEMBER_ID,MEMBER_KEY,RANK_ID,STA_DATETIME,END_DATETIME,STATUS,VIP_STATUS,NOTE,CREATE_DATE,CREATE_BY,LAST_UPD_DATE,LAST_UPD_BY, REASON_CODE)
                  Values(RANK_VIP_SEQ.NEXTVAL,v_Data.MEMBER_ID,v_Data.MEMBER_KEY,v_Data.RANK_ID,Trunc(P_SUM_DATE,'MONTH'),LAST_DAY(P_SUM_DATE),'1','1','Thang Hang Noi bo theo RANK_HISTORY',SYSDATE,'SYSTEM',SYSDATE,'SYSTEM',v_Data.REASON_CODE);
              End If;
              v_Row_ID := null;
          End Loop;

          /*INSERT STG_ETL_LOG*/
          PCK_LOG.INSERT_STG_ETL_LOG
          (
              V_ETL_STARTIME,
              V_ETL_SERVER_NAME,V_ETL_PROCESSOR_NAME,
              V_ETL_TASK_NAME,V_SOURCE_SYSTEM_CD,
              V_SOURCE_OBJECT_TYPE,
              V_SOURCE_OBJECT_NAME,V_TARGET_TABLE,V_SOURCE_RECORD_CNT,
              V_TARGET_RECORD_CNT,V_TASK_STARTTIME,V_TASK_ENDTIME,
              V_TASK_STATUS_CD,
              V_STATUS,V_TASK_DRTN,V_ETL_ENDTIME,V_ETL_USERNAME,V_DATA_PROCESS_DATE
          );         
    EXCEPTION
        WHEN OTHERS THEN
            Rollback;
            RAISE_APPLICATION_ERROR(-20084,SQLERRM);
    END; -- PROCEDUR
END;
/
