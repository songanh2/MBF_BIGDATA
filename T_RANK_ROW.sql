--------------------------------------------------------
--  File created - Friday-June-30-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Type T_RANK_ROW
--------------------------------------------------------

  CREATE OR REPLACE TYPE "KNDL_GOLD"."T_RANK_ROW" FORCE  AS OBJECT 
(
  ORDER_ID          NUMBER,  
  RANK_CODE         VARCHAR2(10),
  MEMBER_ID         NUMBER,
  MEMBER_KEY        VARCHAR2(20),
  RANK_ORDER        NUMBER,
  RANK_MARK         NUMBER,
  FROM_MARK         NUMBER,
  TO_MARK           NUMBER,
  MIN_MARK          NUMBER,
  FROM_RANK_ID      NUMBER,
  TO_RANK_ID        NUMBER,
  PARTNER_ID        NUMBER,
  PARTNER_RANK_ID   NUMBER,
  MONTHS            NUMBER,
  ALERT_MARK        NUMBER,
  OLD_RANK_ID       NUMBER,
  OLD_RANK_ORDER    NUMBER,
  REASON_CODE       VARCHAR2(50),
  USE_MONTHS        NUMBER,
  RANK_DATE         DATE,
  MOB_TYPE          VARCHAR2(10),
  RANK_TYPE         VARCHAR2(10),
  OLD_MEMBER_LINK_ID    NUMBER,
  NEW_MEMBER_LINK_ID    NUMBER,
  STA_DATETIME      DATE, 
  END_DATETIME      DATE,
  CREATE_BY         VARCHAR2(30),
  ROW_ID            VARCHAR2(50),
  ONNET_SUB         VARCHAR2(1)
);

/
