-- table used (query to create it is at the end)
REST.TEST.Risk_Segmentation_03_31_2021;

-- page 10: Within Tier 1, non-DDers are the ones driving the high ATM dispute rate 
select 
TYPE_OF_TRXN,
CASE WHEN DD_AMT > 0 THEN 'DD>0'
ELSE 'DD=0'
END as DD_AMT_BIN,
sum(case when FLG_DISPUTED=1 then FINAL_AMT else 0 end)/sum(FINAL_AMT) as dispute_rate
from REST.TEST.Risk_Segmentation_03_31_2021
where LATEST_TIER = 'TIER1'
and DATE_TRUNC('month', TRANS_DATE)::date = '2021-02-01'
and TYPE_OF_TRXN in ('Debit Purchase','ATM Withdrawals' )
group by 1,2
order by 1,2;
       
-- decline rate across time
select 
TYPE_OF_TRXN,
DATE_TRUNC('week', TRANS_DATE)::date,
LATEST_TIER,
COUNT(DISTINCT DEC_USER)/COUNT(DISTINCT USER_ID) as decline_rate
from REST.TEST.Risk_Segmentation_03_31_2021
where trans_date >= '2020-09-28' and  trans_date  <= '2021-03-28'
and TYPE_OF_TRXN in ('Debit Purchase','ATM Withdrawals', 'Credit Purchase')
group by 1,2,3
order by 1,2,3;

-- within tier 1
-- decline rate across time
select 
TYPE_OF_TRXN,
DATE_TRUNC('week', TRANS_DATE)::date,
CASE WHEN DD_AMT > 0 THEN 'DD>0'
ELSE 'DD=0'
END as DD_AMT_BIN,
COUNT(DISTINCT DEC_USER)/COUNT(DISTINCT USER_ID) as decline_rate
from REST.TEST.Risk_Segmentation_03_31_2021
where trans_date >= '2020-09-28' and  trans_date  <= '2021-03-28'
and TYPE_OF_TRXN in ('Debit Purchase','ATM Withdrawals')
and LATEST_TIER = 'TIER1'
group by 1,2,3
order by 1,2,3;

-- page 15: With the current framework, 1.9% of active MMs experience a declined transaction due to limits (Q4 20)
select 
--TYPE_OF_TRXN,
DATE_TRUNC('month', TRANS_DATE)::date,
--LATEST_TIER,
COUNT(DISTINCT DEC_USER) as decline_users,
COUNT(DISTINCT USER_ID) as total_users,
COUNT(DISTINCT DEC_USER)/COUNT(DISTINCT USER_ID) as decline_rate
from REST.TEST.Risk_Segmentation_03_31_2021
where trans_date >= '2020-10-01' and  trans_date  <= '2020-12-31'
and TYPE_OF_TRXN in ('Debit Purchase','ATM Withdrawals', 'Credit Purchase')
group by 1
order by 1 desc;
--group by 1,2,3
--order by 1,2,3;

-- page 15: answer brian's question to compare with the version with suspended/cancelled accounts removed
-- results saved here https://docs.google.com/spreadsheets/d/115CAzH6KBwd1H203cRbGweru2hxf-A-gBNq-ce1HtWY/edit#gid=0
select 
DATE_TRUNC('month', TRANS_DATE)::date,
COUNT(DISTINCT DEC_USER),
COUNT(DISTINCT USER_ID),
COUNT(DISTINCT DEC_USER)/COUNT(DISTINCT USER_ID)
from REST.TEST.double_check_0331
where trans_date >= '2020-10-01' and  trans_date  <= '2020-12-31'
and TYPE_OF_TRXN in ('Debit Purchase','ATM Withdrawals', 'Credit Purchase')
and user_id not in (
 select distinct id as user_id
 from (
        (select
        users.id,
        case when users.status = 'suspended' then 1
             when users.status = 'cancelled' and adr_cancellation.reason <> 'member' then 1
             when users.status = 'cancelled_no_refund' and adr_cancellation.reason <> 'member' then 1
             else 0
        end as if_suspended_or_cancelled,
        users.status,
        adr_cancellation.reason
        FROM "MYSQL_DB"."CHIME_PROD"."USERS" users
        LEFT JOIN "MYSQL_DB"."CHIME_PROD"."USER_ACCOUNT_DEACTIVATIONS" AS uad_cancellation
        ON users.id = uad_cancellation.user_id
        AND uad_cancellation.account_deactivation_reason_type = 'AccountCancellationReason'
        LEFT JOIN "MYSQL_DB"."CHIME_PROD"."ACCOUNT_DEACTIVATION_REASONS" AS adr_cancellation
        ON uad_cancellation.account_deactivation_reason_id = adr_cancellation.id
        where if_suspended_or_cancelled = 1) tmp )) 
group by 1;

-- check distribution of suspended/cancelled
select
if_suspended_or_cancelled,
count(distinct id)
from
(
  select
  users.id,
  case when users.status = 'suspended' then 1
       when users.status = 'cancelled' and adr_cancellation.reason <> 'member' then 1
       when users.status = 'cancelled_no_refund' and adr_cancellation.reason <> 'member' then 1
       else 0
  end as if_suspended_or_cancelled,
  users.status,
  adr_cancellation.reason
  FROM "MYSQL_DB"."CHIME_PROD"."USERS" users
  LEFT JOIN "MYSQL_DB"."CHIME_PROD"."USER_ACCOUNT_DEACTIVATIONS" AS uad_cancellation
  ON users.id = uad_cancellation.user_id
  AND uad_cancellation.account_deactivation_reason_type = 'AccountCancellationReason'
  LEFT JOIN "MYSQL_DB"."CHIME_PROD"."ACCOUNT_DEACTIVATION_REASONS" AS adr_cancellation
  ON uad_cancellation.account_deactivation_reason_id = adr_cancellation.id) tmp
group by 1;

        
        
        
        
        
        
        
        
        
-- Shu's logic to build the temp table
CREATE OR REPLACE TABLE REST.TEST.Risk_Segmentation_03_31_2021 AS (
WITH STRICT_PDD AS (
 SELECT TO_DATE(POST_DATE) AS TRXN_DATE,
  DATEADD(DAY, -32, TO_DATE(POST_DATE)) AS TRXN_DATE_1M,
  USER_ID,
  SUM(TRANSACTION_AMOUNT) AS STRICT_PDD_AMT,
  COUNT(ID) AS NUM_DD
 FROM REST.TEST.PAYROLL_DIRECT_DEPOSITS_SL_02_17_2021 --ANALYTICS.LOOKER.PAYROLL_DIRECT_DEPOSITS
 GROUP BY 1, 2, 3
)

, SUMCUM_STRICT_PDD AS (
  SELECT A.USER_ID,
  A.TRXN_DATE,
  SUM(B.STRICT_PDD_AMT) AS DD_AMT
  FROM STRICT_PDD AS A
  LEFT JOIN STRICT_PDD AS B
  ON A.USER_ID = B.USER_ID
    AND (B.TRXN_DATE>=A.TRXN_DATE_1M AND B.TRXN_DATE<=A.TRXN_DATE)
  GROUP BY 1, 2
)

, max_DD AS (
    SELECT *
    FROM SUMCUM_STRICT_PDD
    WHERE TRXN_DATE >= '2020-02-11'
    QUALIFY (ROW_NUMBER() OVER (PARTITION BY USER_ID ORDER BY DD_AMT DESC)) = 1  -- keep the first date when that tier upgrade happens
)

, REAL_TIER_CHANGE AS (
    SELECT
    ID,
    --DATE_PT AS UPGRADE_DT,
    "TIMESTAMP":: DATE AS UPGRADE_DT,
    USER_ID,
    -- new_product_id => new_product_id
    TRY_TO_NUMBER(dd_amount, 10, 2) AS dd_amount,
    TO_CHAR(new_product_id) AS new_product_id,
    TO_CHAR(old_product_id) AS old_product_id,
    (CASE
      --WHEN TO_CHAR(new_product_id) = 'virtual_tier4' THEN 'TIER4'
      WHEN TRY_TO_NUMBER(TO_CHAR(new_product_id))  IN (6524,6523,6495,6287,6216,6254,6200,6049,6048,5887,5176,5048,2295,2294,2239,2070,2068) THEN 'TIER1' --02/19:add (6524,2295,2294,2070)
      WHEN TRY_TO_NUMBER(TO_CHAR(new_product_id))  IN (8069,6526,6525,6222,6121,5888,2300,2298,2297,2246,2081,2067) THEN 'TIER2'  --02/16Update: IN (6525,6222,6121,5888,2081,2246) --02/19:add (8069,6526,2300,2067)
      WHEN TRY_TO_NUMBER(TO_CHAR(new_product_id))   IN (6638,6627,2299,2240,2074,2071) THEN 'TIER3' -- 02/16Update: IN (6638,6627,2071) THEN 'TIER3'
      WHEN TRY_TO_NUMBER(TO_CHAR(new_product_id))   IN (2227,2226,2225,2189) THEN 'TIER4' -- 02/19Update: add Tier4
      ELSE 'OTHERS'
    END) AS new_tier,
    (CASE
      --WHEN TO_CHAR(old_product_id) = 'virtual_tier4' THEN 'TIER4'
      WHEN TRY_TO_NUMBER(TO_CHAR(old_product_id))  IN (6524,6523,6495,6287,6216,6254,6200,6049,6048,5887,5176,5048,2295,2294,2239,2070,2068) THEN 'TIER1' --02/19:add (6524,2295,2294,2070)
      WHEN TRY_TO_NUMBER(TO_CHAR(old_product_id))  IN (8069,6526,6525,6222,6121,5888,2300,2298,2297,2246,2081,2067) THEN 'TIER2'  --02/16Update: IN (6525,6222,6121,5888,2081,2246) --02/19:add (8069,6526,2300,2067)
      WHEN TRY_TO_NUMBER(TO_CHAR(old_product_id))  IN (6638,6627,2299,2240,2074,2071) THEN 'TIER3' -- 02/16Update: IN (6638,6627,2071) THEN 'TIER3'
      WHEN TRY_TO_NUMBER(TO_CHAR(old_product_id))  IN (2227,2226,2225,2189) THEN 'TIER4' -- 02/19Update: add Tier4
      ELSE 'OTHERS'
    END) AS old_tier
    --FROM ANALYTICS.LOOKER.SIO_TRACKS
    FROM SEGMENT.CHIME_PROD.PRODUCT_ID_UPGRADED_FOR_HIGHER_SPENDING_LIMITS
    WHERE new_tier <> old_tier AND UPGRADE_DT >= '2020-02-11'
    AND new_product_id != 'virtual_tier4'
    --WHERE EVENT = 'product_id upgraded for higher spending limits' AND new_tier <> old_tier AND UPGRADE_DT >= '2020-02-11'
    QUALIFY (ROW_NUMBER() OVER (PARTITION BY USER_ID, NEW_TIER ORDER BY UPGRADE_DT)) = 1  -- keep the first date when that tier upgrade happens
)

-- Add records before 2020-02-11
,ALL_REAL_TIER_CHANGE AS (
    SELECT *
    FROM REAL_TIER_CHANGE
    UNION
     (SELECT TO_CHAR(-1) AS ID, TO_DATE(UPDATED_AT) AS UPGRADE_DT, USER_ID, NULL AS DD_AMOUNT, TO_CHAR(PRODUCT_ID) AS NEW_PRODUCT_ID, NULL AS OLD_PRODUCT_ID, 'TIER2' AS NEW_TIER, 'TIER1' AS OLD_TIER
         FROM MYSQL_DB.CHIME_PROD.USER_BANK_ACCOUNTS
         WHERE TYPE = 'checking' AND UPDATED_AT < '2020-02-11'
         AND PRODUCT_ID IN (8069,6526,6525,6222,6121,5888,2300,2298,2297,2246,2081,2067) -- TIER2 ONLY -02/19: update PIDs
         QUALIFY (RANK() OVER (PARTITION BY USER_ID ORDER BY UPDATED_AT DESC)) = 1)
)


,DATES_REAL_NEW_TIER_CHANGE AS (
  SELECT
  USER_ID,
  (CASE WHEN
      (SUM(CASE WHEN NEW_TIER='TIER3' AND OLD_TIER = 'TIER2' THEN 1 ELSE 0 END) > 0
      AND
      SUM(CASE WHEN NEW_TIER='TIER2' AND OLD_TIER = 'TIER1' THEN 1 ELSE 0 END) = 0)
      OR
      (SUM(CASE WHEN NEW_TIER='TIER4' AND OLD_TIER = 'TIER2' THEN 1 ELSE 0 END) > 0
      AND
      SUM(CASE WHEN NEW_TIER='TIER2' AND OLD_TIER = 'TIER1' THEN 1 ELSE 0 END) = 0)
  THEN TO_DATE('2020-02-11')
  ELSE MIN(CASE WHEN NEW_TIER='TIER2' THEN UPGRADE_DT END)
  END) AS FIRST_DATE_TIER2,

  MIN(CASE WHEN NEW_TIER='TIER3' THEN UPGRADE_DT END) AS FIRST_DATE_TIER3,
  MIN(CASE WHEN NEW_TIER='TIER4' THEN UPGRADE_DT END) AS FIRST_DATE_TIER4

  FROM ALL_REAL_TIER_CHANGE
  GROUP BY 1
)



,WEEKS_DATES AS (
  SELECT DISTINCT
  TO_DATE(TRANS_DATE) AS TRANS_DATE,
  DAYOFWEEK(TRANS_DATE) AS DOW,
  (CASE
      WHEN DAYOFWEEK(TRANS_DATE)=1 THEN TO_DATE(TRANS_DATE)
      WHEN DAYOFWEEK(TRANS_DATE)=0 THEN DATEADD(DAY, -6, TO_DATE(TRANS_DATE))
      ELSE DATEADD(DAY, -1*(DAYOFWEEK(TRANS_DATE)-1), TO_DATE(TRANS_DATE))
  END) AS START_WEEK
  FROM MYSQL_DB.CHIME_PROD.REALTIME_AUTH_EVENTS
)

, DECLINE_RATE_TRXN AS(
    SELECT DISTINCT
          --TO_DATE(A.TRANSACTION_TIMESTAMP) AS TRANS_DATE,
          B.START_WEEK,
          (CASE WHEN A.UNIQUE_PROGRAM_ID IN (600, 278, 1014) THEN 'Credit Purchase'
          WHEN A.MERCHANT_CATEGORY_CODE IN (6010, 6011) THEN 'ATM Withdrawals'
          ELSE 'Debit Purchase'
          END) AS TYPE_OF_TRXN,
          A.USER_ID
    FROM mysql_db.galileo.galileo_authorized_transactions A
    LEFT JOIN WEEKS_DATES AS B
      ON TO_DATE(A.TRANSACTION_TIMESTAMP)=B.TRANS_DATE
    WHERE AUTHORIZATION_RESPONSE = 61
    AND TRANSACTION_TIMESTAMP >= '2020-02-01'
)
-- 02-19 add socure synth scores
,socure_scores AS
( -- some user
SELECT USER_ID
        , MIN(socure_chime_score_final) AS socure_chime_score
        , MIN(socure_generic_score_final) AS socure_generic_score
        , MIN(socure_sigma_score_final) AS socure_sigma_score
        , MIN(socure_sigma_score_v2_final) AS socure_sigma_score_v2
        , MIN(socure_synth_score_final) AS socure_synth_score
  FROM
        (select user_id, created_at
                , case when parse_json(result):fraud:scores[0]:name like 'Chime.3%' then parse_json(result):fraud:scores[0]:score
                       when parse_json(result):fraud:scores[1]:name like 'Chime.3%' then parse_json(result):fraud:scores[1]:score
                       when parse_json(result):fraud:scores[2]:name like 'Chime.3%' then parse_json(result):fraud:scores[2]:score
                       when parse_json(result):fraud:scores[3]:name like 'Chime.3%' then parse_json(result):fraud:scores[3]:score
                       when parse_json(result):fraud:scores[4]:name like 'Chime.3%' then parse_json(result):fraud:scores[4]:score
                       when parse_json(result):fraud:scores[5]:name like 'Chime.3%' then parse_json(result):fraud:scores[5]:score
                       else null
                    end as socure_chime_score
                , case when parse_json(result):fraud:scores[0]:name = 'generic' then parse_json(result):fraud:scores[0]:score
                       when parse_json(result):fraud:scores[1]:name = 'generic' then parse_json(result):fraud:scores[1]:score
                       when parse_json(result):fraud:scores[2]:name = 'generic' then parse_json(result):fraud:scores[2]:score
                       when parse_json(result):fraud:scores[3]:name = 'generic' then parse_json(result):fraud:scores[3]:score
                       when parse_json(result):fraud:scores[4]:name = 'generic' then parse_json(result):fraud:scores[4]:score
                       when parse_json(result):fraud:scores[5]:name = 'generic' then parse_json(result):fraud:scores[5]:score
                       else null
                    end as socure_generic_score
                , case when parse_json(result):fraud:scores[0]:name = 'sigma' and parse_json(result):fraud:scores[0]:version = '1.0' then parse_json(result):fraud:scores[0]:score
                       when parse_json(result):fraud:scores[1]:name = 'sigma' and parse_json(result):fraud:scores[1]:version = '1.0' then parse_json(result):fraud:scores[1]:score
                       when parse_json(result):fraud:scores[2]:name = 'sigma' and parse_json(result):fraud:scores[2]:version = '1.0' then parse_json(result):fraud:scores[2]:score
                       when parse_json(result):fraud:scores[3]:name = 'sigma' and parse_json(result):fraud:scores[3]:version = '1.0' then parse_json(result):fraud:scores[3]:score
                       when parse_json(result):fraud:scores[4]:name = 'sigma' and parse_json(result):fraud:scores[4]:version = '1.0' then parse_json(result):fraud:scores[4]:score
                       when parse_json(result):fraud:scores[5]:name = 'sigma' and parse_json(result):fraud:scores[5]:version = '1.0' then parse_json(result):fraud:scores[5]:score
                       else null
                    end as socure_sigma_score
                ,  case when parse_json(result):fraud:scores[0]:name = 'sigma' and parse_json(result):fraud:scores[0]:version = '2.0' then parse_json(result):fraud:scores[0]:score
                       when parse_json(result):fraud:scores[1]:name = 'sigma' and parse_json(result):fraud:scores[1]:version = '2.0' then parse_json(result):fraud:scores[1]:score
                       when parse_json(result):fraud:scores[2]:name = 'sigma' and parse_json(result):fraud:scores[2]:version = '2.0' then parse_json(result):fraud:scores[2]:score
                       when parse_json(result):fraud:scores[3]:name = 'sigma' and parse_json(result):fraud:scores[3]:version = '2.0' then parse_json(result):fraud:scores[3]:score
                       when parse_json(result):fraud:scores[4]:name = 'sigma' and parse_json(result):fraud:scores[4]:version = '2.0' then parse_json(result):fraud:scores[4]:score
                       when parse_json(result):fraud:scores[5]:name = 'sigma' and parse_json(result):fraud:scores[5]:version = '2.0' then parse_json(result):fraud:scores[5]:score
                       else null
                    end as socure_sigma_score_v2
                --new synthetic score
                , case when parse_json(result):synthetic:scores[0]:name = 'synthetic' then parse_json(result):synthetic:scores[0]:score
                       when parse_json(result):synthetic:scores[1]:name = 'synthetic' then parse_json(result):synthetic:scores[1]:score
                       when parse_json(result):synthetic:scores[2]:name = 'synthetic' then parse_json(result):synthetic:scores[2]:score
                       when parse_json(result):synthetic:scores[3]:name = 'synthetic' then parse_json(result):synthetic:scores[3]:score
                       when parse_json(result):synthetic:scores[4]:name = 'synthetic' then parse_json(result):synthetic:scores[4]:score
                       when parse_json(result):synthetic:scores[5]:name = 'synthetic' then parse_json(result):synthetic:scores[5]:score
                       else null
                       end as socure_synth_score

                --pull the most recent non-NULL value
                , first_value(socure_chime_score) ignore nulls over(partition by user_id order by api.created_at desc) as socure_chime_score_final
                , first_value(socure_generic_score) ignore nulls over(partition by user_id order by api.created_at desc) as socure_generic_score_final
                , first_value(socure_sigma_score) ignore nulls over(partition by user_id order by api.created_at desc) as socure_sigma_score_final
                , first_value(socure_sigma_score_v2) ignore nulls over(partition by user_id order by api.created_at desc) as socure_sigma_score_v2_final
                , first_value(socure_synth_score) ignore nulls over(partition by user_id order by api.created_at desc) as socure_synth_score_final

        FROM MYSQL_DB.CHIME_PROD.external_api_requests api
        where service='socure3'
            --AND api.created_at > '2020-10-12'
            and  CHECK_JSON(result) is null --checking for valid JSON's
        )
  GROUP BY 1
)

, socure_score_contact as (
  select user_id
        , max(TRY_PARSE_JSON(result):emailRisk:score) as socure_email_risk_score
        , max(TRY_PARSE_JSON(result):phoneRisk:score) as socure_phone_risk_score
        , max(TRY_PARSE_JSON(result):addressRisk:score) as socure_address_risk_score
  from MYSQL_DB.CHIME_PROD.external_api_requests
  where service = 'socure3'
  GROUP BY 1
)
-----

, ALL_TRANS AS(
  SELECT DISTINCT
  A.TRANSACTION_TIMESTAMP AS TRANS_DATE,
  C.START_WEEK,
  A.USER_ID,
  ABS(A.TRANSACTION_AMOUNT) AS FINAL_AMT,
  A.MERCHANT_CATEGORY_CODE AS MCC,
  A.ID,
  A.INTERCHANGE_FEE_AMOUNT,
  A.TYPE_OF_TRXN,
  D.FIRST_DATE_TIER2,
  D.FIRST_DATE_TIER3,
  D.FIRST_DATE_TIER4,
  (CASE
      WHEN TO_DATE(A.TRANSACTION_TIMESTAMP)>=D.FIRST_DATE_TIER4 THEN 'TIER4'
      WHEN TO_DATE(A.TRANSACTION_TIMESTAMP)>=D.FIRST_DATE_TIER3 THEN 'TIER3'
      WHEN TO_DATE(A.TRANSACTION_TIMESTAMP)>=D.FIRST_DATE_TIER2 THEN 'TIER2'
      ELSE 'TIER1'
    END) AS ADMIN_REAL_LIMIT,
   (CASE WHEN D.USER_ID IS NOT NULL AND D.FIRST_DATE_TIER4 IS NOT NULL THEN 'TIER4'
         WHEN D.USER_ID IS NOT NULL AND D.FIRST_DATE_TIER3 IS NOT NULL THEN 'TIER3'
         WHEN D.USER_ID IS NOT NULL AND D.FIRST_DATE_TIER2 IS NOT NULL THEN 'TIER2'
    ELSE 'TIER1' END) AS LATEST_TIER,
  (CASE WHEN E.TRANSACTION_ID IS NOT NULL THEN 1 ELSE 0 END) AS FLG_DISPUTED,
  (CASE WHEN E.TRANSACTION_ID IS NOT NULL THEN A.USER_ID END) AS FLG_DISPUTED_USER,
  F.USER_ID AS DEC_USER,
  -- DD Amount Data
  M.USER_ID AS DD_USER,
  M.TRXN_DATE AS DD_TRXN_DATE,
  M.DD_AMT,
  /*
  (CASE WHEN DD_AMT >= 200 AND DD_AMT < 400 THEN '2.200-400'
       WHEN DD_AMT >= 400 AND DD_AMT < 600 THEN '3.400-600'
       WHEN DD_AMT >= 600 AND DD_AMT < 800 THEN '4.600-800'
       WHEN DD_AMT >= 800 AND DD_AMT < 1000 THEN '5.800-1000'
       WHEN DD_AMT >= 1000 THEN '6.>=1000'
       WHEN DD_AMT > 0 AND DD_AMT < 200 THEN '1.<200'
       --ELSE '1.<200'
       ELSE '0.NonDDer'
       END) AS DD_AMT_BIN
  */
    (CASE WHEN DD_AMT >= 200 THEN '2.>=200'
       WHEN DD_AMT >= 10 AND DD_AMT < 200 THEN '1.10-200'
       --ELSE '1.<200'
       ELSE '0.<10'
       END) AS DD_AMT_BIN,
  E.REASON,
  --SOCURE SCORE
  S1.socure_chime_score,
  S1.socure_generic_score,
  S1.socure_sigma_score,
  S1.socure_sigma_score_v2,
  S1.socure_synth_score,

  s2.socure_email_risk_score,
  s2.socure_phone_risk_score,
  s2.socure_address_risk_score,
  A.AUTHORIZATION_CODE


  FROM
    (SELECT *,
    (CASE WHEN (MERCHANT_CATEGORY_CODE) IN (6011, 6010) OR ((MERCHANT_CATEGORY_CODE = 0) AND
                   (TRANSACTION_CODE in ('VSW','MPW','MPM', 'MPR','PLW', 'PLR','PRW', 'SDW'))) THEN 'ATM Withdrawals'
       WHEN (transaction_code in ('ISA', 'ISC', 'ISJ', 'ISL', 'ISM', 'ISR', 'ISZ', 'VSA', 'VSC', 'VSJ', 'VSL', 'VSM', 'VSR', 'VSZ',
                 'SDA', 'SDC', 'SDL', 'SDM', 'SDR', 'SDV', 'SDZ', 'PLM', 'PLA', 'PRA') AND unique_program_id IN (600, 278, 1014) AND transaction_amount < 0) THEN 'Credit Purchase'
       WHEN transaction_code in ('ISA', 'ISC', 'ISJ', 'ISL', 'ISM', 'ISR', 'ISZ', 'VSA', 'VSC', 'VSJ', 'VSL', 'VSM', 'VSR', 'VSZ',
                 'SDA', 'SDC', 'SDL', 'SDM', 'SDR', 'SDV', 'SDZ', 'PLM', 'PLA', 'PRA') AND transaction_amount < 0 THEN 'Debit Purchase'
       WHEN TRANSACTION_CODE = 'ADS' THEN 'ACH Push'
       WHEN TRANSACTION_CODE = 'PMAC' THEN 'ACH Pull'
       ELSE 'Others'
       END) AS TYPE_OF_TRXN,
      (case when transaction_code like 'AD%' then 4
            when transaction_code like 'FE%' then 5
            when transaction_code like 'IS%' then 6
            when transaction_code like 'PM%' then 7
            when transaction_code like 'SD%' then 8
            when transaction_code like 'VS%' then 9 else 0 end) AS leading_num
     FROM ANALYTICS.LOOKER.TRANSACTIONS) AS A
  LEFT JOIN WEEKS_DATES AS C
  ON TO_DATE(A.TRANSACTION_TIMESTAMP)=C.TRANS_DATE
  LEFT JOIN DATES_REAL_NEW_TIER_CHANGE D
  ON A.USER_ID = D.USER_ID
  LEFT JOIN (SELECT DISTINCT A.USER_ID, B.TRANSACTION_ID, A.REASON
             FROM ANALYTICS.LOOKER.USER_DISPUTE_CLAIMS AS A
             LEFT JOIN ANALYTICS.LOOKER.USER_DISPUTE_CLAIM_TRANSACTIONS AS B
             ON A.ID=B.USER_DISPUTE_CLAIM_ID) AS E
  ON A.USER_ID=E.USER_ID
  AND TO_NUMBER(CONCAT(A.leading_num, A.AUTHORIZATION_CODE)) = TO_NUMBER(E.TRANSACTION_ID)
  LEFT JOIN DECLINE_RATE_TRXN AS F
  ON C.START_WEEK=F.START_WEEK --weekly
    AND A.TYPE_OF_TRXN = F.TYPE_OF_TRXN
    AND A.USER_ID=F.USER_ID
  LEFT JOIN max_DD M
  ON M.USER_ID = A.USER_ID
  LEFT JOIN socure_scores S1
  ON A.USER_ID = S1.USER_ID
  LEFT JOIN socure_score_contact S2
  ON A.USER_ID = S2.USER_ID
  WHERE (TO_DATE(A.TRANSACTION_TIMESTAMP)>='2020-07-01'))
    --AND (TO_DATE(A.TRANSACTION_TIMESTAMP)< '2021-02-01')
    --AND DATEDIFF(WEEK, A.TRANSACTION_TIMESTAMP, CURRENT_DATE()) >= 1
    --AND A.TYPE_OF_TRXN IN ('ATM Withdrawals', 'Credit Purchase', 'Debit Purchase', 'ACH Push', 'ACH Pull'))
    --AND MCC>0)

SELECT * FROM ALL_TRANS
 --WHERE TYPE_OF_TRXN IN ('ATM Withdrawals')
);

                       
-- update Shu's logic to incorporate the simulated tier, latest tier and tier at transaction from 2020-01-01
CREATE OR REPLACE TABLE REST.TEST.Risk_Segmentation_04_12_2021 AS (
WITH STRICT_PDD AS (
 SELECT TO_DATE(POST_DATE) AS TRXN_DATE,
  DATEADD(DAY, -32, TO_DATE(POST_DATE)) AS TRXN_DATE_1M,
  USER_ID,
  SUM(TRANSACTION_AMOUNT) AS STRICT_PDD_AMT,
  COUNT(ID) AS NUM_DD
 FROM REST.TEST.PAYROLL_DIRECT_DEPOSITS_SL_02_17_2021 --ANALYTICS.LOOKER.PAYROLL_DIRECT_DEPOSITS
 GROUP BY 1, 2, 3
)

, SUMCUM_STRICT_PDD AS (
  SELECT A.USER_ID,
  A.TRXN_DATE,
  SUM(B.STRICT_PDD_AMT) AS DD_AMT
  FROM STRICT_PDD AS A
  LEFT JOIN STRICT_PDD AS B
  ON A.USER_ID = B.USER_ID
    AND (B.TRXN_DATE>=A.TRXN_DATE_1M AND B.TRXN_DATE<=A.TRXN_DATE)
  GROUP BY 1, 2
)

, max_DD AS (
    SELECT *
    FROM SUMCUM_STRICT_PDD
    WHERE TRXN_DATE >= '2020-02-11'
    QUALIFY (ROW_NUMBER() OVER (PARTITION BY USER_ID ORDER BY DD_AMT DESC)) = 1  -- keep the first date when that tier upgrade happens
)
  
, WEEKS_DATES AS (
  SELECT DISTINCT
  TO_DATE(TRANS_DATE) AS TRANS_DATE,
  DAYOFWEEK(TRANS_DATE) AS DOW,
  (CASE
      WHEN DAYOFWEEK(TRANS_DATE)=1 THEN TO_DATE(TRANS_DATE)
      WHEN DAYOFWEEK(TRANS_DATE)=0 THEN DATEADD(DAY, -6, TO_DATE(TRANS_DATE))
      ELSE DATEADD(DAY, -1*(DAYOFWEEK(TRANS_DATE)-1), TO_DATE(TRANS_DATE))
  END) AS START_WEEK
  FROM MYSQL_DB.CHIME_PROD.REALTIME_AUTH_EVENTS
)

, DECLINE_RATE_TRXN AS(
    SELECT DISTINCT
          --TO_DATE(A.TRANSACTION_TIMESTAMP) AS TRANS_DATE,
          B.START_WEEK,
          (CASE WHEN A.UNIQUE_PROGRAM_ID IN (600, 278, 1014) THEN 'Credit Purchase'
          WHEN A.MERCHANT_CATEGORY_CODE IN (6010, 6011) THEN 'ATM Withdrawals'
          ELSE 'Debit Purchase'
          END) AS TYPE_OF_TRXN,
          A.USER_ID
    FROM mysql_db.galileo.galileo_authorized_transactions A
    LEFT JOIN WEEKS_DATES AS B
      ON TO_DATE(A.TRANSACTION_TIMESTAMP)=B.TRANS_DATE
    WHERE AUTHORIZATION_RESPONSE = 61
    AND TRANSACTION_TIMESTAMP >= '2020-02-01'
)
-- 02-19 add socure synth scores
,socure_scores AS
( -- some user
SELECT USER_ID
        , MIN(socure_chime_score_final) AS socure_chime_score
        , MIN(socure_generic_score_final) AS socure_generic_score
        , MIN(socure_sigma_score_final) AS socure_sigma_score
        , MIN(socure_sigma_score_v2_final) AS socure_sigma_score_v2
        , MIN(socure_synth_score_final) AS socure_synth_score
  FROM
        (select user_id, created_at
                , case when parse_json(result):fraud:scores[0]:name like 'Chime.3%' then parse_json(result):fraud:scores[0]:score
                       when parse_json(result):fraud:scores[1]:name like 'Chime.3%' then parse_json(result):fraud:scores[1]:score
                       when parse_json(result):fraud:scores[2]:name like 'Chime.3%' then parse_json(result):fraud:scores[2]:score
                       when parse_json(result):fraud:scores[3]:name like 'Chime.3%' then parse_json(result):fraud:scores[3]:score
                       when parse_json(result):fraud:scores[4]:name like 'Chime.3%' then parse_json(result):fraud:scores[4]:score
                       when parse_json(result):fraud:scores[5]:name like 'Chime.3%' then parse_json(result):fraud:scores[5]:score
                       else null
                    end as socure_chime_score
                , case when parse_json(result):fraud:scores[0]:name = 'generic' then parse_json(result):fraud:scores[0]:score
                       when parse_json(result):fraud:scores[1]:name = 'generic' then parse_json(result):fraud:scores[1]:score
                       when parse_json(result):fraud:scores[2]:name = 'generic' then parse_json(result):fraud:scores[2]:score
                       when parse_json(result):fraud:scores[3]:name = 'generic' then parse_json(result):fraud:scores[3]:score
                       when parse_json(result):fraud:scores[4]:name = 'generic' then parse_json(result):fraud:scores[4]:score
                       when parse_json(result):fraud:scores[5]:name = 'generic' then parse_json(result):fraud:scores[5]:score
                       else null
                    end as socure_generic_score
                , case when parse_json(result):fraud:scores[0]:name = 'sigma' and parse_json(result):fraud:scores[0]:version = '1.0' then parse_json(result):fraud:scores[0]:score
                       when parse_json(result):fraud:scores[1]:name = 'sigma' and parse_json(result):fraud:scores[1]:version = '1.0' then parse_json(result):fraud:scores[1]:score
                       when parse_json(result):fraud:scores[2]:name = 'sigma' and parse_json(result):fraud:scores[2]:version = '1.0' then parse_json(result):fraud:scores[2]:score
                       when parse_json(result):fraud:scores[3]:name = 'sigma' and parse_json(result):fraud:scores[3]:version = '1.0' then parse_json(result):fraud:scores[3]:score
                       when parse_json(result):fraud:scores[4]:name = 'sigma' and parse_json(result):fraud:scores[4]:version = '1.0' then parse_json(result):fraud:scores[4]:score
                       when parse_json(result):fraud:scores[5]:name = 'sigma' and parse_json(result):fraud:scores[5]:version = '1.0' then parse_json(result):fraud:scores[5]:score
                       else null
                    end as socure_sigma_score
                ,  case when parse_json(result):fraud:scores[0]:name = 'sigma' and parse_json(result):fraud:scores[0]:version = '2.0' then parse_json(result):fraud:scores[0]:score
                       when parse_json(result):fraud:scores[1]:name = 'sigma' and parse_json(result):fraud:scores[1]:version = '2.0' then parse_json(result):fraud:scores[1]:score
                       when parse_json(result):fraud:scores[2]:name = 'sigma' and parse_json(result):fraud:scores[2]:version = '2.0' then parse_json(result):fraud:scores[2]:score
                       when parse_json(result):fraud:scores[3]:name = 'sigma' and parse_json(result):fraud:scores[3]:version = '2.0' then parse_json(result):fraud:scores[3]:score
                       when parse_json(result):fraud:scores[4]:name = 'sigma' and parse_json(result):fraud:scores[4]:version = '2.0' then parse_json(result):fraud:scores[4]:score
                       when parse_json(result):fraud:scores[5]:name = 'sigma' and parse_json(result):fraud:scores[5]:version = '2.0' then parse_json(result):fraud:scores[5]:score
                       else null
                    end as socure_sigma_score_v2
                --new synthetic score
                , case when parse_json(result):synthetic:scores[0]:name = 'synthetic' then parse_json(result):synthetic:scores[0]:score
                       when parse_json(result):synthetic:scores[1]:name = 'synthetic' then parse_json(result):synthetic:scores[1]:score
                       when parse_json(result):synthetic:scores[2]:name = 'synthetic' then parse_json(result):synthetic:scores[2]:score
                       when parse_json(result):synthetic:scores[3]:name = 'synthetic' then parse_json(result):synthetic:scores[3]:score
                       when parse_json(result):synthetic:scores[4]:name = 'synthetic' then parse_json(result):synthetic:scores[4]:score
                       when parse_json(result):synthetic:scores[5]:name = 'synthetic' then parse_json(result):synthetic:scores[5]:score
                       else null
                       end as socure_synth_score

                --pull the most recent non-NULL value
                , first_value(socure_chime_score) ignore nulls over(partition by user_id order by api.created_at desc) as socure_chime_score_final
                , first_value(socure_generic_score) ignore nulls over(partition by user_id order by api.created_at desc) as socure_generic_score_final
                , first_value(socure_sigma_score) ignore nulls over(partition by user_id order by api.created_at desc) as socure_sigma_score_final
                , first_value(socure_sigma_score_v2) ignore nulls over(partition by user_id order by api.created_at desc) as socure_sigma_score_v2_final
                , first_value(socure_synth_score) ignore nulls over(partition by user_id order by api.created_at desc) as socure_synth_score_final

        FROM MYSQL_DB.CHIME_PROD.external_api_requests api
        where service='socure3'
            --AND api.created_at > '2020-10-12'
            and  CHECK_JSON(result) is null --checking for valid JSON's
        )
  GROUP BY 1
)

, socure_score_contact as (
  select user_id
        , max(TRY_PARSE_JSON(result):emailRisk:score) as socure_email_risk_score
        , max(TRY_PARSE_JSON(result):phoneRisk:score) as socure_phone_risk_score
        , max(TRY_PARSE_JSON(result):addressRisk:score) as socure_address_risk_score
  from MYSQL_DB.CHIME_PROD.external_api_requests
  where service = 'socure3'
  GROUP BY 1
)
-----

, ALL_TRANS AS(
  SELECT DISTINCT
  A.TRANSACTION_TIMESTAMP AS TRANS_DATE,
  C.START_WEEK,
  A.USER_ID,
  ABS(A.TRANSACTION_AMOUNT) AS FINAL_AMT,
  A.MERCHANT_CATEGORY_CODE AS MCC,
  A.ID,
  A.INTERCHANGE_FEE_AMOUNT,
  A.TYPE_OF_TRXN,
  D.SIMULATED_TIER,
  D.TIER_AT_TRXN,
  D.LATEST_TIER,
  (CASE WHEN E.TRANSACTION_ID IS NOT NULL THEN 1 ELSE 0 END) AS FLG_DISPUTED,
  (CASE WHEN E.TRANSACTION_ID IS NOT NULL THEN A.USER_ID END) AS FLG_DISPUTED_USER,
  F.USER_ID AS DEC_USER,
  -- DD Amount Data
  M.USER_ID AS DD_USER,
  M.TRXN_DATE AS DD_TRXN_DATE,
  M.DD_AMT,
    (CASE WHEN DD_AMT >= 200 THEN '2.>=200'
       WHEN DD_AMT >= 10 AND DD_AMT < 200 THEN '1.10-200'
       --ELSE '1.<200'
       ELSE '0.<10'
       END) AS DD_AMT_BIN,
  E.REASON,
  --SOCURE SCORE
  S1.socure_chime_score,
  S1.socure_generic_score,
  S1.socure_sigma_score,
  S1.socure_sigma_score_v2,
  S1.socure_synth_score,

  s2.socure_email_risk_score,
  s2.socure_phone_risk_score,
  s2.socure_address_risk_score,
  A.AUTHORIZATION_CODE

  FROM
    (SELECT *,
    (CASE WHEN (MERCHANT_CATEGORY_CODE) IN (6011, 6010) OR ((MERCHANT_CATEGORY_CODE = 0) AND
                   (TRANSACTION_CODE in ('VSW','MPW','MPM', 'MPR','PLW', 'PLR','PRW', 'SDW'))) THEN 'ATM Withdrawals'
       WHEN (transaction_code in ('ISA', 'ISC', 'ISJ', 'ISL', 'ISM', 'ISR', 'ISZ', 'VSA', 'VSC', 'VSJ', 'VSL', 'VSM', 'VSR', 'VSZ',
                 'SDA', 'SDC', 'SDL', 'SDM', 'SDR', 'SDV', 'SDZ', 'PLM', 'PLA', 'PRA') AND unique_program_id IN (600, 278, 1014) AND transaction_amount < 0) THEN 'Credit Purchase'
       WHEN transaction_code in ('ISA', 'ISC', 'ISJ', 'ISL', 'ISM', 'ISR', 'ISZ', 'VSA', 'VSC', 'VSJ', 'VSL', 'VSM', 'VSR', 'VSZ',
                 'SDA', 'SDC', 'SDL', 'SDM', 'SDR', 'SDV', 'SDZ', 'PLM', 'PLA', 'PRA') AND transaction_amount < 0 THEN 'Debit Purchase'
       WHEN TRANSACTION_CODE = 'ADS' THEN 'ACH Push'
       WHEN TRANSACTION_CODE = 'PMAC' THEN 'ACH Pull'
       ELSE 'Others'
       END) AS TYPE_OF_TRXN,
      (case when transaction_code like 'AD%' then 4
            when transaction_code like 'FE%' then 5
            when transaction_code like 'IS%' then 6
            when transaction_code like 'PM%' then 7
            when transaction_code like 'SD%' then 8
            when transaction_code like 'VS%' then 9 else 0 end) AS leading_num
     FROM ANALYTICS.LOOKER.TRANSACTIONS) AS A
  LEFT JOIN WEEKS_DATES AS C
  ON TO_DATE(A.TRANSACTION_TIMESTAMP)=C.TRANS_DATE
  LEFT JOIN REST.TEST.TRANSACTIONS_TIERS_MAPPING_0411 D
  ON A.id = D.TRXN_ID
  LEFT JOIN (SELECT DISTINCT A.USER_ID, B.TRANSACTION_ID, A.REASON
             FROM ANALYTICS.LOOKER.USER_DISPUTE_CLAIMS AS A
             LEFT JOIN ANALYTICS.LOOKER.USER_DISPUTE_CLAIM_TRANSACTIONS AS B
             ON A.ID=B.USER_DISPUTE_CLAIM_ID) AS E
  ON A.USER_ID=E.USER_ID
  AND TO_NUMBER(CONCAT(A.leading_num, A.AUTHORIZATION_CODE)) = TO_NUMBER(E.TRANSACTION_ID)
  LEFT JOIN DECLINE_RATE_TRXN AS F
  ON C.START_WEEK=F.START_WEEK --weekly
    AND A.TYPE_OF_TRXN = F.TYPE_OF_TRXN
    AND A.USER_ID=F.USER_ID
  LEFT JOIN max_DD M
  ON M.USER_ID = A.USER_ID
  LEFT JOIN socure_scores S1
  ON A.USER_ID = S1.USER_ID
  LEFT JOIN socure_score_contact S2
  ON A.USER_ID = S2.USER_ID
  WHERE (TO_DATE(A.TRANSACTION_TIMESTAMP)>='2020-01-01'))
    --AND (TO_DATE(A.TRANSACTION_TIMESTAMP)< '2021-02-01')
    --AND DATEDIFF(WEEK, A.TRANSACTION_TIMESTAMP, CURRENT_DATE()) >= 1
    --AND A.TYPE_OF_TRXN IN ('ATM Withdrawals', 'Credit Purchase', 'Debit Purchase', 'ACH Push', 'ACH Pull'))
    --AND MCC>0)

SELECT * FROM ALL_TRANS
 --WHERE TYPE_OF_TRXN IN ('ATM Withdrawals')
);
