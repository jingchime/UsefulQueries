-- main research tables for graphs:
WITH GROUPED_INFO AS (
  SELECT
  TO_CHAR(DATE_TRUNC('MONTH', TRANS_DATE), 'YYYY-MM') AS MTH, 
  TYPE_OF_TRXN,
  -- different criteria
  case when DD_AMT>0 then 1 else 0 end as if_dd_greater_than_0,
  --case when DD_AMT>200 then 1 else 0 end as if_dd_greater_than_200,
  --case when A.AVAILABLE_BALANCE > 100 then 1 else 0 end as if_greater_than_100,
  --case when P.num_of_phone_change_in_past_32_days > 0 then 0 else 1 end as if_no_phone_number_change_past_32_day,
  --case when SOCURE_SIGMA_SCORE <= 0.9 then 1 else 0 end as if_sigma_less_than,
  --case when SOCURE_EMAIL_RISK_SCORE <= 0.9 then 1 else 0 end as if_email_less_than,
  --case when SOCURE_PHONE_RISK_SCORE <= 0.9 then 1 else 0 end as if_phone_less_than,
  
  COUNT(DISTINCT R.USER_ID) AS TOT_NUM_USER,
  SUM(CASE WHEN FLG_DISPUTED=1 THEN FINAL_AMT END)/SUM(FINAL_AMT) AS PERC_DISP_AMT,
  COUNT(DISTINCT DEC_USER)/COUNT(DISTINCT R.USER_ID) AS DEC_RATE_USER,
  sum(INTERCHANGE_FEE_AMOUNT) as total_interchange,
  sum(FINAL_AMT) as total_transaction,
  sum(CASE WHEN FLG_DISPUTED=1 THEN FINAL_AMT ENd) as total_disputed_dollar,
  COUNT(DISTINCT case when debit_still_declines.user_id is not null or atm_still_declines.user_id is not null then R.DEC_USER end)/COUNT(DISTINCT R.USER_ID) AS simulated_dec_RATE_USER,
  COUNT(DISTINCT case when debit_still_declines.user_id is not null or atm_still_declines.user_id is not null then R.DEC_USER end) as simulated_still_decline_comnbine_users,
  COUNT(DISTINCT debit_still_declines.user_id) as simulated_still_decline_debit_users,
  COUNT(DISTINCT atm_still_declines.user_id) as simulated_still_decline_atm_users
  
  FROM REST.TEST.Risk_Segmentation_04_12_2021 R
  LEFT JOIN REST.TEST.Phone_Change_Tracker_04_13_2021 P
  ON R.USER_ID = P.USER_ID AND TO_DATE(R.TRANS_DATE) = TO_DATE(P.DTE)
  LEFT JOIN (SELECT USER_ID, BALANCE_ON_DATE, MAX(AVAILABLE_BALANCE) AS AVAILABLE_BALANCE
            FROM mysql_db.galileo.GALILEO_DAILY_BALANCES
            WHERE ACCOUNT_TYPE = '6' 
            GROUP BY 1,2) A
  ON R.USER_ID = A.USER_ID AND TO_DATE(R.TRANS_DATE) = DATEADD('DAY', 1, A.BALANCE_ON_DATE) -- add one day
  LEFT JOIN rest.test.debit_still_decline_with_tier2_0414 debit_still_declines
  ON TO_CHAR(DATE_TRUNC('MONTH', R.TRANS_DATE), 'YYYY-MM') = debit_still_declines.mth and R.DEC_USER = debit_still_declines.user_id and r.TYPE_OF_TRXN = 'Debit Purchase'
  LEFT JOIN rest.test.atm_still_decline_with_tier2_0414 atm_still_declines
  ON TO_CHAR(DATE_TRUNC('MONTH', R.TRANS_DATE), 'YYYY-MM') = atm_still_declines.mth and R.DEC_USER = atm_still_declines.user_id and r.TYPE_OF_TRXN = 'ATM Withdrawals'
  WHERE TYPE_OF_TRXN IN ('ATM Withdrawals','Debit Purchase')
  and trans_date >= '2020-09-01' and  trans_date  <= '2021-02-28'
  and TIER_AT_TRXN in ('TIER1')
  GROUP BY 1, 2, 3
 )
SELECT * FROM GROUPED_INFO 
ORDER BY 1, 2, 3;

-- the above query leveraged several temp tables with the queries summarized below:

-- temp table 1: update Shu's logic in 5001 to incorporate the simulated tier, latest tier and tier at transaction from 2020-01-01
-- simulated tier table in code 2012
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

-- temp table 2: simulate the declines should tier 1 upgrade to tier 2
-- part 1: debit
create or replace table rest.test.debit_still_decline_with_tier2_0414 as
(
  with distinct_daily_declines as 
  ( select distinct TO_DATE(TRANSACTION_TIMESTAMP) AS TXN_DATE,
          USER_ID,
          authorization_amount, 
          MERCHANT_CATEGORY_CODE,
          merchant_number,
          CASE WHEN A.UNIQUE_PROGRAM_ID IN (600, 278, 1014) THEN 'Credit Purchase'
           WHEN A.MERCHANT_CATEGORY_CODE IN (6010, 6011) THEN 'ATM Withdrawals'
           ELSE 'Debit Purchase'
          END AS TYPE_OF_TRXN
   FROM mysql_db.galileo.galileo_authorized_transactions A
   WHERE AUTHORIZATION_RESPONSE = 61 AND TYPE_OF_TRXN = 'Debit Purchase'
   AND TRANSACTION_TIMESTAMP >= '2020-07-01'
  )
  , total_daily_declines_more_than_2500 as
  ( select txn_date
   , user_id
   , sum(authorization_amount) as total_declined_amount
   from distinct_daily_declines
  group by 1, 2
  having total_declined_amount < -2500
  )
  select  
  distinct TO_CHAR(DATE_TRUNC('month', TXN_DATE), 'YYYY-MM') AS mth,
  user_id
  from total_daily_declines_more_than_2500
);

-- part 2: ATM
create or replace table rest.test.atm_still_decline_with_tier2_0414 as
(
  with distinct_daily_declines as 
  ( select distinct TO_DATE(TRANSACTION_TIMESTAMP) AS TXN_DATE,
          USER_ID,
          authorization_amount, 
          MERCHANT_CATEGORY_CODE,
          merchant_number,
          CASE WHEN A.UNIQUE_PROGRAM_ID IN (600, 278, 1014) THEN 'Credit Purchase'
           WHEN A.MERCHANT_CATEGORY_CODE IN (6010, 6011) THEN 'ATM Withdrawals'
           ELSE 'Debit Purchase'
          END AS TYPE_OF_TRXN
   FROM mysql_db.galileo.galileo_authorized_transactions A
   WHERE AUTHORIZATION_RESPONSE = 61 AND TYPE_OF_TRXN = 'ATM Withdrawals'
   AND TRANSACTION_TIMESTAMP >= '2020-07-01'
  )
  , total_daily_declines_more_than_2500 as
  ( select txn_date
   , user_id
   , sum(authorization_amount) as total_declined_amount
   from distinct_daily_declines
  group by 1, 2
  having total_declined_amount < -500
  )
  select  
  distinct TO_CHAR(DATE_TRUNC('month', TXN_DATE), 'YYYY-MM') AS mth,
  user_id
  from total_daily_declines_more_than_2500
);

-- phone number temp table can be found in 4001
-- if updates needed just put refresh the query there
