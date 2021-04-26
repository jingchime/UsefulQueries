-- temp table for all tier 2 to tier 3 users
CREATE OR REPLACE table rest.test.Tier2_tier3_users as (
SELECT B.*, A.user_id as user,
  ABS(A.TRANSACTION_AMOUNT) AS FINAL_AMT,
  (CASE WHEN E.TRANSACTION_ID IS NOT NULL THEN 1 ELSE 0 END) AS FLG_DISPUTED,
  (CASE WHEN E.TRANSACTION_ID IS NOT NULL THEN A.USER_ID END) AS FLG_DISPUTED_USER,
  D.AMOUNT_CRED + D.AMOUNT_REV + D.AMOUNT_FINAL AS TOTAL_LOSS,
  (CASE WHEN (A.MERCHANT_CATEGORY_CODE) IN (6011, 6010) OR ((A.MERCHANT_CATEGORY_CODE = 0) AND 
        (A.TRANSACTION_CODE in ('VSW','MPW','MPM', 'MPR','PLW', 'PLR','PRW', 'SDW'))) THEN 'ATM Withdrawals'
       WHEN (A.transaction_code in ('ISA', 'ISC', 'ISJ', 'ISL', 'ISM', 'ISR', 'ISZ', 'VSA', 'VSC', 'VSJ', 'VSL', 'VSM', 'VSR', 'VSZ',
                 'SDA', 'SDC', 'SDL', 'SDM', 'SDR', 'SDV', 'SDZ', 'PLM', 'PLA', 'PRA') AND A.unique_program_id IN (600, 278, 1014) AND A.transaction_amount < 0) THEN 'Credit Purchase'
       WHEN (A.transaction_code in ('ISA', 'ISC', 'ISJ', 'ISL', 'ISM', 'ISR', 'ISZ', 'VSA', 'VSC', 'VSJ', 'VSL', 'VSM', 'VSR', 'VSZ',
                 'SDA', 'SDC', 'SDL', 'SDM', 'SDR', 'SDV', 'SDZ', 'PLM', 'PLA', 'PRA') AND A.transaction_amount < 0) THEN 'Debit Purchase'      
       ELSE 'Others'
       END) AS TYPE_OF_TRXN
  FROM REST.TEST.TRANSACTIONS_TIERS_MAPPING_0411 AS b
left join (SELECT *,
    (case when transaction_code like 'AD%' then 4
          when transaction_code like 'FE%' then 5
          when transaction_code like 'IS%' then 6
          when transaction_code like 'PM%' then 7
          when transaction_code like 'SD%' then 8
          when transaction_code like 'VS%' then 9 else 0 end) AS leading_num
           from mysql_db.galileo.galileo_posted_transactions) AS a
on B.TRXN_id = a.id 
  LEFT JOIN (SELECT DISTINCT A.USER_ID, B.TRANSACTION_ID
             FROM ANALYTICS.LOOKER.USER_DISPUTE_CLAIMS AS A
             LEFT JOIN ANALYTICS.LOOKER.USER_DISPUTE_CLAIM_TRANSACTIONS AS B
             ON A.ID=B.USER_DISPUTE_CLAIM_ID) AS E
  ON A.USER_ID=E.USER_ID
  AND TO_NUMBER(CONCAT(A.leading_num, A.AUTHORIZATION_CODE)) = TO_NUMBER(E.TRANSACTION_ID)
  LEFT JOIN risk.prod.all_disputable_transactions AS D 
on  D.user_id = E.user_id
    and (D.authorization_code = E.transaction_id 
    or D.authorization_code = right(E.transaction_id, len(E.transaction_id) - 1)) 
where TIER_AT_TRXN = 'TIER3' OR TIER_AT_TRXN = 'TIER2');

-- check before and after change for each cohort changes
SELECT TO_CHAR(DATE_TRUNC('MONTH', B.TRANSACTION_TIMESTAMP::date), 'YYYY-MM'), B.TYPE_OF_TRXN, SUM(FLG_DISPUTED), 
        COUNT(DISTINCT TRXN_ID), SUM(TOTAL_LOSS) ,
SUM(case when FLG_DISPUTED = 1 then FINAL_amt else 0 end) as disputes_$,
SUM(FINAL_AMT), SUM(FINAL_AMT)/COUNT(DISTINCT User) as avg_trnx_per_user
FROM (SELECT distinct TO_CHAR(DATE_TRUNC('MONTH', FIRST_DATE_TIER3), 'YYYY-MM') as mth, FIRST_DATE_TIER3,
      FIRST_DATE_TIER3 -30 as back_date_30, 
      FIRST_DATE_TIER3 +30 as Fore_date_30, 
      FIRST_DATE_TIER3 -60 as back_date_60, 
      FIRST_DATE_TIER3 +180 as Fore_date_60, 
      d.USER_ID  
FROM REST.TEST.TRANSACTIONS_TIERS_MAPPING_0411 AS d
left join (SELECT *,
    (case when transaction_code like 'AD%' then 4
          when transaction_code like 'FE%' then 5
          when transaction_code like 'IS%' then 6
          when transaction_code like 'PM%' then 7
          when transaction_code like 'SD%' then 8
          when transaction_code like 'VS%' then 9 else 0 end) AS leading_num
           from mysql_db.galileo.galileo_posted_transactions) AS c
on d.TRXN_id = c.id 
where FIRST_DATE_TIER4 is null and FIRST_DATE_TIER3 is not null and FIRST_DATE_TIER3 between '2020-08-01' and '2020-08-31') AS A -- change the month as per the data
LEFT JOIN rest.test.Tier2_tier3_users AS B
on A.user_id = B.user and B.TRANSACTION_TIMESTAMP::date >= A.back_date_60 and B.TRANSACTION_TIMESTAMP::date <= A.Fore_date_60
group by 1,2;

-- updating overall tier data for debit
SELECT TO_CHAR(DATE_TRUNC('MONTH', TRANSACTION_TIMESTAMP::date), 'YYYY-MM'), TYPE_OF_TRXN, SUM(FLG_DISPUTED), 
        COUNT(DISTINCT TRXN_ID), SUM(TOTAL_LOSS) ,
SUM(case when FLG_DISPUTED = 1 then FINAL_amt else 0 end) as disputes_$,
SUM(FINAL_AMT), SUM(FINAL_AMT)/COUNT(DISTINCT User) as AVg_trxn_amt_per_user
FROM rest.test.Tier3_users 
where TYPE_of_trxn = 'Debit Purchase'
group by 1,2 LIMIT 100;

-- updating overall tier data for ATM
SELECT TO_CHAR(DATE_TRUNC('MONTH', TRANSACTION_TIMESTAMP::date), 'YYYY-MM'), TYPE_OF_TRXN, SUM(FLG_DISPUTED), 
        COUNT(DISTINCT TRXN_ID), SUM(TOTAL_LOSS) ,
SUM(case when FLG_DISPUTED = 1 then FINAL_amt else 0 end) as disputes_$,
SUM(FINAL_AMT), SUM(FINAL_AMT)/COUNT(DISTINCT User) as AVg_trxn_amt_per_user
FROM rest.test.Tier3_users 
where TYPE_of_trxn = 'ATM Withdrawals' 
group by 1,2 LIMIT 100;

-- temp table for the above
CREATE OR REPLACE table rest.test.Tier3_users as (
SELECT B.*, A.user_id as user,
  ABS(A.TRANSACTION_AMOUNT) AS FINAL_AMT,
  (CASE WHEN E.TRANSACTION_ID IS NOT NULL THEN 1 ELSE 0 END) AS FLG_DISPUTED,
  (CASE WHEN E.TRANSACTION_ID IS NOT NULL THEN A.USER_ID END) AS FLG_DISPUTED_USER,
  D.AMOUNT_CRED + D.AMOUNT_REV + D.AMOUNT_FINAL AS TOTAL_LOSS,
  (CASE WHEN (A.MERCHANT_CATEGORY_CODE) IN (6011, 6010) OR ((A.MERCHANT_CATEGORY_CODE = 0) AND 
        (A.TRANSACTION_CODE in ('VSW','MPW','MPM', 'MPR','PLW', 'PLR','PRW', 'SDW'))) THEN 'ATM Withdrawals'
       WHEN (A.transaction_code in ('ISA', 'ISC', 'ISJ', 'ISL', 'ISM', 'ISR', 'ISZ', 'VSA', 'VSC', 'VSJ', 'VSL', 'VSM', 'VSR', 'VSZ',
                 'SDA', 'SDC', 'SDL', 'SDM', 'SDR', 'SDV', 'SDZ', 'PLM', 'PLA', 'PRA') AND A.unique_program_id IN (600, 278, 1014) AND A.transaction_amount < 0) THEN 'Credit Purchase'
       WHEN (A.transaction_code in ('ISA', 'ISC', 'ISJ', 'ISL', 'ISM', 'ISR', 'ISZ', 'VSA', 'VSC', 'VSJ', 'VSL', 'VSM', 'VSR', 'VSZ',
                 'SDA', 'SDC', 'SDL', 'SDM', 'SDR', 'SDV', 'SDZ', 'PLM', 'PLA', 'PRA') AND A.transaction_amount < 0) THEN 'Debit Purchase'      
       ELSE 'Others'
       END) AS TYPE_OF_TRXN
  FROM REST.TEST.TRANSACTIONS_TIERS_MAPPING_0411 AS b
left join (SELECT *,
    (case when transaction_code like 'AD%' then 4
          when transaction_code like 'FE%' then 5
          when transaction_code like 'IS%' then 6
          when transaction_code like 'PM%' then 7
          when transaction_code like 'SD%' then 8
          when transaction_code like 'VS%' then 9 else 0 end) AS leading_num
           from mysql_db.galileo.galileo_posted_transactions) AS a
on B.TRXN_id = a.id 
  LEFT JOIN (SELECT DISTINCT A.USER_ID, B.TRANSACTION_ID
             FROM ANALYTICS.LOOKER.USER_DISPUTE_CLAIMS AS A
             LEFT JOIN ANALYTICS.LOOKER.USER_DISPUTE_CLAIM_TRANSACTIONS AS B
             ON A.ID=B.USER_DISPUTE_CLAIM_ID) AS E
  ON A.USER_ID=E.USER_ID
  AND TO_NUMBER(CONCAT(A.leading_num, A.AUTHORIZATION_CODE)) = TO_NUMBER(E.TRANSACTION_ID)
  LEFT JOIN risk.prod.all_disputable_transactions AS D 
on  D.user_id = E.user_id
    and (D.authorization_code = E.transaction_id 
    or D.authorization_code = right(E.transaction_id, len(E.transaction_id) - 1)) 
where TIER_AT_TRXN = 'TIER3');
