--------------tier 2 to tier 3----------------------
-- tagged transactions with dispute or not
-- filter only those who are in tier 2 and tier 3
CREATE OR REPLACE table rest.test.Tier2_tier3_users_0416 as (
SELECT B.*, A.user_id,
  ABS(A.TRANSACTION_AMOUNT) AS FINAL_AMT,
  (CASE WHEN E.TRANSACTION_ID IS NOT NULL THEN 1 ELSE 0 END) AS FLG_DISPUTED,
  (CASE WHEN E.TRANSACTION_ID IS NOT NULL THEN A.USER_ID END) AS FLG_DISPUTED_USER,
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
where TIER_AT_TRXN = 'TIER3' OR TIER_AT_TRXN = 'TIER2');

-- check transactions
-- only those who never move to tier 4
-- but moved to tier 3
-- check their tier 2,3
SELECT 
TO_CHAR(DATE_TRUNC('MONTH', B.TRANSACTION_TIMESTAMP::date), 'YYYY-MM'), 
B.TYPE_OF_TRXN, 
SUM(FLG_DISPUTED), 
COUNT(DISTINCT TRXN_ID), 
SUM(case when FLG_DISPUTED = 1 then FINAL_amt else 0 end) as disputes_$,
SUM(FINAL_AMT) 

FROM (
        SELECT distinct 
            TO_CHAR(DATE_TRUNC('MONTH', FIRST_DATE_TIER3), 'YYYY-MM') as mth, 
            FIRST_DATE_TIER3,
            FIRST_DATE_TIER3 -30 as back_date_30, 
            FIRST_DATE_TIER3 +30 as Fore_date_30, 
            FIRST_DATE_TIER3 -180 as back_date_180, 
            FIRST_DATE_TIER3 +180 as Fore_date_180, 
            d.USER_ID
        FROM REST.TEST.TRANSACTIONS_TIERS_MAPPING_0411 AS d
        where FIRST_DATE_TIER4 is null and FIRST_DATE_TIER3 is not null and FIRST_DATE_TIER3 between '2020-08-01' and '2020-08-30') AS A  -- those who never become tier 4 but become tier 3 in Apr
LEFT JOIN rest.test.Tier2_tier3_users_0416 AS B
on A.user_id = B.user_id and B.TRANSACTION_TIMESTAMP::date >= A.back_date_180 and B.TRANSACTION_TIMESTAMP::date <= A.Fore_date_180
group by 1,2
order by 1,2;

-- from the table above interestingly no transactions back in 2019
-- makes me wonder if people becomes tier 3 in Apr 2020, if they ever made transactions in 2019
-- the query below shows that they do
select min(TRANSACTION_TIMESTAMP)
from REST.TEST.TRANSACTIONS_TIERS_MAPPING_0411
where FIRST_DATE_TIER3 between '2020-04-01' and '2020-04-30';

-- oh I get it, becase B table asked that users need to come from tier 2/3
-- we don't record tier information before Feb-2020, everyone default on tier 1
-- so no tier 2/3






-------------------tier 3 to tier 4
-- tagged transactions with dispute or not
CREATE OR REPLACE table rest.test.Tier3_tier4_users_0416 as (
SELECT B.TRANSACTION_TIMESTAMP, A.user_id,
  ABS(A.TRANSACTION_AMOUNT) AS FINAL_AMT,
  (CASE WHEN E.TRANSACTION_ID IS NOT NULL THEN 1 ELSE 0 END) AS FLG_DISPUTED,
  (CASE WHEN E.TRANSACTION_ID IS NOT NULL THEN A.USER_ID END) AS FLG_DISPUTED_USER,
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
where TIER_AT_TRXN = 'TIER3' OR TIER_AT_TRXN = 'TIER4');


-- check transactions
SELECT 
TO_CHAR(DATE_TRUNC('MONTH', B.TRANSACTION_TIMESTAMP::date), 'YYYY-MM'), 
B.TYPE_OF_TRXN, 
SUM(FLG_DISPUTED), 
SUM(case when FLG_DISPUTED = 1 then FINAL_amt else 0 end) as disputes_$,
SUM(FINAL_AMT) 
FROM (
        SELECT distinct 
            TO_CHAR(DATE_TRUNC('MONTH', FIRST_DATE_TIER3), 'YYYY-MM') as mth, 
            FIRST_DATE_TIER4,
            FIRST_DATE_TIER4 -30 as back_date_30, 
            FIRST_DATE_TIER4 +30 as Fore_date_30, 
            FIRST_DATE_TIER4 -180 as back_date_180, 
            FIRST_DATE_TIER4 +180 as Fore_date_180, 
            d.USER_ID
        FROM REST.TEST.TRANSACTIONS_TIERS_MAPPING_0411 AS d
        where FIRST_DATE_TIER4 is not null and FIRST_DATE_TIER4 between '2021-02-01' and '2021-02-28') AS A  -- those who never become tier 4 but become tier 3 in Apr
LEFT JOIN rest.test.Tier3_tier4_users_0416 AS B
on A.user_id = B.user_id and B.TRANSACTION_TIMESTAMP::date >= A.back_date_180 and B.TRANSACTION_TIMESTAMP::date <= A.Fore_date_180
group by 1,2
order by 1,2;
