--- 1: updated my previous query for proceed transfer time distribution   
select 
case when c.id is not null then 1 else 0 end as if_dd,
CASE WHEN (to_date(a.transaction_timestamp) - to_date(trf.submitted_at)) >= 14 THEN '14+'
ELSE TO_CHAR((to_date(a.transaction_timestamp) - to_date(trf.submitted_at))) END hold_days,
count(*) as frequency,
sum(TRANSACTION_AMOUNT) as total_amount

FROM "MYSQL_DB"."GALILEO"."GALILEO_POSTED_TRANSACTIONS" a
JOIN MYSQL_DB.CHIME_PROD.ach_accounts act ON a.user_id = act.user_id 
JOIN "MYSQL_DB"."CHIME_PROD"."ACH_TRANSFERS" trf on a.authorization_code = trf.payment_id and trf.ach_account_id = act.id
left join (select distinct id from CHIME.FINANCE.MEMBERS where FIRST_DD_DATE IS NOT NULL) c
on a.user_id = c.id

where transaction_code = 'PMAC' AND trf."TYPE" = 'debit' AND trf.STATUS = 'processed'
and a.transaction_timestamp> '2020-08-01' AND a.TRANSACTION_TIMESTAMP < '2020-12-01'
group by 1,2 
order by 1,2 asc;


-- 2. Shu's long query for return post date
WITH ach_returns AS (
  SELECT DISTINCT 
        id,
        user_id, 
        TRAN_ID AS return_auth_code,
        try_to_number(num_ar.value) as orig_auth_code,
        description,
        type,
        amount,
        balance,
        timestamp AS return_timestamp,
        otype
    from 
        MYSQL_DB.CHIME_PROD.alert_transaction_events a,
        lateral split_to_table(regexp_replace(description, '[^0-9]', ''), '') as num_ar
    where 
        id NOT in (177893361) --this alert has a huge typo in it that skews the numbers
        and otype in ('a')
        AND act_type = 'AD'
        AND description ILIKE '%ACH return%')
        
        
, return_agg AS (
 SELECT  R.*,
    T2.USER_ID AS T2_USER_ID,
    T2.ID AS T2_ID,
    T2.TRANSACTION_AMOUNT AS T2_AMT,
      T2.TRANSACTION_TIMESTAMP AS return_TRXN_timestamp,
        T2.TRANSACTION_CODE AS return_TRXN_code,
      T.TRXN_ID,
        T.TRANSACTION_TIMESTAMP AS orig_timestamp,
        T.SUBMITTED_AT AS orig_submitted_timestamp,
        T.EXTERNAL_DEBIT_CONFIRMED_AT,
        T.TRANSACTION_CODE, 
        T.GALILEO_HOLD_DAYS,
        DATEDIFF('day', orig_timestamp, return_TRXN_timestamp) AS days_trxn_to_return,
        DATEDIFF('day', orig_submitted_timestamp, return_TRXN_timestamp) AS days_submitted_to_return
  FROM ach_returns R
  LEFT JOIN (
    (SELECT g.ID AS TRXN_ID, g.USER_ID, g.TRANSACTION_CODE, g.TRANSACTION_AMOUNT, g.TRANSACTION_TIMESTAMP, g.AUTHORIZATION_CODE,
      act.id, 
      trf.SUBMITTED_AT, trf.GALILEO_HOLD_DAYS, trf.EXTERNAL_DEBIT_CONFIRMED_AT 
    FROM ANALYTICS.LOOKER."TRANSACTIONS" g
    JOIN MYSQL_DB.CHIME_PROD.ach_accounts act ON g.user_id = act.user_id 
    JOIN MYSQL_DB.CHIME_PROD.ach_transfers trf 
    ON g.authorization_code = trf.payment_id AND trf.ach_account_id = act.id
    WHERE g.TRANSACTION_CODE = 'PMAC' AND trf."TYPE" = 'debit')) T
      ON T.USER_ID = R.USER_ID AND T.AUTHORIZATION_CODE = R.orig_auth_code
    RIGHT JOIN (SELECT * 
          FROM ANALYTICS.LOOKER."TRANSACTIONS" 
          WHERE TRANSACTION_CODE IN ('ADa', 'ADA')) T2
      ON T2.USER_ID = R.USER_ID AND T2.AUTHORIZATION_CODE = R.return_auth_code
    --WHERE T.TRANSACTION_TIMESTAMP > '2020-08-01' AND T.TRANSACTION_TIMESTAMP < '2020-12-01')
    WHERE --(T.TRANSACTION_TIMESTAMP > '2020-08-01' AND T.TRANSACTION_TIMESTAMP < '2020-12-01') 
          --OR (T.TRANSACTION_TIMESTAMP IS NULL)
        (T2.TRANSACTION_TIMESTAMP > '2020-08-01' AND T2.TRANSACTION_TIMESTAMP < '2020-12-01')
)
    
--SELECT * FROM return_agg 

-- Days_to_return distribution analysis        
SELECT
  --days_submitted_to_return,
  
  (CASE WHEN days_submitted_to_return >= 14 THEN '14+'
          ELSE TO_CHAR(days_submitted_to_return) END) days_submitted_to_returns,
  /*
    (CASE WHEN DAYS_TO_RETURN >= 14 THEN '14+'
              ELSE TO_CHAR(DAYS_TO_RETURN) END) DAYS_TO_RETURNS,
  */
  --TO_CHAR(DATE_TRUNC('MONTH', return_TRXN_timestamp), 'YYYY-MM') AS MTH,
  --COUNT(DISTINCT T2_USER_ID) AS NUM_USERS,
    COUNT(T2_ID) AS TOT_NUM,
  SUM(ABS(T2_AMT)) AS TOT_AMT
  
  /*
  COUNT(DISTINCT USER_ID) AS NUM_USERS,
    COUNT(ID) AS TOT_NUM,
  SUM(ABS(AMOUNT)) AS TOT_AMT
  */
  FROM return_agg
  WHERE T2_USER_ID not IN (select distinct id from CHIME.FINANCE.MEMBERS where FIRST_DD_DATE IS NOT NULL) -- DDer vs. Non-DDers
    GROUP BY 1
    ORDER BY TRY_TO_NUMBER(regexp_replace(days_submitted_to_returns, '[^0-9]', ''));



----3. logic to get proceeded ACH transfers
select 
case when c.id is not null then 1 else 0 end as if_dd,
(to_date(a.transaction_timestamp) - to_date(b.submitted_at)) as hold_days, 
--avg(galileo_hold_days) as galileo_avg_hold_days, 
count(*) as frequency,
sum(TRANSACTION_AMOUNT) as total_amount

from "MYSQL_DB"."GALILEO"."GALILEO_POSTED_TRANSACTIONS" a
join "MYSQL_DB"."CHIME_PROD"."ACH_TRANSFERS" b on authorization_code = payment_id
left join (select distinct id from CHIME.FINANCE.MEMBERS where FIRST_DD_DATE IS NOT NULL) c
on a.user_id = c.id

where transaction_code = 'PMAC' and b.STATUS = 'processed'
and hold_days between 0 and 20
group by 1,2 order by 1,2 asc;

----4. logic to get failed ACH transfers
select 
case when c.id is not null then 1 else 0 end as if_dd,
(to_date(a.transaction_timestamp) - to_date(b.submitted_at)) as hold_days, 
--avg(galileo_hold_days) as galileo_avg_hold_days, 
count(*) as frequency,
sum(TRANSACTION_AMOUNT) as total_amount
from "MYSQL_DB"."GALILEO"."GALILEO_POSTED_TRANSACTIONS" a
join "MYSQL_DB"."CHIME_PROD"."ACH_TRANSFERS" b on authorization_code = payment_id
left join (select distinct id from CHIME.FINANCE.MEMBERS where FIRST_DD_DATE IS NOT NULL) c
on a.user_id = c.id
where transaction_code = 'PMAC' and b.STATUS = 'failed'
and hold_days between 0 and 20
group by 1,2 order by 1,2 asc;

--- 5. shu's return logic
SELECT DISTINCT 
id,
user_id, 
TRAN_ID AS return_auth_code,
try_to_number(num_ar.value) as orig_auth_code,
description,
type,
amount,
balance,
timestamp AS return_timestamp,
otype
from 
MYSQL_DB.CHIME_PROD.alert_transaction_events a,
lateral split_to_table(regexp_replace(description, '[^0-9]', ''), '') as num_ar
where 
id NOT in (177893361) --this alert has a huge typo in it that skews the numbers
and otype in ('a')
AND act_type = 'AD'
AND description ILIKE '%ACH return%';


-- 6. combine return logic with fail logic and compare
select 
return_method.return_timestamp as return_method_timestamp,
return_method.user_id as return_method_user_id,
return_method.orig_auth_code as return_method_orig_auth_code,
return_method.return_auth_code as return_method_return_auth_code,
failed_method.user_id as fail_method_user_id,
failed_method.authorization_code as fail_method_auth_code,
*

from 

(select *
from "MYSQL_DB"."GALILEO"."GALILEO_POSTED_TRANSACTIONS" a
join "MYSQL_DB"."CHIME_PROD"."ACH_TRANSFERS" b on authorization_code = payment_id
where transaction_code = 'PMAC' and b.STATUS = 'failed') failed_method

full outer join

(SELECT DISTINCT 
id,
user_id, 
TRAN_ID AS return_auth_code,
try_to_number(num_ar.value) as orig_auth_code,
description,
type,
amount,
balance,
timestamp AS return_timestamp,
otype
from 
MYSQL_DB.CHIME_PROD.alert_transaction_events a,
lateral split_to_table(regexp_replace(description, '[^0-9]', ''), '') as num_ar
where 
id NOT in (177893361) --this alert has a huge typo in it that skews the numbers
and otype in ('a')
AND act_type = 'AD'
AND description ILIKE '%ACH return%') return_method
on failed_method.user_id = return_method.user_id and failed_method.authorization_code = return_method.orig_auth_code;




