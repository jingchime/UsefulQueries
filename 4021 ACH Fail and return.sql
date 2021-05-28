-- FAILED ACH Transfers
select 
aa.user_id,
to_date(ach_transfer.CREATED_AT) as initialization_day,
GALILEO_HOLD_DAYS,
to_date(dateadd(day,GALILEO_HOLD_DAYS,ach_transfer.CREATED_AT)) as hold_till_day,
to_date(ach_transfer.UPDATED_AT) as return_day,
datediff(day, ach_transfer.CREATED_AT, ach_transfer.UPDATED_AT) as days_take_to_return,
case when return_day <= hold_till_day then 1 else 0 end as if_mitigated,
AMOUNT,
aa.*,
ach_transfer.*
from "MYSQL_DB"."CHIME_PROD"."ACH_TRANSFERS" ach_transfer
join mysql_db.chime_prod.ach_accounts aa on ach_transfer.ach_account_id = aa.id
where ach_transfer."TYPE" = 'debit' AND ach_transfer."STATUS" = 'failed' and ach_transfer.CREATED_AT between '2020-10-01' and '2020-12-31';

--- ACH returns
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
        
 SELECT 
  T2.USER_ID AS T2_USER_ID,
  to_date(T.created_at) as initialization_day,
  --T2.ID AS T2_ID,
  T.GALILEO_HOLD_DAYS,
  to_date(dateadd(day,T.GALILEO_HOLD_DAYS,T.created_at)) as hold_until_day,
  to_date(T2.TRANSACTION_TIMESTAMP) AS return_day,
  datediff(day, T.CREATED_AT, T2.TRANSACTION_TIMESTAMP) as days_take_to_return,
  T2.TRANSACTION_AMOUNT AS AMOUNT,
  T2.TRANSACTION_CODE AS return_TRXN_code,
  T.TRXN_ID,
  T.TRANSACTION_TIMESTAMP AS orig_timestamp,
  T.SUBMITTED_AT AS orig_submitted_timestamp,
  T.EXTERNAL_DEBIT_CONFIRMED_AT,
  T.TRANSACTION_CODE, 
  T.GALILEO_HOLD_DAYS,
  DATEDIFF('day', orig_timestamp, T2.TRANSACTION_TIMESTAMP) AS days_trxn_to_return,
  DATEDIFF('day', orig_submitted_timestamp, T2.TRANSACTION_TIMESTAMP) AS days_submitted_to_return
  FROM ach_returns R
  LEFT JOIN (
    (SELECT g.ID AS TRXN_ID, g.USER_ID, g.TRANSACTION_CODE, g.TRANSACTION_AMOUNT, g.TRANSACTION_TIMESTAMP, g.AUTHORIZATION_CODE,
      act.id, 
      trf.SUBMITTED_AT, trf.GALILEO_HOLD_DAYS, trf.created_at, trf.EXTERNAL_DEBIT_CONFIRMED_AT 
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
        (T2.TRANSACTION_TIMESTAMP > '2020-10-01' AND T2.TRANSACTION_TIMESTAMP < '2020-12-31')