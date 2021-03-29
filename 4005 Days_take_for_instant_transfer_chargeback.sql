-- old version, which didn't take care of the seasoning of transactions
-- also it's user level, not tie back to the transactions
select
diff_days_to_chargeback,
count(user_id) as num_users
from
(
  SELECT 
  user_id,
  min(case when transaction_code in ('PMDB', 'PMTP') then DATE_TRUNC('day', TRANSACTION_TIMESTAMP)::DATE end) as instant_transfer_date,
  min(case when transaction_code in ('ADac', 'ADAS', 'ADTR', 'ADar') then DATE_TRUNC('day', TRANSACTION_TIMESTAMP)::DATE end) as charge_back_date,
  datediff(day, instant_transfer_date, charge_back_date) as diff_days_to_chargeback
  FROM ANALYTICS.LOOKER."TRANSACTIONS" 
  WHERE user_id IN (
    SELECT DISTINCT user_id FROM ANALYTICS.LOOKER."TRANSACTIONS" 
    WHERE TRANSACTION_CODE IN ('ADac', 'ADAS', 'ADTR', 'ADar')) 
  GROUP BY 1
) tmp
group by 1
order by 1;

-- below is how I develop the tie back logic step by step leveraging the magic table "MYSQL_DB"."CHIME_PROD"."USER_ADJUSTMENTS"
-- step 1 all instant transfer in past 30 days - 5386 rows
select *
from ANALYTICS.LOOKER."TRANSACTIONS" T  
where transaction_code in ('PMDB', 'PMTP')
AND TRANSACTION_TIMESTAMP >= dateadd(day, -30, current_date())
;

-- step 2 add magic table -10770 rows - add type reduce to 5386~!
select *
from ANALYTICS.LOOKER."TRANSACTIONS" transfers
LEFT JOIN (select * from "MYSQL_DB"."CHIME_PROD"."USER_ADJUSTMENTS" where TYPE = 'external_card_transfer') t
on transfers.AUTHORIZATION_CODE = t.PAYMENT_TRANSACTION_ID
where transaction_code in ('PMDB', 'PMTP')
AND TRANSACTION_TIMESTAMP >= dateadd(day, -30, current_date())
;

-- step 3 join back to check if the transaction got chargedback 5386!
select 
DATE_TRUNC('day', transfers.TRANSACTION_TIMESTAMP)::DATE as trx_date,
case when t2.TYPE = 'external_card_chargeback' then 1 else 0 end as if_chargeback,
case when t2.TYPE = 'external_card_chargeback' then DATE_TRUNC('day', t2.CREATED_AT)::DATE end as chargeback_date
from ANALYTICS.LOOKER."TRANSACTIONS" transfers
LEFT JOIN (select * from "MYSQL_DB"."CHIME_PROD"."USER_ADJUSTMENTS" where TYPE = 'external_card_transfer') t
on transfers.AUTHORIZATION_CODE = t.PAYMENT_TRANSACTION_ID
LEFT JOIN "MYSQL_DB"."CHIME_PROD"."USER_ADJUSTMENTS" t2
ON t.RELATED_USER_ADJUSTMENT_ID = t2.id
where transaction_code in ('PMDB', 'PMTP')
AND transfers.TRANSACTION_TIMESTAMP < dateadd(day, -90, current_date()) and transfers.TRANSACTION_TIMESTAMP >= dateadd(day, -180, current_date()) 