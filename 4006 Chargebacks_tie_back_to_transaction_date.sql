
-- update on May 3rd:
-- Removing the GPT table. Only use user_adjustment table
WITH score as (select 
to_date(api.created_at) as created_on,
api.user_id as member_id
, row_number() over(partition by user_id order by created_at) as rn          

, case when parse_json(result):fraud:scores[0]:name = 'sigma' and  parse_json(result):fraud:scores[0]:version = '1.0' then parse_json(result):fraud:scores[0]:score
       when parse_json(result):fraud:scores[1]:name = 'sigma' and  parse_json(result):fraud:scores[1]:version = '1.0' then parse_json(result):fraud:scores[1]:score
       when parse_json(result):fraud:scores[2]:name = 'sigma' and  parse_json(result):fraud:scores[2]:version = '1.0' then parse_json(result):fraud:scores[2]:score
       else null
       end as sigma_v1  

, case when parse_json(result):fraud:scores[0]:name = 'sigma' and  parse_json(result):fraud:scores[0]:version = '2.0' then parse_json(result):fraud:scores[0]:score
       when parse_json(result):fraud:scores[1]:name = 'sigma' and  parse_json(result):fraud:scores[1]:version = '2.0' then parse_json(result):fraud:scores[1]:score
       when parse_json(result):fraud:scores[2]:name = 'sigma' and  parse_json(result):fraud:scores[2]:version = '2.0' then parse_json(result):fraud:scores[2]:score
       else null
       end as sigma_v2  

, case when parse_json(result):synthetic:scores[0]:name = 'synthetic' then parse_json(result):synthetic:scores[0]:score
       when parse_json(result):synthetic:scores[1]:name = 'synthetic' then parse_json(result):synthetic:scores[1]:score
       when parse_json(result):synthetic:scores[2]:name = 'synthetic' then parse_json(result):synthetic:scores[2]:score
       else null
       end as synthetic_score
from mysql_db.chime_prod.external_api_requests api 
where 1=1
and api.service='socure3' 
and  CHECK_JSON(api.result) is null 
and api.created_at >= '2020-07-01' 
qualify rn=1) 

select 
DATE_TRUNC('day', t.CREATED_AT)::DATE as chargeback_day,
t.id,
t.amount,
t.user_id,
DATE_TRUNC('day', t2.CREATED_AT)::DATE as transaction_day,
member.enrollment_date,
score.sigma_v1,
score.sigma_v2,
users.socure_enrollment_score,
bank.bank_name
FROM  
(select distinct ID, user_id, CREATED_AT, ADJUSTMENT_TRANSACTION_ID, amount from "MYSQL_DB"."CHIME_PROD"."USER_ADJUSTMENTS" where type = 'external_card_chargeback') t
left join "MYSQL_DB"."CHIME_PROD"."USER_ADJUSTMENTS" t2
on t2.RELATED_USER_ADJUSTMENT_ID = t.id
left join analytics.looker.member_acquisition_facts member on t.user_id = member.user_id
left join score on score.member_id = t.user_id
left join mysql_db.chime_prod.users users on users.id = t.user_id
LEFT JOIN MYSQL_DB.CHIME_PROD.ACH_ACCOUNTS bank on t.user_id = bank.user_id
WHERE DATE_TRUNC('month', t.CREATED_AT)::DATE = '2021-04-01';  -- chargeback month



--- previous research process
---STEP 1 Feb chargebacks 1709
select *
FROM ANALYTICS.LOOKER."TRANSACTIONS" transfers
WHERE TRANSACTION_CODE IN ('ADac', 'ADAS', 'ADTR', 'ADar') 
and DATE_TRUNC('month', TRANSACTION_TIMESTAMP)::DATE = '2021-02-01';

-- Step 2 add in user_adjustment table 4966..so many duplicates!
select *
FROM ANALYTICS.LOOKER."TRANSACTIONS" transfers
LEFT JOIN "MYSQL_DB"."CHIME_PROD"."USER_ADJUSTMENTS" t
on transfers.AUTHORIZATION_CODE = t.ADJUSTMENT_TRANSACTION_ID
WHERE TRANSACTION_CODE IN ('ADac', 'ADAS', 'ADTR', 'ADar') 
and DATE_TRUNC('month', TRANSACTION_TIMESTAMP)::DATE = '2021-02-01';

-- Step 3 try only keeping key fields in t table to remove duplicates. still 4966.
-- research and add external_card_chargeback type
-- 1709!! yeah~
select t.*
FROM ANALYTICS.LOOKER."TRANSACTIONS" transfers
LEFT JOIN 
(select distinct ID, ADJUSTMENT_TRANSACTION_ID from "MYSQL_DB"."CHIME_PROD"."USER_ADJUSTMENTS" where type = 'external_card_chargeback') t
on transfers.AUTHORIZATION_CODE = t.ADJUSTMENT_TRANSACTION_ID
WHERE TRANSACTION_CODE IN ('ADac', 'ADAS', 'ADTR', 'ADar') 
and DATE_TRUNC('month', TRANSACTION_TIMESTAMP)::DATE = '2021-02-01';

-- step 4 link back to original transaction
select 
transfers.id as trx_id,
t.id,
DATE_TRUNC('month', t2.CREATED_AT)::DATE as transaction_month
FROM ANALYTICS.LOOKER."TRANSACTIONS" transfers
LEFT JOIN 
(select distinct ID, ADJUSTMENT_TRANSACTION_ID from "MYSQL_DB"."CHIME_PROD"."USER_ADJUSTMENTS" where type = 'external_card_chargeback') t
on transfers.AUTHORIZATION_CODE = t.ADJUSTMENT_TRANSACTION_ID
left join "MYSQL_DB"."CHIME_PROD"."USER_ADJUSTMENTS" t2
on t2.RELATED_USER_ADJUSTMENT_ID = t.id
WHERE TRANSACTION_CODE IN ('ADac', 'ADAS', 'ADTR', 'ADar') 
and DATE_TRUNC('month', TRANSACTION_TIMESTAMP)::DATE = '2021-02-01';

-- step 5 add stride and bancorp
select 
transfers.id as trx_id,
t.id,
bank.PRIMARY_PROGRAM_ASSIGNED,
DATE_TRUNC('month', t2.CREATED_AT)::DATE as transaction_month
FROM ANALYTICS.LOOKER."TRANSACTIONS" transfers
LEFT JOIN (select distinct ID, ADJUSTMENT_TRANSACTION_ID from "MYSQL_DB"."CHIME_PROD"."USER_ADJUSTMENTS" where type = 'external_card_chargeback') t
on transfers.AUTHORIZATION_CODE = t.ADJUSTMENT_TRANSACTION_ID
left join "MYSQL_DB"."CHIME_PROD"."USER_ADJUSTMENTS" t2
on t2.RELATED_USER_ADJUSTMENT_ID = t.id
LEFT JOIN "ANALYTICS"."LOOKER"."MEMBER_PARTNER_BANK" bank 
on transfers.user_id = bank.user_id
WHERE TRANSACTION_CODE IN ('ADac', 'ADAS', 'ADTR', 'ADar') 
and DATE_TRUNC('month', TRANSACTION_TIMESTAMP)::DATE = '2021-02-01';



