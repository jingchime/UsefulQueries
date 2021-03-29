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