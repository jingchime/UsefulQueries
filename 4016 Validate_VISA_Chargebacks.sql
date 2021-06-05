// firstly double check Mar - bancorp has 393 chargebacks 
WITH instant_transfers AS (select id, user_id,transaction_code, transaction_timestamp, transaction_amount 
          from ANALYTICS.LOOKER."TRANSACTIONS" T  
          where transaction_code in ('PMDB', 'PMTP', 'ADac', 'ADAS', 'ADTR', 'ADar')  
          and transaction_timestamp >= '2021-01-01')  
            
SELECT DATE_TRUNC('month', TRANSACTION_TIMESTAMP)::DATE AS TXN_day, 
    bank.PRIMARY_PROGRAM_ASSIGNED,
    COUNT(DISTINCT CASE WHEN transaction_code in ('PMDB', 'PMTP') THEN instant_transfers.user_id ELSE NULL END) AS "total_users",
    COUNT(DISTINCT CASE WHEN transaction_code in ('PMDB', 'PMTP') THEN instant_transfers.id ELSE NULL END) AS "total_transactions",
    COUNT(DISTINCT CASE WHEN transaction_code in ('ADac', 'ADAS', 'ADTR') THEN instant_transfers.id ELSE NULL END) AS "chargeback_transactions",
    COALESCE(SUM(CASE WHEN transaction_code in ('PMDB', 'PMTP') THEN instant_transfers.transaction_amount ELSE NULL END  ), 0) AS "transaction_volume", 
  COALESCE(SUM(CASE WHEN transaction_code in ( 'ADac', 'ADAS', 'ADTR') THEN instant_transfers.transaction_amount ELSE NULL END  ), 0) AS "cash_chargeback"
FROM instant_transfers  
LEFT JOIN "ANALYTICS"."LOOKER"."MEMBER_PARTNER_BANK" bank on instant_transfers.user_id = bank.user_id
WHERE (instant_transfers.transaction_timestamp  >= TO_TIMESTAMP('2021-1-1'))  
GROUP BY 1,2  
ORDER BY 1,2; 

// then pull the 393 chargebacks from our data
WITH instant_transfers AS (select id, user_id,transaction_code, transaction_timestamp, transaction_amount 
          from ANALYTICS.LOOKER."TRANSACTIONS" T  
          where transaction_code in ('PMDB', 'PMTP', 'ADac', 'ADAS', 'ADTR', 'ADar')  
          and transaction_timestamp >= '2021-01-01')        
SELECT 
DATE_TRUNC('month', TRANSACTION_TIMESTAMP)::DATE AS TXN_month,  
instant_transfers.id,
instant_transfers.user_id,
instant_transfers.transaction_amount,
transaction_timestamp
FROM instant_transfers  
LEFT JOIN "ANALYTICS"."LOOKER"."MEMBER_PARTNER_BANK" bank on instant_transfers.user_id = bank.user_id
WHERE 
bank.PRIMARY_PROGRAM_ASSIGNED = 'bancorp'
and TXN_month = '2021-03-01'
and transaction_code in ('ADac', 'ADAS', 'ADTR');


-- trying to tie VISA numbers with our numbers but failed
-- because Beth's table only has transactions using Chime card
// beth's code
select VISA_TRANSACTION_ID,galileo_authorized_transaction_id, *
from mysql_db.galileo.galileo_authorized_transaction_enrichments
where galileo_authorized_transaction_id = '4348104389';

select min(_LOADED_AT) from mysql_db.galileo.galileo_authorized_transaction_enrichments;

// leverage beth's code
// 1-1 align visa id and our id
select VISA_TRANSACTION_ID,galileo_authorized_transaction_id
from mysql_db.galileo.galileo_authorized_transaction_enrichments
where galileo_authorized_transaction_id in 
(select distinct id from 
  (WITH instant_transfers AS (select id, user_id,transaction_code, transaction_timestamp, transaction_amount  
            from ANALYTICS.LOOKER."TRANSACTIONS" T  
            where transaction_code in ('PMDB', 'PMTP', 'ADac', 'ADAS', 'ADTR', 'ADar')  
            and transaction_timestamp >= '2021-01-01')        
  SELECT 
  DATE_TRUNC('month', TRANSACTION_TIMESTAMP)::DATE AS TXN_month,  
  instant_transfers.id,
  instant_transfers.user_id,
  instant_transfers.transaction_amount
  FROM instant_transfers  
  LEFT JOIN "ANALYTICS"."LOOKER"."MEMBER_PARTNER_BANK" bank on instant_transfers.user_id = bank.user_id
  WHERE 
  bank.PRIMARY_PROGRAM_ASSIGNED = 'bancorp'
  and TXN_month = '2021-03-01'
  and transaction_code in ('ADac', 'ADAS', 'ADTR')) tmp );

---------above method won't work ---------------------------

---------try manual match for 393---------------------------------------
---------tie back the transaction date for the chargeback date----------
select 
transfers.id as trx_id,
transfers.user_id as user_id,
transfers.transaction_amount as amount,
DATE_TRUNC('day', transfers.transaction_timestamp)::DATE as chargeback_day,
t.id,
DATE_TRUNC('day', t2.CREATED_AT)::DATE as transaction_day

FROM ANALYTICS.LOOKER."TRANSACTIONS" transfers
LEFT JOIN (select distinct ID, ADJUSTMENT_TRANSACTION_ID from "MYSQL_DB"."CHIME_PROD"."USER_ADJUSTMENTS" where type = 'external_card_chargeback') t
on transfers.AUTHORIZATION_CODE = t.ADJUSTMENT_TRANSACTION_ID
left join "MYSQL_DB"."CHIME_PROD"."USER_ADJUSTMENTS" t2
on t2.RELATED_USER_ADJUSTMENT_ID = t.id
LEFT JOIN "ANALYTICS"."LOOKER"."MEMBER_PARTNER_BANK" bank 
on transfers.user_id = bank.user_id
WHERE TRANSACTION_CODE IN ('ADac', 'ADAS', 'ADTR', 'ADar') 
and DATE_TRUNC('month', TRANSACTION_TIMESTAMP)::DATE = '2021-04-01'
and bank.PRIMARY_PROGRAM_ASSIGNED='bancorp';

-- we missed Mar 26-30 chargeback in transaction table, which could leaad to horrible results
-- checking adjustment table to see if transaction missed t table has
select * from  "MYSQL_DB"."CHIME_PROD"."USER_ADJUSTMENTS" where TYPE = 'external_card_transfer'; --98,008
select * from  "MYSQL_DB"."CHIME_PROD"."USER_ADJUSTMENTS" where TYPE = 'external_card_chargeback'; --4,788
select * from ANALYTICS.LOOKER."TRANSACTIONS" T where transaction_code in ('PMDB', 'PMTP'); --439,225
select * from ANALYTICS.LOOKER."TRANSACTIONS" T where transaction_code in ('ADac', 'ADAS', 'ADTR', 'ADar'); --4,653

-- check all chargeabcks in GPT table
select DATE_TRUNC('day', TRANSACTION_TIMESTAMP)::DATE as chargeback_day, id, user_id, transaction_amount
from ANALYTICS.LOOKER."TRANSACTIONS" T  
where transaction_code in ('ADac', 'ADAS', 'ADTR', 'ADar')
and TRANSACTION_TIMESTAMP >= '2021-04-01'
order by 1;

-- check all chargeabcks in adjustment table
select DATE_TRUNC('day', CREATED_AT)::DATE as chargeback_day, id, USER_ID, AMOUNT, *
from  "MYSQL_DB"."CHIME_PROD"."USER_ADJUSTMENTS" 
where TYPE = 'external_card_chargeback'
and CREATED_AT >= '2021-04-01'
order by 1;

-- align VISA results using adjustment table
-- almost the same with VISA bancorp result
-- except VISA missed two chargebacks on Mar-13 and one record has a 5 dollar diff
WITH instant_transfers AS (select CREATED_AT, id, USER_ID, AMOUNT
    from  "MYSQL_DB"."CHIME_PROD"."USER_ADJUSTMENTS" 
    where TYPE = 'external_card_chargeback'
    and CREATED_AT >= '2020-11-01') 
            
SELECT DATE_TRUNC('day', CREATED_AT)::DATE AS ORIGINAL_TXN_day, 
    AMOUNT
FROM instant_transfers  
LEFT JOIN "ANALYTICS"."LOOKER"."MEMBER_PARTNER_BANK" bank 
on instant_transfers.user_id = bank.user_id
WHERE bank.PRIMARY_PROGRAM_ASSIGNED = 'bancorp' 
and CREATED_AT >= TO_TIMESTAMP('2021-03-01')  
and CREATED_AT < TO_TIMESTAMP('2021-04-01') 
ORDER BY 1; 
