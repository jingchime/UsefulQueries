// for chargeback users
select 
DAYS_SINCE_INSTANT_TRANSFER,
count(*) as num_transactions
from
(
    WITH instant_transfers AS (select id, user_id,transaction_code, transaction_timestamp, transaction_amount	
              from ANALYTICS.LOOKER."TRANSACTIONS" T	
              where transaction_code in ('PMDB', 'PMTP', 'ADac', 'ADAS', 'ADTR', 'ADar') 	
              and transaction_timestamp >= '2020-10-01')	         	
    SELECT 
    member.enrollment_date,
    DATE_TRUNC(day, TRANSACTION_TIMESTAMP)::DATE as instant_transfer_date,
    datediff(day, enrollment_date, instant_transfer_date) as days_since_instant_transfer
    FROM instant_transfers	
    left join analytics.looker.member_acquisition_facts member on instant_transfers.user_id = member.user_id
    WHERE instant_transfers.transaction_timestamp  >= TO_TIMESTAMP('2020-10-1')	
           and instant_transfers.user_id in (select distinct user_id	
              from ANALYTICS.LOOKER."TRANSACTIONS" transactions	
              where transaction_code in ('ADac', 'ADAS', 'ADTR', 'ADar'))
    AND transaction_code in ('PMDB', 'PMTP')
) tmp
group by 1 
order by 1
;	

// for all instant transfers
select 
DAYS_SINCE_INSTANT_TRANSFER,
count(*) as num_transactions
from
(
    WITH instant_transfers AS (select id, user_id,transaction_code, transaction_timestamp, transaction_amount	
              from ANALYTICS.LOOKER."TRANSACTIONS" T	
              where transaction_code in ('PMDB', 'PMTP', 'ADac', 'ADAS', 'ADTR', 'ADar') 	
              and transaction_timestamp >= '2020-10-01')	         	
    SELECT 
    member.enrollment_date,
    DATE_TRUNC(day, TRANSACTION_TIMESTAMP)::DATE as instant_transfer_date,
    datediff(day, enrollment_date, instant_transfer_date) as days_since_instant_transfer
    FROM instant_transfers	
    left join analytics.looker.member_acquisition_facts member on instant_transfers.user_id = member.user_id
    WHERE instant_transfers.transaction_timestamp  >= TO_TIMESTAMP('2020-10-1')	
    AND transaction_code in ('PMDB', 'PMTP')
) tmp
group by 1 
order by 1
;	
