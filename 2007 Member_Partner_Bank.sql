select user_id, PRIMARY_PROGRAM_ASSIGNED from "ANALYTICS"."LOOKER"."MEMBER_PARTNER_BANK" where user_id = 20489095;

// Following query for updating ticket ALYTMM-75
// update cash charge back weekly	
WITH instant_transfers AS (select id, user_id,transaction_code, transaction_timestamp, transaction_amount	
          from ANALYTICS.LOOKER."TRANSACTIONS" T	
          where transaction_code in ('PMDB', 'PMTP', 'ADac', 'ADAS', 'ADTR', 'ADar') 	
          and transaction_timestamp >= '2021-01-01')	
          	
SELECT DATE_TRUNC('day', TRANSACTION_TIMESTAMP)::DATE AS TXN_day,	
    bank.PRIMARY_PROGRAM_ASSIGNED,
    COUNT(DISTINCT CASE WHEN transaction_code in ('PMDB', 'PMTP') THEN instant_transfers.user_id ELSE NULL END) AS "total_users",	
    COALESCE(SUM(CASE WHEN transaction_code in ('PMDB', 'PMTP') THEN instant_transfers.transaction_amount ELSE NULL END  ), 0) AS "transaction_volume",	
	COALESCE(SUM(CASE WHEN transaction_code in ( 'ADac', 'ADAS', 'ADTR') THEN instant_transfers.transaction_amount ELSE NULL END  ), 0) AS "cash_chargeback"
FROM instant_transfers 	
LEFT JOIN "ANALYTICS"."LOOKER"."MEMBER_PARTNER_BANK" bank on instant_transfers.user_id = bank.user_id
WHERE (instant_transfers.transaction_timestamp  >= TO_TIMESTAMP('2021-1-1'))	
GROUP BY 1,2	
ORDER BY 1,2;	
	
//update cohort charge back weekly	
WITH instant_transfers AS (select id, user_id,transaction_code, transaction_timestamp, transaction_amount	
          from ANALYTICS.LOOKER."TRANSACTIONS" T	
          where transaction_code in ('PMDB', 'PMTP', 'ADac', 'ADAS', 'ADTR', 'ADar') 	
          and transaction_timestamp >= '2020-11-01')	
          	
SELECT DATE_TRUNC('day', TRANSACTION_TIMESTAMP)::DATE AS TXN_day,
   bank.PRIMARY_PROGRAM_ASSIGNED,
    COUNT(DISTINCT CASE WHEN transaction_code in ('PMDB', 'PMTP') THEN instant_transfers.user_id ELSE NULL END) AS "instant_transfers.chargeback_users",	
    COALESCE(SUM(CASE WHEN transaction_code in ('PMDB', 'PMTP') THEN instant_transfers.transaction_amount ELSE NULL END  ), 0) AS "instant_transfers.instant_transfers_transaction_volume"	
FROM instant_transfers
LEFT JOIN "ANALYTICS"."LOOKER"."MEMBER_PARTNER_BANK" bank on instant_transfers.user_id = bank.user_id
WHERE instant_transfers.transaction_timestamp  >= TO_TIMESTAMP('2021-1-1')	
       and instant_transfers.user_id in (select distinct user_id	
          from ANALYTICS.LOOKER."TRANSACTIONS" transactions	
          where transaction_code in ('ADac', 'ADAS', 'ADTR', 'ADar'))	
GROUP BY 1,2	
ORDER BY 1,2;	