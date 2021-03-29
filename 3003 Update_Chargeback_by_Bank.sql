
-- chargeback by bank
WITH instant_transfers AS (select T.id, T.user_id, A.bank_name,T.transaction_code, T.transaction_timestamp, T.transaction_amount
          from ANALYTICS.LOOKER."TRANSACTIONS" T
          LEFT JOIN MYSQL_DB.CHIME_PROD.ACH_ACCOUNTS A on T.user_id = A.user_id
          where transaction_code in ('PMDB', 'PMTP', 'ADac', 'ADAS', 'ADTR', 'ADar') 
            and transaction_timestamp >= '2020-11-30')
          
SELECT DATE_TRUNC('week', TRANSACTION_TIMESTAMP)::DATE AS weeks,
    bank_name,
    COUNT(DISTINCT CASE WHEN transaction_code in ('PMDB', 'PMTP') THEN instant_transfers.user_id ELSE NULL END) AS "total_users",
    COALESCE(SUM(CASE WHEN transaction_code in ('PMDB', 'PMTP') THEN instant_transfers.transaction_amount ELSE NULL END  ), 0) AS "instant_transfers_transaction_volume",
  COALESCE(SUM(CASE WHEN transaction_code in ( 'ADac', 'ADAS', 'ADTR') THEN instant_transfers.transaction_amount ELSE NULL END  ), 0) AS "instant_transfers_chargeback_volume"
FROM instant_transfers 
WHERE (instant_transfers.transaction_timestamp  >= TO_TIMESTAMP('2020-11-30'))
GROUP BY 1,2
ORDER BY 1,2;

-- check how off we are by adding the bank as aggregate layer (potential duplicates as mapping is on user level and user can have multiple bank accounts)
WITH instant_transfers AS (select T.id, T.user_id, T.transaction_code, T.transaction_timestamp, T.transaction_amount
          from ANALYTICS.LOOKER."TRANSACTIONS" T
          where transaction_code in ('PMDB', 'PMTP', 'ADac', 'ADAS', 'ADTR', 'ADar') 
            and transaction_timestamp >= '2020-11-30')
          
SELECT DATE_TRUNC('week', TRANSACTION_TIMESTAMP)::DATE AS weeks,
    COUNT(DISTINCT CASE WHEN transaction_code in ('PMDB', 'PMTP') THEN instant_transfers.user_id ELSE NULL END) AS "total_users",
    COALESCE(SUM(CASE WHEN transaction_code in ('PMDB', 'PMTP') THEN instant_transfers.transaction_amount ELSE NULL END  ), 0) AS "instant_transfers_transaction_volume",
  COALESCE(SUM(CASE WHEN transaction_code in ( 'ADac', 'ADAS', 'ADTR') THEN instant_transfers.transaction_amount ELSE NULL END  ), 0) AS "instant_transfers_chargeback_volume"
FROM instant_transfers 
WHERE (instant_transfers.transaction_timestamp  >= TO_TIMESTAMP('2020-11-30'))
GROUP BY 1
ORDER BY 1;
