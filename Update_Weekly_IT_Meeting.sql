
'Walmart MoneyCard by Green Dot'
'Netspend'
'Netspend - SkylightOne'
'TD Bank'

-- part I transaction and chargebacks
WITH instant_transfers AS (select id, user_id, transaction_code, transaction_timestamp, transaction_amount
          from ANALYTICS.LOOKER."TRANSACTIONS" transactions
          where transaction_code in ('PMDB', 'PMTP', 'ADac', 'ADAS', 'ADTR', 'ADar') and USER_ID IN (
              SELECT DISTINCT USER_ID
                FROM MYSQL_DB.CHIME_PROD.ACH_ACCOUNTS
              WHERE BANK_NAME = 'Netspend - SkylightOne'
          and transaction_timestamp >= '2020-11-30')
         )
          
SELECT DATE_TRUNC('week', TRANSACTION_TIMESTAMP)::DATE AS weeks,
    COUNT(DISTINCT CASE WHEN transaction_code in ('PMDB', 'PMTP') THEN instant_transfers.user_id ELSE NULL END) AS "instant_transfers.total_users",
    COALESCE(SUM(CASE WHEN transaction_code in ('PMDB', 'PMTP') THEN instant_transfers.transaction_amount ELSE NULL END  ), 0) AS "instant_transfers.instant_transfers_transaction_volume",
    COALESCE(SUM(CASE WHEN transaction_code in ( 'ADac', 'ADAS', 'ADTR') THEN instant_transfers.transaction_amount ELSE NULL END  ), 0) AS "instant_transfers.instant_transfers_chargeback_volume"
FROM CHIME.FINANCE.MEMBERS  AS members
LEFT JOIN instant_transfers ON (members."ID") = instant_transfers.user_id
WHERE (instant_transfers.transaction_timestamp  >= TO_TIMESTAMP('2020-11-30'))
GROUP BY 1
ORDER BY 1;

-- part II transaction made by users who chargeback
WITH instant_transfers AS (select id, user_id, transaction_code, transaction_timestamp, transaction_amount
          from ANALYTICS.LOOKER."TRANSACTIONS" transactions
          where transaction_code in ('PMDB', 'PMTP') and USER_ID IN (
              SELECT DISTINCT USER_ID
                FROM MYSQL_DB.CHIME_PROD.ACH_ACCOUNTS
              WHERE BANK_NAME = 'Netspend - SkylightOne')
          and transaction_timestamp >= '2020-11-30')
          
SELECT DATE_TRUNC('week', TRANSACTION_TIMESTAMP)::DATE AS weeks,
    COUNT(DISTINCT CASE WHEN transaction_code in ('PMDB', 'PMTP') THEN instant_transfers.user_id ELSE NULL END) AS "instant_transfers.chargeback_users",
    COALESCE(SUM(CASE WHEN transaction_code in ('PMDB', 'PMTP') THEN instant_transfers.transaction_amount ELSE NULL END  ), 0) AS "instant_transfers.instant_transfers_transaction_volume"
FROM CHIME.FINANCE.MEMBERS  AS members
LEFT JOIN instant_transfers ON (members."ID") = instant_transfers.user_id
WHERE instant_transfers.transaction_timestamp  >= TO_TIMESTAMP('2020-11-30')
       and instant_transfers.user_id in (select distinct user_id
          from ANALYTICS.LOOKER."TRANSACTIONS" transactions
          where transaction_code in ('ADac', 'ADAS', 'ADTR', 'ADar') and USER_ID IN (
                 SELECT DISTINCT USER_ID
                 FROM MYSQL_DB.CHIME_PROD.ACH_ACCOUNTS
                WHERE BANK_NAME = 'Netspend - SkylightOne'))
GROUP BY 1
ORDER BY 1;

-- part III transactions from non-chargeback customers with balance <=5
WITH instant_transfers AS (select id, user_id, transaction_code, transaction_timestamp, transaction_amount
          from ANALYTICS.LOOKER."TRANSACTIONS" transactions
          where transaction_code in ('PMDB', 'PMTP') and USER_ID IN (
              SELECT DISTINCT USER_ID
                FROM MYSQL_DB.CHIME_PROD.ACH_ACCOUNTS
              WHERE BANK_NAME = 'Walmart MoneyCard by Green Dot')
          and transaction_timestamp >= '2020-11-30')
, balance AS (SELECT *
              FROM mysql_db.galileo.GALILEO_DAILY_BALANCES
              WHERE ACCOUNT_TYPE = '6' AND BALANCE_ON_DATE = DATEADD('DAY', -1, CURRENT_DATE()) 
)

SELECT DATE_TRUNC('week', TRANSACTION_TIMESTAMP)::DATE AS weeks,
  COALESCE(SUM(CASE WHEN transaction_code in ('PMDB', 'PMTP') THEN instant_transfers.transaction_amount ELSE NULL END  ), 0) AS "instant_transfers.instant_transfers_transaction_volume"
FROM CHIME.FINANCE.MEMBERS  AS members
LEFT JOIN instant_transfers ON (members."ID") = instant_transfers.user_id
LEFT JOIN balance ON instant_transfers.user_id = balance.user_id 
WHERE instant_transfers.transaction_timestamp  >= TO_TIMESTAMP('2020-11-30')
       and instant_transfers.user_id not in (select distinct user_id
          from ANALYTICS.LOOKER."TRANSACTIONS" transactions
          where transaction_code in ('ADac', 'ADAS', 'ADTR', 'ADar') and USER_ID IN (
                 SELECT DISTINCT USER_ID
                 FROM MYSQL_DB.CHIME_PROD.ACH_ACCOUNTS
                WHERE BANK_NAME = 'Walmart MoneyCard by Green Dot')) and balance.AVAILABLE_BALANCE<=5
GROUP BY 1
ORDER BY 1;

-- check volume after blocking
WITH inst_trfs AS (
  SELECT *
  FROM ANALYTICS.LOOKER."TRANSACTIONS"
  WHERE TRANSACTION_CODE IN ('PMDB', 'PMTP')
  AND TRANSACTION_TIMESTAMP >= '2020-12-01' --'2020-10-01' 
  AND USER_ID IN (
  SELECT DISTINCT USER_ID
  FROM MYSQL_DB.CHIME_PROD.ACH_ACCOUNTS
  WHERE BANK_NAME = 'Netspend - SkylightOne')
)

SELECT DATE_TRUNC('DAY', TRANSACTION_TIMESTAMP::TIMESTAMP)::DATE AS DT,
  COUNT(DISTINCT USER_ID) AS NUM_USERS,
  COUNT(ID) AS TOT_NUM,
  SUM(TRANSACTION_AMOUNT) AS TOT_AMT
FROM inst_trfs
GROUP BY 1
ORDER BY 1

-- ACH Pull and Push



