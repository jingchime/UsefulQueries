-- main tables
MYSQL_DB.CHIME_PROD.ACH_ACCOUNTS
ANALYTICS.LOOKER."TRANSACTIONS"

inst_trfs AS (
    SELECT *
    FROM ANALYTICS.LOOKER."TRANSACTIONS"
    WHERE TRANSACTION_CODE IN ('PMDB', 'PMTP')
    AND TRANSACTION_TIMESTAMP >= '2020-10-01'
)

--subquery from the above inst_trfs
(SELECT USER_ID,                                            
     COUNT(ID) AS num_instant_trfs,                                             
     MIN(TRANSACTION_TIMESTAMP) AS MIN_TRANSACTION_TIMESTAMP,                                            
     SUM(TRANSACTION_AMOUNT) AS TOTAL_TRANSACTION_AMOUNT                                            
     FROM inst_trfs                                         
     GROUP BY 1
) 

chargebacks AS (
    SELECT *
    FROM ANALYTICS.LOOKER."TRANSACTIONS"
    WHERE TRANSACTION_CODE IN ('ADac', 'ADAS', 'ADTR', 'ADar')
    AND TRANSACTION_TIMESTAMP >= '2020-10-01'
)

