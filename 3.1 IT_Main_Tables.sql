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

-- logic shared by Baishi to map BIN to user_id (not accurate)
debit_card as(
    select try_cast (user_id as integer) as user_id, bin, 
        row_number() over (partition by user_id order by timestamp desc) rn 
    from "SEGMENT"."MOVE_MONEY_SERVICE"."DEBIT_CARD_LINKING_SUCCEEDED" qualify rn = 1
)

