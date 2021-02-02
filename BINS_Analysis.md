```

// transaction and chargebacks by bins (verified to match looker report)
WITH instant_transfers AS (select id, user_id, transaction_code, transaction_timestamp, transaction_amount
          from ANALYTICS.LOOKER."TRANSACTIONS" transactions
          where transaction_code in ('PMDB', 'PMTP', 'ADac', 'ADAS', 'ADTR', 'ADar') 
          and transaction_timestamp >= '2020-12-01')
          
SELECT card.bin,
    COUNT(DISTINCT CASE WHEN transaction_code in ('PMDB', 'PMTP') THEN instant_transfers.user_id ELSE NULL END) AS "instant_transfers.total_users",
    COALESCE(SUM(CASE WHEN transaction_code in ('PMDB', 'PMTP') THEN instant_transfers.transaction_amount ELSE NULL END  ), 0) AS "instant_transfers.instant_transfers_transaction_volume",
	COALESCE(SUM(CASE WHEN transaction_code in ( 'ADac', 'ADAS', 'ADTR') THEN instant_transfers.transaction_amount ELSE NULL END  ), 0) AS "instant_transfers.instant_transfers_chargeback_volume"
FROM instant_transfers 
left join (select try_cast (user_id as integer) as user_id, bin, 									
    row_number() over (partition by user_id order by timestamp desc) rn 									
    from "SEGMENT"."MOVE_MONEY_SERVICE"."DEBIT_CARD_LINKING_SUCCEEDED" qualify rn = 1) card 
on card.user_id = instant_transfers.user_id

WHERE (instant_transfers.transaction_timestamp  >= TO_TIMESTAMP('2020-12-1'))
GROUP BY 1
ORDER BY 1;   

```

``` 
//user_id maps to multiple banks
SELECT T.USER_ID, count(B.BANK_NAME)					
    FROM ANALYTICS.LOOKER."TRANSACTIONS" T					
    LEFT JOIN MYSQL_DB.CHIME_PROD.ACH_ACCOUNTS B ON T.USER_ID = B.USER_ID					
    WHERE TRANSACTION_CODE IN ('PMDB', 'PMTP')					
    AND TRANSACTION_TIMESTAMP >= '2021-01-01' 					
    group by 1 order by 2 desc;					
```

```
// shared by Baishi, user - bin: 1-1
select try_cast (user_id as integer) as user_id, bin, 							
        row_number() over (partition by user_id order by timestamp desc) rn 							
    from "SEGMENT"."MOVE_MONEY_SERVICE"."DEBIT_CARD_LINKING_SUCCEEDED" qualify rn = 1							
```

```
// mapping bank to BIN		
// this mapping is not accurate as user can have multiple accounts										
// each transaction will be tagged several different banks										
// 50% of customers has 1 bank; 30% has 2; 13% has 3;										
inst_trfs AS (										
    SELECT T.ID, B.BANK_NAME,T.USER_ID,T.TRANSACTION_TIMESTAMP,T.TRANSACTION_AMOUNT										
    FROM ANALYTICS.LOOKER."TRANSACTIONS" T										
    LEFT JOIN MYSQL_DB.CHIME_PROD.ACH_ACCOUNTS B ON T.USER_ID = B.USER_ID										
    WHERE TRANSACTION_CODE IN ('PMDB', 'PMTP')										
    AND TRANSACTION_TIMESTAMP >= '2021-01-01' 										
)																
```
