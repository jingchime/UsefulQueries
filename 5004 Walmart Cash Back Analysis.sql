--- 1_1_1 Overall ATM disputes/transactions t1-> t4
SELECT
TO_CHAR(DATE_TRUNC('MONTH', TRANS_DATE), 'YYYY-MM') AS MTH, 
TIER_AT_TRXN,
sum(FINAL_AMT) as total_transaction,
sum(CASE WHEN FLG_DISPUTED=1 THEN FINAL_AMT ENd) as total_disputed_dollar
FROM REST.TEST.Risk_Segmentation_04_12_2021 R
WHERE TYPE_OF_TRXN IN ('ATM Withdrawals')
and trans_date >= '2021-01-01' and  trans_date  <= '2021-03-31'
GROUP BY 1, 2;

SELECT
TO_CHAR(DATE_TRUNC('MONTH', TRANS_DATE), 'YYYY-MM') AS MTH, 
TIER_AT_TRXN,
sum(FINAL_AMT) as total_transaction,
sum(CASE WHEN FLG_DISPUTED=1 THEN FINAL_AMT ENd) as total_disputed_dollar
FROM REST.TEST.Risk_Segmentation_04_12_2021 R
WHERE TYPE_OF_TRXN IN ('Debit Purchase')
and trans_date >= '2021-01-01' and  trans_date  <= '2021-03-31'
GROUP BY 1, 2;

SELECT
TO_CHAR(DATE_TRUNC('MONTH', TRANS_DATE), 'YYYY-MM') AS MTH, 
TIER_AT_TRXN,
sum(FINAL_AMT) as total_transaction,
sum(CASE WHEN FLG_DISPUTED=1 THEN FINAL_AMT ENd) as total_disputed_dollar
FROM REST.TEST.Risk_Segmentation_04_12_2021 R
WHERE TYPE_OF_TRXN IN ('Credit Purchase')
and trans_date >= '2021-01-01' and  trans_date  <= '2021-03-31'
GROUP BY 1, 2;

-- 1_1_2
SELECT
TO_CHAR(DATE_TRUNC('MONTH', TRANS_DATE), 'YYYY-MM') AS MTH, 
TIER_AT_TRXN,
COUNT(DISTINCT R.USER_ID) AS TOT_NUM_USER,
COUNT(DISTINCT DEC_USER) AS TOT_DEC_USER
FROM REST.TEST.Risk_Segmentation_04_12_2021 R
WHERE TYPE_OF_TRXN IN ('ATM Withdrawals')
and trans_date >= '2021-01-01' and  trans_date  <= '2021-03-31'
GROUP BY 1, 2;

-- 1_1_3
select 
TO_CHAR(DATE_TRUNC('MONTH', TRANS_DATE), 'YYYY-MM') AS MTH, 
tier.TIER_AT_TRXN,
sum(cashback_amt) as total_cash_back
from mysql_db.chime_prod.realtime_auth_events cashback
LEFT JOIN REST.TEST.TRANSACTIONS_TIERS_MAPPING_0411 tier
  ON cashback.id = tier.TRXN_ID
where cashback_amt < 0
and TRANS_DATE >= '2021-01-01' and  TRANS_DATE  <= '2021-03-31'
--and (merch_name like 'WM %' or merch_name ilike 'Wal-mart%') 
and resp_code='00' --approved
group by 1,2
order by 1,2;

-- 1_2
select 
TO_CHAR(DATE_TRUNC('MONTH', TRANS_DATE), 'YYYY-MM') AS MTH, 
-sum(cashback_amt) as total_cash_back
from mysql_db.chime_prod.realtime_auth_events cashback
where cashback_amt < 0
and TRANS_DATE >= '2020-01-01' and  TRANS_DATE  <= '2021-03-31'
and (merch_name like 'WM %' or merch_name ilike 'Wal-mart%') 
and resp_code='00' --approved
group by 1
order by 1;

select 
TO_CHAR(DATE_TRUNC('MONTH', TRANS_DATE), 'YYYY-MM') AS MTH, 
-sum(cashback_amt) as total_cash_back
from mysql_db.chime_prod.realtime_auth_events cashback
where cashback_amt <= -500
and TRANS_DATE >= '2020-01-01' and  TRANS_DATE  <= '2021-03-31'
and (merch_name like 'WM %' or merch_name ilike 'Wal-mart%') 
and resp_code='00' --approved
group by 1
order by 1;


select 
TO_CHAR(DATE_TRUNC('MONTH', TRANS_DATE), 'YYYY-MM') AS MTH, 
-sum(cashback_amt) as total_cash_back
from mysql_db.chime_prod.realtime_auth_events cashback
where cashback_amt > -500 and cashback_amt <= -250
and TRANS_DATE >= '2020-01-01' and  TRANS_DATE  <= '2021-03-31'
and (merch_name like 'WM %' or merch_name ilike 'Wal-mart%') 
and resp_code='00' --approved
group by 1
order by 1;

select 
TO_CHAR(DATE_TRUNC('MONTH', TRANS_DATE), 'YYYY-MM') AS MTH, 
-sum(cashback_amt) as total_cash_back
from mysql_db.chime_prod.realtime_auth_events cashback
where cashback_amt > -250 and cashback_amt <= -100
and TRANS_DATE >= '2020-01-01' and  TRANS_DATE  <= '2021-03-31'
and (merch_name like 'WM %' or merch_name ilike 'Wal-mart%') 
and resp_code='00' --approved
group by 1
order by 1;

select 
TO_CHAR(DATE_TRUNC('MONTH', TRANS_DATE), 'YYYY-MM') AS MTH, 
-sum(cashback_amt) as total_cash_back
from mysql_db.chime_prod.realtime_auth_events cashback
where cashback_amt > -100 and cashback_amt <0
and TRANS_DATE >= '2020-01-01' and  TRANS_DATE  <= '2021-03-31'
and (merch_name like 'WM %' or merch_name ilike 'Wal-mart%') 
and resp_code='00' --approved
group by 1
order by 1;

-- 1_3
select 
TO_CHAR(DATE_TRUNC('MONTH', TRANS_DATE), 'YYYY-MM') AS MTH, 
-sum(cashback_amt) as total_cash_back,
SUM(CASE WHEN E.TRANSACTION_ID IS NOT NULL THEN -cashback_amt ELSE 0 END) AS disputed_cash_back,
COUNT(DISTINCT cashback.id) AS TOTAL_CASH_BACK_COUNT,
COUNT(DISTINCT CASE WHEN E.TRANSACTION_ID IS NOT NULL THEN cashback.id ELSE NULL END) AS disputed_cash_COUNT
from mysql_db.chime_prod.realtime_auth_events cashback
LEFT JOIN (SELECT *,
      (case when transaction_code like 'AD%' then 4
            when transaction_code like 'FE%' then 5
            when transaction_code like 'IS%' then 6
            when transaction_code like 'PM%' then 7
            when transaction_code like 'SD%' then 8
            when transaction_code like 'VS%' then 9 else 0 end) AS leading_num
     FROM ANALYTICS.LOOKER.TRANSACTIONS) AS trnx
  ON cashback.id = trnx.id
  LEFT JOIN (SELECT DISTINCT A.USER_ID, B.TRANSACTION_ID, A.REASON
             FROM ANALYTICS.LOOKER.USER_DISPUTE_CLAIMS AS A
             LEFT JOIN ANALYTICS.LOOKER.USER_DISPUTE_CLAIM_TRANSACTIONS AS B
             ON A.ID=B.USER_DISPUTE_CLAIM_ID) AS E
  ON trnx.USER_ID=E.USER_ID
  AND TO_NUMBER(CONCAT(trnx.leading_num, trnx.AUTHORIZATION_CODE)) = TO_NUMBER(E.TRANSACTION_ID)  
where cashback_amt <= -1000
and TRANS_DATE >= '2020-01-01' and  TRANS_DATE  <= '2021-03-31'
--and (merch_name like 'WM %' or merch_name ilike 'Wal-mart%') 
and resp_code='00' --approved
group by 1
order by 1;

-- benchmarks
SELECT
TO_CHAR(DATE_TRUNC('MONTH', TRANS_DATE), 'YYYY-MM') AS MTH, 
TYPE_OF_TRXN,
SUM(CASE WHEN FLG_DISPUTED=1 THEN FINAL_AMT END),
SUM(CASE WHEN FLG_DISPUTED=1 THEN FINAL_AMT END)/SUM(FINAL_AMT) AS PERC_DISP_AMT
FROM REST.TEST.Risk_Segmentation_04_12_2021 R
where  trans_date >= '2020-01-01' and  trans_date  <= '2021-03-31'
GROUP BY 1, 2;

-- 1_4
select 
DATE_TRUNC('month', TRANSACTION_TIMESTAMP)::DATE AS MTH, 
sum(transaction_amount) as total_transaction,
SUM(CASE WHEN E.TRANSACTION_ID IS NOT NULL THEN transaction_amount ELSE 0 END) AS disputed_transaction_amount,
COUNT(DISTINCT id) AS TOTAL_num_transactions,
COUNT(DISTINCT CASE WHEN E.TRANSACTION_ID IS NOT NULL THEN id ELSE NULL END) AS disputed_num_transactions
from (SELECT *,
      (case when transaction_code like 'AD%' then 4
            when transaction_code like 'FE%' then 5
            when transaction_code like 'IS%' then 6
            when transaction_code like 'PM%' then 7
            when transaction_code like 'SD%' then 8
            when transaction_code like 'VS%' then 9 else 0 end) AS leading_num
     FROM ANALYTICS.LOOKER.TRANSACTIONS) AS trnx
  LEFT JOIN (SELECT DISTINCT A.USER_ID, B.TRANSACTION_ID, A.REASON
             FROM ANALYTICS.LOOKER.USER_DISPUTE_CLAIMS AS A
             LEFT JOIN ANALYTICS.LOOKER.USER_DISPUTE_CLAIM_TRANSACTIONS AS B
             ON A.ID=B.USER_DISPUTE_CLAIM_ID) AS E
  ON trnx.USER_ID=E.USER_ID
  AND TO_NUMBER(CONCAT(trnx.leading_num, trnx.AUTHORIZATION_CODE)) = TO_NUMBER(E.TRANSACTION_ID)  
where TRANSACTION_TIMESTAMP >= '2020-01-01' and  TRANSACTION_TIMESTAMP  <= '2021-03-31'
group by 1
order by 1;  

-- 1_5
select 
TO_CHAR(DATE_TRUNC('MONTH', TRANS_DATE), 'YYYY-MM') AS MTH, 
E.reason,
-sum(cashback_amt) as total_cash_back
from mysql_db.chime_prod.realtime_auth_events cashback
LEFT JOIN (SELECT *,
      (case when transaction_code like 'AD%' then 4
            when transaction_code like 'FE%' then 5
            when transaction_code like 'IS%' then 6
            when transaction_code like 'PM%' then 7
            when transaction_code like 'SD%' then 8
            when transaction_code like 'VS%' then 9 else 0 end) AS leading_num
     FROM ANALYTICS.LOOKER.TRANSACTIONS) AS trnx
  ON cashback.id = trnx.id
  LEFT JOIN (SELECT DISTINCT A.USER_ID, B.TRANSACTION_ID, A.REASON
             FROM ANALYTICS.LOOKER.USER_DISPUTE_CLAIMS AS A
             LEFT JOIN ANALYTICS.LOOKER.USER_DISPUTE_CLAIM_TRANSACTIONS AS B
             ON A.ID=B.USER_DISPUTE_CLAIM_ID) AS E
  ON trnx.USER_ID=E.USER_ID
  AND TO_NUMBER(CONCAT(trnx.leading_num, trnx.AUTHORIZATION_CODE)) = TO_NUMBER(E.TRANSACTION_ID)  
where cashback_amt < 0
and TRANS_DATE >= '2020-01-01' and  TRANS_DATE  <= '2021-03-31'
and (merch_name like 'WM %' or merch_name ilike 'Wal-mart%') 
and resp_code='00' --approved
and E.TRANSACTION_ID IS NOT NULL -- disputed
group by 1,2
order by 1,2;