// 1 Top five merchants instant transfer chargeback users spent their money on:
SELECT 
MERCHANT_NAME,
SUM(TRANSACTION_AMOUNT) 
FROM ANALYTICS.LOOKER."TRANSACTIONS" T
WHERE user_id in (select distinct user_id	
      from ANALYTICS.LOOKER."TRANSACTIONS" transactions	
      where transaction_code in ('ADac', 'ADAS', 'ADTR', 'ADar')) AND TRANSACTION_CODE = 'VSC'  -- IT charged back users spent on what merchants
GROUP BY 1
ORDER BY 2
limit 5;

// 2 Instant Transfer volume for the above five merchants and the $ chargeback rate
WITH user_merchant AS(
select distinct user_id, MERCHANT_NAME from ANALYTICS.LOOKER."TRANSACTIONS"
WHERE MERCHANT_NAME IN (
      'Alibaba.com            408-7855580  CAUS', 
      'NIKE.COM AP            NIKE.COM     ORUS', 
      'CASH APP*JUAN ANTON    8774174551   CAUS', 
      'FIVERR                 9543682267   NYUS', 
      'CASH APP*PRINCESS A    8774174551   CAUS'))
SELECT 
M.MERCHANT_NAME,
COALESCE(SUM(CASE WHEN transaction_code in ('PMDB', 'PMTP') THEN transaction_amount ELSE NULL END  ), 0) AS "transaction_volume",
COALESCE(SUM(CASE WHEN transaction_code in ( 'ADac', 'ADAS', 'ADTR', 'ADar') THEN transaction_amount ELSE NULL END  ), 0) AS "cash_chargeback"
FROM ANALYTICS.LOOKER."TRANSACTIONS" T
LEFT JOIN user_merchant M on T.user_id = M.user_id
WHERE T.user_id IN (SELECT DISTINCT user_id from user_merchant)
GROUP BY 1
ORDER BY 2 DESC;

// 3 Spent (VSC) by top merchants 
SELECT 
DATE_TRUNC('WEEK', TRANSACTION_TIMESTAMP)::DATE AS TXN_Week,
MERCHANT_NAME, 
SUM(TRANSACTION_AMOUNT)
FROM ANALYTICS.LOOKER."TRANSACTIONS"
WHERE MERCHANT_NAME IN (
      'Alibaba.com            408-7855580  CAUS', 
      'NIKE.COM AP            NIKE.COM     ORUS', 
      'CASH APP*JUAN ANTON    8774174551   CAUS', 
      'FIVERR                 9543682267   NYUS', 
      'CASH APP*PRINCESS A    8774174551   CAUS')
AND TRANSACTION_CODE = 'VSC'
AND transaction_timestamp >= '2020-11-01'
GROUP BY 1,2
ORDER BY 1,2;

----------focuse on Alibaba after the above three-------------------
// 4 alibaba spending by instant transfer chargedback users
SELECT DATE_TRUNC('WEEK', TRANSACTION_TIMESTAMP)::DATE AS TXN_Week,
SUM(TRANSACTION_AMOUNT) AS TOTAL_SPENDING,
SUM(CASE WHEN user_id in (select distinct user_id	
      from ANALYTICS.LOOKER."TRANSACTIONS" transactions	
      where transaction_code in ('ADac', 'ADAS', 'ADTR', 'ADar')) THEN TRANSACTION_AMOUNT ELSE 0 END) AS SPENDING_BY_IT_Chargedback_Users
FROM ANALYTICS.LOOKER."TRANSACTIONS"
WHERE MERCHANT_NAME ilike '%Alibaba.com%'
AND TRANSACTION_CODE = 'VSC'
AND transaction_timestamp >= '2020-11-01'
GROUP BY 1;

// 5-1 
SELECT 
user_id,
min(case when transaction_code in ('PMDB', 'PMTP') then DATE_TRUNC('day', TRANSACTION_TIMESTAMP)::DATE end) as instant_transfer_date,
min(case when transaction_code in ('VSC') and MERCHANT_NAME ilike '%Alibaba.com%' then DATE_TRUNC('day', TRANSACTION_TIMESTAMP)::DATE end) as alibaba_date,
sum(case when transaction_code in ('PMDB', 'PMTP') then transaction_amount END) as total_transaction_volume
FROM ANALYTICS.LOOKER."TRANSACTIONS" 
WHERE user_id IN (
    SELECT DISTINCT user_id FROM ANALYTICS.LOOKER."TRANSACTIONS" 
    WHERE TRANSACTION_CODE IN ('ADac', 'ADAS', 'ADTR', 'ADar')) 
GROUP BY 1;

// 5 supplimental
select day,
DATE_TRUNC('week', day)::DATE as week
FROM(
  select distinct DATE_TRUNC('day', TRANSACTION_TIMESTAMP)::DATE day
  from ANALYTICS.LOOKER."TRANSACTIONS" ) ORDER BY 1;

// 5-2
SELECT 
user_id,
min(case when transaction_code in ('PMDB', 'PMTP') then DATE_TRUNC('day', TRANSACTION_TIMESTAMP)::DATE end) as instant_transfer_date,
min(case when transaction_code in ('VSC') and MERCHANT_NAME ilike '%Alibaba.com%' then DATE_TRUNC('day', TRANSACTION_TIMESTAMP)::DATE end) as alibaba_date,
sum(case when transaction_code in ('PMDB', 'PMTP') then transaction_amount END) as total_transaction_volume
FROM ANALYTICS.LOOKER."TRANSACTIONS" 
WHERE user_id IN (
    SELECT DISTINCT user_id FROM ANALYTICS.LOOKER."TRANSACTIONS" 
    WHERE TRANSACTION_CODE IN ('PMDB', 'PMTP', 'ADac', 'ADAS', 'ADTR', 'ADar')) 
GROUP BY 1;

// 6 IT users category
select
DATE_TRUNC('WEEK', TRANSACTION_TIMESTAMP)::DATE AS TXN_Week,
tmp.CUSTOMER_GROUP,
count(distinct transactions.user_id) as num_users,
COALESCE(SUM(CASE WHEN transaction_code in ('PMDB', 'PMTP') THEN transaction_amount ELSE NULL END  ), 0) AS "transaction_volume",
COALESCE(SUM(CASE WHEN transaction_code in ( 'ADac', 'ADAS', 'ADTR') THEN transaction_amount ELSE NULL END  ), 0) AS "chargeback_volume"
FROM
ANALYTICS.LOOKER."TRANSACTIONS" transactions
JOIN
    (
      SELECT 
      user_id,
      -sum(case when transaction_code in ('VSC') and MERCHANT_NAME ilike '%Alibaba.com%' then transaction_amount end) as alibaba_spend,
      -sum(case when transaction_code in ('VSC') then transaction_amount end) as total_spend,
      alibaba_spend/total_spend as pct_alibaba,
      case when pct_alibaba is null then '0%'
           when pct_alibaba<0.2 then '(0-20%)'
           when pct_alibaba<0.4 then '[20-40%)'
           when pct_alibaba<0.6 then '[40-60%)'
           when pct_alibaba<0.8 then '[60-80%)'
           when pct_alibaba<1 then '[80-100%)'
           else '100%'
      end as customer_group
      FROM ANALYTICS.LOOKER."TRANSACTIONS" 
      WHERE user_id IN (
          SELECT DISTINCT user_id FROM ANALYTICS.LOOKER."TRANSACTIONS" 
          WHERE TRANSACTION_CODE IN ('PMDB', 'PMTP', 'ADac', 'ADAS', 'ADTR', 'ADar')) 
      GROUP BY 1
    ) tmp
on transactions.user_id = tmp.user_id
where TXN_WEEK >= '2020-12-1'
group by 1,2
order by 1,2;

// 7 Non-IT users category
select
DATE_TRUNC('WEEK', TRANSACTION_TIMESTAMP)::DATE AS TXN_Week,
tmp.CUSTOMER_GROUP,
count(distinct transactions.user_id) as num_users,
COALESCE(SUM(CASE WHEN transaction_code in ('PMDB', 'PMTP') THEN transaction_amount ELSE NULL END  ), 0) AS "transaction_volume",
COALESCE(SUM(CASE WHEN transaction_code in ( 'ADac', 'ADAS', 'ADTR') THEN transaction_amount ELSE NULL END  ), 0) AS "chargeback_volume"
FROM
ANALYTICS.LOOKER."TRANSACTIONS" transactions
JOIN
    (
      SELECT 
      user_id,
      -sum(case when transaction_code in ('VSC') and MERCHANT_NAME ilike '%Alibaba.com%' then transaction_amount end) as alibaba_spend,
      -sum(case when transaction_code in ('VSC') then transaction_amount end) as total_spend,
      alibaba_spend/total_spend as pct_alibaba,
      case when pct_alibaba is null then '0%'
           when pct_alibaba<0.2 then '(0-20%)'
           when pct_alibaba<0.4 then '[20-40%)'
           when pct_alibaba<0.6 then '[40-60%)'
           when pct_alibaba<0.8 then '[60-80%)'
           when pct_alibaba<1 then '[80-100%)'
           else '100%'
      end as customer_group
      FROM ANALYTICS.LOOKER."TRANSACTIONS" 
      WHERE user_id NOT IN (
          SELECT DISTINCT user_id FROM ANALYTICS.LOOKER."TRANSACTIONS" 
          WHERE TRANSACTION_CODE IN ('PMDB', 'PMTP', 'ADac', 'ADAS', 'ADTR', 'ADar') and user_id is not null) 
      GROUP BY 1
    ) tmp
on transactions.user_id = tmp.user_id
where TXN_WEEK >= '2020-12-1'
group by 1,2
order by 1,2;

// 8 Alibaba spending by instant transfer chargedback users
SELECT DATE_TRUNC('WEEK', TRANSACTION_TIMESTAMP)::DATE AS TXN_Week,
case when b.FIRST_DD_DATE is not null then 1 else 0 end as if_dd,
count(distinct user_id) as num_users,
SUM(TRANSACTION_AMOUNT) AS TOTAL_SPENDING,
SUM(CASE WHEN user_id in (select distinct user_id	
      from ANALYTICS.LOOKER."TRANSACTIONS" transactions	
      where transaction_code in ('ADac', 'ADAS', 'ADTR', 'ADar')) THEN TRANSACTION_AMOUNT ELSE 0 END) AS SPENDING_BY_IT_Chargedback_Users
FROM ANALYTICS.LOOKER."TRANSACTIONS" transactions
LEFT JOIN CHIME.FINANCE.MEMBERS AS B
    on transactions.user_id = B.id
WHERE MERCHANT_NAME ilike '%Alibaba.com%'
AND TRANSACTION_CODE = 'VSC'
AND transaction_timestamp >= '2020-11-01'
GROUP BY 1,2
order by 1,2;

// sample sent to Bin: non-dders non-IT Alibaba users
SELECT 
distinct user_id
FROM ANALYTICS.LOOKER."TRANSACTIONS" transactions
LEFT JOIN CHIME.FINANCE.MEMBERS AS B
    on transactions.user_id = B.id
WHERE MERCHANT_NAME ilike '%Alibaba.com%'
AND TRANSACTION_CODE = 'VSC'
AND transaction_timestamp >= '2020-11-01'
AND user_id not in (SELECT DISTINCT user_id FROM ANALYTICS.LOOKER."TRANSACTIONS" 
          WHERE TRANSACTION_CODE IN ('PMDB', 'PMTP', 'ADac', 'ADAS', 'ADTR', 'ADar') and user_id is not null)
AND b.FIRST_DD_DATE IS NULL;

// 9 latest balance for the above group
with non_dd_non_it_alibaba_users as (
select
distinct user_id
FROM ANALYTICS.LOOKER."TRANSACTIONS" transactions
LEFT JOIN CHIME.FINANCE.MEMBERS AS B
    on transactions.user_id = B.id
WHERE MERCHANT_NAME ilike '%Alibaba.com%'
AND TRANSACTION_CODE = 'VSC'
AND transaction_timestamp >= '2020-11-01'
AND user_id not in (SELECT DISTINCT user_id FROM ANALYTICS.LOOKER."TRANSACTIONS" 
          WHERE TRANSACTION_CODE IN ('PMDB', 'PMTP', 'ADac', 'ADAS', 'ADTR', 'ADar') and user_id is not null)
AND b.FIRST_DD_DATE IS NULL
)

select case when CURRENT_BALANCE<0 then 'negative_balance'
            when CURRENT_BALANCE=0 then 'zero_balance'
            when CURRENT_BALANCE>0 then 'positive_balance'
end as balance_grp,
count(*) as user_num
from
(
  select
  users.user_id,
  balance.CURRENT_BALANCE
  from
  non_dd_non_it_alibaba_users users
  left join MYSQL_DB.GALILEO.galileo_daily_balances balance
  ON users.user_id = balance.user_id
  WHERE account_type = '6' 
  and unique_program_id in (609, 512, 660) 
  and BALANCE_ON_DATE = dateadd(day, -2, current_date())
) tmp
group by 1;


