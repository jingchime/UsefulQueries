select
diff_days_to_chargeback,
count(user_id) as num_users
from
(
  SELECT 
  user_id,
  min(case when transaction_code in ('PMDB', 'PMTP') then DATE_TRUNC('day', TRANSACTION_TIMESTAMP)::DATE end) as instant_transfer_date,
  min(case when transaction_code in ('ADac', 'ADAS', 'ADTR', 'ADar') then DATE_TRUNC('day', TRANSACTION_TIMESTAMP)::DATE end) as charge_back_date,
  datediff(day, instant_transfer_date, charge_back_date) as diff_days_to_chargeback
  FROM ANALYTICS.LOOKER."TRANSACTIONS" 
  WHERE user_id IN (
    SELECT DISTINCT user_id FROM ANALYTICS.LOOKER."TRANSACTIONS" 
    WHERE TRANSACTION_CODE IN ('ADac', 'ADAS', 'ADTR', 'ADar')) 
  GROUP BY 1
) tmp
group by 1
order by 1;