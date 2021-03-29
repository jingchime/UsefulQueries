with debit_purchase_history as(
  select user_id, DATE_TRUNC('day', TRANSACTION_TIMESTAMP)::DATE AS trxn_date, transaction_amount
  from ANALYTICS.LOOKER."TRANSACTIONS"  
   where transaction_code in ('ISA', 'ISC', 'ISJ', 'ISL', 'ISM', 'ISR', 'ISZ', 'VSA', 'VSC', 'VSJ', 'VSL', 'VSM', 'VSR', 'VSZ',
      'SDA', 'SDC', 'SDL', 'SDM', 'SDR', 'SDV', 'SDZ', 'PLM', 'PLA', 'PRA') and TRANSACTION_TIMESTAMP >= '2020-06-01'
),

day_series as(
  select dte from (
  select dateadd(day, '-' || seq4(), current_date()) as dte 
  from table (generator(rowcount => 365))
  where dte >= '2021-01-01')
)
  
select
user_id,
dte,
sum(case when datediff(day, trxn_date, dte) >= 0 and datediff(day, trxn_date, dte) <= 32 then transaction_amount else 0 end) as last_32_day_debit_purchase_amount,
sum(case when datediff(day, trxn_date, dte) >= 0 and datediff(day, trxn_date, dte) <= 92 then transaction_amount else 0 end)/3 as avg_month_last_92_day_debit_purchase_amount,
sum(case when datediff(day, trxn_date, dte) >= 0 and datediff(day, trxn_date, dte) <= 183 then transaction_amount else 0 end)/6 as avg_month_last_183_day_debit_purchase_amount
from(
  select 
  day_series.dte,
  debit_purchase_history.user_id,
  debit_purchase_history.trxn_date,
  debit_purchase_history.transaction_amount
  from day_series
  cross join debit_purchase_history
) panel_data
group by 1,2
order by 1,2
;

-- create table
CREATE OR REPLACE TABLE REST.TEST.Debit_Purchase_Amount_Tracker_03_24_2021 AS (with debit_purchase_history as(
  select user_id, DATE_TRUNC('day', TRANSACTION_TIMESTAMP)::DATE AS trxn_date, transaction_amount
  from ANALYTICS.LOOKER."TRANSACTIONS"  
   where transaction_code in ('ISA', 'ISC', 'ISJ', 'ISL', 'ISM', 'ISR', 'ISZ', 'VSA', 'VSC', 'VSJ', 'VSL', 'VSM', 'VSR', 'VSZ',
      'SDA', 'SDC', 'SDL', 'SDM', 'SDR', 'SDV', 'SDZ', 'PLM', 'PLA', 'PRA') and TRANSACTION_TIMESTAMP >= '2020-06-01'
),

day_series as(
  select dte from (
  select dateadd(day, '-' || seq4(), current_date()) as dte 
  from table (generator(rowcount => 365))
  where dte >= '2021-01-01')
)
  
select
user_id,
dte,
sum(case when datediff(day, trxn_date, dte) >= 0 and datediff(day, trxn_date, dte) <= 32 then transaction_amount else 0 end) as last_32_day_debit_purchase_amount,
sum(case when datediff(day, trxn_date, dte) >= 0 and datediff(day, trxn_date, dte) <= 92 then transaction_amount else 0 end)/3 as avg_month_last_92_day_debit_purchase_amount,
sum(case when datediff(day, trxn_date, dte) >= 0 and datediff(day, trxn_date, dte) <= 183 then transaction_amount else 0 end)/6 as avg_month_last_183_day_debit_purchase_amount
from(
  select 
  day_series.dte,
  debit_purchase_history.user_id,
  debit_purchase_history.trxn_date,
  debit_purchase_history.transaction_amount
  from day_series
  cross join debit_purchase_history
) panel_data
group by 1,2
order by 1,2);  