with dd_history as(
  select user_id, DATE_TRUNC('day', TRANSACTION_TIMESTAMP)::DATE AS dd_date, transaction_amount
  from ANALYTICS.LOOKER.PAYROLL_DIRECT_DEPOSITS
  where TRANSACTION_TIMESTAMP >= '2020-06-01'
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
sum(case when datediff(day, dd_date, dte) >= 0 and datediff(day, dd_date, dte) <= 32 then transaction_amount else 0 end) as last_32_day_DD_amount,
sum(case when datediff(day, dd_date, dte) >= 0 and datediff(day, dd_date, dte) <= 92 then transaction_amount else 0 end)/3 as avg_month_last_92_day_DD_amount,
sum(case when datediff(day, dd_date, dte) >= 0 and datediff(day, dd_date, dte) <= 183 then transaction_amount else 0 end)/6 as avg_month_last_183_day_DD_amount
from(
  select 
  day_series.dte,
  dd_history.user_id,
  dd_history.dd_date,
  dd_history.transaction_amount
  from day_series
  cross join dd_history
) panel_data
group by 1,2
order by 1,2
;

-- create a temp table for Shu    
CREATE OR REPLACE TABLE REST.TEST.DD_Amount_Tracker_03_24_2021 AS (with dd_history as(
  select user_id, DATE_TRUNC('day', TRANSACTION_TIMESTAMP)::DATE AS dd_date, transaction_amount
  from ANALYTICS.LOOKER.PAYROLL_DIRECT_DEPOSITS
  where TRANSACTION_TIMESTAMP >= '2020-06-01'
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
sum(case when datediff(day, dd_date, dte) >= 0 and datediff(day, dd_date, dte) <= 32 then transaction_amount else 0 end) as last_32_day_DD_amount,
sum(case when datediff(day, dd_date, dte) >= 0 and datediff(day, dd_date, dte) <= 92 then transaction_amount else 0 end)/3 as avg_month_last_92_day_DD_amount,
sum(case when datediff(day, dd_date, dte) >= 0 and datediff(day, dd_date, dte) <= 183 then transaction_amount else 0 end)/6 as avg_month_last_183_day_DD_amount
from(
  select 
  day_series.dte,
  dd_history.user_id,
  dd_history.dd_date,
  dd_history.transaction_amount
  from day_series
  cross join dd_history
) panel_data
group by 1,2
order by 1,2);  

select * from REST.TEST.DD_Amount_Tracker_03_24_2021 limit 10;