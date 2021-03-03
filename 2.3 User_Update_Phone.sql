select item_id as user_id, created_at, *
from analytics.looker.versions_pivot 
where item_type='User' and object in ('email','phone');

-- daily track if user updated the phone within past 32 days. Put in a table for easy use:
CREATE OR REPLACE TABLE REST.TEST.Phone_Change_Tracker_03_02_2021 AS (
  
with log_phone_change as(
  select item_id as user_id, DATE_TRUNC('day', created_at)::DATE AS created_at
  from analytics.looker.versions_pivot
  where item_type= 'User' and object ='phone' and created_at >= '2020-06-20' 
),

day_series as(
  select dte from (
  select dateadd(day, '-' || seq4(), current_date()) as dte 
  from table (generator(rowcount => 365))
  where dte >= '2020-08-01')
)
  
select
user_id,
dte,
sum(case when datediff(day, created_at, dte) >= 0 and datediff(day, created_at, dte) <= 31 then 1 else 0 end) as num_of_phone_change_in_past_32_days
from(
  select 
  day_series.dte,
  log_phone_change.user_id,
  log_phone_change.created_at
  from day_series
  cross join log_phone_change
) panel_data
group by 1,2
order by 1,2);
