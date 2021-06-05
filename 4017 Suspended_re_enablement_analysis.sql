WITH account_deactivations AS (-- get set of deactivations (account goes from active to cancelled or suspended) from versions:
    with deactivations as (
        SELECT
          versions_id, created_at, item_type, item_id, event, object, object_changes, change_from, change_to, whodunnit
          , lag(created_at, 1) OVER (PARTITION BY item_id ORDER BY versions_id ASC) AS next_deactivation_date
          , count(versions_id) over (partition by item_id) as lifetime_deactivations
          , case when change_to in ('cancelled', 'cancelled_no_refund') then 'AccountCancellationReason'
                 else 'AccountSuspensionReason' end as type_key
        FROM
          "ANALYTICS"."LOOKER"."VERSIONS_PIVOT"
        WHERE
          item_type = 'User' and event = 'update' and object = 'status'
        AND change_from = 'active'
        AND change_to IN ('cancelled', 'cancelled_no_refund', 'suspended')

      ),

-- here we're joining our deactivations the admin_users table to get
-- the email of the agent who suspended or closed the account AND
-- USERS: here we're checking to see if the account is currently active, AND looking for cancelled_by
-- in the event that we're missing it bc it was a future scheduled closure e.g.
-- (we're also handling the issue of duplication where one closure sometimes generates > 1 entry
-- and restricting to a single closure for a given reason
d as (
select
    date_trunc(day, ad.created_at)::date as deactivation_date
    ,ad.item_id as user_id
    ,ad.change_to
    ,ad.type_key
    ,ad.whodunnit
    ,au.email as agent_email
    ,ad.lifetime_deactivations
    ,u.cancelled_by
    ,case when u.status = 'active' then 'true' else 'false' end as was_reenabled
    ,u.updated_at as last_update
from
    deactivations ad left join mysql_db.chime_prod.users u on ad.item_id = u.id
left join
    mysql_db.chime_prod.admin_users au on ad.whodunnit::varchar = au.id::varchar
group by 1,2,3,4,5,6,7,8,9,10
),

-- getting the reasons for a given closure, date_trunc + group by to dedupe the multiple entries <> one closure
reasons AS (
          select date_trunc(day, v.created_at)::date as deactivation_date,
          v.whodunnit,
          v.change_to as reason_id
          ,regexp_replace(split_part(split_part(object_changes, 'user_id:',2), 'account_deactivation_reason_id',1), '[^0-9]', '') as user_id
          ,adr.label
          ,adr.reason
          ,adr.type
          from "ANALYTICS"."LOOKER"."VERSIONS_PIVOT" v 
          left join mysql_db.chime_prod.account_deactivation_reasons AS adr
              on v.change_to = adr.id
          where
              v.item_type = 'UserAccountDeactivation'
          and v.object = 'account_deactivation_reason_id'

          group by 1,2,3,4,5,6,7
      ),

ds as (
  select d.*, r.reason, r.label, r.type as reason_type
  from d 
  left join reasons r 
  on d.user_id::varchar = r.user_id::varchar and d.whodunnit = r.whodunnit and d.deactivation_date = r.deactivation_date and d.type_key = r.type
),
-- identifying scheduled closures and correlating w/ active_admin_comments to figure out which agent
-- did the schedule (sio tracks event does NOT log whodunnit)
future as (select s.id, s.timestamp, s.user_id, s.event,
      s.properties:close_date::varchar as close_date_str,
      s.properties:closed_per::varchar as close_reason,
      properties:current_checking::number as balance,
      s.properties:scheduled_for::varchar as scheduled_for,
      ac.created_at,
      ac.author_id,
      datediff(second, s.timestamp, ac.created_at) as diff,
      ac.body,
      au.email as agent_email,
      s.properties,
      split_part(split_part(body, 'ACCOUNT WILL BE CLOSED ON',2), '.',1) as date_string,
      split_part(date_string, '/',1)::number as month,
      split_part(date_string, '/',2)::number as day,
      row_number() over (partition by s.id order by abs(diff) asc) rank

      from "ANALYTICS"."LOOKER"."SIO_TRACKS" s left join mysql_db.chime_prod.active_admin_comments ac on s.user_id::varchar = ac.resource_id::varchar
      left join mysql_db.chime_prod.admin_users au on ac.author_id = au.id
      where s.event = 'Account Scheduled Closure'
        and s.timestamp >= '2019-01-01'
        and (ac.body like '%ACCOUNT WILL BE CLOSED ON%' or ac.created_at is null)
        and (length(ac.body) between 100 and 500 or ac.created_at is null)
        and (datediff(minute, s.timestamp, ac.created_at) between -1 and 1 or ac.created_at is null)
        and scheduled_for = 'future'
      ),

data_set as (
select ds.deactivation_date
,case when future.scheduled_for is not null then future.scheduled_for else 'now' end as schedule_type
,case when future.timestamp is not null then date_trunc(day, future.timestamp)::date else ds.deactivation_date end as decision_date
,ds.user_id
,ds.last_update
,ds.change_to
,ds.type_key
,ds.LABEL  
,case when length(ds.whodunnit) > 0 then ds.whodunnit else future.author_id::varchar end as deactivator
,case when ds.agent_email is not null then ds.agent_email
      when future.agent_email is not null then future.agent_email
      else null end as deactivator_email
,left(right(deactivator_email, length(deactivator_email) - CHARINDEX('@', deactivator_email)), CHARINDEX('.', RIGHT(deactivator_email, LENgth(deactivator_email) - CHARINDEX('@', deactivator_email))) - 1) AS domain
, case when domain is null then deactivator else domain end as actioning_body
,ds.lifetime_deactivations
,ds.was_reenabled
,case when length(ds.reason) > 0 then ds.reason
      when length(future.close_reason) > 0 then future.close_reason else ds.cancelled_by end as deactivation_reason

from ds left join future
        on ds.user_id = future.user_id
        and date_part(month, ds.deactivation_date)::number =  future.month
        and date_part(day, ds.deactivation_date)::number = future.day

where future.rank = 1 or future.rank is null
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
)

select * from data_set
where deactivation_date >= '2019-01-01'
 )
 
select 
DEACTIVATION_DATE,
LABEL,
account_deactivations."WAS_REENABLED",
account_deactivations."USER_ID",
balance.AVAILABLE_BALANCE as prior_day_available_balance,
case when prior_day_available_balance < 0 then '<0'
     when prior_day_available_balance = 0 or prior_day_available_balance is null then '=0'
     when prior_day_available_balance <= 250 then '(0,250]'
     when prior_day_available_balance <= 500 then '(250,500]'
     when prior_day_available_balance <= 5000 then '(500,5000]'
     else '>5000'
end as balance_bucket,
case when DEACTIVATION_DATE>='2020-11-01' and DEACTIVATION_DATE<='2021-04-30' then 'target_period' else 'not' end as if_target_period 

from account_deactivations
LEFT JOIN (SELECT USER_ID, BALANCE_ON_DATE, MAX(AVAILABLE_BALANCE) AS AVAILABLE_BALANCE
            FROM mysql_db.galileo.GALILEO_DAILY_BALANCES
            WHERE ACCOUNT_TYPE = '6' 
            GROUP BY 1,2) balance
on account_deactivations.USER_ID = balance.USER_ID and TO_DATE(account_deactivations.DEACTIVATION_DATE) = DATEADD('DAY', 1, balance.BALANCE_ON_DATE)  
WHERE deactivation_date >= '2020-01-01' and (LABEL ilike '%Cash on%' or LABEL ilike '%Funds on%' or LABEL ilike '%Tainted Email%' or LABEL ilike '%Tainted Phone Number%' 
                                             or LABEL ilike '%Per Chime - Violation of Terms%')
      and (case when account_deactivations."CHANGE_TO" like 'cancel%' then 'cancelled' else  account_deactivations."CHANGE_TO" end ) = 'suspended' 
 AND (account_deactivations."DEACTIVATION_REASON" is not null and length( account_deactivations."DEACTIVATION_REASON") > 0 )

; 

-- check balance for those suspended on the day before suspension 
-- 83% has lower than 250 balance
WITH account_deactivations AS (-- get set of deactivations (account goes from active to cancelled or suspended) from versions:
    with deactivations as (
        SELECT
          versions_id, created_at, item_type, item_id, event, object, object_changes, change_from, change_to, whodunnit
          , lag(created_at, 1) OVER (PARTITION BY item_id ORDER BY versions_id ASC) AS next_deactivation_date
          , count(versions_id) over (partition by item_id) as lifetime_deactivations
          , case when change_to in ('cancelled', 'cancelled_no_refund') then 'AccountCancellationReason'
                 else 'AccountSuspensionReason' end as type_key
        FROM
          "ANALYTICS"."LOOKER"."VERSIONS_PIVOT"
        WHERE
          item_type = 'User' and event = 'update' and object = 'status'
        AND change_from = 'active'
        AND change_to IN ('cancelled', 'cancelled_no_refund', 'suspended')

      ),

d as (
select
    date_trunc(day, ad.created_at)::date as deactivation_date
    ,ad.item_id as user_id
    ,ad.change_to
    ,ad.type_key
    ,ad.whodunnit
    ,au.email as agent_email
    ,ad.lifetime_deactivations
    ,u.cancelled_by
    ,case when u.status = 'active' then 'true' else 'false' end as was_reenabled
    ,u.updated_at as last_update
from
    deactivations ad left join mysql_db.chime_prod.users u on ad.item_id = u.id
left join
    mysql_db.chime_prod.admin_users au on ad.whodunnit::varchar = au.id::varchar
group by 1,2,3,4,5,6,7,8,9,10
),

reasons AS (
          select date_trunc(day, v.created_at)::date as deactivation_date,
          v.whodunnit,
          v.change_to as reason_id
          ,regexp_replace(split_part(split_part(object_changes, 'user_id:',2), 'account_deactivation_reason_id',1), '[^0-9]', '') as user_id
          ,adr.label
          ,adr.reason
          ,adr.type
          from "ANALYTICS"."LOOKER"."VERSIONS_PIVOT" v 
          left join mysql_db.chime_prod.account_deactivation_reasons AS adr
              on v.change_to = adr.id
          where
              v.item_type = 'UserAccountDeactivation'
          and v.object = 'account_deactivation_reason_id'

          group by 1,2,3,4,5,6,7
      ),

ds as (
  select d.*, r.reason, r.label, r.type as reason_type
  from d 
  left join reasons r 
  on d.user_id::varchar = r.user_id::varchar and d.whodunnit = r.whodunnit and d.deactivation_date = r.deactivation_date and d.type_key = r.type
),

future as (select s.id, s.timestamp, s.user_id, s.event,
      s.properties:close_date::varchar as close_date_str,
      s.properties:closed_per::varchar as close_reason,
      properties:current_checking::number as balance,
      s.properties:scheduled_for::varchar as scheduled_for,
      ac.created_at,
      ac.author_id,
      datediff(second, s.timestamp, ac.created_at) as diff,
      ac.body,
      au.email as agent_email,
      s.properties,
      split_part(split_part(body, 'ACCOUNT WILL BE CLOSED ON',2), '.',1) as date_string,
      split_part(date_string, '/',1)::number as month,
      split_part(date_string, '/',2)::number as day,
      row_number() over (partition by s.id order by abs(diff) asc) rank

      from "ANALYTICS"."LOOKER"."SIO_TRACKS" s left join mysql_db.chime_prod.active_admin_comments ac on s.user_id::varchar = ac.resource_id::varchar
      left join mysql_db.chime_prod.admin_users au on ac.author_id = au.id
      where s.event = 'Account Scheduled Closure'
        and s.timestamp >= '2019-01-01'
        and (ac.body like '%ACCOUNT WILL BE CLOSED ON%' or ac.created_at is null)
        and (length(ac.body) between 100 and 500 or ac.created_at is null)
        and (datediff(minute, s.timestamp, ac.created_at) between -1 and 1 or ac.created_at is null)
        and scheduled_for = 'future'
      ),

data_set as (
select ds.deactivation_date
,case when future.scheduled_for is not null then future.scheduled_for else 'now' end as schedule_type
,case when future.timestamp is not null then date_trunc(day, future.timestamp)::date else ds.deactivation_date end as decision_date
,ds.user_id
,ds.last_update
,ds.change_to
,ds.type_key
,ds.LABEL  
,case when length(ds.whodunnit) > 0 then ds.whodunnit else future.author_id::varchar end as deactivator
,case when ds.agent_email is not null then ds.agent_email
      when future.agent_email is not null then future.agent_email
      else null end as deactivator_email
,left(right(deactivator_email, length(deactivator_email) - CHARINDEX('@', deactivator_email)), CHARINDEX('.', RIGHT(deactivator_email, LENgth(deactivator_email) - CHARINDEX('@', deactivator_email))) - 1) AS domain
, case when domain is null then deactivator else domain end as actioning_body
,ds.lifetime_deactivations
,ds.was_reenabled
,case when length(ds.reason) > 0 then ds.reason
      when length(future.close_reason) > 0 then future.close_reason else ds.cancelled_by end as deactivation_reason

from ds left join future
        on ds.user_id = future.user_id
        and date_part(month, ds.deactivation_date)::number =  future.month
        and date_part(day, ds.deactivation_date)::number = future.day

where future.rank = 1 or future.rank is null
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
)

select * from data_set
where deactivation_date >= '2019-01-01'
 )
 
select 
case when balance.AVAILABLE_BALANCE < 0 then '<0'
     when balance.AVAILABLE_BALANCE = 0 or balance.AVAILABLE_BALANCE is null then '=0'
     when balance.AVAILABLE_BALANCE <= 250 then '(0,250]'
     when balance.AVAILABLE_BALANCE <= 500 then '(250,500]'
     when balance.AVAILABLE_BALANCE <= 5000 then '(500,5000]'
     else '>5000'
end as balance_bucket,
count(account_deactivations."USER_ID") ,
avg(balance.AVAILABLE_BALANCE) 

from account_deactivations
LEFT JOIN (SELECT USER_ID, BALANCE_ON_DATE, MAX(AVAILABLE_BALANCE) AS AVAILABLE_BALANCE
            FROM mysql_db.galileo.GALILEO_DAILY_BALANCES
            WHERE ACCOUNT_TYPE = '6' 
            GROUP BY 1,2) balance
on account_deactivations.USER_ID = balance.USER_ID and TO_DATE(account_deactivations.DEACTIVATION_DATE) = DATEADD('DAY', 1, balance.BALANCE_ON_DATE)  
WHERE DEACTIVATION_DATE>='2020-11-01' and DEACTIVATION_DATE<='2021-04-30' and (LABEL ilike '%Cash on%' or LABEL ilike '%Funds on%' or LABEL ilike '%Tainted Email%' or LABEL ilike '%Tainted Phone Number%' 
                                             or LABEL ilike '%Per Chime - Violation of Terms%')
                                       and (case when account_deactivations."CHANGE_TO" like 'cancel%' then 'cancelled' else  account_deactivations."CHANGE_TO" end ) = 'suspended' 
 AND (account_deactivations."DEACTIVATION_REASON" is not null and length( account_deactivations."DEACTIVATION_REASON") > 0 )
group by 1
; 