-- Sizing those with PMVT 30 days rolling >= 7500
select 
to_date(date_trunc('month', transaction_timestamp)) as tran_month,
count(distinct user_id) as high_pmvt_30_days_users
FROM
(
  select
  tmp1.user_id,
  tmp1.transaction_timestamp,
  sum(tmp2.TRANSACTION_AMOUNT) as total_amount_last30_days
  from
    (SELECT
    user_id,
    transaction_timestamp,
    (DATEADD('day', -30, transaction_timestamp)) as prev_30_timestamp
    FROM "ANALYTICS"."LOOKER"."TRANSACTIONS" t
    WHERE transaction_code = 'PMVT') tmp1

    left join 

    (SELECT
    user_id,
    transaction_timestamp,
    TRANSACTION_AMOUNT
    FROM "ANALYTICS"."LOOKER"."TRANSACTIONS" t
    WHERE transaction_code = 'PMVT') tmp2

    on tmp1.user_id = tmp2.user_id and tmp2.transaction_timestamp between tmp1.prev_30_timestamp and tmp1.transaction_timestamp
    group by 1,2 
    having total_amount_last30_days >= 7500
)
where transaction_timestamp >= '2020-11-01'
group by 1
order by 1;

-- QA the query above
-- 1155030   Sun, 12 Jul 2020 11:24:24 -0700  8155.65
-- 7122117   Tue, 21 Jul 2020 09:01:12 -0700  9590.92

SELECT
user_id,
transaction_timestamp,
(DATEADD('day', -30, transaction_timestamp)) as prev_30_timestamp,
TRANSACTION_AMOUNT
FROM "ANALYTICS"."LOOKER"."TRANSACTIONS" t
WHERE transaction_code = 'PMVT' and user_id = 7122117
order by 2;

-- available balance for the above
-- each user only count the lastest time it passed 7500
-- otherwise a lot of duplicates
select 
case when balance.AVAILABLE_BALANCE < 0 then '<0'
     when balance.AVAILABLE_BALANCE = 0 or balance.AVAILABLE_BALANCE is null then '=0'
     when balance.AVAILABLE_BALANCE <= 250 then '(0,250]'
     when balance.AVAILABLE_BALANCE <= 500 then '(250,500]'
     when balance.AVAILABLE_BALANCE <= 5000 then '(500,5000]'
     else '>5000'
end as balance_bucket,
count(tmp_dedup.user_id) ,
avg(balance.AVAILABLE_BALANCE) 

FROM
(select 
 user_id,
 max(to_date(transaction_timestamp)) as latest_pass_7500_day
 from
    (
      select
      tmp1.user_id,
      tmp1.transaction_timestamp,
      sum(tmp2.TRANSACTION_AMOUNT) as total_amount_last30_days
      from
        (SELECT
        user_id,
        transaction_timestamp,
        (DATEADD('day', -30, transaction_timestamp)) as prev_30_timestamp
        FROM "ANALYTICS"."LOOKER"."TRANSACTIONS" t
        WHERE transaction_code = 'PMVT') tmp1

        left join 

        (SELECT
        user_id,
        transaction_timestamp,
        TRANSACTION_AMOUNT
        FROM "ANALYTICS"."LOOKER"."TRANSACTIONS" t
        WHERE transaction_code = 'PMVT') tmp2

        on tmp1.user_id = tmp2.user_id and tmp2.transaction_timestamp between tmp1.prev_30_timestamp and tmp1.transaction_timestamp
        group by 1,2 
        having total_amount_last30_days >= 7500
    ) tmp
 group by 1) tmp_dedup  -- previously when we did analysis we care about the balance prior to the suspension
                        -- now since these people never been suspended, there will be lots of duplicated entires for each of the member
                        -- hence dedup to keep only the last time it touched the rule
LEFT JOIN (SELECT USER_ID, BALANCE_ON_DATE, MAX(AVAILABLE_BALANCE) AS AVAILABLE_BALANCE
            FROM mysql_db.galileo.GALILEO_DAILY_BALANCES
            WHERE ACCOUNT_TYPE = '6' 
            GROUP BY 1,2) balance
on tmp_dedup.USER_ID = balance.USER_ID and tmp_dedup.latest_pass_7500_day = DATEADD('DAY', 1, balance.BALANCE_ON_DATE)  
WHERE tmp_dedup.latest_pass_7500_day between '2020-11-01' and '2021-04-30'
group by 1
; 

-- Below are all deduping and QC analysis between 4020 and 4022
-- Firstly trying to understand if the newly added suspension code overlap with previously selected 6 codes
-- removing the 6 codes from the current one
select 
part_1.tran_month,
bank.PRIMARY_PROGRAM_ASSIGNED,
count(distinct part_1.user_id)
from
-- part 1: those who have >=7500 PMVT in the past 30 days
(
  select 
  to_date(date_trunc('month', transaction_timestamp)) as tran_month,
  user_id
  from 
  ( select
    tmp1.user_id,
    tmp1.transaction_timestamp,
    sum(tmp2.TRANSACTION_AMOUNT) as total_amount_last30_days
    from
      (SELECT
      user_id,
      transaction_timestamp,
      (DATEADD('day', -30, transaction_timestamp)) as prev_30_timestamp
      FROM "ANALYTICS"."LOOKER"."TRANSACTIONS" t
      WHERE transaction_code = 'PMVT') tmp1

      left join 

      (SELECT
      user_id,
      transaction_timestamp,
      TRANSACTION_AMOUNT
      FROM "ANALYTICS"."LOOKER"."TRANSACTIONS" t
      WHERE transaction_code = 'PMVT') tmp2

      on tmp1.user_id = tmp2.user_id and tmp2.transaction_timestamp between tmp1.prev_30_timestamp and tmp1.transaction_timestamp
      group by 1,2 
      having total_amount_last30_days >= 7500
  ) tmp
  where tran_month between '2020-11-01' and '2021-04-30'
  group by 1,2
) part_1
-- part 2 those who previous suspended by other categories
left join REST.TEST.Tmp_suspended_by_six_codes part_2
on part_1.user_id = part_2.user_id and part_1.TRAN_MONTH = part_2.deactivated_month
LEFT JOIN "ANALYTICS"."LOOKER"."MEMBER_PARTNER_BANK" bank 
on part_1.user_id = bank.user_id
where part_2.user_id is null

group by 1,2
order by 1,2;

-- the query above leveraged a temp table that is built from previous 4020 code
-- which is built upon previous query
CREATE OR REPLACE TABLE REST.TEST.Tmp_suspended_by_six_codes AS (

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
to_date(DATE_TRUNC('MONTH',DEACTIVATION_DATE)) as deactivated_month,
account_deactivations."USER_ID"
from account_deactivations
LEFT JOIN "ANALYTICS"."LOOKER"."MEMBER_PARTNER_BANK" bank on account_deactivations.user_id = bank.user_id
WHERE deactivated_month >= '2020-11-01' and deactivated_month <= '2021-04-30'
     and (LABEL ilike '%Cash on%' or LABEL ilike '%Funds on%' or LABEL ilike '%Tainted Email%' or LABEL ilike '%Tainted Phone Number%' 
                                             or LABEL ilike '%Per Chime - Violation of Terms Suspected%' or LABEL ilike '%Per Chime - Violation of Terms :: Business Use')
      and (case when account_deactivations."CHANGE_TO" like 'cancel%' then 'cancelled' else  account_deactivations."CHANGE_TO" end ) = 'suspended' 
 AND (account_deactivations."DEACTIVATION_REASON" is not null and length( account_deactivations."DEACTIVATION_REASON") > 0 )
 
group by 1,2
  
)


