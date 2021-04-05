with ato_castle as (

            select
             member_id
             ,transaction_timestamp
             ,unique_transaction_id
             , authorization_code
              , date_trunc(month,transaction_timestamp)::date as txn_month
              , date_trunc(week,transaction_timestamp)::date as txn_week
              , date_trunc(day,transaction_timestamp)::date as txn_date
              -- these are the standard buckets
              , case when hr_diff <=24 then '<=24hr' end as hr_24
              , case when hr_diff <=48 then '<=48hr' end as hr_48
              , case when hr_diff <=72 then '<=72hr' end as hr_72
              , case when disp_bool = 1 then 1 else 0 end as ato_occurred
              , transaction_type
              , transaction_code
              , merchant_name
              , transaction_amount
              , sio_ts as ato_ts
              , event
              , hr_diff
              , dispute_type
              , sum(case when provisional_credit_issued = 'TRUE' then transaction_amount else 0 end) as pvc_amount
              , platform
              , user_agent
            from (
                  select
                    gpt.user_id as member_id
                    , gpt.transaction_timestamp
                    , gpt.id as unique_transaction_id
                    , gpt.authorization_code
                    , gpt.transaction_code
                    ,  case  when (gpt.transaction_code in ('ISA', 'ISC', 'ISJ', 'ISL', 'ISM', 'ISR', 'ISZ', 'VSA', 'VSC', 'VSJ', 'VSL', 'VSM', 'VSR', 'VSZ',
                            'SDA', 'SDC', 'SDL', 'SDM', 'SDR', 'SDV', 'SDZ', 'PLM', 'PLA', 'PRA') and gpt.unique_program_id in (600, 278,1014))  then 'Credit Purchase'
                            when gpt.transaction_code in ('ISA', 'ISC', 'ISJ', 'ISL', 'ISM', 'ISR', 'ISZ', 'VSA', 'VSC', 'VSJ', 'VSL', 'VSM', 'VSR', 'VSZ',
                            'SDA', 'SDC', 'SDL', 'SDM', 'SDR', 'SDV', 'SDZ', 'PLM', 'PLA', 'PRA') then 'Debit Purchase'
                            WHEN gpt.TRANSACTION_CODE = 'ADS' THEN 'ACH Transfer'  --ACH PUSH
                            WHEN gpt.TRANSACTION_CODE in ('ADbc', 'ADcn') then 'ACH debit'
                            when gpt.transaction_code in ('ADM', 'ADTS', 'ADTU','ADpb') then 'PF_outgoing'
                            when gpt.transaction_code in ('ADPF') then 'Pay_anyone'
                            when gpt.transaction_code in ('VSW','MPW','MPM', 'MPR','PLW', 'PLR','PRW', 'SDW') then 'ATM Withdrawals'
                            when gpt.transaction_code = 'ADZ' then 'Bill Pay'
                            else 'Other' end as transaction_type
                    , abs(gpt.transaction_amount) as transaction_amount
                    , mem.sio_ts
                    , mem.event
                    , gpt.merchant_name
                    , gpt.merchant_category_code
                    , datediff(hour,mem.sio_ts, gpt.transaction_timestamp) as hr_diff
                    , (case when disp.user_id is null then 0 else 1 end) as disp_bool
                    , dispute_type
                    , provisional_credit_issued
                    , mem.platform
                    , mem.user_agent
                    , row_number() over(partition by gpt.user_id, gpt.id order by gpt.transaction_timestamp asc) rnum
                  from mysql_db.galileo.galileo_posted_transactions as gpt
                  inner join (
                              select
                                user_id as ber
                                , timestamp as sio_ts
                                , event
                                , properties:platform as platform
                                , context:user_agent as user_agent
                              from LOOKER.SIO_TRACKS
                              where event = 'consumer.login_security.castle.challenge.succeeded'
                              ) as mem on mem.ber=gpt.user_id
                                and datediff(hour,mem.sio_ts, gpt.transaction_timestamp) between 0 and 72
                                and mem.sio_ts<gpt.transaction_timestamp
                  left join (
                              select
                                gpt.authorization_code as auth_code
                                , udc.user_id
                                , udc.created_at
                                , udcu.dispute_type
                                , udcu.provisional_credit_issued
                              from "ANALYTICS"."LOOKER"."USER_DISPUTE_CLAIMS" as udc
                              JOIN "ANALYTICS"."LOOKER"."USER_DISPUTE_CLAIM_TRANSACTIONS" as udct ON udc.id = udct.user_dispute_claim_id
                              LEFT JOIN "ANALYTICS"."LOOKER"."USER_DISPUTE_CLAIM_UPDATES" as udcu ON udc.id = udcu.user_dispute_claim_id
                              JOIN mysql_db.galileo.galileo_posted_transactions as gpt ON gpt.user_id = udc.user_id and
                                                                     gpt.authorization_code = ROUND(CASE WHEN LEFT(udct.transaction_id,1) in (4,5,6,7,9)
                                                                                                         THEN substr(udct.transaction_id, 2)
                                                                                                         ELSE udct.transaction_id
                                                                                                         END,0)::varchar
                              ) as disp on disp.auth_code=gpt.authorization_code
                                and disp.user_id=gpt.user_id
                                and disp.created_at > gpt.transaction_timestamp
                  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
                  )
                  where rnum = 1 and transaction_type in ('PF_outgoing', 'Pay_anyone', 'Debit Purchase', 'ACH Transfer', 'Credit Purchase') --and hr_24 = '<=24hr'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,21,22
),

ato_device_score as (
            select
             member_id
             ,transaction_timestamp
             ,unique_transaction_id
             , authorization_code
              , date_trunc(month,transaction_timestamp)::date as txn_month
              , date_trunc(week,transaction_timestamp)::date as txn_week
              , date_trunc(day,transaction_timestamp)::date as txn_date
              -- these are the standard buckets
              , case when hr_diff <=24 then '<=24hr' end as hr_24
              , case when hr_diff <=48 then '<=48hr' end as hr_48
              , case when hr_diff <=72 then '<=72hr' end as hr_72
              , case when disp_bool = 1 then 1 else 0 end as ato_occurred
              , transaction_type
              , transaction_code
              , merchant_name
              , transaction_amount
              , sio_ts as ato_ts
              , event
              , hr_diff
              , dispute_type
              , sum(case when provisional_credit_issued = 'TRUE' then transaction_amount else 0 end) as pvc_amount
              , platform
              , user_agent
            from (
                  select
                    gpt.user_id as member_id
                    , gpt.transaction_timestamp
                    , gpt.id as unique_transaction_id
                    , gpt.authorization_code
                    , gpt.transaction_code
                    ,  case  when (gpt.transaction_code in ('ISA', 'ISC', 'ISJ', 'ISL', 'ISM', 'ISR', 'ISZ', 'VSA', 'VSC', 'VSJ', 'VSL', 'VSM', 'VSR', 'VSZ',
                            'SDA', 'SDC', 'SDL', 'SDM', 'SDR', 'SDV', 'SDZ', 'PLM', 'PLA', 'PRA') and gpt.unique_program_id in (600, 278,1014))  then 'Credit Purchase'
                            when gpt.transaction_code in ('ISA', 'ISC', 'ISJ', 'ISL', 'ISM', 'ISR', 'ISZ', 'VSA', 'VSC', 'VSJ', 'VSL', 'VSM', 'VSR', 'VSZ',
                            'SDA', 'SDC', 'SDL', 'SDM', 'SDR', 'SDV', 'SDZ', 'PLM', 'PLA', 'PRA') then 'Debit Purchase'
                            WHEN gpt.TRANSACTION_CODE = 'ADS' THEN 'ACH Transfer'  --ACH PUSH
                            WHEN gpt.TRANSACTION_CODE in ('ADbc', 'ADcn') then 'ACH debit'
                            when gpt.transaction_code in ('ADM', 'ADTS', 'ADTU','ADpb') then 'PF_outgoing'
                            when gpt.transaction_code in ('ADPF') then 'Pay_anyone'
                            when gpt.transaction_code in ('VSW','MPW','MPM', 'MPR','PLW', 'PLR','PRW', 'SDW') then 'ATM Withdrawals'
                            when gpt.transaction_code = 'ADZ' then 'Bill Pay'
                            else 'Other' end as transaction_type
                    , abs(gpt.transaction_amount) as transaction_amount
                    , mem.sio_ts
                    , mem.event
                    , gpt.merchant_name
                    , gpt.merchant_category_code
                    , datediff(hour,mem.sio_ts, gpt.transaction_timestamp) as hr_diff
                    , (case when disp.user_id is null then 0 else 1 end) as disp_bool
                    , dispute_type
                    , provisional_credit_issued
                    , das.score
                    , mem.platform
                    , mem.user_agent
                    , row_number() over(partition by gpt.user_id, gpt.id order by gpt.transaction_timestamp asc) rnum
                  from mysql_db.galileo.galileo_posted_transactions as gpt
                  inner join (
                              select
                                user_id as ber
                                , timestamp as sio_ts
                                , timestamp::date as sio_date
                                , event
                                , properties:platform as platform
                                , context:user_agent as user_agent
                              from LOOKER.SIO_TRACKS
                              where event = 'Castle V3 MFA Verdict'
                                    and properties:verdict_v3 = 'allow'
                              ) as mem on mem.ber=gpt.user_id
                                and datediff(hour,mem.sio_ts, gpt.transaction_timestamp) between 0 and 72
                                and mem.sio_ts<gpt.transaction_timestamp
                  inner join (
                             select
                             user_id,
                             login_date::date as login_date,
                             score
                             from "ANALYTICS"."LOOKER"."DEVICE_ACCESS_SCORES"
                             where 1=1
                             and score <= 0.5

                              ) as  das on das.user_id = gpt.user_id
                                        and mem.sio_date = das.login_date
                  left join (
                              select
                                gpt.authorization_code as auth_code
                                , udc.user_id
                                , udc.created_at
                                , udcu.dispute_type
                                , udcu.provisional_credit_issued
                             from "ANALYTICS"."LOOKER"."USER_DISPUTE_CLAIMS" as udc
                              JOIN "ANALYTICS"."LOOKER"."USER_DISPUTE_CLAIM_TRANSACTIONS" as udct ON udc.id = udct.user_dispute_claim_id
                              LEFT JOIN "ANALYTICS"."LOOKER"."USER_DISPUTE_CLAIM_UPDATES" as udcu ON udc.id = udcu.user_dispute_claim_id
                              JOIN mysql_db.galileo.galileo_posted_transactions as gpt ON gpt.user_id = udc.user_id and
                                                                     gpt.authorization_code = ROUND(CASE WHEN LEFT(udct.transaction_id,1) in (4,5,6,7,9)
                                                                                                         THEN substr(udct.transaction_id, 2)
                                                                                                         ELSE udct.transaction_id
                                                                                                         END,0)::varchar
                              ) as disp on disp.auth_code=gpt.authorization_code
                                and disp.user_id=gpt.user_id
                                and disp.created_at > gpt.transaction_timestamp
                  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
                  )
                  where rnum = 1 and transaction_type in ('PF_outgoing', 'Pay_anyone', 'Debit Purchase', 'ACH Transfer', 'Credit Purchase') --and hr_24 = '<=24hr'
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,21,22
    ),

--new table looks good, now will try the union method
all_ato as (
select
member_id
,transaction_timestamp
,unique_transaction_id
, authorization_code
,txn_month
,txn_week
,txn_date
-- these are the standard buckets
, hr_24
, hr_48
, hr_72
, ato_occurred
, transaction_type
, transaction_code
, merchant_name
, transaction_amount
, ato_ts
, event
, hr_diff
, dispute_type
, pvc_amount
, platform
, user_agent
, 'castle' as source_
from ato_castle ac
where txn_week>='2020-08-03'

UNION

select
member_id
,transaction_timestamp
,unique_transaction_id
, authorization_code
,txn_month
,txn_week
,txn_date
-- these are the standard buckets
, hr_24
, hr_48
, hr_72
, ato_occurred
, transaction_type
, transaction_code
, merchant_name
, transaction_amount
, ato_ts
, event
, hr_diff
, dispute_type
, pvc_amount
, platform
, user_agent
, 'device score' as source_
from ato_device_score ads
where txn_week>='2020-08-03'
order by member_id, transaction_timestamp, source_
),

ato_totals as (
select
*,
row_number() over(partition by member_id, unique_transaction_id order by transaction_timestamp asc) rnum
from all_ato)

select
member_id,
source_,
unique_transaction_id,
txn_week,
txn_date,
transaction_type,
sum(transaction_amount) as txn_amount,
sum(pvc_amount) as pvc_amount,
hr_diff as time_from_login,
concat(member_id, transaction_type, txn_date) as rownum
--count(distinct(case when ato_occurred = 1 then member_id else 0 end)) as ato_member_count,
--sum(case when ato_occurred = 1 then txn_amount else 0 end) as ato_dispute_dollars
from ato_totals
where 1=1
and rnum = 1
and ato_occurred = 1
group by 1,2,3,4,5,6,9,10
