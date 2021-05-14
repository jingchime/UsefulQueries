with ato_castle as (

              select
               user_id
               ,transaction_timestamp
               ,unique_transaction_id
               , authorization_code
                , date_trunc(month,transaction_timestamp)::date as txn_month
                , date_trunc(week,transaction_timestamp)::date as txn_week
                -- these are the standard buckets
                , case when hr_diff <=24 then '<=24hr' end as hr_24
                , case when hr_diff <=48 then '<=48hr' end as hr_48
                , case when hr_diff <=72 then '<=72hr' end as hr_72
                , case when disp_bool = 1 then 1 else 0 end as ato_occurred
                , transaction_type
                , transaction_code
                , merchant_name
                , txn_amount
                , sio_ts as ato_ts
                , event
                , hr_diff
                , dispute_type
                , platform
                , user_agent
              from (
                    select
                      gpt.user_id
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
                              when gpt.transaction_code in ('ADM', 'ADPF', 'ADTS', 'ADTU','ADpb') then 'PF_outgoing'
                              when gpt.transaction_code in ('VSW','MPW','MPM', 'MPR','PLW', 'PLR','PRW', 'SDW') then 'ATM Withdrawals'
                              when gpt.transaction_code = 'ADZ' then 'Bill Pay'
                              else 'Other' end as transaction_type
                      , abs(gpt.transaction_amount) as txn_amount
                      , mem.sio_ts
                      , mem.event
                      , gpt.merchant_name
                      , datediff(hour,mem.sio_ts, gpt.transaction_timestamp) as hr_diff
                      , (case when disp.user_id is null then 0 else 1 end) as disp_bool
                      , dispute_type
                      , mem.platform
                      , mem.user_agent
                      , row_number() over(partition by gpt.user_id, gpt.id order by gpt.transaction_timestamp, hr_diff asc) rnum
                    from mysql_db.galileo.galileo_posted_transactions as gpt
                    inner join (
                                select 
                                  user_id as ber
                                  , timestamp as sio_ts
                                  , event
                                  , properties:platform as platform
                                  , context:user_agent as user_agent
                                from "ANALYTICS"."LOOKER"."SIO_TRACKS"
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
                                from "ANALYTICS"."LOOKER"."USER_DISPUTE_CLAIMS" as udc
                                JOIN "ANALYTICS"."LOOKER"."USER_DISPUTE_CLAIM_TRANSACTIONS" as udct ON udc.id = udct.user_dispute_claim_id 
                                LEFT JOIN "ANALYTICS"."LOOKER"."USER_DISPUTE_CLAIM_UPDATES" as udcu ON udc.id = udcu.user_dispute_claim_id
                                JOIN mysql_db.galileo.galileo_posted_transactions as gpt ON gpt.user_id = udc.user_id and 
                                                                       gpt.authorization_code = ROUND(CASE WHEN LEFT(udct.transaction_id,1) in (4,5,6,7,8,9) 
                                                                                                           THEN substr(udct.transaction_id, 2) 
                                                                                                           ELSE udct.transaction_id 
                                                                                                           END,0)::varchar
                                ) as disp on disp.auth_code=gpt.authorization_code
                                  and disp.user_id=gpt.user_id
                                  and disp.created_at > gpt.transaction_timestamp
                    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
                    )
                    where rnum = 1 and transaction_type ='ACH Transfer' and hr_24 = '<=24hr'
                    --and  user_id in ( '5997681', '1281994')
                   -- group by 1,2,3,4--,5
),

ato_device_score as (
              select
               user_id
               ,transaction_timestamp
               ,unique_transaction_id
               , authorization_code
                , date_trunc(month,transaction_timestamp)::date as txn_month
                , date_trunc(week,transaction_timestamp)::date as txn_week
                -- these are the standard buckets
                , case when hr_diff <=24 then '<=24hr' end as hr_24
                , case when hr_diff <=48 then '<=48hr' end as hr_48
                , case when hr_diff <=72 then '<=72hr' end as hr_72
                , case when disp_bool = 1 then 1 else 0 end as ato_occurred
                , transaction_type
                , transaction_code
                , merchant_name
                , txn_amount
                , sio_ts as ato_ts
                , event
                , hr_diff
                , dispute_type
                , platform
                , user_agent
              from (
                    select
                      gpt.user_id
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
                              when gpt.transaction_code in ('ADM', 'ADPF', 'ADTS', 'ADTU','ADpb') then 'PF_outgoing'
                              when gpt.transaction_code in ('VSW','MPW','MPM', 'MPR','PLW', 'PLR','PRW', 'SDW') then 'ATM Withdrawals'
                              when gpt.transaction_code = 'ADZ' then 'Bill Pay'
                              else 'Other' end as transaction_type
                      , abs(gpt.transaction_amount) as txn_amount
                      , mem.sio_ts
                      , mem.event
                      , gpt.merchant_name
                      , datediff(hour,mem.sio_ts, gpt.transaction_timestamp) as hr_diff
                      , (case when disp.user_id is null then 0 else 1 end) as disp_bool
                      , dispute_type
                      , das.score
                      , mem.platform
                      , mem.user_agent
                      , row_number() over(partition by gpt.user_id, gpt.id order by gpt.transaction_timestamp, hr_diff asc) rnum
                    from mysql_db.galileo.galileo_posted_transactions as gpt
                    inner join (
                                select 
                                  user_id as ber
                                  , timestamp as sio_ts
                                  , timestamp::date as sio_date
                                  , event
                                  , properties:platform as platform
                                  , context:user_agent as user_agent
                                from "ANALYTICS"."LOOKER"."SIO_TRACKS"
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
                                from "ANALYTICS"."LOOKER"."USER_DISPUTE_CLAIMS" as udc
                                JOIN "ANALYTICS"."LOOKER"."USER_DISPUTE_CLAIM_TRANSACTIONS" as udct ON udc.id = udct.user_dispute_claim_id 
                                LEFT JOIN "ANALYTICS"."LOOKER"."USER_DISPUTE_CLAIM_UPDATES" as udcu ON udc.id = udcu.user_dispute_claim_id
                                JOIN mysql_db.galileo.galileo_posted_transactions as gpt ON gpt.user_id = udc.user_id and 
                                                                       gpt.authorization_code = ROUND(CASE WHEN LEFT(udct.transaction_id,1) in (4,5,6,7,8,9) 
                                                                                                           THEN substr(udct.transaction_id, 2) 
                                                                                                           ELSE udct.transaction_id 
                                                                                                           END,0)::varchar
                                ) as disp on disp.auth_code=gpt.authorization_code
                                  and disp.user_id=gpt.user_id
                                  and disp.created_at > gpt.transaction_timestamp
                    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
                    )
                    where rnum = 1 and transaction_type ='ACH Transfer' and hr_24 = '<=24hr'

),

--new table looks good, now will try the union method
all_ato as (
select
user_id
,transaction_timestamp
,unique_transaction_id
, authorization_code
,txn_month
,txn_week
-- these are the standard buckets
, hr_24
, ato_occurred
, transaction_type
, transaction_code
, merchant_name
, txn_amount
, ato_ts
, event
, hr_diff
, dispute_type
, platform
, user_agent
, 'castle' as source_  
from ato_castle ac

UNION

select 
user_id
,transaction_timestamp
,unique_transaction_id
, authorization_code
,txn_month
,txn_week
-- these are the standard buckets
, hr_24
, ato_occurred
, transaction_type
, transaction_code
, merchant_name
, txn_amount
, ato_ts
, event
, hr_diff
, dispute_type
, platform
, user_agent
, 'device score' as source_  
from ato_device_score ads
order by user_id, transaction_timestamp, source_
),

ato_clean as (
select
*,
row_number() over(partition by user_id, unique_transaction_id order by transaction_timestamp asc) rnum
from all_ato
),

ato_final as (
select
*
from ato_clean
where rnum = 1
),

ato_ach as (
select
af.*,
ach.type,
ach2.created_at as ach_link_ts,
ach2.created_at::date as ach_link_date,
ach2.bank_name,
ach2.routing_number,
ach2.external_id,
ach2.subtype,
ach2.is_deleted,
ach2.proof_of_ownership,
ach2.bank_type,
ach2.plaid_permissions,
ach2.type,
ach2.address_matched,
ach2.name_matched,
ach2.identity_check_status,
ach2.account_name
from ato_final af
left join MYSQL_DB.CHIME_PROD.ACH_TRANSFERS ach on af.authorization_code = ach.payment_id and af.txn_amount = ach.amount
left join MYSQL_DB.CHIME_PROD.ACH_ACCOUNTS ach2 on ach.ach_account_id = ach2.id and ach2.user_id = af.user_id
where 1=1
and af.transaction_type = 'ACH Transfer'
),

enhanced_detail as (
select *,
case when ach_link_ts > ato_ts and ach_link_ts < transaction_timestamp and name_matched = 0 and ato_occurred = 1 then 1 else 0 end as no_name_tp_,
case when ach_link_ts > ato_ts and ach_link_ts < transaction_timestamp and name_matched = 0 and ato_occurred = 0 then 1 else 0 end as no_name_fp_,
case when ach_link_ts > ato_ts and ach_link_ts < transaction_timestamp and name_matched = 0 and address_matched = 0 and ato_occurred = 1 then 1 else 0 end as no_name_add_tp_,
case when ach_link_ts > ato_ts and ach_link_ts < transaction_timestamp and name_matched = 0 and address_matched = 0 and ato_occurred = 0 then 1 else 0 end as no_name_add_fp_

from ato_ach
where txn_month >= '2021-01-01'
)


select
bank_name,
txn_month,

count(distinct(case when no_name_add_tp_ = 1 then user_id end)) as tp_ec_mm_count,
count(distinct(case when no_name_add_fp_ = 1 then user_id end)) as fp_ec_mm_count,
tp_ec_mm_count / nullif((tp_ec_mm_count + fp_ec_mm_count),0) as precision_ec_name_mm,
sum(case when no_name_add_tp_ = 1 then txn_amount else 0 end) as tp_ec_name$,
sum(case when no_name_add_fp_ = 1 then txn_amount else 0 end) as fp_ec_name$,
tp_ec_name$ / nullif((tp_ec_name$ + fp_ec_name$),0) as precision_ec_name_$
from enhanced_detail
group by 1,2
having  tp_ec_mm_count >= 1
order by 1,2;