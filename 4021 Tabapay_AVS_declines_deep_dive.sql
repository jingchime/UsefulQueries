----------------------
----------------------
-- 1_suspension rate
----------------------
----------------------
SELECT 
decline_month,
count(user_id) as total_user,
sum(if_suspended) as total_suspended
from
  (SELECT
  cvs_fail.DECLINE_MONTH,
  cvs_fail.user_id,
  case when suspended.user_id is not null then 1 else 0 end as if_suspended
  FROM
  (select 
  TO_DATE(DATE_TRUNC('MONTH',timestamp)) AS decline_month,
  user_id
  from SEGMENT.MOVE_MONEY_SERVICE.DEBIT_CARD_LINKING_FAILED
  where error = 'tabapay/avs_failure'
  GROUP BY 1,2) cvs_fail
  LEFT JOIN REST.TEST.Account_Deactivations_SL_06_23_2021 suspended
  ON cvs_fail.user_id = suspended.user_id) tmp
group by 1
order by 1
;

----------------------
----------------------
-- 2_user_avs_decline_count
----------------------
----------------------
select
decline_num,
count(user_id) as users
from
(
  select 
  user_id,
  count(*) as decline_num
  from SEGMENT.MOVE_MONEY_SERVICE.DEBIT_CARD_LINKING_FAILED
  where error = 'tabapay/avs_failure'
  group by 1
) tmp
group by 1
order by 1;


--------------------------------------------
--------------------------------------------
--3 Dispute rate of avs declined people by trnx type
--------------------------------------------
--------------------------------------------
select
transaction_type,
trxn_month,
count(distinct user_id) total_users,
count(distinct id) total_transactions,
-sum(TRANSACTION_AMOUNT) as total_transaction_amount,
-sum(disputed_amount) as total_disputed_amount,
total_disputed_amount/total_transaction_amount as dispute_rate
from
(
    select
    t.trxn_month,
    t.transaction_type,
    t.user_id,
    t.id,
    t.TRANSACTION_AMOUNT,
    CASE WHEN dispute.TRANSACTION_ID IS NOT NULL THEN 1 ELSE 0 END AS FLG_DISPUTED,
    case when FLG_DISPUTED = 1 then TRANSACTION_AMOUNT else 0 end as disputed_amount
    from
    (select     
          case when transaction_code like 'AD%' then 4
                when transaction_code like 'FE%' then 5
                when transaction_code like 'IS%' then 6
                when transaction_code like 'PM%' then 7
                when transaction_code like 'SD%' then 8
                when transaction_code like 'VS%' then 9 else 0 end
          AS leading_num, -- Add the leading num
       case when (t.transaction_code in ('ISA', 'ISC', 'ISJ', 'ISL', 'ISM', 'ISR', 'ISZ', 'VSA', 'VSC', 'VSJ', 'VSL', 'VSM', 'VSR', 'VSZ',
                       'SDA', 'SDC', 'SDL', 'SDM', 'SDR', 'SDV', 'SDZ', 'PLM', 'PLA', 'PRA') and unique_program_id in (600, 278))  then 'Credit Purchase'
       when t.transaction_code in ('ISA', 'ISC', 'ISJ', 'ISL', 'ISM', 'ISR', 'ISZ', 'VSA', 'VSC', 'VSJ', 'VSL', 'VSM', 'VSR', 'VSZ',
                       'SDA', 'SDC', 'SDL', 'SDM', 'SDR', 'SDV', 'SDZ', 'PLM', 'PLA', 'PRA') then 'Debit Purchase'      
       WHEN t.TRANSACTION_CODE = 'ADS' THEN 'ACH Transfer'  --ACH PUSH
       WHEN t.TRANSACTION_CODE in ('ADbc', 'ADcn') then 'ACH debit'
       when t.transaction_code in ('ADM', 'ADPF', 'ADTS', 'ADTU','ADpb') then 'PF_outgoing'
       when t.transaction_code in ('VSW','MPW','MPM', 'MPR','PLW', 'PLR','PRW', 'SDW') then 'ATM Withdrawals'    
       else 'Other' end as transaction_type,
       to_date(date_trunc('month',TRANSACTION_TIMESTAMP)) as trxn_month,
       *  
    from ANALYTICS.LOOKER."TRANSACTIONS" t  
    where user_id in (select distinct user_id from SEGMENT.MOVE_MONEY_SERVICE.DEBIT_CARD_LINKING_FAILED where error = 'tabapay/avs_failure')) t

    LEFT JOIN (SELECT DISTINCT A.USER_ID, B.TRANSACTION_ID, A.REASON
             FROM ANALYTICS.LOOKER.USER_DISPUTE_CLAIMS AS A
             LEFT JOIN ANALYTICS.LOOKER.USER_DISPUTE_CLAIM_TRANSACTIONS AS B
             ON A.ID=B.USER_DISPUTE_CLAIM_ID) AS dispute
    ON t.USER_ID=dispute.USER_ID AND TO_NUMBER(CONCAT(t.leading_num, t.AUTHORIZATION_CODE)) = TO_NUMBER(dispute.TRANSACTION_ID)
)
where transaction_type in ('ATM Withdrawals','Debit Purchase','ACH debit','Credit Purchase') and TRXN_MONTH>= '2020-10-01'
group by 1,2
order by 1,2
;

------------------------------------------------------------------
------------------------------------------------------------------
-- 4_1 sigma score distribution for all AVS declined people
------------------------------------------------------------------
------------------------------------------------------------------
with score as (select 
to_date(api.created_at) as created_on,
api.user_id,
api.result as api_result
--,ip.enrollment_ip 
,replace(parse_json(result):referenceId,'"','') as reference_id 
, row_number() over(partition by user_id order by created_at) as rn
, parse_json(result):emailRisk:score as email_risk
, parse_json(result):phoneRisk:score as phone_risk
, parse_json(result):addressRisk:score as address_risk               
/* before  
, case when parse_json(result):fraud:scores[0]:name = 'sigma' then parse_json(result):fraud:scores[0]:score
                       when parse_json(result):fraud:scores[1]:name = 'sigma' then parse_json(result):fraud:scores[1]:score
                       when parse_json(result):fraud:scores[2]:name = 'sigma' then parse_json(result):fraud:scores[2]:score
                       else null
                    end as sigma_generic_score   
*/
--new sigma v1
, case when parse_json(result):fraud:scores[0]:name = 'sigma' and  parse_json(result):fraud:scores[0]:version = '1.0' then parse_json(result):fraud:scores[0]:score
       when parse_json(result):fraud:scores[1]:name = 'sigma' and  parse_json(result):fraud:scores[1]:version = '1.0' then parse_json(result):fraud:scores[1]:score
       when parse_json(result):fraud:scores[2]:name = 'sigma' and  parse_json(result):fraud:scores[2]:version = '1.0' then parse_json(result):fraud:scores[2]:score
       else null
       end as sigma_v1  
--new sigma_v2
, case when parse_json(result):fraud:scores[0]:name = 'sigma' and  parse_json(result):fraud:scores[0]:version = '2.0' then parse_json(result):fraud:scores[0]:score
       when parse_json(result):fraud:scores[1]:name = 'sigma' and  parse_json(result):fraud:scores[1]:version = '2.0' then parse_json(result):fraud:scores[1]:score
       when parse_json(result):fraud:scores[2]:name = 'sigma' and  parse_json(result):fraud:scores[2]:version = '2.0' then parse_json(result):fraud:scores[2]:score
       else null
       end as sigma_v2  
--new synthetic score
, case when parse_json(result):synthetic:scores[0]:name = 'synthetic' then parse_json(result):synthetic:scores[0]:score
       when parse_json(result):synthetic:scores[1]:name = 'synthetic' then parse_json(result):synthetic:scores[1]:score
       when parse_json(result):synthetic:scores[2]:name = 'synthetic' then parse_json(result):synthetic:scores[2]:score
       else null
       end as synthetic_score,  
case when parse_json(result):fraud:scores[0]:name like 'Chime.3%' then parse_json(result):fraud:scores[0]:score
                       when parse_json(result):fraud:scores[1]:name like 'Chime.3%' then parse_json(result):fraud:scores[1]:score
                       when parse_json(result):fraud:scores[2]:name like 'Chime.3%' then parse_json(result):fraud:scores[2]:score
                       when parse_json(result):fraud:scores[3]:name like 'Chime.3%' then parse_json(result):fraud:scores[3]:score
                       when parse_json(result):fraud:scores[4]:name like 'Chime.3%' then parse_json(result):fraud:scores[4]:score
                       else null
                    end as socure_chime_score,
row_number() over(partition by api.user_id order by api.created_at asc) as rnum
from mysql_db.chime_prod.external_api_requests api 
where 1=1
and api.service='socure3' 
and  CHECK_JSON(api.result) is null 
and api.created_at >= '2020-07-01' 
qualify rn=1)

select 
SIGMA_V1_ntile_100,
avg(sigma_v1) as avg_v1,
count(user_id) as num_users
from
(
  select 
  avs_declines.user_id,
  score.sigma_v1,
  ntile(100) over (order by score.sigma_v1 desc) as SIGMA_V1_ntile_100
  from
  (select 
  distinct user_id
  from SEGMENT.MOVE_MONEY_SERVICE.DEBIT_CARD_LINKING_FAILED
  where error = 'tabapay/avs_failure'
  ) avs_declines
  left join score
  on avs_declines.user_id = score.user_id
  --where score.sigma_v1 is not null
)
group by 1
order by 1
;

--------------------------------------------
--------------------------------------------
--4_2 sigma score distribution for everyone
--------------------------------------------
--------------------------------------------
with score as (select 
to_date(api.created_at) as created_on,
api.user_id,
api.result as api_result
--,ip.enrollment_ip 
,replace(parse_json(result):referenceId,'"','') as reference_id 
, row_number() over(partition by user_id order by created_at) as rn
, parse_json(result):emailRisk:score as email_risk
, parse_json(result):phoneRisk:score as phone_risk
, parse_json(result):addressRisk:score as address_risk               
/* before  
, case when parse_json(result):fraud:scores[0]:name = 'sigma' then parse_json(result):fraud:scores[0]:score
                       when parse_json(result):fraud:scores[1]:name = 'sigma' then parse_json(result):fraud:scores[1]:score
                       when parse_json(result):fraud:scores[2]:name = 'sigma' then parse_json(result):fraud:scores[2]:score
                       else null
                    end as sigma_generic_score   
*/
--new sigma v1
, case when parse_json(result):fraud:scores[0]:name = 'sigma' and  parse_json(result):fraud:scores[0]:version = '1.0' then parse_json(result):fraud:scores[0]:score
       when parse_json(result):fraud:scores[1]:name = 'sigma' and  parse_json(result):fraud:scores[1]:version = '1.0' then parse_json(result):fraud:scores[1]:score
       when parse_json(result):fraud:scores[2]:name = 'sigma' and  parse_json(result):fraud:scores[2]:version = '1.0' then parse_json(result):fraud:scores[2]:score
       else null
       end as sigma_v1  
--new sigma_v2
, case when parse_json(result):fraud:scores[0]:name = 'sigma' and  parse_json(result):fraud:scores[0]:version = '2.0' then parse_json(result):fraud:scores[0]:score
       when parse_json(result):fraud:scores[1]:name = 'sigma' and  parse_json(result):fraud:scores[1]:version = '2.0' then parse_json(result):fraud:scores[1]:score
       when parse_json(result):fraud:scores[2]:name = 'sigma' and  parse_json(result):fraud:scores[2]:version = '2.0' then parse_json(result):fraud:scores[2]:score
       else null
       end as sigma_v2  
--new synthetic score
, case when parse_json(result):synthetic:scores[0]:name = 'synthetic' then parse_json(result):synthetic:scores[0]:score
       when parse_json(result):synthetic:scores[1]:name = 'synthetic' then parse_json(result):synthetic:scores[1]:score
       when parse_json(result):synthetic:scores[2]:name = 'synthetic' then parse_json(result):synthetic:scores[2]:score
       else null
       end as synthetic_score,  
case when parse_json(result):fraud:scores[0]:name like 'Chime.3%' then parse_json(result):fraud:scores[0]:score
                       when parse_json(result):fraud:scores[1]:name like 'Chime.3%' then parse_json(result):fraud:scores[1]:score
                       when parse_json(result):fraud:scores[2]:name like 'Chime.3%' then parse_json(result):fraud:scores[2]:score
                       when parse_json(result):fraud:scores[3]:name like 'Chime.3%' then parse_json(result):fraud:scores[3]:score
                       when parse_json(result):fraud:scores[4]:name like 'Chime.3%' then parse_json(result):fraud:scores[4]:score
                       else null
                    end as socure_chime_score,
row_number() over(partition by api.user_id order by api.created_at asc) as rnum
from mysql_db.chime_prod.external_api_requests api 
where 1=1
and api.service='socure3' 
and  CHECK_JSON(api.result) is null 
and api.created_at >= '2020-07-01' 
qualify rn=1)

select 
SIGMA_V1_ntile_100,
avg(sigma_v1) as avg_v1,
count(user_id) as num_users
from
(
  select 
  score.user_id,
  score.sigma_v1,
  ntile(100) over (order by score.sigma_v1 desc) as SIGMA_V1_ntile_100
  from
  score
  where score.created_on >= '2021-05-01' and sigma_v1 is not null
)
group by 1
order by 1
;
--------------------------------------------
--------------------------------------------
--- 5 check if avs declines have plaid name/address match
--------------------------------------------
--------------------------------------------
select
DATE_TRUNC('month', avs_decline_day) as avs_decline_month,
count(distinct user_id) as total_avs_decline_users,
count(distinct user_any_plaid_name_address_matched_at_time_of_avs_decline) as num_any_plaid_name_address_matched_at_time_of_avs_decline,
count(distinct user_any_plaid_name_matched_at_time_of_avs_decline) as num_any_plaid_name_matched_at_time_of_avs_decline,
count(distinct user_any_plaid_address_matched_at_time_of_avs_decline) as num_any_plaid_address_matched_at_time_of_avs_decline
from
(
  SELECT 
  DATE_TRUNC('day', avs_declines.timestamp)::DATE AS avs_decline_day,
  avs_declines.user_id,
  case when to_date(validated.validation_date)<= avs_decline_day then avs_declines.user_id else '' end as user_any_plaid_name_address_matched_at_time_of_avs_decline,
  case when to_date(name_match.validation_date)<= avs_decline_day then avs_declines.user_id else '' end as user_any_plaid_name_matched_at_time_of_avs_decline,
  case when to_date(address_match.validation_date)<= avs_decline_day then avs_declines.user_id else '' end as user_any_plaid_address_matched_at_time_of_avs_decline
  FROM
  (select distinct timestamp, user_id
  from SEGMENT.MOVE_MONEY_SERVICE.DEBIT_CARD_LINKING_FAILED
  where error = 'tabapay/avs_failure'
  ) avs_declines
  left join (select user_id, min(created_at) as validation_date from mysql_db.chime_prod.ach_accounts where transfer_direction = 'both' OR transfer_direction = 'out' or ADDRESS_MATCHED = 1 OR NAME_MATCHED = 1 group by 1) as validated 
  on avs_declines.user_id = validated.user_id
  left join (select user_id, min(created_at) as validation_date from mysql_db.chime_prod.ach_accounts where transfer_direction = 'both'  OR NAME_MATCHED = 1 group by 1) as name_match 
  on avs_declines.user_id = name_match.user_id
  left join (select user_id, min(created_at) as validation_date from mysql_db.chime_prod.ach_accounts where transfer_direction = 'both'  OR ADDRESS_MATCHED = 1 group by 1) as address_match 
  on avs_declines.user_id = address_match.user_id
)
group by 1
order by 1
;

--------------------------------------------
--------------------------------------------
--- 6 check if AVS decline can be an indicator of ACH return
--------------------------------------------
--------------------------------------------
select
TRN_QUARTER,
IF_AVS_DECLINED_BEFORE_TRANSACTION,
count(id) as total_ach_transfers,
sum(IF_RETURNED_ACH)/total_ach_transfers as ach_return_rate
from
(
  select 
  to_date(date_trunc(day,ach_transfer.CREATED_AT)) as trn_day,
  to_date(date_trunc(quarter,ach_transfer.CREATED_AT)) as trn_quarter,
  ach_transfer.id,
  case when ach_transfer."STATUS" = 'failed' then 1 else 0 end if_returned_ach,
  to_date(avs_declines.decline_time) as avs_declined_date,
  case when avs_declined_date<= ach_transfer.CREATED_AT then 1 else 0 end as if_avs_declined_before_transaction
  from "MYSQL_DB"."CHIME_PROD"."ACH_TRANSFERS" ach_transfer
  join mysql_db.chime_prod.ach_accounts aa on ach_transfer.ach_account_id = aa.id
  left join (select user_id, min(timestamp) as decline_time
    from SEGMENT.MOVE_MONEY_SERVICE.DEBIT_CARD_LINKING_FAILED
    where error = 'tabapay/avs_failure'
    group by 1
    ) avs_declines
  on avs_declines.user_id = aa.user_id
  where ach_transfer."TYPE" = 'debit' AND ach_transfer."STATUS" in ('failed','processed') and ach_transfer.CREATED_AT >= '2020-07-01'
) tmp
group by 1,2
order by 1,2
;
--------------------------------------------
--------------------------------------------
---7 distribution of other errors 
--------------------------------------------
--------------------------------------------
select 
TO_DATE(DATE_TRUNC('MONTH',timestamp)) AS decline_month,
error,
count(distinct user_id)
from SEGMENT.MOVE_MONEY_SERVICE.DEBIT_CARD_LINKING_FAILED
GROUP BY 1,2;

--------------------------------------------
--------------------------------------------
--8 dispute rate for avs decline number bucket
--------------------------------------------
--------------------------------------------
select
trxn_quarter,
avs_decline_time,
count(distinct user_id) total_users,
count(distinct id) total_transactions,
-sum(TRANSACTION_AMOUNT) as total_transaction_amount,
-sum(disputed_amount) as total_disputed_amount,
total_disputed_amount/total_transaction_amount as dispute_rate
from
(
    select
    t.trxn_quarter,
    t.transaction_type,
    t.user_id,
    case when t.decline_num > 1 then '2+' else '1' end as avs_decline_time, 
    t.id,
    t.TRANSACTION_AMOUNT,
    CASE WHEN dispute.TRANSACTION_ID IS NOT NULL THEN 1 ELSE 0 END AS FLG_DISPUTED,
    case when FLG_DISPUTED = 1 then TRANSACTION_AMOUNT else 0 end as disputed_amount
    from
    (select     
          case when transaction_code like 'AD%' then 4
                when transaction_code like 'FE%' then 5
                when transaction_code like 'IS%' then 6
                when transaction_code like 'PM%' then 7
                when transaction_code like 'SD%' then 8
                when transaction_code like 'VS%' then 9 else 0 end
          AS leading_num, -- Add the leading num
       case when (t.transaction_code in ('ISA', 'ISC', 'ISJ', 'ISL', 'ISM', 'ISR', 'ISZ', 'VSA', 'VSC', 'VSJ', 'VSL', 'VSM', 'VSR', 'VSZ',
                       'SDA', 'SDC', 'SDL', 'SDM', 'SDR', 'SDV', 'SDZ', 'PLM', 'PLA', 'PRA') and unique_program_id in (600, 278))  then 'Credit Purchase'
       when t.transaction_code in ('ISA', 'ISC', 'ISJ', 'ISL', 'ISM', 'ISR', 'ISZ', 'VSA', 'VSC', 'VSJ', 'VSL', 'VSM', 'VSR', 'VSZ',
                       'SDA', 'SDC', 'SDL', 'SDM', 'SDR', 'SDV', 'SDZ', 'PLM', 'PLA', 'PRA') then 'Debit Purchase'      
       WHEN t.TRANSACTION_CODE = 'ADS' THEN 'ACH Transfer'  --ACH PUSH
       WHEN t.TRANSACTION_CODE in ('ADbc', 'ADcn') then 'ACH debit'
       when t.transaction_code in ('ADM', 'ADPF', 'ADTS', 'ADTU','ADpb') then 'PF_outgoing'
       when t.transaction_code in ('VSW','MPW','MPM', 'MPR','PLW', 'PLR','PRW', 'SDW') then 'ATM Withdrawals'    
       else 'Other' end as transaction_type,
       to_date(date_trunc('quarter',TRANSACTION_TIMESTAMP)) as trxn_quarter,
       t.user_id,
       t.id,
       t.AUTHORIZATION_CODE,
       tmp.decline_num,
       TRANSACTION_AMOUNT    
    from ANALYTICS.LOOKER."TRANSACTIONS" t  
    join (select user_id, count(*) as decline_num from SEGMENT.MOVE_MONEY_SERVICE.DEBIT_CARD_LINKING_FAILED  where error = 'tabapay/avs_failure' group by 1) tmp
     on t.user_id = tmp.user_id) t

    LEFT JOIN (SELECT DISTINCT A.USER_ID, B.TRANSACTION_ID, A.REASON
             FROM ANALYTICS.LOOKER.USER_DISPUTE_CLAIMS AS A
             LEFT JOIN ANALYTICS.LOOKER.USER_DISPUTE_CLAIM_TRANSACTIONS AS B
             ON A.ID=B.USER_DISPUTE_CLAIM_ID) AS dispute
    ON t.USER_ID=dispute.USER_ID AND TO_NUMBER(CONCAT(t.leading_num, t.AUTHORIZATION_CODE)) = TO_NUMBER(dispute.TRANSACTION_ID)
)
where transaction_type = 'Debit Purchase' and trxn_quarter>= '2020-10-01'
group by 1,2
order by 1,2
;

--------------------------------------------
--------------------------------------------
--temp table for account deactivation check--
--------------------------------------------
--------------------------------------------
CREATE OR REPLACE TABLE REST.TEST.Account_Deactivations_SL_06_23_2021 COMMENT = 'All deactivated users' AS (
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
 
SELECT USER_ID,
DEACTIVATION_DATE,
LABEL,
deactivation_reason,
(case WHEN "CHANGE_TO" like 'cancel%' then 'cancelled' else  account_deactivations."CHANGE_TO" end) AS status
from account_deactivations
);
