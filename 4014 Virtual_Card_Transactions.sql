
select gpt.* 
from mysql_db.galileo.galileo_posted_transactions gpt
join mysql_db.galileo.galileo_account_cards gac on gpt.card_id = gac.card_id
join mysql_db.chime_prod.card_attributes ca     on gac.id=ca.GALILEO_ACCOUNT_CARD_ID
where ca.virtual=1 and gpt.user_id = 11395377  --card_id 52976279
order by TRANSACTION_TIMESTAMP;

-- for analysis
select 
if_virtual_card_transactions,
transaction_type,
sum(TRANSACTION_AMOUNT) as total_trx_amount,
sum(case when FLG_DISPUTED = 1 then TRANSACTION_AMOUNT else 0 end) as disputed_amount,
disputed_amount/total_trx_amount as dispute_rate
from
(
  select 
  case when (t.transaction_code in ('ISA', 'ISC', 'ISJ', 'ISL', 'ISM', 'ISR', 'ISZ', 'VSA', 'VSC', 'VSJ', 'VSL', 'VSM', 'VSR', 'VSZ',
                   'SDA', 'SDC', 'SDL', 'SDM', 'SDR', 'SDV', 'SDZ', 'PLM', 'PLA', 'PRA') and unique_program_id in (600, 278))  then 'Credit Purchase'
   when t.transaction_code in ('ISA', 'ISC', 'ISJ', 'ISL', 'ISM', 'ISR', 'ISZ', 'VSA', 'VSC', 'VSJ', 'VSL', 'VSM', 'VSR', 'VSZ',
                   'SDA', 'SDC', 'SDL', 'SDM', 'SDR', 'SDV', 'SDZ', 'PLM', 'PLA', 'PRA') then 'Debit Purchase'      
   WHEN t.TRANSACTION_CODE = 'ADS' THEN 'ACH Transfer'  --ACH PUSH
   WHEN t.TRANSACTION_CODE in ('ADbc', 'ADcn') then 'ACH debit'
   when t.transaction_code in ('ADM', 'ADPF', 'ADTS', 'ADTU','ADpb') then 'PF_outgoing'
   when t.transaction_code in ('VSW','MPW','MPM', 'MPR','PLW', 'PLR','PRW', 'SDW') then 'ATM Withdrawals'    
   else 'Other' end as transaction_type,
   t.TRANSACTION_TIMESTAMP,
   case when virtual.id is not null then 1
   else 0 end as if_virtual_card_transactions,
   CASE WHEN dispute.TRANSACTION_ID IS NOT NULL THEN 1 ELSE 0 END AS FLG_DISPUTED,
   t.*
  from (select    
        case when transaction_code like 'AD%' then 4
              when transaction_code like 'FE%' then 5
              when transaction_code like 'IS%' then 6
              when transaction_code like 'PM%' then 7
              when transaction_code like 'SD%' then 8
              when transaction_code like 'VS%' then 9 else 0 end
        AS leading_num,
        *
        from ANALYTICS.LOOKER."TRANSACTIONS"   
        ) t
  left join (select distinct gpt.id
              from mysql_db.galileo.galileo_posted_transactions gpt
              join mysql_db.galileo.galileo_account_cards gac on gpt.card_id = gac.card_id
              join mysql_db.chime_prod.card_attributes ca     on gac.id=ca.GALILEO_ACCOUNT_CARD_ID
              where ca.virtual=1 and gpt.TRANSACTION_TIMESTAMP>= '2020-10-01' and gpt.TRANSACTION_TIMESTAMP <= '2020-12-31') virtual 
  on t.id = virtual.id
  LEFT JOIN (SELECT DISTINCT A.USER_ID, B.TRANSACTION_ID, A.REASON
             FROM ANALYTICS.LOOKER.USER_DISPUTE_CLAIMS AS A
             LEFT JOIN ANALYTICS.LOOKER.USER_DISPUTE_CLAIM_TRANSACTIONS AS B
             ON A.ID=B.USER_DISPUTE_CLAIM_ID) AS dispute
  ON t.USER_ID=dispute.USER_ID
  AND TO_NUMBER(CONCAT(t.leading_num, t.AUTHORIZATION_CODE)) = TO_NUMBER(dispute.TRANSACTION_ID)
  --where t.TRANSACTION_TIMESTAMP >= '2021-1-1' and t.TRANSACTION_TIMESTAMP <= '2021-2-28'
  where t.TRANSACTION_TIMESTAMP >= '2020-10-01' and t.TRANSACTION_TIMESTAMP <= '2020-12-31' --Q4 2020
) subquery
group by 1,2
order by 3
;

-- add v1 bucket
-- distribution of enrollement month
select 
if_virtual_card_transactions,
transaction_type,
--enrollment_month,
days_since_enrollment,
score_bucket,
sum(TRANSACTION_AMOUNT) as total_trx_amount,
sum(case when FLG_DISPUTED = 1 then TRANSACTION_AMOUNT else 0 end) as disputed_amount
from
( 
   with score as (select 
    to_date(api.created_at) as created_on,
    api.user_id as member_id,
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
  case when (t.transaction_code in ('ISA', 'ISC', 'ISJ', 'ISL', 'ISM', 'ISR', 'ISZ', 'VSA', 'VSC', 'VSJ', 'VSL', 'VSM', 'VSR', 'VSZ',
                   'SDA', 'SDC', 'SDL', 'SDM', 'SDR', 'SDV', 'SDZ', 'PLM', 'PLA', 'PRA') and unique_program_id in (600, 278))  then 'Credit Purchase'
   when t.transaction_code in ('ISA', 'ISC', 'ISJ', 'ISL', 'ISM', 'ISR', 'ISZ', 'VSA', 'VSC', 'VSJ', 'VSL', 'VSM', 'VSR', 'VSZ',
                   'SDA', 'SDC', 'SDL', 'SDM', 'SDR', 'SDV', 'SDZ', 'PLM', 'PLA', 'PRA') then 'Debit Purchase'      
   WHEN t.TRANSACTION_CODE = 'ADS' THEN 'ACH Transfer'  --ACH PUSH
   WHEN t.TRANSACTION_CODE in ('ADbc', 'ADcn') then 'ACH debit'
   when t.transaction_code in ('ADM', 'ADPF', 'ADTS', 'ADTU','ADpb') then 'PF_outgoing'
   when t.transaction_code in ('VSW','MPW','MPM', 'MPR','PLW', 'PLR','PRW', 'SDW') then 'ATM Withdrawals'    
   else 'Other' end as transaction_type,
   case when datediff(day,member.enrollment_date, t.TRANSACTION_TIMESTAMP) <= 10 then '[0,10]'
        when datediff(day,member.enrollment_date, t.TRANSACTION_TIMESTAMP) <= 20 then '[11,20]'
        when datediff(day,member.enrollment_date, t.TRANSACTION_TIMESTAMP) <= 30 then '[21,30]'
        when datediff(day,member.enrollment_date, t.TRANSACTION_TIMESTAMP) <= 40 then '[31,40]'
        when datediff(day,member.enrollment_date, t.TRANSACTION_TIMESTAMP) <= 60 then '[41,60]'
   else '(60+'
  end as days_since_enrollment, 
   --DATE_TRUNC(month, member.enrollment_date)::DATE as enrollment_month,                                                   
   case when score.sigma_v1 is null then 'empty'
        when score.sigma_v1 >= 0.9 then '[0.9-1]'
        when score.sigma_v1 >= 0.7 then '[0.7-0.9)'
        when score.sigma_v1 >= 0.3 then '[0.3-0.7)' 
   else '[0-0.3)' end as score_bucket,
   t.TRANSACTION_TIMESTAMP,
   case when virtual.id is not null then 1
   else 0 end as if_virtual_card_transactions,
   CASE WHEN dispute.TRANSACTION_ID IS NOT NULL THEN 1 ELSE 0 END AS FLG_DISPUTED,
   t.*
  from (select    
        case when transaction_code like 'AD%' then 4
              when transaction_code like 'FE%' then 5
              when transaction_code like 'IS%' then 6
              when transaction_code like 'PM%' then 7
              when transaction_code like 'SD%' then 8
              when transaction_code like 'VS%' then 9 else 0 end
        AS leading_num,
        *
        from ANALYTICS.LOOKER."TRANSACTIONS"   
        ) t
  left join (select distinct gpt.id
              from mysql_db.galileo.galileo_posted_transactions gpt
              join mysql_db.galileo.galileo_account_cards gac on gpt.card_id = gac.card_id
              join mysql_db.chime_prod.card_attributes ca     on gac.id=ca.GALILEO_ACCOUNT_CARD_ID
              where ca.virtual=1 and gpt.TRANSACTION_TIMESTAMP>= '2020-10-01' and gpt.TRANSACTION_TIMESTAMP <= '2020-12-31') virtual 
  on t.id = virtual.id
  LEFT JOIN (SELECT DISTINCT A.USER_ID, B.TRANSACTION_ID, A.REASON
             FROM ANALYTICS.LOOKER.USER_DISPUTE_CLAIMS AS A
             LEFT JOIN ANALYTICS.LOOKER.USER_DISPUTE_CLAIM_TRANSACTIONS AS B
             ON A.ID=B.USER_DISPUTE_CLAIM_ID) AS dispute
  ON t.USER_ID=dispute.USER_ID
  AND TO_NUMBER(CONCAT(t.leading_num, t.AUTHORIZATION_CODE)) = TO_NUMBER(dispute.TRANSACTION_ID)
  LEFT JOIN score on score.member_id = t.user_id
  left join analytics.looker.member_acquisition_facts member on member.user_id = t.user_id
  --where t.TRANSACTION_TIMESTAMP >= '2021-1-1' and t.TRANSACTION_TIMESTAMP <= '2021-2-28'
  where t.TRANSACTION_TIMESTAMP >= '2020-10-01' and t.TRANSACTION_TIMESTAMP <= '2020-12-31' --Q4 2020
) subquery
group by 1,2,3,4
order by 5
;

