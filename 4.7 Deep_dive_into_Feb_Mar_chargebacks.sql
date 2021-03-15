WITH instant_transfers AS (select id, user_id,transaction_code, transaction_timestamp, transaction_amount	
          from ANALYTICS.LOOKER."TRANSACTIONS" T	
          where transaction_code in ('PMDB', 'PMTP', 'ADac', 'ADAS', 'ADTR', 'ADar') 	
          and transaction_timestamp >= '2020-11-01'),

score as (select 
to_date(api.created_at) as created_on,
api.user_id as member_id
, row_number() over(partition by user_id order by created_at) as rn          
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
       end as synthetic_score
from mysql_db.chime_prod.external_api_requests api 
where 1=1
and api.service='socure3' 
and  CHECK_JSON(api.result) is null 
and api.created_at >= '2020-07-01' 
qualify rn=1) 
          	
SELECT DATE_TRUNC('day', TRANSACTION_TIMESTAMP)::DATE AS TXN_day,	
    instant_transfers.user_id,
    member.enrollment_date,
    instant_transfers.transaction_amount,
    score.sigma_v1,
    score.sigma_v2,
    users.socure_enrollment_score,
    accounts.bank_name
FROM instant_transfers 
left join analytics.looker.member_acquisition_facts member on instant_transfers.user_id = member.user_id
left join score on score.member_id = instant_transfers.user_id
left join mysql_db.chime_prod.users users on users.id = instant_transfers.user_id
LEFT JOIN MYSQL_DB.CHIME_PROD.ACH_ACCOUNTS accounts on instant_transfers.user_id = accounts.user_id
WHERE instant_transfers.transaction_timestamp  >= TO_TIMESTAMP('2021-2-1')	
       and instant_transfers.user_id in (select distinct user_id	
          from ANALYTICS.LOOKER."TRANSACTIONS" transactions	
          where transaction_code in ('ADac', 'ADAS', 'ADTR', 'ADar'))	
       and transaction_code in ('PMDB', 'PMTP')
order by 1 
;