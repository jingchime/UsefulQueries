---- 2021/07/19 update
---- new kyc table
---- query needs to be modifed

select user_id,result as api_result
        , parse_json(result):emailRisk:score as email_risk
        , case when parse_json(result):fraud:scores[0]:name = 'sigma' then parse_json(result):fraud:scores[0]:score
             when parse_json(result):fraud:scores[1]:name = 'sigma' then parse_json(result):fraud:scores[1]:score
             when parse_json(result):fraud:scores[2]:name = 'sigma' then parse_json(result):fraud:scores[2]:score
             else null
          end as sigma_generic_score     
      , row_number() over(partition by api.user_id order by api.created_at asc) as rnum
from mysql_db.chime_prod.external_api_requests api 
where  service='socure3' 
      and  CHECK_JSON(result) is null --checking for valid JSON's
and api.created_at >= '2019-01-01' qualify rnum = 1

UNION

select DISTINCT user_id, raw_response as api_result,
raw_response:emailRisk:score as email_risk,
raw_response:fraud:scores[2]:score as sigma_generic_score,
1 as rnum
from POSTGRES_DB.KYC_SERVICE.decisions 
join POSTGRES_DB.KYC_SERVICE.vendor_inquiries 
on decisions.id = decision_id
where vendor='socure' and client != 'credit_builder' ;



----previous version
----comprehensive but table is being abondoned

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
qualify rn=1),

instant_transfers_users AS (select distinct user_id
          from ANALYTICS.LOOKER."TRANSACTIONS" T  
          where transaction_code in ('PMDB', 'PMTP', 'ADac', 'ADAS', 'ADTR', 'ADar')  
          and transaction_timestamp >= '2020-11-01')
          
SELECT instant_transfers_users.user_id,
case when chargeback_users.user_id is not null then 1 else 0 end as if_chargeback,
score.SIGMA_V2,
score.SYNTHETIC_SCORE,
score.SOCURE_CHIME_SCORE,
ntile(100) over (order by score.SIGMA_V2 desc) as SIGMA_V2_ntile_100,
ntile(100) over (order by score.SYNTHETIC_SCORE desc) as SYNTHETIC_SCORE_ntile_100,
ntile(100) over (order by score.SOCURE_CHIME_SCORE desc) as SOCURE_CHIME_SCORE_ntile_100
FROM instant_transfers_users
LEFT JOIN (select distinct user_id  
  from ANALYTICS.LOOKER."TRANSACTIONS" transactions 
  where transaction_code in ('ADac', 'ADAS', 'ADTR', 'ADar')) 
  chargeback_users on instant_transfers_users.user_id = chargeback_users.user_id
LEFT JOIN score on score.member_id = instant_transfers_users.user_id
WHERE score.SIGMA_V2 is not null and score.SYNTHETIC_SCORE is not null and score.SOCURE_CHIME_SCORE is not null
;
          
