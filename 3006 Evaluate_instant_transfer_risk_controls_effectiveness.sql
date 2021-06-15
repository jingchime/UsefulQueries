with score as (
select 
api.user_id 
--new sigma_v2
, case when parse_json(result):fraud:scores[0]:name = 'sigma' and  parse_json(result):fraud:scores[0]:version = '1.0' then parse_json(result):fraud:scores[0]:score
       when parse_json(result):fraud:scores[1]:name = 'sigma' and  parse_json(result):fraud:scores[1]:version = '1.0' then parse_json(result):fraud:scores[1]:score
       when parse_json(result):fraud:scores[2]:name = 'sigma' and  parse_json(result):fraud:scores[2]:version = '1.0' then parse_json(result):fraud:scores[2]:score
       else null
       end as sigma_v1  
, case when parse_json(result):fraud:scores[0]:name = 'sigma' and  parse_json(result):fraud:scores[0]:version = '2.0' then parse_json(result):fraud:scores[0]:score
       when parse_json(result):fraud:scores[1]:name = 'sigma' and  parse_json(result):fraud:scores[1]:version = '2.0' then parse_json(result):fraud:scores[1]:score
       when parse_json(result):fraud:scores[2]:name = 'sigma' and  parse_json(result):fraud:scores[2]:version = '2.0' then parse_json(result):fraud:scores[2]:score
       else null
       end as sigma_v2  
, row_number() over(partition by api.user_id order by api.created_at asc) as rnum
from mysql_db.chime_prod.external_api_requests api
where 1=1
and api.service='socure3' 
and  CHECK_JSON(api.result) is null 
and api.created_at >= '2020-07-01' 
qualify rnum=1)

SELECT 
DATE_TRUNC('day', CREATED_AT)::DATE AS transaction_day,
t1.user_id,
amount,
CASE WHEN cb_user.user_id IS NOT NULL then 1 else 0 end as if_chargedback,
score.sigma_v1,
score.sigma_v2,
case when validated.user_id is not null then 1 else 0 end as if_name_address_matched,
to_date(validated.validation_date) as validation_day,
case when validation_day<= transaction_day then 1 else 0 end as if_name_address_matched_at_time_of_transaction

FROM "MYSQL_DB"."CHIME_PROD"."USER_ADJUSTMENTS" t1
LEFT JOIN (select distinct user_id
            from  "MYSQL_DB"."CHIME_PROD"."USER_ADJUSTMENTS" 
            where TYPE = 'external_card_chargeback') cb_user
ON t1.user_id = cb_user.user_id
left join score 
on t1.user_id = score.user_id
left join (select user_id, min(created_at) as validation_date from mysql_db.chime_prod.ach_accounts where transfer_direction = 'both' group by 1) as validated 
on t1.user_id = validated.user_id
WHERE CREATED_AT  >= TO_TIMESTAMP('2020-10-26') 
       and TYPE = 'external_card_transfer'
;


-- exploring the name address match data
-- check if a user can have more than 1 direction 
--- yes
select 
num,
count(user_id) 
from
(
  select 
  USER_ID,
  count(distinct transfer_direction) as num
  from 
  mysql_db.chime_prod.ach_accounts 
  group by 1
)
group by 1
order by 2 desc;

-- finding some examples of people who has 3 directions
select 
USER_ID,
count(distinct transfer_direction) as num
from 
mysql_db.chime_prod.ach_accounts 
group by 1
having num = 3
limit 10;

-- check details
select 
CREATED_AT,
transfer_direction,
*
from 
mysql_db.chime_prod.ach_accounts 
where user_id = 1161377
order by 1;


