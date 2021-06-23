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

select 
to_date(date_trunc(day,ach_transfer.CREATED_AT)) as trn_day,
to_date(date_trunc(quarter,ach_transfer.CREATED_AT)) as trn_quarter,
ach_transfer.id,
case when ach_transfer."STATUS" = 'failed' then 1 else 0 end if_returned_ach,
score.sigma_v1,
score.sigma_v2,
case when score.sigma_v1 >= 0.9 then 1 else 0 end as if_v1_GET_9,
case when validated.user_id is not null then 1 else 0 end as if_name_address_matched,
to_date(validated.validation_date) as validation_day,
case when validation_day<= ach_transfer.CREATED_AT then 1 else 0 end as if_name_address_matched_at_time_of_transaction,
case when new_match.user_id is not null then 1 else 0 end as if_new_plaid_name_address_matched

from "MYSQL_DB"."CHIME_PROD"."ACH_TRANSFERS" ach_transfer
join mysql_db.chime_prod.ach_accounts aa on ach_transfer.ach_account_id = aa.id
left join score 
on aa.user_id = score.user_id
left join (select user_id, min(created_at) as validation_date from mysql_db.chime_prod.ach_accounts where transfer_direction = 'both' or transfer_direction = 'out' or ADDRESS_MATCHED = 1 OR NAME_MATCHED = 1 group by 1) as validated 
on aa.user_id = validated.user_id
left join (select distinct user_id from "MYSQL_DB"."CHIME_PROD"."ACH_ACCOUNTS" where ADDRESS_MATCHED = 1 OR NAME_MATCHED = 1) as new_match
on aa.user_id = new_match.user_id
where ach_transfer."TYPE" = 'debit' AND ach_transfer."STATUS" in ('failed','processed') and ach_transfer.CREATED_AT >= '2020-07-01'
;