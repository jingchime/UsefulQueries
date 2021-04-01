-- tables to start with
select * from "MYSQL_DB"."CHIME_PROD"."REALTIME_AUTH_EVENTS" limit 10;
select * from ANALYTICS.LOOKER."TRANSACTIONS"  limit 10;

-- table 1 case study
select *
from ANALYTICS.LOOKER."TRANSACTIONS" t 
where user_id = 5108088; --2337 rows

-- table 2 case study
select *
from "MYSQL_DB"."CHIME_PROD"."REALTIME_AUTH_EVENTS" 
where user_id = 5108088; --3870 rows

-- join logic one
select a.TRANS_DATE, a.available_funds, *
from ANALYTICS.LOOKER."TRANSACTIONS" t 
join "MYSQL_DB"."CHIME_PROD"."REALTIME_AUTH_EVENTS" a
on t.user_id = a.user_id and t.AUTHORIZATION_CODE = a.AUTH_ID
where t.user_id = 5108088
order by a.TRANS_DATE; -- 2218 rows