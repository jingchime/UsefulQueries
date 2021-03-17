-- dig deeper into within 10days transactions
select DATE_TRUNC('week', transactions.transaction_timestamp)::DATE as TRX_Date,
member.enrollment_date,
datediff(day,member.enrollment_date,TRX_Date) as days_since_enroll,
transactions.user_id,
case when t.user_id is not null then 1 else 0 end as if_user_chargeback,
case when days_since_enroll<=10 then 1 else 0 end as if_within_10,
case when it_users.user_id is not null then 1 else 0 end as if_instant_transfer_user
from ANALYTICS.LOOKER."TRANSACTIONS" transactions
left join analytics.looker.member_acquisition_facts member
on transactions.user_id = member.user_id
left join (select distinct user_id from "MYSQL_DB"."CHIME_PROD"."USER_ADJUSTMENTS" where type = 'external_card_chargeback') t 
on transactions.user_id = t.user_id
left join (select distinct user_id
          from ANALYTICS.LOOKER."TRANSACTIONS" T  
          where transaction_code in ('PMDB', 'PMTP', 'ADac', 'ADAS', 'ADTR', 'ADar')  
          and transaction_timestamp >= '2020-11-01') it_users
on transactions.user_id = it_users.user_id
where MERCHANT_NAME ilike '%Alibaba.com%' and if_within_10 = 1; 

-- check those not falling in 10days
select DATE_TRUNC('week', transactions.transaction_timestamp)::DATE as TRX_Date,
member.enrollment_date,
datediff(day,member.enrollment_date,TRX_Date) as days_since_enroll,
transactions.user_id,
case when t.user_id is not null then 1 else 0 end as if_user_chargeback,
case when days_since_enroll<=10 then 1 else 0 end as if_within_10,
case when it_users.user_id is not null then 1 else 0 end as if_instant_transfer_user
from ANALYTICS.LOOKER."TRANSACTIONS" transactions
left join analytics.looker.member_acquisition_facts member
on transactions.user_id = member.user_id
left join (select distinct user_id from "MYSQL_DB"."CHIME_PROD"."USER_ADJUSTMENTS" where type = 'external_card_chargeback') t 
on transactions.user_id = t.user_id
left join (select distinct user_id
          from ANALYTICS.LOOKER."TRANSACTIONS" T  
          where transaction_code in ('PMDB', 'PMTP', 'ADac', 'ADAS', 'ADTR', 'ADar')  
          and transaction_timestamp >= '2020-11-01') it_users
on transactions.user_id = it_users.user_id
where MERCHANT_NAME ilike '%Alibaba.com%' and if_within_10 = 0; 
