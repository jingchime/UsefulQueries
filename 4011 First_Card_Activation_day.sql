--three tables are used
-- 2018/11/21
select * from "MYSQL_DB"."GALILEO"."GALILEO_ACCOUNT_CARDS"
where user_id = 2178430
order by CREATED_AT;

-- 2018/11/27
select 
*
FROM SEGMENT.CHIME_PROD.CARD_ACTIVATED
WHERE user_id = 2178430;

-- 2018/11/27
SELECT
*
FROM mysql_db.chime_prod.ACCOUNT_EVENTS
WHERE user_id = 2178430;

-- compare 7 methods based on three tables for 13 users:
select
method_1.user_id,
date_trunc(day,method_1.card_activated_at)::DATE as method_1_day,
date_trunc(day,method_2.card_activated_at)::DATE as method_2_day,
date_trunc(day,method_3.card_activated_at)::DATE as method_3_day,
date_trunc(day,method_4.card_activated_at)::DATE as method_4_day,
date_trunc(day,method_5.card_activated_at)::DATE as method_5_day,
date_trunc(day,method_6.card_activated_at)::DATE as method_6_day,
date_trunc(day,method_7.card_activated_at)::DATE as method_7_day
from
(SELECT
user_id,
min(CASE WHEN type = 'card_activated' THEN CREATED_AT END)::timestamp AS card_activated_at
FROM mysql_db.chime_prod.ACCOUNT_EVENTS
GROUP BY 1) method_1
left join
(SELECT
user_id,
min(CASE WHEN STATUS = 'N' THEN CREATED_AT END)::timestamp AS card_activated_at
FROM "MYSQL_DB"."GALILEO"."GALILEO_ACCOUNT_CARDS"
GROUP BY 1
) method_2
on method_1.user_id = method_2.user_id
left join 
(SELECT
user_id,
min(CREATED_AT)::timestamp AS card_activated_at 
FROM "MYSQL_DB"."GALILEO"."GALILEO_ACCOUNT_CARDS"
GROUP BY 1
) method_3
on method_1.user_id = method_3.user_id
left join
(select 
USER_ID,
min(ORIGINAL_TIMESTAMP) as card_activated_at
FROM SEGMENT.CHIME_PROD.CARD_ACTIVATED
WHERE FIRST_CARD = True
group by 1
) method_4
on method_1.user_id = method_4.user_id
left join
(select 
USER_ID,
min(ORIGINAL_TIMESTAMP) as card_activated_at
FROM SEGMENT.CHIME_PROD.CARD_ACTIVATED
group by 1
) method_5
on method_1.user_id = method_5.user_id
left join
(SELECT
user_id,
min(CASE WHEN STATUS in ('N', 'S', 'L') THEN CREATED_AT END)::timestamp AS card_activated_at
FROM "MYSQL_DB"."GALILEO"."GALILEO_ACCOUNT_CARDS"
GROUP BY 1
) method_6
on method_1.user_id = method_6.user_id
left join
(SELECT
user_id,
min(CASE WHEN type in ('card_activated','card_status_change') THEN CREATED_AT END)::timestamp AS card_activated_at
FROM mysql_db.chime_prod.ACCOUNT_EVENTS
GROUP BY 1
) method_7
on method_1.user_id = method_7.user_id
where method_1.user_id in
(2178430,
 3336306,
10108129,
 3561129,
 3279873,
 239070,
 7383340,
 1420051,
 196086,
16447636,
2046525,
450793,
15230817,
1420051,
7383340);

-- final method combining all parts:
select 
if_trx_earlier_than_activation,
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
   activate.card_activated_at,
   t.TRANSACTION_TIMESTAMP,
   case when activate.card_activated_at is null then 'no first card info'
        when t.TRANSACTION_TIMESTAMP < activate.card_activated_at then 'trx before activation'
   else 'trx after activation' end as if_trx_earlier_than_activation,
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
  left join (select
             user_id,
            dateadd(day, 5, min(CREATED_AT)::timestamp) AS card_activated_at 
            FROM "MYSQL_DB"."GALILEO"."GALILEO_ACCOUNT_CARDS"
            GROUP BY 1) activate 
  on t.user_id = activate.user_id
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

-- for spot check case study
-- user_id 19800418 actual activated 2020-11-16, this method show 2020-11-15 (PF_outgoing)

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
 activate.card_activated_at,
 t.TRANSACTION_TIMESTAMP,
 case when t.TRANSACTION_TIMESTAMP < activate.card_activated_at then 1 else 0 end as if_trx_earlier_than_activation,
 CASE WHEN dispute.TRANSACTION_ID IS NOT NULL THEN 1 ELSE 0 END AS FLG_DISPUTED,
 t.user_id,
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
left join (select
            user_id,
            dateadd(day, 5, min(CREATED_AT)::timestamp) AS card_activated_at 
            FROM "MYSQL_DB"."GALILEO"."GALILEO_ACCOUNT_CARDS"
            GROUP BY 1) activate 
on t.user_id = activate.user_id
LEFT JOIN (SELECT DISTINCT A.USER_ID, B.TRANSACTION_ID, A.REASON
           FROM ANALYTICS.LOOKER.USER_DISPUTE_CLAIMS AS A
           LEFT JOIN ANALYTICS.LOOKER.USER_DISPUTE_CLAIM_TRANSACTIONS AS B
           ON A.ID=B.USER_DISPUTE_CLAIM_ID) AS dispute
ON t.USER_ID=dispute.USER_ID
AND TO_NUMBER(CONCAT(t.leading_num, t.AUTHORIZATION_CODE)) = TO_NUMBER(dispute.TRANSACTION_ID)
where if_trx_earlier_than_activation = 1 and transaction_type not in ('Other') 
limit 100;