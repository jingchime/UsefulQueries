-- all tables join together logic 
select *
from "POSTGRES_DB"."MMS"."EXTERNAL_CARD_TRANSFERS" t
join "MYSQL_DB"."GALILEO"."GALILEO_POSTED_TRANSACTIONS" gpt on gpt.external_transaction_id = to_varchar(t.chime_transaction_id)
join "MYSQL_DB"."CHIME_PROD"."USER_ADJUSTMENTS" a on a.id = t.payment_adjustment_id
join "PARTNER_DB"."TABAPAY"."EXCEPTIONS" e on e.original_transaction_id = t.tabapay_transaction_id limit 1;

-- method 1
select 
TO_CHAR(DATE_TRUNC('month',e.STATUS_DATE), 'YYYY-MM') as trnx_month,
sum(t.amount)
from "POSTGRES_DB"."MMS"."EXTERNAL_CARD_TRANSFERS" t
--left join "MYSQL_DB"."CHIME_PROD"."USER_ADJUSTMENTS" a on a.id = t.payment_adjustment_id
left join "PARTNER_DB"."TABAPAY"."EXCEPTIONS" e on e.original_transaction_id = t.tabapay_transaction_id
where t.created_at>='2020-12-01' and exception_type = 'REPRESENTMENT'
group by 1
order by 1;

-- method 2
select TO_CHAR(DATE_TRUNC('month',created_at), 'YYYY-MM') as trnx_month,
sum(AMOUNT)
from  "MYSQL_DB"."CHIME_PROD"."USER_ADJUSTMENTS" 
where TYPE = 'external_card_representment'
and CREATED_AT >= '2020-12-01'
group by 1
order by 1;

-- compare difference
select
tabapay.*,
adjustment.*
from
(select 
TO_CHAR(DATE_TRUNC('day',t.created_at), 'YYYY-MM-DD') as transaction_day,
TO_CHAR(DATE_TRUNC('day',e.STATUS_DATE), 'YYYY-MM-DD') as representment_post_day,
a.user_id,
sum(e.exception_settled_amount) as tabapay_total_representment
from "POSTGRES_DB"."MMS"."EXTERNAL_CARD_TRANSFERS" t
left join "MYSQL_DB"."CHIME_PROD"."USER_ADJUSTMENTS" a on a.id = t.payment_adjustment_id
left join "PARTNER_DB"."TABAPAY"."EXCEPTIONS" e on e.original_transaction_id = t.tabapay_transaction_id
where t.created_at>='2020-12-01' and exception_type = 'REPRESENTMENT'
group by 1,2,3) tabapay
left join
(SELECT
TO_CHAR(DATE_TRUNC('day',created_at), 'YYYY-MM-DD') as trnx_day,
USER_ID,
sum(AMOUNT) as adjustment_total_representment
from  "MYSQL_DB"."CHIME_PROD"."USER_ADJUSTMENTS" 
where TYPE = 'external_card_representment'
and CREATED_AT >= '2020-12-01'
group by 1,2) adjustment
ON tabapay.representment_post_day = adjustment.trnx_day and tabapay.user_id = adjustment.user_id
WHERE adjustment.USER_ID IS NOT NULL

