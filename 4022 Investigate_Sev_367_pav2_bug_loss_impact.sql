--- final query behind the excel file
-- This query finds completed TabaPay transactions with invalid associated sets of Galileo transactions.
-- Valid Galileo transaction set: only one ADPF transaction per TabaPay transaction
with 
tabapay_transactions as (
    select *
    from partner_db.tabapay.transactions
    where created_date between 
        current_timestamp() - interval '90 days' and
        current_timestamp() - interval '2 days'
),
gpt as (
    select *
    from mysql_db.galileo.galileo_posted_transactions
    where 
        transaction_timestamp >= current_timestamp() - interval '120 days' and
        transaction_code in ('ADPF', 'ADpb', 'PMTS', 'PMTU', 'PMPB', 'ADTS', 'ADTU')
),
claims as (
    select *
    from postgres_db.mms.payfriend_claims
    where created_at >= current_timestamp() - interval '120 days'
),
payfriend_records as (
    select pf.*
    from mysql_db.chime_prod.pay_friends pf
    where pf.created_at >= current_timestamp() - interval '120 days'
),
pav2_tabapay_transactions as (
    select *, tt.memo::integer as sender_adjustment_id
    from tabapay_transactions tt
    where tt.status = 'Complete' and (tt.location_name = 'pf' or tt.memo::integer > 0)
),
reconciliation as (
    select
        pf.sender_id,
        pf.receiver_id,
        tt.transaction_id as tabapay_transaction_id,
        max(tt.transaction_amount) as amount,
        array_agg(gpt.transaction_code) as gpt_transactions
    from pav2_tabapay_transactions tt
    left join payfriend_records pf on pf.sender_adjustment_id = tt.sender_adjustment_id
    left join gpt on gpt.merchant_number = pf.sender_adjustment_id::varchar
    where pf.id is not null
    group by 1,2,3
)
select
    sender_id,
    receiver_id,
    count(r.tabapay_transaction_id),
    sum(r.amount)
from reconciliation r
where gpt_transactions != array_construct('ADPF')
group by 1,2

---------------------------------------------------------
---------------------------------------------------------
---------------------------------------------------------
---------------------------------------------------------
-- add bank and email---
---tables used:
select user_id, bank.PRIMARY_PROGRAM_ASSIGNED
from "ANALYTICS"."LOOKER"."MEMBER_PARTNER_BANK" bank 
where user_id in ();

select id, u.email
from mysql_db.chime_prod.users u
where u.id in ()
;

---------------------------------------------------------
---------------------------------------------------------
---------------------------------------------------------
---------------------------------------------------------
--investigation (not necessarily correct code)
-- sergey's 1.3M
select p.sender_id
 , sum(p.amount)
from postgres_db.mms.payfriend_claims c
join mysql_db.chime_prod.pay_friends p on p.id = c.payfriend_id
join mysql_db.chime_prod.users u on p.sender_id = u.id
where c.id in (select payfriend_claim_id from "FIVETRAN"."CSV"."PF_CLAIMS")
group by 1 order by sum(p.amount) desc
;

-- check user_ids in the fivetran table
select distinct p.sender_id as user_id
from postgres_db.mms.payfriend_claims c 
join mysql_db.chime_prod.pay_friends p on p.id = c.payfriend_id
where c.id in (select payfriend_claim_id from "FIVETRAN"."CSV"."PF_CLAIMS")
;

-- GPT transactions for those users
select 
gpt.user_id,
sum(transaction_amount) as gpt_amount, 
sum(case transaction_code when 'ADPF' then transaction_amount else 0 end) as gpt_sent,
sum(case transaction_code when 'ADpb' then transaction_amount else 0 end) as gpt_returned
from mysql_db.galileo.galileo_posted_transactions gpt
where USER_ID IN (select distinct p.sender_id as user_id
      from postgres_db.mms.payfriend_claims c 
      join mysql_db.chime_prod.pay_friends p on p.id = c.payfriend_id
      where c.id in (select payfriend_claim_id from "FIVETRAN"."CSV"."PF_CLAIMS"))
   and transaction_code in ('ADPF', 'ADpb')
   and transaction_timestamp between
        current_timestamp() - interval '2 days' - interval '60 days' and 
      current_timestamp() - interval '2 days'
group by 1;

-- Urvi's checking code
select *  
from mysql_db.chime_prod.pay_friends a
join partner_db.tabapay.transactions b
on a.SENDER_ADJUSTMENT_ID = b.memo::integer
where SENDER_ID = 21756362
and a.status<>'succeeded';
