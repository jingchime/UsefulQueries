--dispute rates
  with outbound_transaction_id as
(
  select user_id, GALILEO_TRANSACTION_ID, GALILEO_TRANSACTION_ID authorization_code, to_date(SENT_AT)SENT_AT, AMOUNT
  from segment.move_money_service.instant_outbound_transfer_succeeded
)
, all_disputes_transaction_id as
(
  select distinct dc.user_id
  , dt.USER_DISPUTE_CLAIM_ID
  , dc.created_at::timestamp as dispute_created_at
  , dc.reason
  , dc.dispute_type
  , dt.transaction_id
  , dt.amount
  from fivetran.mysql_rds_disputes.user_dispute_claims dc
  join fivetran.mysql_rds_disputes.user_dispute_claim_transactions dt
    on dc.id=dt.USER_DISPUTE_CLAIM_ID 
  where dispute_created_at >= '2021-01-01'
  group by 1, 2, 3, 4, 5, 6, 7
)
select 
to_date(SENT_AT)dt, 
count(p.AUTHORIZATION_CODE)outbound_transactions_cnt, 
sum(p.AMOUNT)outbound_transactions_amt, 
count(case when d.TRANSACTION_ID is not null then d.TRANSACTION_ID end)disputed_trxn_cnt, 
coalesce(sum(case when d.TRANSACTION_ID is not null then d.amount*-1 end),0)disputed_trxn_amt 
from outbound_transaction_id p
left join all_disputes_transaction_id d 
        on p.user_id = d.user_id
        and (d.transaction_id  = p.authorization_code
    or right(d.transaction_id, len(d.transaction_id) - 1) = p.authorization_code)
    group by 1;

               

--dispute resolution
with outbound_transaction_id as
(     select user_id, GALILEO_TRANSACTION_ID, GALILEO_TRANSACTION_ID authorization_code, to_date(SENT_AT)SENT_AT, AMOUNT
  from segment.move_money_service.instant_outbound_transfer_succeeded
)

, all_disputes_transaction_id as
(
  select udc.user_id, udct.USER_DISPUTE_CLAIM_ID, udc.created_at::timestamp as dispute_created_at, udc.reason, udc.dispute_type, udct.transaction_id, udct.transaction_code, udcu.close_code, resolution_code
    , case 
    when close_code in (300, 301, 302, 303, 307, 357, 361, 363, 340) then 'approved'
    when close_code in (321, 362) then 'merchant_credit'
    when close_code in (322, 326, 356, 367) then 'cancelled/withdrawn/deleted'
    when close_code in (325, 350, 353, 354, 366) then 'denied' 
    when close_code in (320, 327, 364, 365) then 'unknown'
    else 'null' end as close_code_grp_updated
  from fivetran.mysql_rds_disputes.user_dispute_claims udc
  join fivetran.mysql_rds_disputes.user_dispute_claim_transactions udct on udc.id=udct.USER_DISPUTE_CLAIM_ID
  left join fivetran.mysql_rds_disputes.user_dispute_claim_updates udcu 
  on (udcu.user_dispute_claim_id = udct.user_dispute_claim_id and (udcu.GALILEO_TRANSACTION_ID = udct.TRANSACTION_ID or udcu.USER_DISPUTE_CLAIM_TRANSACTION_ID =udct.id))
  QUALIFY ROW_NUMBER() OVER (PARTITION BY udc.id, udc.user_id, udct.transaction_id  ORDER BY udcu.created_at DESC) = 1
)

, all_disputes as 
(
  select distinct *
  from outbound_transaction_id p
  join all_disputes_transaction_id d
    on p.user_id = d.user_id         
  and (d.transaction_id  = p.authorization_code
    or right(d.transaction_id, len(d.transaction_id) - 1) = p.authorization_code)
)
select round(count(case when close_code_grp_updated = 'approved' then TRANSACTION_ID end),0) as approved_cnt
, round(sum(case when close_code_grp_updated = 'approved' then amount end),0) as approved_amt
, round(count(case when close_code_grp_updated in ('merchant_credit') then TRANSACTION_ID end),0) as merchant_credit_cnt
, round(sum(case when close_code_grp_updated in ('merchant_credit') then amount end),0) as merchant_credit_amt
, round(count(case when close_code_grp_updated in ('denied', 'cancelled/withdrawn/deleted') then TRANSACTION_ID end),0) as denied_cnt
, round(sum(case when close_code_grp_updated in ('denied', 'cancelled/withdrawn/deleted') then amount end),0) as denied_amt
, round(count(case when close_code_grp_updated in ('unknown') then TRANSACTION_ID end),0) as unknown_cnt
, round(sum(case when close_code_grp_updated in ('unknown') then amount end),0) as unknown_amt
, round(count(case when close_code_grp_updated in ('null') then TRANSACTION_ID end),0) as pending_cnt
, round(sum(case when close_code_grp_updated in ('null') then amount end),0) as pending_amt
from all_disputes;

---dispute approved category
with outbound_transaction_id as
(
  select user_id, GALILEO_TRANSACTION_ID, GALILEO_TRANSACTION_ID authorization_code, to_date(SENT_AT)SENT_AT, AMOUNT
  from segment.move_money_service.instant_outbound_transfer_succeeded
)
, all_disputes_transaction_id as
(
  select distinct dc.user_id
  , dt.USER_DISPUTE_CLAIM_ID
  , dc.created_at::timestamp as dispute_created_at
  , dc.reason
  , dc.dispute_type
  , dt.transaction_id
  , dt.amount
  , close_code
  from fivetran.mysql_rds_disputes.user_dispute_claims dc
  join fivetran.mysql_rds_disputes.user_dispute_claim_transactions dt on dc.id=dt.USER_DISPUTE_CLAIM_ID
  left join fivetran.mysql_rds_disputes.user_dispute_claim_updates udcu on (udcu.user_dispute_claim_id = dt.user_dispute_claim_id and (udcu.GALILEO_TRANSACTION_ID = dt.TRANSACTION_ID or udcu.USER_DISPUTE_CLAIM_TRANSACTION_ID =dt.id))
  QUALIFY ROW_NUMBER() OVER (PARTITION BY dc.id, dc.user_id, dt.transaction_id  ORDER BY udcu.created_at DESC) = 1
  --where dispute_created_at >= '2021-03-03'
  --group by 1, 2, 3, 4, 5, 6, 7,8
)

select round(count(case when reason = 'transfer_not_received_by_recipient' then d.TRANSACTION_ID end),0) as transfer_not_received_by_recipient_cnt
, round(sum(case when reason = 'transfer_not_received_by_recipient' then d.amount end),0)  as transfer_not_received_by_recipient_amt
, round(count(case when reason = 'transfer_to_incorrect_recipient' then d.TRANSACTION_ID end),0)  as transfer_to_incorrect_recipient_cnt
, round(sum(case when reason = 'transfer_to_incorrect_recipient' then d.amount end),0)  as transfer_to_incorrect_recipient_amt
, round(count(case when reason = 'unauthorized_transfer' then d.TRANSACTION_ID end),0)  as unauthorized_transfer_cnt
, round(sum(case when reason = 'unauthorized_transfer' then d.amount end),0)  as unauthorized_transfer_amt
, round(count(case when reason = 'incorrect_transfer_amount' then d.TRANSACTION_ID end),0)  as incorrect_transfer_cnt
, round(sum(case when reason = 'incorrect_transfer_amount' then d.amount end),0)  as incorrect_transfer_amt
, round(count(case when reason = 'duplicate_transfer_amount' then d.TRANSACTION_ID end),0)  as duplicate_transfer_cnt
, round(sum(case when reason = 'duplicate_transfer_amount' then d.amount end),0)  as duplicate_transfer_amt
from  outbound_transaction_id p
join all_disputes_transaction_id d on p.user_id = d.user_id
        and (d.transaction_id  = p.authorization_code
    or right(d.transaction_id, len(d.transaction_id) - 1) = p.authorization_code)
where p.SENT_AT >= '2021-05-27' 
--and close_code <>''
and close_code in ('300', '301', '302', '303', '307', '357', '361', '363', '365')
;