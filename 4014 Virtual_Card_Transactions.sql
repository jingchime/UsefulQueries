
select gpt.* 
from mysql_db.galileo.galileo_posted_transactions gpt
join mysql_db.galileo.galileo_account_cards gac on gpt.card_id = gac.card_id
join mysql_db.chime_prod.card_attributes ca     on gac.id=ca.GALILEO_ACCOUNT_CARD_ID
where ca.virtual=1 and gpt.user_id = 11395377  --card_id 52976279
order by TRANSACTION_TIMESTAMP;

-- for analysis
select 
if_virtual_card_transactions,
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
   t.TRANSACTION_TIMESTAMP,
   case when virtual.id is not null then 1
   else 0 end as if_virtual_card_transactions,
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
  left join (select distinct gpt.id
              from mysql_db.galileo.galileo_posted_transactions gpt
              join mysql_db.galileo.galileo_account_cards gac on gpt.card_id = gac.card_id
              join mysql_db.chime_prod.card_attributes ca     on gac.id=ca.GALILEO_ACCOUNT_CARD_ID
              where ca.virtual=1 and gpt.TRANSACTION_TIMESTAMP>= '2020-10-01' and gpt.TRANSACTION_TIMESTAMP <= '2020-12-31') virtual 
  on t.id = virtual.id
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