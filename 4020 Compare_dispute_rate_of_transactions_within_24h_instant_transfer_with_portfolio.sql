--dispute rate for those with instant transfers within 24 hours
select
trxn_month,
count(distinct user_id) total_users,
count(distinct id) total_transactions,
-sum(TRANSACTION_AMOUNT) as total_transaction_amount,
-sum(disputed_amount) as total_disputed_amount,
total_disputed_amount/total_transaction_amount as dispute_rate
from
(
    select
    t1.trxn_month,
    t1.user_id,
    t1.id,
    t1.TRANSACTION_AMOUNT,
    CASE WHEN dispute.TRANSACTION_ID IS NOT NULL THEN 1 ELSE 0 END AS FLG_DISPUTED,
    case when FLG_DISPUTED = 1 then TRANSACTION_AMOUNT else 0 end as disputed_amount
    from
        (select     
              case when transaction_code like 'AD%' then 4
                    when transaction_code like 'FE%' then 5
                    when transaction_code like 'IS%' then 6
                    when transaction_code like 'PM%' then 7
                    when transaction_code like 'SD%' then 8
                    when transaction_code like 'VS%' then 9 else 0 end
              AS leading_num, -- Add the leading num
             case when (transaction_code in ('ISA', 'ISC', 'ISJ', 'ISL', 'ISM', 'ISR', 'ISZ', 'VSA', 'VSC', 'VSJ', 'VSL', 'VSM', 'VSR', 'VSZ',
                             'SDA', 'SDC', 'SDL', 'SDM', 'SDR', 'SDV', 'SDZ', 'PLM', 'PLA', 'PRA') and unique_program_id in (600, 278))  then 'Credit Purchase'
                   when transaction_code in ('ISA', 'ISC', 'ISJ', 'ISL', 'ISM', 'ISR', 'ISZ', 'VSA', 'VSC', 'VSJ', 'VSL', 'VSM', 'VSR', 'VSZ',
                                   'SDA', 'SDC', 'SDL', 'SDM', 'SDR', 'SDV', 'SDZ', 'PLM', 'PLA', 'PRA') then 'Debit Purchase'      
                   WHEN TRANSACTION_CODE = 'ADS' THEN 'ACH Transfer'  --ACH PUSH
                   WHEN TRANSACTION_CODE in ('PMDB', 'PMTP') then 'Instant Transfer'
                   WHEN TRANSACTION_CODE in ('ADbc', 'ADcn') then 'ACH debit'
                   when transaction_code in ('ADM', 'ADPF', 'ADTS', 'ADTU','ADpb') then 'PF_outgoing'
                   when transaction_code in ('VSW','MPW','MPM', 'MPR','PLW', 'PLR','PRW', 'SDW') then 'ATM Withdrawals'    
             else 'Other' end as transaction_type,
           to_date(date_trunc('quarter',TRANSACTION_TIMESTAMP)) as trxn_quarter,
           to_date(date_trunc('month',TRANSACTION_TIMESTAMP)) as trxn_month,
           to_date(date_trunc('day',TRANSACTION_TIMESTAMP)) as trxn_day,
           TRANSACTION_TIMESTAMP,
           t.user_id,
           t.id,
           AUTHORIZATION_CODE,
           TRANSACTION_AMOUNT
        from ANALYTICS.LOOKER."TRANSACTIONS" t 
        join (select CREATED_AT, id, USER_ID, AMOUNT
               from  "MYSQL_DB"."CHIME_PROD"."USER_ADJUSTMENTS" 
               where TYPE = 'external_card_transfer') instant_transfers 
         on t.user_id = instant_transfers.user_id and t.TRANSACTION_TIMESTAMP between instant_transfers.created_at and dateadd(hour, 24, instant_transfers.created_at) 
         where transaction_type = 'Debit Purchase' and trxn_month>= '2020-10-01'
         group by 1,2,3,4,5,6,7,8,9,10 -- dedup the same transactions due to quick subsequent instant transfers
    ) t1                                                                                         
    LEFT JOIN (SELECT DISTINCT A.USER_ID, B.TRANSACTION_ID, A.REASON
             FROM ANALYTICS.LOOKER.USER_DISPUTE_CLAIMS AS A
             LEFT JOIN ANALYTICS.LOOKER.USER_DISPUTE_CLAIM_TRANSACTIONS AS B
             ON A.ID=B.USER_DISPUTE_CLAIM_ID) AS dispute
    ON t1.USER_ID=dispute.USER_ID AND TO_NUMBER(CONCAT(t1.leading_num, t1.AUTHORIZATION_CODE)) = TO_NUMBER(dispute.TRANSACTION_ID)
)
group by 1
order by 1
;

---portfolio
-- portfolio benchmark
select
trxn_month,
count(distinct user_id) total_users,
count(distinct id) total_transactions,
-sum(TRANSACTION_AMOUNT) as total_transaction_amount,
-sum(disputed_amount) as total_disputed_amount,
total_disputed_amount/total_transaction_amount as dispute_rate
from
  (
  select
    t.trxn_month,
    t.transaction_type,
    t.user_id,
    t.id,
    t.TRANSACTION_AMOUNT,
    CASE WHEN dispute.TRANSACTION_ID IS NOT NULL THEN 1 ELSE 0 END AS FLG_DISPUTED,
    case when FLG_DISPUTED = 1 then TRANSACTION_AMOUNT else 0 end as disputed_amount
  from
  ( select    
  to_date(date_trunc('month',TRANSACTION_TIMESTAMP)) as trxn_month,
    case when transaction_code like 'AD%' then 4
                when transaction_code like 'FE%' then 5
                when transaction_code like 'IS%' then 6
                when transaction_code like 'PM%' then 7
                when transaction_code like 'SD%' then 8
                when transaction_code like 'VS%' then 9 else 0 end
          AS leading_num, -- Add the leading num
  case when (t.transaction_code in ('ISA', 'ISC', 'ISJ', 'ISL', 'ISM', 'ISR', 'ISZ', 'VSA', 'VSC', 'VSJ', 'VSL', 'VSM', 'VSR', 'VSZ',
               'SDA', 'SDC', 'SDL', 'SDM', 'SDR', 'SDV', 'SDZ', 'PLM', 'PLA', 'PRA') and unique_program_id in (600, 278))  then 'Credit Purchase'
    when t.transaction_code in ('ISA', 'ISC', 'ISJ', 'ISL', 'ISM', 'ISR', 'ISZ', 'VSA', 'VSC', 'VSJ', 'VSL', 'VSM', 'VSR', 'VSZ',
                 'SDA', 'SDC', 'SDL', 'SDM', 'SDR', 'SDV', 'SDZ', 'PLM', 'PLA', 'PRA') then 'Debit Purchase'      
    WHEN t.TRANSACTION_CODE = 'ADS' THEN 'ACH Transfer'  --ACH PUSH
    WHEN t.TRANSACTION_CODE in ('ADbc', 'ADcn') then 'ACH debit'
    when t.transaction_code in ('ADM', 'ADPF', 'ADTS', 'ADTU','ADpb') then 'PF_outgoing'
    when t.transaction_code in ('VSW','MPW','MPM', 'MPR','PLW', 'PLR','PRW', 'SDW') then 'ATM Withdrawals'    
    else 'Other' end as transaction_type,
       t.user_id,
       t.id,
       t.AUTHORIZATION_CODE,
       TRANSACTION_AMOUNT   
  from ANALYTICS.LOOKER."TRANSACTIONS" t) t
  LEFT JOIN (SELECT DISTINCT A.USER_ID, B.TRANSACTION_ID, A.REASON
             FROM ANALYTICS.LOOKER.USER_DISPUTE_CLAIMS AS A
             LEFT JOIN ANALYTICS.LOOKER.USER_DISPUTE_CLAIM_TRANSACTIONS AS B
             ON A.ID=B.USER_DISPUTE_CLAIM_ID) AS dispute
    ON t.USER_ID=dispute.USER_ID AND TO_NUMBER(CONCAT(t.leading_num, t.AUTHORIZATION_CODE)) = TO_NUMBER(dispute.TRANSACTION_ID)
  where t.transaction_type = 'Debit Purchase' and t.trxn_month>='2020-10-01'
  ) tmp 
group by 1;