-- close code
  select udc.user_id
  , udct.USER_DISPUTE_CLAIM_ID
  , udc.created_at::timestamp as dispute_created_at
  , udc.reason
  , udc.dispute_type
  , udct.transaction_id
  , udct.amount
  , udcu.disputed_amount
  , case when close_code = '' then 'close_code_null'
        when close_code in (300, 301, 302, 307, 357, 361, 363, 365) then 'approved'
        when close_code in (322, 326, 356, 367) then 'cancelled'
        when close_code in (325, 350, 353, 354, 364, 366) then 'denied'
        when close_code in (321, 362) then 'merchant_credit'
        when close_code in (303, 320, 327, 340) then 'unknown'
        else 'np' end as close_code_grp
  , case when close_code = '' then 'close_code_null' 
        when close_code in (300, 301, 302, 303, 307, 357, 361, 363, 365) then 'approved'
        when close_code in (322, 325, 326, 356, 367, 340) then 'cancelled/withdrawn'
        when close_code in (350, 353, 354, 364, 366) then 'denied' 
        when close_code in (321, 362) then 'merchant_credit'
        when close_code in (320, 327) then 'unknown'
        else 'null' end as close_code_grp_updated      -- updated with information Tucker and Nik shared
  , case when close_code = '' then null
        when close_code in (300, 301, 302, 307, 357, 361, 363, 365) then udcu.transaction_amount ELSE null END AS udcu_loss_from_disputes
  , case when close_code = '' then null when close_code in (322, 326, 356, 367) then udcu.transaction_amount ELSE null END AS udcu_cancelled
  , case when close_code = '' then null when close_code in (325, 350, 353, 354, 364, 366) then udcu.transaction_amount ELSE null END AS udcu_denied
  , case when close_code = '' then null when close_code in (321, 362) then udcu.transaction_amount ELSE null END AS udcu_merchant_credit
  , case when close_code = '' then null when close_code in (303, 320, 327, 340) then udcu.transaction_amount ELSE null END AS udcu_unknown
  from fivetran.mysql_rds_disputes.user_dispute_claims udc
  join fivetran.mysql_rds_disputes.user_dispute_claim_transactions udct
    on udc.id=udct.USER_DISPUTE_CLAIM_ID and udct.transaction_code in  ('ADM', 'ADTS', 'ADTU')
  left join fivetran.mysql_rds_disputes.user_dispute_claim_updates udcu
    on udcu.user_dispute_claim_id = udct.user_dispute_claim_id
    and (udcu.GALILEO_TRANSACTION_ID = udct.TRANSACTION_ID or udcu.USER_DISPUTE_CLAIM_TRANSACTION_ID =udct.id)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY udc.id, udc.user_id, udct.transaction_id  ORDER BY udcu.created_at DESC) = 1;


-- join it with transaction table
-- R table is used to prepare for the limits presentation
with close_status as ( select 
    udc.user_id
    , udct.USER_DISPUTE_CLAIM_ID
    , udc.created_at::timestamp as dispute_created_at
    , udc.reason
    , udc.dispute_type
    , udct.transaction_id
    , udct.amount
    , udcu.disputed_amount
  , case when close_code = '' then 'close_code_null' 
        when close_code in (300, 301, 302, 303, 307, 357, 361, 363, 365) then 'approved'
        when close_code in (322, 325, 326, 356, 367, 340) then 'cancelled/withdrawn'
        when close_code in (350, 353, 354, 364, 366) then 'denied' 
        when close_code in (321, 362) then 'merchant_credit'
        when close_code in (320, 327) then 'unknown'
        else 'null' end as close_code_grp_updated    
  from fivetran.mysql_rds_disputes.user_dispute_claims udc
  join fivetran.mysql_rds_disputes.user_dispute_claim_transactions udct
    on udc.id=udct.USER_DISPUTE_CLAIM_ID
  left join fivetran.mysql_rds_disputes.user_dispute_claim_updates udcu
    on udcu.user_dispute_claim_id = udct.user_dispute_claim_id
    and (udcu.GALILEO_TRANSACTION_ID = udct.TRANSACTION_ID or udcu.USER_DISPUTE_CLAIM_TRANSACTION_ID =udct.id)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY udc.id, udc.user_id, udct.transaction_id  ORDER BY udcu.created_at DESC) = 1)
  
SELECT --R.LATEST_TIER,
TO_CHAR(DATE_TRUNC('MONTH', TRANS_DATE), 'YYYY-MM') AS MTH,
R.TYPE_OF_TRXN,
A.close_code_grp_updated,
SUM(FINAL_AMT) AS TOT_AMT,
COUNT(DISTINCT R.ID) AS NUM_TRXN,
COUNT(DISTINCT R.USER_ID) AS NUM_USERS
FROM REST.TEST.RISK_SEGMENTATION_03_31_2021 R
left join (select     
      case when transaction_code like 'AD%' then 4
          when transaction_code like 'FE%' then 5
          when transaction_code like 'IS%' then 6
          when transaction_code like 'PM%' then 7
          when transaction_code like 'SD%' then 8
          when transaction_code like 'VS%' then 9 else 0 end
      AS leading_num, -- Add the leading num
      *
      from ANALYTICS.LOOKER."TRANSACTIONS"   
      ) t on r.id = t.id 
LEFT JOIN close_status A  -- close status table needs to join by user_id and authorization code
 ON A.USER_ID=t.USER_ID
  AND TO_NUMBER(CONCAT(t.leading_num, t.AUTHORIZATION_CODE)) = TO_NUMBER(A.TRANSACTION_ID)
WHERE R.FLG_DISPUTED = 1
AND R.TRANS_DATE >= '2020-09-01'
AND R.REASON = 'unauthorized_transaction'
and type_of_trxn in ('ATM Withdrawals','Debit Purchase')
GROUP BY 1,2,3
ORDER BY 1,2,3;
