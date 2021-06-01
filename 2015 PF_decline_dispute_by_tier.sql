select 
TRXN_MONTH,
tier, 
sum(DISPUTED_AMOUNT)DISPUTED_AMOUNT, 
sum(amount)amount, 
sum(LIMIT_EXCEED_FLAG)exceeded_limit, 
count(USER_ID)no_of_members 
from(
  select a.*, case when no_of_exceeded_limits>0 then 1 else 0 end as limit_exceed_flag 
      from(select user_id, 
           last_day(TRANS_DATE)trxn_month, 
           sum(case when FLG_DISPUTED=1 then FINAL_AMT else 0 end)disputed_amount, 
           sum(FINAL_AMT)amount, 
           max(tier_no)tier from(
              select a.*, TRANSACTION_CODE, substr(ADMIN_REAL_LIMIT,5)tier_no
              from Rest.test.risk_segmentation_05_19_2021 a
              join MYSQL_DB.GALILEO.GALILEO_POSTED_TRANSACTIONS b
              on a.user_id=b.user_id
              and a.id=b.id
              and a.TRANS_DATE=TRANSACTION_TIMESTAMP
              where TRANSACTION_CODE in ('ADM', 'ADTS', 'ADTU', 'ADPF', 'ADpb'))
           group by 1,2)a
  left join(select USER_ID, last_day(TIMESTAMP)dt, count(*)no_of_exceeded_limits from SEGMENT.CHIME_PROD.PAY_FRIENDS_ERROR
  where (error like('%limit%') or error like('%$2,000%'))--and CONTEXT_LIBRARY_NAME='analytics-ruby' 
  group by 1,2 )b
  on a.user_id=b.user_id
  and a.trxn_month=b.dt)
group by 1,2