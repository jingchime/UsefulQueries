-- BI + days to return
select 
(CASE
    when O.SCORES_BANK_IDENTIFIED_SCORE is null then 'NULL'
    WHEN O.SCORES_BANK_IDENTIFIED_SCORE<=11 THEN TO_CHAR(O.SCORES_BANK_IDENTIFIED_SCORE, '00')
    WHEN O.SCORES_BANK_IDENTIFIED_SCORE<=15 THEN '12 - 15'
    WHEN O.SCORES_BANK_IDENTIFIED_SCORE<=20 THEN '16 - 20'
    WHEN O.SCORES_BANK_IDENTIFIED_SCORE<=25 THEN '20 - 25'
    WHEN O.SCORES_BANK_IDENTIFIED_SCORE>=26 THEN '26 +'
END) AS BI_BUCKET,
case when ach_transfer."STATUS" = 'failed' and datediff(day, ach_transfer.CREATED_AT, ach_transfer.UPDATED_AT)<=7 then TO_CHAR(datediff(day, ach_transfer.CREATED_AT, ach_transfer.UPDATED_AT))
     when ach_transfer."STATUS" = 'failed' then '>=8'
     else 'Not returned'
end as days_take_to_return_bucket,
count(ach_transfer.id) as num_ach_trnx,
count(distinct aa.user_id) as num_ach_users,
sum(ach_transfer.AMOUNT) as total_ach_volume,
sum(case when ach_transfer."STATUS" = 'failed' then ach_transfer.AMOUNT else 0 end) as total_returned_volume,
sum(case when ach_transfer."STATUS" = 'failed' then 1 else 0 end) as total_returned_trnx

from "MYSQL_DB"."CHIME_PROD"."ACH_TRANSFERS" ach_transfer
join mysql_db.chime_prod.ach_accounts aa on ach_transfer.ach_account_id = aa.id
left join segment.chime_prod.ACH_FRAUD_RISK_EVALUATE O on ach_transfer.id = O.ACH_TRANSFER_ID
where ach_transfer."TYPE" = 'debit' AND ach_transfer."STATUS" in ('failed','processed') and ach_transfer.CREATED_AT between '2020-05-01' and '2021-02-28'
group by 1,2
order by 1,2;

-- BI + CI
select 
(CASE
    when O.SCORES_CONSUMER_IDENTIFIED_SCORE is null then 'NULL'
    WHEN O.SCORES_CONSUMER_IDENTIFIED_SCORE<=1 THEN TO_CHAR(O.SCORES_CONSUMER_IDENTIFIED_SCORE, '00')
    WHEN O.SCORES_CONSUMER_IDENTIFIED_SCORE>=2 THEN '2 +'
END) AS CI_BUCKET,
(CASE
    when O.SCORES_BANK_IDENTIFIED_SCORE is null then 'NULL'
    WHEN O.SCORES_BANK_IDENTIFIED_SCORE<=11 THEN TO_CHAR(O.SCORES_BANK_IDENTIFIED_SCORE, '00')
    WHEN O.SCORES_BANK_IDENTIFIED_SCORE<=15 THEN '12 - 15'
    WHEN O.SCORES_BANK_IDENTIFIED_SCORE<=20 THEN '16 - 20'
    WHEN O.SCORES_BANK_IDENTIFIED_SCORE<=25 THEN '20 - 25'
    WHEN O.SCORES_BANK_IDENTIFIED_SCORE>=26 THEN '26 +'
END) AS BI_BUCKET,
count(ach_transfer.id) as num_ach_trnx,
count(distinct aa.user_id) as num_ach_users,
sum(ach_transfer.AMOUNT) as total_ach_volume,
sum(case when ach_transfer."STATUS" = 'failed' then ach_transfer.AMOUNT else 0 end) as total_returned_volume,
sum(case when ach_transfer."STATUS" = 'failed' then 1 else 0 end) as total_returned_trnx

from "MYSQL_DB"."CHIME_PROD"."ACH_TRANSFERS" ach_transfer
join mysql_db.chime_prod.ach_accounts aa on ach_transfer.ach_account_id = aa.id
left join segment.chime_prod.ACH_FRAUD_RISK_EVALUATE O on ach_transfer.id = O.ACH_TRANSFER_ID
where ach_transfer."TYPE" = 'debit' AND ach_transfer."STATUS" in ('failed','processed') and ach_transfer.CREATED_AT between '2020-05-01' and '2021-02-28'
group by 1,2
order by 1,2;

-- BI + CI + days to return
select 
(CASE
    when O.SCORES_CONSUMER_IDENTIFIED_SCORE is null then 'NULL'
    WHEN O.SCORES_CONSUMER_IDENTIFIED_SCORE<=1 THEN TO_CHAR(O.SCORES_CONSUMER_IDENTIFIED_SCORE, '00')
    WHEN O.SCORES_CONSUMER_IDENTIFIED_SCORE>=2 THEN '2 +'
END) AS CI_BUCKET,
(CASE
    when O.SCORES_BANK_IDENTIFIED_SCORE is null then 'NULL'
    WHEN O.SCORES_BANK_IDENTIFIED_SCORE<=11 THEN TO_CHAR(O.SCORES_BANK_IDENTIFIED_SCORE, '00')
    WHEN O.SCORES_BANK_IDENTIFIED_SCORE<=15 THEN '12 - 15'
    WHEN O.SCORES_BANK_IDENTIFIED_SCORE<=20 THEN '16 - 20'
    WHEN O.SCORES_BANK_IDENTIFIED_SCORE<=25 THEN '20 - 25'
    WHEN O.SCORES_BANK_IDENTIFIED_SCORE>=26 THEN '26 +'
END) AS BI_BUCKET,
case when ach_transfer."STATUS" = 'failed' and datediff(day, ach_transfer.CREATED_AT, ach_transfer.UPDATED_AT)<=7 then TO_CHAR(datediff(day, ach_transfer.CREATED_AT, ach_transfer.UPDATED_AT))
     when ach_transfer."STATUS" = 'failed' then '>=8'
     else 'Not returned'
end as days_take_to_return_bucket,
count(ach_transfer.id) as num_ach_trnx,
count(distinct aa.user_id) as num_ach_users,
sum(ach_transfer.AMOUNT) as total_ach_volume,
sum(case when ach_transfer."STATUS" = 'failed' then ach_transfer.AMOUNT else 0 end) as total_returned_volume,
sum(case when ach_transfer."STATUS" = 'failed' then 1 else 0 end) as total_returned_trnx

from "MYSQL_DB"."CHIME_PROD"."ACH_TRANSFERS" ach_transfer
join mysql_db.chime_prod.ach_accounts aa on ach_transfer.ach_account_id = aa.id
left join segment.chime_prod.ACH_FRAUD_RISK_EVALUATE O on ach_transfer.id = O.ACH_TRANSFER_ID
where ach_transfer."TYPE" = 'debit' AND ach_transfer."STATUS" in ('failed','processed') and ach_transfer.CREATED_AT between '2020-05-01' and '2021-02-28'
group by 1,2,3
order by 1,2,3;