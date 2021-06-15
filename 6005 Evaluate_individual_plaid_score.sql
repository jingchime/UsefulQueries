-- Plaid testing May-Nov 2019
select min(DATE), max(DATE) from FIVETRAN.CSV.CHIME_PLAID_ACH;
-- Plaid score - Apr 2020 - Mar 2021
select min(TIMESTAMP), max(TIMESTAMP) from segment.chime_prod.ACH_FRAUD_RISK_EVALUATE;

-- old
--SCORES_FUNDING_SCORE
--SCORES_UNAUTHORIZED_SCORE 
--SCORES_ACCOUNT_ISSUE_SCORE 

-- new
--SCORES_BANK_IDENTIFIED_SCORE
--SCORES_CONSUMER_IDENTIFIED_SCORE

--unknown
--SCORES_CLIENT_IDENTIFIED_SCORE

--signals
--SCORES_FUNDING_SIGNALS
--SCORES_UNAUTHORIZED_SIGNALS
--SCORES_ACCOUNT_ISSUE_SIGNALS

-- summary statistics
select 
to_date(date_trunc(month,ach_transfer.CREATED_AT)) as trn_month,
count(ach_transfer.id) as num_ach_trnx,
count(distinct aa.user_id) as num_ach_users,
sum(ach_transfer.AMOUNT) as total_ach_volume,
sum(case when ach_transfer."STATUS" = 'failed' then ach_transfer.AMOUNT else 0 end) as total_returned_volume,
count(O.SCORES_FUNDING_SCORE),
count(O.SCORES_UNAUTHORIZED_SCORE),
count(O.SCORES_ACCOUNT_ISSUE_SCORE), 
count(O.SCORES_BANK_IDENTIFIED_SCORE),
count(O.SCORES_CONSUMER_IDENTIFIED_SCORE),
count(O.SCORES_CLIENT_IDENTIFIED_SCORE),

count(distinct O.SCORES_FUNDING_SCORE),
count(distinct O.SCORES_UNAUTHORIZED_SCORE),
count(distinct O.SCORES_ACCOUNT_ISSUE_SCORE), 
count(distinct O.SCORES_BANK_IDENTIFIED_SCORE),
count(distinct O.SCORES_CONSUMER_IDENTIFIED_SCORE),
count(distinct O.SCORES_CLIENT_IDENTIFIED_SCORE)

from "MYSQL_DB"."CHIME_PROD"."ACH_TRANSFERS" ach_transfer
join mysql_db.chime_prod.ach_accounts aa on ach_transfer.ach_account_id = aa.id
left join segment.chime_prod.ACH_FRAUD_RISK_EVALUATE O on ach_transfer.id = O.ACH_TRANSFER_ID
where ach_transfer."TYPE" = 'debit' AND ach_transfer."STATUS" in ('failed','processed') and ach_transfer.CREATED_AT between '2020-05-01' and '2021-02-28'
group by 1
order by 1;

-- 1_1 evaluating SCORES_FUNDING_SCORE performance
select 
(CASE
    when O.SCORES_FUNDING_SCORE is null then 'NULL'
    WHEN O.SCORES_FUNDING_SCORE<=11 THEN TO_CHAR(O.SCORES_FUNDING_SCORE, '00')
    WHEN O.SCORES_FUNDING_SCORE<=15 THEN '12 - 15'
    WHEN O.SCORES_FUNDING_SCORE<=20 THEN '16 - 20'
    WHEN O.SCORES_FUNDING_SCORE<=25 THEN '20 - 25'
    WHEN O.SCORES_FUNDING_SCORE>=26 THEN '26 +'
END) AS FS_BUCKET,
count(ach_transfer.id) as num_ach_trnx,
count(distinct aa.user_id) as num_ach_users,
sum(ach_transfer.AMOUNT) as total_ach_volume,
sum(case when ach_transfer."STATUS" = 'failed' then ach_transfer.AMOUNT else 0 end) as total_returned_volume,
sum(case when ach_transfer."STATUS" = 'failed' then 1 else 0 end) as total_returned_trnx

from "MYSQL_DB"."CHIME_PROD"."ACH_TRANSFERS" ach_transfer
join mysql_db.chime_prod.ach_accounts aa on ach_transfer.ach_account_id = aa.id
left join segment.chime_prod.ACH_FRAUD_RISK_EVALUATE O on ach_transfer.id = O.ACH_TRANSFER_ID
where ach_transfer."TYPE" = 'debit' AND ach_transfer."STATUS" in ('failed','processed') and ach_transfer.CREATED_AT between '2020-05-01' and '2021-02-28'
group by 1
order by 1;

-- 1_2 evaluating SCORES_FUNDING_SCORE performance 2021
select 
(CASE
    when O.SCORES_FUNDING_SCORE is null then 'NULL'
    WHEN O.SCORES_FUNDING_SCORE<=11 THEN TO_CHAR(O.SCORES_FUNDING_SCORE, '00')
    WHEN O.SCORES_FUNDING_SCORE<=15 THEN '12 - 15'
    WHEN O.SCORES_FUNDING_SCORE<=20 THEN '16 - 20'
    WHEN O.SCORES_FUNDING_SCORE<=25 THEN '20 - 25'
    WHEN O.SCORES_FUNDING_SCORE>=26 THEN '26 +'
END) AS FS_BUCKET,
count(ach_transfer.id) as num_ach_trnx,
count(distinct aa.user_id) as num_ach_users,
sum(ach_transfer.AMOUNT) as total_ach_volume,
sum(case when ach_transfer."STATUS" = 'failed' then ach_transfer.AMOUNT else 0 end) as total_returned_volume,
sum(case when ach_transfer."STATUS" = 'failed' then 1 else 0 end) as total_returned_trnx

from "MYSQL_DB"."CHIME_PROD"."ACH_TRANSFERS" ach_transfer
join mysql_db.chime_prod.ach_accounts aa on ach_transfer.ach_account_id = aa.id
left join segment.chime_prod.ACH_FRAUD_RISK_EVALUATE O on ach_transfer.id = O.ACH_TRANSFER_ID
where ach_transfer."TYPE" = 'debit' AND ach_transfer."STATUS" in ('failed','processed') and ach_transfer.CREATED_AT between '2021-01-01' and '2021-02-28'
group by 1
order by 1;

-- 2_1 evaluating SCORES_UNAUTHORIZED_SCORE performance
select 
(CASE
    when O.SCORES_UNAUTHORIZED_SCORE is null then 'NULL'
    WHEN O.SCORES_UNAUTHORIZED_SCORE<=11 THEN TO_CHAR(O.SCORES_UNAUTHORIZED_SCORE, '00')
    WHEN O.SCORES_UNAUTHORIZED_SCORE<=15 THEN '12 - 15'
    WHEN O.SCORES_UNAUTHORIZED_SCORE<=20 THEN '16 - 20'
    WHEN O.SCORES_UNAUTHORIZED_SCORE<=25 THEN '20 - 25'
    WHEN O.SCORES_UNAUTHORIZED_SCORE>=26 THEN '26 +'
END) AS BUCKET,
count(ach_transfer.id) as num_ach_trnx,
count(distinct aa.user_id) as num_ach_users,
sum(ach_transfer.AMOUNT) as total_ach_volume,
sum(case when ach_transfer."STATUS" = 'failed' then ach_transfer.AMOUNT else 0 end) as total_returned_volume,
sum(case when ach_transfer."STATUS" = 'failed' then 1 else 0 end) as total_returned_trnx

from "MYSQL_DB"."CHIME_PROD"."ACH_TRANSFERS" ach_transfer
join mysql_db.chime_prod.ach_accounts aa on ach_transfer.ach_account_id = aa.id
left join segment.chime_prod.ACH_FRAUD_RISK_EVALUATE O on ach_transfer.id = O.ACH_TRANSFER_ID
where ach_transfer."TYPE" = 'debit' AND ach_transfer."STATUS" in ('failed','processed') and ach_transfer.CREATED_AT between '2020-05-01' and '2021-02-28'
group by 1
order by 1;

-- 2_2 evaluating SCORES_UNAUTHORIZED_SCORE performance
select 
(CASE
    when O.SCORES_UNAUTHORIZED_SCORE is null then 'NULL'
    WHEN O.SCORES_UNAUTHORIZED_SCORE<=11 THEN TO_CHAR(O.SCORES_UNAUTHORIZED_SCORE, '00')
    WHEN O.SCORES_UNAUTHORIZED_SCORE<=15 THEN '12 - 15'
    WHEN O.SCORES_UNAUTHORIZED_SCORE<=20 THEN '16 - 20'
    WHEN O.SCORES_UNAUTHORIZED_SCORE<=25 THEN '20 - 25'
    WHEN O.SCORES_UNAUTHORIZED_SCORE>=26 THEN '26 +'
END) AS BUCKET,
count(ach_transfer.id) as num_ach_trnx,
count(distinct aa.user_id) as num_ach_users,
sum(ach_transfer.AMOUNT) as total_ach_volume,
sum(case when ach_transfer."STATUS" = 'failed' then ach_transfer.AMOUNT else 0 end) as total_returned_volume,
sum(case when ach_transfer."STATUS" = 'failed' then 1 else 0 end) as total_returned_trnx

from "MYSQL_DB"."CHIME_PROD"."ACH_TRANSFERS" ach_transfer
join mysql_db.chime_prod.ach_accounts aa on ach_transfer.ach_account_id = aa.id
left join segment.chime_prod.ACH_FRAUD_RISK_EVALUATE O on ach_transfer.id = O.ACH_TRANSFER_ID
where ach_transfer."TYPE" = 'debit' AND ach_transfer."STATUS" in ('failed','processed') and ach_transfer.CREATED_AT between '2021-01-01' and '2021-02-28'
group by 1
order by 1;

-- 3_1 evaluating SCORES_ACCOUNT_ISSUE_SCORE performance
select 
(CASE
    when O.SCORES_ACCOUNT_ISSUE_SCORE is null then 'NULL'
    WHEN O.SCORES_ACCOUNT_ISSUE_SCORE<=11 THEN TO_CHAR(O.SCORES_ACCOUNT_ISSUE_SCORE, '00')
    WHEN O.SCORES_ACCOUNT_ISSUE_SCORE<=15 THEN '12 - 15'
    WHEN O.SCORES_ACCOUNT_ISSUE_SCORE<=20 THEN '16 - 20'
    WHEN O.SCORES_ACCOUNT_ISSUE_SCORE<=25 THEN '20 - 25'
    WHEN O.SCORES_ACCOUNT_ISSUE_SCORE>=26 THEN '26 +'
END) AS BUCKET,
count(ach_transfer.id) as num_ach_trnx,
count(distinct aa.user_id) as num_ach_users,
sum(ach_transfer.AMOUNT) as total_ach_volume,
sum(case when ach_transfer."STATUS" = 'failed' then ach_transfer.AMOUNT else 0 end) as total_returned_volume,
sum(case when ach_transfer."STATUS" = 'failed' then 1 else 0 end) as total_returned_trnx

from "MYSQL_DB"."CHIME_PROD"."ACH_TRANSFERS" ach_transfer
join mysql_db.chime_prod.ach_accounts aa on ach_transfer.ach_account_id = aa.id
left join segment.chime_prod.ACH_FRAUD_RISK_EVALUATE O on ach_transfer.id = O.ACH_TRANSFER_ID
where ach_transfer."TYPE" = 'debit' AND ach_transfer."STATUS" in ('failed','processed') and ach_transfer.CREATED_AT between '2020-05-01' and '2021-02-28'
group by 1
order by 1;

-- 3_2 evaluating SCORES_ACCOUNT_ISSUE_SCORE performance 2021
select 
(CASE
    when O.SCORES_ACCOUNT_ISSUE_SCORE is null then 'NULL'
    WHEN O.SCORES_ACCOUNT_ISSUE_SCORE<=11 THEN TO_CHAR(O.SCORES_ACCOUNT_ISSUE_SCORE, '00')
    WHEN O.SCORES_ACCOUNT_ISSUE_SCORE<=15 THEN '12 - 15'
    WHEN O.SCORES_ACCOUNT_ISSUE_SCORE<=20 THEN '16 - 20'
    WHEN O.SCORES_ACCOUNT_ISSUE_SCORE<=25 THEN '20 - 25'
    WHEN O.SCORES_ACCOUNT_ISSUE_SCORE>=26 THEN '26 +'
END) AS BUCKET,
count(ach_transfer.id) as num_ach_trnx,
count(distinct aa.user_id) as num_ach_users,
sum(ach_transfer.AMOUNT) as total_ach_volume,
sum(case when ach_transfer."STATUS" = 'failed' then ach_transfer.AMOUNT else 0 end) as total_returned_volume,
sum(case when ach_transfer."STATUS" = 'failed' then 1 else 0 end) as total_returned_trnx

from "MYSQL_DB"."CHIME_PROD"."ACH_TRANSFERS" ach_transfer
join mysql_db.chime_prod.ach_accounts aa on ach_transfer.ach_account_id = aa.id
left join segment.chime_prod.ACH_FRAUD_RISK_EVALUATE O on ach_transfer.id = O.ACH_TRANSFER_ID
where ach_transfer."TYPE" = 'debit' AND ach_transfer."STATUS" in ('failed','processed') and ach_transfer.CREATED_AT between '2021-01-01' and '2021-02-28'
group by 1
order by 1;

-- 4 evaluating SCORES_BANK_IDENTIFIED_SCORE performance
select 
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
group by 1
order by 1;

-- 5 evaluating SCORES_CONSUMER_IDENTIFIED_SCORE performance
select 
(CASE
    when O.SCORES_CONSUMER_IDENTIFIED_SCORE is null then 'NULL'
    WHEN O.SCORES_CONSUMER_IDENTIFIED_SCORE<=11 THEN TO_CHAR(O.SCORES_CONSUMER_IDENTIFIED_SCORE, '00')
    WHEN O.SCORES_CONSUMER_IDENTIFIED_SCORE<=15 THEN '12 - 15'
    WHEN O.SCORES_CONSUMER_IDENTIFIED_SCORE<=20 THEN '16 - 20'
    WHEN O.SCORES_CONSUMER_IDENTIFIED_SCORE<=25 THEN '20 - 25'
    WHEN O.SCORES_CONSUMER_IDENTIFIED_SCORE>=26 THEN '26 +'
END) AS CI_BUCKET,
count(ach_transfer.id) as num_ach_trnx,
count(distinct aa.user_id) as num_ach_users,
sum(ach_transfer.AMOUNT) as total_ach_volume,
sum(case when ach_transfer."STATUS" = 'failed' then ach_transfer.AMOUNT else 0 end) as total_returned_volume,
sum(case when ach_transfer."STATUS" = 'failed' then 1 else 0 end) as total_returned_trnx

from "MYSQL_DB"."CHIME_PROD"."ACH_TRANSFERS" ach_transfer
join mysql_db.chime_prod.ach_accounts aa on ach_transfer.ach_account_id = aa.id
left join segment.chime_prod.ACH_FRAUD_RISK_EVALUATE O on ach_transfer.id = O.ACH_TRANSFER_ID
where ach_transfer."TYPE" = 'debit' AND ach_transfer."STATUS" in ('failed','processed') and ach_transfer.CREATED_AT between '2020-05-01' and '2021-02-28'
group by 1
order by 1;