------------------------------------------------------------------------
-- Three individual queries for 6003------------------------------------
------------------------------------------------------------------------
-- volume distribution
select 
case when datediff(day, ach_transfer.CREATED_AT, ach_transfer.UPDATED_AT)<=7 then TO_CHAR(datediff(day, ach_transfer.CREATED_AT, ach_transfer.UPDATED_AT))
     else '>=8'
end as days_take_to_return_bucket,
sum(AMOUNT) as failed_amount
from "MYSQL_DB"."CHIME_PROD"."ACH_TRANSFERS" ach_transfer
join mysql_db.chime_prod.ach_accounts aa on ach_transfer.ach_account_id = aa.id
where ach_transfer."TYPE" = 'debit' AND ach_transfer."STATUS" in ('failed','processed') and ach_transfer.CREATED_AT between '2020-10-01' and '2020-12-31'
group by 1
order by 1;

-- failed distribution
select 
case when datediff(day, ach_transfer.CREATED_AT, ach_transfer.UPDATED_AT)<=7 then TO_CHAR(datediff(day, ach_transfer.CREATED_AT, ach_transfer.UPDATED_AT))
     else '>=8'
end as days_take_to_return_bucket,
sum(AMOUNT) as failed_amount
from "MYSQL_DB"."CHIME_PROD"."ACH_TRANSFERS" ach_transfer
join mysql_db.chime_prod.ach_accounts aa on ach_transfer.ach_account_id = aa.id
where ach_transfer."TYPE" = 'debit' AND ach_transfer."STATUS" = 'failed' and ach_transfer.CREATED_AT between '2020-10-01' and '2020-12-31'
group by 1
order by 1;

-- returned distribution
WITH ach_returns AS (
  SELECT DISTINCT 
        id,
        user_id, 
        TRAN_ID AS return_auth_code,
        try_to_number(num_ar.value) as orig_auth_code,
        description,
        type,
        amount,
        balance,
        timestamp AS return_timestamp,
        otype
    from 
        MYSQL_DB.CHIME_PROD.alert_transaction_events a,
        lateral split_to_table(regexp_replace(description, '[^0-9]', ''), '') as num_ar
    where 
        id NOT in (177893361) --this alert has a huge typo in it that skews the numbers
        and otype in ('a')
        AND act_type = 'AD'
        AND description ILIKE '%ACH return%')      
 SELECT 
  case when datediff(day, T.CREATED_AT, T2.TRANSACTION_TIMESTAMP) is null then 'NULL'
       when datediff(day, T.CREATED_AT, T2.TRANSACTION_TIMESTAMP)<=7 then to_char(datediff(day, T.CREATED_AT, T2.TRANSACTION_TIMESTAMP))
       else '>=8'
  end as days_take_to_return,
  -sum(T2.TRANSACTION_AMOUNT) AS returned_amount
  FROM ach_returns R
  LEFT JOIN (
    (SELECT g.ID AS TRXN_ID, g.USER_ID, g.TRANSACTION_CODE, g.TRANSACTION_AMOUNT, g.TRANSACTION_TIMESTAMP, g.AUTHORIZATION_CODE,
      act.id, 
      trf.SUBMITTED_AT, trf.GALILEO_HOLD_DAYS, trf.created_at, trf.EXTERNAL_DEBIT_CONFIRMED_AT 
    FROM ANALYTICS.LOOKER."TRANSACTIONS" g
    JOIN MYSQL_DB.CHIME_PROD.ach_accounts act ON g.user_id = act.user_id 
    JOIN MYSQL_DB.CHIME_PROD.ach_transfers trf 
    ON g.authorization_code = trf.payment_id AND trf.ach_account_id = act.id
    WHERE g.TRANSACTION_CODE = 'PMAC' AND trf."TYPE" = 'debit')) T
      ON T.USER_ID = R.USER_ID AND T.AUTHORIZATION_CODE = R.orig_auth_code
    RIGHT JOIN (SELECT * 
          FROM ANALYTICS.LOOKER."TRANSACTIONS" 
          WHERE TRANSACTION_CODE IN ('ADa', 'ADA')) T2
      ON T2.USER_ID = R.USER_ID AND T2.AUTHORIZATION_CODE = R.return_auth_code
  WHERE T2.TRANSACTION_TIMESTAMP > '2020-10-01' AND T2.TRANSACTION_TIMESTAMP < '2020-12-31'
  group by 1
  order by 1;
  
------------------------------------------------------------------------
-- Combine and join all three together----------------------------------
-- Add user_profile to segment the population---------------------------
------------------------------------------------------------------------
-- user_profile.IF_DDER   
-- user_profile.IF_SIGMA_GREATER_THAN_0.9
-- user_profile.IF_PREVIOUS_ACH

select 
part_1.days_take_to_return_bucket,
part_1.total_amount,
part_2.failed_amount,
part_3.returned_amount
from 
    (select 
    case when datediff(day, ach_transfer.CREATED_AT, ach_transfer.UPDATED_AT)<=7 then TO_CHAR(datediff(day, ach_transfer.CREATED_AT, ach_transfer.UPDATED_AT))
         else '>=8'
    end as days_take_to_return_bucket,
    sum(AMOUNT) as total_amount
    from "MYSQL_DB"."CHIME_PROD"."ACH_TRANSFERS" ach_transfer
    join mysql_db.chime_prod.ach_accounts aa on ach_transfer.ach_account_id = aa.id
    left join REST.TEST.User_Profile_Tracker_06_02_2021 user_profile on aa.user_id = user_profile.user_id and TO_DATE(DATE_TRUNC('DAY',ach_transfer.CREATED_AT)) = user_profile.DATE     
    where user_profile.IF_DDER=1 and ach_transfer."TYPE" = 'debit' AND ach_transfer."STATUS" in ('failed','processed') and ach_transfer.CREATED_AT between '2020-10-01' and '2020-12-31'
    group by 1
    order by 1) part_1
full join
    (select 
    case when datediff(day, ach_transfer.CREATED_AT, ach_transfer.UPDATED_AT)<=7 then TO_CHAR(datediff(day, ach_transfer.CREATED_AT, ach_transfer.UPDATED_AT))
         else '>=8'
    end as days_take_to_return_bucket,
    sum(AMOUNT) as failed_amount
    from "MYSQL_DB"."CHIME_PROD"."ACH_TRANSFERS" ach_transfer
    join mysql_db.chime_prod.ach_accounts aa on ach_transfer.ach_account_id = aa.id
    left join REST.TEST.User_Profile_Tracker_06_02_2021 user_profile on aa.user_id = user_profile.user_id and TO_DATE(DATE_TRUNC('DAY',ach_transfer.CREATED_AT)) = user_profile.DATE 
    where user_profile.IF_DDER=1 and ach_transfer."TYPE" = 'debit' AND ach_transfer."STATUS" = 'failed' and ach_transfer.CREATED_AT between '2020-10-01' and '2020-12-31'
    group by 1
    order by 1) part_2
    on part_1.days_take_to_return_bucket = part_2.days_take_to_return_bucket
full join
    (WITH ach_returns AS (
      SELECT DISTINCT 
            id,
            user_id, 
            TRAN_ID AS return_auth_code,
            try_to_number(num_ar.value) as orig_auth_code,
            description,
            type,
            amount,
            balance,
            timestamp AS return_timestamp,
            otype
        from 
            MYSQL_DB.CHIME_PROD.alert_transaction_events a,
            lateral split_to_table(regexp_replace(description, '[^0-9]', ''), '') as num_ar
        where 
            id NOT in (177893361) --this alert has a huge typo in it that skews the numbers
            and otype in ('a')
            AND act_type = 'AD'
            AND description ILIKE '%ACH return%')      
     SELECT 
      case when datediff(day, T.CREATED_AT, T2.TRANSACTION_TIMESTAMP) is null then 'NULL'
           when datediff(day, T.CREATED_AT, T2.TRANSACTION_TIMESTAMP)<=7 then to_char(datediff(day, T.CREATED_AT, T2.TRANSACTION_TIMESTAMP))
           else '>=8'
      end as days_take_to_return,
      -sum(T2.TRANSACTION_AMOUNT) AS returned_amount
      FROM ach_returns R
      LEFT JOIN (
        (SELECT g.ID AS TRXN_ID, g.USER_ID, g.TRANSACTION_CODE, g.TRANSACTION_AMOUNT, g.TRANSACTION_TIMESTAMP, g.AUTHORIZATION_CODE,
          act.id, 
          trf.SUBMITTED_AT, trf.GALILEO_HOLD_DAYS, trf.created_at, trf.EXTERNAL_DEBIT_CONFIRMED_AT 
        FROM ANALYTICS.LOOKER."TRANSACTIONS" g
        JOIN MYSQL_DB.CHIME_PROD.ach_accounts act ON g.user_id = act.user_id 
        JOIN MYSQL_DB.CHIME_PROD.ach_transfers trf 
        ON g.authorization_code = trf.payment_id AND trf.ach_account_id = act.id
        WHERE g.TRANSACTION_CODE = 'PMAC' AND trf."TYPE" = 'debit')) T
          ON T.USER_ID = R.USER_ID AND T.AUTHORIZATION_CODE = R.orig_auth_code
        left join REST.TEST.User_Profile_Tracker_06_02_2021 user_profile on T.user_id = user_profile.user_id and TO_DATE(DATE_TRUNC('DAY',T.TRANSACTION_TIMESTAMP)) = user_profile.DATE  
        RIGHT JOIN (SELECT * 
              FROM ANALYTICS.LOOKER."TRANSACTIONS" 
              WHERE TRANSACTION_CODE IN ('ADa', 'ADA')) T2
          ON T2.USER_ID = R.USER_ID AND T2.AUTHORIZATION_CODE = R.return_auth_code
      WHERE user_profile.IF_DDER=1 and T2.TRANSACTION_TIMESTAMP between '2020-10-01' and '2020-12-31'
      group by 1
      order by 1) part_3
 on part_1.days_take_to_return_bucket = part_3.days_take_to_return
order by part_1.days_take_to_return_bucket;

------------------------------------------------------------------------
-- user_profile table---------------------------------------------------
------------------------------------------------------------------------
CREATE OR REPLACE TABLE REST.TEST.User_Profile_Tracker_06_02_2021 AS
(
-- built daily table on if previously made ACH transfer
  with tmp1 as (
  select 
  aa.user_id,
  TO_DATE(DATE_TRUNC('DAY',min(ach_transfer.CREATED_AT))) AS FIRST_ACH_DATE
  from "MYSQL_DB"."CHIME_PROD"."ACH_TRANSFERS" ach_transfer
  join mysql_db.chime_prod.ach_accounts aa on ach_transfer.ach_account_id = aa.id
  where ach_transfer."TYPE" = 'debit' AND ach_transfer."STATUS" = 'processed' 
  group by 1),

-- leverage segmentation table to get daily user profile on if_dd/sigma score bucket
 tmp2 as(
   SELECT 
    USER_ID,
    MIN(CASE WHEN DD_USER IS NOT NULL THEN TO_DATE(DATE_TRUNC('DAY',TRANS_DATE)) END) AS DD_DATE,
    MAX(CASE WHEN SOCURE_SIGMA_SCORE>=0.9 THEN 1 ELSE 0 END) AS "IF_SIGMA_GREATER_THAN_0.9"
    FROM "REST"."TEST"."RISK_SEGMENTATION_05_19_2021"
    GROUP BY 1)
  
   select 
   tmp.*, 
   case when tmp.DATE > tmp1.FIRST_ACH_DATE then 1 else 0 end as if_previous_ach
   from
    (select
    date_series.DATE,
    tmp2.user_id,
    case when date_series.DATE >= tmp2.DD_DATE then 1 else 0 end as if_DDer,
    tmp2."IF_SIGMA_GREATER_THAN_0.9"
    from 
      (select distinct TO_DATE(DATE_TRUNC('DAY',ach_transfer.CREATED_AT)) AS DATE
      from "MYSQL_DB"."CHIME_PROD"."ACH_TRANSFERS" ach_transfer order by 1) date_series
      cross join tmp2
      where date_series.DATE between '2020-10-01' and '2020-12-31'
    group by 1,2,3,4
     ) tmp
    left join tmp1
    on tmp.user_id = tmp1.user_id
);


------------------------------------------------------------------------
-- for data QA only-----------------------------------------------------
------------------------------------------------------------------------
select 
aa.user_id,
TO_DATE(DATE_TRUNC('DAY',ach_transfer.CREATED_AT)),
user_profile.user_id
from "MYSQL_DB"."CHIME_PROD"."ACH_TRANSFERS" ach_transfer
join mysql_db.chime_prod.ach_accounts aa on ach_transfer.ach_account_id = aa.id
left join REST.TEST.User_Profile_Tracker_06_02_2021 user_profile on aa.user_id = user_profile.user_id and TO_DATE(DATE_TRUNC('DAY',ach_transfer.CREATED_AT)) = user_profile.DATE     
where user_profile."IF_SIGMA_GREATER_THAN_0.9" IS NULL and ach_transfer."TYPE" = 'debit' AND ach_transfer."STATUS" in ('failed','processed') and ach_transfer.CREATED_AT between '2020-10-01' and '2020-12-31'
limit 100;

-- 19739745
-- 6650600
-- 17840543 -- no information except failed ACH
-- 20282379 -- no information except failed ACH -- so volume cannot be assigned to either 1 or 0 group
select user_id, count(*) from REST.TEST.User_Profile_Tracker_06_02_2021 group by 1 order by 2 desc;

select * from REST.TEST.User_Profile_Tracker_06_02_2021 where user_id = 20282379 order by DATE;

select * FROM "REST"."TEST"."RISK_SEGMENTATION_05_19_2021" where user_id = 20282379 order by TRANS_DATE; 

select 
TO_DATE(DATE_TRUNC('DAY',ach_transfer.CREATED_AT)), ach_transfer.*
from "MYSQL_DB"."CHIME_PROD"."ACH_TRANSFERS" ach_transfer
join mysql_db.chime_prod.ach_accounts aa on ach_transfer.ach_account_id = aa.id
where user_id = 20282379
order by 1;

 