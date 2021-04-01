-- final
select
users.id,
case when users.status = 'suspended' then 1
     when users.status = 'cancelled' and adr_cancellation.reason <> 'member' then 1
     when users.status = 'cancelled_no_refund' and adr_cancellation.reason <> 'member' then 1
     else 0
end as if_suspended_or_cancelled,
users.status,
adr_cancellation.reason
FROM "MYSQL_DB"."CHIME_PROD"."USERS" users
LEFT JOIN "MYSQL_DB"."CHIME_PROD"."USER_ACCOUNT_DEACTIVATIONS" AS uad_cancellation
ON users.id = uad_cancellation.user_id
AND uad_cancellation.account_deactivation_reason_type = 'AccountCancellationReason'
LEFT JOIN "MYSQL_DB"."CHIME_PROD"."ACCOUNT_DEACTIVATION_REASONS" AS adr_cancellation
ON uad_cancellation.account_deactivation_reason_id = adr_cancellation.id
limit 1000;

-- check distribution
select
case when users.status = 'suspended' then 1
     when users.status = 'cancelled' and adr_cancellation.reason <> 'member' then 1
     when users.status = 'cancelled_no_refund' and adr_cancellation.reason <> 'member' then 1
     else 0
end as if_suspended_or_cancelled,
count(a.id)
FROM "MYSQL_DB"."CHIME_PROD"."REALTIME_AUTH_EVENTS" a
LEFT JOIN  "MYSQL_DB"."CHIME_PROD"."USERS" users
on a.user_id = users.id
LEFT JOIN "MYSQL_DB"."CHIME_PROD"."USER_ACCOUNT_DEACTIVATIONS" AS uad_cancellation
ON users.id = uad_cancellation.user_id
AND uad_cancellation.account_deactivation_reason_type = 'AccountCancellationReason'
LEFT JOIN "MYSQL_DB"."CHIME_PROD"."ACCOUNT_DEACTIVATION_REASONS" AS adr_cancellation
ON uad_cancellation.account_deactivation_reason_id = adr_cancellation.id
group by 1;

-- ref 1: Total number of users with ach accounts by user status
SELECT status
, (CASE WHEN ach.user_id IS NULL THEN 0 ELSE 1 END) AS plaid_linked_flag
, COUNT(DISTINCT users.id)
FROM "MYSQL_DB"."CHIME_PROD"."USERS" users
LEFT JOIN (SELECT DISTINCT acc.user_id
    FROM "MYSQL_DB"."CHIME_PROD"."ACH_ACCOUNTS" acc
    WHERE acc.is_deleted = 0 AND acc.status = 'verified'
    AND acc.bank_name <> 'Stride') ach
ON users.id = ach.user_id
GROUP BY 1, 2
ORDER BY 1, 2;

--- ref2:Why cancelled ??
SELECT plaid_linked_flag, status, reason, label, COUNT(DISTINCT id)
FROM (
    SELECT users.id, status
    , (CASE WHEN ach.user_id IS NULL THEN 0 ELSE 1 END) AS plaid_linked_flag
    --, uad_cancellation.account_deacdtivation_reason_type
    , adr_cancellation.reason
    , adr_cancellation.label
    FROM "MYSQL_DB"."CHIME_PROD"."USERS" users
    LEFT JOIN (SELECT DISTINCT acc.user_id
        FROM "MYSQL_DB"."CHIME_PROD"."ACH_ACCOUNTS" acc
        WHERE acc.is_deleted = 0 AND acc.status = 'verified'
        AND acc.bank_name <> 'Stride') ach
    ON users.id = ach.user_id
    LEFT JOIN "MYSQL_DB"."CHIME_PROD"."USER_ACCOUNT_DEACTIVATIONS" AS uad_cancellation
    ON users.id = uad_cancellation.user_id
    AND uad_cancellation.account_deactivation_reason_type = 'AccountCancellationReason'
    LEFT JOIN "MYSQL_DB"."CHIME_PROD"."ACCOUNT_DEACTIVATION_REASONS" AS adr_cancellation
    ON uad_cancellation.account_deactivation_reason_id = adr_cancellation.id
    WHERE users.status IN ('cancelled', 'cancelled_no_refund'))
GROUP BY 1, 2, 3, 4;


