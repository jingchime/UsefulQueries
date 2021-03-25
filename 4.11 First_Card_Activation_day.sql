--three tables are used
-- 2018/11/21
select * from "MYSQL_DB"."GALILEO"."GALILEO_ACCOUNT_CARDS"
where user_id = 2178430
order by CREATED_AT;

-- 2018/11/27
select 
*
FROM SEGMENT.CHIME_PROD.CARD_ACTIVATED
WHERE user_id = 2178430;

-- 2018/11/27
SELECT
*
FROM mysql_db.chime_prod.ACCOUNT_EVENTS
WHERE user_id = 2178430;

-- compare 7 methods based on three tables for 13 users:
select
method_1.user_id,
date_trunc(day,method_1.card_activated_at)::DATE as method_1_day,
date_trunc(day,method_2.card_activated_at)::DATE as method_2_day,
date_trunc(day,method_3.card_activated_at)::DATE as method_3_day,
date_trunc(day,method_4.card_activated_at)::DATE as method_4_day,
date_trunc(day,method_5.card_activated_at)::DATE as method_5_day,
date_trunc(day,method_6.card_activated_at)::DATE as method_6_day,
date_trunc(day,method_7.card_activated_at)::DATE as method_7_day
from
(SELECT
user_id,
min(CASE WHEN type = 'card_activated' THEN CREATED_AT END)::timestamp AS card_activated_at
FROM mysql_db.chime_prod.ACCOUNT_EVENTS
GROUP BY 1) method_1
left join
(SELECT
user_id,
min(CASE WHEN STATUS = 'N' THEN CREATED_AT END)::timestamp AS card_activated_at
FROM "MYSQL_DB"."GALILEO"."GALILEO_ACCOUNT_CARDS"
GROUP BY 1
) method_2
on method_1.user_id = method_2.user_id
left join 
(SELECT
user_id,
min(CREATED_AT)::timestamp AS card_activated_at 
FROM "MYSQL_DB"."GALILEO"."GALILEO_ACCOUNT_CARDS"
GROUP BY 1
) method_3
on method_1.user_id = method_3.user_id
left join
(select 
USER_ID,
min(ORIGINAL_TIMESTAMP) as card_activated_at
FROM SEGMENT.CHIME_PROD.CARD_ACTIVATED
WHERE FIRST_CARD = True
group by 1
) method_4
on method_1.user_id = method_4.user_id
left join
(select 
USER_ID,
min(ORIGINAL_TIMESTAMP) as card_activated_at
FROM SEGMENT.CHIME_PROD.CARD_ACTIVATED
group by 1
) method_5
on method_1.user_id = method_5.user_id
left join
(SELECT
user_id,
min(CASE WHEN STATUS in ('N', 'S', 'L') THEN CREATED_AT END)::timestamp AS card_activated_at
FROM "MYSQL_DB"."GALILEO"."GALILEO_ACCOUNT_CARDS"
GROUP BY 1
) method_6
on method_1.user_id = method_6.user_id
left join
(SELECT
user_id,
min(CASE WHEN type in ('card_activated','card_status_change') THEN CREATED_AT END)::timestamp AS card_activated_at
FROM mysql_db.chime_prod.ACCOUNT_EVENTS
GROUP BY 1
) method_7
on method_1.user_id = method_7.user_id
where method_1.user_id in
(2178430,
 3336306,
10108129,
 3561129,
 3279873,
 239070,
 7383340,
 1420051,
 196086,
16447636,
2046525,
450793,
15230817,
1420051,
7383340);