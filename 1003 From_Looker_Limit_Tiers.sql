// Active members
-- raw sql results do not include filled-in values for 'limit_tier_performance_debit_credit_atm.start_week_month'
WITH limit_tier_performance_debit_credit_atm AS (select * from analytics.test.limit_tier_performance_debit_credit_atm)

SELECT * FROM (
        SELECT *, DENSE_RANK() OVER (ORDER BY z___min_rank) as z___pivot_row_rank, RANK() OVER (PARTITION BY z__pivot_col_rank ORDER BY z___min_rank) as z__pivot_col_ordering, CASE WHEN z___min_rank = z___rank THEN 1 ELSE 0 END AS z__is_highest_ranked_cell 
        FROM (
            SELECT *, MIN(z___rank) OVER (PARTITION BY "limit_tier_performance_debit_credit_atm.start_week_month") as z___min_rank 
            FROM (
                SELECT *, RANK() OVER (ORDER BY "limit_tier_performance_debit_credit_atm.start_week_month" DESC, z__pivot_col_rank) AS z___rank 
                FROM (
                    SELECT *, DENSE_RANK() OVER (ORDER BY CASE WHEN "limit_tier_performance_debit_credit_atm.limit_tier" IS NULL THEN 1 ELSE 0 END, "limit_tier_performance_debit_credit_atm.limit_tier") AS z__pivot_col_rank 
                    FROM (
                        SELECT
                            limit_tier_performance_debit_credit_atm."ADMIN_REAL_LIMIT"  AS "limit_tier_performance_debit_credit_atm.limit_tier",
                            TO_CHAR(DATE_TRUNC('month', limit_tier_performance_debit_credit_atm."START_WEEK" ), 'YYYY-MM') AS "limit_tier_performance_debit_credit_atm.start_week_month",
                            count(distinct limit_tier_performance_debit_credit_atm."USER_ID") AS "limit_tier_performance_debit_credit_atm.total_users"
                        FROM limit_tier_performance_debit_credit_atm

                        WHERE
                            (((limit_tier_performance_debit_credit_atm."START_WEEK" ) >= ((TO_DATE(DATEADD('month', -5, DATE_TRUNC('month', CURRENT_DATE()))))) AND (limit_tier_performance_debit_credit_atm."START_WEEK" ) < ((TO_DATE(DATEADD('month', 6, DATEADD('month', -5, DATE_TRUNC('month', CURRENT_DATE()))))))))
                        GROUP BY 1,DATE_TRUNC('month', limit_tier_performance_debit_credit_atm."START_WEEK" )) ww  
                    ) bb WHERE z__pivot_col_rank <= 16384
            ) aa
      ) xx
) zz
 WHERE (z__pivot_col_rank <= 50 OR z__is_highest_ranked_cell = 1) AND (z___pivot_row_rank <= 500 OR z__pivot_col_ordering = 1) ORDER BY z___pivot_row_rank;
 
 // Transaction count
 -- raw sql results do not include filled-in values for 'limit_tier_performance_debit_credit_atm.start_week_month'
WITH limit_tier_performance_debit_credit_atm AS (select * from analytics.test.limit_tier_performance_debit_credit_atm)
SELECT * FROM (
SELECT *, DENSE_RANK() OVER (ORDER BY z___min_rank) as z___pivot_row_rank, RANK() OVER (PARTITION BY z__pivot_col_rank ORDER BY z___min_rank) as z__pivot_col_ordering, CASE WHEN z___min_rank = z___rank THEN 1 ELSE 0 END AS z__is_highest_ranked_cell FROM (
SELECT *, MIN(z___rank) OVER (PARTITION BY "limit_tier_performance_debit_credit_atm.start_week_month") as z___min_rank FROM (
SELECT *, RANK() OVER (ORDER BY "limit_tier_performance_debit_credit_atm.start_week_month" DESC, z__pivot_col_rank) AS z___rank FROM (
SELECT *, DENSE_RANK() OVER (ORDER BY CASE WHEN "limit_tier_performance_debit_credit_atm.limit_tier" IS NULL THEN 1 ELSE 0 END, "limit_tier_performance_debit_credit_atm.limit_tier") AS z__pivot_col_rank FROM (
SELECT
	limit_tier_performance_debit_credit_atm."ADMIN_REAL_LIMIT"  AS "limit_tier_performance_debit_credit_atm.limit_tier",
	TO_CHAR(DATE_TRUNC('month', limit_tier_performance_debit_credit_atm."START_WEEK" ), 'YYYY-MM') AS "limit_tier_performance_debit_credit_atm.start_week_month",
	count(limit_tier_performance_debit_credit_atm."TRANS_DATE")  AS "limit_tier_performance_debit_credit_atm.total_txn_count"
FROM limit_tier_performance_debit_credit_atm

WHERE
	(((limit_tier_performance_debit_credit_atm."START_WEEK" ) >= ((TO_DATE(DATEADD('month', -5, DATE_TRUNC('month', CURRENT_DATE()))))) AND (limit_tier_performance_debit_credit_atm."START_WEEK" ) < ((TO_DATE(DATEADD('month', 6, DATEADD('month', -5, DATE_TRUNC('month', CURRENT_DATE()))))))))
GROUP BY 1,DATE_TRUNC('month', limit_tier_performance_debit_credit_atm."START_WEEK" )) ww
) bb WHERE z__pivot_col_rank <= 16384
) aa
) xx
) zz
 WHERE (z__pivot_col_rank <= 50 OR z__is_highest_ranked_cell = 1) AND (z___pivot_row_rank <= 500 OR z__pivot_col_ordering = 1) ORDER BY z___pivot_row_rank;
 
 //Transaction dollars
 -- raw sql results do not include filled-in values for 'limit_tier_performance_debit_credit_atm.start_week_month'
WITH limit_tier_performance_debit_credit_atm AS (select * from analytics.test.limit_tier_performance_debit_credit_atm)
SELECT * FROM (
SELECT *, DENSE_RANK() OVER (ORDER BY z___min_rank) as z___pivot_row_rank, RANK() OVER (PARTITION BY z__pivot_col_rank ORDER BY z___min_rank) as z__pivot_col_ordering, CASE WHEN z___min_rank = z___rank THEN 1 ELSE 0 END AS z__is_highest_ranked_cell FROM (
SELECT *, MIN(z___rank) OVER (PARTITION BY "limit_tier_performance_debit_credit_atm.start_week_month") as z___min_rank FROM (
SELECT *, RANK() OVER (ORDER BY "limit_tier_performance_debit_credit_atm.start_week_month" DESC, z__pivot_col_rank) AS z___rank FROM (
SELECT *, DENSE_RANK() OVER (ORDER BY CASE WHEN "limit_tier_performance_debit_credit_atm.limit_tier" IS NULL THEN 1 ELSE 0 END, "limit_tier_performance_debit_credit_atm.limit_tier") AS z__pivot_col_rank FROM (
SELECT
	limit_tier_performance_debit_credit_atm."ADMIN_REAL_LIMIT"  AS "limit_tier_performance_debit_credit_atm.limit_tier",
	TO_CHAR(DATE_TRUNC('month', limit_tier_performance_debit_credit_atm."START_WEEK" ), 'YYYY-MM') AS "limit_tier_performance_debit_credit_atm.start_week_month",
	sum(limit_tier_performance_debit_credit_atm."FINAL_AMT")  AS "limit_tier_performance_debit_credit_atm.total_txn_amount"
FROM limit_tier_performance_debit_credit_atm

WHERE
	(((limit_tier_performance_debit_credit_atm."START_WEEK" ) >= ((TO_DATE(DATEADD('month', -5, DATE_TRUNC('month', CURRENT_DATE()))))) AND (limit_tier_performance_debit_credit_atm."START_WEEK" ) < ((TO_DATE(DATEADD('month', 6, DATEADD('month', -5, DATE_TRUNC('month', CURRENT_DATE()))))))))
GROUP BY 1,DATE_TRUNC('month', limit_tier_performance_debit_credit_atm."START_WEEK" )) ww
) bb WHERE z__pivot_col_rank <= 16384
) aa
) xx
) zz
 WHERE (z__pivot_col_rank <= 50 OR z__is_highest_ranked_cell = 1) AND (z___pivot_row_rank <= 500 OR z__pivot_col_ordering = 1) ORDER BY z___pivot_row_rank;
 
 //active members
 -- raw sql results do not include filled-in values for 'limit_tier_performance_debit_credit_atm.start_week_month'
WITH limit_tier_performance_debit_credit_atm AS (select * from analytics.test.limit_tier_performance_debit_credit_atm)
SELECT * FROM (
SELECT *, DENSE_RANK() OVER (ORDER BY z___min_rank) as z___pivot_row_rank, RANK() OVER (PARTITION BY z__pivot_col_rank ORDER BY z___min_rank) as z__pivot_col_ordering, CASE WHEN z___min_rank = z___rank THEN 1 ELSE 0 END AS z__is_highest_ranked_cell FROM (
SELECT *, MIN(z___rank) OVER (PARTITION BY "limit_tier_performance_debit_credit_atm.start_week_month") as z___min_rank FROM (
SELECT *, RANK() OVER (ORDER BY "limit_tier_performance_debit_credit_atm.start_week_month" DESC, z__pivot_col_rank) AS z___rank FROM (
SELECT *, DENSE_RANK() OVER (ORDER BY CASE WHEN "limit_tier_performance_debit_credit_atm.limit_tier" IS NULL THEN 1 ELSE 0 END, "limit_tier_performance_debit_credit_atm.limit_tier") AS z__pivot_col_rank FROM (
SELECT
	limit_tier_performance_debit_credit_atm."ADMIN_REAL_LIMIT"  AS "limit_tier_performance_debit_credit_atm.limit_tier",
	TO_CHAR(DATE_TRUNC('month', limit_tier_performance_debit_credit_atm."START_WEEK" ), 'YYYY-MM') AS "limit_tier_performance_debit_credit_atm.start_week_month",
	count(distinct limit_tier_performance_debit_credit_atm."USER_ID") AS "limit_tier_performance_debit_credit_atm.total_users"
FROM limit_tier_performance_debit_credit_atm

WHERE
	(((limit_tier_performance_debit_credit_atm."START_WEEK" ) >= ((TO_DATE(DATEADD('month', -5, DATE_TRUNC('month', CURRENT_DATE()))))) AND (limit_tier_performance_debit_credit_atm."START_WEEK" ) < ((TO_DATE(DATEADD('month', 6, DATEADD('month', -5, DATE_TRUNC('month', CURRENT_DATE()))))))))
GROUP BY 1,DATE_TRUNC('month', limit_tier_performance_debit_credit_atm."START_WEEK" )) ww
) bb WHERE z__pivot_col_rank <= 16384
) aa
) xx
) zz
 WHERE (z__pivot_col_rank <= 50 OR z__is_highest_ranked_cell = 1) AND (z___pivot_row_rank <= 500 OR z__pivot_col_ordering = 1) ORDER BY z___pivot_row_rank;
 
 // transaction count
 -- raw sql results do not include filled-in values for 'limit_tier_performance_debit_credit_atm.start_week_month'
WITH limit_tier_performance_debit_credit_atm AS (select * from analytics.test.limit_tier_performance_debit_credit_atm)
SELECT * FROM (
SELECT *, DENSE_RANK() OVER (ORDER BY z___min_rank) as z___pivot_row_rank, RANK() OVER (PARTITION BY z__pivot_col_rank ORDER BY z___min_rank) as z__pivot_col_ordering, CASE WHEN z___min_rank = z___rank THEN 1 ELSE 0 END AS z__is_highest_ranked_cell FROM (
SELECT *, MIN(z___rank) OVER (PARTITION BY "limit_tier_performance_debit_credit_atm.start_week_month") as z___min_rank FROM (
SELECT *, RANK() OVER (ORDER BY "limit_tier_performance_debit_credit_atm.start_week_month" DESC, z__pivot_col_rank) AS z___rank FROM (
SELECT *, DENSE_RANK() OVER (ORDER BY CASE WHEN "limit_tier_performance_debit_credit_atm.limit_tier" IS NULL THEN 1 ELSE 0 END, "limit_tier_performance_debit_credit_atm.limit_tier") AS z__pivot_col_rank FROM (
SELECT
	limit_tier_performance_debit_credit_atm."ADMIN_REAL_LIMIT"  AS "limit_tier_performance_debit_credit_atm.limit_tier",
	TO_CHAR(DATE_TRUNC('month', limit_tier_performance_debit_credit_atm."START_WEEK" ), 'YYYY-MM') AS "limit_tier_performance_debit_credit_atm.start_week_month",
	count(limit_tier_performance_debit_credit_atm."TRANS_DATE")  AS "limit_tier_performance_debit_credit_atm.total_txn_count"
FROM limit_tier_performance_debit_credit_atm

WHERE
	(((limit_tier_performance_debit_credit_atm."START_WEEK" ) >= ((TO_DATE(DATEADD('month', -5, DATE_TRUNC('month', CURRENT_DATE()))))) AND (limit_tier_performance_debit_credit_atm."START_WEEK" ) < ((TO_DATE(DATEADD('month', 6, DATEADD('month', -5, DATE_TRUNC('month', CURRENT_DATE()))))))))
GROUP BY 1,DATE_TRUNC('month', limit_tier_performance_debit_credit_atm."START_WEEK" )) ww
) bb WHERE z__pivot_col_rank <= 16384
) aa
) xx
) zz
 WHERE (z__pivot_col_rank <= 50 OR z__is_highest_ranked_cell = 1) AND (z___pivot_row_rank <= 500 OR z__pivot_col_ordering = 1) ORDER BY z___pivot_row_rank;
 
 // transaction dollars
 -- raw sql results do not include filled-in values for 'limit_tier_performance_debit_credit_atm.start_week_month'
WITH limit_tier_performance_debit_credit_atm AS (select * from analytics.test.limit_tier_performance_debit_credit_atm)
SELECT * FROM (
SELECT *, DENSE_RANK() OVER (ORDER BY z___min_rank) as z___pivot_row_rank, RANK() OVER (PARTITION BY z__pivot_col_rank ORDER BY z___min_rank) as z__pivot_col_ordering, CASE WHEN z___min_rank = z___rank THEN 1 ELSE 0 END AS z__is_highest_ranked_cell FROM (
SELECT *, MIN(z___rank) OVER (PARTITION BY "limit_tier_performance_debit_credit_atm.start_week_month") as z___min_rank FROM (
SELECT *, RANK() OVER (ORDER BY "limit_tier_performance_debit_credit_atm.start_week_month" DESC, z__pivot_col_rank) AS z___rank FROM (
SELECT *, DENSE_RANK() OVER (ORDER BY CASE WHEN "limit_tier_performance_debit_credit_atm.limit_tier" IS NULL THEN 1 ELSE 0 END, "limit_tier_performance_debit_credit_atm.limit_tier") AS z__pivot_col_rank FROM (
SELECT
	limit_tier_performance_debit_credit_atm."ADMIN_REAL_LIMIT"  AS "limit_tier_performance_debit_credit_atm.limit_tier",
	TO_CHAR(DATE_TRUNC('month', limit_tier_performance_debit_credit_atm."START_WEEK" ), 'YYYY-MM') AS "limit_tier_performance_debit_credit_atm.start_week_month",
	sum(limit_tier_performance_debit_credit_atm."FINAL_AMT")  AS "limit_tier_performance_debit_credit_atm.total_txn_amount"
FROM limit_tier_performance_debit_credit_atm

WHERE
	(((limit_tier_performance_debit_credit_atm."START_WEEK" ) >= ((TO_DATE(DATEADD('month', -5, DATE_TRUNC('month', CURRENT_DATE()))))) AND (limit_tier_performance_debit_credit_atm."START_WEEK" ) < ((TO_DATE(DATEADD('month', 6, DATEADD('month', -5, DATE_TRUNC('month', CURRENT_DATE()))))))))
GROUP BY 1,DATE_TRUNC('month', limit_tier_performance_debit_credit_atm."START_WEEK" )) ww
) bb WHERE z__pivot_col_rank <= 16384
) aa
) xx
) zz
 WHERE (z__pivot_col_rank <= 50 OR z__is_highest_ranked_cell = 1) AND (z___pivot_row_rank <= 500 OR z__pivot_col_ordering = 1) ORDER BY z___pivot_row_rank;
 
 
 