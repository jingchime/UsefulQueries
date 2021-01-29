# Instant transfer queries from looker dashboard

## All time users
```
WITH instant_transfers AS (select id, user_id, transaction_code, transaction_timestamp, transaction_amount
          from ANALYTICS.LOOKER."TRANSACTIONS" transactions
          where transaction_code in ('PMDB', 'PMTP', 'ADac', 'ADAS', 'ADTR', 'ADar')
          and transaction_timestamp >= '2020-07-01')
SELECT
	COUNT(DISTINCT CASE WHEN transaction_code in ('PMDB', 'PMTP') THEN  instant_transfers.id  ELSE NULL END  ) AS "instant_transfers.instant_transfers_transaction_count"
FROM CHIME.FINANCE.MEMBERS  AS members
LEFT JOIN instant_transfers ON (members."ID") = instant_transfers.user_id

WHERE
	(instant_transfers.transaction_timestamp  >= TO_TIMESTAMP('2020-10-22'))
LIMIT 500;
```

// All time volume
WITH instant_transfers AS (select id, user_id, transaction_code, transaction_timestamp, transaction_amount
          from ANALYTICS.LOOKER."TRANSACTIONS" transactions
          where transaction_code in ('PMDB', 'PMTP', 'ADac', 'ADAS', 'ADTR', 'ADar')
          and transaction_timestamp >= '2020-07-01')
SELECT
	COALESCE(SUM(CASE WHEN transaction_code in ('PMDB', 'PMTP') THEN instant_transfers.transaction_amount ELSE NULL END  ), 0) AS "instant_transfers.instant_transfers_transaction_volume"
FROM CHIME.FINANCE.MEMBERS  AS members
LEFT JOIN instant_transfers ON (members."ID") = instant_transfers.user_id

WHERE
	(instant_transfers.transaction_timestamp  >= TO_TIMESTAMP('2020-10-22'))
LIMIT 500;

// Transaction count
WITH instant_transfers AS (select id, user_id, transaction_code, transaction_timestamp, transaction_amount
          from ANALYTICS.LOOKER."TRANSACTIONS" transactions
          where transaction_code in ('PMDB', 'PMTP', 'ADac', 'ADAS', 'ADTR', 'ADar')
          and transaction_timestamp >= '2020-07-01')
SELECT
	COUNT(DISTINCT instant_transfers.id ) AS "instant_transfers.transaction_count"
FROM CHIME.FINANCE.MEMBERS  AS members
LEFT JOIN instant_transfers ON (members."ID") = instant_transfers.user_id

WHERE
	(instant_transfers.transaction_timestamp  >= TO_TIMESTAMP('2020-10-22'))
LIMIT 500;

// avg instant transfer volume
WITH instant_transfers AS (select id, user_id, transaction_code, transaction_timestamp, transaction_amount
          from ANALYTICS.LOOKER."TRANSACTIONS" transactions
          where transaction_code in ('PMDB', 'PMTP', 'ADac', 'ADAS', 'ADTR', 'ADar')
          and transaction_timestamp >= '2020-07-01')
SELECT
	AVG(CASE WHEN transaction_code in ('PMDB', 'PMTP') THEN instant_transfers.transaction_amount ELSE NULL END  ) AS "instant_transfers.avg_instant_transfers_transaction_volume"
FROM CHIME.FINANCE.MEMBERS  AS members
LEFT JOIN instant_transfers ON (members."ID") = instant_transfers.user_id

WHERE
	(instant_transfers.transaction_timestamp  >= TO_TIMESTAMP('2020-10-22'))
LIMIT 500;

// loss rate: (${instant_transfers.instant_transfers_chargeback_volume} - ${instant_transfers.instant_transfers_representment_volume})/ ${instant_transfers.instant_transfers_transaction_volume}
WITH instant_transfers AS (select id, user_id, transaction_code, transaction_timestamp, transaction_amount
          from ANALYTICS.LOOKER."TRANSACTIONS" transactions
          where transaction_code in ('PMDB', 'PMTP', 'ADac', 'ADAS', 'ADTR', 'ADar')
          and transaction_timestamp >= '2020-07-01')
SELECT
	COALESCE(SUM(CASE WHEN transaction_code in ( 'ADac', 'ADAS', 'ADTR') THEN instant_transfers.transaction_amount*-1 ELSE NULL END  ), 0) AS "instant_transfers.instant_transfers_chargeback_volume",
	COALESCE(SUM(CASE WHEN transaction_code in ('PMDB', 'PMTP') THEN instant_transfers.transaction_amount ELSE NULL END  ), 0) AS "instant_transfers.instant_transfers_transaction_volume",
	COALESCE(SUM(CASE WHEN transaction_code in ( 'ADar') THEN instant_transfers.transaction_amount ELSE NULL END  ), 0) AS "instant_transfers.instant_transfers_representment_volume"
FROM CHIME.FINANCE.MEMBERS  AS members
LEFT JOIN instant_transfers ON (members."ID") = instant_transfers.user_id

WHERE
	(instant_transfers.transaction_timestamp  >= TO_TIMESTAMP('2020-10-22'))
LIMIT 500;

// charge back rate: ${instant_transfers.instant_transfers_chargeback_count}/${instant_transfers.instant_transfers_transaction_count}
WITH instant_transfers AS (select id, user_id, transaction_code, transaction_timestamp, transaction_amount
          from ANALYTICS.LOOKER."TRANSACTIONS" transactions
          where transaction_code in ('PMDB', 'PMTP', 'ADac', 'ADAS', 'ADTR', 'ADar')
          and transaction_timestamp >= '2020-07-01')
SELECT
	COUNT(DISTINCT CASE WHEN transaction_code in ( 'ADac', 'ADAS', 'ADTR') THEN  instant_transfers.id  ELSE NULL END  ) AS "instant_transfers.instant_transfers_chargeback_count",
	COUNT(DISTINCT CASE WHEN transaction_code in ('PMDB', 'PMTP') THEN  instant_transfers.id  ELSE NULL END  ) AS "instant_transfers.instant_transfers_transaction_count"
FROM CHIME.FINANCE.MEMBERS  AS members
LEFT JOIN instant_transfers ON (members."ID") = instant_transfers.user_id

WHERE
	(instant_transfers.transaction_timestamp  >= TO_TIMESTAMP('2020-10-22'))
LIMIT 500;

// adoption rate: ${instant_transfers.user_count}/${member_acquisition_facts.enrolled_member_count}
WITH instant_transfers AS (select id, user_id, transaction_code, transaction_timestamp, transaction_amount
          from ANALYTICS.LOOKER."TRANSACTIONS" transactions
          where transaction_code in ('PMDB', 'PMTP', 'ADac', 'ADAS', 'ADTR', 'ADar')
          and transaction_timestamp >= '2020-07-01')
SELECT
	COUNT(DISTINCT (CASE WHEN instant_transfers.transaction_code in ('PMTP', 'PMDB') THEN instant_transfers.user_id else NULL END) ) AS "instant_transfers.user_count",
	COUNT(DISTINCT (member_acquisition_facts."USER_ID") ) AS "member_acquisition_facts.enrolled_member_count"
FROM CHIME.FINANCE.MEMBERS  AS members
LEFT JOIN CHIME.FINANCE.MEMBER_ACQUISITION_FACTS  AS member_acquisition_facts ON (members."ID") = (member_acquisition_facts."USER_ID")
LEFT JOIN instant_transfers ON (members."ID") = instant_transfers.user_id

WHERE
	(member_acquisition_facts."ENROLLMENT_TIME"  >= TO_TIMESTAMP('2020-10-22'))
LIMIT 500;






