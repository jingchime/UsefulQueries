-- transaction
select * FROM ANALYTICS.LOOKER."TRANSACTIONS"
where user_id = 2090532;

-- galileo
select * from mysql_db.galileo.galileo_authorized_transactions 
where user_id = 2090532
order by TRANSACTION_TIMESTAMP;

-- daily balance
select *
FROM mysql_db.galileo.GALILEO_DAILY_BALANCES
where user_id = 2090532;

-- both 51 and 61
select a.user_id
FROM
(select distinct user_id from mysql_db.galileo.galileo_authorized_transactions WHERE AUTHORIZATION_RESPONSE = 61 AND TRANSACTION_TIMESTAMP >= '2020-02-01') a 
JOIN 
(select distinct user_id from mysql_db.galileo.galileo_authorized_transactions WHERE AUTHORIZATION_RESPONSE = 51 AND TRANSACTION_TIMESTAMP >= '2020-02-01') b
on a.user_id = b.user_id limit 100;