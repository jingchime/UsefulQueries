select
current_balance
MYSQL_DB.GALILEO.galileo_daily_balances
WHERE account_type = '6' 
and unique_program_id in (609, 512, 660)