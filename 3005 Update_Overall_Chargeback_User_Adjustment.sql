----refresh overall monitoring using adjustment table
// update total volume and users  
WITH instant_transfers AS (select CREATED_AT, id, USER_ID, AMOUNT
    from  "MYSQL_DB"."CHIME_PROD"."USER_ADJUSTMENTS" 
    where TYPE = 'external_card_transfer'
    and CREATED_AT >= '2020-11-01') 
            
SELECT DATE_TRUNC('week', CREATED_AT)::DATE AS ORIGINAL_TXN_month,  
    COUNT(DISTINCT instant_transfers.USER_ID) AS "total_users", 
    SUM(AMOUNT) AS "transaction_volume"
FROM instant_transfers  
LEFT JOIN "ANALYTICS"."LOOKER"."MEMBER_PARTNER_BANK" bank 
on instant_transfers.user_id = bank.user_id
WHERE CREATED_AT  >= TO_TIMESTAMP('2020-11-1') --and bank.PRIMARY_PROGRAM_ASSIGNED = 'stride'
GROUP BY 1  
ORDER BY 1; 

// update cash charge back weekly 
WITH instant_transfers AS (select CREATED_AT, id, USER_ID, AMOUNT
    from  "MYSQL_DB"."CHIME_PROD"."USER_ADJUSTMENTS" 
    where TYPE = 'external_card_chargeback'
    and CREATED_AT >= '2020-11-01') 
            
SELECT DATE_TRUNC('week', CREATED_AT)::DATE AS ORIGINAL_TXN_month,  
    SUM(AMOUNT) AS  total_chargebacks
FROM instant_transfers  
LEFT JOIN "ANALYTICS"."LOOKER"."MEMBER_PARTNER_BANK" bank 
on instant_transfers.user_id = bank.user_id
WHERE CREATED_AT >= TO_TIMESTAMP('2020-11-1')
--and bank.PRIMARY_PROGRAM_ASSIGNED = 'stride' 
GROUP BY 1  
ORDER BY 1; 
  
//update cohort charge back weekly  
WITH instant_transfers AS (select CREATED_AT, id, USER_ID, AMOUNT
      from  "MYSQL_DB"."CHIME_PROD"."USER_ADJUSTMENTS" 
      where TYPE = 'external_card_transfer'
      and CREATED_AT >= '2020-11-01') 
            
SELECT DATE_TRUNC('week', CREATED_AT)::DATE AS ORIGINAL_TXN_week, 
    COUNT(DISTINCT instant_transfers.user_id) AS chargeback_users,  
    SUM(amount) AS chargeback_volume
FROM instant_transfers  
LEFT JOIN "ANALYTICS"."LOOKER"."MEMBER_PARTNER_BANK" bank 
on instant_transfers.user_id = bank.user_id
WHERE CREATED_AT >= TO_TIMESTAMP('2020-11-1')
      --and bank.PRIMARY_PROGRAM_ASSIGNED = 'stride' 
       and instant_transfers.user_id in (select distinct user_id  
            from  "MYSQL_DB"."CHIME_PROD"."USER_ADJUSTMENTS" 
            where TYPE = 'external_card_chargeback')  
GROUP BY 1  
ORDER BY 1; 