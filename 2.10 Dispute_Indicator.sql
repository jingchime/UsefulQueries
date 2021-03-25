SELECT
 t.*,
 CASE WHEN dispute.TRANSACTION_ID IS NOT NULL THEN 1 ELSE 0 END AS FLG_DISPUTED,
from (select 	  
      case when transaction_code like 'AD%' then 4
	        when transaction_code like 'FE%' then 5
	        when transaction_code like 'IS%' then 6
	        when transaction_code like 'PM%' then 7
	        when transaction_code like 'SD%' then 8
	        when transaction_code like 'VS%' then 9 else 0 end
      AS leading_num, -- Add the leading num
      *
      from ANALYTICS.LOOKER."TRANSACTIONS"   
      ) t
LEFT JOIN (SELECT DISTINCT A.USER_ID, B.TRANSACTION_ID, A.REASON
           FROM ANALYTICS.LOOKER.USER_DISPUTE_CLAIMS AS A
           LEFT JOIN ANALYTICS.LOOKER.USER_DISPUTE_CLAIM_TRANSACTIONS AS B
           ON A.ID=B.USER_DISPUTE_CLAIM_ID) AS dispute
ON t.USER_ID=dispute.USER_ID
AND TO_NUMBER(CONCAT(t.leading_num, t.AUTHORIZATION_CODE)) = TO_NUMBER(dispute.TRANSACTION_ID)