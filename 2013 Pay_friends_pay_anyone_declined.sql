--PF/PA declined transactions due to ML or rules
with perv_pf_pa_decline as (

select sender_id, sent_at, amount, error
from "SEGMENT"."CHIME_PROD"."PAY_FRIENDS_ERROR"
where error like '%account%'

union

SELECT pf.sender_id,
 TO_TIMESTAMP(pf.created_at) AS pf_timestamp,
  i.amount,
 i.model_version as error
FROM mysql_db.chime_prod.pay_friends pf
JOIN SEGMENT.ML_INFERENCE_LOG_PROD.INFERENCE_INVOKED i ON pf.id::string = i.inference_id::string
JOIN "SEGMENT"."CHIME_PROD"."PAY_FRIENDS_ML_FRAUD_CHECK" mlfc ON mlfc.pay_friend_id::string =pf.id::string
LEFT JOIN mysql_db.chime_prod.user_contact_transfers uct on uct.id = pf.user_contact_transfer_id
WHERE type_code='to_member' AND
mlfc.decision = 'reject' AND
mlfc.model_decision = 'reject' AND
mlfc.reason= 'model_decision'
)

select * from perv_pf_pa_decline
where sender_id = 557697