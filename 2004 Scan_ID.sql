WITH persona_rollout AS (
  with persona_info as (
              SELECT
              inquiry_id,
              user_id,
              context_user_agent,
              request_created_timestamp,
              request_returned_result,
              num_verifications,
              num_verifications_license,
              num_verifications_passport,
              num_verifications_selfie,
              request_returned_timestamp,
              case when second_scan = 'verification/selfie, passed' then 'pass'
                   when (second_scan = 'verification/selfie, failed' AND third_scan is null) then 'fail'
                   when (first_scan IN ('verification/driver-license, failed', 'verification/passport, failed', 'verification/national-identity-card, failed') AND second_scan is null) then 'fail'
                   when first_scan is null then 'fail'
              else (case when third_scan = 'verification/selfie, passed' then 'pass'
                         when (third_scan = 'verification/selfie, failed' AND fourth_scan is null) then 'fail'
                    else (case when fourth_scan = 'verification/selfie, passed' then 'pass'
                               when (fourth_scan = 'verification/selfie, failed' AND fifth_scan is null) then 'fail'
                          else (case when fifth_scan = 'verification/selfie, passed' then 'pass'
                                     when (fifth_scan = 'verification/selfie, failed' and sixth_scan is null) then 'fail'
                                else (case when sixth_scan = 'verification/selfie, passed' then 'pass'
                                      when (sixth_scan = 'verification/selfie, failed' AND seventh_scan is null) then 'fail'
                                      else (case when seventh_scan = 'verification/selfie, passed' then 'pass'
                                            when (seventh_scan = 'verification/selfie, failed' AND eighth_scan is null) then 'fail'
                                            else (case when eighth_scan = 'verification/selfie, passed' then 'pass'
                                                       when (eighth_scan = 'verification/selfie, failed' AND ninth_scan is null) then 'fail'
                                                  else (case when ninth_scan = 'verification/selfie, passed' then 'pass'
                                                             when (ninth_scan = 'verification/selfie, passed' AND tenth_scan is null) then 'fail'
                                                        else 'fail' end)
                                                  end)
                                            end)
                                      end)
                                end)
                          end)
                    end) end as result_summary
              FROM
              (SELECT request_created.inquiry_id AS inquiry_id,
                       request_created.user_id AS user_id,
                       request_created.context_user_agent AS context_user_agent,
                       request_created.timestamp AS request_created_timestamp,
                       request_returned.verifications AS request_returned_result,
                       concat(parse_json(verifications)[0]:verification_type,', ', parse_json(verifications)[0]:result) as first_scan,
                      concat(parse_json(verifications)[1]:verification_type,', ', parse_json(verifications)[1]:result) as second_scan,
                      concat(parse_json(verifications)[2]:verification_type,', ', parse_json(verifications)[2]:result) as third_scan,
                      concat(parse_json(verifications)[3]:verification_type,', ', parse_json(verifications)[3]:result) as fourth_scan,
                      concat(parse_json(verifications)[4]:verification_type,', ', parse_json(verifications)[4]:result) as fifth_scan,
                      concat(parse_json(verifications)[5]:verification_type,', ', parse_json(verifications)[5]:result) as sixth_scan,
                      concat(parse_json(verifications)[6]:verification_type,', ', parse_json(verifications)[6]:result) as seventh_scan,
                      concat(parse_json(verifications)[7]:verification_type,', ', parse_json(verifications)[7]:result) as eighth_scan,
                      concat(parse_json(verifications)[8]:verification_type,', ', parse_json(verifications)[8]:result) as ninth_scan,
                      concat(parse_json(verifications)[9]:verification_type,', ', parse_json(verifications)[9]:result) as tenth_scan,
                       regexp_count(request_returned.verifications, '\\bresult\\b',1) as num_verifications,
                       regexp_count(request_returned.verifications, '\\blicense\\b',1) as num_verifications_license,
                       regexp_count(request_returned.verifications, '\\bpassport\\b',1) as num_verifications_passport,
                       regexp_count(request_returned.verifications, '\\bselfie\\b',1) as num_verifications_selfie,
                       request_returned.timestamp AS request_returned_timestamp
                    FROM segment.chime_prod.persona_inquiry_request_created request_created
                    LEFT JOIN segment.chime_prod.persona_verification_request_returned request_returned
                      ON request_created.inquiry_id = request_returned.inquiry_id
                WHERE request_created.timestamp > '2020-08-01')
                )
//                select * from persona_info
//                where RESULT_SUMMARY = 'pass'
//                and user_id in (select user_id from transactions group by 1);
//                limit 100;
            , manual_review as  (SELECT
              id,
              inquiry_id,
              user_id,
              request_id,
              original_timestamp,
              decision
              FROM segment.chime_prod.persona_manual_review_applicant_decision
            WHERE original_timestamp > '2020-08-01'),
  
             hiop as (SELECT
                case when hiop.inquiry_id is not null then hiop.inquiry_id
                     when fails.inquiry_id is not null then fails.inquiry_id
                     else hiop.inquiry_id end as inquiry_id,
                generated_date_time AS hiop_review_timestamp,
                expired,
                id_version,
                id_compare,
                manual_id_review_passed_,
                confidence_score,
                case when hiop.do_you_agree_with_persona_result_ is not null then hiop.do_you_agree_with_persona_result_
                     when hiop.do_you_agree_with_persona_result_ is null then 'unknown'
                    else 'unknown' end as do_you_agree_with_persona_result_,
                case when hiop.IF_FAILED_OR_DECLINED_WITH_PERSONA_WHY_DO_YOU_AGREE_ is not null then hiop.IF_FAILED_OR_DECLINED_WITH_PERSONA_WHY_DO_YOU_AGREE_
                     when hiop.IF_FAILED_OR_DECLINED_WITH_PERSONA_WHY_DO_YOU_AGREE_ is null then 'unknown'
                    else 'unknown' end as IF_FAILED_OR_DECLINED_WITH_PERSONA_WHY_DO_YOU_AGREE_,
                selfie_review,
                case when hiop.HI_OP_PASS_FAIL is not null then hiop.HI_OP_PASS_FAIL
                     when hiop.HI_OP_PASS_FAIL is null then 'fail'
                    else 'unknown' end as HI_OP_PASS_FAIL,
                case when hiop.fail_reason is not null then fail_reason
                    when fails.WHAT_DID_PERSONA_FAIL is not null then WHAT_DID_PERSONA_FAIL
                    else 'unknown' end as fail_reason,
                DAMAGED_ID_,
                PERSONA_BUG_ISSUE_IF_YES_THEN_ADD_CONTEXT,
                concat(FAILURE_ITEM_1, FAILURE_ITEM_2, FAILURE_ITEM_3, FAILURE_ITEM_4) as failure_item,
                concat(HIOP_FINDING_1, HIOP_FINDING_2, HIOP_FINDING_3, HIOP_FINDING_4, HIOP_FINDING_5) as hiop_finding,
                NOTES,
                concat(BIAS_INDICATOR_1, BIAS_INDICATOR_2, BIAS_INDICATOR_3, BIAS_INDICATOR_4, BIAS_INDICATOR_5, BIAS_INDICATOR_6,BIAS_INDICATOR_7) as bias_indicators,
                WHAT_HI_OP_HAS_FOUND
          FROM fivetran.gsheets.chime_persona_rollout hiop
           full outer join fivetran.gsheets.persona_hioperator_fails fails
            on hiop.inquiry_id = fails.inquiry_id)
  
        SELECT persona_info.inquiry_id AS persona_inquiry_id,
                hiop.inquiry_id AS hiop_inquiry_id,
                persona_info.user_id AS user_id,
                context_user_agent,
                request_created_timestamp,
                hiop_review_timestamp,
                request_returned_timestamp,
                request_returned_result,
                case when (result_summary is not null and manual_review.decision is not null) then 'manual_review'
                     when (result_summary is null and manual_review.decision is not null) then 'manual_review'
                    when (result_summary is not null and manual_review.decision is null) then result_summary
                else null end as result_summary,  // if there is manual review then it's manual, otherwise it's result
                num_verifications,
                num_verifications_license,
                num_verifications_passport,
                num_verifications_selfie,
                expired,
                id_version,
                id_compare,
                manual_id_review_passed_,
                confidence_score,
                do_you_agree_with_persona_result_,
                IF_FAILED_OR_DECLINED_WITH_PERSONA_WHY_DO_YOU_AGREE_,
                selfie_review,
                HI_OP_PASS_FAIL,
                fail_reason,
                DAMAGED_ID_,
                PERSONA_BUG_ISSUE_IF_YES_THEN_ADD_CONTEXT,
                failure_item,
                hiop_finding,
                NOTES,
                bias_indicators,
                WHAT_HI_OP_HAS_FOUND,
                manual_review.original_timestamp AS manual_review_timestamp,
                manual_review.id AS manual_review_id,
                manual_review.decision AS manual_review_decision
          FROM persona_info
          LEFT JOIN manual_review
            ON persona_info.inquiry_id = manual_review.inquiry_id
          LEFT JOIN hiop
            ON persona_info.inquiry_id = hiop.inquiry_id
          WHERE persona_inquiry_id IS NOT NULL
      )
      
select
    user_id,
    request_created_timestamp,
    result_summary
from persona_rollout;