SELECT USER_ID
      , MIN(socure_chime_score_final) AS socure_chime_score
      , MIN(socure_generic_score_final) AS socure_generic_score
      , MIN(socure_sigma_score_final) AS socure_sigma_score
      , MIN(socure_sigma_score_v2_final) AS socure_sigma_score_v2
FROM
      (select user_id, created_at
              , case when parse_json(result):fraud:scores[0]:name like 'Chime.3%' then parse_json(result):fraud:scores[0]:score
                     when parse_json(result):fraud:scores[1]:name like 'Chime.3%' then parse_json(result):fraud:scores[1]:score
                     when parse_json(result):fraud:scores[2]:name like 'Chime.3%' then parse_json(result):fraud:scores[2]:score
                     when parse_json(result):fraud:scores[3]:name like 'Chime.3%' then parse_json(result):fraud:scores[3]:score
                     when parse_json(result):fraud:scores[4]:name like 'Chime.3%' then parse_json(result):fraud:scores[2]:score
                     when parse_json(result):fraud:scores[5]:name like 'Chime.3%' then parse_json(result):fraud:scores[3]:score
                     else null
                  end as socure_chime_score
              , case when parse_json(result):fraud:scores[0]:name = 'generic' then parse_json(result):fraud:scores[0]:score
                     when parse_json(result):fraud:scores[1]:name = 'generic' then parse_json(result):fraud:scores[1]:score
                     when parse_json(result):fraud:scores[2]:name = 'generic' then parse_json(result):fraud:scores[2]:score
                     when parse_json(result):fraud:scores[3]:name = 'generic' then parse_json(result):fraud:scores[3]:score
                     when parse_json(result):fraud:scores[4]:name = 'generic' then parse_json(result):fraud:scores[2]:score
                     when parse_json(result):fraud:scores[5]:name = 'generic' then parse_json(result):fraud:scores[3]:score
                     else null
                  end as socure_generic_score
              , case when parse_json(result):fraud:scores[0]:name = 'sigma' and parse_json(result):fraud:scores[0]:version = '1.0' then parse_json(result):fraud:scores[0]:score 
                     when parse_json(result):fraud:scores[1]:name = 'sigma' and parse_json(result):fraud:scores[1]:version = '1.0' then parse_json(result):fraud:scores[1]:score 
                     when parse_json(result):fraud:scores[2]:name = 'sigma' and parse_json(result):fraud:scores[2]:version = '1.0' then parse_json(result):fraud:scores[2]:score
                     when parse_json(result):fraud:scores[3]:name = 'sigma' and parse_json(result):fraud:scores[3]:version = '1.0' then parse_json(result):fraud:scores[3]:score
                     when parse_json(result):fraud:scores[4]:name = 'sigma' and parse_json(result):fraud:scores[4]:version = '1.0' then parse_json(result):fraud:scores[4]:score
                     when parse_json(result):fraud:scores[5]:name = 'sigma' and parse_json(result):fraud:scores[5]:version = '1.0' then parse_json(result):fraud:scores[5]:score
                     else null
                  end as socure_sigma_score
              ,  case when parse_json(result):fraud:scores[0]:name = 'sigma' and parse_json(result):fraud:scores[0]:version = '2.0' then parse_json(result):fraud:scores[0]:score 
                     when parse_json(result):fraud:scores[1]:name = 'sigma' and parse_json(result):fraud:scores[1]:version = '2.0' then parse_json(result):fraud:scores[1]:score 
                     when parse_json(result):fraud:scores[2]:name = 'sigma' and parse_json(result):fraud:scores[2]:version = '2.0' then parse_json(result):fraud:scores[2]:score
                     when parse_json(result):fraud:scores[3]:name = 'sigma' and parse_json(result):fraud:scores[3]:version = '2.0' then parse_json(result):fraud:scores[3]:score
                     when parse_json(result):fraud:scores[4]:name = 'sigma' and parse_json(result):fraud:scores[4]:version = '2.0' then parse_json(result):fraud:scores[3]:score
                     when parse_json(result):fraud:scores[5]:name = 'sigma' and parse_json(result):fraud:scores[5]:version = '2.0' then parse_json(result):fraud:scores[3]:score
                     else null
                  end as socure_sigma_score_v2
              --pull the most recent non-NULL value
              , first_value(socure_chime_score) ignore nulls over(partition by user_id order by api.created_at desc) as socure_chime_score_final
              , first_value(socure_generic_score) ignore nulls over(partition by user_id order by api.created_at desc) as socure_generic_score_final
              , first_value(socure_sigma_score) ignore nulls over(partition by user_id order by api.created_at desc) as socure_sigma_score_final
              , first_value(socure_sigma_score_v2) ignore nulls over(partition by user_id order by api.created_at desc) as socure_sigma_score_v2_final
      FROM MYSQL_DB.CHIME_PROD.external_api_requests api
      where service='socure3'
          AND api.created_at > '2020-10-12'
          and  CHECK_JSON(result) is null --checking for valid JSON's
      )
GROUP BY 1