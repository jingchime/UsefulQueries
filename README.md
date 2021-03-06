# Queries Backup
Starting from 0, gradually learning the query logic used at Chime\
[tricks learned](https://docs.google.com/document/d/1Z5V8FhfCzu4Ve260wrZRJjorSxIZ-2uxfUdKmTBVnFk/edit)

## From Looker Dashboard
1001 From_Looker_Fraud_And_Loss.sql\
1002 From_Looker_Instant_Transfer.sql\
1003 From_Looker_Limit_Tiers.sql\
1004 From_Looker_ATO.sql: this is the logic to know which transaction is ATO. It is shared by Bin extracted from the [dashboard](https://chime.looker.com/dashboards-next/1349)

## From teammembers
2001 Seven_Scores.sql: Stuti shared all scores used at Chime. Modified on Jul-19 to address new KYC table\
2002 Dispute_Transaction_Close_Status: Hemali shared how to pull most recent status of dispute. Updated it with Rakesh and Shu based on several versions\
2003 User_Update_Phone.sql: from data-ask channel get how to check if user update their phone or email\
2004 Scan_ID.sql: from Karan get if the user went through scan ID and pass/fail/manual review\
2005 Limit_Dispute_Rate_By_Buckets.sql: Shu shared the query for decline rate, dispute rate calculation by different group. Note that everything is grouped together. So need to comment or uncomment to change the logic if needed.\
2006 Recent_balance.sql: Bin shared the logic with me to pull recent balance\
2007 Member_Partner_Bank.sql: Meeri shared the logic to pull stride/bancorp so I can finish the request on ALYTMM-75\
2008 If_DD.sql: Stuti shared the logic to determine if someone is DD or not\
2009 Transaction_Type.sql: Bin shared the logic to get the transaction type from ANALYTICS.LOOKER."TRANSACTIONS" \
2010 Dispute_Indicator.sql: Shu shared the logic to tag the dispute\
2011 Real_time_available_funds.sql: Shu shared the logic, built to support analysis for [disputes capped by limits](https://docs.google.com/spreadsheets/d/1HtIoJrf7BqJE6URUcX6YL4vc4eTILmg4sSrQfD2FjPU/edit?ts=606519e4#gid=0)<br>
2012 Simulate_Tiers_Across_Time.sql: Shu's logic to build the table of simulated tiers for analysis\
2013 Pay_friends_pay_anyone_declined.sql: Urvi's logic to get the declined PA PF timestamp\
2014 ACH_transfer_ato.sql: Nik shared the query to identify the ato in ACH transfers and evaluate the rule impact\
2015 PF_decline_dispute_by_tier.sql: Urvi used the query to generate PF decline and dispute rate by tiers\
2016 Outbound_PA_instant_transfer_monitoring.sql: Urvi used the query to monitor the outbound test performance\
2017 New_DDer_Definition.sql: Meeri shared the logic to update DD definition 

## Instant Transfer
3001 Instant_Transfer_Main_Tables.sql: key tables for updating and analyzing instant transfer\
3002 Update_Weekly_IT_Meeting.sql: used to update weekly monitoring the blocked banks\
3003 Update_Chargeback_by_Bank.sql: used to monitor the chargeback by bank to give alerts\
3004 Instant_Transfer_Chargeback_Weekly.sql: used to update overall instant transfer chargebacks (both cash and cohort) by week\
3005 Update_Overall_Chargeback_User_Adjustment.sql: refresh the monitoring logic using user_adjustment table instead of GPT table as GPT missed those who suspended\
3006 Evaluate_instant_transfer_risk_controls_effectiveness.sql: evaluated the performance of risk controls using [confusion matrix](https://docs.google.com/spreadsheets/d/1GbQUbFnQ8CozVso2p2TVF9AQRTw6bL8B1jbi_DQAdFY/edit#gid=1396155568)

## Ad-hoc queries
4001 Phone_number_change_cumulative_tracker.sql: cumulatively track phone number change in the last 32 days\
4002 Available_Balance_Distribution.sql: researched the distribution to set up the right cutoff\
4003 Response_code_51_61_analysis.sql: check if one transaction can have both 51 and 61 code\
4004 Alibaba_Analysis.sql: supported the ad-hoc analysis for [Alibaba](https://docs.google.com/document/d/1Z_De49MtG56AdRa_FE4lgHpyoRW6uQ7diKMNwkB-wuQ/edit) <br/>
4005 Days_take_for_instant_transfer_chargeback.sql: supported the follow-up analysis after the weekly meeting [days for instant transfer to chargeback](https://docs.google.com/document/d/1R6riAlQeNHjZ8aRbDdk2tnZWvIxLlfvBA2sgtpXYjok/edit) <br/>
4006 Chargebacks_tie_back_to_transaction_date.sql: supported the request here [instant transfer chargebacks](https://docs.google.com/spreadsheets/d/1kFga0lzeyIQ_OrLvXrjeXuCRi04FRnxvBk4DhMOTgWM/edit?ts=60494835#gid=438205306) <br/>
4007 Deep_dive_into_Feb_Mar_chargebacks.sql: looked into chargebacks related to Feb and Mar transactions and see if there is pattern on the bank or socure scores there. Found that missing scores are allowed instant transfers\
4008 RI465_Alibaba_Block_Analysis.sql: validate the [RI465 decision](https://chime.atlassian.net/browse/RI-465) with the [document](https://docs.google.com/document/d/1kyB5wIrEy1mhivQ74xbEH40xXej8wZpQVOJcrF8GnG8/edit#) <br/> 
4009 Distribution_of_days_from_enrollment_to_transfer.sql: ad-hoc analysis supporting the data backfill requirement of at least 60 days\
4010 Compare_enrollment_score_with_chime_score.sql: compare enrollment score with chime score for instant transfer users (they are the same) and check the missing rate\
4011 First_Card_Activation_day.sql: 7 methods used to get the first card activation day for the analysis[Transactions prior to card activation](https://docs.google.com/document/d/1tKEuecSFfQo4jtn5ZCOmXhXVkFaBFMkXLqVIKiP8V3E/edit#) <br/>
4012 DD_Amount_Tracker.sql: cumulatively track total DD amount in the last 1,3,6 months\
4013 Debit_Purchase_Amount_Tracker.sql: cumulatively track total debit purchase amount in the last 1,3,6 months\
4014 Virtual_Card_Transactions.sql: used for the virtual card transaction analysis [doc](https://docs.google.com/document/d/1Hcpq5Aqgc7H6Bu9IeU7xdisCySX5qHez1MrewomBzs0/edit#heading=h.75r11l61c0on) <br/>
4015 Member_Status.sql: find the members who are suspended or cancelled (not member initiated) and exclude them from analysis\
4016 Validate_VISA_Chargebacks.sql: Used to compare our GPT result with Andrew's result and identified the reason why there is a discrepancy: suspended accounts don't have chargebacks. Solution: user_adjustment\
4017 Suspended_re_enablement_analysis.sql: this code is used to calculate the [requests for tier 0 analysis](https://docs.google.com/document/d/19e1BmM8em2lohaMSad_E2PS3RVP3q_bAP-xABC1mFcw/edit) <br/>
4018 Representment_analysis.sql: documented the codes behind the hex file comparing representment between Tabapay and user_adjustment [Hex analysis](https://chime.hex.tech/global/hex/9f04aefd-98d9-4fbd-8ac2-4193f3353eb4/draft/logic/47d1dedb-feec-4700-b293-3adcacb787c4) <br/>
4019 Followup_4017_7th_code_to_suspend.sql: follow up on 4017 tier 0 analysis to add a new code (previously didn't suspend) to suspend those who transfer >= 7500 PMVT in the previous 30 days. Size the impact\
4020 Compare_dispute_rate_of_transactions_within_24h_instant_transfer_with_portfolio.sql: compared the debit purchases dispute rates of transactions made within 24 hours after incoming instant transfers with the portfolio. [gsheet](https://docs.google.com/spreadsheets/d/1m5Scz9lT68RbgLVBmgT3IRwXSpFoj9KGLK8XVCZN_k4/edit#gid=343122903) <br/>
4021 Tabapay_AVS_declines_deep_dive.sql: checked the performance of tabapay avs declines. results in local folder 29. shared version is [gsheet](https://docs.google.com/spreadsheets/d/16Rda5cMBciMBybeRU4m7EY49Pj0djj_i3BFbFaqc4Pk/edit#gid=1015229486)<br/>
4022 Investigate_Sev_367_pav2_bug_loss_impact.sql: query used to track the 1.3M loss from sev 367 bug. [loss file](https://docs.google.com/spreadsheets/d/1XeOf-u13yvPj5WdT0t1Oc0Kadb4NZSLNSLb0RK_JTbE/edit?ts=60de28df#gid=597101487)


## Limits analysis
5001 Limits_Presentation.sql: documented the queries I used to update page 10 and 15 of the [presentaion](https://docs.google.com/presentation/d/1FovHs6LSREvmq-a0UVUwwIP77z2ocbWe2YlS7CBsjg4/edit#slide=id.g62bd80da81_0_529) <br/>
5002 Limits_Feature_Research_Analysis.sql: documented the queries used for feature analysis [feature analysis](https://docs.google.com/document/d/1Dcj97uDBjBn-N6Ybf0YCMyWhvETEX3c7jJKSEkw4BmU/edit#heading=h.5ws9etdhs4mi) with all the intermediate temp table queries. Results summarized in the slide [P0 analysis](https://docs.google.com/presentation/d/1tsdNx17c4Ta5AZaCaVK6xpC01LGoH0toyEQr-xhgAeA/edit#slide=id.gc4f7623fb2_0_0) <br/>
5003 Dispute_Rate_Before_After_Upgrade.sql: Stuti used the logic to calculate the dispute rate before and after the upgrade from tier 2 to tier 3. Also modified a little bit for tier 3 to tier 4. [dispute_rate_before_after_tier_upgrade](https://docs.google.com/spreadsheets/d/1K9YfpAQv8JiPUmaA4p3yonBm0gyy8BmlX45decIvwJ8/edit#gid=404191872) <br/>
5004 Walmart Cash Back Analysis.sql: This is the code supporting [Walmart analysis](https://docs.google.com/document/d/1I156jt55ggvJbp2MuNnvLTjxWKCk1_S4McjGhLUuGsk/edit) <br/>
5005 Avg_transaction_size_before_after_tier_change.sql: used to update the user level spend change before and after tier change. [excel](https://docs.google.com/spreadsheets/d/1K9YfpAQv8JiPUmaA4p3yonBm0gyy8BmlX45decIvwJ8/edit#gid=102292978&fvid=1813606055) [slides](https://docs.google.com/presentation/d/1GI4Vq0qCERBDJPipcybyVrIZ7axtHaT5oi62K_3ikVY/edit#slide=id.gd45a0a7a43_0_11) <br/>

## ACH analysis
6001 ACH_Pull_Days_Distribution.sql: get the distribution of proceed/return ACH pull transfers. The excel file is [Excel data](https://docs.google.com/spreadsheets/d/1d3c-1-1ftpTCODh4Azn4SaD6wyXD5Hp1HZ--xIJ2rbs/edit#gid=1794551769) and the document is [doc](https://docs.google.com/document/d/1780t_rL93RK0Ro9o-G1IpI8dsKko1dqeI5t0sClMT-s/edit) <br/>
6002 ACH_transfer_ATO_add_flag_declined_papf.sql: support Nik's analysis on [RI-448](https://docs.google.com/document/d/1JjCgrURwWfr8Q8XpT_6HMX9ZnXr07aH6lG8aOgBrK30/edit?ts=609d6280). Added previous declined timestamp as flag to Nik's original [query](https://chime.looker.com/sql/2kmhhzdsjtrbby?toggle=dat,sql) <br/>
6003 ACH_Fail_and_return.sql: the code is used for calculating [ACH fail/return distribution](https://docs.google.com/spreadsheets/d/1skk_P7Wl42DomvesMshDhGgAm-VT-zc89_SRo-vWYaw/edit#gid=478155585) <br/>
6004 ACH_fail_and_return_followup.sql: follow up the 6003 analysis by adding the segmentation to the ACH returns" [analysis](https://docs.google.com/document/d/1tIrtNzjw_NPbDgFoWI7nixJWp2nxuSk4JQdmXhd6vTY/edit)<br/>
6005 Evaluate_individual_plaid_score.sql: evaluate the performance of plaid scores in predicting ACH returns. [gsheet](https://docs.google.com/spreadsheets/d/1r62ByeNoaKuZD8b0bpGomXQl64W5_M_T30Ns1rIif54/edit#gid=1768211984) <br/>
6006 Deep_dive_BI_CI_days_to_return.sql: deep dive into BI score by evaluating the possibility of reducing hold time. [gsheet](https://docs.google.com/spreadsheets/d/11-qZfY8V-_gxSZ0t6f_yTVIuwkSPWbLETZQPdItYUe4/edit#gid=158792646)<br/>
6007 Evaluate_ACH_pull_risk_controls_effectiveness.sql: following 3006, do the same thing for ACH to check the fail rate. Result in local folder 29 v3
