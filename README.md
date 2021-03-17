# Queries Backup
Starting from 0, gradually learning the query logic used at Chime\
[tricks learned](https://docs.google.com/document/d/1Z5V8FhfCzu4Ve260wrZRJjorSxIZ-2uxfUdKmTBVnFk/edit)

## From Looker Dashboard
1.1 From_Looker_Fraud_And_Loss.sql\
1.2 From_Looker_Instant_Transfer.sql\
1.3 From_Looker_Limit_Tiers.sql

## From teammembers
2.1 Seven_Scores.sql: Stuti shared all scores used at Chime\
2.2 Dispute_Transaction_Status: Hemali shared how to pull most recent status of dispute. Updated it with Rakesh and Shu based on several versions\
2.3 User_Update_Phone.sql: from data-ask channel get how to check if user update their phone or email\
2.4 Scan_ID.sql: from Karan get if the user went through scan ID and pass/fail/manual review\
2.5 Limit_Dispute_Rate_By_Buckets.sql: Shu shared the query for decline rate, dispute rate calculation by different group. Note that everything is grouped together. So need to comment or uncomment to change the logic if needed.\
2.6 Recent_balance.sql: Bin shared the logic with me to pull recent balance\
2.7 Member_Partner_Bank.sql: Meeri shared the logic to pull stride/bancorp so I can finish the request on ALYTMM-75\
2.8 If_DD.sql: Stuti shared the logic to determine if someone is DD or not

## Instant Transfer
3.1 Instant_Transfer_Main_Tables.sql: key tables for updating and analyzing instant transfer\
3.2 Update_Weekly_IT_Meeting.sql: used to update weekly monitoring the blocked banks\
3.3 Update_Chargeback_by_Bank.sql: used to monitor the chargeback by bank to give alerts\
3.4 Instant_Transfer_Chargeback_Weekly.sql: used to update overall instant transfer chargebacks (both cash and cohort) by week

## Ad-hoc queries
4.1 Phone_number_change_cumulative_tracker.sql: cumulatively track phone number change in the last 32 days\
4.2 Available_Balance_Distribution.sql: researched the distribution to set up the right cutoff\
4.3 Response_code_51_61_analysis.sql: check if one transaction can have both 51 and 61 code\
4.4 Alibaba_Analysis.sql: supported the ad-hoc analysis for [Alibaba](https://docs.google.com/document/d/1Z_De49MtG56AdRa_FE4lgHpyoRW6uQ7diKMNwkB-wuQ/edit) <br/>
4.5 Days_take_for_instant_transfer_chargeback.sql: supported the follow-up analysis after the weekly meeting [days for instant transfer to chargeback](https://docs.google.com/document/d/1R6riAlQeNHjZ8aRbDdk2tnZWvIxLlfvBA2sgtpXYjok/edit) <br/>
4.6 Chargebacks_tie_back_to_transaction_date.sql: supported the request here [instant transfer chargebacks](https://docs.google.com/spreadsheets/d/1kFga0lzeyIQ_OrLvXrjeXuCRi04FRnxvBk4DhMOTgWM/edit?ts=60494835#gid=438205306) <br/>
4.7 Deep_dive_into_Feb_Mar_chargebacks.sql: looked into chargebacks related to Feb and Mar transactions and see if there is pattern on the bank or socure scores there. Found that missing scores are allowed instant transfers\
4.8 RI465_Alibaba_Block_Analysis.sql: validate the [RI465 decision](https://chime.atlassian.net/browse/RI-465) with the [document](https://docs.google.com/document/d/1kyB5wIrEy1mhivQ74xbEH40xXej8wZpQVOJcrF8GnG8/edit#) <br/> 
4.9 Distribution_of_days_from_enrollment_to_transfer.sql: ad-hoc analysis supporting the data backfill requirement of at least 60 days\
4.10 Compare_enrollment_score_with_chime_score.sql: compare enrollment score with chime score for instant transfer users (they are the same) and check the missing rate
