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
2.6 Available_balance.sql: from data-ask channel get query on available balance\
2.7 Member_Partner_Bank.sql: Meeri shared the logic to pull stride/bancorp so I can finish the request on ALYTMM-75

## Instant Transfer
3.1 Instant_Transfer_Main_Tables.sql: key tables for updating and analyzing instant transfer\
3.2 Update_Weekly_IT_Meeting.sql: used to update weekly monitoring the blocked banks\
3.3 Update_Chargeback_by_Bank.sql: used to monitor the chargeback by bank to give alerts\
3.4 Instant_Transfer_Chargeback_Weekly.sql: used to update overall instant transfer chargebacks (both cash and cohort) by week

## Ad-hoc queries
4.1 Phone_number_change_cumulative_tracker.sql: cumulatively track phone number change in the last 32 days\
4.2 Available_Balance_Distribution.sql: researched the distribution to set up the right cutoff\
4.3 Response_code_51_61_analysis.sql: check if one transaction can have both 51 and 61 code\
4.4 Alibaba_Analysis.sql: supported the ad-hoc analysis for [Alibaba](https://docs.google.com/document/d/1Z_De49MtG56AdRa_FE4lgHpyoRW6uQ7diKMNwkB-wuQ/edit) 
4.5 Days_take_for_instant_transfer_chargeback.sql: supported the follow-up analysis after the weekly meeting [days for instant transfer to chargeback](https://docs.google.com/document/d/1R6riAlQeNHjZ8aRbDdk2tnZWvIxLlfvBA2sgtpXYjok/edit) 
