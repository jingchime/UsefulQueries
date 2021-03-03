# Queries Backup
Starting from 0, gradually learning the query logic used at Chime

## From Looker Dashboard
1.1 From_Looker_Fraud_And_Loss.sql\
1.2 From_Looker_Instant_Transfer.sql\
1.3 From_Looker_Limit_Tiers.sql

## From teammembers
2.1 Seven_Scores.sql: all scores used at Chime\
2.2 Dispute_Transaction_Status: most recent status of dispute\
2.3 User_Update_Phone.sql: how to check if user update their phone or email\
2.4 Scan_ID.sql: if the user went through scan ID and pass/fail/manual review\
2.5 Limit_Dispute_Rate_By_Buckets.sql: Shu shared the query for decline rate, dispute rate calculation by different group. Note that everything is grouped together. So need to comment or uncomment to change the logic if needed.

## Instant Transfer
3.1 IT_Main_Tables.sql: key tables for updating and analyzing instant transfer\
3.2 Update_Weekly_IT_Meeting.sql: used to update weekly monitoring the blocked banks\
3.3 Update_Chargeback_by_Bank.sql: used to monitor the chargeback by bank to give alerts

## Ad-hoc queries
4.1 Phone_number_change_cumulative_tracker.sql: cumulatively track phone number change in the last 32 days
