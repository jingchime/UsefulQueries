-- Query
-- new count with CTC + new payroll definition

with active_members as 
(
    select distinct user_id
    from "ANALYTICS"."TEST"."ACTIVITY_SUMMARY"
    where transaction_month = '2021-04-01'
), 

updated_payroll_dd as
(  select * 
from analytics.test.new_payroll_dd
             
),

ui as (
select m.id as user_id
from "CHIME"."FINANCE"."MEMBERS" m
join "ANALYTICS"."TEST"."ACTIVITY_SUMMARY" t
on m.id = t.user_id
where converted_to = 'UI'
and first_deposit_date < '2021-04-01'
and transaction_month = '2021-04-01'
),

government_benefit_dder as 
(
select distinct 
        user_id
    from looker.transactions
    where dd_type in ('government_benefit')
        and transaction_amount >= 200
        and transaction_timestamp <= '2021-04-01'
        and user_data_2 IN ('SSA  TREAS 310', 'SSI  TREAS 310', 'VACP TREAS 310')
),

gig_economy_dder as 
(
    select
            distinct transactions.user_id
    from        analytics.looker.transactions
    join        mysql_db.galileo.galileo_posted_transaction_enrichments
    on          transactions.id = galileo_posted_transaction_enrichments.galileo_posted_transaction_id
    join        chime.finance.merchant_categories  
    on          transactions.merchant_category_code = merchant_categories.merchant_category_code  
    where       1=1
                and transactions.transaction_code in ('PMVT','PMVH')
                and trim(galileo_posted_transaction_enrichments.business_application_identifier) in ('MD','FD')
                and merchant_categories.merchant_category_description in ( 
                            'Eating places and Restaurants' -- doordash, postmaters
                            , 'Grocery Stores, Supermarkets' -- instacart
                            , 'Taxicabs and Limousines' -- lyft uber
                            , 'Miscellaneous and Specialty Retail Stores' -- mercari
                            , 'Courier Services - Air or Ground, Freight forwarders' -- shipt
                            , 'Professional Services ( Not Elsewhere Defined)' -- 
                            , 'Cleaning and Maintenance, Janitorial Services' -- homeaglow
                            )
                and transactions.transaction_amount >= 200
                and transactions.transaction_timestamp < '2021-04-01'
        
), 

ctc as (
select distinct a.user_id 
from  analytics.test.conversion_permutations a
where mechanism = 'CTC'
)

select count (distinct a.user_id) 
from active_members a
left join updated_payroll_dd dd
   on a.user_id = dd.user_id
left join government_benefit_dder gvt
   on a.user_id = gvt.user_id 
left join gig_economy_dder gig
    on a.user_id = gig.user_id
left join ctc
    on a.user_id = ctc.user_id 
left join ui 
    on ui.user_id = a.user_id 
where dd.user_id is not null 
    OR gvt.user_id is not null
    OR gig.user_id is not null
    OR ctc.user_id is not null
    or ui.user_id is not null 

-- 2.6 M (compared to 2.7M)

-- where analytics.test.new_payroll_dd is using the query:
--2974284 with all new
create or replace table analytics.test.new_payroll_dd as 
select distinct t.user_id 
   from looker.ach_details a
   join looker.transactions t
    on a.id = t.id 
   where t.transaction_timestamp < '2021-04-01'
    and 
    t.transaction_amount >= 200
    and
        ((standard_entry_class_code = 'PPD'
               and (upper (company_entry_description) like '%ACH P/R%'
               or upper (company_entry_description) like '%ACH%'
               or upper (company_entry_description) like '%AP PAYMENT%'
               or upper (company_entry_description) like '%AP%'
               or upper (company_entry_description) like '%BATCH%'
               or upper (company_entry_description) like '%COMDATA%'
               or upper (company_entry_description) like '%CREDITS%'
               or upper (company_entry_description) like '%DC%'
               or upper (company_entry_description) like '%DEP TRANSF%'
               or upper (company_entry_description) like '%DIR DEP%'
               or upper (company_entry_description) like '%DIRCT DPST%'
               or upper (company_entry_description) like '%DIRDEP47%'
               or upper (company_entry_description) like '%DIRECT DEP%'
               or upper (company_entry_description) like '%DIRECT PAY%'
               or upper (company_entry_description) like '%EPOSPYMNTS%'
               or upper (company_entry_description) like '%FED SAL%'
               or upper (company_entry_description) like '%FED SALARY%'
               or upper (company_entry_description) like '%IPSC%'
               or upper (company_entry_description) like '%MEIJER PAY%'
               or upper (company_entry_description) like '%MGL PAYROL%'
               or upper (company_entry_description) like '%NY%'
               or upper (company_entry_description) like '%PAY%'
               or upper (company_entry_description) like '%PAYRLL DEP%'
               or upper (company_entry_description) like '%PAYROL%'
               or upper (company_entry_description) like '%PAYROLL DD%'
               or upper (company_entry_description) like '%PAYROLL%'  
               or upper (company_entry_description) like '%PENSION%'
               or upper (company_entry_description) like '%QUICKBOOKS%'
               or upper (company_entry_description) like '%REG SALARY%'
               or upper (company_entry_description) like '%REG.SALARY%'
               or upper (company_entry_description) like '%REGULAR%'
               or upper (company_entry_description) like '%RESTAURANT%'
               or upper (company_entry_description) like '%SALARY%'
               or upper (company_entry_description) like '%TA DDP%'
               or upper (company_entry_description) like '%TEAMMEMBER%'
               or upper (company_entry_description) like '%WF PAYROLL%'
               or upper (company_entry_description) = 'PR'
                    ))
              OR upper (company_entry_description) like '%BLUECREW%'
              OR upper (company_entry_description) like '%DIR DEP%' 
              OR upper (company_entry_description) like '%DIRDEP%'
              OR upper (company_entry_description) like '%NET=PAY%'
              OR upper (company_entry_description) like '%PAYRL%'
              OR upper (company_entry_description) like '%PAYROL%'
              OR upper (company_entry_description) like '%STARBUCKS CORP DIRECT DEPOSIT%'
              OR trim(lower(substring(t.user_data_1, 54,10))) like '%dir%dep%' 
              or trim(lower(substring(t.user_data_1, 54,10))) like '%payrol%'
              or substring(t.user_data_1, 41,10) = '3263101267'
              or trim(lower(substring(t.user_data_1, 54,10))) in ('paydep', 
                                                              'for job on', 
                                                              'net pay', 
                                                              'pr payment')
              or trim(upper(substring(t.user_data_1, 5,16))) in ('DOLLAR GENERAL', 
                                                              'DOLLAR TREE STOR', 
                                                              'COSTCO WHOLESALE', 
                                                              'STARBUCKS CORP', 
                                                              'CHIPGRIL', 
                                                              'SHIPT LLC', 
                                                              'CONDUENT BUS SVC', 
                                                              'DAILYPAY', 
                                                              'GUSTO', 
                                                               'STAFFMARK INVEST')
              or trim(upper(substring(t.user_data_1, 5,16))) like 'DFAS-%'
              or trim(upper(substring(t.user_data_1, 5,16))) like '%AMAZON.COM SERVI%'
              or trim(upper(substring(t.user_data_1, 5,16))) like '%POSTMATES%'
              or trim(upper(substring(t.user_data_1, 5,16))) like '%DOORDASH%'
              or trim(upper(substring(t.user_data_1, 5,16))) like '%GRUBHUB%'
              or trim(upper(substring(t.user_data_1, 5,16))) like 'RAISER%'
              or trim(upper(substring(t.user_data_1, 5,16))) like 'UBER USA%'
              or trim(upper(substring(t.user_data_1, 5,16))) like 'POSHMARK%'
              or trim(upper(substring(t.user_data_1, 5,16))) like 'ETSY%'
              or trim(upper(substring(t.user_data_1, 5,16))) like '%PAYROLL%'
              or trim(upper(substring(t.user_data_1, 5,16))) like '%PAYCHEX ADVANCE%'
              or trim(upper(substring(t.user_data_1, 5,16))) like '%MURPHY OIL USA P%'
                    )