----------------------
--SIMULATED TIER
----------------------
WITH QUALIFYING_DD_TRANSACTIONS AS (--KEEPS ALL DD TRANSACTIONS >$1 
  SELECT GPT.ID,
  GPT.USER_ID,
  TRANSACTION_TIMESTAMP,
  TRANSACTION_AMOUNT,
  UPPER(USER_DATA_1) AS USER_DATA_1,
  USER_DATA_2,
  POST_DATE::DATE AS POST_DATE
  FROM MYSQL_DB.GALILEO.GALILEO_POSTED_TRANSACTIONS GPT
  WHERE  TRANSACTION_CODE IN ( 'PMDK', 'PMDD', 'PMCN' )
         AND ( TRANSACTION_AMOUNT >= 1 )
)

-- UI_DD transactions
,UI_DD AS (
SELECT
	A.ID,
	T.USER_ID,
	STATE_CODE,
	TRANSACTION_TIMESTAMP,
	TRANSACTION_AMOUNT,
	T.USER_DATA_1,
	T.USER_DATA_2,
	POST_DATE
FROM
	ANALYTICS.LOOKER.ACH_DETAILS A
JOIN ANALYTICS.LOOKER.TRANSACTIONS T ON
	A.ID = T.ID
LEFT JOIN FIVETRAN.CSV.UI_STATE_MAPPING UI ON
	UPPER(A.COMPANY_NAME) = UPPER(UI.COMPANY_NAME)
WHERE
	UI.COMPANY_NAME IS NOT NULL
    AND T.POST_DATE >= '2020-01-01') -- Add threshold date to reduce data processing time

,KNOWN_NON_PAYROLLS_EXCLUDED AS ( --EXCLUDES TRANSACTIONS WHICH HAVE KNOWN NON-PAYROLL PATTERNS
  SELECT ID,
  USER_ID,
  TRANSACTION_TIMESTAMP,
  TRANSACTION_AMOUNT,
  USER_DATA_1,
  USER_DATA_2,
  POST_DATE,
  (CASE
    WHEN --STRICT PAYROLL PATTERN
       ( USER_DATA_1 LIKE '%PAYROL%'
      OR USER_DATA_1 LIKE '%DIRDEP%'
      OR USER_DATA_1 LIKE '%PPDNY%'
      OR USER_DATA_1 LIKE '%PPDDEP TRANSF%'
      OR USER_DATA_1 LIKE '%PPDDIRECT DEP%'
      OR USER_DATA_1 LIKE '%PPDDIR DEP%'
      OR USER_DATA_1 LIKE '%PPDREG.SALARY%'
      OR USER_DATA_1 LIKE '%PPDREG SALARY%'
      OR USER_DATA_1 LIKE '%PPDPAY %'
      OR USER_DATA_1 LIKE '%PPDPAYRLL DEP%'
      OR USER_DATA_1 LIKE '%PPDFED SALARY%'
      OR USER_DATA_1 LIKE '%PPD  FED SAL%'
      OR USER_DATA_1 LIKE '%DIR DEP%'
      OR USER_DATA_1 LIKE '%PPDQUICKBOOKS%'
      OR USER_DATA_1 LIKE '%PPDACH P/R%'
      OR USER_DATA_1 LIKE '%PPDACH  %'
      OR USER_DATA_1 LIKE '%PPDPAY %'
      OR USER_DATA_1 LIKE '%PPDMEIJER PAY%'
      OR USER_DATA_1 LIKE '%NET=PAY%'
      OR USER_DATA_1 LIKE '%PPDCREDITS%'
      OR USER_DATA_1 LIKE '%PPDSALARY%'
      OR USER_DATA_1 LIKE '%PDBATCH%'
      OR USER_DATA_1 LIKE '%PPDAP PAYMENT%'
      OR USER_DATA_1 LIKE '%PPDIPSC%'
      OR USER_DATA_1 LIKE '%PPDAP %'
      OR USER_DATA_1 LIKE '%PPDDC %'
      OR USER_DATA_1 LIKE '%PPDDIRCT DPST%'
      OR USER_DATA_1 LIKE '%PDDIRECT PAY%'
      OR USER_DATA_1 LIKE '%PPDREGULAR%'
      OR USER_DATA_1 LIKE '%PPDPENSION%'
      OR USER_DATA_1 LIKE '%PAYRL%'
      OR USER_DATA_1 LIKE '%PPDTEAMMEMBER%'
      OR USER_DATA_1 LIKE '%PPDRESTAURANT%'
      OR USER_DATA_1 LIKE '%PPDCOMDATA%'
      OR USER_DATA_1 LIKE '%PPDMGL PAYROL%'
      OR USER_DATA_1 LIKE '%STARBUCKS CORP  DIRECT DEPOSIT%'
      OR USER_DATA_1 LIKE '%PPDTA DDP%'
      OR USER_DATA_1 LIKE '%PPDEPOSPYMNTS%')
       THEN 1
    ELSE 0
  END) AS PAYROLL_PATTERN
  FROM   QUALIFYING_DD_TRANSACTIONS
  WHERE  ( --EXCLUDE KNOWN NON-PAYROLL PATTERNS
               USER_DATA_1 NOT LIKE '%PPDSBTPG LLC%'
           AND USER_DATA_1 NOT LIKE '%ATP LLC%'
           AND USER_DATA_1 NOT LIKE '%IRS  TREAS 310%'
           AND USER_DATA_1 NOT LIKE '%PPDRT FED%'
           AND USER_DATA_1 NOT LIKE '%REPUBLIC TRS %'
           AND USER_DATA_1 NOT LIKE '%CCDRFND DISB%'
           AND USER_DATA_1 NOT LIKE '%TAX REF%'
           AND USER_DATA_1 NOT LIKE '%FRANCHISE TAX BD%'
           AND USER_DATA_1 NOT LIKE '%PPDTAX%'
           AND USER_DATA_1 NOT LIKE '%RIVER CITY BANK%'
           AND USER_DATA_1 NOT LIKE '%TAX PRODUCTS%'
           AND USER_DATA_1 NOT LIKE '%REFUND ADVANTAGE%'
           AND USER_DATA_1 NOT LIKE '%TAXRFD%'
           AND USER_DATA_1 NOT LIKE '%REFUNDO%'
           AND USER_DATA_1 NOT LIKE '%PPDTRANSFER%'
           AND USER_DATA_1 NOT LIKE '%CCDTRANSFER%'
           AND USER_DATA_1 NOT LIKE '%PPDACH TRNSFR%'
           AND USER_DATA_1 NOT LIKE '%PPDACH CREDIT%'
           AND USER_DATA_1 NOT LIKE '%PPD8663313065%'
           AND USER_DATA_1 NOT LIKE '%WEBP2P%'
           AND USER_DATA_1 NOT LIKE '%WEBTRANSFER%'
           AND USER_DATA_1 NOT LIKE '%WEBAUTOTRNSFR%'
           AND USER_DATA_1 NOT LIKE '%CIEBILLPAY CR%'
           AND USER_DATA_1 NOT LIKE '%PPDEXT TRNSFR%'
           AND USER_DATA_1 NOT LIKE '%PPDDEP TRANSF%'
           AND USER_DATA_1 NOT LIKE '%PPDCASHOUT%'
           AND USER_DATA_1 NOT LIKE '%VENMO%'
           AND USER_DATA_1 NOT LIKE '%PPDXFERS%'
           AND USER_DATA_1 NOT LIKE '%PPDCOMMISSION%'
           AND USER_DATA_1 NOT LIKE '%PPDXXSUPP SEC%'
           AND USER_DATA_1 NOT LIKE '%PPDUI BENEFIT%'
           AND USER_DATA_1 NOT LIKE '%PPDUI PAYMENT%'
           AND USER_DATA_1 NOT LIKE '%PPDXXVA BENEF%'
           AND USER_DATA_1 NOT LIKE '%PPDIHSSCMIPSE%'
           AND USER_DATA_1 NOT LIKE '%NYS DOL UI DD%'
           AND USER_DATA_1 NOT LIKE '%CCDKY21000%'
           AND USER_DATA_1 NOT LIKE '%PPDST OF MN%'
           AND USER_DATA_1 NOT LIKE '%PPDCHILD SUPP%'
           AND USER_DATA_1 NOT LIKE '%AR CHILD SUPPORT%'
           AND USER_DATA_1 NOT LIKE '%AZ CHILD SUPPORT%'
           AND USER_DATA_1 NOT LIKE '%CASDU CHILD SUP%'
           AND USER_DATA_1 NOT LIKE '%CHILD SUPPORT SP%'
           AND USER_DATA_1 NOT LIKE '%CT CHILD SUPPORT%'
           AND USER_DATA_1 NOT LIKE '%DC CHILD SUPPORT%'
           AND USER_DATA_1 NOT LIKE '%GA CHILD SUPPORT%'
           AND USER_DATA_1 NOT LIKE '%IN CHILD SUPPORT%'
           AND USER_DATA_1 NOT LIKE '%MA DOR/CHILD SUP%'
           AND USER_DATA_1 NOT LIKE '%NE CHILD SUPPORT%'
           AND USER_DATA_1 NOT LIKE '%OR CHILD SUPPORT%'
           AND USER_DATA_1 NOT LIKE '%SDU CHILD SUPPT%'
           AND USER_DATA_1 NOT LIKE '%RI CHILD SUPPORT%'
           AND USER_DATA_1 NOT LIKE '%VA CHILD SUPPORT%'
           AND USER_DATA_1 NOT LIKE '%WY CHILD SUPPORT%'
           AND USER_DATA_1 NOT LIKE '%PPDTRIALCREDT%'
           AND USER_DATA_1 NOT LIKE '%PPDVERIFY%'
           AND USER_DATA_1 NOT LIKE '%PPDCHALLENGES%'
           AND USER_DATA_1 NOT LIKE '%PPDTEST%'
           AND USER_DATA_1 NOT LIKE '%PPDACCNT AUTH%'
           AND USER_DATA_1 NOT LIKE '%PPDCAPITALONE%'
           AND USER_DATA_1 NOT LIKE '%PPDBNKSETUP%'
           AND USER_DATA_1 NOT LIKE '%PPDAUTH CRDT%'
           AND USER_DATA_1 NOT LIKE '%PPDCD VERIFY%'
           AND USER_DATA_1 NOT LIKE '%PPDACCT CNFRM%'
           AND USER_DATA_1 NOT LIKE '%PPDTEST TRAN%'
           AND USER_DATA_1 NOT LIKE '%PPDACCOUNTREG%'
           AND USER_DATA_1 NOT LIKE '%CCDVERIFYBAN%'
           AND USER_DATA_1 NOT LIKE '%PPDBANKSETUP%'
           AND USER_DATA_1 NOT LIKE '%PPDMICRO-DEP%'
           AND USER_DATA_1 NOT LIKE '%WEBTRIALDEP%'
           AND USER_DATA_1 NOT LIKE '%PPDAUTHDEPOSI%'
           AND USER_DATA_1 NOT LIKE '%PPDACCT CONF%'
           AND USER_DATA_1 NOT LIKE '%WEBMICRO DEPO%'
           AND USER_DATA_1 NOT LIKE '%CCDMICRODEPST%'
           AND USER_DATA_1 NOT LIKE '%PPDTRIAL CR%'
           AND USER_DATA_1 NOT LIKE '%PPDMICRO DPST%'
           AND USER_DATA_1 NOT LIKE '%CCDWEB_ACCVAL%'
           AND USER_DATA_1 NOT LIKE '%WEBTRIAL%'
           AND USER_DATA_1 NOT LIKE '%PPDTRANVERIFY%'
           AND USER_DATA_1 NOT LIKE '%CCDAUTH CRDT%' )
     AND ID NOT IN (SELECT ID FROM UI_DD) --EXCLUDE UI_DD TRANSACTIONS
)

,PAYROLL_PATTERN_DIRECT_DEPOSITS AS ( 
  SELECT ID,
  USER_ID,
  TRANSACTION_TIMESTAMP,
  TRANSACTION_AMOUNT,
  USER_DATA_1,
  USER_DATA_2,
  POST_DATE
  FROM KNOWN_NON_PAYROLLS_EXCLUDED
  WHERE PAYROLL_PATTERN=1
)

,RECURRENCE_PATTERN_CANDIDATES AS ( 
  SELECT ID,
  USER_ID,
  TRANSACTION_TIMESTAMP,
  TRANSACTION_AMOUNT,
  USER_DATA_1,
  USER_DATA_2,
  POST_DATE
  FROM KNOWN_NON_PAYROLLS_EXCLUDED
) 

,RECURRENT_DIRECT_DEPOSITS AS ( --ANY DD THAT IS NOT PART OF PAYROLL PATTERN AND WHICH HAS A SIMILAR DD AMOUNT (+-25%) PAID FROM SAME SOURCE (USER_DATA_2) POSTED WITHIN 31(???SHOULD IT BE 30???) DAYS AND NOT ON THE SAME WEEK
  SELECT DISTINCT L.ID, L.USER_ID, L.TRANSACTION_TIMESTAMP, L.TRANSACTION_AMOUNT, L.USER_DATA_1, L.USER_DATA_2, L.POST_DATE
  FROM RECURRENCE_PATTERN_CANDIDATES L
  JOIN RECURRENCE_PATTERN_CANDIDATES R
      ON L.USER_ID=R.USER_ID AND L.USER_DATA_2=R.USER_DATA_2
  WHERE 
      DATEDIFF(DAY, L.POST_DATE, R.POST_DATE) BETWEEN 0 AND 30 --WITHIN 31 DAYS. ONLY CHECK THE EARLIER AND LATER DD, NOT IN THE REVERSE ORDER (TO SIMULATE ENG LOGIC)
      AND NOT L.ID=R.ID
      AND (L.TRANSACTION_AMOUNT/R.TRANSACTION_AMOUNT BETWEEN 0.75 AND 1.25)-- OR R.TRANSACTION_AMOUNT/L.TRANSACTION_AMOUNT BETWEEN 0.75 AND 1.25) --???REMOVED SECOND RATIO TO MAKE IT SIMILAR TO ENG LOGIC
      AND NOT WEEK(L.POST_DATE) = WEEK(R.POST_DATE)
)

,RECURRENT_DIRECT_DEPOSITS_ALL AS ( --ANY DD THAT IS NOT PART OF PAYROLL PATTERN AND WHICH HAS A SIMILAR DD AMOUNT (+-25%) PAID FROM SAME SOURCE (USER_DATA_2) POSTED WITHIN 31(???SHOULD IT BE 30???) DAYS AND NOT ON THE SAME WEEK
  SELECT L.ID, L.USER_ID, L.TRANSACTION_TIMESTAMP, L.TRANSACTION_AMOUNT, L.USER_DATA_1, L.USER_DATA_2, L.POST_DATE
  FROM RECURRENCE_PATTERN_CANDIDATES L
  JOIN (SELECT DISTINCT USER_ID, USER_DATA_2 FROM RECURRENT_DIRECT_DEPOSITS) R
      ON L.USER_ID=R.USER_ID AND L.USER_DATA_2=R.USER_DATA_2
)

,STRICT_PAYROLL_DIRECT_DEPOSITS AS (
  SELECT ID, USER_ID, TRANSACTION_TIMESTAMP, TRANSACTION_AMOUNT, USER_DATA_1, USER_DATA_2, POST_DATE
  FROM PAYROLL_PATTERN_DIRECT_DEPOSITS
  UNION
  SELECT ID, USER_ID, TRANSACTION_TIMESTAMP, TRANSACTION_AMOUNT, USER_DATA_1, USER_DATA_2, POST_DATE
  FROM RECURRENT_DIRECT_DEPOSITS_ALL
)


-- IDENTIFY THE STRICT PDDS AMONG ALL THE ELIGIBLE DDS
, AUX_LIST_ELIG_DDS AS (
  SELECT
  A.*,
  COALESCE(B.TRANSACTION_AMOUNT, 0) AS STRICT_PDD_AMT
  FROM QUALIFYING_DD_TRANSACTIONS AS A
  LEFT JOIN STRICT_PAYROLL_DIRECT_DEPOSITS AS B
  ON A.ID=B.ID
)


-- TO DETERMINE THE LIMITS, WE ONLY CONSIDER STRICT PAYROLL TRXNS
, STRICT_PDD AS (
  SELECT TO_DATE(POST_DATE) AS TRXN_DATE,
  DATEADD(DAY, -31, TO_DATE(POST_DATE)) AS TRXN_DATE_1M,
  USER_ID,
  SUM(STRICT_PDD_AMT) AS STRICT_PDD_AMT,
  SUM(TRANSACTION_AMOUNT) AS TRXN_AMT
  FROM AUX_LIST_ELIG_DDS
  GROUP BY 1, 2, 3
)

, SUMCUM_STRICT_PDD AS (
  SELECT A.USER_ID,
  A.TRXN_DATE AS ELIG_DD_DATE,
  SUM(B.STRICT_PDD_AMT) AS SUMCUM_STRICTPDD_1M,
  (CASE
    WHEN SUM(B.STRICT_PDD_AMT)>=6000 THEN 'TIER4'
    WHEN SUM(B.STRICT_PDD_AMT)>=2000 THEN 'TIER3'
    WHEN SUM(B.STRICT_PDD_AMT)>=1000 THEN 'TIER2'
    ELSE 'TIER1'
  END) AS LIMITS_TIER
  FROM STRICT_PDD AS A
  LEFT JOIN STRICT_PDD AS B
  ON A.USER_ID = B.USER_ID
    AND (B.TRXN_DATE>=A.TRXN_DATE_1M AND B.TRXN_DATE<=A.TRXN_DATE)
  GROUP BY 1, 2
)


,DATES_LIMIT_TIER_CHANGE_SIMUL AS (
  SELECT
  USER_ID,
  MIN(CASE WHEN LIMITS_TIER='TIER2' THEN ELIG_DD_DATE END) AS FIRST_DATE_SIMULATED_TIER2,
  MIN(CASE WHEN LIMITS_TIER='TIER3' THEN ELIG_DD_DATE END) AS FIRST_DATE_SIMULATED_TIER3,
  MIN(CASE WHEN LIMITS_TIER='TIER4' THEN ELIG_DD_DATE END) AS FIRST_DATE_SIMULATED_TIER4
  FROM SUMCUM_STRICT_PDD
  GROUP BY 1
)


----------------------
--TIER AT TRXN
----------------------
, REAL_TIER_CHANGE AS (
	SELECT
	ID,
	--DATE_PT AS UPGRADE_DT,
	"TIMESTAMP":: DATE AS UPGRADE_DT,
	USER_ID,
	-- new_product_id => new_product_id
	TRY_TO_NUMBER(dd_amount, 10, 2) AS dd_amount,
	TO_CHAR(new_product_id) AS new_product_id,
	TO_CHAR(old_product_id) AS old_product_id,
	(CASE
	  --WHEN TO_CHAR(new_product_id) = 'virtual_tier4' THEN 'TIER4'
	  WHEN TRY_TO_NUMBER(TO_CHAR(new_product_id))  IN (6524,6523,6495,6287,6216,6254,6200,6049,6048,5887,5176,5048,2295,2294,2239,2070,2068) THEN 'TIER1' --02/19:add (6524,2295,2294,2070)
	  WHEN TRY_TO_NUMBER(TO_CHAR(new_product_id))  IN (8069,6526,6525,6222,6121,5888,2300,2298,2297,2246,2081,2067) THEN 'TIER2'  --02/16Update: IN (6525,6222,6121,5888,2081,2246) --02/19:add (8069,6526,2300,2067)
	  WHEN TRY_TO_NUMBER(TO_CHAR(new_product_id))   IN (6638,6627,2299,2240,2074,2071) THEN 'TIER3' -- 02/16Update: IN (6638,6627,2071) THEN 'TIER3'
	  WHEN TRY_TO_NUMBER(TO_CHAR(new_product_id))   IN (2227,2226,2225,2189) THEN 'TIER4' -- 02/19Update: add Tier4
	  ELSE 'OTHERS'
	END) AS new_tier,
	(CASE
	  --WHEN TO_CHAR(old_product_id) = 'virtual_tier4' THEN 'TIER4'
	  WHEN TRY_TO_NUMBER(TO_CHAR(old_product_id))  IN (6524,6523,6495,6287,6216,6254,6200,6049,6048,5887,5176,5048,2295,2294,2239,2070,2068) THEN 'TIER1' --02/19:add (6524,2295,2294,2070)
	  WHEN TRY_TO_NUMBER(TO_CHAR(old_product_id))  IN (8069,6526,6525,6222,6121,5888,2300,2298,2297,2246,2081,2067) THEN 'TIER2'  --02/16Update: IN (6525,6222,6121,5888,2081,2246) --02/19:add (8069,6526,2300,2067)
	  WHEN TRY_TO_NUMBER(TO_CHAR(old_product_id))  IN (6638,6627,2299,2240,2074,2071) THEN 'TIER3' -- 02/16Update: IN (6638,6627,2071) THEN 'TIER3'
	  WHEN TRY_TO_NUMBER(TO_CHAR(old_product_id))  IN (2227,2226,2225,2189) THEN 'TIER4' -- 02/19Update: add Tier4
	  ELSE 'OTHERS'
	END) AS old_tier
	--FROM ANALYTICS.LOOKER.SIO_TRACKS
	FROM SEGMENT.CHIME_PROD.PRODUCT_ID_UPGRADED_FOR_HIGHER_SPENDING_LIMITS
	WHERE new_tier <> old_tier AND UPGRADE_DT >= '2020-02-11'
	AND new_product_id != 'virtual_tier4'
	--WHERE EVENT = 'product_id upgraded for higher spending limits' AND new_tier <> old_tier AND UPGRADE_DT >= '2020-02-11'
	QUALIFY (ROW_NUMBER() OVER (PARTITION BY USER_ID, NEW_TIER ORDER BY UPGRADE_DT)) = 1  -- keep the first date when that tier upgrade happens
)

-- Add records before 2020-02-11
,ALL_REAL_TIER_CHANGE AS (
	SELECT *
	FROM REAL_TIER_CHANGE
	UNION
	 (SELECT TO_CHAR(-1) AS ID, TO_DATE(UPDATED_AT) AS UPGRADE_DT, USER_ID, NULL AS DD_AMOUNT, TO_CHAR(PRODUCT_ID) AS NEW_PRODUCT_ID, NULL AS OLD_PRODUCT_ID, 'TIER2' AS NEW_TIER, 'TIER1' AS OLD_TIER
		 FROM MYSQL_DB.CHIME_PROD.USER_BANK_ACCOUNTS
		 WHERE TYPE = 'checking' AND UPDATED_AT < '2020-02-11'
		 AND PRODUCT_ID IN (8069,6526,6525,6222,6121,5888,2300,2298,2297,2246,2081,2067) -- TIER2 ONLY -02/19: update PIDs
		 QUALIFY (RANK() OVER (PARTITION BY USER_ID ORDER BY UPDATED_AT DESC)) = 1)
)


,DATES_REAL_NEW_TIER_CHANGE AS (
  SELECT
  USER_ID,
  (CASE WHEN
	  (SUM(CASE WHEN NEW_TIER='TIER3' AND OLD_TIER = 'TIER2' THEN 1 ELSE 0 END) > 0
	  AND
	  SUM(CASE WHEN NEW_TIER='TIER2' AND OLD_TIER = 'TIER1' THEN 1 ELSE 0 END) = 0)
	  OR
	  (SUM(CASE WHEN NEW_TIER='TIER4' AND OLD_TIER = 'TIER2' THEN 1 ELSE 0 END) > 0
	  AND
	  SUM(CASE WHEN NEW_TIER='TIER2' AND OLD_TIER = 'TIER1' THEN 1 ELSE 0 END) = 0)
  THEN TO_DATE('2020-02-11')
  ELSE MIN(CASE WHEN NEW_TIER='TIER2' THEN UPGRADE_DT END)
  END) AS FIRST_DATE_TIER2,

  MIN(CASE WHEN NEW_TIER='TIER3' THEN UPGRADE_DT END) AS FIRST_DATE_TIER3,
  MIN(CASE WHEN NEW_TIER='TIER4' THEN UPGRADE_DT END) AS FIRST_DATE_TIER4

  FROM ALL_REAL_TIER_CHANGE
  GROUP BY 1
)

, ALL_TRANS AS(
  SELECT DISTINCT
  A.ID AS TRXN_ID,
  (CASE
  	  WHEN TO_DATE(A.TRANSACTION_TIMESTAMP)>=S.FIRST_DATE_SIMULATED_TIER4 THEN 'TIER4'
	  WHEN TO_DATE(A.TRANSACTION_TIMESTAMP)>=S.FIRST_DATE_SIMULATED_TIER3 THEN 'TIER3'
	  WHEN TO_DATE(A.TRANSACTION_TIMESTAMP)>=S.FIRST_DATE_SIMULATED_TIER2 THEN 'TIER2'
	  ELSE 'TIER1'
   END) AS SIMULATED_TIER,
   
  (CASE
	  WHEN TO_DATE(A.TRANSACTION_TIMESTAMP)>=D.FIRST_DATE_TIER4 THEN 'TIER4'
	  WHEN TO_DATE(A.TRANSACTION_TIMESTAMP)>=D.FIRST_DATE_TIER3 THEN 'TIER3'
	  WHEN TO_DATE(A.TRANSACTION_TIMESTAMP)>=D.FIRST_DATE_TIER2 THEN 'TIER2'
	  ELSE 'TIER1'
	END) AS TIER_AT_TRXN,
	
  (CASE WHEN D.FIRST_DATE_TIER4 IS NOT NULL THEN 'TIER4'
   		 WHEN D.FIRST_DATE_TIER3 IS NOT NULL THEN 'TIER3'
   		 WHEN D.FIRST_DATE_TIER2 IS NOT NULL THEN 'TIER2'
   	ELSE 'TIER1' 
    END) AS LATEST_TIER,
  A.TRANSACTION_TIMESTAMP,
  S.FIRST_DATE_SIMULATED_TIER2,
  S.FIRST_DATE_SIMULATED_TIER3,
  S.FIRST_DATE_SIMULATED_TIER4,
  D.FIRST_DATE_TIER2,
  D.FIRST_DATE_TIER3,
  D.FIRST_DATE_TIER4
   
  FROM ANALYTICS.LOOKER.TRANSACTIONS A
  LEFT JOIN DATES_LIMIT_TIER_CHANGE_SIMUL S 
  ON A.USER_ID = S.USER_ID
  LEFT JOIN DATES_REAL_NEW_TIER_CHANGE D
  ON A.USER_ID = D.USER_ID
  WHERE A.TRANSACTION_TIMESTAMP >= '2020-01-01'
)
 
SELECT * FROM ALL_TRANS