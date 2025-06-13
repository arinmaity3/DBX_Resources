# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import  col,expr,max,lower,upper,min

def fin_L1_STG_GLAccountClassification_populate():
    try:
        logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
        global fin_L1_STG_GLAccountClassification
        objGenHelper = gen_genericHelper()
        objDataTransformation = gen_dataTransformation()
        
        
        cteFlat=fin_L1_MD_GLAccount.alias("gla1")\
        .join(fin_L1_STG_GLAccountMapping.alias("glam1"),\
        expr("( gla1.accountNumber = glam1.accountNumber ) "),"inner")\
        .join(fin_L1_STG_GLAccountCategoryMaster.alias("glcm1"),\
        expr("( glam1.accountCategoryID = \
        glcm1.glAccountCategoryMasterSurrogateKey ) "),"inner")\
        .join(fin_L1_STG_GLAccountFlattenHierarchy.alias("glfh1"),\
        expr("( glfh1.leafID = glam1.accountCategoryID )"),"inner")\
        .selectExpr("glam1.accountNumber as accountNumber"\
        ,"gla1.accountName	  			 as accountDescription"\
        ,"glfh1.accountName_01			 as accountCategory1"\
        ,"glfh1.accountName_02			 as accountCategory2"\
        ,"glfh1.accountName_03			 as accountCategory3"\
        ,"glfh1.accountName_04			 as accountCategory4"\
        ,"glfh1.accountName_05			 as accountCategory5"\
        ,"glfh1.accountName_06			 as accountCategory6"\
        ,"glfh1.accountName_07			 as accountCategory7"\
        ,"glfh1.accountName_08			 as accountCategory8"\
        ,"glfh1.accountName_09			 as accountCategory9"\
        ,"glcm1.accountCategory           accountLeafNodeCategory")
        
        fin_L1_TMP_AccountCategoryLevel=\
        cteFlat.distinct().alias("flat1")\
        .select(expr("flat1.accountNumber")\
         ,expr("flat1.accountDescription")\
         ,expr("stack(9,accountCategory1,'1'\
         ,accountCategory2,'2'\
         ,accountCategory3,'3'\
         ,accountCategory4,'4'\
         ,accountCategory5,'5'\
         ,accountCategory6,'6'\
         ,accountCategory7,'7'\
         ,accountCategory8,'8'\
         ,accountCategory9,'9') as (accountCategory,accountCategoryLevel)")\
         ,expr("accountLeafNodeCategory")).filter((col("accountCategory"))\
         .isNotNull()).distinct()
        
        fin_L1_TMP_TrackCategory=\
        fin_L1_TMP_AccountCategoryLevel.alias("accl1")\
        .crossJoin(knw_LK_CD_ReportingSetup.alias("lkrs"))\
        .groupBy("lkrs.companyCode"\
        ,"accl1.accountNumber")\
        .agg( expr("max(accl1.accountCategoryLevel)  as maxTreeLevel ")\
        ,expr("max(accl1.accountCategoryLevel)     as trackTreeLevel ")\
        ,expr("max(accl1.accountLeafNodeCategory)  as trackCategory") )
        
        fin_L1_TMP_TrackCategory=\
        fin_L1_TMP_TrackCategory.alias("tkcy1")\
        .join(fin_L1_TMP_AccountCategoryLevel.alias("accl1"),\
        expr("( accl1.accountNumber = tkcy1.accountNumber\
         AND accl1.accountCategory = tkcy1.trackCategory\
         AND tkcy1.trackCategory IS NOT NULL )"),"left")\
        .withColumn("trackTreeLevel_upd",expr("CASE	WHEN tkcy1.trackCategory IS NOT NULL\
                                                THEN accl1.accountCategoryLevel \
                                                ELSE tkcy1.maxTreeLevel \
        									END"))\
        .groupBy("tkcy1.companyCode","tkcy1.accountNumber","tkcy1.maxTreeLevel",\
                 "tkcy1.trackTreeLevel","tkcy1.trackCategory")\
        .agg(expr("min(trackTreeLevel_upd) as trackTreeLevel_upd"))\
        .withColumn("trackTreeLevel",col("trackTreeLevel_upd"))\
        .drop(col("trackTreeLevel_upd"))
        
        fin_L1_TMP_TrackCategory=\
        fin_L1_TMP_TrackCategory.alias("tkcy1")\
        .join(fin_L1_TMP_AccountCategoryLevel.alias("accl1"),\
        expr("( accl1.accountNumber = tkcy1.accountNumber\
         AND accl1.accountCategoryLevel = tkcy1.maxTreeLevel\
         AND tkcy1.trackCategory IS NULL )"),"left")\
        .withColumn("trackCategory_upd",expr("CASE WHEN tkcy1.trackCategory IS NULL THEN \
        													accl1.accountCategory \
        										  ELSE		tkcy1.trackCategory \
        										  END"))\
        .withColumn("trackTreeLevel_upd",expr("CASE WHEN tkcy1.trackCategory IS NULL THEN \
        													tkcy1.maxTreeLevel \
        										  ELSE		tkcy1.trackTreeLevel \
        										  END"))\
        .groupBy("tkcy1.companyCode","tkcy1.accountNumber","tkcy1.maxTreeLevel",\
        "tkcy1.trackTreeLevel","tkcy1.trackCategory")\
        .agg(expr("max(trackCategory_upd) as trackCategory_upd"),\
         expr("max(trackTreeLevel_upd) as trackTreeLevel_upd"))\
        .withColumn("trackCategory",col("trackCategory_upd"))\
        .withColumn("trackTreeLevel",col("trackTreeLevel_upd"))\
        .drop(col("trackCategory_upd"))\
        .drop(col("trackTreeLevel_upd"))
        fin_L1_TMP_GLAccountClassification=\
        fin_L1_TMP_TrackCategory.alias("tkcy1")\
        .join(fin_L1_MD_GLAccount.alias("gla1"),\
        expr("( tkcy1.accountNumber = gla1.accountNumber\
         AND tkcy1.companyCode = gla1.companyCode ) "),"inner")\
        .join(knw_LK_CD_ReportingSetup.alias("lkrs"),\
        expr("( tkcy1.companyCode = lkrs.companyCode ) "),"inner")\
        .join(fin_L1_STG_GLAccountMapping.alias("map"),\
        expr("( gla1.accountNumber = map.accountNumber ) "),"inner")\
        .join(fin_L1_TMP_AccountCategoryLevel.alias("maxlevel"),\
        expr("( tkcy1.accountNumber = maxlevel.accountNumber\
         AND tkcy1.maxTreeLevel = maxlevel.accountCategoryLevel )"),"left")\
        .groupBy("tkcy1.accountNumber"\
        ,"tkcy1.companyCode")\
        .agg(expr("max(lkrs.chartOfAccount) as chartOfAccount")\
        ,expr("max(gla1.accountName) as accountDescription")\
        ,expr("max(coalesce(maxlevel.accountCategory, \
        tkcy1.trackCategory)) as accountCategory")\
        ,expr("max((CASE WHEN ISNULL(maxlevel.accountCategoryLevel)\
        THEN  tkcy1.maxTreeLevel \
        ELSE maxlevel.accountCategoryLevel END)) as treeLevel")\
        ,expr("max(tkcy1.trackCategory) as trackCategory")\
        ,expr("CASE WHEN MAX(cast(map.isBank as int))=0 \
        AND MAX(cast(map.isCash as int))=0 THEN 0 \
        ELSE 1 END as isBankCash")\
        ,expr("MAX(cast(map.isTradeAR as int)) as isTradeAR")\
        ,expr("MAX(cast(map.isTradeAP as int)) as isTradeAP")\
        ,expr("MAX(cast(map.isRevenue as int)) as isRevenue")\
        ,expr("max(CASE WHEN gla1.accountType = 'PL' THEN 1 ELSE 0 END) as isPL")\
        ,expr("max(CASE WHEN gla1.isInterCoFlag = 1 \
        THEN 1 ELSE 0 END) as isInterCoAccount"))\
        .selectExpr("accountNumber"\
        ,"companyCode"\
        ,"chartOfAccount"\
        ,"accountDescription"\
        ,"accountCategory"\
        ,"treeLevel"\
        ,"trackCategory"\
        ,"isBankCash"\
        ,"isTradeAR"\
        ,"isTradeAP"\
        ,"isRevenue"\
        ,"0 as isVAT"\
        ,"0 as isCashDiscount"\
        ,"0 as isBankCharge"\
        ,"0 as isCurrencyFX"\
        ,"isPL"\
        ,"isInterCoAccount"\
        ,"0 as heuristicRule")
        
        fin_L1_TMP_AccountVAT=\
        fin_L1_TD_Journal.alias("jrnl1")\
        .filter(expr("jrnl1.lineItemID = 'T'"))\
        .selectExpr("jrnl1.accountNumber").alias("accountNumber").distinct()
        
        fin_L1_TMP_GLAccountClassification=\
        fin_L1_TMP_GLAccountClassification.alias("glcf1")\
        .join(fin_L1_TMP_AccountVAT.alias("acvt1"),\
        expr("( glcf1.accountNumber = acvt1.accountNumber\
         AND glcf1.isBankCash = 0\
         AND glcf1.isTradeAR = 0\
         AND glcf1.isRevenue = 0\
         AND glcf1.isCashDiscount = 0\
         AND glcf1.isBankCharge = 0\
         AND glcf1.isPL = 0 )"),"left")\
        .groupBy("glcf1.accountNumber","glcf1.companyCode","glcf1.chartOfAccount",\
                 "glcf1.accountDescription","glcf1.accountCategory",\
                 "glcf1.treeLevel","glcf1.trackCategory",\
                 "glcf1.isBankCash","glcf1.isTradeAR","glcf1.isTradeAP",\
                 "glcf1.isRevenue",\
                 "glcf1.isCashDiscount",\
                 "glcf1.isBankCharge","glcf1.isCurrencyFX","glcf1.isPL",\
                 "glcf1.isInterCoAccount")\
        .agg(expr("MAX(CASE WHEN acvt1.accountNumber IS NOT NULL THEN 1 \
        ELSE glcf1.isVAT END ) AS isVAT")\
        ,expr("MAX(CASE WHEN acvt1.accountNumber IS NOT NULL THEN 1\
         ELSE glcf1.heuristicRule END ) AS heuristicRule"))
        
        
        fin_L1_TMP_BankClearingCandidate=\
        fin_L1_TD_Journal.alias("jrnl1")\
        .filter(expr("jrnl1.transactionCode IN('FBZ1', 'FBZ2', 'F110') \
        			 			 AND jrnl1.documentStatus = 'N'"))\
        .groupBy("jrnl1.documentNumber"\
        ,"jrnl1.companyCode"\
        ,"jrnl1.fiscalYear")\
        .agg( expr("max(CASE WHEN jrnl1.accountType = 'S' THEN jrnl1.accountNumber \
        ELSE NULL END) as accountNumber")\
        ,expr("count(*) as cnt")\
        ,expr("count(DISTINCT (CASE WHEN jrnl1.accountType = 'S' THEN jrnl1.accountNumber\
         ELSE NULL END)) as dstntcntS")\
        ,expr("count(DISTINCT (CASE WHEN jrnl1.accountType = 'D' THEN jrnl1.accountNumber\
         ELSE NULL END)) as dstntcntD") )\
        .filter(expr("cnt=2 and dstntcntS=1 and dstntcntD=1"))\
        .select(col("accountNumber")).distinct()
        
        fin_L1_TMP_GLAccountClassification=\
        fin_L1_TMP_GLAccountClassification.alias("glcf1")\
        .join(fin_L1_TMP_BankClearingCandidate.alias("bkcc1"),\
        expr("( glcf1.accountNumber = bkcc1.accountNumber\
         AND glcf1.isBankCash = 0 \
         AND glcf1.isTradeAR = 0 \
         AND glcf1.isRevenue = 0 \
         AND glcf1.isVAT = 0 \
         AND glcf1.isCashDiscount = 0 \
         AND glcf1.isBankCharge = 0 \
         AND glcf1.isPL = 0 \
         AND lower(glcf1.accountCategory) NOT LIKE '%liabilities%' )"),"left")\
        .groupBy("glcf1.accountNumber",\
                 "glcf1.companyCode","glcf1.chartOfAccount",\
                 "glcf1.accountDescription","glcf1.accountCategory",\
                 "glcf1.treeLevel","glcf1.trackCategory",\
                 "glcf1.isTradeAR","glcf1.isTradeAP","glcf1.isRevenue",\
                 "glcf1.isCashDiscount",\
                 "glcf1.isBankCharge","glcf1.isCurrencyFX","glcf1.isPL",\
                 "glcf1.isInterCoAccount","glcf1.isVAT")\
        .agg(expr("MAX(CASE WHEN bkcc1.accountNumber IS NOT NULL THEN 1 ELSE\
        glcf1.isBankCash END ) AS isBankCash")\
        ,expr("MAX(CASE WHEN bkcc1.accountNumber IS NOT NULL THEN 1 ELSE \
        glcf1.heuristicRule END ) AS heuristicRule"))
        
        fin_L1_TMP_GLAccountClassification=\
        fin_L1_TMP_GLAccountClassification.alias("glcf1")\
        .join(fin_L1_STG_AccountConfiguration.alias("accf"),\
        expr("( glcf1.accountNumber = accf.accountNumber \
         AND glcf1.companyCode = accf.companyCode \
         AND accf.configurationName ='HouseBankAccounts' \
         AND glcf1.isBankCash = 0 \
         AND glcf1.isTradeAR = 0 \
         AND glcf1.isRevenue = 0 \
         AND glcf1.isVAT = 0 \
         AND glcf1.isCashDiscount = 0 \
         AND glcf1.isBankCharge = 0 \
         AND glcf1.isPL = 0 )"),"left")\
        .groupBy("glcf1.accountNumber"\
        ,"glcf1.isBankCharge"\
        ,"glcf1.companyCode"\
        ,"glcf1.isCurrencyFX"\
        ,"glcf1.accountCategory"\
        ,"glcf1.chartOfAccount"\
        ,"glcf1.treeLevel"\
        ,"glcf1.isInterCoAccount"\
        ,"glcf1.isVAT"\
        ,"glcf1.isTradeAR"\
        ,"glcf1.trackCategory"\
        ,"glcf1.isRevenue"\
        ,"glcf1.accountDescription"\
        ,"glcf1.isCashDiscount"\
        ,"glcf1.isTradeAP"\
        ,"glcf1.isPL")\
        .agg(expr("max(case when accf.accountNumber is not null then 1 \
        else glcf1.isBankCash end) as isBankCash")\
        ,expr("max(case when accf.accountNumber is not null then 1 \
        else glcf1.heuristicRule end) as heuristicRule"))
        
        fin_L1_TMP_GLAccountClassification=\
        fin_L1_TMP_GLAccountClassification.alias("glcf1")\
        .join(fin_L1_STG_AccountConfiguration.alias("accf"),\
        expr("( glcf1.accountNumber = accf.accountNumber\
         AND glcf1.chartOfAccount = accf.chartOfAccount\
         AND accf.configurationName ='StandardAccountsBSP'\
         AND glcf1.isBankCash = 0\
         AND glcf1.isTradeAR = 0\
         AND glcf1.isRevenue = 0\
         AND glcf1.isVAT = 0\
         AND glcf1.isCashDiscount = 0\
         AND glcf1.isBankCharge = 0\
         AND glcf1.isPL = 1 )"),"left")\
        .groupBy("glcf1.accountNumber"\
        ,"glcf1.companyCode"\
        ,"glcf1.isCurrencyFX"\
        ,"glcf1.accountCategory"\
        ,"glcf1.heuristicRule"\
        ,"glcf1.chartOfAccount"\
        ,"glcf1.treeLevel"\
        ,"glcf1.isInterCoAccount"\
        ,"glcf1.isBankCash"\
        ,"glcf1.isVAT"\
        ,"glcf1.isTradeAR"\
        ,"glcf1.trackCategory"\
        ,"glcf1.isRevenue"\
        ,"glcf1.accountDescription"\
        ,"glcf1.isCashDiscount"\
        ,"glcf1.isTradeAP"\
        ,"glcf1.isPL")\
        .agg(expr("max(case when accf.accountNumber is not null \
        then 1 else glcf1.isBankCharge end) as isBankCharge"))
        
        
        fin_L1_TMP_ClearingAccountBankByDoc=\
        fin_L1_TD_Journal.alias("jrnl1")\
        .join(fin_L1_TMP_GLAccountClassification.alias("glcf1"),\
        expr("( glcf1.accountNumber = jrnl1.accountNumber\
         AND glcf1.companyCode = jrnl1.companyCode\
         AND glcf1.isBankCash = 1 )"),"inner")\
        .filter(expr("jrnl1.documentStatus = 'N'"))\
        .selectExpr("jrnl1.companyCode"\
        ,"jrnl1.fiscalYear"\
        ,"jrnl1.documentNumber").distinct()
        
        fin_L1_TMP_ClearingAccountCashDiscountByDoc=\
        fin_L1_TD_Journal.alias("jrnl1")\
        .join(fin_L1_TMP_ClearingAccountBankByDoc.alias("clab1"),\
        expr("( jrnl1.documentNumber = clab1.documentNumber\
         AND jrnl1.companyCode = clab1.companyCode\
         AND jrnl1.fiscalYear = clab1.fiscalYear ) "),"inner")\
        .join(fin_L1_TMP_GLAccountClassification.alias("glcf1"),\
        expr("( glcf1.accountNumber = jrnl1.accountNumber\
         AND glcf1.companyCode = jrnl1.companyCode )"),"left")\
        .groupBy("jrnl1.companyCode"\
        ,"jrnl1.fiscalYear"\
        ,"jrnl1.documentNumber")\
        .agg( expr("sum(CASE WHEN glcf1.isBankCash = 1 THEN \
        (CASE WHEN jrnl1.debitCreditIndicator = 'C'\
        THEN (-1.0) * jrnl1.amountDC ELSE jrnl1.amountDC END) \
        ELSE 0 END) as amountBank")\
        ,expr("sum(CASE WHEN jrnl1.accountType ='D' THEN\
       (CASE WHEN jrnl1.debitCreditIndicator = 'C' THEN\
        (-1.0) * jrnl1.amountDC ELSE jrnl1.amountDC END) ELSE 0 END) as amountAR")\
        ,expr("sum(CASE WHEN jrnl1.accountType ='K' THEN \
        (CASE WHEN jrnl1.debitCreditIndicator = 'C' THEN\
        (-1.0) * jrnl1.amountDC ELSE jrnl1.amountDC END) ELSE 0 END) as amountAP")\
        ,expr("sum(CASE WHEN jrnl1.accountType IN ('D') THEN\
       (CASE WHEN jrnl1.debitCreditIndicator = 'C' THEN \
        (-1.0) * jrnl1.cashDiscountAmount ELSE \
        jrnl1.cashDiscountAmount END) ELSE 0 END) as amountDiscount")\
        ,expr("sum(CASE WHEN glcf1.isBankCash = 0 AND jrnl1.accountType = 'S'\
        THEN (CASE WHEN jrnl1.debitCreditIndicator = 'H' THEN\
        (-1.0) * jrnl1.amountDC ELSE jrnl1.amountDC END) ELSE 0 END) as amountOther")\
        ,expr("sum(CASE WHEN glcf1.isBankCash = 0 \
        AND jrnl1.accountType = 'S' AND glcf1.isVAT = 1 THEN\
        (CASE WHEN jrnl1.debitCreditIndicator = 'C' \
        THEN (-1.0) * jrnl1.amountDC ELSE jrnl1.amountDC END)\
        ELSE 0 END) as amountOtherVAT")\
        ,expr("sum(CASE WHEN glcf1.isBankCash = 0\
        AND jrnl1.accountType = 'S' AND glcf1.isVAT = 0 THEN \
        (CASE WHEN jrnl1.debitCreditIndicator = 'C'\
        THEN (-1.0) * jrnl1.amountDC ELSE jrnl1.amountDC END) \
        ELSE 0 END) as amountOtherNonVAT")\
        ,expr("sum(CASE WHEN jrnl1.accountType IN ('D') THEN \
        (CASE WHEN jrnl1.debitCreditIndicator = 'C' THEN \
        (-1.0) * jrnl1.cashDiscountAmount ELSE\
        jrnl1.cashDiscountAmount END) ELSE 0 END)\
        + sum(CASE WHEN glcf1.isBankCash = 0 AND jrnl1.accountType= 'S' \
        THEN (CASE WHEN jrnl1.debitCreditIndicator = 'C'\
        THEN (-1.0) * jrnl1.amountDC ELSE jrnl1.amountDC END) \
        ELSE 0 END) as diffDiscountOther") )\
        .filter("round(amountDiscount,2) <> 0 and  round(diffDiscountOther,2)=0")
        
        fin_L1_TMP_ClearingAccountDiscountCandidateByDoc=\
        fin_L1_TD_Journal.alias("jrnl1")\
        .join(fin_L1_TMP_ClearingAccountCashDiscountByDoc.alias("clab1"),\
        expr("( jrnl1.documentNumber = clab1.documentNumber\
         AND jrnl1.companyCode = clab1.companyCode\
         AND jrnl1.fiscalYear = clab1.fiscalYear )"),"inner")\
        .groupBy("jrnl1.companyCode"\
        ,"jrnl1.fiscalYear"\
        ,"jrnl1.documentNumber"\
        ,"jrnl1.accountNumber")\
        .agg( expr("sum(CASE WHEN jrnl1.debitCreditIndicator = 'C' THEN\
        (-1.0) * jrnl1.amountDC ELSE jrnl1.amountDC END) as amountAccountNumber")\
        ,expr("max(clab1.amountOtherNonVAT) as amountOtherNonVAT")\
        ,expr("max(clab1.diffDiscountOther) as diffDiscountOther")\
        ,expr("sum(CASE WHEN jrnl1.debitCreditIndicator = 'C' THEN \
        (-1.0) * jrnl1.amountDC ELSE jrnl1.amountDC END)\
        - max(clab1.amountOtherNonVAT) as diffAccountNumberOtherNonVAT") )\
        .filter("round(diffAccountNumberOtherNonVAT,2)=0")
        
        fin_L1_TMP_ClearingPurchaseReceiptCashDiscountByDoc=\
        fin_L1_TD_Journal.alias("jrnl")\
        .join(fin_L1_TMP_GLAccountClassification.alias("glacc"),\
        expr("( glacc.accountNumber = jrnl.accountNumber\
         AND glacc.companyCode = jrnl.companyCode )"),"inner")\
        .filter(expr("jrnl.documentStatus = 'N' \
        		AND jrnl.referenceTransaction = 'RMRP'"))\
        .groupBy("jrnl.documentNumber"\
        ,"jrnl.companyCode"\
        ,"jrnl.fiscalYear")\
        .agg( expr("SUM(CASE WHEN jrnl.accountType = 'K' THEN \
        (CASE WHEN jrnl.debitCreditIndicator = 'C' THEN \
        (-1.0) * jrnl.amountLC ELSE jrnl.amountLC END) ELSE NULL END) as amountAP")\
        ,expr("SUM(CASE WHEN jrnl.accountType IN ('S', 'M') \
        AND (jrnl.lineItemID = 'W' \
        OR jrnl.transactionKey = 'WRX') THEN \
        (CASE WHEN jrnl.debitCreditIndicator = 'C' THEN \
        (-1.0) * jrnl.amountLC ELSE jrnl.amountLC END) \
        ELSE NULL END) as amountGRIR")\
        ,expr("SUM(CASE WHEN ((jrnl.accountType IN ('S', 'M') \
        AND (jrnl.lineItemID = 'W' OR jrnl.transactionKey IN ('WRX'))) OR \
        (jrnl.accountType IN ('A') AND (jrnl.lineItemID = 'A' OR\
        jrnl.transactionKey IN ('ANL'))) \
        OR jrnl.postingKey IN ('81', '91')) THEN \
        (CASE WHEN jrnl.debitCreditIndicator = 'C' THEN \
        (-1.0) * jrnl.cashDiscountAmount ELSE jrnl.cashDiscountAmount END) \
        ELSE 0 END) as amountCashDiscount")\
        ,expr("SUM(CASE WHEN jrnl.accountType = 'S' AND glacc.isPL = 0 \
        AND jrnl.cashDiscountAmount = 0 \
        AND (jrnl.lineItemID = 'Z' OR jrnl.transactionKey = 'SKV') THEN\
       (CASE WHEN jrnl.debitCreditIndicator = 'C' THEN \
        (-1.0) * jrnl.amountLC ELSE jrnl.amountLC END) \
        ELSE NULL END) as amountDiscountHeuristic")\
        ,expr("ROUND(SUM(CASE WHEN jrnl.accountType IN ('S', 'M', 'A') \
        AND (jrnl.lineItemID = 'W' OR jrnl.transactionKey IN \
        ('WRX', 'ANL') OR jrnl.postingKey IN ('81', '91')) THEN \
        (CASE WHEN jrnl.debitCreditIndicator = 'C' THEN \
        (-1.0) * jrnl.cashDiscountAmount ELSE jrnl.cashDiscountAmount END) ELSE 0 END)- \
        SUM(CASE WHEN jrnl.accountType = 'S' AND glacc.isPL = 0 \
        AND jrnl.cashDiscountAmount = 0 \
        AND (jrnl.lineItemID = 'Z' OR jrnl.transactionKey = 'SKV') THEN \
        (CASE WHEN jrnl.debitCreditIndicator = 'C' THEN \
        (-1.0) * jrnl.amountLC ELSE jrnl.amountLC END)\
        ELSE 0 END), 2) as diffCashDiscountHeuristic") \
        ,expr(" ROUND(SUM(CASE WHEN ((jrnl.accountType IN ('S', 'M')\
        AND (jrnl.lineItemID = 'W' OR jrnl.transactionKey IN \
        ('WRX'))) OR (jrnl.accountType IN ('A') AND (jrnl.lineItemID = 'A'\
        OR jrnl.transactionKey IN ('ANL'))) OR \
        jrnl.postingKey IN ('81', '91')) THEN \
        (CASE WHEN jrnl.debitCreditIndicator = 'C' THEN \
        (-1.0) * jrnl.cashDiscountAmount ELSE \
        jrnl.cashDiscountAmount END) ELSE 0 END), 2) \
        as diffCashDiscountHeuristicToEqualZero"))\
        .filter("amountAP is not null and amountGRIR is not null and \
        amountDiscountHeuristic is not null and diffCashDiscountHeuristicToEqualZero=0 ")
        
        fin_L1_TMP_ClearingAccountPurchaseCashDiscountByDoc=\
        fin_L1_TD_Journal.alias("jrnl1")\
        .join(fin_L1_TMP_ClearingPurchaseReceiptCashDiscountByDoc.alias("clpr1"),\
        expr("( jrnl1.documentNumber = clpr1.documentNumber\
         AND jrnl1.companyCode = clpr1.companyCode\
         AND jrnl1.fiscalYear = clpr1.fiscalYear ) "),"inner")\
        .join(fin_L1_TMP_GLAccountClassification.alias("glacc"),\
        expr("( glacc.accountNumber = jrnl1.accountNumber\
         AND glacc.companyCode = jrnl1.companyCode )"),"inner")\
        .groupBy("jrnl1.companyCode"\
        ,"jrnl1.fiscalYear"\
        ,"jrnl1.documentNumber"\
        ,"jrnl1.accountNumber")\
        .agg( expr("sum(CASE WHEN jrnl1.debitCreditIndicator = 'C' THEN \
        (-1.0) * jrnl1.amountLC ELSE jrnl1.amountLC END) as amountAccountNumber")\
        ,expr("max(clpr1.amountDiscountHeuristic) as amountDiscountHeuristic")\
        ,expr("SUM(CASE WHEN jrnl1.accountType = 'S' AND glacc.isPL = 0 \
        AND jrnl1.cashDiscountAmount = 0 AND (jrnl1.lineItemID = 'Z'\
        OR jrnl1.transactionKey = 'SKV') THEN \
        (CASE WHEN jrnl1.debitCreditIndicator = 'C' THEN \
        (-1.0) * jrnl1.amountLC ELSE jrnl1.amountLC END) \
        ELSE NULL END)- max(clpr1.amountDiscountHeuristic)\
        as diffAccountNumberDiscountHeuristic") )\
        .filter("round(diffAccountNumberDiscountHeuristic,2)=0")
        
        fin_L1_TMP_ClearingAccountCashDiscountCandidate=\
        fin_L1_TMP_ClearingAccountDiscountCandidateByDoc.alias("cladc1")\
        .selectExpr("cladc1.accountNumber as accountNumber","1 as heuristicRule").distinct()\
        .union(fin_L1_TMP_ClearingAccountPurchaseCashDiscountByDoc.alias("clapcd1")\
        .selectExpr("clapcd1.accountNumber as accountNumber",\
        "2 as heuristicRule").distinct()).alias("uni")\
        .groupBy("uni.accountNumber")\
        .agg(expr("MIN(uni.heuristicRule) as heuristicRule"))
        
        fin_L1_TMP_GLAccountClassification=\
        fin_L1_TMP_GLAccountClassification.alias("glcf1")\
        .join(fin_L1_TMP_ClearingAccountCashDiscountCandidate.alias("clacd1"),\
        expr("( clacd1.accountNumber = glcf1.accountNumber\
         AND glcf1.isBankCash = 0\
         AND glcf1.isTradeAR = 0\
         AND glcf1.isVAT = 0\
         AND glcf1.isCashDiscount = 0\
         AND glcf1.isPL = 1 )"),"left")\
        .groupBy("glcf1.accountNumber"\
        ,"glcf1.isBankCharge"\
        ,"glcf1.companyCode"\
        ,"glcf1.isCurrencyFX"\
        ,"glcf1.accountCategory"\
        ,"glcf1.chartOfAccount"\
        ,"glcf1.treeLevel"\
        ,"glcf1.isInterCoAccount"\
        ,"glcf1.isBankCash"\
        ,"glcf1.isVAT"\
        ,"glcf1.isTradeAR"\
        ,"glcf1.trackCategory"\
        ,"glcf1.isRevenue"\
        ,"glcf1.accountDescription"\
        ,"glcf1.isTradeAP"\
        ,"glcf1.isPL")\
        .agg(expr("max(case when clacd1.accountNumber is not null then\
         1 else glcf1.isCashDiscount end) as isCashDiscount")\
        ,expr("max(case when clacd1.accountNumber is not null then\
        clacd1.heuristicRule else glcf1.heuristicRule end) as heuristicRule"))
        
        fin_L1_TMP_GLClearingAccountRateDiffAccount=\
        fin_L1_TD_Journal.alias("jrnl1")\
        .join(fin_L1_TMP_GLAccountClassification.alias("glcf1"),\
        expr("( jrnl1.accountNumber = glcf1.accountNumber\
         AND jrnl1.companyCode = glcf1.companyCode\
         AND jrnl1.transactionKey = 'KDF' )"),"inner")\
        .selectExpr("chartOfAccount as chartOfAccount"\
        ,"jrnl1.accountNumber as accountNumber").distinct()
        
        fin_L1_TMP_GLClearingAccountRateDiffAccount=\
        fin_L1_TMP_GLClearingAccountRateDiffAccount\
        .select(col("chartOfAccount"),col("accountNumber"))\
        .union(fin_L1_STG_AccountConfiguration.alias("accnfg")\
        .join(fin_L1_TMP_GLClearingAccountRateDiffAccount.alias("rtdff"),\
        expr("( accnfg.configurationName = 'StandardAccountsKDF'\
         AND accnfg.accountNumber = rtdff.accountNumber\
         AND accnfg.chartOfAccount = rtdff.chartOfAccount )"),"left")\
        .filter(expr("accnfg.accountNumber			IS NULL"))\
        .selectExpr("accnfg.chartOfAccount as chartOfAccount"\
        ,"accnfg.accountNumber as accountNumber"))
        
        fin_L1_TMP_GLAccountClassification=\
        fin_L1_TMP_GLAccountClassification.alias("glcf1")\
        .join(fin_L1_TMP_GLClearingAccountRateDiffAccount.alias("glca1"),\
        expr("( glca1.accountNumber = glcf1.accountNumber\
         AND glca1.chartOfAccount = glcf1.chartOfAccount\
         AND glcf1.isBankCash = 0\
         AND glcf1.isTradeAR = 0\
         AND glcf1.isVAT = 0\
         AND glcf1.isRevenue = 0\
         AND glcf1.isCashDiscount = 0\
         AND glcf1.isPL = 1 )"),"left")\
        .groupBy("glcf1.accountNumber"\
        ,"glcf1.isBankCharge"\
        ,"glcf1.companyCode"\
        ,"glcf1.accountCategory"\
        ,"glcf1.heuristicRule"\
        ,"glcf1.chartOfAccount"\
        ,"glcf1.treeLevel"\
        ,"glcf1.isInterCoAccount"\
        ,"glcf1.isBankCash"\
        ,"glcf1.isVAT"\
        ,"glcf1.isTradeAR"\
        ,"glcf1.trackCategory"\
        ,"glcf1.isRevenue"\
        ,"glcf1.accountDescription"\
        ,"glcf1.isCashDiscount"\
        ,"glcf1.isTradeAP"\
        ,"glcf1.isPL")\
        .agg(expr("max(case when glca1.accountNumber is not null then \
        1 else glcf1.isCurrencyFX end) as isCurrencyFX"))
        
        fin_L1_TMP_GLAccountClassification=\
        fin_L1_TMP_GLAccountClassification.withColumn("isCurrencyFX",\
                                                      expr("case when \
                                                      glcf1.isBankCash	= 0 \
        			AND glcf1.isBankCharge			= 0 \
        			AND glcf1.isTradeAR				= 0 \
        			AND glcf1.isVAT					= 0 \
        			AND glcf1.isRevenue				= 0 \
        			AND glcf1.isCashDiscount		= 0 \
        			AND glcf1.isPL					= 1 \
        			AND (lower(glcf1.accountDescription) LIKE '%kursgew%'\
                  OR lower(glcf1.accountDescription) LIKE '%currency gain%' \
        			OR	lower(glcf1.accountDescription) LIKE '%kursverl%' \
                    OR lower(glcf1.accountDescription) LIKE '%currency loss%' \
        			OR	lower(glcf1.accountDescription)  LIKE '%kursdiff%' \
                    OR glcf1.accountDescription LIKE '%currency diff%' \
        			) then 1 else isCurrencyFX end"))
        fin_L1_TMP_GLAccountClassification=\
        fin_L1_TMP_GLAccountClassification.alias("glcf1")\
        .join(fin_L1_TMP_GLClearingAccountRateDiffAccount.alias("glca1"),\
        expr("( glca1.accountNumber = glcf1.accountNumber\
         AND glca1.chartOfAccount = glcf1.chartOfAccount\
         AND glcf1.isBankCash = 0\
         AND glcf1.isTradeAR = 0\
         AND glcf1.isVAT = 0\
         AND glcf1.isRevenue = 0\
         AND glcf1.isCashDiscount = 0\
         AND glcf1.isPL = 1 )"),"left")\
        .groupBy("glcf1.accountNumber"\
        ,"glcf1.isBankCharge"\
        ,"glcf1.companyCode"\
        ,"glcf1.accountCategory"\
        ,"glcf1.heuristicRule"\
        ,"glcf1.chartOfAccount"\
        ,"glcf1.treeLevel"\
        ,"glcf1.isInterCoAccount"\
        ,"glcf1.isBankCash"\
        ,"glcf1.isVAT"\
        ,"glcf1.isTradeAR"\
        ,"glcf1.trackCategory"\
        ,"glcf1.isRevenue"\
        ,"glcf1.accountDescription"\
        ,"glcf1.isCashDiscount"\
        ,"glcf1.isTradeAP"\
        ,"glcf1.isPL")\
        .agg(expr("max(case when glca1.accountNumber is not null then 1 \
        else glcf1.isCurrencyFX end) as isCurrencyFX"))
        
        fin_L1_TMP_GLAccountClassification=\
        fin_L1_TMP_GLAccountClassification.withColumn("isCurrencyFX",\
                                                      expr("case when \
                                                      glcf1.isBankCash			= 0 \
        			AND glcf1.isBankCharge			= 0 \
        			AND glcf1.isTradeAR				= 0 \
        			AND glcf1.isVAT					= 0 \
        			AND glcf1.isRevenue				= 0 \
        			AND glcf1.isCashDiscount		= 0 \
        			AND glcf1.isPL					= 1 \
        			AND (lower(glcf1.accountDescription) LIKE '%kursgew%'  \
                    OR glcf1.accountDescription LIKE '%currency gain%' \
        			OR	 lower(glcf1.accountDescription)  LIKE '%kursverl%'\
                   OR glcf1.accountDescription LIKE '%currency loss%' \
        			OR	 lower(glcf1.accountDescription)  LIKE '%kursdiff%' \
                    OR glcf1.accountDescription LIKE '%currency diff%' \
        			) then 1 else isCurrencyFX end"))
        
        fin_L1_TMP_ClearingAccountTaxAuthority=\
        ptp_L1_MD_Vendor.alias("ven1")\
        .join(fin_L1_TD_Journal.alias("jou1"),\
        expr("( ven1.vendorNumber = jou1.vendorNumber\
         AND ven1.vendorCompanyCode = jou1.companyCode ) "),"inner")\
        .join(fin_L1_TMP_GLAccountClassification.alias("glcf1"),\
        expr("( jou1.accountNumber = glcf1.accountNumber\
         AND jou1.companyCode = glcf1.companyCode )"),"inner")\
        .filter(expr("upper(ven1.vendorAccountGroup)			= 'TAX' \
        			AND upper(jou1.accountType)				= 'K' \
        			AND lower(glcf1.accountDescription)		LIKE '%tax%'"))\
        .selectExpr("jou1.companyCode as companyCode"\
        ,"jou1.accountNumber as accountNumber").distinct()
        
        
        fin_L1_STG_GLAccountClassification=\
        fin_L1_TMP_GLAccountClassification.alias("glac1")\
        .join(fin_L1_TMP_ClearingAccountTaxAuthority.alias("cata1"),\
        expr("( glac1.companyCode = cata1.companyCode\
         AND glac1.accountNumber = cata1.accountNumber )"),"left")\
        .selectExpr("glac1.accountNumber as accountNumber"\
        ,"glac1.companyCode as companyCode"\
        ,"glac1.chartOfAccount as chartOfAccount"\
        ,"glac1.accountDescription as accountDescription"\
        ,"glac1.accountCategory as accountCategory"\
        ,"glac1.treeLevel as treeLevel"\
        ,"glac1.trackCategory as trackCategory"\
        ,"glac1.isBankCash as isBankCash"\
        ,"glac1.isTradeAR as isTradeAR"\
        ,"glac1.isTradeAP as isTradeAP"\
        ,"glac1.isRevenue as isRevenue"\
        ,"glac1.isVAT as isVAT"\
        ,"glac1.isCashDiscount as isCashDiscount"\
        ,"glac1.isBankCharge as isBankCharge"\
        ,"glac1.isCurrencyFX as isCurrencyFX"\
        ,"glac1.isPL as isPL"\
        ,"glac1.isInterCoAccount as isInterCoAccount"\
        ,"glac1.heuristicRule as heuristicRule"\
        ,"CASE WHEN cata1.accountNumber IS NOT NULL THEN \
        1 ELSE 0 END as isTaxAuthority")
        
        
        fin_L1_STG_GLAccountClassification =\
           objDataTransformation.gen_convertToCDMandCache \
        (fin_L1_STG_GLAccountClassification,'fin','L1_STG_GLAccountClassification',True)
        
        executionStatus = "fin_L1_STG_GLAccountClassification populated sucessfully"
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

    except Exception as err:
        executionStatus = objGenHelper.gen_exceptionDetails_log()       
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]



