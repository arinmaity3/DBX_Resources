# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import expr,col,lit,min,max,coalesce,trim,abs,instr

def gen_L1_STG_ClearingBox_01_Initialization_populate():
    try:
        logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
        global gen_L1_STG_ClearingBox_01_Journal
        global gen_L1_STG_ClearingBox_01_AllLogisticItem

        objGenHelper = gen_genericHelper()
        objDataTransformation = gen_dataTransformation()

        startDate =  str(parse(objGenHelper\
		.gen_lk_cd_parameter_get('GLOBAL', 'START_DATE')).date())
        endDate =  str(parse(objGenHelper\
		.gen_lk_cd_parameter_get('GLOBAL', 'END_DATE')).date())
        erpSourceSystemID = objGenHelper\
		.gen_lk_cd_parameter_get('GLOBAL', 'ERP_SYSTEM_ID')

        gen_L1_TMP_ClearingBox_01_BasicPopulation      = None
        gen_L1_TMP_ClearingBox_01_DefineIdClearingChain= None
        gen_L1_TMP_ClearingBox_01_AllUnclearingChain   = None
        gen_L1_TMP_ClearingBox_01_CollectedIdGLJELine  = None
        gen_L1_TMP_ClearingBox_01_BasicPopulation_1    = None
        gen_L1_TMP_ClearingBox_01_BasicPopulation_2    = None
        gen_L1_TMP_ClearingBox_01_BasicPopulation_3    = None
        gen_L1_TMP_ClearingBox_01_BasicPopulation_4    = None
        gen_L1_TMP_ClearingBox_01_BasicPopulation_5    = None

        defaultGLSpecialIndicator=\
		gen_L1_STG_ClearingBoxBusinessDataMapping.alias("map")\
        .filter(expr("map.businessDatatype = 'GL Special Indicator' \
         AND map.sourceSystemValue = '' \
         AND map.sourceERPSystemID = '"+erpSourceSystemID+"'"))\
        .select("targetSystemvalue").first()

        if defaultGLSpecialIndicator is not None:
          defaultGLSpecialIndicator=defaultGLSpecialIndicator[0]
        else:
           defaultGLSpecialIndicator = 'None'

        gen_L1_STG_ClearingBox_01_Journal=\
        fin_L1_STG_JELineItem.alias("jlli1")\
        .join(fin_L1_TD_Journal.alias("jrnl"),\
        expr("( jrnl.documentNumber = jlli1.GLJEDocumentNumber\
         AND jrnl.lineItem = jlli1.GLJELineNumber\
         AND jrnl.fiscalYear = jlli1.financialYear\
         AND jrnl.companyCode = jlli1.companyCode ) "),"left")\
        .join(fin_L1_STG_GLAccountClassification.alias("glc1"),\
        expr("( jrnl.accountNumber = glc1.accountNumber\
         AND jrnl.companyCode = glc1.companyCode ) "),"left")\
        .join(gen_L1_STG_ClearingBoxBusinessDataMapping.alias("cbbdm1"),\
        expr("( cbbdm1.businessDatatype = 'GL Account Type'\
         AND cbbdm1.sourceSystemValue = jrnl.accountType ) "),"left")\
        .join(gen_L1_STG_ClearingBoxBusinessDataMapping.alias("cbbdm1_2"),\
        expr("( cbbdm1_2.businessDatatype = 'GL Posting Key'\
         AND cbbdm1_2.sourceSystemValue = jrnl.postingKey ) "),"left")\
        .join(gen_L1_STG_ClearingBoxBusinessDataMapping.alias("cbbdm1_3"),\
        expr("( cbbdm1_3.businessDatatype = 'GL Document Type'\
         AND cbbdm1_3.sourceSystemValue = jrnl.documentType ) "),"left")\
        .join(gen_L1_STG_ClearingBoxBusinessDataMapping.alias("cbbdm1_4"),\
        expr("( cbbdm1_4.businessDatatype ='GL Special Indicator'\
         AND cbbdm1_4.sourceSystemValue = concat(\
		 rtrim(ltrim(jrnl.accountType)), '/' ,\
		 rtrim(ltrim(jrnl.GLSpecialIndicator))) )"),"left")\
        .selectExpr("jlli1.idGLJE as idGLJE"\
        ,"jlli1.idGLJELine as idGLJELine"\
        ,"jlli1.client as clearingBoxClientERP"\
        ,"jlli1.companyCode as clearingBoxCompanyCode"\
        ,"jlli1.financialYear as clearingBoxGLFiscalYear"\
        ,"jrnl.financialPeriod as clearingBoxGLFinancialPeriod"\
        ,"jrnl.documentStatus as clearingBoxGLDocumentStatus"\
        ,"jrnl.postingDate as clearingBoxGLPostingDate"\
        ,"jrnl.documentType as clearingBoxGLDocumentTypeERP"\
        ,"(CASE when \
        ISNULL(cbbdm1_3.targetSystemValue) then \
        jrnl.documentType else \
        cbbdm1_3.targetSystemValue END) as clearingBoxGLDocumentType"\
                ,"jlli1.GLJEDocumentNumber as clearingBoxGLJEDocument"\
                ,"jlli1.GLJELineNumber as clearingBoxGLJELineNumber"\
                ,"jrnl.clearingDocumentNumber as clearingBoxGLClearingDocument"\
                ,"jrnl.clearingDate as clearingBoxGLClearingPostingDate"\
                ,"jrnl.postingKey as clearingBoxGLPostingKeyERP"\
                ,"(CASE when \
        ISNULL(cbbdm1_2.targetSystemValue) then \
        jrnl.postingKey else \
        cbbdm1_2.targetSystemValue END) as clearingBoxGLPostingKey"\
                ,"jrnl.accountNumber as clearingBoxGLAccountNumber"\
                ,"(CASE when \
        jrnl.documentNumber IS NOT NULL AND glc1.accountCategory IS NULL then \
        '#NA#' else \
        glc1.accountCategory END) as clearingBoxGLAccountCategory"\
                ,"jrnl.debitCreditIndicator as clearingBoxGLDebitCreditIndicator"\
                ,"(CASE when \
        jrnl.debitCreditIndicator = 'C' then \
        (-1.0) * jrnl.amountLC else \
        jrnl.amountLC END) as clearingBoxGLAmountLC"\
                ,"(CASE when \
        jrnl.debitCreditIndicator = 'C' then \
        (-1.0) * abs(jrnl.cashDiscountAmount) else \
        abs(jrnl.cashDiscountAmount) END) as clearingBoxGLAmountDiscountLC"\
                ,"jrnl.transactionCode as clearingBoxGLTransactionCodeERP"\
                ,"jrnl.headerDescription as clearingBoxGLHeaderDescription"\
                ,"jrnl.lineDescription as clearingBoxGLLineDescription"\
                ,"(CASE when \
        ISNULL(jrnl.referenceSubledger) then \
        '#NA#' else \
        jrnl.referenceSubledger END) as clearingBoxGLReferenceSLSource"\
                ,"(CASE when \
        ISNULL(jrnl.referenceSubledgerDocumentNumber) then \
        '#NA#' else \
        jrnl.referenceSubledgerDocumentNumber END) \
        as clearingBoxGLReferenceSLDocument"\
                ,"CASE when \
        jrnl.referenceSubledger='Sales S/L'		then \
        (CASE when \
        instr(jrnl.referenceSubledgerDocumentNumber,'-') > 0 then \
        substring(jrnl.referenceSubledgerDocumentNumber,1,\
        (instr(jrnl.referenceSubledgerDocumentNumber,'-')-1)) else \
        jrnl.referenceSubledgerDocumentNumber END) \
                     when \
        jrnl.referenceSubledger='Purchase S/L'	then \
        (LEFT(rtrim(ltrim(jrnl.referenceSubledgerDocumentNumber)), 10)) \
                     when \
        jrnl.referenceSubledger='Inventory S/L'	then \
        (LEFT(rtrim(ltrim(jrnl.referenceSubledgerDocumentNumber)), 10)) \
                     else \
        '' END as clearingBoxSLDocumentNumber"\
                ,"CASE when \
        jrnl.referenceSubledger= 'Sales S/L' then \
        jlli1.financialYear when \
         jrnl.referenceSubledger='Purchase S/L' then \
        (SUBSTRING(rtrim(ltrim(jrnl.referenceSubledgerDocumentNumber)), 11, 4)) when \
        jrnl.referenceSubledger= 'Inventory S/L' then \
        (SUBSTRING(rtrim(ltrim(jrnl.referenceSubledgerDocumentNumber)), 11, 4)) else \
        '' END as clearingBoxSLFiscalYear"\
                ,"cast('' as varchar(50)) as clearingBoxSLDocumentTypeERP"\
                ,"cast('' as varchar(100)) as clearingBoxSLDocumentType"\
                ,"cast(0 as boolean) as isClearingBoxSLSubsequentDocument"\
                ,"jrnl.billingDocumentNumber as clearingBoxGLBillingDocument"\
                ,"cast((CASE when \
        jrnl.documentNumber IS NOT NULL then \
        '' else \
        NULL END) as varchar(50)) as clearingBoxGLPurchaseOrder"\
                ,"jrnl.customerNumber as clearingBoxGLCustomerNumber"\
                ,"jrnl.vendorNumber as clearingBoxGLVendorNumber"\
                ,"rtrim(ltrim(jrnl.GLSpecialIndicator)) as clearingBoxGLSpecialIndicatorERP"\
                ,"CASE when \
        jrnl.GLSpecialIndicator = '' then \
        '"+defaultGLSpecialIndicator+"' else \
        cbbdm1_4.targetSystemValue END as clearingBoxGLSpecialIndicator"\
                ,"rtrim(ltrim(jrnl.accountType)) as clearingBoxAccountTypeERP"\
                ,"(CASE when \
        ISNULL(cbbdm1.targetSystemValue) then \
        jrnl.accountType else \
        cbbdm1.targetSystemValue END) as clearingBoxAccountType"\
                ,"jrnl.referenceDocumentNumber as clearingBoxGLReferenceDocument"\
                ,"(CASE when \
        ISNULL(jrnl.createdBy) then \
        '' else \
        jrnl.createdBy END) as clearingBoxGLCreatedByUser"\
                ,"CASE when \
        jrnl.reversalDocumentNumber <> '' OR (case when \
        jrnl.reversalType is null then \
        0 else \
        jrnl.reversalType end) > 0 then \
        1 else \
        0 END   as isReversedOrReversal"\
                ,"concat(rtrim(ltrim(jrnl.accountType)), '/' ,\
        		rtrim(ltrim(jrnl.GLSpecialIndicator))) \
        		as clearingBoxAccountTypeSpecialIndicator"\
                ,"(CASE when \
        jrnl.documentNumber IS NOT NULL then \
        'gl_journal' else \
        'unknown' END) as clearingBoxOriginOfItem").cache()
        
        
        gen_L1_STG_ClearingBox_01_Journal=\
        gen_L1_STG_ClearingBox_01_Journal.alias("stgcbj01")\
        .join(fin_L1_TD_Journal.alias("jrnl"),\
        expr("( stgcbj01.clearingBoxGLJEDocument = jrnl.reversalDocumentNumber\
         AND stgcbj01.clearingBoxGLFiscalYear = jrnl.reversalFiscalYear\
         AND stgcbj01.clearingBoxCompanyCode = jrnl.companyCode\
         AND stgcbj01.isReversedOrReversal = 0 )"),"left")\
        .groupBy("stgcbj01.clearingBoxGLLineDescription"\
        ,"stgcbj01.clearingBoxGLReferenceSLDocument"\
        ,"stgcbj01.clearingBoxCompanyCode"\
        ,"stgcbj01.isClearingBoxSLSubsequentDocument"\
        ,"stgcbj01.clearingBoxGLAmountLC"\
        ,"stgcbj01.clearingBoxGLJELineNumber"\
        ,"stgcbj01.clearingBoxGLFiscalYear"\
        ,"stgcbj01.clearingBoxGLDocumentTypeERP"\
        ,"stgcbj01.clearingBoxGLCreatedByUser"\
        ,"stgcbj01.clearingBoxGLHeaderDescription"\
        ,"stgcbj01.clearingBoxGLBillingDocument"\
        ,"stgcbj01.clearingBoxGLCustomerNumber"\
        ,"stgcbj01.clearingBoxGLAccountCategory"\
        ,"stgcbj01.clearingBoxAccountTypeERP"\
        ,"stgcbj01.clearingBoxGLDocumentStatus"\
        ,"stgcbj01.clearingBoxGLPurchaseOrder"\
        ,"stgcbj01.clearingBoxSLDocumentType"\
        ,"stgcbj01.idGLJE"\
        ,"stgcbj01.clearingBoxSLDocumentTypeERP"\
        ,"stgcbj01.clearingBoxGLVendorNumber"\
        ,"stgcbj01.clearingBoxSLFiscalYear"\
        ,"stgcbj01.clearingBoxGLFinancialPeriod"\
        ,"stgcbj01.clearingBoxGLClearingPostingDate"\
        ,"stgcbj01.clearingBoxAccountTypeSpecialIndicator"\
        ,"stgcbj01.clearingBoxGLPostingKey"\
        ,"stgcbj01.clearingBoxGLAmountDiscountLC"\
        ,"stgcbj01.clearingBoxGLPostingDate"\
        ,"stgcbj01.clearingBoxGLPostingKeyERP"\
        ,"stgcbj01.clearingBoxOriginOfItem"\
        ,"stgcbj01.clearingBoxGLSpecialIndicator"\
        ,"stgcbj01.clearingBoxGLReferenceSLSource"\
        ,"stgcbj01.clearingBoxGLClearingDocument"\
        ,"stgcbj01.clearingBoxGLReferenceDocument"\
        ,"stgcbj01.clearingBoxGLDebitCreditIndicator"\
        ,"stgcbj01.clearingBoxClientERP"\
        ,"stgcbj01.clearingBoxGLTransactionCodeERP"\
        ,"stgcbj01.clearingBoxGLAccountNumber"\
        ,"stgcbj01.clearingBoxSLDocumentNumber"\
        ,"stgcbj01.clearingBoxAccountType"\
        ,"stgcbj01.clearingBoxGLSpecialIndicatorERP"\
        ,"stgcbj01.clearingBoxGLJEDocument"\
        ,"stgcbj01.clearingBoxGLDocumentType"\
        ,"stgcbj01.idGLJELine")\
        .agg(expr("max(case when \
        jrnl.reversalDocumentNumber is not null then \
        1 else \
        stgcbj01.isReversedOrReversal end) as isReversedOrReversal"))
        
        
        gen_L1_STG_ClearingBox_01_Journal=\
        gen_L1_STG_ClearingBox_01_Journal.alias("stgcbj01")\
        .join(fin_L1_TD_AccountReceivable.alias("acrv1"),\
        expr("( stgcbj01.clearingBoxGLJEDocument = acrv1.GLJEDocumentNumber\
         AND stgcbj01.clearingBoxGLJELineNumber = acrv1.GLJELineNumber\
         AND stgcbj01.clearingBoxGLFiscalYear = acrv1.financialYear\
         AND stgcbj01.clearingBoxCompanyCode = acrv1.companyCode\
         AND stgcbj01.clearingBoxClientERP = acrv1.accountReceivableClientCode\
         AND stgcbj01.clearingBoxOriginOfItem = 'unknown' ) "),"left")\
        .join(fin_L1_STG_GLAccountClassification.alias("glc1"),\
        expr("( acrv1.accountNumber = glc1.accountNumber\
         AND acrv1.companyCode = glc1.companyCode ) "),"left")\
        .join(gen_L1_STG_ClearingBoxBusinessDataMapping.alias("cbbdm1"),\
        expr("( cbbdm1.businessDatatype ='GL Account Type'\
         AND cbbdm1.sourceSystemValue = acrv1.accountType ) "),"left")\
        .join(gen_L1_STG_ClearingBoxBusinessDataMapping.alias("cbbdm1_2"),\
        expr("( cbbdm1_2.businessDatatype ='GL Posting Key'\
         AND cbbdm1_2.sourceSystemValue = acrv1.postingKey ) "),"left")\
        .join(gen_L1_STG_ClearingBoxBusinessDataMapping.alias("cbbdm1_3"),\
        expr("( cbbdm1_3.businessDatatype ='GL Document Type'\
         AND cbbdm1_3.sourceSystemValue = acrv1.documentType ) "),"left")\
        .join(gen_L1_STG_ClearingBoxBusinessDataMapping.alias("cbbdm1_4"),\
        expr("( cbbdm1_4.businessDatatype ='GL Special Indicator'\
         AND cbbdm1_4.sourceSystemValue = concat(rtrim(ltrim(acrv1.accountType)),\
		 '/' ,rtrim(ltrim(acrv1.GLSpecialIndicator))) )"),"left")\
        .selectExpr("case when \
        acrv1.GLJEDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLFinancialPeriod ,acrv1.financialPeriod) else \
        stgcbj01.clearingBoxGLFinancialPeriod end as clearingBoxGLFinancialPeriod"\
                ,"case when \
        acrv1.GLJEDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLDocumentStatus ,acrv1.documentStatus) else \
        stgcbj01.clearingBoxGLDocumentStatus end as clearingBoxGLDocumentStatus"\
                ,"case when \
        acrv1.GLJEDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLDebitCreditIndicator ,acrv1.debitCreditIndicator) else \
        stgcbj01.clearingBoxGLDebitCreditIndicator end as clearingBoxGLDebitCreditIndicator"\
                ,"case when \
        acrv1.GLJEDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLPostingDate ,acrv1.GLJEPostingDate ,'1900-01-01') else \
        stgcbj01.clearingBoxGLPostingDate end as clearingBoxGLPostingDate"\
                ,"case when \
        acrv1.GLJEDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLDocumentTypeERP ,acrv1.documentType ,'#NA#') else \
        stgcbj01.clearingBoxGLDocumentTypeERP end as clearingBoxGLDocumentTypeERP"\
                ,"case when \
        acrv1.GLJEDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLDocumentType ,cbbdm1_3.targetSystemValue ,'#NA#') else \
        stgcbj01.clearingBoxGLDocumentType end as clearingBoxGLDocumentType"\
                ,"case when \
        acrv1.GLJEDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLClearingDocument ,\
        acrv1.clearingDocumentNumber ,'#NA#') else \
        stgcbj01.clearingBoxGLClearingDocument end as clearingBoxGLClearingDocument"\
                ,"case when \
        acrv1.GLJEDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLClearingPostingDate ,\
        acrv1.documentClearingDate ,'1900-01-01') else \
        stgcbj01.clearingBoxGLClearingPostingDate end as clearingBoxGLClearingPostingDate"\
                ,"case when \
        acrv1.GLJEDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLPostingKeyERP ,acrv1.postingKey ,'#NA#') else \
        stgcbj01.clearingBoxGLPostingKeyERP end as clearingBoxGLPostingKeyERP"\
                ,"case when \
        acrv1.GLJEDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLPostingKey ,cbbdm1_2.targetSystemValue ,'#NA#') else \
        stgcbj01.clearingBoxGLPostingKey end as clearingBoxGLPostingKey"\
                ,"case when \
        acrv1.GLJEDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLAccountNumber ,acrv1.accountNumber ,'#NA#') else \
        stgcbj01.clearingBoxGLAccountNumber end as clearingBoxGLAccountNumber"\
                ,"case when \
        acrv1.GLJEDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLAccountCategory ,glc1.accountCategory ,'#NA#') else \
        stgcbj01.clearingBoxGLAccountCategory end as clearingBoxGLAccountCategory"\
                ,"case when \
        acrv1.GLJEDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLAmountLC ,acrv1.amountLC) else \
        stgcbj01.clearingBoxGLAmountLC end as clearingBoxGLAmountLC"\
                ,"case when \
        acrv1.GLJEDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLAmountDiscountLC ,\
        abs(acrv1.cashDiscountLocalCurrency)) else \
        stgcbj01.clearingBoxGLAmountDiscountLC end as clearingBoxGLAmountDiscountLC"\
                ,"case when \
        acrv1.GLJEDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLTransactionCodeERP ,'#NA#') else \
        stgcbj01.clearingBoxGLTransactionCodeERP end as clearingBoxGLTransactionCodeERP"\
                ,"case when \
        acrv1.GLJEDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLHeaderDescription ,'#NA#') else \
        stgcbj01.clearingBoxGLHeaderDescription end as clearingBoxGLHeaderDescription"\
                ,"case when \
        acrv1.GLJEDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLLineDescription ,\
        acrv1.accountReceivableLineitemDescription ,'#NA#') else \
        stgcbj01.clearingBoxGLLineDescription end as clearingBoxGLLineDescription"\
                ,"case when \
        acrv1.GLJEDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLBillingDocument ,\
        acrv1.billingDocumentNumber ,'#NA#') else \
        stgcbj01.clearingBoxGLBillingDocument end as clearingBoxGLBillingDocument"\
                ,"case when \
        acrv1.GLJEDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLPurchaseOrder ,'#NA#') else \
        stgcbj01.clearingBoxGLPurchaseOrder end as clearingBoxGLPurchaseOrder"\
                ,"case when \
        acrv1.GLJEDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLCustomerNumber ,acrv1.customerNumber ,'#NA#') else \
        stgcbj01.clearingBoxGLCustomerNumber end as clearingBoxGLCustomerNumber"\
                ,"case when \
        acrv1.GLJEDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLVendorNumber ,'#NA#') else \
        stgcbj01.clearingBoxGLVendorNumber end as clearingBoxGLVendorNumber"\
                ,"case when \
        acrv1.GLJEDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLSpecialIndicatorERP ,acrv1.GLSpecialIndicator, '') else \
        stgcbj01.clearingBoxGLSpecialIndicatorERP end as clearingBoxGLSpecialIndicatorERP"\
                ,"case when \
        acrv1.GLJEDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLSpecialIndicator ,\
        cbbdm1_4.targetSystemValue ,'"+defaultGLSpecialIndicator+"') else \
        stgcbj01.clearingBoxGLSpecialIndicator end as clearingBoxGLSpecialIndicator"\
                ,"case when \
        acrv1.GLJEDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxAccountTypeERP ,acrv1.accountType ,'#NA#') else \
        stgcbj01.clearingBoxAccountTypeERP end as clearingBoxAccountTypeERP"\
                ,"case when \
        acrv1.GLJEDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxAccountType ,cbbdm1.targetSystemValue ,'#NA#') else \
        stgcbj01.clearingBoxAccountType end as clearingBoxAccountType"\
                ,"case when \
        acrv1.GLJEDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLReferenceDocument ,\
        acrv1.referenceDocumentNumber ,'#NA#') else \
        stgcbj01.clearingBoxGLReferenceDocument end as clearingBoxGLReferenceDocument"\
                ,"case when \
        acrv1.GLJEDocumentNumber is not null then \
        coalesce(clearingBoxAccountTypeSpecialIndicator ,\
        concat(rtrim(ltrim(acrv1.accountType)), '/' ,\
        rtrim(ltrim(acrv1.GLSpecialIndicator))) ,'#NA#') else \
        stgcbj01.clearingBoxAccountTypeSpecialIndicator end \
        as clearingBoxAccountTypeSpecialIndicator"\
                ,"case when \
        acrv1.GLJEDocumentNumber is not null then \
        'ar_journal' else \
        stgcbj01.clearingBoxOriginOfItem end as clearingBoxOriginOfItem"\
        ,"stgcbj01.clearingBoxSLFiscalYear"\
        ,"stgcbj01.clearingBoxGLReferenceSLDocument"\
        ,"stgcbj01.clearingBoxCompanyCode"\
        ,"stgcbj01.isClearingBoxSLSubsequentDocument"\
        ,"stgcbj01.clearingBoxGLJELineNumber"\
        ,"stgcbj01.clearingBoxSLDocumentType"\
        ,"stgcbj01.clearingBoxClientERP"\
        ,"stgcbj01.clearingBoxSLDocumentNumber"\
        ,"stgcbj01.clearingBoxGLFiscalYear"\
        ,"stgcbj01.isReversedOrReversal"\
        ,"stgcbj01.clearingBoxGLJEDocument"\
        ,"stgcbj01.clearingBoxGLCreatedByUser"\
        ,"stgcbj01.idGLJELine"\
        ,"stgcbj01.idGLJE"\
        ,"stgcbj01.clearingBoxSLDocumentTypeERP"\
        ,"stgcbj01.clearingBoxGLReferenceSLSource")
        
        gen_L1_STG_ClearingBox_01_Journal=\
        gen_L1_STG_ClearingBox_01_Journal.alias("stgcbj01")\
        .join(fin_L1_TD_AccountPayable.alias("apa1"),\
        expr("( stgcbj01.clearingBoxGLJEDocument = apa1.accountPayableDocumentNumber\
         AND stgcbj01.clearingBoxGLJELineNumber = apa1.accountPayableDocumentLineItem\
         AND stgcbj01.clearingBoxGLFiscalYear = apa1.accountPayableFiscalYear\
         AND stgcbj01.clearingBoxCompanyCode = apa1.accountPayableCompanyCode\
         AND stgcbj01.clearingBoxClientERP = apa1.accountPayableClientCode\
         AND stgcbj01.clearingBoxOriginOfItem = 'unknown' ) "),"left")\
        .join(fin_L1_STG_GLAccountClassification.alias("glc1"),\
        expr("( apa1.accountPayableAccountNumber = glc1.accountNumber\
         AND apa1.accountPayableCompanyCode = glc1.companyCode ) "),"left")\
        .join(gen_L1_STG_ClearingBoxBusinessDataMapping.alias("cbbdm1"),\
        expr("( cbbdm1.businessDatatype ='GL Account Type'\
         AND cbbdm1.sourceSystemValue = apa1.accountPayableAccountType ) "),"left")\
        .join(gen_L1_STG_ClearingBoxBusinessDataMapping.alias("cbbdm1_2"),\
        expr("( cbbdm1_2.businessDatatype ='GL Posting Key'\
         AND cbbdm1_2.sourceSystemValue = apa1.accountPayablePostingKey ) "),"left")\
        .join(gen_L1_STG_ClearingBoxBusinessDataMapping.alias("cbbdm1_3"),\
        expr("( cbbdm1_3.businessDatatype ='GL Document Type'\
         AND cbbdm1_3.sourceSystemValue = apa1.accountPayableDocumentType ) "),"left")\
        .join(gen_L1_STG_ClearingBoxBusinessDataMapping.alias("cbbdm1_4"),\
        expr("( cbbdm1_4.businessDatatype ='GL Special Indicator'\
         AND cbbdm1_4.sourceSystemValue = rtrim(ltrim(apa1.accountPayableAccountType))+\
		 '/' +rtrim(ltrim(apa1.accountPayableGLSpecialIndicator)) )"),"left")\
        .selectExpr("case when \
        apa1.accountPayableDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLFinancialPeriod ,apa1.accountPayableFinancialPeriod) else \
        stgcbj01.clearingBoxGLFinancialPeriod end as clearingBoxGLFinancialPeriod"\
                ,"case when \
        apa1.accountPayableDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLDocumentStatus ,apa1.accountPayableDocumentStatus) else \
        stgcbj01.clearingBoxGLDocumentStatus end as clearingBoxGLDocumentStatus"\
                ,"case when \
        apa1.accountPayableDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLDebitCreditIndicator ,\
        apa1.accountPayableDebitCreditIndicator) else \
        stgcbj01.clearingBoxGLDebitCreditIndicator end as clearingBoxGLDebitCreditIndicator"\
                ,"case when \
        apa1.accountPayableDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLPostingDate ,\
        apa1.accountPayablePostingDate ,'1900-01-01') else \
        stgcbj01.clearingBoxGLPostingDate end as clearingBoxGLPostingDate"\
                ,"case when \
        apa1.accountPayableDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLDocumentTypeERP ,\
        apa1.accountPayableDocumentType ,'#NA#') else \
        stgcbj01.clearingBoxGLDocumentTypeERP end as clearingBoxGLDocumentTypeERP"\
                ,"case when \
        apa1.accountPayableDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLDocumentType ,\
        cbbdm1_3.targetSystemValue ,'#NA#') else \
        stgcbj01.clearingBoxGLDocumentType end as clearingBoxGLDocumentType"\
                ,"case when \
        apa1.accountPayableDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLClearingDocument ,\
        apa1.accountPayableClearingDocumentNumber ,'#NA#') else \
        stgcbj01.clearingBoxGLClearingDocument end as clearingBoxGLClearingDocument"\
                ,"case when \
        apa1.accountPayableDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLClearingPostingDate ,\
        apa1.accountPayableClearingDate ,'1900-01-01') else \
        stgcbj01.clearingBoxGLClearingPostingDate end as clearingBoxGLClearingPostingDate"\
                ,"case when \
        apa1.accountPayableDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLPostingKeyERP ,\
        apa1.accountPayablePostingKey ,'#NA#') else \
        stgcbj01.clearingBoxGLPostingKeyERP end as clearingBoxGLPostingKeyERP"\
                ,"case when \
        apa1.accountPayableDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLPostingKey ,\
        cbbdm1_2.targetSystemValue ,'#NA#') else \
        stgcbj01.clearingBoxGLPostingKey end as clearingBoxGLPostingKey"\
                ,"case when \
        apa1.accountPayableDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLAccountNumber ,\
        apa1.accountPayableAccountNumber ,'#NA#') else \
        stgcbj01.clearingBoxGLAccountNumber end as clearingBoxGLAccountNumber"\
                ,"case when \
        apa1.accountPayableDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLAccountCategory ,\
        glc1.accountCategory ,'#NA#') else \
        stgcbj01.clearingBoxGLAccountCategory end as clearingBoxGLAccountCategory"\
                ,"case when \
        apa1.accountPayableDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLAmountLC ,(CASE when \
        apa1.accountPayableDebitCreditIndicator = 'C' then \
        (-1.0) * apa1.accountPayableAmountLC else \
        apa1.accountPayableAmountLC END)) else \
        stgcbj01.clearingBoxGLAmountLC end as clearingBoxGLAmountLC"\
                ,"case when \
        apa1.accountPayableDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLAmountDiscountLC ,(CASE when \
        apa1.accountPayableDebitCreditIndicator = 'C' then \
        (-1.0) * abs(apa1.accountPayableCashDiscountAmountLC) else \
        abs(apa1.accountPayableCashDiscountAmountLC) END)) else \
        stgcbj01.clearingBoxGLAmountDiscountLC end as clearingBoxGLAmountDiscountLC"\
                ,"case when \
        apa1.accountPayableDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLTransactionCodeERP ,\
        apa1.accountPayableTransactionCode ,'#NA#') else \
        stgcbj01.clearingBoxGLTransactionCodeERP end as clearingBoxGLTransactionCodeERP"\
                ,"case when \
        apa1.accountPayableDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLHeaderDescription ,\
        apa1.accountPayableDocumentHeaderDescription ,'#NA#') else \
        stgcbj01.clearingBoxGLHeaderDescription end as clearingBoxGLHeaderDescription"\
                ,"case when \
        apa1.accountPayableDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLLineDescription ,\
        apa1.accountPayableDocumentLineItemDescription ,'#NA#') else \
        stgcbj01.clearingBoxGLLineDescription end as clearingBoxGLLineDescription"\
                ,"case when \
        apa1.accountPayableDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLBillingDocument ,'#NA#') else \
        stgcbj01.clearingBoxGLBillingDocument end as clearingBoxGLBillingDocument"\
                ,"case when \
        apa1.accountPayableDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLPurchaseOrder ,\
        apa1.accountPayablePurchaseOrderNumber ,'#NA#') else \
        stgcbj01.clearingBoxGLPurchaseOrder end as clearingBoxGLPurchaseOrder"\
                ,"case when \
        apa1.accountPayableDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLCustomerNumber ,\
        apa1.accountPayableCustomerNumber ,'#NA#') else \
        stgcbj01.clearingBoxGLCustomerNumber end as clearingBoxGLCustomerNumber"\
                ,"case when \
        apa1.accountPayableDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLVendorNumber ,\
        apa1.accountPayableVendorNumber ,'#NA#') else \
        stgcbj01.clearingBoxGLVendorNumber end as clearingBoxGLVendorNumber"\
                ,"case when \
        apa1.accountPayableDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLSpecialIndicatorERP ,apa1.accountPayableGLSpecialIndicator, '') else \
        stgcbj01.clearingBoxGLSpecialIndicatorERP end as clearingBoxGLSpecialIndicatorERP"\
                ,"case when \
        apa1.accountPayableDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLSpecialIndicator ,\
        cbbdm1_4.targetSystemValue ,'"+defaultGLSpecialIndicator+"') else \
        stgcbj01.clearingBoxGLSpecialIndicator end as clearingBoxGLSpecialIndicator"\
                ,"case when \
        apa1.accountPayableDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxAccountTypeERP ,apa1.accountPayableAccountType ,'#NA#') else \
        stgcbj01.clearingBoxAccountTypeERP end as clearingBoxAccountTypeERP"\
                ,"case when \
        apa1.accountPayableDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxAccountType ,cbbdm1.targetSystemValue ,'#NA#') else \
        stgcbj01.clearingBoxAccountType end as clearingBoxAccountType"\
                ,"case when \
        apa1.accountPayableDocumentNumber is not null then \
        coalesce(stgcbj01.clearingBoxGLReferenceDocument ,apa1.accountPayableReferenceHeader2 ,'#NA#') else \
        stgcbj01.clearingBoxGLReferenceDocument end as clearingBoxGLReferenceDocument"\
                ,"case when \
        apa1.accountPayableDocumentNumber is not null then \
        coalesce(clearingBoxAccountTypeSpecialIndicator ,concat(rtrim(ltrim(apa1.accountPayableAccountType)),\
         '/' ,rtrim(ltrim(apa1.accountPayableGLSpecialIndicator))) ,'#NA#') else \
        stgcbj01.clearingBoxAccountTypeSpecialIndicator end as clearingBoxAccountTypeSpecialIndicator"\
                ,"case when \
        apa1.accountPayableDocumentNumber is not null then \
        'ap_journal' else \
        stgcbj01.clearingBoxOriginOfItem end as clearingBoxOriginOfItem"\
        ,"stgcbj01.clearingBoxSLFiscalYear"\
        ,"stgcbj01.clearingBoxGLReferenceSLDocument"\
        ,"stgcbj01.clearingBoxCompanyCode"\
        ,"stgcbj01.isClearingBoxSLSubsequentDocument"\
        ,"stgcbj01.clearingBoxGLJELineNumber"\
        ,"stgcbj01.clearingBoxSLDocumentType"\
        ,"stgcbj01.clearingBoxClientERP"\
        ,"stgcbj01.clearingBoxSLDocumentNumber"\
        ,"stgcbj01.clearingBoxGLFiscalYear"\
        ,"stgcbj01.isReversedOrReversal"\
        ,"stgcbj01.clearingBoxGLJEDocument"\
        ,"stgcbj01.clearingBoxGLCreatedByUser"\
        ,"stgcbj01.idGLJELine"\
        ,"stgcbj01.idGLJE"\
        ,"stgcbj01.clearingBoxSLDocumentTypeERP"\
        ,"stgcbj01.clearingBoxGLReferenceSLSource")
        
        
        gen_L1_STG_ClearingBox_01_Journal=\
        gen_L1_STG_ClearingBox_01_Journal\
        .withColumn("clearingBoxGLPostingDate",expr("case when \
        clearingBoxOriginOfItem='unknown' then \
        case when \
        isnull(clearingBoxGLPostingDate ) then \
        '1900-01-01' else \
         clearingBoxGLPostingDate  end else \
        clearingBoxGLPostingDate end" ))\
                .withColumn("clearingBoxGLDocumentTypeERP",expr("case when \
        clearingBoxOriginOfItem='unknown' then \
        case when \
        isnull(clearingBoxGLDocumentTypeERP ) then \
        '#NA#' else \
         clearingBoxGLDocumentTypeERP  end else \
        clearingBoxGLDocumentTypeERP end"))\
                .withColumn("clearingBoxGLDocumentType",expr("case when \
        clearingBoxOriginOfItem='unknown' then \
        case when \
        isnull(clearingBoxGLDocumentType ) then \
        '#NA#' else \
         clearingBoxGLDocumentType  end else \
        clearingBoxGLDocumentType end"))\
                .withColumn("clearingBoxGLClearingDocument",expr("case when \
        clearingBoxOriginOfItem='unknown' then \
        case when \
        isnull(clearingBoxGLClearingDocument ) then \
        '#NA#' else \
         clearingBoxGLClearingDocument  end else \
        clearingBoxGLClearingDocument end"))\
                .withColumn("clearingBoxGLClearingPostingDate",expr("case when \
        clearingBoxOriginOfItem='unknown' then \
        case when \
        isnull(clearingBoxGLClearingPostingDate ) then \
        '1900-01-01' else \
         clearingBoxGLClearingPostingDate  end else \
        clearingBoxGLClearingPostingDate end"))\
                .withColumn("clearingBoxGLPostingKeyERP",expr("case when \
        clearingBoxOriginOfItem='unknown' then \
        case when \
        isnull(clearingBoxGLPostingKeyERP ) then \
        '#NA#' else \
         clearingBoxGLPostingKeyERP  end else \
        clearingBoxGLPostingKeyERP end"))\
                .withColumn("clearingBoxGLPostingKey",expr("case when \
        clearingBoxOriginOfItem='unknown' then \
        case when \
        isnull(clearingBoxGLPostingKey ) then \
        '#NA#' else \
         clearingBoxGLPostingKey  end else \
        clearingBoxGLPostingKey end"))\
                .withColumn("clearingBoxGLAccountNumber",expr("case when \
        clearingBoxOriginOfItem='unknown' then \
        case when \
        isnull(clearingBoxGLAccountNumber ) then \
        '#NA#' else \
         clearingBoxGLAccountNumber  end else \
        clearingBoxGLAccountNumber end"))\
                .withColumn("clearingBoxGLAccountCategory",expr("case when \
        clearingBoxOriginOfItem='unknown' then \
        case when \
        isnull(clearingBoxGLAccountCategory ) then \
        '#NA#' else \
         clearingBoxGLAccountCategory  end else \
        clearingBoxGLAccountCategory end"))\
                .withColumn("clearingBoxGLTransactionCodeERP",expr("case when \
        clearingBoxOriginOfItem='unknown' then \
        case when \
        isnull(clearingBoxGLTransactionCodeERP ) then \
        '#NA#' else \
         clearingBoxGLTransactionCodeERP  end else \
        clearingBoxGLTransactionCodeERP end"))\
                .withColumn("clearingBoxGLHeaderDescription",expr("case when \
        clearingBoxOriginOfItem='unknown' then \
        case when \
        isnull(clearingBoxGLHeaderDescription ) then \
        '#NA#' else \
         clearingBoxGLHeaderDescription  end else \
        clearingBoxGLHeaderDescription end"))\
                .withColumn("clearingBoxGLLineDescription",expr("case when \
        clearingBoxOriginOfItem='unknown' then \
        case when \
        isnull(clearingBoxGLLineDescription ) then \
        '#NA#' else \
         clearingBoxGLLineDescription  end else \
        clearingBoxGLLineDescription end"))\
                .withColumn("clearingBoxSLDocumentTypeERP",expr("case when \
        clearingBoxOriginOfItem='unknown' then \
        case when \
        isnull(clearingBoxSLDocumentTypeERP ) then \
        '#NA#' else \
         clearingBoxSLDocumentTypeERP  end else \
        clearingBoxSLDocumentTypeERP end"))\
                .withColumn("clearingBoxSLDocumentType",expr("case when \
        clearingBoxOriginOfItem='unknown' then \
        case when \
        isnull(clearingBoxSLDocumentType ) then \
        '#NA#' else \
         clearingBoxSLDocumentType  end else \
        clearingBoxSLDocumentType end"))\
                .withColumn("clearingBoxGLBillingDocument",expr("case when \
        clearingBoxOriginOfItem='unknown' then \
        case when \
        isnull(clearingBoxGLBillingDocument ) then \
        '#NA#' else \
         clearingBoxGLBillingDocument  end else \
        clearingBoxGLBillingDocument end"))\
                .withColumn("clearingBoxGLPurchaseOrder",expr("case when \
        clearingBoxOriginOfItem='unknown' then \
        case when \
        isnull(clearingBoxGLPurchaseOrder ) then \
        '#NA#' else \
         clearingBoxGLPurchaseOrder  end else \
        clearingBoxGLPurchaseOrder end"))\
                .withColumn("clearingBoxGLCustomerNumber",expr("case when \
        clearingBoxOriginOfItem='unknown' then \
        case when \
        isnull(clearingBoxGLCustomerNumber ) then \
        '#NA#' else \
         clearingBoxGLCustomerNumber  end else \
        clearingBoxGLCustomerNumber end"))\
                .withColumn("clearingBoxGLVendorNumber",expr("case when \
        clearingBoxOriginOfItem='unknown' then \
        case when \
        isnull(clearingBoxGLVendorNumber ) then \
        '#NA#' else \
         clearingBoxGLVendorNumber  end else \
        clearingBoxGLVendorNumber end"))\
                .withColumn("clearingBoxGLSpecialIndicatorERP",expr("case when \
        clearingBoxOriginOfItem='unknown' then \
        case when \
        isnull(clearingBoxGLSpecialIndicatorERP ) then \
        '' else \
         clearingBoxGLSpecialIndicatorERP  end else \
        clearingBoxGLSpecialIndicatorERP end"))\
                .withColumn("clearingBoxGLSpecialIndicator",expr("case when \
        clearingBoxOriginOfItem='unknown' then \
        case when \
        isnull(clearingBoxGLSpecialIndicator ) then \
        '"+defaultGLSpecialIndicator+"' else \
         clearingBoxGLSpecialIndicator  end else \
        clearingBoxGLSpecialIndicator end"))\
                .withColumn("clearingBoxAccountTypeERP",expr("case when \
        clearingBoxOriginOfItem='unknown' then \
        case when \
        isnull(clearingBoxAccountTypeERP ) then \
        '#NA#' else \
         clearingBoxAccountTypeERP  end else \
        clearingBoxAccountTypeERP end"))\
                .withColumn("clearingBoxAccountType",expr("case when \
        clearingBoxOriginOfItem='unknown' then \
        case when \
        isnull(clearingBoxAccountType ) then \
        '#NA#' else \
         clearingBoxAccountType  end else \
        clearingBoxAccountType end"))\
                .withColumn("clearingBoxGLReferenceDocument",expr("case when \
        clearingBoxOriginOfItem='unknown' then \
        case when \
        isnull(clearingBoxGLReferenceDocument ) then \
        '#NA#' else \
         clearingBoxGLReferenceDocument  end else \
        clearingBoxGLReferenceDocument end"))\
                .withColumn("clearingBoxAccountTypeSpecialIndicator",expr("case when \
        clearingBoxOriginOfItem='unknown' then \
        case when \
        isnull(clearingBoxAccountTypeSpecialIndicator ) then \
        '#NA#' else \
         clearingBoxAccountTypeSpecialIndicator  end else \
        clearingBoxAccountTypeSpecialIndicator end"))
        
        gen_L1_TMP_ClearingBox_01_BillingDocumentByDoc=\
        otc_L1_TD_Billing.alias("blng1")\
        .groupBy("blng1.billingDocumentCompanyCode"\
        ,"blng1.billingDocumentNumber")\
        .agg(expr("max(blng1.billingDocumentTypeClient) as billingDocumentTypeClient"))
        
        
        gen_L1_STG_ClearingBox_01_Journal=\
        gen_L1_STG_ClearingBox_01_Journal.alias("stgcbj01")\
        .join(gen_L1_TMP_ClearingBox_01_BillingDocumentByDoc.alias("tcbd01"),\
        expr("( stgcbj01.clearingBoxSLDocumentNumber = tcbd01.billingDocumentNumber\
         AND stgcbj01.clearingBoxCompanyCode = tcbd01.billingDocumentCompanycode\
         AND stgcbj01.clearingBoxGLReferenceSLSource = 'Sales S/L' ) "),"left")\
        .join(gen_L1_STG_ClearingBoxBusinessDataMapping.alias("cbbdm1"),\
        expr("( cbbdm1.businessDatatype ='SD Document Type'\
         AND cbbdm1.sourceSystemValue = tcbd01.billingDocumentTypeClient )"),"left")\
        .groupBy("stgcbj01.clearingBoxGLLineDescription"\
        ,"stgcbj01.clearingBoxGLReferenceSLDocument"\
        ,"stgcbj01.clearingBoxCompanyCode"\
        ,"stgcbj01.isClearingBoxSLSubsequentDocument"\
        ,"stgcbj01.clearingBoxGLAmountLC"\
        ,"stgcbj01.clearingBoxGLJELineNumber"\
        ,"stgcbj01.clearingBoxGLFiscalYear"\
        ,"stgcbj01.clearingBoxGLDocumentTypeERP"\
        ,"stgcbj01.clearingBoxGLCreatedByUser"\
        ,"stgcbj01.clearingBoxGLHeaderDescription"\
        ,"stgcbj01.clearingBoxGLBillingDocument"\
        ,"stgcbj01.clearingBoxGLCustomerNumber"\
        ,"stgcbj01.clearingBoxGLAccountCategory"\
        ,"stgcbj01.clearingBoxAccountTypeERP"\
        ,"stgcbj01.clearingBoxGLDocumentStatus"\
        ,"stgcbj01.clearingBoxGLPurchaseOrder"\
        ,"stgcbj01.idGLJE"\
        ,"stgcbj01.clearingBoxGLVendorNumber"\
        ,"stgcbj01.clearingBoxSLFiscalYear"\
        ,"stgcbj01.clearingBoxGLFinancialPeriod"\
        ,"stgcbj01.clearingBoxGLClearingPostingDate"\
        ,"stgcbj01.clearingBoxAccountTypeSpecialIndicator"\
        ,"stgcbj01.clearingBoxGLPostingKey"\
        ,"stgcbj01.clearingBoxGLAmountDiscountLC"\
        ,"stgcbj01.clearingBoxGLPostingDate"\
        ,"stgcbj01.clearingBoxGLPostingKeyERP"\
        ,"stgcbj01.clearingBoxOriginOfItem"\
        ,"stgcbj01.isReversedOrReversal"\
        ,"stgcbj01.clearingBoxGLSpecialIndicator"\
        ,"stgcbj01.clearingBoxGLReferenceSLSource"\
        ,"stgcbj01.clearingBoxGLClearingDocument"\
        ,"stgcbj01.clearingBoxGLReferenceDocument"\
        ,"stgcbj01.clearingBoxGLDebitCreditIndicator"\
        ,"stgcbj01.clearingBoxClientERP"\
        ,"stgcbj01.clearingBoxGLTransactionCodeERP"\
        ,"stgcbj01.clearingBoxGLAccountNumber"\
        ,"stgcbj01.clearingBoxSLDocumentNumber"\
        ,"stgcbj01.clearingBoxAccountType"\
        ,"stgcbj01.clearingBoxGLSpecialIndicatorERP"\
        ,"stgcbj01.clearingBoxGLJEDocument"\
        ,"stgcbj01.clearingBoxGLDocumentType"\
        ,"stgcbj01.idGLJELine")\
        .agg(expr("max(case when \
        tcbd01.billingDocumentNumber is not null then \
        (CASE when \
        ISNULL(cbbdm1.targetSystemValue) then \
        stgcbj01.clearingBoxSLDocumentType else \
        cbbdm1.targetSystemValue END) else \
        stgcbj01.clearingBoxSLDocumentType end) as clearingBoxSLDocumentType")\
                ,expr("max(case when \
        tcbd01.billingDocumentNumber is not null then \
        tcbd01.billingDocumentTypeClient else \
        stgcbj01.clearingBoxSLDocumentTypeERP end) as clearingBoxSLDocumentTypeERP"))
        
        gen_L1_TMP_ClearingBox_01_InvoiceReceiptByDoc=\
        ptp_L1_TD_InvoiceReceipt.alias("inr1")\
        .groupBy("inr1.invoiceReceiptClientCode"\
        ,"inr1.invoiceReceiptCompanyCode"\
        ,"inr1.invoiceReceiptPostingYear"\
        ,"inr1.invoiceReceiptNumber")\
        .agg(expr("max(inr1.invoiceReceiptTransactionType) as invoiceReceiptTransactionTypeERP")\
        ,expr("cast(max(cast(inr1.isSubsequentInvoice AS int)) AS boolean) as isSubsequentInvoice")\
        ,expr("cast(max(cast(inr1.isDeliveryCostInvoice AS int)) AS boolean) as isDeliveryCostInvoice"))
        
        gen_L1_STG_ClearingBox_01_Journal=\
        gen_L1_STG_ClearingBox_01_Journal.alias("stgcbj01")\
        .join(gen_L1_TMP_ClearingBox_01_InvoiceReceiptByDoc.alias("tcir01"),\
        expr("( stgcbj01.clearingBoxSLDocumentNumber = tcir01.invoiceReceiptNumber\
         AND stgcbj01.clearingBoxCompanyCode = tcir01.invoiceReceiptCompanyCode\
         AND stgcbj01.clearingBoxGLFiscalYear = tcir01.invoiceReceiptPostingYear\
         AND stgcbj01.clearingBoxGLReferenceSLSource ='Purchase S/L' ) "),"left")\
        .join(gen_L1_STG_ClearingBoxBusinessDataMapping.alias("cbbdm1"),\
        expr("( tcir01.invoiceReceiptTransactionTypeERP = cbbdm1.sourceSystemValue\
         AND cbbdm1.businessDatatype = 'IR Transaction Type' )"),"left")\
        .selectExpr("case when \
        tcir01.invoiceReceiptNumber is not null then \
        (CASE when \
        ISNULL(cbbdm1.targetSystemValue) then \
        stgcbj01.clearingBoxSLDocumentType else \
        cbbdm1.targetSystemValue END) else \
        stgcbj01.clearingBoxSLDocumentType end as clearingBoxSLDocumentType"\
                ,"case when \
        tcir01.invoiceReceiptNumber is not null then \
        tcir01.invoiceReceiptTransactionTypeERP else \
        stgcbj01.clearingBoxSLDocumentTypeERP end as clearingBoxSLDocumentTypeERP"\
                ,"case when \
        tcir01.invoiceReceiptNumber is not null then \
        cast(CASE when \
        tcir01.isSubsequentInvoice = 1 OR tcir01.isDeliveryCostInvoice = 1 then \
        1 else \
        0 END as boolean) else \
        stgcbj01.isClearingBoxSLSubsequentDocument end as isClearingBoxSLSubsequentDocument"\
        ,"stgcbj01.clearingBoxGLLineDescription"\
        ,"stgcbj01.clearingBoxGLReferenceSLDocument"\
        ,"stgcbj01.clearingBoxCompanyCode"\
        ,"stgcbj01.clearingBoxGLAmountLC"\
        ,"stgcbj01.clearingBoxGLJELineNumber"\
        ,"stgcbj01.clearingBoxGLFiscalYear"\
        ,"stgcbj01.clearingBoxGLDocumentTypeERP"\
        ,"stgcbj01.clearingBoxGLCreatedByUser"\
        ,"stgcbj01.clearingBoxGLHeaderDescription"\
        ,"stgcbj01.clearingBoxGLBillingDocument"\
        ,"stgcbj01.clearingBoxGLCustomerNumber"\
        ,"stgcbj01.clearingBoxGLAccountCategory"\
        ,"stgcbj01.clearingBoxAccountTypeERP"\
        ,"stgcbj01.clearingBoxGLDocumentStatus"\
        ,"stgcbj01.clearingBoxGLPurchaseOrder"\
        ,"stgcbj01.idGLJE"\
        ,"stgcbj01.clearingBoxGLVendorNumber"\
        ,"stgcbj01.clearingBoxSLFiscalYear"\
        ,"stgcbj01.clearingBoxGLFinancialPeriod"\
        ,"stgcbj01.clearingBoxGLClearingPostingDate"\
        ,"stgcbj01.clearingBoxAccountTypeSpecialIndicator"\
        ,"stgcbj01.clearingBoxGLPostingKey"\
        ,"stgcbj01.clearingBoxGLAmountDiscountLC"\
        ,"stgcbj01.clearingBoxGLPostingDate"\
        ,"stgcbj01.clearingBoxGLPostingKeyERP"\
        ,"stgcbj01.clearingBoxOriginOfItem"\
        ,"stgcbj01.isReversedOrReversal"\
        ,"stgcbj01.clearingBoxGLSpecialIndicator"\
        ,"stgcbj01.clearingBoxGLReferenceSLSource"\
        ,"stgcbj01.clearingBoxGLClearingDocument"\
        ,"stgcbj01.clearingBoxGLReferenceDocument"\
        ,"stgcbj01.clearingBoxGLDebitCreditIndicator"\
        ,"stgcbj01.clearingBoxClientERP"\
        ,"stgcbj01.clearingBoxGLTransactionCodeERP"\
        ,"stgcbj01.clearingBoxSLDocumentNumber"\
        ,"stgcbj01.clearingBoxGLAccountNumber"\
        ,"stgcbj01.clearingBoxAccountType"\
        ,"stgcbj01.clearingBoxGLSpecialIndicatorERP"\
        ,"stgcbj01.clearingBoxGLJEDocument"\
        ,"stgcbj01.clearingBoxGLDocumentType"\
        ,"stgcbj01.idGLJELine").cache()
        
        
        gen_L1_TMP_ClearingBox_01_BasicPopulation=\
        fin_L1_STG_Receivable.alias("srec1")\
        .join(gen_L1_STG_ClearingBox_01_Journal.alias("stgcbj01"),\
        expr("( srec1.GLJEDocumentNumber = stgcbj01.clearingBoxGLJEDocument\
         AND srec1.GLJELineNumber = stgcbj01.clearingBoxGLJELineNumber\
         AND srec1.fiscalYear = stgcbj01.clearingBoxGLFiscalYear\
         AND srec1.companyCode = stgcbj01.clearingBoxCompanyCode\
         AND srec1.client = stgcbj01.clearingBoxClientERP ) "),"inner")\
        .join(fin_L1_TD_AccountReceivable.alias("acrv1"),\
        expr("( srec1.GLJEDocumentNumber = acrv1.GLJEDocumentNumber\
         AND srec1.GLJELineNumber = acrv1.GLJELineNumber\
         AND srec1.fiscalYear = acrv1.financialYear\
         AND srec1.companyCode = acrv1.companyCode ) "),"left")\
        .join(gen_L1_STG_ClearingBoxBusinessDataMapping.alias("cbbdm1"),\
        expr("( cbbdm1.businessDatatype = 'GL Posting Key'\
         AND cbbdm1.sourceSystemValue = srec1.GLDocumentKey )"),"left")\
        .selectExpr("stgcbj01.idGLJE as idGLJE"\
        ,"stgcbj01.idGLJELine as idGLJELine"\
        ,"stgcbj01.clearingBoxClientERP as clearingBoxClientERP"\
        ,"stgcbj01.clearingBoxCompanyCode as clearingBoxCompanyCode"\
        ,"stgcbj01.clearingBoxGLFiscalYear as clearingBoxGLFiscalYear"\
        ,"srec1.financialPeriod as clearingBoxGLFinancialPeriod"\
        ,"srec1.GLDocumentPostingDate as clearingBoxGLPostingDate"\
        ,"stgcbj01.clearingBoxGLJEDocument as clearingBoxGLJEDocument"\
        ,"stgcbj01.clearingBoxGLJELineNumber as clearingBoxGLJELineNumber"\
        ,"(CASE when \
        (CASE when \
        ISNULL(stgcbj01.clearingBoxGLClearingDocument) then \
        '' else \
        stgcbj01.clearingBoxGLClearingDocument END) <> '' then \
        stgcbj01.clearingBoxGLClearingDocument else \
        srec1.glClearingDocumentNumber END ) as clearingBoxGLClearingDocument"\
                ,"(CASE when \
        (CASE when \
        ISNULL(stgcbj01.clearingBoxGLClearingDocument) then \
        '' else \
        stgcbj01.clearingBoxGLClearingDocument END) <> '' then \
        stgcbj01.clearingBoxGLClearingPostingDate else \
        srec1.glClearingDocumentPostingDate END ) as clearingBoxGLClearingPostingDate"\
                ,"srec1.glAccountNumber as clearingBoxGLAccountNumber"\
                ,"srec1.debitCreditKey as clearingBoxGLDebitCreditIndicator"\
                ,"(CASE when \
        ISNULL(stgcbj01.clearingBoxGLAmountLC) then \
        (CASE when \
        srec1.debitCreditKey = 'C' then \
        (-1.0) * srec1.amountLC else \
        srec1.amountLC END) else \
        stgcbj01.clearingBoxGLAmountLC END) as clearingBoxGLAmountLC"\
                ,"(CASE when \
        ISNULL(stgcbj01.clearingBoxGLAmountDiscountLC) then \
        0 else \
        stgcbj01.clearingBoxGLAmountDiscountLC END) as clearingBoxGLAmountDiscountLC"\
                ,"coalesce(CASE when \
        stgcbj01.clearingBoxGLReferenceSLSource = 'Sales S/L' then \
        stgcbj01.clearingBoxSLDocumentNumber else \
        NULL END, stgcbj01.clearingBoxGLBillingDocument, acrv1.billingDocumentNumber, '')\
         as clearingBoxGLBillingDocument"\
                ,"coalesce(CASE when \
        stgcbj01.clearingBoxGLReferenceSLSource = 'Purchase S/L' then \
        stgcbj01.clearingBoxSLDocumentNumber else \
        NULL END, stgcbj01.clearingBoxGLPurchaseOrder, '') as clearingBoxGLPurchaseOrder"\
                ,"(CASE when \
        (CASE when \
        ISNULL(srec1.customerNumber) then \
        '' else \
        srec1.customerNumber END) = '' then \
        '#NA#' else \
        srec1.customerNumber END ) as clearingBoxGLCustomerNumber"\
                ,"(CASE when \
        (CASE when \
        ISNULL(stgcbj01.clearingBoxGLVendorNumber) then \
        '' else \
        stgcbj01.clearingBoxGLVendorNumber END) = '' then \
        '#NA#' else \
        stgcbj01.clearingBoxGLVendorNumber END ) as clearingBoxGLVendorNumber"\
                ,"srec1.GLDocumentType as clearingBoxGLDocumentTypeERP"\
                ,"srec1.GLDocumentKey as clearingBoxGLPostingKeyERP"\
                ,"(CASE when \
        ISNULL(cbbdm1.targetSystemValue) then \
        srec1.GLDocumentKey else \
        cbbdm1.targetSystemValue END) as clearingBoxGLPostingKey"\
                ,"rtrim(ltrim(coalesce(stgcbj01.clearingBoxAccountTypeERP, acrv1.accountType)))\
        		as clearingBoxAccountTypeERP"\
                ,"cast('' as VARCHAR(32)) as clearingBoxAccountType"\
                ,"rtrim(ltrim((CASE when \
        ISNULL(stgcbj01.clearingBoxGLSpecialIndicatorERP) then \
        acrv1.GLSpecialIndicator else \
        stgcbj01.clearingBoxGLSpecialIndicatorERP END)) ) \
        as clearingBoxGLSpecialIndicatorERP"\
                ,"stgcbj01.clearingBoxGLSpecialIndicator \
        		as clearingBoxGLSpecialIndicator"\
                ,"cast((CASE when \
        (CASE when \
        (CASE when \
        ISNULL(stgcbj01.clearingBoxGLClearingDocument) then \
        '' else \
        stgcbj01.clearingBoxGLClearingDocument END) <> '' then \
        stgcbj01.clearingBoxGLClearingDocument else \
        srec1.glClearingDocumentNumber END) <> '' then \
        1 else \
        0 END) as boolean ) as isCleared"\
        ,"srec1.isOpenItemPY as isOpenItemPY"\
        ,"srec1.isOpenItemYE as isOpenItemYE"\
        ,"cast(1 as int) as caseID"\
        ,"cast('AR' as CHAR(2)) as caseLogistic").cache()
        
        gen_L1_TMP_ClearingBox_01_CollectedIdGLJELine=\
        gen_L1_TMP_ClearingBox_01_BasicPopulation.alias("tcbbp01")\
        .selectExpr("tcbbp01.idGLJELine as idGLJELine").distinct().cache()
        
        
        clearingGL="CASE when \
        (CASE when \
        ISNULL(stgcbj01.clearingBoxGLClearingDocument) then \
         '' else \
        stgcbj01.clearingBoxGLClearingDocument END) <> '' then \
        stgcbj01.clearingBoxGLClearingPostingDate else \
        apa1.accountPayableClearingDate END"
        isOpenItemPY="CASE when \
        apa1.accountPayablePostingDate < '"+startDate+"'\
        AND ((CASE when \
        (CASE when \
        ISNULL(stgcbj01.clearingBoxGLClearingDocument) then \
         '' else \
        \
        stgcbj01.clearingBoxGLClearingDocument END) <> ''	then \
        stgcbj01.clearingBoxGLClearingPostingDate else \
        \
        apa1.accountPayableClearingDate END) >= '"+startDate+"' \
        OR case when \
        isnull("+clearingGL+") then \
        '1900-01-01' else \
        "+clearingGL+" end = '1900-01-01')	\
        then \
        1 else \
        0 END"
        gen_L1_TMP_ClearingBox_01_BasicPopulation_1=\
        fin_L1_STG_AccountPayable.alias("apa1")\
        .join(gen_L1_STG_ClearingBox_01_Journal.alias("stgcbj01"),\
        expr("( apa1.accountPayableDocumentNumber = \
		stgcbj01.clearingBoxGLJEDocument\
         AND apa1.accountPayableDocumentLineItem = \
		 stgcbj01.clearingBoxGLJELineNumber\
         AND apa1.accountPayableFiscalYear = stgcbj01.clearingBoxGLFiscalYear\
         AND apa1.accountPayableCompanyCode = stgcbj01.clearingBoxCompanyCode\
         AND apa1.accountPayableClientCode = \
		 stgcbj01.clearingBoxClientERP ) "),"inner")\
        .join(fin_L1_TD_AccountPayable.alias("apa1_2"),\
        expr("( apa1.accountPayableDocumentNumber = apa1_2.accountPayableDocumentNumber\
         AND apa1.accountPayableDocumentLineItem = apa1_2.accountPayableDocumentLineItem\
         AND apa1.accountPayableFiscalYear = apa1_2.accountPayableFiscalYear\
         AND apa1.accountPayableCompanyCode = apa1_2.accountPayableCompanyCode ) "),"left")\
        .join(gen_L1_STG_ClearingBoxBusinessDataMapping.alias("cbbdm1"),\
        expr("( cbbdm1.businessDatatype ='GL Posting Key'\
         AND cbbdm1.sourceSystemValue = (CASE when \
        ISNULL(stgcbj01.clearingBoxGLPostingKeyERP) then \
         apa1_2.accountPayablePostingKey else \
        stgcbj01.clearingBoxGLPostingKeyERP END) ) "),"left" )\
        .join(gen_L1_TMP_ClearingBox_01_CollectedIdGLJELine.alias("tcidgll1"),\
        expr("( stgcbj01.idGLJELine = tcidgll1.idGLJELine )"),"left")\
        .filter(expr("tcidgll1.idGLJELine IS NULL \
         AND apa1.accountPayableDocumentStatus IN ('N','C')"))\
        .selectExpr("stgcbj01.idGLJE as idGLJE"\
        ,"stgcbj01.idGLJELine as idGLJELine"\
        ,"stgcbj01.clearingBoxClientERP as clearingBoxClientERP"\
        ,"stgcbj01.clearingBoxCompanyCode as clearingBoxCompanyCode"\
        ,"stgcbj01.clearingBoxGLFiscalYear as clearingBoxGLFiscalYear"\
        ,"apa1.accountPayableFinancialPeriod as clearingBoxGLFinancialPeriod"\
        ,"apa1.accountPayablePostingDate as clearingBoxGLPostingDate"\
        ,"stgcbj01.clearingBoxGLJEDocument as clearingBoxGLJEDocument"\
        ,"stgcbj01.clearingBoxGLJELineNumber as clearingBoxGLJELineNumber"\
        ,"(CASE when \
        (CASE when \
        ISNULL(stgcbj01.clearingBoxGLClearingDocument) then \
        '' else \
        stgcbj01.clearingBoxGLClearingDocument END) <> '' then \
        stgcbj01.clearingBoxGLClearingDocument else \
        apa1.accountPayableClearingDocumentNumber END ) as clearingBoxGLClearingDocument"\
                ,"(CASE when \
        (CASE when \
        ISNULL(stgcbj01.clearingBoxGLClearingDocument) then \
        '' else \
        stgcbj01.clearingBoxGLClearingDocument END) <> '' then \
        stgcbj01.clearingBoxGLClearingPostingDate else \
        apa1.accountPayableClearingDate END ) as clearingBoxGLClearingPostingDate"\
                ,"apa1.accountPayableAccountNumber as clearingBoxGLAccountNumber"\
                ,"apa1.accountPayableDebitCreditIndicator as clearingBoxGLDebitCreditIndicator"\
                ,"(CASE when \
        ISNULL(stgcbj01.clearingBoxGLAmountLC) then \
        apa1.accountPayableAmountLC else \
        stgcbj01.clearingBoxGLAmountLC END) as clearingBoxGLAmountLC"\
                ,"coalesce(apa1.accountPayableCashDiscountAmountLC, \
        		stgcbj01.clearingBoxGLAmountDiscountLC, 0) as clearingBoxGLAmountDiscountLC"\
                ,"coalesce(CASE when \
        stgcbj01.clearingBoxGLReferenceSLSource = 'Sales S/L' then \
        stgcbj01.clearingBoxGLReferenceSLDocument else \
        NULL END, stgcbj01.clearingBoxGLBillingDocument,'') as clearingBoxGLBillingDocument"\
                ,"coalesce(CASE when \
        stgcbj01.clearingBoxGLReferenceSLSource = 'Purchase S/L' then \
        LEFT(stgcbj01.clearingBoxGLReferenceSLDocument, 10) else \
        NULL END, apa1_2.accountPayablePurchaseOrderNumber, '') as clearingBoxGLPurchaseOrder"\
                ,"(CASE when \
        (CASE when \
        ISNULL(stgcbj01.clearingBoxGLCustomerNumber) then \
        ''  else \
        stgcbj01.clearingBoxGLCustomerNumber END) = '' then \
        '#NA#' else \
        stgcbj01.clearingBoxGLCustomerNumber END) as clearingBoxGLCustomerNumber"\
                ,"(CASE when \
        (CASE when \
        ISNULL(apa1.accountPayableVendorNumber) then \
        '' else \
        apa1.accountPayableVendorNumber END) = '' then \
        '#NA#' else \
        apa1.accountPayableVendorNumber END ) as clearingBoxGLVendorNumber"\
                ,"apa1.accountPayableDocumentType as clearingBoxGLDocumentTypeERP"\
                ,"(CASE when \
        ISNULL(stgcbj01.clearingBoxGLPostingKeyERP) then \
        apa1_2.accountPayablePostingKey else \
        stgcbj01.clearingBoxGLPostingKeyERP END) as clearingBoxGLPostingKeyERP"\
                ,"coalesce(cbbdm1.targetSystemValue, \
        		stgcbj01.clearingBoxGLPostingKeyERP, \
        		apa1_2.accountPayablePostingKey) as clearingBoxGLPostingKey"\
                ,"rtrim(ltrim(coalesce(stgcbj01.clearingBoxAccountTypeERP,\
        		apa1.accountPayableAccountType))) as clearingBoxAccountTypeERP"\
                ,"cast('' as VARCHAR(32)) as clearingBoxAccountType"\
                ,"rtrim(ltrim((CASE when \
        ISNULL(stgcbj01.clearingBoxGLSpecialIndicatorERP) then \
        '' else \
        stgcbj01.clearingBoxGLSpecialIndicatorERP END)) ) as clearingBoxGLSpecialIndicatorERP"\
                ,"stgcbj01.clearingBoxGLSpecialIndicator as clearingBoxGLSpecialIndicator"\
                ,"cast((CASE when \
        (CASE when \
        (CASE when \
        ISNULL(stgcbj01.clearingBoxGLClearingDocument) then \
        ''  else \
        stgcbj01.clearingBoxGLClearingDocument END) <> '' then \
        stgcbj01.clearingBoxGLClearingDocument else \
        apa1.accountPayableClearingDocumentNumber END) <> '' then \
        1 else \
        0 END) as boolean) as isCleared"\
                ,"cast("+isOpenItemPY+" as boolean) as isOpenItemPY "\
                ,"cast(CASE when \
        (	CASE when \
        (case when \
        isnull(stgcbj01.clearingBoxGLClearingDocument) then \
        '' else \
        stgcbj01.clearingBoxGLClearingDocument end)	<> '' \
        then stgcbj01.clearingBoxGLClearingDocument \
        else \
        apa1.accountPayableClearingDocumentNumber END) = '' \
        OR (  CASE when \
          (case when \
        isnull(stgcbj01.clearingBoxGLClearingDocument) then \
        '' else \
        stgcbj01.clearingBoxGLClearingDocument end) <> '' \
        then \
        stgcbj01.clearingBoxGLClearingPostingDate \
        else \
        apa1.accountPayableClearingDate END ) > '"+endDate+"'     \
        then \
        1 else \
        0 END as boolean) as isOpenItemYE"\
        ,"cast(1 as int) as caseID"\
        ,"cast('AP' as CHAR(2)) as caseLogistic").cache()
        
        gen_L1_TMP_ClearingBox_01_BasicPopulation=\
        gen_L1_TMP_ClearingBox_01_BasicPopulation\
        .union(gen_L1_TMP_ClearingBox_01_BasicPopulation_1).cache()

        gen_L1_TMP_ClearingBox_01_CollectedAccount=\
        gen_L1_TMP_ClearingBox_01_BasicPopulation.alias("tcbbp01")\
        .groupBy("tcbbp01.clearingBoxCompanyCode"\
        ,"tcbbp01.clearingBoxGLAccountNumber")\
        .agg(expr("max(tcbbp01.caseLogistic) as caseLogistic"))
        
        
        
        gen_L1_TMP_ClearingBox_01_CollectedIdGLJELine=\
        gen_L1_TMP_ClearingBox_01_CollectedIdGLJELine\
        .union(gen_L1_TMP_ClearingBox_01_BasicPopulation_1.alias("tcbbp01")\
        .selectExpr("tcbbp01.idGLJELine as idGLJELine").distinct()).cache()
        
        if gen_L1_TMP_ClearingBox_01_BasicPopulation_1 is not None:
            gen_L1_TMP_ClearingBox_01_BasicPopulation_1.unpersist()
        
        
        gen_L1_TMP_ClearingBox_01_BasicPopulation_2=\
        fin_L1_TD_AccountReceivable.alias("acrv1")\
        .join(gen_L1_STG_ClearingBox_01_Journal.alias("stgcbj01"),\
        expr("( acrv1.GLJEDocumentNumber = stgcbj01.clearingBoxGLJEDocument\
         AND acrv1.GLJELineNumber = stgcbj01.clearingBoxGLJELineNumber\
         AND acrv1.financialYear = stgcbj01.clearingBoxGLFiscalYear\
         AND acrv1.companyCode = stgcbj01.clearingBoxCompanyCode ) "),"inner")\
        .join(gen_L1_TMP_ClearingBox_01_CollectedIdGLJELine.alias("tcidgll1"),\
        expr("( stgcbj01.idGLJELine = tcidgll1.idGLJELine ) "),"left")\
        .join(fin_L1_STG_Receivable.alias("srec1"),\
        expr("( srec1.GLJEDocumentNumber = acrv1.GLJEDocumentNumber\
         AND srec1.GLJELineNumber = acrv1.GLJELineNumber\
         AND srec1.fiscalYear = acrv1.financialYear\
         AND srec1.companyCode = acrv1.companyCode ) "),"left")\
        .join(gen_L1_STG_ClearingBoxBusinessDataMapping.alias("cbbdm1"),\
        expr("( cbbdm1.businessDatatype = 'GL Posting Key'\
         AND cbbdm1.sourceSystemValue = acrv1.postingKey )"),"left")\
        .filter(expr("tcidgll1.idGLJELine IS NULL \
         AND acrv1.documentStatus IN ('N','C') \
         AND acrv1.clearingDocumentNumber <> ''"))\
        .selectExpr("stgcbj01.idGLJE as idGLJE"\
        ,"stgcbj01.idGLJELine as idGLJELine"\
        ,"stgcbj01.clearingBoxClientERP as clearingBoxClientERP"\
        ,"stgcbj01.clearingBoxCompanyCode as clearingBoxCompanyCode"\
        ,"acrv1.financialYear as clearingBoxGLFiscalYear"\
        ,"acrv1.financialPeriod as clearingBoxGLFinancialPeriod"\
        ,"acrv1.GLJEPostingDate as clearingBoxGLPostingDate"\
        ,"stgcbj01.clearingBoxGLJEDocument as clearingBoxGLJEDocument"\
        ,"stgcbj01.clearingBoxGLJELineNumber as clearingBoxGLJELineNumber"\
        ,"acrv1.clearingDocumentNumber as clearingBoxGLClearingDocument"\
        ,"acrv1.documentClearingDate as clearingBoxGLClearingPostingDate"\
        ,"acrv1.accountNumber as clearingBoxGLAccountNumber"\
        ,"acrv1.debitCreditIndicator as clearingBoxGLDebitCreditIndicator"\
        ,"(CASE when \
        ISNULL(stgcbj01.clearingBoxGLAmountLC) then \
        acrv1.amountLC else \
        stgcbj01.clearingBoxGLAmountLC END) as clearingBoxGLAmountLC"\
                ,"coalesce((CASE when \
        acrv1.debitCreditIndicator = 'C' then \
        (-1.0) * acrv1.cashDiscountLocalCurrency else \
        acrv1.cashDiscountLocalCurrency END), \
        stgcbj01.clearingBoxGLAmountDiscountLC, 0) as clearingBoxGLAmountDiscountLC"\
                ,"coalesce(CASE when \
        stgcbj01.clearingBoxGLReferenceSLSource = 'Sales S/L' then \
        stgcbj01.clearingBoxSLDocumentNumber else \
        NULL END, stgcbj01.clearingBoxGLBillingDocument,\
         acrv1.billingDocumentNumber, '') as clearingBoxGLBillingDocument"\
                ,"coalesce(CASE when \
        stgcbj01.clearingBoxGLReferenceSLSource = 'Purchase S/L' then \
        stgcbj01.clearingBoxSLDocumentNumber else \
        NULL END, stgcbj01.clearingBoxGLPurchaseOrder, '')\
         as clearingBoxGLPurchaseOrder"\
                ,"(CASE when \
        (CASE when \
        ISNULL(acrv1.customerNumber) then \
        '' else \
        acrv1.customerNumber END) = '' then \
        '#NA#' else \
        acrv1.customerNumber END) as clearingBoxGLCustomerNumber"\
                ,"(CASE when \
        (CASE when \
        ISNULL(stgcbj01.clearingBoxGLVendorNumber) then \
        '' else \
        stgcbj01.clearingBoxGLVendorNumber END) = '' then \
        '#NA#' else \
        stgcbj01.clearingBoxGLVendorNumber END) as clearingBoxGLVendorNumber"\
                ,"acrv1.documentType as clearingBoxGLDocumentTypeERP"\
                ,"acrv1.postingKey as clearingBoxGLPostingKeyERP"\
                ,"(CASE when \
        ISNULL(cbbdm1.targetSystemValue) then \
        acrv1.postingKey else \
        cbbdm1.targetSystemValue END) as clearingBoxGLPostingKey"\
                ,"rtrim(ltrim(acrv1.accountType)) as clearingBoxAccountTypeERP"\
                ,"cast('' as VARCHAR(32)) as clearingBoxAccountType"\
                ,"rtrim(ltrim(acrv1.GLSpecialIndicator)) as clearingBoxGLSpecialIndicatorERP"\
                ,"stgcbj01.clearingBoxGLSpecialIndicator as clearingBoxGLSpecialIndicator"\
                ,"cast((CASE when \
        acrv1.clearingDocumentNumber <> '' then \
        1 else \
        0 END) as boolean) as isCleared"\
                ,"(CASE when \
        ISNULL(srec1.isOpenItemPY) then \
        false else \
        srec1.isOpenItemPY END) as isOpenItemPY"\
                ,"(CASE when \
        ISNULL(srec1.isOpenItemYE) then \
        false else \
        srec1.isOpenItemYE END) as isOpenItemYE"\
        ,"cast(2 as int) as caseID"\
        ,"cast('AR' as CHAR(2)) as caseLogistic").cache()
        
        gen_L1_TMP_ClearingBox_01_BasicPopulation=\
        gen_L1_TMP_ClearingBox_01_BasicPopulation\
		.union(gen_L1_TMP_ClearingBox_01_BasicPopulation_2).cache()
        
        gen_L1_TMP_ClearingBox_01_CollectedIdGLJELine=\
        gen_L1_TMP_ClearingBox_01_CollectedIdGLJELine\
        .union(gen_L1_TMP_ClearingBox_01_BasicPopulation_2\
		.select("idGLJELine").distinct()).cache()

        if gen_L1_TMP_ClearingBox_01_BasicPopulation_2 is not None:
            gen_L1_TMP_ClearingBox_01_BasicPopulation_2.unpersist()
        
        A="(CASE when \
        (CASE when \
        ISNULL(stgcbj01.clearingBoxGLClearingDocument) then \
         '' else \
        stgcbj01.clearingBoxGLClearingDocument END) <> '' then \
        stgcbj01.clearingBoxGLClearingPostingDate else \
        acpy1.accountPayableClearingDate END)"
        
        gen_L1_TMP_ClearingBox_01_BasicPopulation_3=\
        fin_L1_TD_AccountPayable.alias("acpy1")\
        .join(gen_L1_STG_ClearingBox_01_Journal.alias("stgcbj01"),\
        expr("( acpy1.accountPayableDocumentNumber = stgcbj01.clearingBoxGLJEDocument\
         AND acpy1.accountPayableDocumentLineItem = stgcbj01.clearingBoxGLJELineNumber\
         AND acpy1.accountPayableFiscalYear = stgcbj01.clearingBoxGLFiscalYear\
         AND acpy1.accountPayableCompanyCode = stgcbj01.clearingBoxCompanyCode\
         AND acpy1.accountPayableClientCode = stgcbj01.clearingBoxClientERP ) "),"inner")\
        .join(gen_L1_TMP_ClearingBox_01_CollectedIdGLJELine.alias("tcidgll1"),\
        expr("( stgcbj01.idGLJELine = tcidgll1.idGLJELine ) "),"left")\
        .join(gen_L1_STG_ClearingBoxBusinessDataMapping.alias("cbbdm1"),\
        expr("( cbbdm1.businessDatatype = 'GL Posting Key'\
         AND cbbdm1.sourceSystemValue = acpy1.accountPayablePostingKey )"),"left")\
        .filter(expr("tcidgll1.idGLJELine IS NULL \
         AND acpy1.accountPayableDocumentStatus IN ('N','C') \
         AND acpy1.accountPayableClearingDocumentNumber <> '' \
         AND acpy1.accountPayableAccountType = 'K'"))\
        .selectExpr("stgcbj01.idGLJE as idGLJE"\
        ,"stgcbj01.idGLJELine as idGLJELine"\
        ,"stgcbj01.clearingBoxClientERP as clearingBoxClientERP"\
        ,"stgcbj01.clearingBoxCompanyCode as clearingBoxCompanyCode"\
        ,"acpy1.accountPayableFiscalYear as clearingBoxGLFiscalYear"\
        ,"acpy1.accountPayableFinancialPeriod as clearingBoxGLFinancialPeriod"\
        ,"acpy1.accountPayablePostingDate as clearingBoxGLPostingDate"\
        ,"stgcbj01.clearingBoxGLJEDocument as clearingBoxGLJEDocument"\
        ,"stgcbj01.clearingBoxGLJELineNumber as clearingBoxGLJELineNumber"\
        ,"(CASE when \
        (CASE when \
        ISNULL(stgcbj01.clearingBoxGLClearingDocument) then \
        '' else \
        stgcbj01.clearingBoxGLClearingDocument END) <> '' then \
        stgcbj01.clearingBoxGLClearingDocument else \
        acpy1.accountPayableClearingDocumentNumber END) as clearingBoxGLClearingDocument"\
                ,"(CASE when \
        (CASE when \
        ISNULL(stgcbj01.clearingBoxGLClearingDocument) then \
        '' else \
        stgcbj01.clearingBoxGLClearingDocument END) <> '' then \
        stgcbj01.clearingBoxGLClearingPostingDate else \
        acpy1.accountPayableClearingDate END) as clearingBoxGLClearingPostingDate"\
                ,"acpy1.accountPayableAccountNumber as clearingBoxGLAccountNumber"\
                ,"acpy1.accountPayableDebitCreditIndicator as clearingBoxGLDebitCreditIndicator"\
                ,"case when \
        isnull(stgcbj01.clearingBoxGLAmountLC) then \
        (CASE when \
        acpy1.accountPayableDebitCreditIndicator = 'C' then \
        (-1.0) * acpy1.accountPayableAmountLC else \
        acpy1.accountPayableAmountLC END) else \
        stgcbj01.clearingBoxGLAmountLC end  as clearingBoxGLAmountLC"\
                ,"acpy1.accountPayableCashDiscountAmountLC as clearingBoxGLAmountDiscountLC"\
                ,"coalesce(CASE when \
        stgcbj01.clearingBoxGLReferenceSLSource = 'Sales S/L' then \
        stgcbj01.clearingBoxSLDocumentNumber else \
        NULL END, stgcbj01.clearingBoxGLBillingDocument, '') as clearingBoxGLBillingDocument"\
                ,"coalesce(CASE when \
        stgcbj01.clearingBoxGLReferenceSLSource = 'Purchase S/L' then \
        stgcbj01.clearingBoxSLDocumentNumber else \
        NULL END, acpy1.accountPayablePurchaseOrderNumber, \
        stgcbj01.clearingBoxGLPurchaseOrder, '') as clearingBoxGLPurchaseOrder"\
                ,"(CASE when \
        (CASE when \
        ISNULL(stgcbj01.clearingBoxGLCustomerNumber) then \
        '' else \
        stgcbj01.clearingBoxGLCustomerNumber END) = '' then \
        '#NA#' else \
        stgcbj01.clearingBoxGLCustomerNumber END) as clearingBoxGLCustomerNumber"\
                ,"(CASE when \
        (CASE when \
        ISNULL(acpy1.accountPayableVendorNumber) then \
        '' else \
        acpy1.accountPayableVendorNumber END) = '' then \
        '#NA#' else \
        acpy1.accountPayableVendorNumber END) as clearingBoxGLVendorNumber"\
                ,"acpy1.accountPayableDocumentType as clearingBoxGLDocumentTypeERP"\
                ,"acpy1.accountPayablePostingKey as clearingBoxGLPostingKeyERP"\
                ,"(CASE when \
        ISNULL(cbbdm1.targetSystemValue) then \
        acpy1.accountPayablePostingKey else \
        cbbdm1.targetSystemValue END) as clearingBoxGLPostingKey"\
                ,"rtrim(ltrim(acpy1.accountPayableAccountType)) as clearingBoxAccountTypeERP"\
                ,"cast('' as VARCHAR(32)) as clearingBoxAccountType"\
                ,"rtrim(ltrim((CASE when \
        ISNULL(stgcbj01.clearingBoxGLSpecialIndicatorERP) then \
        '' else \
        stgcbj01.clearingBoxGLSpecialIndicatorERP END))) as clearingBoxGLSpecialIndicatorERP"\
                ,"stgcbj01.clearingBoxGLSpecialIndicator as clearingBoxGLSpecialIndicator"\
                ,"cast((CASE when \
        (CASE when \
        (CASE when \
        ISNULL(stgcbj01.clearingBoxGLClearingDocument) then \
        '' else \
        stgcbj01.clearingBoxGLClearingDocument END) <> '' then \
        stgcbj01.clearingBoxGLClearingDocument else \
        acpy1.accountPayableClearingDocumentNumber END) <> '' then \
        1 else \
        0 END) as boolean) as isCleared"\
                ,"CASE when \
        acpy1.accountPayablePostingDate < '"+startDate+"' \
                        AND ((CASE when \
        (CASE when \
        ISNULL(stgcbj01.clearingBoxGLClearingDocument) then \
         '' else stgcbj01.clearingBoxGLClearingDocument END) <> ''	\
        then \
        stgcbj01.clearingBoxGLClearingPostingDate else \
        acpy1.accountPayableClearingDate END) >= '"+startDate+"' \
                        OR (CASE when \
        ISNULL("+A+") then \
         '1900-01-01' else \
        "+A+" END) = '1900-01-01') \
                then \
        true else \
        false END as isOpenItemPY"\
                ,"CASE WHEN(CASE when \
        (CASE when \
        ISNULL(stgcbj01.clearingBoxGLClearingDocument) then \
         '' else \
        stgcbj01.clearingBoxGLClearingDocument END) <> ''\
                          then \
        stgcbj01.clearingBoxGLClearingDocument else \
        acpy1.accountPayableClearingDocumentNumber END) = ''\
                OR (CASE when \
        (CASE when \
        ISNULL(stgcbj01.clearingBoxGLClearingDocument) then \
         '' else \
        stgcbj01.clearingBoxGLClearingDocument END) <> ''\
                    then \
        stgcbj01.clearingBoxGLClearingPostingDate\
                    else \
        acpy1.accountPayableClearingDate END) > '"+endDate+"' then \
        true else \
        false END as isOpenItemYE"\
        ,"cast(2 as int) as caseID"\
        ,"cast('AP' as CHAR(2)) as caseLogistic")
        
        gen_L1_TMP_ClearingBox_01_BasicPopulation=\
        gen_L1_TMP_ClearingBox_01_BasicPopulation\
		.union(gen_L1_TMP_ClearingBox_01_BasicPopulation_3).cache()
        
        gen_L1_TMP_ClearingBox_01_CollectedIdGLJELine=\
        gen_L1_TMP_ClearingBox_01_CollectedIdGLJELine\
        .union(gen_L1_TMP_ClearingBox_01_BasicPopulation_3\
        .select("idGLJELine").distinct()).cache()
        
        if gen_L1_TMP_ClearingBox_01_BasicPopulation_3 is not None:
            gen_L1_TMP_ClearingBox_01_BasicPopulation_3.unpersist()

        gen_L1_TMP_ClearingBox_01_BasicPopulation_4=\
        gen_L1_STG_ClearingBox_01_Journal.alias("stgcbj01")\
        .join(gen_L1_TMP_ClearingBox_01_CollectedAccount.alias("tcbca01"),\
        expr("( tcbca01.clearingBoxGLAccountNumber = stgcbj01.clearingBoxGLAccountNumber\
         AND tcbca01.clearingBoxCompanyCode = stgcbj01.clearingBoxCompanyCode\
         AND tcbca01.caseLogistic = 'AR' ) "),"left")\
        .join(gen_L1_TMP_ClearingBox_01_CollectedIdGLJELine.alias("tcidgll1"),\
        expr("( stgcbj01.idGLJELine = tcidgll1.idGLJELine ) "),"left")\
        .join(fin_L1_STG_Receivable.alias("srec1"),\
        expr("( srec1.GLJEDocumentNumber = stgcbj01.clearingBoxGLJEDocument\
         AND srec1.GLJELineNumber = stgcbj01.clearingBoxGLJELineNumber\
         AND srec1.fiscalYear = stgcbj01.clearingBoxGLFiscalYear\
         AND srec1.companyCode = stgcbj01.clearingBoxCompanyCode\
         AND srec1.client = stgcbj01.clearingBoxClientERP )"),"left")\
        .filter(expr("tcidgll1.idGLJELine IS NULL \
         AND stgcbj01.clearingBoxGLClearingDocument <> '' \
         AND stgcbj01.clearingBoxGLDocumentStatus IN ('N','C') \
         AND ((stgcbj01.clearingBoxAccountType ='Customer') \
		 OR (tcbca01.clearingBoxGLAccountNumber IS NOT NULL))"))\
        .selectExpr("stgcbj01.idGLJE as idGLJE"\
        ,"stgcbj01.idGLJELine as idGLJELine"\
        ,"stgcbj01.clearingBoxClientERP as clearingBoxClientERP"\
        ,"stgcbj01.clearingBoxCompanyCode as clearingBoxCompanyCode"\
        ,"stgcbj01.clearingBoxGLFiscalYear as clearingBoxGLFiscalYear"\
        ,"stgcbj01.clearingBoxGLFinancialPeriod as clearingBoxGLFinancialPeriod"\
        ,"stgcbj01.clearingBoxGLPostingDate as clearingBoxGLPostingDate"\
        ,"stgcbj01.clearingBoxGLJEDocument as clearingBoxGLJEDocument"\
        ,"stgcbj01.clearingBoxGLJELineNumber as clearingBoxGLJELineNumber"\
        ,"stgcbj01.clearingBoxGLClearingDocument as clearingBoxGLClearingDocument"\
        ,"stgcbj01.clearingBoxGLClearingPostingDate as clearingBoxGLClearingPostingDate"\
        ,"stgcbj01.clearingBoxGLAccountNumber as clearingBoxGLAccountNumber"\
        ,"stgcbj01.clearingBoxGLDebitCreditIndicator as clearingBoxGLDebitCreditIndicator"\
        ,"stgcbj01.clearingBoxGLAmountLC as clearingBoxGLAmountLC"\
        ,"stgcbj01.clearingBoxGLAmountDiscountLC as clearingBoxGLAmountDiscountLC"\
        ,"coalesce(CASE when \
        stgcbj01.clearingBoxGLReferenceSLSource = 'Sales S/L' then \
        stgcbj01.clearingBoxSLDocumentNumber else \
        NULL END, stgcbj01.clearingBoxGLBillingDocument, '') as clearingBoxGLBillingDocument"\
                ,"coalesce(CASE when \
        stgcbj01.clearingBoxGLReferenceSLSource = 'Purchase S/L' then \
        stgcbj01.clearingBoxSLDocumentNumber else \
        NULL END, stgcbj01.clearingBoxGLPurchaseOrder, '') as clearingBoxGLPurchaseOrder"\
                ,"(CASE when \
        (CASE when \
        ISNULL(stgcbj01.clearingBoxGLCustomerNumber) then \
        '' else \
        stgcbj01.clearingBoxGLCustomerNumber END) = '' then \
        '#NA#' else \
        stgcbj01.clearingBoxGLCustomerNumber END) as clearingBoxGLCustomerNumber"\
                ,"(CASE when \
        (CASE when \
        ISNULL(stgcbj01.clearingBoxGLVendorNumber) then \
        '' else \
        stgcbj01.clearingBoxGLVendorNumber END) = '' then \
        '#NA#' else \
        stgcbj01.clearingBoxGLVendorNumber END) as clearingBoxGLVendorNumber"\
        ,"stgcbj01.clearingBoxGLDocumentTypeERP as clearingBoxGLDocumentTypeERP"\
        ,"stgcbj01.clearingBoxGLPostingKeyERP as clearingBoxGLPostingKeyERP"\
        ,"stgcbj01.clearingBoxGLPostingKey as clearingBoxGLPostingKey"\
        ,"rtrim(ltrim(stgcbj01.clearingBoxAccountTypeERP))\
		as clearingBoxAccountTypeERP"\
        ,"cast('' as VARCHAR(32)) as clearingBoxAccountType"\
        ,"rtrim(ltrim(stgcbj01.clearingBoxGLSpecialIndicatorERP))\
		as clearingBoxGLSpecialIndicatorERP"\
        ,"stgcbj01.clearingBoxGLSpecialIndicator as clearingBoxGLSpecialIndicator"\
        ,"cast((CASE when \
        stgcbj01.clearingBoxGLClearingDocument <> '' then \
        1 else \
        0 END) as boolean) as isCleared"\
                ,"(CASE when \
        ISNULL(srec1.isOpenItemPY) then \
        false else \
        srec1.isOpenItemPY END) as isOpenItemPY"\
                ,"(CASE when \
        ISNULL(srec1.isOpenItemYE) then \
        false else \
        srec1.isOpenItemYE END) as isOpenItemYE"\
        ,"cast(3 as int) as caseID"\
        ,"cast('AR' as CHAR(2)) as caseLogistic").cache()
        
        gen_L1_TMP_ClearingBox_01_BasicPopulation=\
        gen_L1_TMP_ClearingBox_01_BasicPopulation\
		.union(gen_L1_TMP_ClearingBox_01_BasicPopulation_4).cache()
        
        
        gen_L1_TMP_ClearingBox_01_CollectedIdGLJELine=\
        gen_L1_TMP_ClearingBox_01_CollectedIdGLJELine\
        .union(gen_L1_TMP_ClearingBox_01_BasicPopulation_4\
        .select("idGLJELine").distinct()).cache()
        
        if gen_L1_TMP_ClearingBox_01_BasicPopulation_4 is not None:
            gen_L1_TMP_ClearingBox_01_BasicPopulation_4.unpersist()

        gen_L1_TMP_ClearingBox_01_BasicPopulation_5=\
        gen_L1_STG_ClearingBox_01_Journal.alias("stgcbj01")\
        .join(gen_L1_TMP_ClearingBox_01_CollectedAccount.alias("tcbca01"),\
        expr("( tcbca01.clearingBoxGLAccountNumber = stgcbj01.clearingBoxGLAccountNumber\
         AND tcbca01.clearingBoxCompanyCode = stgcbj01.clearingBoxCompanyCode\
         AND tcbca01.caseLogistic ='AP' ) "),"left")\
        .join(gen_L1_TMP_ClearingBox_01_CollectedIdGLJELine.alias("tcidgll1"),\
        expr("( stgcbj01.idGLJELine = tcidgll1.idGLJELine )"),"left")\
        .filter(expr("tcidgll1.idGLJELine IS NULL \
         AND stgcbj01.clearingBoxGLClearingDocument <> '' \
         AND stgcbj01.clearingBoxGLDocumentStatus IN ('N','C') \
         AND ((stgcbj01.clearingBoxAccountType ='Vendor') \
		 OR (tcbca01.clearingBoxGLAccountNumber IS NOT NULL))"))\
        .selectExpr("stgcbj01.idGLJE as idGLJE"\
        ,"stgcbj01.idGLJELine as idGLJELine"\
        ,"stgcbj01.clearingBoxClientERP as clearingBoxClientERP"\
        ,"stgcbj01.clearingBoxCompanyCode as clearingBoxCompanyCode"\
        ,"stgcbj01.clearingBoxGLFiscalYear as clearingBoxGLFiscalYear"\
        ,"stgcbj01.clearingBoxGLFinancialPeriod as clearingBoxGLFinancialPeriod"\
        ,"stgcbj01.clearingBoxGLPostingDate as clearingBoxGLPostingDate"\
        ,"stgcbj01.clearingBoxGLJEDocument as clearingBoxGLJEDocument"\
        ,"stgcbj01.clearingBoxGLJELineNumber as clearingBoxGLJELineNumber"\
        ,"stgcbj01.clearingBoxGLClearingDocument as clearingBoxGLClearingDocument"\
        ,"stgcbj01.clearingBoxGLClearingPostingDate as clearingBoxGLClearingPostingDate"\
        ,"stgcbj01.clearingBoxGLAccountNumber as clearingBoxGLAccountNumber"\
        ,"stgcbj01.clearingBoxGLDebitCreditIndicator as clearingBoxGLDebitCreditIndicator"\
        ,"stgcbj01.clearingBoxGLAmountLC as clearingBoxGLAmountLC"\
        ,"stgcbj01.clearingBoxGLAmountDiscountLC as clearingBoxGLAmountDiscountLC"\
        ,"coalesce(CASE when \
        stgcbj01.clearingBoxGLReferenceSLSource = 'Sales S/L' then \
        LEFT(stgcbj01.clearingBoxGLReferenceSLDocument, 10) else \
        NULL END, stgcbj01.clearingBoxGLBillingDocument, '') as clearingBoxGLBillingDocument"\
                ,"coalesce(CASE when \
        stgcbj01.clearingBoxGLReferenceSLSource = 'Purchase S/L' then \
        LEFT(stgcbj01.clearingBoxGLReferenceSLDocument, 10) else \
        NULL END, stgcbj01.clearingBoxGLPurchaseOrder, '') as clearingBoxGLPurchaseOrder"\
                ,"(CASE when \
        (CASE when \
        ISNULL(stgcbj01.clearingBoxGLCustomerNumber) then \
        '' else \
        stgcbj01.clearingBoxGLCustomerNumber END) = '' then \
        '#NA#' else \
        stgcbj01.clearingBoxGLCustomerNumber END) as clearingBoxGLCustomerNumber"\
                ,"(CASE when \
        (CASE when \
        ISNULL(stgcbj01.clearingBoxGLVendorNumber) then \
        '' else \
        stgcbj01.clearingBoxGLVendorNumber END) = '' then \
        '#NA#' else \
        stgcbj01.clearingBoxGLVendorNumber END) as clearingBoxGLVendorNumber"\
        ,"stgcbj01.clearingBoxGLDocumentTypeERP as clearingBoxGLDocumentTypeERP"\
        ,"stgcbj01.clearingBoxGLPostingKeyERP as clearingBoxGLPostingKeyERP"\
        ,"stgcbj01.clearingBoxGLPostingKey as clearingBoxGLPostingKey"\
        ,"rtrim(ltrim(stgcbj01.clearingBoxAccountTypeERP)) as clearingBoxAccountTypeERP"\
        ,"cast('' as VARCHAR(32)) as clearingBoxAccountType"\
        ,"rtrim(ltrim(stgcbj01.clearingBoxGLSpecialIndicatorERP)) \
       as clearingBoxGLSpecialIndicatorERP"\
        ,"stgcbj01.clearingBoxGLSpecialIndicator as clearingBoxGLSpecialIndicator"\
        ,"cast((CASE when \
        stgcbj01.clearingBoxGLClearingDocument <> '' then \
        1 else \
        0 END) as boolean) as isCleared"\
                ,"CASE when \
        stgcbj01.clearingBoxGLPostingDate < '"+startDate+"' \
                AND (stgcbj01.clearingBoxGLClearingPostingDate >= '"+startDate+"'\
                     OR (CASE when \
        ISNULL(stgcbj01.clearingBoxGLClearingPostingDate)\
                         then \
         '1900-01-01' else \
        stgcbj01.clearingBoxGLClearingPostingDate END) = '1900-01-01')	then \
        true else \
        false END as isOpenItemPY"\
                ,"CASE when \
        (stgcbj01.clearingBoxGLClearingDocument = '' \
                OR stgcbj01.clearingBoxGLClearingPostingDate > '"+endDate+"') then \
        true else \
        false END as isOpenItemYE"\
        ,"cast(3 as int) as caseID"\
        ,"cast('AP' as CHAR(2)) as caseLogistic").cache()
        
        gen_L1_TMP_ClearingBox_01_BasicPopulation=\
        gen_L1_TMP_ClearingBox_01_BasicPopulation\
		.union(gen_L1_TMP_ClearingBox_01_BasicPopulation_5).cache()

        if gen_L1_TMP_ClearingBox_01_BasicPopulation_5 is not None:
            gen_L1_TMP_ClearingBox_01_BasicPopulation_5.unpersist()
        
        gen_L1_TMP_ClearingBox_01_BasicPopulation=\
        gen_L1_TMP_ClearingBox_01_BasicPopulation\
        .withColumn("clearingBoxGLSpecialIndicator",\
                    expr("case when \
        clearingBoxGLSpecialIndicator IS NULL then \
        'Normal' else \
        clearingBoxGLSpecialIndicator end"))

        gen_L1_TMP_ClearingBox_01_BasicPopulation=\
        gen_L1_TMP_ClearingBox_01_BasicPopulation.alias("tcbbp01")\
        .join(gen_L1_STG_ClearingBoxBusinessDataMapping.alias("cbbdm1"),\
        expr("( cbbdm1.businessDatatype = 'GL Account Type'\
         AND cbbdm1.sourceSystemValue = \
         tcbbp01.clearingBoxAccountTypeERP )"),"left")\
        .groupBy("tcbbp01.clearingBoxGLAmountDiscountLC"\
        ,"tcbbp01.clearingBoxGLPostingKeyERP"\
        ,"tcbbp01.clearingBoxGLVendorNumber"\
        ,"tcbbp01.caseID"\
        ,"tcbbp01.clearingBoxGLFiscalYear"\
        ,"tcbbp01.isOpenItemPY"\
        ,"tcbbp01.isOpenItemYE"\
        ,"tcbbp01.clearingBoxCompanyCode"\
        ,"tcbbp01.clearingBoxGLCustomerNumber"\
        ,"tcbbp01.isCleared"\
        ,"tcbbp01.clearingBoxGLAccountNumber"\
        ,"tcbbp01.clearingBoxGLFinancialPeriod"\
        ,"tcbbp01.idGLJE"\
        ,"tcbbp01.clearingBoxGLDocumentTypeERP"\
        ,"tcbbp01.clearingBoxGLPurchaseOrder"\
        ,"tcbbp01.clearingBoxGLSpecialIndicator"\
        ,"tcbbp01.clearingBoxGLAmountLC"\
        ,"tcbbp01.clearingBoxAccountTypeERP"\
        ,"tcbbp01.clearingBoxGLClearingDocument"\
        ,"tcbbp01.clearingBoxGLBillingDocument"\
        ,"tcbbp01.clearingBoxGLSpecialIndicatorERP"\
        ,"tcbbp01.clearingBoxGLPostingDate"\
        ,"tcbbp01.clearingBoxGLJEDocument"\
        ,"tcbbp01.clearingBoxClientERP"\
        ,"tcbbp01.idGLJELine"\
        ,"tcbbp01.clearingBoxGLDebitCreditIndicator"\
        ,"tcbbp01.clearingBoxGLJELineNumber"\
        ,"tcbbp01.caseLogistic"\
        ,"tcbbp01.clearingBoxGLPostingKey"\
        ,"tcbbp01.clearingBoxGLClearingPostingDate")\
        .agg(expr("max(case when \
        cbbdm1.targetSystemValue is not null then \
        cbbdm1.targetSystemValue else \
        tcbbp01.clearingBoxAccountType end) as clearingBoxAccountType")).cache()
        
        
        gen_L1_TMP_ClearingBox_01_DefineIdClearingChain=\
        gen_L1_TMP_ClearingBox_01_BasicPopulation.alias("tcbbp01")\
        .selectExpr("CASE when \
        tcbbp01.clearingBoxGLClearingDocument = '' THEN \
        (-1) * dense_rank() OVER (ORDER BY tcbbp01.clearingBoxClientERP,\
        tcbbp01.clearingBoxCompanyCode)  else \
        dense_rank() OVER (ORDER BY tcbbp01.clearingBoxClientERP, \
        tcbbp01.clearingBoxCompanyCode, tcbbp01.clearingBoxGLClearingDocument,\
        tcbbp01.clearingBoxGLClearingPostingDate) END as idClearingChain"\
        ,"tcbbp01.clearingBoxClientERP as clearingBoxClientERP"\
        ,"tcbbp01.clearingBoxCompanyCode as clearingBoxCompanyCode"\
        ,"tcbbp01.clearingBoxGLClearingDocument as clearingBoxGLClearingDocument"\
        ,"tcbbp01.clearingBoxGLClearingPostingDate as clearingBoxGLClearingPostingDate")\
        .distinct().cache()
        
        gen_L1_TMP_ClearingBox_01_BasicPopulation=\
        gen_L1_TMP_ClearingBox_01_BasicPopulation.alias("tcbbp01")\
        .join(gen_L1_TMP_ClearingBox_01_DefineIdClearingChain.alias("tcbdfc01"),\
        expr("( tcbbp01.clearingBoxGLClearingDocument = tcbdfc01.clearingBoxGLClearingDocument\
         AND tcbbp01.clearingBoxGLClearingPostingDate = tcbdfc01.clearingBoxGLClearingPostingDate\
         AND tcbbp01.clearingBoxCompanyCode = tcbdfc01.clearingBoxCompanyCode\
         AND tcbbp01.clearingBoxClientERP = tcbdfc01.clearingBoxClientERP )"),"left")\
        .groupBy("tcbbp01.clearingBoxGLAmountDiscountLC"\
        ,"tcbbp01.clearingBoxGLPostingKeyERP"\
        ,"tcbbp01.clearingBoxGLVendorNumber"\
        ,"tcbbp01.caseID"\
        ,"tcbbp01.clearingBoxGLFiscalYear"\
        ,"tcbbp01.isOpenItemPY"\
        ,"tcbbp01.isOpenItemYE"\
        ,"tcbbp01.clearingBoxCompanyCode"\
        ,"tcbbp01.clearingBoxGLCustomerNumber"\
        ,"tcbbp01.isCleared"\
        ,"tcbbp01.clearingBoxGLAccountNumber"\
        ,"tcbbp01.clearingBoxGLFinancialPeriod"\
        ,"tcbbp01.idGLJE"\
        ,"tcbbp01.clearingBoxGLDocumentTypeERP"\
        ,"tcbbp01.clearingBoxGLPurchaseOrder"\
        ,"tcbbp01.clearingBoxAccountType"\
        ,"tcbbp01.clearingBoxGLSpecialIndicator"\
        ,"tcbbp01.clearingBoxGLAmountLC"\
        ,"tcbbp01.clearingBoxAccountTypeERP"\
        ,"tcbbp01.clearingBoxGLClearingDocument"\
        ,"tcbbp01.clearingBoxGLBillingDocument"\
        ,"tcbbp01.clearingBoxGLSpecialIndicatorERP"\
        ,"tcbbp01.clearingBoxGLPostingDate"\
        ,"tcbbp01.clearingBoxGLJEDocument"\
        ,"tcbbp01.clearingBoxClientERP"\
        ,"tcbbp01.idGLJELine"\
        ,"tcbbp01.clearingBoxGLDebitCreditIndicator"\
        ,"tcbbp01.clearingBoxGLJELineNumber"\
        ,"tcbbp01.caseLogistic"\
        ,"tcbbp01.clearingBoxGLPostingKey"\
        ,"tcbbp01.clearingBoxGLClearingPostingDate")\
        .agg(expr("max(tcbdfc01.idClearingChain)")).cache()
       
        gen_L1_TMP_ClearingBox_01_AllUnclearingChain=\
        gen_L1_TMP_ClearingBox_01_BasicPopulation.alias("tcbbp01")\
        .selectExpr("(-1) * cast(dense_rank() OVER (ORDER BY tcbbp01.clearingBoxClientERP,\
       tcbbp01.clearingBoxCompanyCode) as int) as idClearingChain"\
        ,"tcbbp01.clearingBoxClientERP as clearingBoxClientERP"\
        ,"tcbbp01.clearingBoxCompanyCode as clearingBoxCompanyCode"\
        ,"cast('' as varchar(10)) as clearingBoxGLClearingDocument"\
        ,"cast('1900-01-01' as DATE) as clearingBoxGLClearingPostingDate")\
        .distinct().cache()

        gen_L1_STG_ClearingBox_01_AllLogisticItem=\
        gen_L1_TMP_ClearingBox_01_BasicPopulation.alias("tcbbp01")\
        .join(gen_L1_TMP_ClearingBox_01_DefineIdClearingChain.alias("tcbicc01"),\
        expr("( tcbicc01.clearingBoxGLClearingDocument = tcbbp01.clearingBoxGLClearingDocument\
         AND  tcbicc01.clearingBoxGLClearingPostingDate = tcbbp01.clearingBoxGLClearingPostingDate\
         AND tcbicc01.clearingBoxCompanyCode = tcbbp01.clearingBoxCompanyCode\
         AND tcbicc01.clearingBoxClientERP = tcbbp01.clearingBoxClientERP) "),"left")\
        .join(gen_L1_TMP_ClearingBox_01_AllUnclearingChain.alias("taucc01"),\
        expr("( tcbbp01.clearingBoxGLClearingDocument = ''\
         AND tcbbp01.clearingBoxCompanyCode = taucc01.clearingBoxCompanyCode\
         AND tcbbp01.clearingBoxClientERP = taucc01.clearingBoxClientERP) "),"left")\
        .join(fin_L1_STG_GLAccountClassification.alias("glc1"),\
        expr("( tcbbp01.clearingBoxGLAccountNumber = glc1.accountNumber\
         AND tcbbp01.clearingBoxCompanyCode = glc1.companyCode ) "),"left")\
        .join(gen_L1_STG_ClearingBox_01_Journal.alias("stgcbj01"),\
        expr("( tcbbp01.idGLJELine = stgcbj01.idGLJELine )"),"left")\
        .selectExpr("case when isnull(tcbicc01.idClearingChain) then \
       taucc01.idClearingChain else\
      tcbicc01.idClearingChain end as idClearingChain"\
        ,"tcbbp01.idGLJE as idGLJE"\
        ,"tcbbp01.idGLJELine as idGLJELine"\
        ,"tcbbp01.clearingBoxClientERP as clearingBoxClientERP"\
        ,"tcbbp01.clearingBoxCompanyCode as clearingBoxCompanyCode"\
        ,"tcbbp01.clearingBoxGLClearingDocument as clearingBoxGLClearingDocument"\
        ,"tcbbp01.clearingBoxGLClearingPostingDate as clearingBoxGLClearingPostingDate"\
        ,"tcbbp01.clearingBoxGLFiscalYear as clearingBoxGLFiscalYear"\
        ,"tcbbp01.clearingBoxGLFinancialPeriod as clearingBoxGLFinancialPeriod"\
        ,"tcbbp01.clearingBoxGLJEDocument as clearingBoxGLJEDocument"\
        ,"tcbbp01.clearingBoxGLJELineNumber as clearingBoxGLJELineNumber"\
        ,"tcbbp01.clearingBoxGLPostingDate as clearingBoxGLPostingDate"\
        ,"tcbbp01.clearingBoxGLAccountNumber as clearingBoxGLAccountNumber"\
        ,"(CASE when \
        ISNULL(glc1.accountCategory) then \
        '#NA#' else \
        glc1.accountCategory END) as clearingBoxGLAccountCategory"\
                ,"tcbbp01.clearingBoxGLCustomerNumber as clearingBoxGLCustomerNumber"\
                ,"tcbbp01.clearingBoxGLVendorNumber as clearingBoxGLVendorNumber"\
                ,"tcbbp01.clearingBoxGLDocumentTypeERP as clearingBoxGLDocumentTypeERP"\
                ,"(CASE when \
        ISNULL(stgcbj01.clearingBoxGLTransactionCodeERP) then \
        '#NA#' else \
        stgcbj01.clearingBoxGLTransactionCodeERP END) as clearingBoxGLTransactionCodeERP"\
                ,"tcbbp01.clearingBoxGLPostingKeyERP as clearingBoxGLPostingKeyERP"\
                ,"tcbbp01.clearingBoxGLPostingKey as clearingBoxGLPostingKey"\
                ,"tcbbp01.clearingBoxAccountType as clearingBoxAccountType"\
                ,"tcbbp01.clearingBoxGLSpecialIndicator as clearingBoxGLSpecialIndicator"\
                ,"(CASE when \
        ISNULL(stgcbj01.clearingBoxGLReferenceSLSource) then \
        '#NA#' else \
        stgcbj01.clearingBoxGLReferenceSLSource END) as clearingBoxGLReferenceSLSource"\
                ,"(CASE when \
        ISNULL(stgcbj01.clearingBoxGLReferenceSLDocument) then \
        '' else \
        stgcbj01.clearingBoxGLReferenceSLDocument END) as clearingBoxGLReferenceSLDocument"\
                ,"(CASE when \
        ISNULL(stgcbj01.clearingBoxSLDocumentNumber) then \
        '' else \
        stgcbj01.clearingBoxSLDocumentNumber END) as clearingBoxSLDocumentNumber"\
                ,"(CASE when \
        ISNULL(stgcbj01.clearingBoxSLFiscalYear) then \
        '' else \
        stgcbj01.clearingBoxSLFiscalYear END) as clearingBoxSLFiscalYear"\
                ,"(CASE when \
        ISNULL(stgcbj01.clearingBoxSLDocumentTypeERP) then \
        '' else \
        stgcbj01.clearingBoxSLDocumentTypeERP END) as clearingBoxSLDocumentTypeERP"\
                ,"(CASE when \
        ISNULL(stgcbj01.clearingBoxSLDocumentType) then \
        '' else \
        stgcbj01.clearingBoxSLDocumentType END) as clearingBoxSLDocumentType"\
                ,"(CASE when \
        ISNULL(stgcbj01.isClearingBoxSLSubsequentDocument) then \
        false else \
        stgcbj01.isClearingBoxSLSubsequentDocument END) as isClearingBoxSLSubsequentDocument"\
                ,"(CASE when \
        ISNULL(stgcbj01.clearingBoxGLReferenceDocument) then \
        '' else \
        stgcbj01.clearingBoxGLReferenceDocument END) as clearingBoxGLReferenceDocument"\
                ,"tcbbp01.clearingBoxGLAmountLC as clearingBoxGLAmountLC"\
                ,"tcbbp01.clearingBoxGLAmountDiscountLC as clearingBoxGLAmountDiscountLC"\
                ,"tcbbp01.clearingBoxGLBillingDocument as clearingBoxGLBillingDocument"\
                ,"tcbbp01.clearingBoxGLPurchaseOrder as clearingBoxGLPurchaseOrder"\
                ,"(CASE when \
        ISNULL(glc1.isTradeAR) then \
        false else \
        glc1.isTradeAR END) as isTradeAR"\
                ,"(CASE when \
        ISNULL(glc1.isTradeAP) then \
        false else \
        glc1.isTradeAP END) as isTradeAP"\
                ,"cast((CASE when \
        (stgcbj01.clearingBoxGLReferenceSLSource = 'Sales S/L' \
        OR (tcbbp01.clearingBoxGLBillingDocument <> '' \
        AND stgcbj01.clearingBoxGLJEDocument IS NULL)) then \
        true else \
        false END) as boolean) as isSD"\
                ,"cast((CASE when \
        (stgcbj01.clearingBoxGLReferenceSLSource = 'Purchase S/L' \
        OR (tcbbp01.clearingBoxGLPurchaseOrder <> '' \
        AND stgcbj01.clearingBoxGLJEDocument IS NULL)) then \
        true else \
        false END) as boolean) as isMM"\
                ,"cast((CASE when \
        stgcbj01.clearingBoxOriginOfItem = 'gl_journal' then \
        false else \
        true END) as boolean) as isMissingJE"\
                ,"(CASE when \
        ISNULL(stgcbj01.isReversedOrReversal) then \
        false else \
        cast(stgcbj01.isReversedOrReversal as boolean) END) \
        as isReversedOrReversal"\
                ,"(CASE when \
        tcbbp01.clearingBoxGLPostingDate\
        BETWEEN '"+startDate+"' AND '"+endDate+"' then \
        true else \
        false END)  as isInTimeFrame"\
                ,"(CASE when \
        ISNULL(tcbbp01.isOpenItemPY) then \
        false else \
        tcbbp01.isOpenItemPY END) as isOpenItemPY"\
                ,"(CASE when \
        ISNULL(tcbbp01.isOpenItemYE) then \
        false else \
        tcbbp01.isOpenItemYE END) as isOpenItemYE"\
                ,"(CASE when \
        ISNULL(tcbbp01.caseLogistic) then \
        '0' else \
        tcbbp01.caseLogistic END) as caseLogistic")

        gen_L1_STG_ClearingBox_01_Journal =\
           objDataTransformation.gen_convertToCDMandCache\
        (gen_L1_STG_ClearingBox_01_Journal,'gen',\
        'L1_STG_ClearingBox_01_Journal',True)
        
        gen_L1_STG_ClearingBox_01_AllLogisticItem =\
           objDataTransformation.gen_convertToCDMandCache\
        (gen_L1_STG_ClearingBox_01_AllLogisticItem,'gen',\
        'L1_STG_ClearingBox_01_AllLogisticItem',True)
        
        executionStatus = "gen_L1_STG_ClearingBox_01_AllLogisticItem and gen_L1_STG_ClearingBox_01_Journal populated successfully"
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

    except Exception as err:
        executionStatus = objGenHelper.gen_exceptionDetails_log()       
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
    finally:
        if gen_L1_TMP_ClearingBox_01_CollectedIdGLJELine is not None:
            gen_L1_TMP_ClearingBox_01_CollectedIdGLJELine.unpersist()
        if gen_L1_TMP_ClearingBox_01_BasicPopulation is not None:
            gen_L1_TMP_ClearingBox_01_BasicPopulation.unpersist()
        if gen_L1_TMP_ClearingBox_01_BasicPopulation_1 is not None:
            gen_L1_TMP_ClearingBox_01_BasicPopulation_1.unpersist()
        if gen_L1_TMP_ClearingBox_01_BasicPopulation_2 is not None:
            gen_L1_TMP_ClearingBox_01_BasicPopulation_2.unpersist()
        if gen_L1_TMP_ClearingBox_01_BasicPopulation_3 is not None:
            gen_L1_TMP_ClearingBox_01_BasicPopulation_3.unpersist()
        if gen_L1_TMP_ClearingBox_01_BasicPopulation_4 is not None:
            gen_L1_TMP_ClearingBox_01_BasicPopulation_4.unpersist()
        if gen_L1_TMP_ClearingBox_01_BasicPopulation_5 is not None:
            gen_L1_TMP_ClearingBox_01_BasicPopulation_5.unpersist()
        if gen_L1_TMP_ClearingBox_01_DefineIdClearingChain is not None:
            gen_L1_TMP_ClearingBox_01_DefineIdClearingChain.unpersist()
        if gen_L1_TMP_ClearingBox_01_AllUnclearingChain is not None:
            gen_L1_TMP_ClearingBox_01_AllUnclearingChain.unpersist()
