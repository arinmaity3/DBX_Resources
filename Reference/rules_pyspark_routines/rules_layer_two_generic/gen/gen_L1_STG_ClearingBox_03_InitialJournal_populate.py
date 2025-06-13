# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import expr,col,lit,min,max
def gen_L1_STG_ClearingBox_03_InitialJournal_populate():
    try:
        logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
        global gen_L1_STG_ClearingBox_03_AllJournalLineItem
        
        objGenHelper = gen_genericHelper()
        objDataTransformation = gen_dataTransformation()
        startDate =  str(parse(objGenHelper\
		.gen_lk_cd_parameter_get('GLOBAL', 'START_DATE')).date())
        endDate =  str(parse(objGenHelper\
		.gen_lk_cd_parameter_get('GLOBAL', 'END_DATE')).date())

        gen_L1_TMP_ClearingBox_03_AllPostingByJEID          = None
        idGLJEs                                          = None

        gen_L1_TMP_ClearingBox_03_RevenueDocumentByJEID=\
        gen_L1_STG_ClearingBox_01_Journal\
        .alias("stgcbj01")\
        .join(fin_L1_STG_GLAccountClassification\
        .alias("glc1"),\
        expr("( stgcbj01.clearingBoxGLAccountNumber = glc1.accountNumber\
         AND stgcbj01.clearingBoxCompanyCode = glc1.companyCode\
         AND glc1.isRevenue = 1\
         AND stgcbj01.clearingBoxGLPostingDate >= '"+startDate+"' )"),"inner")\
        .filter(expr("stgcbj01.clearingBoxGLDocumentStatus IN ('N','C')"))\
        .groupBy("stgcbj01.idGLJE")\
        .agg(expr("max(stgcbj01.clearingBoxClientERP) as clearingBoxClientERP")\
        ,expr("max(stgcbj01.clearingBoxCompanyCode) as clearingBoxCompanyCode")\
        ,expr("max(stgcbj01.clearingBoxGLPostingDate) as clearingBoxGLPostingDate")\
        ,expr("max(stgcbj01.clearingBoxGLFiscalYear) as clearingBoxGLFiscalYear")\
        ,expr("max(stgcbj01.clearingBoxGLJEDocument) as clearingBoxGLJEDocument"))

        gen_L1_TMP_ClearingBox_03_AllPostingByJEID=\
        gen_L1_STG_ClearingBox_02_AllAccountLogisticItemsByDoc\
        .alias("stgaalibd01")\
        .join(gen_L1_STG_ClearingBox_02_AllLogisticItemsMapByDocAndClearing\
        .alias("doccl"),\
        expr("( stgaalibd01.idGLJE = doccl.idGLJE ) "),"inner")\
        .join(gen_L1_STG_ClearingBox_02_ClearingChainWithGroup\
        .alias("clgrp"),\
        expr("( doccl.idClearingChain = clgrp.idClearingChain\
         AND doccl.idClearingChain > 0 ) "),"left")\
        .join(gen_L1_STG_ClearingBox_02_ChainGroup\
        .alias("grp"),\
        expr("( grp.idGroupChain = clgrp.idGroupChain )"),"left")\
        .groupBy("stgaalibd01.idGLJE")\
        .agg(expr("max(stgaalibd01.clearingBoxClientERP) as clearingBoxClientERP")\
        ,expr("max(stgaalibd01.clearingBoxCompanyCode) as clearingBoxCompanyCode")\
        ,expr("max(stgaalibd01.clearingBoxGLFiscalYear) as clearingBoxGLFiscalYear")\
        ,expr("max(stgaalibd01.clearingBoxGLPostingDate) as clearingBoxGLPostingDate")\
        ,expr("max(stgaalibd01.clearingBoxGLJEDocument) as clearingBoxGLJEDocument")\
        ,expr("cast(max(cast(stgaalibd01.isLogisticCaseAR as int)) as boolean) \
         as isLogisticCaseAR")\
        ,expr("cast(max(cast(stgaalibd01.isLogisticCaseAP as int)) as boolean) \
         as isLogisticCaseAP")\
        ,expr("max(clgrp.idClearingChain)	as idClearingChain")\
        ,expr("max(cast(grp.isOpenItemPY as TINYINT)) as isOpenItemPY")\
        ,expr("max(cast(grp.isOpenItemYE as TINYINT)) as isOpenItemYE ")\
        ,expr("max(grp.clearingBoxGLPostingDateMax) as clearingBoxGLPostingDateMax")\
        ,expr("max(grp.clearingBoxGLClearingPostingDateMax) \
		            as clearingBoxGLClearingPostingDateMax"))\
        .filter(expr("idClearingChain	IS NULL	\
        OR isOpenItemPY	 = 1 \
        OR isOpenItemYE      = 1 \
        OR clearingBoxGLPostingDateMax	 >= '"+startDate+"' \
        OR clearingBoxGLClearingPostingDateMax >= '"+startDate+"'"))\
        .selectExpr("idGLJE"\
        ,"clearingBoxClientERP"\
        ,"clearingBoxCompanyCode"\
        ,"clearingBoxGLFiscalYear"\
        ,"clearingBoxGLPostingDate"\
        ,"clearingBoxGLJEDocument"\
        ,"isLogisticCaseAR"\
        ,"isLogisticCaseAP").cache()


        gen_L1_TMP_ClearingBox_03_AllPostingByJEID_01=\
        gen_L1_TMP_ClearingBox_03_RevenueDocumentByJEID\
        .alias("tmprdbjid03")\
        .join(gen_L1_STG_ClearingBox_02_AllAccountLogisticItemsByDoc\
        .alias("stgaalibd01"),\
        expr("( stgaalibd01.idGLJE = tmprdbjid03.idGLJE )"),"left")\
        .filter(expr("stgaalibd01.idGLJE IS NULL"))\
        .selectExpr("tmprdbjid03.idGLJE as idGLJE"\
        ,"tmprdbjid03.clearingBoxClientERP as clearingBoxClientERP"\
        ,"tmprdbjid03.clearingBoxCompanyCode as clearingBoxCompanyCode"\
        ,"tmprdbjid03.clearingBoxGLFiscalYear as clearingBoxGLFiscalYear"\
        ,"tmprdbjid03.clearingBoxGLPostingDate as clearingBoxGLPostingDate"\
        ,"tmprdbjid03.clearingBoxGLJEDocument as clearingBoxGLJEDocument"\
        ,"cast(null as boolean) \
         as isLogisticCaseAR"\
        ,"cast(null as boolean) \
         as isLogisticCaseAR").cache()

        
        gen_L1_TMP_ClearingBox_03_AllPostingByJEID=\
        gen_L1_TMP_ClearingBox_03_AllPostingByJEID\
        .union(gen_L1_TMP_ClearingBox_03_AllPostingByJEID_01)

        idGLJEs=gen_L1_STG_ClearingBox_02_AllLogisticItemsMapByDocAndClearing\
        .select("idGLJE").distinct().cache()

        gen_L1_STG_ClearingBox_03_AllJournalLineItem=\
        gen_L1_STG_ClearingBox_01_Journal\
        .alias("stgcbj01")\
        .join(gen_L1_TMP_ClearingBox_03_AllPostingByJEID\
        .alias("tmpapbjeid03"),\
        expr("( stgcbj01.idGLJE = tmpapbjeid03.idGLJE ) "),"inner")\
        .join(fin_L1_STG_GLAccountClassification\
        .alias("glc1"),\
        expr("( stgcbj01.clearingBoxGLAccountNumber = glc1.accountNumber\
         AND stgcbj01.clearingBoxCompanyCode = glc1.companyCode ) "),"left")\
        .join(gen_L1_STG_ClearingBox_01_AllLogisticItem\
        .alias("stgali01"),\
        expr("( stgcbj01.idGLJELine = stgali01.idGLJELine ) "),"left")\
        .join(gen_L1_STG_ClearingBox_02_ClearingChainWithGroup\
        .alias("stgccwg02"),\
        expr("( stgali01.idClearingChain = stgccwg02.idClearingChain\
         AND stgccwg02.idClearingChain > 0 )"),"left")\
        .join(idGLJEs\
        .alias('idGLJEs'),expr("idGLJEs.idGLJE=stgcbj01.idGLJE"),"left")\
        .filter(expr("stgcbj01.clearingBoxGLDocumentStatus IN ('N','C')\
        and (stgcbj01.clearingBoxGLPostingDate >= '"+startDate+"'\
            or idGLJEs.idGLJE is not null)"))\
        .selectExpr("stgccwg02.idGroupChain as idGroupChain"\
        ,"stgcbj01.idGLJE as idGLJE"\
        ,"stgcbj01.idGLJELine as idGLJELine"\
        ,"stgcbj01.clearingBoxClientERP as clearingBoxClientERP"\
        ,"stgcbj01.clearingBoxCompanyCode as clearingBoxCompanyCode"\
        ,"stgcbj01.clearingBoxGLFiscalYear as clearingBoxGLFiscalYear"\
        ,"stgcbj01.clearingBoxGLFinancialPeriod as clearingBoxGLFinancialPeriod"\
        ,"stgcbj01.clearingBoxGLPostingDate as clearingBoxGLPostingDate"\
        ,"(CASE WHEN \
        (CASE WHEN \
        ISNULL(stgali01.clearingBoxGLClearingDocument) THEN \
            '' ELSE \
                stgali01.clearingBoxGLClearingDocument END) \
             <> '' THEN \
            stgali01.clearingBoxGLClearingDocument ELSE \
                stgcbj01.clearingBoxGLClearingDocument END) \
             as clearingBoxGLClearingDocument"\
        ,"(CASE WHEN \
        (CASE WHEN \
        ISNULL(stgali01.clearingBoxGLClearingPostingDate) THEN \
            '' ELSE \
                stgali01.clearingBoxGLClearingPostingDate END) \
             <> '' THEN \
            stgali01.clearingBoxGLClearingPostingDate ELSE \
                stgcbj01.clearingBoxGLClearingPostingDate END) \
             as clearingBoxGLClearingPostingDate"\
        ,"stgcbj01.clearingBoxGLJEDocument as clearingBoxGLJEDocument"\
        ,"stgcbj01.clearingBoxGLJELineNumber as clearingBoxGLJELineNumber"\
        ,"stgcbj01.clearingBoxGLAccountNumber as clearingBoxGLAccountNumber"\
        ,"stgcbj01.clearingBoxGLAccountCategory as clearingBoxGLAccountCategory"\
        ,"stgcbj01.clearingBoxGLAmountLC as clearingBoxGLAmountLC"\
        ,"stgcbj01.clearingBoxGLAmountDiscountLC as clearingBoxGLAmountDiscountLC"\
        ,"(CASE WHEN \
        stgcbj01.clearingBoxGLCustomerNumber = '' THEN \
            '#NA#' ELSE \
                stgcbj01.clearingBoxGLCustomerNumber END) \
             as clearingBoxGLCustomerNumber"\
        ,"(CASE WHEN \
        stgcbj01.clearingBoxGLVendorNumber = '' THEN \
            '#NA#' ELSE \
                stgcbj01.clearingBoxGLVendorNumber END) \
             as clearingBoxGLVendorNumber"\
        ,"stgcbj01.clearingBoxGLDocumentTypeERP as clearingBoxGLDocumentTypeERP"\
        ,"stgcbj01.clearingBoxGLDocumentType as clearingBoxGLDocumentType"\
        ,"stgcbj01.clearingBoxGLTransactionCodeERP as clearingBoxGLTransactionCodeERP"\
        ,"stgcbj01.clearingBoxGLHeaderDescription as clearingBoxGLHeaderDescription"\
        ,"stgcbj01.clearingBoxGLLineDescription as clearingBoxGLLineDescription"\
        ,"stgcbj01.clearingBoxGLPostingKeyERP as clearingBoxGLPostingKeyERP"\
        ,"stgcbj01.clearingBoxGLPostingKey as clearingBoxGLPostingKey"\
        ,"stgcbj01.clearingBoxGLSpecialIndicator as clearingBoxGLSpecialIndicator"\
        ,"stgcbj01.clearingBoxGLReferenceSLSource as clearingBoxGLReferenceSLSource"\
        ,"stgcbj01.clearingBoxSLDocumentNumber as clearingBoxSLDocumentNumber"\
        ,"stgcbj01.clearingBoxSLFiscalYear as clearingBoxSLFiscalYear"\
        ,"(CASE WHEN \
        ISNULL(stgcbj01.clearingBoxSLDocumentTypeERP) THEN \
            '' ELSE \
                stgcbj01.clearingBoxSLDocumentTypeERP END) \
             as clearingBoxSLDocumentTypeERP"\
        ,"stgcbj01.clearingBoxSLDocumentType as clearingBoxSLDocumentType"\
        ,"stgcbj01.isClearingBoxSLSubsequentDocument as isClearingBoxSLSubsequentDocument"\
        ,"stgcbj01.clearingBoxGLReferenceDocument as clearingBoxGLReferenceDocument"\
        ,"(CASE WHEN \
        ISNULL(glc1.isTradeAR) THEN \
            false ELSE \
                glc1.isTradeAR END) \
             as isTradeAR"\
        ,"(CASE WHEN \
        ISNULL(glc1.isTradeAP) THEN \
            false ELSE \
                glc1.isTradeAP END) \
             as isTradeAP"\
        ,"cast(CASE WHEN \
        (CASE WHEN \
        ISNULL(glc1.isTradeAR) THEN \
            false ELSE \
                glc1.isTradeAR END) \
             = 0 AND stgcbj01.clearingBoxAccountType IN('Customer', 'D') THEN \
            1 ELSE \
                0 END as boolean) \
         as isOtherAR"\
        ,"cast(CASE WHEN \
        (CASE WHEN \
        ISNULL(glc1.isTradeAP) THEN \
            false ELSE \
                glc1.isTradeAP END) \
             = 0 AND stgcbj01.clearingBoxAccountType IN('Vendor', 'K') THEN \
            1 ELSE \
                0 END as boolean) \
         as isOtherAP"\
        ,"cast((CASE WHEN \
        (stgcbj01.clearingBoxGLReferenceSLSource = 'Sales S/L') THEN \
            1 ELSE \
                0 END) \
            as boolean) \
         as isSD"\
        ,"cast((CASE WHEN \
        (stgcbj01.clearingBoxGLReferenceSLSource = 'Purchase S/L') THEN \
            1 ELSE \
                0 END) \
            as boolean) \
         as isMM"\
        ,"cast((CASE WHEN \
        stgcbj01.clearingBoxOriginOfItem = 'gl_journal' THEN \
            0 ELSE \
                1 END) \
             as boolean) \
         as isMissingJE"\
        ,"stgcbj01.isReversedOrReversal as isReversedOrReversal"\
        ,"cast((CASE WHEN \
        stgcbj01.clearingBoxGLPostingDate BETWEEN '"+startDate+"' \
		AND '"+endDate+"' THEN \
            1 ELSE \
                0 END) \
             as boolean) \
         as isInTimeFrame"\
        ,"(CASE WHEN \
        ISNULL(stgali01.isOpenItemPY) THEN \
            false ELSE \
                stgali01.isOpenItemPY END) \
             as isOpenItemPY"\
        ,"(CASE WHEN \
        ISNULL(stgali01.isOpenItemYE) THEN \
            false ELSE \
                stgali01.isOpenItemYE END) \
             as isOpenItemYE")


        gen_L1_STG_ClearingBox_03_AllJournalLineItem=\
        gen_L1_STG_ClearingBox_03_AllJournalLineItem\
        .alias("stgcbajli03")\
        .join(fin_L1_TD_Journal\
        .alias("jrnl1"),\
        expr("( stgcbajli03.clearingBoxGLJEDocument = jrnl1.reversalDocumentNumber\
         AND stgcbajli03.clearingBoxGLFiscalYear = jrnl1.reversalFiscalYear\
         AND stgcbajli03.clearingBoxCompanyCode = jrnl1.companyCode\
         AND stgcbajli03.isReversedOrReversal = 0 )"),"left")\
        .groupBy("stgcbajli03.isTradeAR"\
        ,"stgcbajli03.clearingBoxGLLineDescription"\
        ,"stgcbajli03.clearingBoxGLAmountDiscountLC"\
        ,"stgcbajli03.clearingBoxGLAccountNumber"\
        ,"stgcbajli03.idGLJELine"\
        ,"stgcbajli03.clearingBoxGLDocumentType"\
        ,"stgcbajli03.clearingBoxSLFiscalYear"\
        ,"stgcbajli03.clearingBoxSLDocumentType"\
        ,"stgcbajli03.clearingBoxGLPostingKey"\
        ,"stgcbajli03.clearingBoxGLCustomerNumber"\
        ,"stgcbajli03.isOpenItemPY"\
        ,"stgcbajli03.isClearingBoxSLSubsequentDocument"\
        ,"stgcbajli03.clearingBoxGLVendorNumber"\
        ,"stgcbajli03.clearingBoxGLPostingKeyERP"\
        ,"stgcbajli03.isOtherAR"\
        ,"stgcbajli03.clearingBoxGLClearingDocument"\
        ,"stgcbajli03.clearingBoxGLClearingPostingDate"\
        ,"stgcbajli03.clearingBoxGLAccountCategory"\
        ,"stgcbajli03.isOtherAP"\
        ,"stgcbajli03.idGroupChain"\
        ,"stgcbajli03.clearingBoxGLAmountLC"\
        ,"stgcbajli03.isInTimeFrame"\
        ,"stgcbajli03.clearingBoxGLFiscalYear"\
        ,"stgcbajli03.clearingBoxClientERP"\
        ,"stgcbajli03.clearingBoxGLReferenceDocument"\
        ,"stgcbajli03.isTradeAP"\
        ,"stgcbajli03.isMM"\
        ,"stgcbajli03.clearingBoxGLJELineNumber"\
        ,"stgcbajli03.clearingBoxGLDocumentTypeERP"\
        ,"stgcbajli03.clearingBoxGLPostingDate"\
        ,"stgcbajli03.idGLJE"\
        ,"stgcbajli03.isOpenItemYE"\
        ,"stgcbajli03.clearingBoxGLHeaderDescription"\
        ,"stgcbajli03.clearingBoxSLDocumentTypeERP"\
        ,"stgcbajli03.clearingBoxGLJEDocument"\
        ,"stgcbajli03.isSD"\
        ,"stgcbajli03.isMissingJE"\
        ,"stgcbajli03.clearingBoxGLSpecialIndicator"\
        ,"stgcbajli03.clearingBoxCompanyCode"\
        ,"stgcbajli03.clearingBoxGLTransactionCodeERP"\
        ,"stgcbajli03.clearingBoxSLDocumentNumber"\
        ,"stgcbajli03.clearingBoxGLReferenceSLSource"\
        ,"stgcbajli03.clearingBoxGLFinancialPeriod")\
        .agg(expr("max(case WHEN \
        jrnl1.companyCode is not null THEN \
            1 ELSE \
                stgcbajli03.isReversedOrReversal END) \
             as isReversedOrReversal"))


        gen_L1_STG_ClearingBox_03_AllJournalLineItem =\
           objDataTransformation.gen_convertToCDMandCache\
        (gen_L1_STG_ClearingBox_03_AllJournalLineItem,'gen',\
        'L1_STG_ClearingBox_03_AllJournalLineItem',True)
        
        executionStatus = "gen_L1_STG_ClearingBox_03_AllJournalLineItem populated successfully"
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

    except Exception as err:
        executionStatus = objGenHelper.gen_exceptionDetails_log()       
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
    finally:
        if gen_L1_TMP_ClearingBox_03_AllPostingByJEID is not None:
            gen_L1_TMP_ClearingBox_03_AllPostingByJEID.unpersist()
        if idGLJEs is not None:
            idGLJEs.unpersist()
