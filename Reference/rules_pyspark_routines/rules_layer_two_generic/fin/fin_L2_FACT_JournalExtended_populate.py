# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import  lit,col,row_number,floor,trim
from pyspark.sql.functions import monotonically_increasing_id as mi
from dateutil.parser import parse

def fin_L2_FACT_JournalExtended_populate():
    try:
        logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
        objGenHelper = gen_genericHelper()
        objDataTransformation = gen_dataTransformation()
        global fin_L2_FACT_Journal_Extended

        jrnldocumentReversalFiscalYear = when(col('jrnl.documentReversalFiscalYear').isNull(), lit('') ) \
                              .otherwise(col('jrnl.documentReversalFiscalYear'))
        dbdocumentReversalFiscalYear = when(col('db.documentReversalFiscalYear').isNull(), lit('') ) \
                              .otherwise(col('db.documentReversalFiscalYear'))
        jrnlconsistentEndingNumberOfDigit = when(col('jrnl.consistentEndingNumberOfDigit').isNull(), lit('') ) \
                              .otherwise(col('jrnl.consistentEndingNumberOfDigit'))
        dbconsistentEndingNumberOfDigit = when(col('db.consistentEndingNumberOfDigit').isNull(), lit('') ) \
                              .otherwise(col('db.consistentEndingNumberOfDigit'))
        jrnltransactionType = when(col('jrnl.transactionType').isNull(), lit('') ) \
                              .otherwise(col('jrnl.transactionType'))
        dbtransactionType = when(col('db.transactionType').isNull(), lit('') ) \
                              .otherwise(col('db.transactionType'))
        jrnltransactionTypeDescription = when(col('jrnl.transactionTypeDescription').isNull(), lit('') ) \
                              .otherwise(col('jrnl.transactionTypeDescription'))
        dbtransactionTypeDescription = when(col('db.transactionTypeDescription').isNull(), lit('') ) \
                              .otherwise(col('db.transactionTypeDescription'))
        jrnlfirstDigitOfAmount = when(col('jrnl.firstDigitOfAmount').isNull(), lit(0) ) \
                              .otherwise(col('jrnl.firstDigitOfAmount'))
        dbfirstDigitOfAmount = when(col('db.firstDigitOfAmount').isNull(), lit(0) ) \
                              .otherwise(col('db.firstDigitOfAmount'))
        jrnlpostPeriodDays = when(col('jrnl.postPeriodDays').isNull(), lit(0) ) \
                              .otherwise(col('jrnl.postPeriodDays'))
        dbpostPeriodDays = when(col('db.postPeriodDays').isNull(), lit(0) ) \
                              .otherwise(col('db.postPeriodDays'))
        jrnlreversalID = when(col('jrnl.reversalID').isNull(), lit(0) ) \
                              .otherwise(col('jrnl.reversalID'))
        dbreversalID = when(col('db.reversalID').isNull(), lit(0) ) \
                              .otherwise(col('db.reversalID'))
        jrnlreferenceDocumentNumber = when(col('jrnl.referenceDocumentNumber').isNull(), lit('') ) \
                              .otherwise(col('jrnl.referenceDocumentNumber'))
        dcreferenceDocumentNumber = when(col('dc.referenceDocumentNumber').isNull(), lit('') ) \
                              .otherwise(col('dc.referenceDocumentNumber'))
        jrnlreferenceSubledgerDocumentNumber = when(col('jrnl.referenceSubledgerDocumentNumber').isNull(), lit('') ) \
                              .otherwise(col('jrnl.referenceSubledgerDocumentNumber'))
        dcreferenceSubledgerDocumentNumber = when(col('dc.referenceSubledgerDocumentNumber').isNull(), lit('') ) \
                              .otherwise(col('dc.referenceSubledgerDocumentNumber'))
        jrnlcustomDate = when(col('jrnl.customDate').isNull(), lit('1901-01-01') ) \
                              .otherwise(col('jrnl.customDate'))
        ddcustomDate = when(col('dd.customDate').isNull(), lit('1901-01-01') ) \
                              .otherwise(col('dd.customDate'))

            
        fin_L2_FACT_Journal_Extended = fin_L2_FACT_Journal.alias('jrnl') \
                     .join(fin_L2_DIM_FinancialPeriod.alias('fp'), \
                           (col('jrnl.postingDateSurrogateKey').eqNullSafe(col('fp.financialPeriodSurrogateKey'))),how ='left') \
                     .join(fin_L2_DIM_DocumentDetail_DocumentBase.alias('db'), \
                           ((col('jrnl.documentCurrencySurrogateKey').eqNullSafe(col('db.currencySurrogateKey'))) \
                           &(lit(jrnldocumentReversalFiscalYear) == lit(dbdocumentReversalFiscalYear)) \
                           &(lit(jrnlconsistentEndingNumberOfDigit) == lit(dbconsistentEndingNumberOfDigit)) \
                           &(lit(jrnltransactionType) == lit(dbtransactionType)) \
                           &(lit(jrnltransactionTypeDescription) == lit(dbtransactionTypeDescription)) \
                           &(lit(jrnlfirstDigitOfAmount) == lit(dbfirstDigitOfAmount)) \
                           &(lit(jrnlpostPeriodDays) == lit(dbpostPeriodDays)) \
                           &(lit(jrnlreversalID) == lit(dbreversalID))),how ='left') \
                     .join(fin_L2_DIM_DocumentDetail_DocumentCore.alias('dc'), \
                           ((col('jrnl.documentNumber').eqNullSafe(col('dc.documentNumber'))) \
                           &(col('jrnl.reverseDocumentNumber').eqNullSafe(col('dc.reverseDocumentNumber'))) \
                           &(lit(jrnlreferenceDocumentNumber) == lit(dcreferenceDocumentNumber)) \
                           &(lit(jrnlreferenceSubledgerDocumentNumber) == lit(dcreferenceSubledgerDocumentNumber))),how = 'left') \
                     .join(fin_L2_DIM_DocumentDetail_DocumentLineItem.alias('dli'), \
                           ((col('jrnl.documentLineNumber').eqNullSafe(col('dli.documentLineNumber'))) \
                           &(col('jrnl.documentLineNumberSort').eqNullSafe(col('dli.documentLineNumberSort')))),how = 'left') \
                     .join(fin_L2_DIM_DocumentDetail_DocumentDetail.alias('dd'), \
                           ((col('jrnl.assignmentNumber').eqNullSafe(col('dd.assignmentNumber'))) \
                           &(col('jrnl.clearingDocumentNumber').eqNullSafe(col('dd.clearingDocumentNumber'))) \
                           &(lit(jrnlcustomDate) == lit(ddcustomDate))),how = 'left') \
                     .join(fin_L2_DIM_DocumentDetail_DocumentDescription.alias('ddesc'), \
                           (col('jrnl.documentHeaderDescription').eqNullSafe(col('ddesc.documentHeaderDescription'))),how = 'left') \
                     .join(fin_L2_DIM_DocumentDetail_DocumentLineItemDescription.alias('dlidesc'), \
                           (col('jrnl.documentLineDescription').eqNullSafe(col('dlidesc.documentLineDescription'))),how = 'left') \
                     .join(fin_L2_DIM_DocumentDetail_DocumentCustomText.alias('dct'), \
                            ((col('jrnl.customText').eqNullSafe(col('dct.customText'))) \
                            &(col('jrnl.customText02').eqNullSafe(col('dct.customText02'))) \
                            &(col('jrnl.customText03').eqNullSafe(col('dct.customText03'))) \
                            &(col('jrnl.customText04').eqNullSafe(col('dct.customText04'))) \
                            &(col('jrnl.customText05').eqNullSafe(col('dct.customText05'))) \
                            &(col('jrnl.customText06').eqNullSafe(col('dct.customText06'))) \
                            &(col('jrnl.customText07').eqNullSafe(col('dct.customText07'))) \
                            &(col('jrnl.customText08').eqNullSafe(col('dct.customText08'))) \
                            &(col('jrnl.customText09').eqNullSafe(col('dct.customText09'))) \
                            &(col('jrnl.customText10').eqNullSafe(col('dct.customText10'))) \
                            &(col('jrnl.customText11').eqNullSafe(col('dct.customText11'))) \
                            &(col('jrnl.customText12').eqNullSafe(col('dct.customText12'))) \
                            &(col('jrnl.customText13').eqNullSafe(col('dct.customText13'))) \
                            &(col('jrnl.customText14').eqNullSafe(col('dct.customText14'))) \
                            &(col('jrnl.customText15').eqNullSafe(col('dct.customText15'))) \
                            &(col('jrnl.customText16').eqNullSafe(col('dct.customText16'))) \
                            &(col('jrnl.customText17').eqNullSafe(col('dct.customText17'))) \
                            &(col('jrnl.customText18').eqNullSafe(col('dct.customText18'))) \
                            &(col('jrnl.customText19').eqNullSafe(col('dct.customText19'))) \
                            &(col('jrnl.customText20').eqNullSafe(col('dct.customText20'))) \
                            &(col('jrnl.customText21').eqNullSafe(col('dct.customText21'))) \
                            &(col('jrnl.customText22').eqNullSafe(col('dct.customText22'))) \
                            &(col('jrnl.customText23').eqNullSafe(col('dct.customText23'))) \
                            &(col('jrnl.customText24').eqNullSafe(col('dct.customText24'))) \
                            &(col('jrnl.customText25').eqNullSafe(col('dct.customText25'))) \
                            &(col('jrnl.customText26').eqNullSafe(col('dct.customText26'))) \
                            &(col('jrnl.customText27').eqNullSafe(col('dct.customText27'))) \
                            &(col('jrnl.customText28').eqNullSafe(col('dct.customText28'))) \
                            &(col('jrnl.customText29').eqNullSafe(col('dct.customText29'))) \
                            &(col('jrnl.customText30').eqNullSafe(col('dct.customText30')))),how = 'left')\
                     .select(col('jrnl.journalSurrogateKey').alias('journalSurrogateKey')\
                             ,when(col('fp.financialPeriod').isNull(),lit(0)) \
                             .otherwise(col('fp.financialPeriod')).alias('financialPeriod')\
                             ,when(col('db.DocumentBaseID').isNull(),lit(-1)) \
                             .otherwise(col('db.DocumentBaseID')).alias('documentBaseID')\
                             ,when(col('dli.documentLineItemID').isNull(),lit(-1)) \
                             .otherwise(col('dli.documentLineItemID')).alias('documentLineItemID')\
                             ,when(col('dd.DocumentDetailID').isNull(),lit(-1)) \
                             .otherwise(col('dd.DocumentDetailID')).alias('documentDetailID')\
                             ,when(col('dc.DocumentCoreID').isNull(),lit(-1)) \
                             .otherwise(col('dc.DocumentCoreID')).alias('documentCoreID')\
                             ,when(col('ddesc.DocumentDescriptionID').isNull(),lit(-1)) \
                             .otherwise(col('ddesc.DocumentDescriptionID')).alias('documentDescriptionID')\
                             ,when(col('dlidesc.DocumentLineItemDescriptionID').isNull(),lit(-1)) \
                             .otherwise(col('dlidesc.DocumentLineItemDescriptionID')).alias('documentLineItemDescriptionID')\
                             ,when(col('dct.DocumentCustomTextID').isNull(),lit(-1)) \
                             .otherwise(col('dct.DocumentCustomTextID')).alias('documentCustomTextID'))
        
        fin_L2_FACT_Journal_Extended  = objDataTransformation.gen_convertToCDMandCache \
            (fin_L2_FACT_Journal_Extended,'fin','L2_FACT_Journal_Extended',False)
        
        executionStatus = "fin_L2_FACT_Journal_Extended populated sucessfully"
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
    
    except Exception as err:
        executionStatus = objGenHelper.gen_exceptionDetails_log()
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

