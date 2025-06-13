# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType
import sys
import traceback
def fin_L2_DIM_DocumentDetailComplete_populate():
  try:
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
    global fin_L2_DIM_DocumentDetail_Complete
    dwh_vw_DIM_DocumentDetail_Complete = dwh_vw_DIM_DocumentDetail_DetailLink.alias('DL') \
        .join(dwh_vw_DIM_DocumentDetail_DocumentBase.alias('DB') \
              ,col('DL.documentBaseID').eqNullSafe(col('DB.documentBaseID')),'left') \
        .join(dwh_vw_DIM_DocumentDetail_DocumentCore.alias('DC') \
              ,col('DL.documentCoreID').eqNullSafe(col('DC.documentCoreID')),'left') \
        .select(col('DL.detailLinkID').alias('detailLinkID') \
                ,col('DB.documentBaseID').alias('documentBaseID') \
                ,col('DB.currencySurrogateKey').alias('currencySurrogateKey') \
                ,col('DB.documentCurrency').alias('documentCurrency') \
                ,col('DB.documentReversalFiscalYear').alias('documentReversalFiscalYear') \
                ,col('DB.consistentEndingNumberOfDigit').alias('consistentEndingNumberOfDigit') \
                ,col('DB.transactionType').alias('transactionType') \
                ,col('DB.transactionTypeDescription').alias('transactionTypeDescription') \
                ,col('DB.firstDigitOfAmount').alias('firstDigitOfAmount') \
                ,col('DB.postPeriodDays').alias('postPeriodDays') \
                ,col('DB.reversalID').alias('reversalID') \
                ,col('DC.documentCoreID').alias('documentCoreID') \
                ,col('DC.documentNumber').alias('documentNumber') \
                ,col('DC.reverseDocumentNumber').alias('reverseDocumentNumber') \
                ,col('DC.referenceDocumentNumber').alias('referenceDocumentNumber') \
                ,col('DC.referenceSubledgerDocumentNumber').alias('referenceSubledgerDocumentNumber'))

    
    fin_L2_DIM_DocumentDetail_Complete = objDataTransformation.gen_convertToCDMStructure_generate \
        (dwh_vw_DIM_DocumentDetail_Complete,'dwh','vw_DIM_DocumentDetail_Complete',isIncludeAnalysisID = True, isSqlFormat = True)[0]
    
    fin_L2_DIM_DocumentDetail_Complete.createOrReplaceTempView('fin_L2_DIM_DocumentDetail_Complete')
    
    objGenHelper.gen_writeToFile_perfom(fin_L2_DIM_DocumentDetail_Complete,gl_CDMLayer2Path \
        + "fin_L2_DIM_DocumentDetail_Complete.parquet" ) 

    executionStatus = "fin_L2_DIM_DocumentDetail_Complete populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus) 
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

  except Exception as e:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus) 
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]


