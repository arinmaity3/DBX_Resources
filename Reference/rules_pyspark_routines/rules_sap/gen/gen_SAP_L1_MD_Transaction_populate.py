# Databricks notebook source
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import sys
import traceback
from pyspark.sql.functions import row_number,lit,col,when

def gen_SAP_L1_MD_Transaction_populate(): 
  try:
    
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None,None)
    global gen_L1_MD_Transaction
    
    df_TransactionClientDataReportingLanguage=erp_TSTCT.alias("tstct")\
        .join (knw_LK_ISOLanguageKey.alias("lngkey")\
          ,(col("lngkey.sourceSystemValue") == col("tstct.SPRSL")), "inner")\
        .join (knw_clientDataReportingLanguage.alias("rlg1")\
          ,((col("rlg1.languageCode")== col("lngkey.targetSystemValue"))), "inner")\
        .select (col('tstct.TCODE').alias('transactionType')\
          ,col('tstct.TTEXT').alias('transactionTypeDescription')\
          ,col('rlg1.languageCode'))\
        .distinct()
                     
    erp_TSTCT_NotExists = erp_TSTCT.alias('tstct')\
        .join(df_TransactionClientDataReportingLanguage.alias('tstct_CDRL')\
          ,(col("tstct_CDRL.transactionType") == col("tstct.TCODE")) , 'leftanti')
    
    df_TransactionClientDataSystemReportingLanguage=erp_TSTCT_NotExists.alias("tstct")\
        .join (knw_LK_ISOLanguageKey.alias("lngkey")\
          ,(col("lngkey.sourceSystemValue").eqNullSafe(col("tstct.SPRSL"))), "left")\
        .join (knw_clientDataSystemReportingLanguage.alias("rlg1")\
          ,(col("rlg1.languageCode").eqNullSafe(col("lngkey.targetSystemValue"))), "left")\
        .select(col('tstct.TCODE').alias('transactionType')\
          ,when((col('tstct.TTEXT').isNull()),'#NA#')\
            .otherwise(col('tstct.TTEXT')).alias('transactionTypeDescription')\
          ,when((col('rlg1.languageCode').isNull()),'NA')\
            .otherwise(col('rlg1.languageCode')).alias('languageCode')\
                ).distinct()
    #df_TransactionClientDataSystemReportingLanguage.display()    
    
    gen_L1_MD_Transaction=df_TransactionClientDataReportingLanguage\
        .union(df_TransactionClientDataSystemReportingLanguage)
    
    gen_L1_MD_Transaction = objDataTransformation.gen_convertToCDMandCache \
          (gen_L1_MD_Transaction,'gen','L1_MD_Transaction',targetPath=gl_CDMLayer1Path)
        
    executionStatus = "L1_MD_Transaction populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
