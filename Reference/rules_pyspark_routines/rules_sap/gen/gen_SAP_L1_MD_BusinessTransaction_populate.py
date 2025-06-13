# Databricks notebook source
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import sys
import traceback
from pyspark.sql.functions import row_number,lit,col,when

def gen_SAP_L1_MD_BusinessTransaction_populate(): 
  try:
    
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None,None)
    global gen_L1_MD_BusinessTransaction   
    
    #gen_SAP_L1_MD_BusinessTransaction_populate
    df_BusinessTransactionClientDataReportingLanguage=erp_T022.alias("t022")\
          .join (erp_T022T.alias("t022t")\
            ,(col("t022.ACTIVITY")== col("t022t.ACTIVITY")), "inner")\
          .join (knw_LK_ISOLanguageKey.alias("lngkey")\
            ,(col("lngkey.sourceSystemValue")    == col("t022t.LANGU")), "inner")\
          .join (knw_clientDataReportingLanguage.alias("rlg1")\
             ,(col("rlg1.languageCode")   == col("lngkey.targetSystemValue")), "inner")\
          .select(col('t022.ACTIVITY').alias('businessTransactionCode')\
             ,col('t022t.TXT').alias('businessTransactionDescription')\
             ,col('rlg1.languageCode')).distinct()
    
    erp_T022_NotExists = erp_T022.alias("t022")\
          .join(df_BusinessTransactionClientDataReportingLanguage.alias('dpk')\
             ,(col("t022.ACTIVITY")   == col("dpk.businessTransactionCode")) , 'leftanti')
  
    df_BusinessTransactionClientDataSystemReportingLanguage=erp_T022_NotExists.alias("t022")\
          .join (erp_T022T.alias("t022t")\
             ,(col("t022.ACTIVITY").eqNullSafe(col("t022t.ACTIVITY"))), "left")\
          .join (knw_LK_ISOLanguageKey.alias("lngkey")\
             ,(col("lngkey.sourceSystemValue").eqNullSafe(col("t022t.LANGU"))), "left")\
           .join (knw_clientDataSystemReportingLanguage.alias("rlg1")\
             ,(col("rlg1.languageCode").eqNullSafe(col("lngkey.targetSystemValue"))), "left")\
          .select(col('t022.ACTIVITY').alias('businessTransactionCode')\
             ,when((col('t022t.TXT').isNull()),'#NA#')\
               .otherwise(col('t022t.TXT')).alias('businessTransactionDescription')\
             ,when((col('rlg1.languageCode').isNull()),'NA')\
               .otherwise(col('rlg1.languageCode')).alias('languageCode')\
                  ).distinct()
   
    gen_L1_MD_BusinessTransaction=df_BusinessTransactionClientDataReportingLanguage.distinct()\
          .union(df_BusinessTransactionClientDataSystemReportingLanguage.distinct())
    
    gen_L1_MD_BusinessTransaction =  objDataTransformation.gen_convertToCDMandCache \
          (gen_L1_MD_BusinessTransaction,'gen','L1_MD_BusinessTransaction',targetPath=gl_CDMLayer1Path)
       
    executionStatus = "L1_MD_BusinessTransaction populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
